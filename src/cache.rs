use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension, Row};

use crate::api::{ChangeItem, DriveFile, FOLDER_MIME_TYPE, GOOGLE_APPS_MIME_PREFIX};

/// Root inode is strictly pinned to FUSE root ID (`1`).
pub const ROOT_INODE: u64 = fuser::FUSE_ROOT_ID;

/// Google Drive identifier for the root folder.
pub const ROOT_DRIVE_ID: &str = "root";

/// Reserved virtual inode (`2`) for the top-level "Shared with me" folder.
pub const SHARED_WITH_ME_INODE: u64 = 2;

/// Google Drive virtual identifier for the "Shared with me" folder.
pub const SHARED_WITH_ME_DRIVE_ID: &str = "shared_with_me";

/// Display name for the virtual "Shared with me" directory.
pub const SHARED_WITH_ME_NAME: &str = "Shared with me";

/// Prefix for temporary `drive_id` values assigned to newly created local files prior to their first upload.
pub const PENDING_DRIVE_ID_PREFIX: &str = "pending:";

/// Key in `sync_state` table for storing the `changes.list` page token.
const CHANGES_PAGE_TOKEN_KEY: &str = "changes_page_token";

/// Default cache database filename inside the user's cache directory.
const DEFAULT_DB_FILENAME: &str = "metadata.db";

/// Default application directory name under XDG cache dir.
const APP_CACHE_DIR_NAME: &str = "gdrive-fuse-rs";

/// Cached metadata for a Google Drive file or folder mapped to a persistent FUSE inode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedFile {
    pub inode: u64,
    pub drive_id: String,
    pub name: String,
    pub mime_type: String,
    pub size: u64,
    pub modified_time: i64,
    pub is_dir: bool,
    pub last_synced_at: Option<i64>,
    pub web_view_link: Option<String>,
    pub dirty: bool,
}

impl CachedFile {
    /// Returns `true` if this file was created locally (`create`) and has not yet completed its initial upload to Google Drive.
    pub fn is_pending_upload(&self) -> bool {
        self.drive_id.starts_with(PENDING_DRIVE_ID_PREFIX)
    }

    /// Returns `true` if this file is a Google Workspace native document
    /// (e.g., Google Docs, Sheets, Slides) rather than a folder or regular binary file.
    pub fn is_google_native_doc(&self) -> bool {
        self.mime_type.starts_with(GOOGLE_APPS_MIME_PREFIX) && !self.is_dir
    }

    /// Returns the browser URL to open this file in Google Docs/Drive.
    /// Uses `web_view_link` from the Drive API if present and non-empty,
    /// otherwise falls back to standard Google Workspace / Drive URLs.
    pub fn web_url(&self) -> String {
        if let Some(ref link) = self.web_view_link {
            let trimmed = link.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }

        match self.mime_type.as_str() {
            "application/vnd.google-apps.document" => {
                format!("https://docs.google.com/document/d/{}/edit", self.drive_id)
            }
            "application/vnd.google-apps.spreadsheet" => {
                format!(
                    "https://docs.google.com/spreadsheets/d/{}/edit",
                    self.drive_id
                )
            }
            "application/vnd.google-apps.presentation" => {
                format!(
                    "https://docs.google.com/presentation/d/{}/edit",
                    self.drive_id
                )
            }
            _ => format!("https://drive.google.com/open?id={}", self.drive_id),
        }
    }
}

/// Represents a resolved child entry inside a parent directory with its deduplicated POSIX filename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryEntry {
    pub inode: u64,
    pub parent_inode: u64,
    pub effective_name: String,
    pub file: CachedFile,
}

/// Errors returned by the SQLite metadata cache layer.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Cache entry not found: {0}")]
    NotFound(String),

    #[error("Inode {0} is not a directory")]
    NotADirectory(u64),

    #[error("Cache mutex lock poisoned")]
    LockPoisoned,
}

impl CacheError {
    /// Maps cache errors cleanly to standard POSIX error codes (`libc`).
    pub fn to_errno(&self) -> libc::c_int {
        match self {
            CacheError::NotFound(_) => libc::ENOENT,
            CacheError::NotADirectory(_) => libc::ENOTDIR,
            CacheError::Sqlite(_) | CacheError::Io(_) | CacheError::LockPoisoned => libc::EIO,
        }
    }
}

/// Alias for `MetadataCache` for convenience across modules and tests.
pub type Cache = MetadataCache;

/// Embedded SQLite metadata and persistent inode cache.
pub struct MetadataCache {
    conn: Mutex<Connection>,
    ttl_secs: u64,
    evicted_inodes: Mutex<HashSet<u64>>,
    removal_tracker: Mutex<RemovalTracker>,
    staging_namespace: Option<String>,
}

#[derive(Default)]
struct RemovalTracker {
    generations: HashMap<u64, u64>,
    pending: HashMap<u64, u64>,
}

impl RemovalTracker {
    fn advance_generation(&mut self, inode: u64) -> u64 {
        let generation = self.generations.entry(inode).or_default();
        *generation = generation.wrapping_add(1);
        *generation
    }

    fn queue_removal(&mut self, inode: u64) -> u64 {
        let generation = self.advance_generation(inode);
        self.pending.insert(inode, generation);
        generation
    }

    fn cancel_removal(&mut self, inode: u64) {
        self.advance_generation(inode);
        self.pending.remove(&inode);
    }

    fn is_current(&self, inode: u64, generation: u64) -> bool {
        self.generations.get(&inode) == Some(&generation)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemovedInode {
    pub inode: u64,
    pub generation: u64,
}

impl MetadataCache {
    /// Returns the default XDG cache database path (`~/.cache/gdrive-fuse-rs/metadata.db`).
    pub fn default_db_path() -> PathBuf {
        let base_dir = dirs::cache_dir().unwrap_or_else(|| PathBuf::from(".cache"));
        base_dir.join(APP_CACHE_DIR_NAME).join(DEFAULT_DB_FILENAME)
    }

    /// Returns the default temporary write-back staging directory path (`~/.cache/gdrive-fuse-rs/staging/`).
    pub fn default_staging_dir() -> PathBuf {
        let base_dir = dirs::cache_dir().unwrap_or_else(|| PathBuf::from(".cache"));
        base_dir.join(APP_CACHE_DIR_NAME).join("staging")
    }

    /// Normalizes a database path for staging namespace computation so that a relative path whose
    /// database file does not exist yet resolves to the exact same canonical path as after
    /// `MetadataCache::open` creates the database file on disk.
    fn normalize_db_path_for_namespace(db_path: &Path) -> PathBuf {
        if let Ok(canonical) = fs::canonicalize(db_path) {
            return canonical;
        }

        let abs_path = if db_path.is_absolute() {
            db_path.to_path_buf()
        } else {
            std::env::current_dir()
                .map(|cwd| cwd.join(db_path))
                .unwrap_or_else(|_| db_path.to_path_buf())
        };

        let mut normalized = PathBuf::new();
        for comp in abs_path.components() {
            match comp {
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    if let Ok(canonical) = fs::canonicalize(&normalized) {
                        normalized = canonical;
                    }
                    let _ = normalized.pop();
                }
                other => normalized.push(other.as_os_str()),
            }
        }

        // Canonicalize the deepest existing ancestor directory and append the remaining tail components
        // so symlinks in parent directories (e.g., `/var/run` -> `/run`) resolve identically before and after file creation.
        let mut cursor = normalized.as_path();
        let mut tail_components = Vec::new();
        while let Some(name) = cursor.file_name() {
            if cursor.exists() {
                if let Ok(canon_ancestor) = fs::canonicalize(cursor) {
                    let mut result = canon_ancestor;
                    for part in tail_components.into_iter().rev() {
                        result.push(part);
                    }
                    return result;
                }
                break;
            }
            tail_components.push(name.to_os_string());
            match cursor.parent() {
                Some(parent) => cursor = parent,
                None => break,
            }
        }

        normalized
    }

    /// Computes a deterministic per-database staging namespace directory name (`db_<64-bit-fnv1a-hex>`)
    /// when `db_path` differs from `default_db_path()`, preventing staging file collisions across
    /// concurrent mounts that use separate `--cache-db` files.
    pub fn staging_namespace_for_db_path(db_path: &Path) -> Option<String> {
        let canonical = Self::normalize_db_path_for_namespace(db_path);
        let default_path = Self::default_db_path();
        let canonical_default = Self::normalize_db_path_for_namespace(&default_path);
        if canonical == canonical_default {
            return None;
        }
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in canonical.to_string_lossy().as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        Some(format!("db_{:016x}", hash))
    }

    /// Returns the default temporary staging directory scoped to `db_path`.
    pub fn default_staging_dir_for_db(db_path: &Path) -> PathBuf {
        let base = Self::default_staging_dir();
        match Self::staging_namespace_for_db_path(db_path) {
            Some(ns) => base.join(ns),
            None => base,
        }
    }

    /// Returns the per-database staging subdirectory namespace, if this cache was opened from a custom `--cache-db`.
    pub fn staging_namespace(&self) -> Option<&str> {
        self.staging_namespace.as_deref()
    }

    /// Returns the default staging directory scoped to this `MetadataCache` instance.
    pub fn scoped_default_staging_dir(&self) -> PathBuf {
        let base = Self::default_staging_dir();
        match self.staging_namespace.as_deref() {
            Some(ns) => base.join(ns),
            None => base,
        }
    }

    /// Removes the SQLite database file along with its `-wal` and `-shm` companion files if present.
    pub fn remove_db_files(db_path: &Path) -> Result<(), CacheError> {
        let mut wal_os = db_path.as_os_str().to_os_string();
        wal_os.push("-wal");
        let wal_path = PathBuf::from(wal_os);

        let mut shm_os = db_path.as_os_str().to_os_string();
        shm_os.push("-shm");
        let shm_path = PathBuf::from(shm_os);

        for candidate in [db_path, wal_path.as_path(), shm_path.as_path()] {
            match fs::remove_file(candidate) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(CacheError::Io(err)),
            }
        }
        Ok(())
    }

    /// Removes any leftover `inode_*.stage` staging files in `staging_dir`.
    pub fn clear_staging_dir(staging_dir: &Path) -> Result<(), CacheError> {
        if !staging_dir.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(staging_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.starts_with("inode_") && name.ends_with(".stage") {
                        let _ = fs::remove_file(&path);
                    }
                }
            }
        }
        Ok(())
    }

    /// Opens or creates the SQLite metadata cache at `db_path` with the specified directory TTL (in seconds).
    pub fn open(db_path: &Path, ttl_secs: u64) -> Result<Self, CacheError> {
        if let Some(parent) = db_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }

        let conn = Connection::open(db_path)?;
        let mut cache = Self::init_connection(conn, ttl_secs)?;
        cache.staging_namespace = Self::staging_namespace_for_db_path(db_path);
        Ok(cache)
    }

    /// Opens the SQLite metadata cache at the default XDG cache path (`~/.cache/gdrive-fuse-rs/metadata.db`).
    pub fn open_default(ttl_secs: u64) -> Result<Self, CacheError> {
        let base_dir = dirs::cache_dir().ok_or_else(|| {
            CacheError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "could not determine the user cache directory; pass --cache-db explicitly",
            ))
        })?;
        let db_path = base_dir.join(APP_CACHE_DIR_NAME).join(DEFAULT_DB_FILENAME);
        Self::open(&db_path, ttl_secs)
    }

    /// Opens an isolated in-memory SQLite metadata cache (ideal for unit and integration testing).
    pub fn open_in_memory(ttl_secs: u64) -> Result<Self, CacheError> {
        let conn = Connection::open_in_memory()?;
        Self::init_connection(conn, ttl_secs)
    }

    /// Returns the configured directory metadata TTL in seconds.
    pub fn ttl_secs(&self) -> u64 {
        self.ttl_secs
    }

    fn init_connection(mut conn: Connection, ttl_secs: u64) -> Result<Self, CacheError> {
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS files (
                inode INTEGER PRIMARY KEY AUTOINCREMENT,
                drive_id TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                size INTEGER NOT NULL DEFAULT 0,
                modified_time INTEGER NOT NULL DEFAULT 0,
                is_dir INTEGER NOT NULL DEFAULT 0,
                last_synced_at INTEGER,
                web_view_link TEXT,
                dirty INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS parent_child (
                parent_inode INTEGER NOT NULL,
                child_inode INTEGER NOT NULL,
                effective_name TEXT NOT NULL,
                PRIMARY KEY (parent_inode, effective_name),
                FOREIGN KEY (parent_inode) REFERENCES files(inode) ON DELETE CASCADE,
                FOREIGN KEY (child_inode) REFERENCES files(inode) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_parent_child_child ON parent_child(child_inode);

            CREATE TABLE IF NOT EXISTS sync_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            ",
        )?;

        // Gracefully migrate existing Phase 1 / Phase 2 databases.
        let _ = conn.execute("ALTER TABLE files ADD COLUMN web_view_link TEXT", []);
        let _ = conn.execute(
            "ALTER TABLE files ADD COLUMN dirty INTEGER NOT NULL DEFAULT 0",
            [],
        );

        let now = Utc::now().timestamp();
        {
            let tx = conn.transaction()?;

            tx.execute(
                "INSERT OR IGNORE INTO files (inode, drive_id, name, mime_type, size, modified_time, is_dir, last_synced_at, web_view_link)
                 VALUES (?1, ?2, '', ?3, 0, ?4, 1, NULL, NULL)",
                params![ROOT_INODE as i64, ROOT_DRIVE_ID, FOLDER_MIME_TYPE, now],
            )?;

            // If migrating an existing Phase 1 database where inode 2 was already assigned
            // to a real Drive item, transactionally remap that item and its parent_child edges
            // to a fresh inode before reserving inode 2 for SHARED_WITH_ME_INODE.
            if let Some(existing) = Self::get_by_inode_with_conn(&tx, SHARED_WITH_ME_INODE)? {
                if existing.drive_id != SHARED_WITH_ME_DRIVE_ID {
                    let new_inode: i64 = tx.query_row(
                        "SELECT COALESCE(MAX(inode), ?1) + 1 FROM files",
                        params![SHARED_WITH_ME_INODE as i64],
                        |r| r.get(0),
                    )?;

                    let mut saved_edges: Vec<(i64, i64, String)> = Vec::new();
                    {
                        let mut edge_stmt = tx.prepare(
                            "SELECT parent_inode, child_inode, effective_name
                             FROM parent_child
                             WHERE parent_inode = ?1 OR child_inode = ?1",
                        )?;
                        let rows = edge_stmt.query_map(params![SHARED_WITH_ME_INODE as i64], |r| {
                            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                        })?;
                        for row in rows {
                            saved_edges.push(row?);
                        }
                    }

                    tx.execute(
                        "DELETE FROM parent_child WHERE parent_inode = ?1 OR child_inode = ?1",
                        params![SHARED_WITH_ME_INODE as i64],
                    )?;

                    tx.execute(
                        "UPDATE files SET inode = ?1 WHERE inode = ?2",
                        params![new_inode, SHARED_WITH_ME_INODE as i64],
                    )?;

                    let _ = tx.execute(
                        "UPDATE sqlite_sequence SET seq = MAX(seq, ?1) WHERE name = 'files'",
                        params![new_inode],
                    );

                    for (parent_ino, child_ino, eff_name) in saved_edges {
                        let new_parent = if parent_ino == SHARED_WITH_ME_INODE as i64 {
                            new_inode
                        } else {
                            parent_ino
                        };
                        let new_child = if child_ino == SHARED_WITH_ME_INODE as i64 {
                            new_inode
                        } else {
                            child_ino
                        };
                        tx.execute(
                            "INSERT INTO parent_child (parent_inode, child_inode, effective_name)
                             VALUES (?1, ?2, ?3)",
                            params![new_parent, new_child, eff_name],
                        )?;
                    }
                }
            }

            tx.execute(
                "INSERT OR IGNORE INTO files (inode, drive_id, name, mime_type, size, modified_time, is_dir, last_synced_at, web_view_link)
                 VALUES (?1, ?2, ?3, ?4, 0, ?5, 1, NULL, NULL)",
                params![
                    SHARED_WITH_ME_INODE as i64,
                    SHARED_WITH_ME_DRIVE_ID,
                    SHARED_WITH_ME_NAME,
                    FOLDER_MIME_TYPE,
                    now
                ],
            )?;

            tx.commit()?;
        }

        Ok(Self {
            conn: Mutex::new(conn),
            ttl_secs,
            evicted_inodes: Mutex::new(HashSet::new()),
            removal_tracker: Mutex::new(RemovalTracker::default()),
            staging_namespace: None,
        })
    }

    fn lock_conn(&self) -> Result<MutexGuard<'_, Connection>, CacheError> {
        self.conn.lock().map_err(|_| CacheError::LockPoisoned)
    }

    fn map_cached_file_row(row: &Row<'_>) -> rusqlite::Result<CachedFile> {
        let inode_i64: i64 = row.get(0)?;
        let drive_id: String = row.get(1)?;
        let name: String = row.get(2)?;
        let mime_type: String = row.get(3)?;
        let size_i64: i64 = row.get(4)?;
        let modified_time: i64 = row.get(5)?;
        let is_dir_int: i64 = row.get(6)?;
        let last_synced_at: Option<i64> = row.get(7)?;
        let web_view_link: Option<String> = row.get(8)?;
        let dirty_int: i64 = row.get(9).unwrap_or(0);

        Ok(CachedFile {
            inode: inode_i64 as u64,
            drive_id,
            name,
            mime_type,
            size: u64::try_from(size_i64).unwrap_or(0),
            modified_time,
            is_dir: is_dir_int != 0,
            last_synced_at,
            web_view_link,
            dirty: dirty_int != 0,
        })
    }

    /// Retrieves cached file metadata by its 64-bit FUSE inode.
    pub fn get_by_inode(&self, inode: u64) -> Result<Option<CachedFile>, CacheError> {
        let conn = self.lock_conn()?;
        Self::get_by_inode_with_conn(&conn, inode)
    }

    fn get_by_inode_with_conn(
        conn: &Connection,
        inode: u64,
    ) -> Result<Option<CachedFile>, CacheError> {
        let mut stmt = conn.prepare_cached(
            "SELECT inode, drive_id, name, mime_type, size, modified_time, is_dir, last_synced_at, web_view_link, dirty
             FROM files
             WHERE inode = ?1",
        )?;

        let file = stmt
            .query_row(params![inode as i64], Self::map_cached_file_row)
            .optional()?;
        Ok(file)
    }

    /// Retrieves cached file metadata by its Google Drive `file_id`.
    pub fn get_by_drive_id(&self, drive_id: &str) -> Result<Option<CachedFile>, CacheError> {
        let conn = self.lock_conn()?;
        Self::get_by_drive_id_with_conn(&conn, drive_id)
    }

    fn get_by_drive_id_with_conn(
        conn: &Connection,
        drive_id: &str,
    ) -> Result<Option<CachedFile>, CacheError> {
        let mut stmt = conn.prepare_cached(
            "SELECT inode, drive_id, name, mime_type, size, modified_time, is_dir, last_synced_at, web_view_link, dirty
             FROM files
             WHERE drive_id = ?1",
        )?;

        let file = stmt
            .query_row(params![drive_id], Self::map_cached_file_row)
            .optional()?;
        Ok(file)
    }

    /// Returns the parent inode for a given `inode`.
    /// Returns `ROOT_INODE` (`1`) if `inode == ROOT_INODE` or if no parent entry is recorded.
    pub fn get_parent_inode(&self, inode: u64) -> Result<u64, CacheError> {
        if inode == ROOT_INODE || inode == SHARED_WITH_ME_INODE {
            return Ok(ROOT_INODE);
        }

        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare_cached(
            "SELECT parent_inode FROM parent_child WHERE child_inode = ?1 ORDER BY rowid DESC LIMIT 1",
        )?;
        let parent: Option<i64> = stmt.query_row(params![inode as i64], |r| r.get(0)).optional()?;
        Ok(parent.map(|p| p as u64).unwrap_or(ROOT_INODE))
    }

    /// Checks whether a directory's cached child listing is missing or older than `ttl_secs`.
    pub fn is_dir_stale(&self, dir_inode: u64) -> Result<bool, CacheError> {
        let conn = self.lock_conn()?;
        let file = Self::get_by_inode_with_conn(&conn, dir_inode)?
            .ok_or_else(|| CacheError::NotFound(format!("inode {}", dir_inode)))?;

        if !file.is_dir {
            return Err(CacheError::NotADirectory(dir_inode));
        }

        match file.last_synced_at {
            None => Ok(true),
            Some(synced_at) => {
                let now = Utc::now().timestamp();
                Ok(now - synced_at >= self.ttl_secs as i64)
            }
        }
    }

    /// Marks a directory's child listing as invalidated (`last_synced_at = NULL`),
    /// forcing a re-sync on the next lookup or readdir.
    pub fn mark_dir_invalidated(&self, dir_inode: u64) -> Result<(), CacheError> {
        let conn = self.lock_conn()?;
        let updated = conn.execute(
            "UPDATE files SET last_synced_at = NULL WHERE inode = ?1",
            params![dir_inode as i64],
        )?;
        if updated == 0 {
            return Err(CacheError::NotFound(format!("inode {}", dir_inode)));
        }
        Ok(())
    }

    /// Looks up a child entry inside `parent_inode` by its deduplicated POSIX `effective_name`.
    pub fn lookup_child(
        &self,
        parent_inode: u64,
        effective_name: &str,
    ) -> Result<Option<CachedFile>, CacheError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare_cached(
            "SELECT f.inode, f.drive_id, f.name, f.mime_type, f.size, f.modified_time, f.is_dir, f.last_synced_at, f.web_view_link, f.dirty
             FROM parent_child pc
             JOIN files f ON f.inode = pc.child_inode
             WHERE pc.parent_inode = ?1 AND pc.effective_name = ?2",
        )?;

        let child = stmt
            .query_row(
                params![parent_inode as i64, effective_name],
                Self::map_cached_file_row,
            )
            .optional()?;
        Ok(child)
    }

    /// Lists all cached children of `parent_inode` ordered deterministically.
    pub fn list_children(&self, parent_inode: u64) -> Result<Vec<DirectoryEntry>, CacheError> {
        let conn = self.lock_conn()?;
        Self::list_children_with_conn(&conn, parent_inode)
    }

    fn list_children_with_conn(
        conn: &Connection,
        parent_inode: u64,
    ) -> Result<Vec<DirectoryEntry>, CacheError> {
        let mut stmt = conn.prepare_cached(
            "SELECT f.inode, f.drive_id, f.name, f.mime_type, f.size, f.modified_time, f.is_dir, f.last_synced_at, f.web_view_link, f.dirty,
                    pc.parent_inode, pc.effective_name
             FROM parent_child pc
             JOIN files f ON f.inode = pc.child_inode
             WHERE pc.parent_inode = ?1
             ORDER BY pc.rowid ASC",
        )?;

        let rows = stmt.query_map(params![parent_inode as i64], |row| {
            let file = Self::map_cached_file_row(row)?;
            let parent_ino_i64: i64 = row.get(10)?;
            let effective_name: String = row.get(11)?;
            Ok(DirectoryEntry {
                inode: file.inode,
                parent_inode: parent_ino_i64 as u64,
                effective_name,
                file,
            })
        })?;

        let mut entries = Vec::new();
        for entry in rows {
            entries.push(entry?);
        }
        Ok(entries)
    }

    /// Inserts or updates a single `DriveFile` in the `files` table while strictly preserving its existing `inode`.
    /// If the file is currently marked `dirty` (actively modified locally), its local `size` and `modified_time` are preserved.
    pub fn upsert_file(&self, file: &DriveFile) -> Result<CachedFile, CacheError> {
        let conn = self.lock_conn()?;
        let cached_file = Self::upsert_file_with_conn(&conn, file)?;
        if let Ok(mut tracker) = self.removal_tracker.lock() {
            tracker.cancel_removal(cached_file.inode);
        }
        Ok(cached_file)
    }

    fn upsert_file_with_conn(
        conn: &Connection,
        file: &DriveFile,
    ) -> Result<CachedFile, CacheError> {
        let is_dir_int = if file.is_dir() { 1i64 } else { 0i64 };
        let size_i64 = i64::try_from(file.size).unwrap_or(i64::MAX);

        conn.execute(
            "INSERT INTO files (drive_id, name, mime_type, size, modified_time, is_dir, web_view_link, dirty)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)
             ON CONFLICT(drive_id) DO UPDATE SET
                 name = CASE WHEN files.dirty != 0 THEN files.name ELSE excluded.name END,
                 mime_type = excluded.mime_type,
                 size = CASE WHEN files.dirty != 0 THEN files.size ELSE excluded.size END,
                 modified_time = CASE WHEN files.dirty != 0 THEN files.modified_time ELSE excluded.modified_time END,
                 is_dir = excluded.is_dir,
                 web_view_link = COALESCE(excluded.web_view_link, files.web_view_link)",
            params![
                file.id,
                file.name,
                file.mime_type,
                size_i64,
                file.modified_time,
                is_dir_int,
                file.web_view_link,
            ],
        )?;

        Self::get_by_drive_id_with_conn(conn, &file.id)?
            .ok_or_else(|| CacheError::NotFound(file.id.clone()))
    }

    /// Atomically replaces and deduplicates the child entries of `parent_inode` in a single SQLite transaction,
    /// preserving existing inodes for known `drive_id`s, retaining any locally dirty/pending children,
    /// and updating `last_synced_at` on `parent_inode`.
    pub fn sync_directory_children(
        &self,
        parent_inode: u64,
        children: &[DriveFile],
    ) -> Result<Vec<DirectoryEntry>, CacheError> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;

        let parent_file = Self::get_by_inode_with_conn(&tx, parent_inode)?
            .ok_or_else(|| CacheError::NotFound(format!("parent inode {}", parent_inode)))?;
        if !parent_file.is_dir {
            return Err(CacheError::NotADirectory(parent_inode));
        }

        // Preserve any locally created/modified dirty children in this directory (and any child folders
        // that contain dirty/pending descendants in their subtree) that haven't been flushed yet.
        let mut preserved_inodes_in_dir = HashSet::new();
        {
            let mut stmt = tx.prepare(
                "WITH RECURSIVE dirty_tree(inode) AS (
                    SELECT inode FROM files WHERE dirty != 0 OR drive_id LIKE 'pending:%'
                    UNION
                    SELECT pc.parent_inode
                    FROM parent_child pc
                    INNER JOIN dirty_tree dt ON pc.child_inode = dt.inode
                )
                SELECT inode FROM dirty_tree",
            )?;
            let rows = stmt.query_map([], |r| {
                let ino: i64 = r.get(0)?;
                Ok(ino as u64)
            })?;
            for row in rows {
                preserved_inodes_in_dir.insert(row?);
            }
        }
        let existing_dirty: Vec<DirectoryEntry> = Self::list_children_with_conn(&tx, parent_inode)?
            .into_iter()
            .filter(|e| {
                e.file.dirty
                    || e.file.is_pending_upload()
                    || preserved_inodes_in_dir.contains(&e.inode)
            })
            .collect();

        // Clear previous parent_child edges for this directory so deleted/moved remote files are removed.
        tx.execute(
            "DELETE FROM parent_child WHERE parent_inode = ?1",
            params![parent_inode as i64],
        )?;

        // Assign deduplicated names in a stable order (name, then id) so that a changed
        // response order cannot swap the names of duplicate siblings.
        let mut naming_order: Vec<usize> = (0..children.len()).collect();
        naming_order.sort_by(|&a, &b| {
            (children[a].name.as_str(), children[a].id.as_str())
                .cmp(&(children[b].name.as_str(), children[b].id.as_str()))
        });
        let mut used_names = HashSet::with_capacity(children.len() + existing_dirty.len());
        let mut names = vec![String::new(); children.len()];
        for idx in naming_order {
            names[idx] = disambiguate_filename(&children[idx].name, &mut used_names);
        }
        let mut entries = Vec::with_capacity(children.len() + existing_dirty.len());
        let mut inserted_inodes = HashSet::with_capacity(children.len());

        for (idx, drive_file) in children.iter().enumerate() {
            let cached_file = Self::upsert_file_with_conn(&tx, drive_file)?;
            let effective_name = names[idx].clone();

            tx.execute(
                "INSERT INTO parent_child (parent_inode, child_inode, effective_name)
                 VALUES (?1, ?2, ?3)",
                params![
                    parent_inode as i64,
                    cached_file.inode as i64,
                    &effective_name
                ],
            )?;

            inserted_inodes.insert(cached_file.inode);
            entries.push(DirectoryEntry {
                inode: cached_file.inode,
                parent_inode,
                effective_name,
                file: cached_file,
            });
        }

        for dirty_entry in existing_dirty {
            if !inserted_inodes.contains(&dirty_entry.inode) {
                let effective_name =
                    disambiguate_filename(&dirty_entry.effective_name, &mut used_names);
                tx.execute(
                    "INSERT INTO parent_child (parent_inode, child_inode, effective_name)
                     VALUES (?1, ?2, ?3)",
                    params![
                        parent_inode as i64,
                        dirty_entry.inode as i64,
                        &effective_name
                    ],
                )?;
                entries.push(DirectoryEntry {
                    inode: dirty_entry.inode,
                    parent_inode,
                    effective_name,
                    file: dirty_entry.file,
                });
            }
        }

        let now = Utc::now().timestamp();
        tx.execute(
            "UPDATE files SET last_synced_at = ?1 WHERE inode = ?2",
            params![now, parent_inode as i64],
        )?;

        tx.commit()?;

        if let Ok(mut tracker) = self.removal_tracker.lock() {
            for inode in inserted_inodes {
                tracker.cancel_removal(inode);
            }
        }

        Ok(entries)
    }

    fn used_sibling_effective_names_with_conn(
        conn: &Connection,
        parent_inode: u64,
        exclude_child_inode: Option<u64>,
    ) -> Result<HashSet<String>, CacheError> {
        let mut stmt = conn.prepare_cached(
            "SELECT child_inode, effective_name FROM parent_child WHERE parent_inode = ?1",
        )?;
        let rows = stmt.query_map(params![parent_inode as i64], |r| {
            let ino: i64 = r.get(0)?;
            let name: String = r.get(1)?;
            Ok((ino as u64, name))
        })?;
        let mut used = HashSet::new();
        for row in rows {
            let (child_ino, name) = row?;
            if exclude_child_inode != Some(child_ino) {
                used.insert(name);
            }
        }
        Ok(used)
    }

    /// Allocates a new local dirty file inode under `parent_inode` with a temporary `pending:<inode>` Drive ID
    /// for FUSE `create`.
    pub fn create_pending_file(
        &self,
        parent_inode: u64,
        name: &str,
        mime_type: &str,
        modified_time: i64,
    ) -> Result<DirectoryEntry, CacheError> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;

        let parent_file = Self::get_by_inode_with_conn(&tx, parent_inode)?
            .ok_or_else(|| CacheError::NotFound(format!("parent inode {}", parent_inode)))?;
        if !parent_file.is_dir {
            return Err(CacheError::NotADirectory(parent_inode));
        }

        let mut used_names = Self::used_sibling_effective_names_with_conn(&tx, parent_inode, None)?;
        let effective_name = disambiguate_filename(name, &mut used_names);

        // Allocate the inode through SQLite's `AUTOINCREMENT` sequence (`last_insert_rowid()`)
        // rather than `MAX(inode) + 1` so unlinked highest inodes are never immediately reused.
        tx.execute(
            "INSERT INTO files (drive_id, name, mime_type, size, modified_time, is_dir, last_synced_at, web_view_link, dirty)
             VALUES ('pending:alloc', ?1, ?2, 0, ?3, 0, NULL, NULL, 1)",
            params![name, mime_type, modified_time],
        )?;
        let next_ino = tx.last_insert_rowid();
        let temp_drive_id = format!("{}{}", PENDING_DRIVE_ID_PREFIX, next_ino);
        tx.execute(
            "UPDATE files SET drive_id = ?1 WHERE inode = ?2",
            params![&temp_drive_id, next_ino],
        )?;

        tx.execute(
            "INSERT INTO parent_child (parent_inode, child_inode, effective_name)
             VALUES (?1, ?2, ?3)",
            params![parent_inode as i64, next_ino, &effective_name],
        )?;

        let cached_file = Self::get_by_inode_with_conn(&tx, next_ino as u64)?
            .ok_or_else(|| CacheError::NotFound(format!("inode {}", next_ino)))?;

        tx.commit()?;
        Ok(DirectoryEntry {
            inode: cached_file.inode,
            parent_inode,
            effective_name,
            file: cached_file,
        })
    }

    /// Updates cached `size`, `modified_time`, and `dirty` flag for `inode` after a local `write` or `setattr`.
    pub fn update_local_write(
        &self,
        inode: u64,
        new_size: u64,
        modified_time: i64,
        mark_dirty: bool,
    ) -> Result<CachedFile, CacheError> {
        let conn = self.lock_conn()?;
        let size_i64 = i64::try_from(new_size).unwrap_or(i64::MAX);
        let updated = if mark_dirty {
            conn.execute(
                "UPDATE files SET size = ?1, modified_time = ?2, dirty = 1 WHERE inode = ?3",
                params![size_i64, modified_time, inode as i64],
            )?
        } else {
            conn.execute(
                "UPDATE files SET size = ?1, modified_time = ?2 WHERE inode = ?3",
                params![size_i64, modified_time, inode as i64],
            )?
        };

        if updated == 0 {
            return Err(CacheError::NotFound(format!("inode {}", inode)));
        }

        Self::get_by_inode_with_conn(&conn, inode)?
            .ok_or_else(|| CacheError::NotFound(format!("inode {}", inode)))
    }

    /// Updates an inode's Drive metadata (`drive_id`, `name`, `mime_type`, `size`, `modified_time`, `web_view_link`)
    /// and clears its `dirty` flag once a Resumable Upload completes.
    pub fn complete_file_upload(
        &self,
        inode: u64,
        uploaded: &DriveFile,
    ) -> Result<CachedFile, CacheError> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;

        // If background change sync inserted a separate row for `uploaded.id` before this upload
        // callback completed, remove that duplicate row so `inode` remains strictly stable.
        if let Some(other) = Self::get_by_drive_id_with_conn(&tx, &uploaded.id)? {
            if other.inode != inode && other.inode > SHARED_WITH_ME_INODE {
                let has_own_parent_edge: bool = tx
                    .query_row(
                        "SELECT 1 FROM parent_child WHERE child_inode = ?1 LIMIT 1",
                        params![inode as i64],
                        |_| Ok(true),
                    )
                    .optional()?
                    .unwrap_or(false);

                tx.execute(
                    "DELETE FROM parent_child WHERE child_inode = ?1",
                    params![other.inode as i64],
                )?;
                tx.execute(
                    "DELETE FROM files WHERE inode = ?1",
                    params![other.inode as i64],
                )?;

                if has_own_parent_edge {
                    // Re-evaluate `inode`'s effective_name in case it had been disambiguated against `other.inode`
                    let parent_opt: Option<i64> = tx
                        .query_row(
                            "SELECT parent_inode FROM parent_child WHERE child_inode = ?1 LIMIT 1",
                            params![inode as i64],
                            |r| r.get(0),
                        )
                        .optional()?;
                    if let Some(p_ino) = parent_opt {
                        let mut used = Self::used_sibling_effective_names_with_conn(
                            &tx,
                            p_ino as u64,
                            Some(inode),
                        )?;
                        let clean_name = disambiguate_filename(&uploaded.name, &mut used);
                        tx.execute(
                            "UPDATE parent_child SET effective_name = ?1 WHERE parent_inode = ?2 AND child_inode = ?3",
                            params![&clean_name, p_ino, inode as i64],
                        )?;
                    }
                }
            }
        }

        let size_i64 = i64::try_from(uploaded.size).unwrap_or(i64::MAX);
        let updated = tx.execute(
            "UPDATE files
             SET drive_id = ?1,
                 name = ?2,
                 mime_type = ?3,
                 size = ?4,
                 modified_time = ?5,
                 web_view_link = COALESCE(?6, web_view_link),
                 dirty = 0
             WHERE inode = ?7",
            params![
                uploaded.id,
                uploaded.name,
                uploaded.mime_type,
                size_i64,
                uploaded.modified_time,
                uploaded.web_view_link,
                inode as i64,
            ],
        )?;

        if updated == 0 {
            return Err(CacheError::NotFound(format!("inode {}", inode)));
        }

        let cached = Self::get_by_inode_with_conn(&tx, inode)?
            .ok_or_else(|| CacheError::NotFound(format!("inode {}", inode)))?;
        tx.commit()?;
        Ok(cached)
    }

    /// Inserts a newly created remote child (`mkdir` or uploaded file) under `parent_inode`.
    pub fn insert_child_file(
        &self,
        parent_inode: u64,
        effective_name: &str,
        file: &DriveFile,
    ) -> Result<DirectoryEntry, CacheError> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;

        let parent_file = Self::get_by_inode_with_conn(&tx, parent_inode)?
            .ok_or_else(|| CacheError::NotFound(format!("parent inode {}", parent_inode)))?;
        if !parent_file.is_dir {
            return Err(CacheError::NotADirectory(parent_inode));
        }

        let cached_file = Self::upsert_file_with_conn(&tx, file)?;
        tx.execute(
            "DELETE FROM parent_child WHERE parent_inode = ?1 AND child_inode = ?2",
            params![parent_inode as i64, cached_file.inode as i64],
        )?;
        let mut used_names =
            Self::used_sibling_effective_names_with_conn(&tx, parent_inode, Some(cached_file.inode))?;
        let disambiguated = disambiguate_filename(effective_name, &mut used_names);

        tx.execute(
            "INSERT INTO parent_child (parent_inode, child_inode, effective_name)
             VALUES (?1, ?2, ?3)",
            params![
                parent_inode as i64,
                cached_file.inode as i64,
                &disambiguated
            ],
        )?;

        tx.commit()?;
        Ok(DirectoryEntry {
            inode: cached_file.inode,
            parent_inode,
            effective_name: disambiguated,
            file: cached_file,
        })
    }

    /// Returns all cached files that are currently marked `dirty` or have a `pending:` Drive ID.
    pub fn list_dirty_or_pending_files(&self) -> Result<Vec<CachedFile>, CacheError> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare_cached(
            "SELECT inode, drive_id, name, mime_type, size, modified_time, is_dir, last_synced_at, web_view_link, dirty
             FROM files
             WHERE dirty != 0 OR drive_id LIKE 'pending:%'
             ORDER BY inode ASC",
        )?;
        let rows = stmt.query_map([], Self::map_cached_file_row)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Removes only the directory entry edge `(parent_inode, child_inode)` from `parent_child`
    /// while retaining the `files` record so open file handles can continue reading/writing until final release.
    pub fn unlink_child_entry(
        &self,
        parent_inode: u64,
        child_inode: u64,
    ) -> Result<(), CacheError> {
        let conn = self.lock_conn()?;
        conn.execute(
            "DELETE FROM parent_child WHERE parent_inode = ?1 AND child_inode = ?2",
            params![parent_inode as i64, child_inode as i64],
        )?;
        Ok(())
    }

    /// Removes a file or folder and its `parent_child` edges from the SQLite cache (`unlink` / `rmdir`).
    pub fn remove_file(&self, inode: u64) -> Result<(), CacheError> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;

        tx.execute(
            "DELETE FROM parent_child WHERE child_inode = ?1 OR parent_inode = ?1",
            params![inode as i64],
        )?;

        if inode > SHARED_WITH_ME_INODE {
            tx.execute(
                "DELETE FROM files WHERE inode = ?1",
                params![inode as i64],
            )?;
        }

        tx.commit()?;
        if let Ok(mut guard) = self.evicted_inodes.lock() {
            guard.insert(inode);
        }
        Ok(())
    }

    /// Atomically renames and/or moves `child_inode` from `old_parent_inode` to `(new_parent_inode, new_effective_name)`,
    /// updating `files` metadata.
    pub fn rename_entry(
        &self,
        old_parent_inode: u64,
        child_inode: u64,
        new_parent_inode: u64,
        new_effective_name: &str,
        updated_file: Option<&DriveFile>,
    ) -> Result<CachedFile, CacheError> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;

        let sanitized_new_name = sanitize_posix_filename(new_effective_name);

        tx.execute(
            "DELETE FROM parent_child WHERE parent_inode = ?1 AND child_inode = ?2",
            params![old_parent_inode as i64, child_inode as i64],
        )?;
        tx.execute(
            "DELETE FROM parent_child WHERE parent_inode = ?1 AND child_inode = ?2",
            params![new_parent_inode as i64, child_inode as i64],
        )?;

        let mut used_names =
            Self::used_sibling_effective_names_with_conn(&tx, new_parent_inode, Some(child_inode))?;
        let final_effective_name = disambiguate_filename(&sanitized_new_name, &mut used_names);

        tx.execute(
            "INSERT INTO parent_child (parent_inode, child_inode, effective_name)
             VALUES (?1, ?2, ?3)",
            params![
                new_parent_inode as i64,
                child_inode as i64,
                &final_effective_name
            ],
        )?;

        if let Some(drive_file) = updated_file {
            let size_i64 = i64::try_from(drive_file.size).unwrap_or(i64::MAX);
            tx.execute(
                "UPDATE files
                 SET name = ?1,
                     mime_type = ?2,
                     size = CASE WHEN dirty != 0 THEN size ELSE ?3 END,
                     modified_time = CASE WHEN dirty != 0 THEN modified_time ELSE ?4 END,
                     web_view_link = COALESCE(?5, web_view_link)
                 WHERE inode = ?6",
                params![
                    drive_file.name,
                    drive_file.mime_type,
                    size_i64,
                    drive_file.modified_time,
                    drive_file.web_view_link,
                    child_inode as i64,
                ],
            )?;
        } else {
            let guessed_mime = crate::api::guess_mime_type(&sanitized_new_name);
            tx.execute(
                "UPDATE files SET name = ?1, mime_type = CASE WHEN drive_id LIKE 'pending:%' THEN ?2 ELSE mime_type END WHERE inode = ?3",
                params![&sanitized_new_name, guessed_mime, child_inode as i64],
            )?;
        }

        let updated = Self::get_by_inode_with_conn(&tx, child_inode)?
            .ok_or_else(|| CacheError::NotFound(format!("inode {}", child_inode)))?;
        tx.commit()?;
        Ok(updated)
    }

    /// Retrieves the saved Google Drive `changes.list` page token from `sync_state`, if any.
    pub fn get_change_page_token(&self) -> Result<Option<String>, CacheError> {
        let conn = self.lock_conn()?;
        let mut stmt =
            conn.prepare_cached("SELECT value FROM sync_state WHERE key = ?1")?;
        let token: Option<String> = stmt
            .query_row(params![CHANGES_PAGE_TOKEN_KEY], |r| r.get(0))
            .optional()?;
        Ok(token)
    }

    /// Saves the Google Drive `changes.list` page token into `sync_state`.
    pub fn set_change_page_token(&self, token: &str) -> Result<(), CacheError> {
        let conn = self.lock_conn()?;
        conn.execute(
            "INSERT INTO sync_state (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![CHANGES_PAGE_TOKEN_KEY, token],
        )?;
        Ok(())
    }

    /// Atomically applies a batch of incremental Drive changes (`changes.list`) and optionally updates
    /// the stored change page token inside a single SQLite transaction.
    ///
    /// Returns the number of changes that affected local cached state.
    pub fn apply_changes(
        &self,
        changes: &[ChangeItem],
        new_page_token: Option<&str>,
    ) -> Result<usize, CacheError> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
        let mut affected_count = 0usize;
        let mut newly_evicted = Vec::new();
        let mut removal_events = Vec::new();

        for change in changes {
            let is_removed_or_trashed =
                change.removed || change.file.as_ref().is_some_and(|f| f.trashed);

            if is_removed_or_trashed {
                if let Some(existing) = Self::get_by_drive_id_with_conn(&tx, &change.file_id)? {
                    let contains_local_writes: bool = tx.query_row(
                        "WITH RECURSIVE descendants(inode) AS (
                            SELECT ?1
                            UNION
                            SELECT pc.child_inode
                            FROM parent_child pc
                            INNER JOIN descendants d ON pc.parent_inode = d.inode
                        )
                        SELECT EXISTS (
                            SELECT 1 FROM files f
                            INNER JOIN descendants d ON f.inode = d.inode
                            WHERE f.dirty != 0 OR f.drive_id LIKE 'pending:%'
                        )",
                        params![existing.inode as i64],
                        |r| r.get(0),
                    )?;
                    if !contains_local_writes {
                        let child_inode_i64 = existing.inode as i64;
                        tx.execute(
                            "UPDATE files SET last_synced_at = NULL
                             WHERE inode IN (SELECT parent_inode FROM parent_child WHERE child_inode = ?1)",
                            params![child_inode_i64],
                        )?;
                        tx.execute(
                            "DELETE FROM parent_child WHERE child_inode = ?1",
                            params![child_inode_i64],
                        )?;
                        if existing.inode > SHARED_WITH_ME_INODE {
                            removal_events.push((existing.inode, true));
                        }
                        newly_evicted.push(existing.inode);
                        affected_count += 1;
                    }
                }
                // Also invalidate the virtual "Shared with me" directory in case a shared item was removed
                tx.execute(
                    "UPDATE files SET last_synced_at = NULL WHERE inode = ?1",
                    params![SHARED_WITH_ME_INODE as i64],
                )?;
            } else if let Some(ref drive_file) = change.file {
                let upserted = Self::upsert_file_with_conn(&tx, drive_file)?;
                removal_events.push((upserted.inode, false));
                let child_inode_i64 = upserted.inode as i64;
                if !upserted.dirty {
                    newly_evicted.push(upserted.inode);
                }

                // Invalidate any previously recorded parent directories for this inode
                tx.execute(
                    "UPDATE files SET last_synced_at = NULL
                     WHERE inode IN (SELECT parent_inode FROM parent_child WHERE child_inode = ?1)",
                    params![child_inode_i64],
                )?;

                // Invalidate any new/current parent directories reported by Drive
                let mut has_known_local_parent = false;
                for parent_drive_id in &drive_file.parents {
                    let updated = tx.execute(
                        "UPDATE files SET last_synced_at = NULL WHERE drive_id = ?1",
                        params![parent_drive_id],
                    )?;
                    if updated > 0 {
                        has_known_local_parent = true;
                    }
                    if parent_drive_id == ROOT_DRIVE_ID {
                        tx.execute(
                            "UPDATE files SET last_synced_at = NULL WHERE inode = ?1",
                            params![ROOT_INODE as i64],
                        )?;
                        has_known_local_parent = true;
                    }
                }

                // If the changed file has no listed parents or its parent is outside our cached
                // hierarchy (e.g., a "Shared with me" item owned by another user), invalidate
                // SHARED_WITH_ME_INODE so its directory listing refreshes on next access.
                if drive_file.parents.is_empty() || !has_known_local_parent {
                    tx.execute(
                        "UPDATE files SET last_synced_at = NULL WHERE inode IN (?1, ?2)",
                        params![SHARED_WITH_ME_INODE as i64, ROOT_INODE as i64],
                    )?;
                }

                affected_count += 1;
            }
        }

        if let Some(token) = new_page_token {
            tx.execute(
                "INSERT INTO sync_state (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![CHANGES_PAGE_TOKEN_KEY, token],
            )?;
        }

        tx.commit()?;

        if !newly_evicted.is_empty() {
            if let Ok(mut guard) = self.evicted_inodes.lock() {
                guard.extend(newly_evicted);
            }
        }
        if let Ok(mut tracker) = self.removal_tracker.lock() {
            for (inode, removed) in removal_events {
                if removed {
                    tracker.queue_removal(inode);
                } else {
                    tracker.cancel_removal(inode);
                }
            }
        }

        Ok(affected_count)
    }

    /// Drains and returns the set of inodes whose content/metadata was updated or removed
    /// by incremental sync so the FUSE layer can evict any stale read-ahead / exported buffers.
    pub fn take_evicted_inodes(&self) -> Vec<u64> {
        match self.evicted_inodes.lock() {
            Ok(mut guard) => guard.drain().collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Drains the inodes removed remotely so the FUSE layer can defer deleting open file records.
    pub fn take_removed_inodes(&self) -> Vec<RemovedInode> {
        match self.removal_tracker.lock() {
            Ok(mut tracker) => tracker
                .pending
                .drain()
                .map(|(inode, generation)| RemovedInode { inode, generation })
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    pub fn is_current_removal(&self, inode: u64, generation: u64) -> bool {
        self.removal_tracker
            .lock()
            .map(|tracker| tracker.is_current(inode, generation))
            .unwrap_or(false)
    }

    pub fn remove_file_if_current_removal(
        &self,
        inode: u64,
        generation: u64,
    ) -> Result<bool, CacheError> {
        let mut conn = self.lock_conn()?;
        let tracker = self
            .removal_tracker
            .lock()
            .map_err(|_| CacheError::LockPoisoned)?;
        if !tracker.is_current(inode, generation) {
            return Ok(false);
        }

        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM parent_child WHERE child_inode = ?1 OR parent_inode = ?1",
            params![inode as i64],
        )?;

        if inode > SHARED_WITH_ME_INODE {
            tx.execute("DELETE FROM files WHERE inode = ?1", params![inode as i64])?;
        }

        tx.commit()?;
        drop(tracker);
        if let Ok(mut guard) = self.evicted_inodes.lock() {
            guard.insert(inode);
        }
        Ok(true)
    }

    /// Clears all clean cached files, directory edges, and sync state while preserving reserved root (`1`),
    /// "Shared with me" (`2`), and any locally dirty/pending inodes (along with their ancestor directory chain
    /// and parent-child edges so nested dirty files remain reachable).
    pub fn clear_all(&self) -> Result<(), CacheError> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;

        const PRESERVED_CTE: &str = "
            WITH RECURSIVE preserved(inode) AS (
                SELECT inode FROM files
                WHERE inode <= ?1 OR dirty != 0 OR drive_id LIKE 'pending:%'
                UNION
                SELECT pc.parent_inode
                FROM parent_child pc
                INNER JOIN preserved p ON pc.child_inode = p.inode
            )
        ";

        let mut evicted_inodes = Vec::new();
        {
            let sql = format!(
                "{} SELECT inode FROM files WHERE inode NOT IN (SELECT inode FROM preserved)",
                PRESERVED_CTE
            );
            let mut stmt = tx.prepare(&sql)?;
            let rows = stmt.query_map(params![SHARED_WITH_ME_INODE as i64], |r| {
                let ino: i64 = r.get(0)?;
                Ok(ino as u64)
            })?;
            for row in rows {
                evicted_inodes.push(row?);
            }
        }

        let delete_edges_sql = format!(
            "{} DELETE FROM parent_child WHERE child_inode NOT IN (SELECT inode FROM preserved) OR parent_inode NOT IN (SELECT inode FROM preserved)",
            PRESERVED_CTE
        );
        tx.execute(&delete_edges_sql, params![SHARED_WITH_ME_INODE as i64])?;

        tx.execute("DELETE FROM sync_state", [])?;

        let delete_files_sql = format!(
            "{} DELETE FROM files WHERE inode NOT IN (SELECT inode FROM preserved)",
            PRESERVED_CTE
        );
        tx.execute(&delete_files_sql, params![SHARED_WITH_ME_INODE as i64])?;

        tx.execute("UPDATE files SET last_synced_at = NULL WHERE is_dir != 0", [])?;

        tx.commit()?;

        if !evicted_inodes.is_empty() {
            if let Ok(mut guard) = self.evicted_inodes.lock() {
                guard.extend(evicted_inodes);
            }
        }

        Ok(())
    }
}

/// Sanitizes a Google Drive filename for POSIX compatibility and deterministically disambiguates
/// collisions among siblings in the same directory (e.g., `report.pdf` -> `report (1).pdf`).
pub(crate) fn disambiguate_filename(raw_name: &str, used_names: &mut HashSet<String>) -> String {
    let base_name = sanitize_posix_filename(raw_name);
    if used_names.insert(base_name.clone()) {
        return base_name;
    }

    let (stem, ext) = split_stem_and_extension(&base_name);
    let mut counter: u64 = 1;
    loop {
        let candidate = format!("{} ({}){}", stem, counter, ext);
        if used_names.insert(candidate.clone()) {
            return candidate;
        }
        counter += 1;
    }
}

/// Replaces characters illegal in POSIX path components (`/` and `\0`) with `_`,
/// and prevents empty, `.` or `..` filenames.
fn sanitize_posix_filename(raw_name: &str) -> String {
    let sanitized: String = raw_name
        .chars()
        .map(|ch| if ch == '/' || ch == '\0' { '_' } else { ch })
        .collect();

    match sanitized.as_str() {
        "" => "_".to_string(),
        "." => "_.".to_string(),
        ".." => "_..".to_string(),
        _ => sanitized,
    }
}

/// Splits a filename into `(stem, extension_with_dot)`.
/// Preserves leading dots on hidden files (e.g. `.gitignore` -> `(".gitignore", "")`).
fn split_stem_and_extension(filename: &str) -> (&str, &str) {
    if let Some(dot_idx) = filename.rfind('.') {
        if dot_idx > 0 && dot_idx < filename.len() - 1 {
            return (&filename[..dot_idx], &filename[dot_idx..]);
        }
    }
    (filename, "")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_drive_file(id: &str, name: &str, mime_type: &str, size: u64) -> DriveFile {
        DriveFile {
            id: id.to_string(),
            name: name.to_string(),
            mime_type: mime_type.to_string(),
            size,
            modified_time: 1_700_000_000,
            parents: vec![ROOT_DRIVE_ID.to_string()],
            trashed: false,
            web_view_link: None,
        }
    }

    #[test]
    fn test_root_and_shared_with_me_inode_initialization() {
        let cache = Cache::open_in_memory(60).expect("Failed to initialize in-memory cache");

        let root_by_ino = cache
            .get_by_inode(ROOT_INODE)
            .expect("Query by inode failed")
            .expect("Root inode must exist");
        assert_eq!(root_by_ino.inode, 1);
        assert_eq!(root_by_ino.drive_id, ROOT_DRIVE_ID);
        assert!(root_by_ino.is_dir);
        assert_eq!(root_by_ino.mime_type, FOLDER_MIME_TYPE);
        assert!(root_by_ino.last_synced_at.is_none());

        let root_by_drive_id = cache
            .get_by_drive_id(ROOT_DRIVE_ID)
            .expect("Query by drive_id failed")
            .expect("Root drive_id must exist");
        assert_eq!(root_by_drive_id.inode, ROOT_INODE);

        let shared_by_ino = cache
            .get_by_inode(SHARED_WITH_ME_INODE)
            .expect("Query shared_with_me by inode failed")
            .expect("Shared with me inode must exist");
        assert_eq!(shared_by_ino.inode, 2);
        assert_eq!(shared_by_ino.drive_id, SHARED_WITH_ME_DRIVE_ID);
        assert_eq!(shared_by_ino.name, SHARED_WITH_ME_NAME);
        assert!(shared_by_ino.is_dir);

        assert_eq!(
            cache
                .get_parent_inode(ROOT_INODE)
                .expect("get_parent_inode failed"),
            ROOT_INODE
        );
        assert_eq!(
            cache
                .get_parent_inode(SHARED_WITH_ME_INODE)
                .expect("get_parent_inode for shared_with_me failed"),
            ROOT_INODE
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_staging_namespace_resolves_symlink_before_parent_component() {
        use std::os::unix::fs::symlink;

        let test_dir = std::env::temp_dir().join(format!(
            "gdrive-fuse-rs-symlink-path-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let target_child = test_dir.join("target/child");
        fs::create_dir_all(&target_child).unwrap();
        symlink("target/child", test_dir.join("link")).unwrap();

        let db_path = test_dir.join("link/../metadata.db");
        let stage_before_open = MetadataCache::default_staging_dir_for_db(&db_path);
        let cache = MetadataCache::open(&db_path, 60).unwrap();
        let stage_after_open = MetadataCache::default_staging_dir_for_db(&db_path);

        assert_eq!(
            stage_before_open, stage_after_open,
            "A symlink followed by `..` must resolve to the same staging namespace before and after database creation"
        );
        assert!(test_dir.join("target/metadata.db").exists());
        drop(cache);
        fs::remove_dir_all(test_dir).unwrap();
    }

    #[test]
    fn test_inode_stability_across_upserts_and_resyncs() {
        let cache = Cache::open_in_memory(60).expect("Failed to open cache");

        let file_v1 = sample_drive_file("drive_file_1", "notes.txt", "text/plain", 128);
        let file_v2 = sample_drive_file("drive_file_2", "image.png", "image/png", 4096);

        let initial_entries = cache
            .sync_directory_children(ROOT_INODE, &[file_v1.clone(), file_v2.clone()])
            .expect("Initial sync failed");
        assert_eq!(initial_entries.len(), 2);

        let ino_1 = initial_entries[0].inode;
        let ino_2 = initial_entries[1].inode;
        assert!(ino_1 > SHARED_WITH_ME_INODE);
        assert!(ino_2 > ino_1);

        // Update file_v1 name and size, and re-sync directory
        let mut file_v1_updated = file_v1.clone();
        file_v1_updated.name = "notes_renamed.txt".to_string();
        file_v1_updated.size = 512;
        file_v1_updated.modified_time = 1_700_000_999;

        let upserted = cache
            .upsert_file(&file_v1_updated)
            .expect("upsert_file failed");
        assert_eq!(upserted.inode, ino_1);
        assert_eq!(upserted.name, "notes_renamed.txt");
        assert_eq!(upserted.size, 512);

        let resynced_entries = cache
            .sync_directory_children(ROOT_INODE, &[file_v1_updated, file_v2])
            .expect("Re-sync failed");
        assert_eq!(resynced_entries[0].inode, ino_1);
        assert_eq!(resynced_entries[0].effective_name, "notes_renamed.txt");
        assert_eq!(resynced_entries[1].inode, ino_2);
        assert_eq!(resynced_entries[1].effective_name, "image.png");
    }

    #[test]
    fn test_duplicate_sibling_filename_disambiguation() {
        let cache = Cache::open_in_memory(60).expect("Failed to open cache");

        let dup_pdf_1 = sample_drive_file("id_pdf_1", "report.pdf", "application/pdf", 100);
        let dup_pdf_2 = sample_drive_file("id_pdf_2", "report.pdf", "application/pdf", 200);
        let dup_pdf_3 = sample_drive_file("id_pdf_3", "report.pdf", "application/pdf", 300);
        let dup_no_ext_1 = sample_drive_file(
            "id_doc_1",
            "Notes",
            "application/vnd.google-apps.document",
            0,
        );
        let dup_no_ext_2 = sample_drive_file(
            "id_doc_2",
            "Notes",
            "application/vnd.google-apps.document",
            0,
        );
        let slash_name = sample_drive_file("id_slash", "2025/03/budget.csv", "text/csv", 50);

        let entries = cache
            .sync_directory_children(
                ROOT_INODE,
                &[
                    dup_pdf_1,
                    dup_pdf_2,
                    dup_pdf_3,
                    dup_no_ext_1,
                    dup_no_ext_2,
                    slash_name,
                ],
            )
            .expect("Sync with duplicates failed");

        let effective_names: Vec<&str> =
            entries.iter().map(|e| e.effective_name.as_str()).collect();
        assert_eq!(
            effective_names,
            vec![
                "report.pdf",
                "report (1).pdf",
                "report (2).pdf",
                "Notes",
                "Notes (1)",
                "2025_03_budget.csv",
            ]
        );

        // Verify lookup_child finds each deduplicated sibling with its distinct inode & drive_id
        let first_pdf = cache
            .lookup_child(ROOT_INODE, "report.pdf")
            .expect("lookup failed")
            .expect("report.pdf missing");
        let second_pdf = cache
            .lookup_child(ROOT_INODE, "report (1).pdf")
            .expect("lookup failed")
            .expect("report (1).pdf missing");
        let third_pdf = cache
            .lookup_child(ROOT_INODE, "report (2).pdf")
            .expect("lookup failed")
            .expect("report (2).pdf missing");

        assert_eq!(first_pdf.drive_id, "id_pdf_1");
        assert_eq!(second_pdf.drive_id, "id_pdf_2");
        assert_eq!(third_pdf.drive_id, "id_pdf_3");
        assert_ne!(first_pdf.inode, second_pdf.inode);
        assert_ne!(second_pdf.inode, third_pdf.inode);

        let second_notes = cache
            .lookup_child(ROOT_INODE, "Notes (1)")
            .expect("lookup failed")
            .expect("Notes (1) missing");
        assert_eq!(second_notes.drive_id, "id_doc_2");
        assert!(second_notes.is_google_native_doc());
        assert_eq!(
            second_notes.web_url(),
            "https://docs.google.com/document/d/id_doc_2/edit"
        );
    }

    #[test]
    fn test_rename_does_not_infer_overwrite_from_effective_name() {
        let cache = Cache::open_in_memory(60).expect("Failed to open cache");

        let native_doc = sample_drive_file(
            "native_plan",
            "Plan",
            "application/vnd.google-apps.document",
            0,
        );
        let regular_file = sample_drive_file("regular_plan", "Plan", "text/plain", 10);
        let renamed_file = sample_drive_file("renamed", "Other", "text/plain", 20);
        let entries = cache
            .sync_directory_children(
                ROOT_INODE,
                &[native_doc, regular_file, renamed_file.clone()],
            )
            .expect("Directory sync failed");
        let regular_inode = entries
            .iter()
            .find(|entry| entry.file.drive_id == "regular_plan")
            .expect("Regular Plan file missing")
            .inode;
        let renamed_inode = entries
            .iter()
            .find(|entry| entry.file.drive_id == "renamed")
            .expect("Rename source missing")
            .inode;

        let mut updated_file = renamed_file;
        updated_file.name = "Plan (1)".to_string();
        cache
            .rename_entry(
                ROOT_INODE,
                renamed_inode,
                ROOT_INODE,
                "Plan (1)",
                Some(&updated_file),
            )
            .expect("Rename failed");

        assert_eq!(
            cache
                .get_by_inode(regular_inode)
                .expect("Regular Plan lookup failed")
                .expect("Regular Plan should remain cached")
                .drive_id,
            "regular_plan"
        );
    }

    #[test]
    fn test_deleted_child_cleanup_on_resync_and_parent_lookup() {
        let cache = Cache::open_in_memory(60).expect("Failed to open cache");

        let folder = sample_drive_file("folder_a", "Work", FOLDER_MIME_TYPE, 0);
        let root_entries = cache
            .sync_directory_children(ROOT_INODE, &[folder])
            .expect("Sync root failed");
        let folder_ino = root_entries[0].inode;

        assert_eq!(
            cache.get_parent_inode(folder_ino).unwrap(),
            ROOT_INODE,
            "Folder parent should be ROOT_INODE"
        );

        let child_a = sample_drive_file("child_a", "keep.txt", "text/plain", 10);
        let child_b = sample_drive_file("child_b", "remove_me.txt", "text/plain", 20);

        let initial_children = cache
            .sync_directory_children(folder_ino, &[child_a.clone(), child_b])
            .expect("Sync folder children failed");
        assert_eq!(initial_children.len(), 2);
        let child_a_ino = initial_children[0].inode;
        assert_eq!(cache.get_parent_inode(child_a_ino).unwrap(), folder_ino);

        // Re-sync folder with only child_a
        let updated_children = cache
            .sync_directory_children(folder_ino, &[child_a])
            .expect("Re-sync folder children failed");
        assert_eq!(updated_children.len(), 1);
        assert_eq!(updated_children[0].effective_name, "keep.txt");
        assert_eq!(updated_children[0].inode, child_a_ino);

        // Verify removed child is no longer returned by lookup_child or list_children
        assert!(cache
            .lookup_child(folder_ino, "remove_me.txt")
            .expect("lookup failed")
            .is_none());
        let listed = cache.list_children(folder_ino).expect("list_children failed");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].effective_name, "keep.txt");
    }

    #[test]
    fn test_ttl_staleness_and_invalidation() {
        let cache_60 = Cache::open_in_memory(60).expect("Failed to open cache with 60s TTL");
        assert!(
            cache_60.is_dir_stale(ROOT_INODE).unwrap(),
            "Unsynced root directory must be stale initially"
        );

        cache_60
            .sync_directory_children(ROOT_INODE, &[])
            .expect("Sync empty root failed");
        assert!(
            !cache_60.is_dir_stale(ROOT_INODE).unwrap(),
            "Recently synced directory with 60s TTL must not be stale"
        );

        cache_60
            .mark_dir_invalidated(ROOT_INODE)
            .expect("mark_dir_invalidated failed");
        assert!(
            cache_60.is_dir_stale(ROOT_INODE).unwrap(),
            "Invalidated directory must report stale"
        );

        // With ttl_secs = 0, directory is always considered stale even immediately after sync
        let cache_zero = Cache::open_in_memory(0).expect("Failed to open cache with 0s TTL");
        cache_zero
            .sync_directory_children(ROOT_INODE, &[])
            .expect("Sync empty root failed");
        assert!(
            cache_zero.is_dir_stale(ROOT_INODE).unwrap(),
            "Directory with 0s TTL must always be stale"
        );
    }

    #[test]
    fn test_change_page_token_and_incremental_apply_changes() {
        let cache = Cache::open_in_memory(300).expect("Failed to open cache");
        assert!(cache.get_change_page_token().unwrap().is_none());

        cache
            .set_change_page_token("token_100")
            .expect("set_change_page_token failed");
        assert_eq!(
            cache.get_change_page_token().unwrap().as_deref(),
            Some("token_100")
        );

        let file_a = sample_drive_file("file_a", "alpha.txt", "text/plain", 100);
        let file_b = sample_drive_file("file_b", "beta.txt", "text/plain", 200);
        cache
            .sync_directory_children(ROOT_INODE, &[file_a.clone(), file_b.clone()])
            .expect("Initial sync failed");
        assert!(!cache.is_dir_stale(ROOT_INODE).unwrap());

        // 1. Modify file_a and trash file_b via apply_changes
        let mut file_a_mod = file_a;
        file_a_mod.name = "alpha_v2.txt".to_string();
        file_a_mod.size = 555;

        let changes = vec![
            ChangeItem {
                file_id: "file_a".to_string(),
                removed: false,
                file: Some(file_a_mod),
            },
            ChangeItem {
                file_id: "file_b".to_string(),
                removed: true,
                file: None,
            },
        ];

        let affected = cache
            .apply_changes(&changes, Some("token_101"))
            .expect("apply_changes failed");
        assert_eq!(affected, 2);
        assert_eq!(
            cache.get_change_page_token().unwrap().as_deref(),
            Some("token_101")
        );

        // Root directory should now be invalidated because its children changed
        assert!(cache.is_dir_stale(ROOT_INODE).unwrap());

        // file_a metadata should be updated in place with the same inode
        let updated_a = cache
            .get_by_drive_id("file_a")
            .unwrap()
            .expect("file_a should still exist");
        assert_eq!(updated_a.name, "alpha_v2.txt");
        assert_eq!(updated_a.size, 555);

        // file_b should be removed from parent_child and queued for FUSE-layer cleanup
        let removed_b = cache.get_by_drive_id("file_b").unwrap().unwrap();
        assert!(cache
            .take_removed_inodes()
            .iter()
            .any(|removed| removed.inode == removed_b.inode));
        assert!(cache.lookup_child(ROOT_INODE, "beta.txt").unwrap().is_none());
    }

    #[test]
    fn test_restored_file_cancels_pending_removal() {
        let cache = MetadataCache::open_in_memory(300).expect("Failed to open cache");
        let file = sample_drive_file("restored_file", "restored.txt", "text/plain", 100);
        let entry = cache
            .sync_directory_children(ROOT_INODE, &[file.clone()])
            .unwrap()
            .remove(0);

        cache
            .apply_changes(
                &[ChangeItem {
                    file_id: file.id.clone(),
                    removed: true,
                    file: None,
                }],
                Some("tok_removed"),
            )
            .unwrap();
        cache
            .apply_changes(
                &[ChangeItem {
                    file_id: file.id.clone(),
                    removed: false,
                    file: Some(file),
                }],
                Some("tok_restored"),
            )
            .unwrap();

        assert!(cache.take_removed_inodes().is_empty());
        assert_eq!(
            cache.get_by_inode(entry.inode).unwrap().unwrap().drive_id,
            "restored_file"
        );
    }

    #[test]
    fn test_clear_all_preserves_reserved_inodes() {
        let cache = Cache::open_in_memory(60).expect("Failed to open cache");
        let file = sample_drive_file("file_1", "doc.txt", "text/plain", 42);
        cache
            .sync_directory_children(ROOT_INODE, &[file])
            .expect("Sync failed");
        cache.set_change_page_token("tok_1").unwrap();

        cache.clear_all().expect("clear_all failed");

        assert!(cache.get_by_inode(ROOT_INODE).unwrap().is_some());
        assert!(cache.get_by_inode(SHARED_WITH_ME_INODE).unwrap().is_some());
        assert!(cache.get_by_drive_id("file_1").unwrap().is_none());
        assert!(cache.list_children(ROOT_INODE).unwrap().is_empty());
        assert!(cache.get_change_page_token().unwrap().is_none());
        assert!(cache.is_dir_stale(ROOT_INODE).unwrap());
    }

    #[test]
    fn test_error_mapping_to_posix_errno() {
        assert_eq!(
            CacheError::NotFound("missing".into()).to_errno(),
            libc::ENOENT
        );
        assert_eq!(CacheError::NotADirectory(42).to_errno(), libc::ENOTDIR);
        assert_eq!(CacheError::LockPoisoned.to_errno(), libc::EIO);
        assert_eq!(
            CacheError::Sqlite(rusqlite::Error::InvalidQuery).to_errno(),
            libc::EIO
        );

        let cache = Cache::open_in_memory(60).expect("Failed to open cache");
        let regular_file = sample_drive_file("file_id_1", "plain.txt", "text/plain", 10);
        let cached = cache.upsert_file(&regular_file).expect("upsert failed");

        let err = cache
            .is_dir_stale(cached.inode)
            .expect_err("Expected NotADirectory error");
        assert_eq!(err.to_errno(), libc::ENOTDIR);

        let not_found_err = cache
            .is_dir_stale(999_999)
            .expect_err("Expected NotFound error");
        assert_eq!(not_found_err.to_errno(), libc::ENOENT);
    }

    #[test]
    fn test_phase1_inode_2_conflict_migration() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE files (
                inode INTEGER PRIMARY KEY AUTOINCREMENT,
                drive_id TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                size INTEGER NOT NULL DEFAULT 0,
                modified_time INTEGER NOT NULL DEFAULT 0,
                is_dir INTEGER NOT NULL DEFAULT 0,
                last_synced_at INTEGER
            );
            CREATE TABLE parent_child (
                parent_inode INTEGER NOT NULL,
                child_inode INTEGER NOT NULL,
                effective_name TEXT NOT NULL,
                PRIMARY KEY (parent_inode, effective_name)
            );
            INSERT INTO files (inode, drive_id, name, mime_type, size, modified_time, is_dir, last_synced_at)
            VALUES (1, 'root', '', 'application/vnd.google-apps.folder', 0, 1700000000, 1, 1700000000);
            INSERT INTO files (inode, drive_id, name, mime_type, size, modified_time, is_dir, last_synced_at)
            VALUES (2, 'legacy_folder_2', 'Projects', 'application/vnd.google-apps.folder', 0, 1700000000, 1, 1700000000);
            INSERT INTO files (inode, drive_id, name, mime_type, size, modified_time, is_dir, last_synced_at)
            VALUES (3, 'legacy_child_3', 'readme.md', 'text/markdown', 256, 1700000000, 0, NULL);
            INSERT INTO parent_child (parent_inode, child_inode, effective_name) VALUES (1, 2, 'Projects');
            INSERT INTO parent_child (parent_inode, child_inode, effective_name) VALUES (2, 3, 'readme.md');
            ",
        )
        .unwrap();

        let cache = Cache::init_connection(conn, 60).expect("Migration of Phase 1 DB failed");

        // Inode 2 must now be the reserved "Shared with me" directory
        let shared = cache
            .get_by_inode(SHARED_WITH_ME_INODE)
            .unwrap()
            .expect("SHARED_WITH_ME_INODE must exist");
        assert_eq!(shared.drive_id, SHARED_WITH_ME_DRIVE_ID);
        assert_eq!(shared.name, SHARED_WITH_ME_NAME);

        // The legacy folder that previously occupied inode 2 must be remapped to a fresh inode > 2
        let remapped_folder = cache
            .get_by_drive_id("legacy_folder_2")
            .unwrap()
            .expect("legacy_folder_2 must still exist");
        assert!(remapped_folder.inode > SHARED_WITH_ME_INODE);
        assert_eq!(remapped_folder.name, "Projects");

        // Both its parent edge under root and its child edge for readme.md must be preserved
        let looked_up_folder = cache
            .lookup_child(ROOT_INODE, "Projects")
            .unwrap()
            .expect("Projects must still be a child of root");
        assert_eq!(looked_up_folder.inode, remapped_folder.inode);

        let looked_up_child = cache
            .lookup_child(remapped_folder.inode, "readme.md")
            .unwrap()
            .expect("readme.md must still be a child of remapped Projects folder");
        assert_eq!(looked_up_child.drive_id, "legacy_child_3");
    }

    #[test]
    fn test_apply_changes_invalidates_shared_with_me_and_tracks_evictions() {
        let cache = Cache::open_in_memory(300).expect("Failed to open cache");
        let shared_file = DriveFile {
            id: "shared_doc_1".to_string(),
            name: "Shared Spec".to_string(),
            mime_type: "text/plain".to_string(),
            size: 100,
            modified_time: 1_700_000_000,
            parents: vec![],
            trashed: false,
            web_view_link: None,
        };
        cache
            .sync_directory_children(SHARED_WITH_ME_INODE, &[shared_file])
            .unwrap();
        assert!(!cache.is_dir_stale(SHARED_WITH_ME_INODE).unwrap());

        // Apply a change for a newly shared item with no local parents
        let new_shared_change = ChangeItem {
            file_id: "shared_doc_2".to_string(),
            removed: false,
            file: Some(DriveFile {
                id: "shared_doc_2".to_string(),
                name: "New Shared File".to_string(),
                mime_type: "text/plain".to_string(),
                size: 200,
                modified_time: 1_700_000_500,
                parents: vec!["external_owner_folder_id".to_string()],
                trashed: false,
                web_view_link: None,
            }),
        };

        let affected = cache
            .apply_changes(&[new_shared_change], Some("tok_200"))
            .unwrap();
        assert_eq!(affected, 1);

        // SHARED_WITH_ME_INODE must now be invalidated so its listing refreshes
        assert!(cache.is_dir_stale(SHARED_WITH_ME_INODE).unwrap());

        // Evicted inodes must include the newly upserted shared file's inode
        let evicted = cache.take_evicted_inodes();
        let new_shared_ino = cache
            .get_by_drive_id("shared_doc_2")
            .unwrap()
            .unwrap()
            .inode;
        assert!(evicted.contains(&new_shared_ino));
        assert!(cache.take_evicted_inodes().is_empty());
    }

    #[test]
    fn test_phase3_pending_file_upload_completion_rename_and_remove() {
        let cache = Cache::open_in_memory(60).expect("Failed to open cache");

        // 1. Create a pending local file under root
        let pending_entry = cache
            .create_pending_file(ROOT_INODE, "draft.txt", "text/plain", 1_750_000_000)
            .expect("create_pending_file failed");
        let draft_ino = pending_entry.inode;
        assert!(draft_ino > SHARED_WITH_ME_INODE);
        assert!(pending_entry.file.is_pending_upload());
        assert!(pending_entry.file.dirty);
        assert_eq!(pending_entry.file.size, 0);

        // 2. Update local write size and mtime
        let written = cache
            .update_local_write(draft_ino, 4096, 1_750_000_010, true)
            .expect("update_local_write failed");
        assert_eq!(written.size, 4096);
        assert_eq!(written.modified_time, 1_750_000_010);
        assert!(written.dirty);

        // 3. Re-syncing root directory while draft.txt is still dirty preserves draft.txt in root
        let remote_sibling = sample_drive_file("remote_1", "existing.txt", "text/plain", 100);
        let synced = cache
            .sync_directory_children(ROOT_INODE, &[remote_sibling])
            .expect("sync_directory_children with dirty file failed");
        assert_eq!(synced.len(), 2);
        assert!(cache.lookup_child(ROOT_INODE, "draft.txt").unwrap().is_some());

        // 4. Complete upload and verify inode stability + dirty cleared
        let uploaded_drive_file = DriveFile {
            id: "drive_real_999".to_string(),
            name: "draft.txt".to_string(),
            mime_type: "text/plain".to_string(),
            size: 4096,
            modified_time: 1_750_000_020,
            parents: vec![ROOT_DRIVE_ID.to_string()],
            trashed: false,
            web_view_link: None,
        };
        let completed = cache
            .complete_file_upload(draft_ino, &uploaded_drive_file)
            .expect("complete_file_upload failed");
        assert_eq!(completed.inode, draft_ino);
        assert_eq!(completed.drive_id, "drive_real_999");
        assert!(!completed.dirty);
        assert!(!completed.is_pending_upload());

        // 5. Insert a subfolder (`mkdir`) and move draft.txt into it (`rename_entry`)
        let subfolder = sample_drive_file("folder_sub", "Archive", FOLDER_MIME_TYPE, 0);
        let sub_entry = cache
            .insert_child_file(ROOT_INODE, "Archive", &subfolder)
            .expect("insert_child_file failed");
        let sub_ino = sub_entry.inode;

        let mut renamed_drive_file = uploaded_drive_file.clone();
        renamed_drive_file.name = "final.txt".to_string();
        renamed_drive_file.parents = vec!["folder_sub".to_string()];

        let renamed = cache
            .rename_entry(
                ROOT_INODE,
                draft_ino,
                sub_ino,
                "final.txt",
                Some(&renamed_drive_file),
            )
            .expect("rename_entry failed");
        assert_eq!(renamed.inode, draft_ino);
        assert_eq!(renamed.name, "final.txt");
        assert!(cache.lookup_child(ROOT_INODE, "draft.txt").unwrap().is_none());
        assert_eq!(
            cache
                .lookup_child(sub_ino, "final.txt")
                .unwrap()
                .unwrap()
                .inode,
            draft_ino
        );
        assert_eq!(cache.get_parent_inode(draft_ino).unwrap(), sub_ino);

        // 6. Remove file (`unlink`)
        cache.remove_file(draft_ino).expect("remove_file failed");
        assert!(cache.get_by_inode(draft_ino).unwrap().is_none());
        assert!(cache.lookup_child(sub_ino, "final.txt").unwrap().is_none());

        // 7. Create a dirty pending file nested inside clean directory `Archive`, then call `clear_all()`.
        // Both `Archive` (its clean ancestor directory) and `nested_dirty.txt` must remain reachable from root.
        let nested_dirty = cache
            .create_pending_file(sub_ino, "nested_dirty.txt", "text/plain", 1_750_000_030)
            .expect("create_pending_file in subfolder failed");
        cache.clear_all().expect("clear_all failed");

        assert!(
            cache.lookup_child(ROOT_INODE, "Archive").unwrap().is_some(),
            "Ancestor directory of dirty file must remain reachable from root after clear_all()"
        );
        assert_eq!(cache.get_parent_inode(sub_ino).unwrap(), ROOT_INODE);
        let preserved_nested = cache
            .lookup_child(sub_ino, "nested_dirty.txt")
            .unwrap()
            .expect("Nested dirty file must remain reachable under its parent folder after clear_all()");
        assert_eq!(preserved_nested.inode, nested_dirty.inode);
        assert_eq!(cache.get_parent_inode(nested_dirty.inode).unwrap(), sub_ino);

        // 8. Removing the highest inode (`nested_dirty.inode`) and creating a new pending file
        // must allocate a strictly higher inode via SQLite AUTOINCREMENT rather than reusing `nested_dirty.inode`.
        cache.remove_file(nested_dirty.inode).unwrap();
        let next_pending = cache
            .create_pending_file(sub_ino, "after_unlink.txt", "text/plain", 1_750_000_040)
            .unwrap();
        assert!(
            next_pending.inode > nested_dirty.inode,
            "SQLite AUTOINCREMENT must not reuse unlinked highest inode {} (got {})",
            nested_dirty.inode,
            next_pending.inode
        );

        // 9. Distinct `--cache-db` paths must resolve to distinct default staging directories.
        let stage_a = MetadataCache::default_staging_dir_for_db(Path::new("/tmp/account_a.db"));
        let stage_b = MetadataCache::default_staging_dir_for_db(Path::new("/tmp/account_b.db"));
        assert_ne!(stage_a, stage_b);
        assert_ne!(stage_a, MetadataCache::default_staging_dir());

        // 10. A relative `--cache-db` path must compute the exact same staging namespace before
        // `MetadataCache::open` creates the database file on disk and after the file exists.
        let rel_db_path = PathBuf::from(format!(
            "target/test-rel-cache-{}-{}.db",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let _ = MetadataCache::remove_db_files(&rel_db_path);
        assert!(!rel_db_path.exists());
        let stage_before_open = MetadataCache::default_staging_dir_for_db(&rel_db_path);
        let opened_rel_cache = MetadataCache::open(&rel_db_path, 60).unwrap();
        assert!(rel_db_path.exists());
        let stage_after_open = MetadataCache::default_staging_dir_for_db(&rel_db_path);
        assert_eq!(
            stage_before_open, stage_after_open,
            "Relative cache DB path must produce identical staging directory before and after DB file creation"
        );
        assert_eq!(opened_rel_cache.scoped_default_staging_dir(), stage_after_open);
        drop(opened_rel_cache);
        let _ = MetadataCache::remove_db_files(&rel_db_path);
    }
}
