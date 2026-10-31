use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension, Row};

use crate::api::{DriveFile, FOLDER_MIME_TYPE, GOOGLE_APPS_MIME_PREFIX};

/// Root inode is strictly pinned to FUSE root ID (`1`).
pub const ROOT_INODE: u64 = fuser::FUSE_ROOT_ID;

/// Google Drive identifier for the root folder.
pub const ROOT_DRIVE_ID: &str = "root";

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
}

impl CachedFile {
    /// Returns `true` if this file is a Google Workspace native document
    /// (e.g., Google Docs, Sheets, Slides) rather than a folder or regular binary file.
    pub fn is_google_native_doc(&self) -> bool {
        self.mime_type.starts_with(GOOGLE_APPS_MIME_PREFIX) && !self.is_dir
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
}

impl MetadataCache {
    /// Opens or creates the SQLite metadata cache at `db_path` with the specified directory TTL (in seconds).
    pub fn open(db_path: &Path, ttl_secs: u64) -> Result<Self, CacheError> {
        if let Some(parent) = db_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }

        let conn = Connection::open(db_path)?;
        Self::init_connection(conn, ttl_secs)
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

    fn init_connection(conn: Connection, ttl_secs: u64) -> Result<Self, CacheError> {
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
                last_synced_at INTEGER
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
            ",
        )?;

        let now = Utc::now().timestamp();
        conn.execute(
            "INSERT OR IGNORE INTO files (inode, drive_id, name, mime_type, size, modified_time, is_dir, last_synced_at)
             VALUES (?1, ?2, '', ?3, 0, ?4, 1, NULL)",
            params![ROOT_INODE as i64, ROOT_DRIVE_ID, FOLDER_MIME_TYPE, now],
        )?;

        Ok(Self {
            conn: Mutex::new(conn),
            ttl_secs,
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

        Ok(CachedFile {
            inode: inode_i64 as u64,
            drive_id,
            name,
            mime_type,
            size: u64::try_from(size_i64).unwrap_or(0),
            modified_time,
            is_dir: is_dir_int != 0,
            last_synced_at,
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
            "SELECT inode, drive_id, name, mime_type, size, modified_time, is_dir, last_synced_at
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
            "SELECT inode, drive_id, name, mime_type, size, modified_time, is_dir, last_synced_at
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
        if inode == ROOT_INODE {
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
            "SELECT f.inode, f.drive_id, f.name, f.mime_type, f.size, f.modified_time, f.is_dir, f.last_synced_at
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
            "SELECT f.inode, f.drive_id, f.name, f.mime_type, f.size, f.modified_time, f.is_dir, f.last_synced_at,
                    pc.parent_inode, pc.effective_name
             FROM parent_child pc
             JOIN files f ON f.inode = pc.child_inode
             WHERE pc.parent_inode = ?1
             ORDER BY pc.rowid ASC",
        )?;

        let rows = stmt.query_map(params![parent_inode as i64], |row| {
            let file = Self::map_cached_file_row(row)?;
            let parent_ino_i64: i64 = row.get(8)?;
            let effective_name: String = row.get(9)?;
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
    pub fn upsert_file(&self, file: &DriveFile) -> Result<CachedFile, CacheError> {
        let conn = self.lock_conn()?;
        Self::upsert_file_with_conn(&conn, file)
    }

    fn upsert_file_with_conn(
        conn: &Connection,
        file: &DriveFile,
    ) -> Result<CachedFile, CacheError> {
        let is_dir_int = if file.is_dir() { 1i64 } else { 0i64 };
        let size_i64 = i64::try_from(file.size).unwrap_or(i64::MAX);

        conn.execute(
            "INSERT INTO files (drive_id, name, mime_type, size, modified_time, is_dir)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(drive_id) DO UPDATE SET
                 name = excluded.name,
                 mime_type = excluded.mime_type,
                 size = excluded.size,
                 modified_time = excluded.modified_time,
                 is_dir = excluded.is_dir",
            params![
                file.id,
                file.name,
                file.mime_type,
                size_i64,
                file.modified_time,
                is_dir_int,
            ],
        )?;

        Self::get_by_drive_id_with_conn(conn, &file.id)?
            .ok_or_else(|| CacheError::NotFound(file.id.clone()))
    }

    /// Atomically replaces and deduplicates the child entries of `parent_inode` in a single SQLite transaction,
    /// preserving existing inodes for known `drive_id`s and updating `last_synced_at` on `parent_inode`.
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
        let mut used_names = HashSet::with_capacity(children.len());
        let mut names = vec![String::new(); children.len()];
        for idx in naming_order {
            names[idx] = disambiguate_filename(&children[idx].name, &mut used_names);
        }
        let mut entries = Vec::with_capacity(children.len());

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

            entries.push(DirectoryEntry {
                inode: cached_file.inode,
                parent_inode,
                effective_name,
                file: cached_file,
            });
        }

        let now = Utc::now().timestamp();
        tx.execute(
            "UPDATE files SET last_synced_at = ?1 WHERE inode = ?2",
            params![now, parent_inode as i64],
        )?;

        tx.commit()?;
        Ok(entries)
    }
}

/// Sanitizes a Google Drive filename for POSIX compatibility and deterministically disambiguates
/// collisions among siblings in the same directory (e.g., `report.pdf` -> `report (1).pdf`).
fn disambiguate_filename(raw_name: &str, used_names: &mut HashSet<String>) -> String {
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
        }
    }

    #[test]
    fn test_root_inode_initialization() {
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

        assert_eq!(
            cache
                .get_parent_inode(ROOT_INODE)
                .expect("get_parent_inode failed"),
            ROOT_INODE
        );
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
        assert!(ino_1 > ROOT_INODE);
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
}
