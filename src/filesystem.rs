use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
    TimeOrNow, FUSE_ROOT_ID,
};
use tokio::runtime::Handle;

use crate::api::{guess_mime_type, ApiError, DriveClient, StorageQuota, FOLDER_MIME_TYPE};
use crate::auth::GoogleDocsMode;
use crate::cache::{
    disambiguate_filename, CachedFile, MetadataCache, ROOT_DRIVE_ID, ROOT_INODE,
    SHARED_WITH_ME_DRIVE_ID, SHARED_WITH_ME_INODE, SHARED_WITH_ME_NAME,
};

/// Default time-to-live for FUSE attribute and directory entry responses.
pub const DEFAULT_ATTR_TTL: Duration = Duration::from_secs(60);

/// Default read-ahead chunk size in bytes (4 MB).
pub const DEFAULT_READ_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Maximum read-ahead chunk size in bytes (16 MB).
pub const MAX_READ_CHUNK_SIZE: usize = 16 * 1024 * 1024;

/// Standard 512-byte POSIX block size used for reporting `FileAttr::blocks`.
pub const BLOCK_SIZE: u32 = 512;

/// Time-to-live for cached `statfs` storage quota responses (5 minutes).
pub const QUOTA_CACHE_TTL: Duration = Duration::from_secs(300);

/// Maximum number of active read-ahead chunks retained in memory simultaneously.
const MAX_CACHED_CHUNKS: usize = 4;

/// Maximum number of files simultaneously staged for writing in the temporary staging directory.
pub const MAX_STAGED_WRITE_FILES: usize = 64;

/// Write-related POSIX open flags that must be rejected on a read-only mount.
const WRITE_OPEN_FLAGS: i32 =
    libc::O_WRONLY | libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC | libc::O_APPEND;

/// Tracks a locally staged file actively open for writing or awaiting upload.
#[derive(Debug, Clone)]
pub struct StagedFileState {
    pub ino: u64,
    pub path: PathBuf,
    pub dirty: bool,
    pub populated_from_remote: bool,
    pub open_handles: HashSet<u64>,
}

fn ensure_private_staging_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn create_truncated_stage_file(path: &Path) -> std::io::Result<File> {
    if let Some(parent) = path.parent() {
        ensure_private_staging_dir(parent)?;
    }
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        f
    };
    #[cfg(not(unix))]
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    Ok(file)
}

fn open_existing_stage_file_rw(path: &Path) -> std::io::Result<File> {
    if let Some(parent) = path.parent() {
        ensure_private_staging_dir(parent)?;
    }
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)?;
        f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        f
    };
    #[cfg(not(unix))]
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    Ok(file)
}

fn tighten_stage_file_permissions(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
}

/// Computed `statfs` metrics returned to the kernel and desktop file managers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatfsSnapshot {
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub bsize: u32,
    pub namelen: u32,
    pub frsize: u32,
}

/// A single in-memory read-ahead chunk cached for an open file.
#[derive(Debug, Clone)]
pub struct ReadChunk {
    pub ino: u64,
    pub offset: u64,
    pub modified_time: i64,
    pub data: Bytes,
    pub last_accessed: Instant,
}

impl ReadChunk {
    /// Checks if the byte range `[offset, offset + len)` lies completely within this chunk,
    /// returning a zero-copy `Bytes` slice if so.
    pub fn slice_if_contained(&mut self, offset: u64, len: usize) -> Option<Bytes> {
        let chunk_end = self.offset.checked_add(self.data.len() as u64)?;
        let req_end = offset.checked_add(len as u64)?;

        if offset >= self.offset && req_end <= chunk_end {
            self.last_accessed = Instant::now();
            let rel_start = (offset - self.offset) as usize;
            let rel_end = rel_start + len;
            Some(self.data.slice(rel_start..rel_end))
        } else {
            None
        }
    }
}

/// Bounded LRU buffer storing read-ahead chunks across open files to avoid HTTP thrashing.
#[derive(Debug)]
pub struct ReadChunkBuffer {
    chunks: Vec<ReadChunk>,
    max_chunks: usize,
}

impl Default for ReadChunkBuffer {
    fn default() -> Self {
        Self::new(MAX_CACHED_CHUNKS)
    }
}

impl ReadChunkBuffer {
    /// Creates a new `ReadChunkBuffer` holding at most `max_chunks` chunks in memory.
    pub fn new(max_chunks: usize) -> Self {
        let cap = max_chunks.max(1);
        Self {
            chunks: Vec::with_capacity(cap),
            max_chunks: cap,
        }
    }

    /// Returns a zero-copy `Bytes` slice if `[offset, offset + requested_len)` is cached for `ino`.
    pub fn get_slice(&mut self, ino: u64, offset: u64, requested_len: usize) -> Option<Bytes> {
        for chunk in &mut self.chunks {
            if chunk.ino == ino {
                if let Some(slice) = chunk.slice_if_contained(offset, requested_len) {
                    return Some(slice);
                }
            }
        }
        None
    }

    /// Returns a zero-copy `Bytes` slice if `[offset, offset + requested_len)` is cached for `ino`
    /// and its cached `modified_time` matches the current file metadata; evicts stale entries on mismatch.
    pub fn get_slice_for_mtime(
        &mut self,
        ino: u64,
        offset: u64,
        requested_len: usize,
        modified_time: i64,
    ) -> Option<Bytes> {
        if self
            .chunks
            .iter()
            .any(|c| c.ino == ino && c.modified_time != modified_time)
        {
            self.evict(ino);
            return None;
        }
        self.get_slice(ino, offset, requested_len)
    }

    /// Returns the full cached `Bytes` if a chunk starting at `offset == 0` is buffered for `ino`.
    pub fn get_zero_offset_chunk(&mut self, ino: u64) -> Option<Bytes> {
        for chunk in &mut self.chunks {
            if chunk.ino == ino && chunk.offset == 0 {
                chunk.last_accessed = Instant::now();
                return Some(chunk.data.clone());
            }
        }
        None
    }

    /// Returns the full cached `Bytes` if a chunk starting at `offset == 0` is buffered for `ino`
    /// and its `modified_time` matches; evicts stale entries on mismatch.
    pub fn get_zero_offset_chunk_for_mtime(
        &mut self,
        ino: u64,
        modified_time: i64,
    ) -> Option<Bytes> {
        if self
            .chunks
            .iter()
            .any(|c| c.ino == ino && c.modified_time != modified_time)
        {
            self.evict(ino);
            return None;
        }
        self.get_zero_offset_chunk(ino)
    }

    /// Returns the length of a cached chunk starting at `offset == 0` for `ino` without mutating access timestamps.
    pub fn peek_zero_offset_len(&self, ino: u64) -> Option<u64> {
        self.chunks
            .iter()
            .find(|c| c.ino == ino && c.offset == 0)
            .map(|c| c.data.len() as u64)
    }

    /// Returns the length of a cached chunk starting at `offset == 0` for `ino` if its `modified_time` matches.
    pub fn peek_zero_offset_len_for_mtime(&self, ino: u64, modified_time: i64) -> Option<u64> {
        self.chunks
            .iter()
            .find(|c| c.ino == ino && c.offset == 0 && c.modified_time == modified_time)
            .map(|c| c.data.len() as u64)
    }

    /// Inserts or replaces the read-ahead chunk for `ino`, evicting the least-recently accessed chunk if full.
    pub fn insert(&mut self, ino: u64, offset: u64, data: Bytes) {
        self.insert_with_mtime(ino, offset, 0, data);
    }

    /// Inserts or replaces the read-ahead chunk for `ino` tagged with `modified_time`,
    /// evicting the least-recently accessed chunk if full.
    pub fn insert_with_mtime(&mut self, ino: u64, offset: u64, modified_time: i64, data: Bytes) {
        let now = Instant::now();
        if let Some(existing) = self.chunks.iter_mut().find(|c| c.ino == ino) {
            existing.offset = offset;
            existing.modified_time = modified_time;
            existing.data = data;
            existing.last_accessed = now;
            return;
        }

        if self.chunks.len() >= self.max_chunks {
            if let Some((oldest_idx, _)) = self
                .chunks
                .iter()
                .enumerate()
                .min_by_key(|(_, c)| c.last_accessed)
            {
                self.chunks.swap_remove(oldest_idx);
            }
        }

        self.chunks.push(ReadChunk {
            ino,
            offset,
            modified_time,
            data,
            last_accessed: now,
        });
    }

    /// Evicts any cached chunk associated with `ino`.
    pub fn evict(&mut self, ino: u64) {
        self.chunks.retain(|c| c.ino != ino);
    }

    /// Returns the number of chunks currently buffered.
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    /// Returns `true` if no chunks are currently buffered.
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
}

/// Polls Google Drive `changes.list` once (initializing `startPageToken` if not yet recorded)
/// and applies any remote changes to the local SQLite metadata cache.
pub async fn sync_changes_once(
    api: &DriveClient,
    cache: &MetadataCache,
) -> Result<usize, ApiError> {
    let maybe_token = match cache.get_change_page_token() {
        Ok(tok) => tok,
        Err(e) => {
            log::error!("Failed to read change page token from cache: {}", e);
            None
        }
    };

    let Some(token) = maybe_token else {
        let start_token = api.get_start_page_token().await?;
        if let Err(e) = cache.set_change_page_token(&start_token) {
            log::error!("Failed to save initial startPageToken to cache: {}", e);
        }
        return Ok(0);
    };

    let (changes, new_token) = match api.list_changes(&token).await {
        Ok(res) => res,
        Err(err) if err.is_expired_page_token() => {
            log::warn!(
                "Google Drive change page token '{}' expired ({}); clearing cache and reinitializing startPageToken",
                token,
                err
            );
            if let Err(clear_err) = cache.clear_all() {
                log::error!(
                    "Failed to clear cache after expired change page token: {}",
                    clear_err
                );
            }
            let fresh_token = api.get_start_page_token().await?;
            if let Err(set_err) = cache.set_change_page_token(&fresh_token) {
                log::error!(
                    "Failed to save fresh startPageToken after token expiry: {}",
                    set_err
                );
            }
            return Ok(0);
        }
        Err(err) => return Err(err),
    };
    match cache.apply_changes(&changes, Some(&new_token)) {
        Ok(affected) => {
            if affected > 0 {
                log::info!(
                    "Applied {} remote Google Drive change(s) (new page token={})",
                    affected,
                    new_token
                );
            }
            Ok(affected)
        }
        Err(e) => {
            log::error!("Failed to apply incremental Drive changes to cache: {}", e);
            Ok(0)
        }
    }
}

/// Google Drive FUSE filesystem implementation supporting both Read-Only and Read-Write modes.
pub struct GDriveFS {
    api: Arc<DriveClient>,
    cache: Arc<MetadataCache>,
    rt_handle: Handle,
    attr_ttl: Duration,
    chunk_size: usize,
    chunk_buffer: ReadChunkBuffer,
    uid: u32,
    gid: u32,
    google_docs_mode: GoogleDocsMode,
    shared_with_me: bool,
    quota_cache: Option<(StorageQuota, Instant)>,
    read_only: bool,
    staging_dir: PathBuf,
    staged_files: HashMap<u64, StagedFileState>,
    open_handles: HashMap<u64, HashMap<u64, i32>>,
    unlinked_inodes: HashSet<u64>,
    next_fh: u64,
}

impl GDriveFS {
    /// Creates a new `GDriveFS` with default attribute TTL (60s) and 4 MB read-ahead chunk size.
    pub fn new(api: Arc<DriveClient>, cache: Arc<MetadataCache>, rt_handle: Handle) -> Self {
        Self::with_config(
            api,
            cache,
            rt_handle,
            DEFAULT_ATTR_TTL,
            DEFAULT_READ_CHUNK_SIZE,
        )
    }

    /// Creates a new `GDriveFS` with custom attribute TTL and read-ahead chunk size.
    pub fn with_config(
        api: Arc<DriveClient>,
        cache: Arc<MetadataCache>,
        rt_handle: Handle,
        attr_ttl: Duration,
        chunk_size: usize,
    ) -> Self {
        Self::with_full_config(
            api,
            cache,
            rt_handle,
            attr_ttl,
            chunk_size,
            GoogleDocsMode::Browser,
            true,
        )
    }

    /// Creates a new `GDriveFS` with full Phase 2 configuration options (defaults to read-only mode
    /// unless configured via `with_phase3_config` or `with_read_only(false)`).
    pub fn with_full_config(
        api: Arc<DriveClient>,
        cache: Arc<MetadataCache>,
        rt_handle: Handle,
        attr_ttl: Duration,
        chunk_size: usize,
        google_docs_mode: GoogleDocsMode,
        shared_with_me: bool,
    ) -> Self {
        let default_staging = cache.scoped_default_staging_dir();
        Self::with_phase3_config(
            api,
            cache,
            rt_handle,
            attr_ttl,
            chunk_size,
            google_docs_mode,
            shared_with_me,
            true,
            default_staging,
        )
    }

    /// Creates a new `GDriveFS` with full Phase 3 configuration including `read_only` mode
    /// and a local write-back `staging_dir`.
    #[allow(clippy::too_many_arguments)]
    pub fn with_phase3_config(
        api: Arc<DriveClient>,
        cache: Arc<MetadataCache>,
        rt_handle: Handle,
        attr_ttl: Duration,
        chunk_size: usize,
        google_docs_mode: GoogleDocsMode,
        shared_with_me: bool,
        read_only: bool,
        staging_dir: PathBuf,
    ) -> Self {
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        let scoped_staging_dir = if staging_dir == MetadataCache::default_staging_dir() {
            cache.scoped_default_staging_dir()
        } else {
            staging_dir
        };
        if !read_only {
            let _ = ensure_private_staging_dir(&scoped_staging_dir);
        }
        let mut fs = Self {
            api,
            cache,
            rt_handle,
            attr_ttl,
            chunk_size: chunk_size.max(4096),
            chunk_buffer: ReadChunkBuffer::default(),
            uid,
            gid,
            google_docs_mode,
            shared_with_me,
            quota_cache: None,
            read_only,
            staging_dir: scoped_staging_dir,
            staged_files: HashMap::new(),
            open_handles: HashMap::new(),
            unlinked_inodes: HashSet::new(),
            next_fh: 1,
        };
        if !read_only {
            fs.recover_dirty_staged_files();
        }
        fs
    }

    /// Sets whether this filesystem instance enforces strict read-only mode (`libc::EROFS`).
    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        if !read_only {
            let _ = ensure_private_staging_dir(&self.staging_dir);
            self.recover_dirty_staged_files();
        }
        self
    }

    /// Overrides the local temporary staging directory path (`~/.cache/gdrive-fuse-rs/staging/`).
    pub fn with_staging_dir(mut self, staging_dir: impl Into<PathBuf>) -> Self {
        let raw_dir = staging_dir.into();
        self.staging_dir = if raw_dir == MetadataCache::default_staging_dir() {
            self.cache.scoped_default_staging_dir()
        } else {
            raw_dir
        };
        if !self.read_only {
            let _ = ensure_private_staging_dir(&self.staging_dir);
            self.recover_dirty_staged_files();
        }
        self
    }

    /// Recovers in-memory `staged_files` entries for any dirty/pending inodes in SQLite that already
    /// have an unsent `inode_*.stage` file on disk (e.g. after a restart or 410-token cache recovery).
    fn recover_dirty_staged_files(&mut self) {
        let Ok(dirty_files) = self.cache.list_dirty_or_pending_files() else {
            return;
        };
        for file in dirty_files {
            if self.staged_files.len() >= MAX_STAGED_WRITE_FILES
                && !self.staged_files.contains_key(&file.inode)
            {
                break;
            }
            let stage_path = self.staging_file_path(file.inode);
            if stage_path.exists() {
                tighten_stage_file_permissions(&stage_path);
                let entry = self
                    .staged_files
                    .entry(file.inode)
                    .or_insert_with(|| StagedFileState {
                        ino: file.inode,
                        path: stage_path.clone(),
                        dirty: true,
                        populated_from_remote: true,
                        open_handles: HashSet::new(),
                    });
                entry.path = stage_path;
                entry.dirty = true;
                entry.populated_from_remote = true;
            }
        }
    }

    fn track_open_handle(&mut self, ino: u64, fh: u64, flags: i32) {
        self.open_handles.entry(ino).or_default().insert(fh, flags);
    }

    fn is_handle_writable(&self, ino: u64, fh: u64) -> bool {
        self.open_handles
            .get(&ino)
            .and_then(|handles| handles.get(&fh))
            .is_some_and(|&flags| {
                let acc = flags & libc::O_ACCMODE;
                acc == libc::O_WRONLY || acc == libc::O_RDWR
            })
    }

    fn untrack_open_handle(&mut self, ino: u64, fh: u64) -> bool {
        let mut was_writable = false;
        if let Some(handles) = self.open_handles.get_mut(&ino) {
            if let Some(flags) = handles.remove(&fh) {
                was_writable = (flags & WRITE_OPEN_FLAGS) != 0;
            }
            if handles.is_empty() {
                self.open_handles.remove(&ino);
            }
        }
        if let Some(st) = self.staged_files.get_mut(&ino) {
            if st.open_handles.remove(&fh) {
                was_writable = true;
            }
        }
        was_writable
    }

    fn has_open_handles(&self, ino: u64) -> bool {
        self.open_handles
            .get(&ino)
            .is_some_and(|handles| !handles.is_empty())
            || self
                .staged_files
                .get(&ino)
                .is_some_and(|st| !st.open_handles.is_empty())
    }

    /// Returns `true` if the filesystem is mounted in strict read-only mode.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Returns the local staging directory path for active write buffers.
    pub fn staging_dir(&self) -> &Path {
        &self.staging_dir
    }

    /// Returns the path of the temporary staging file for `ino` inside `self.staging_dir`.
    pub fn staging_file_path(&self, ino: u64) -> PathBuf {
        let base_dir = if self.staging_dir == MetadataCache::default_staging_dir() {
            match self.cache.staging_namespace() {
                Some(ns) => self.staging_dir.join(ns),
                None => self.staging_dir.clone(),
            }
        } else {
            self.staging_dir.clone()
        };
        base_dir.join(format!("inode_{}.stage", ino))
    }

    fn alloc_fh(&mut self) -> u64 {
        let fh = self.next_fh;
        self.next_fh = self.next_fh.wrapping_add(1).max(1);
        fh
    }

    /// Returns the file extension appended to Google Workspace native files under `mode`.
    pub fn google_doc_extension(mime_type: &str, mode: GoogleDocsMode) -> &'static str {
        match mode {
            GoogleDocsMode::Browser => match mime_type {
                "application/vnd.google-apps.document" => ".gdoc",
                "application/vnd.google-apps.spreadsheet" => ".gsheet",
                "application/vnd.google-apps.presentation" => ".gslides",
                _ => ".gdrive",
            },
            GoogleDocsMode::Desktop => ".desktop",
            GoogleDocsMode::Pdf => ".pdf",
            GoogleDocsMode::Office => match mime_type {
                "application/vnd.google-apps.document" => ".docx",
                "application/vnd.google-apps.spreadsheet" => ".xlsx",
                "application/vnd.google-apps.presentation" => ".pptx",
                _ => ".pdf",
            },
        }
    }

    /// Returns the target MIME type for `/export` when `mode` is `Pdf` or `Office`.
    pub fn google_doc_export_mime(mime_type: &str, mode: GoogleDocsMode) -> Option<&'static str> {
        match mode {
            GoogleDocsMode::Browser | GoogleDocsMode::Desktop => None,
            GoogleDocsMode::Pdf => Some("application/pdf"),
            GoogleDocsMode::Office => match mime_type {
                "application/vnd.google-apps.document" => Some(
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                ),
                "application/vnd.google-apps.spreadsheet" => {
                    Some("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                }
                "application/vnd.google-apps.presentation" => Some(
                    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                ),
                _ => Some("application/pdf"),
            },
        }
    }

    /// Returns the display name exposed in `readdir` and `lookup` for `file`.
    pub fn display_name_for_file(
        effective_name: &str,
        file: &CachedFile,
        mode: GoogleDocsMode,
    ) -> String {
        if !file.is_google_native_doc() {
            return effective_name.to_string();
        }
        let ext = Self::google_doc_extension(&file.mime_type, mode);
        if effective_name.ends_with(ext) {
            effective_name.to_string()
        } else {
            format!("{}{}", effective_name, ext)
        }
    }

    /// Generates an in-memory shortcut document (`.gdoc`/`.gsheet`/`.gslides` JSON pointer or `.desktop` launcher)
    /// for a Google Workspace native document.
    pub fn synthesize_google_doc_shortcut(file: &CachedFile, mode: GoogleDocsMode) -> Bytes {
        let url = file.web_url();
        let clean_name = file.name.replace(['\n', '\r'], " ");
        match mode {
            GoogleDocsMode::Desktop => Bytes::from(format!(
                "[Desktop Entry]\nVersion=1.0\nType=Link\nName={}\nURL={}\nIcon=text-html\n",
                clean_name, url
            )),
            _ => {
                let payload = serde_json::json!({
                    "url": url,
                    "doc_id": file.drive_id,
                    "title": clean_name,
                    "mime_type": file.mime_type,
                });
                let mut json_str = serde_json::to_string_pretty(&payload)
                    .unwrap_or_else(|_| format!("{{\"url\":\"{}\"}}", url));
                json_str.push('\n');
                Bytes::from(json_str)
            }
        }
    }

    /// Evicts any cached read-ahead or exported document chunks for inodes that were
    /// modified or removed in SQLite by background incremental sync.
    fn drain_cache_evictions(&mut self) {
        for ino in self.cache.take_evicted_inodes() {
            self.chunk_buffer.evict(ino);
        }
    }

    fn ensure_google_doc_exported(&mut self, file: &CachedFile) -> Result<Bytes, libc::c_int> {
        self.drain_cache_evictions();
        if let Some(existing) = self
            .chunk_buffer
            .get_zero_offset_chunk_for_mtime(file.inode, file.modified_time)
        {
            return Ok(existing);
        }

        let export_mime = Self::google_doc_export_mime(&file.mime_type, self.google_docs_mode)
            .unwrap_or("application/pdf");

        let exported = self
            .rt_handle
            .block_on(self.api.export_file(&file.drive_id, export_mime))
            .map_err(|api_err| {
                log::error!(
                    "Failed to export Google Doc inode {} (drive_id={}, mime={}): {}",
                    file.inode,
                    file.drive_id,
                    export_mime,
                    api_err
                );
                api_err.to_errno()
            })?;

        self.chunk_buffer.insert_with_mtime(
            file.inode,
            0,
            file.modified_time,
            exported.clone(),
        );
        Ok(exported)
    }

    /// Converts a `CachedFile` record into a `fuser::FileAttr` structure.
    pub fn file_to_attr(&self, file: &CachedFile) -> FileAttr {
        let mtime = unix_secs_to_system_time(file.modified_time);
        let (kind, perm, nlink) = if file.is_dir {
            (FileType::Directory, 0o755, 2)
        } else if file.is_google_native_doc() && self.google_docs_mode == GoogleDocsMode::Desktop {
            (FileType::RegularFile, 0o755, 1)
        } else {
            (FileType::RegularFile, 0o644, 1)
        };

        let size = if file.is_google_native_doc() {
            match self.google_docs_mode {
                GoogleDocsMode::Browser | GoogleDocsMode::Desktop => {
                    Self::synthesize_google_doc_shortcut(file, self.google_docs_mode).len() as u64
                }
                GoogleDocsMode::Pdf | GoogleDocsMode::Office => self
                    .chunk_buffer
                    .peek_zero_offset_len_for_mtime(file.inode, file.modified_time)
                    .unwrap_or(file.size),
            }
        } else {
            file.size
        };

        let blocks = size.div_ceil(BLOCK_SIZE as u64);

        FileAttr {
            ino: file.inode,
            size,
            blocks,
            atime: mtime,
            mtime,
            ctime: mtime,
            crtime: mtime,
            kind,
            perm,
            nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    /// Ensures the target directory's children are synced in the SQLite cache if absent or expired.
    pub fn ensure_dir_synced(&self, dir_inode: u64) -> Result<CachedFile, libc::c_int> {
        let dir_file = match self.cache.get_by_inode(dir_inode) {
            Ok(Some(file)) => file,
            Ok(None) => return Err(libc::ENOENT),
            Err(e) => {
                log::error!("Cache error looking up directory inode {}: {}", dir_inode, e);
                return Err(e.to_errno());
            }
        };

        if !dir_file.is_dir {
            return Err(libc::ENOTDIR);
        }

        let is_stale = match self.cache.is_dir_stale(dir_inode) {
            Ok(stale) => stale,
            Err(e) => {
                log::error!(
                    "Cache error checking staleness for directory inode {}: {}",
                    dir_inode,
                    e
                );
                return Err(e.to_errno());
            }
        };

        if is_stale {
            log::debug!(
                "Directory inode {} (drive_id={}) is stale; fetching children from Drive API",
                dir_inode,
                dir_file.drive_id
            );
            let children_res =
                if dir_inode == SHARED_WITH_ME_INODE || dir_file.drive_id == SHARED_WITH_ME_DRIVE_ID
                {
                    self.rt_handle.block_on(self.api.list_shared_with_me())
                } else {
                    self.rt_handle
                        .block_on(self.api.list_children(&dir_file.drive_id))
                };

            let children = children_res.map_err(|api_err| {
                log::error!(
                    "Failed to list children from Drive API for inode {} (drive_id={}): {}",
                    dir_inode,
                    dir_file.drive_id,
                    api_err
                );
                api_err.to_errno()
            })?;

            self.cache
                .sync_directory_children(dir_inode, &children)
                .map_err(|cache_err| {
                    log::error!(
                        "Failed to sync children in cache for inode {}: {}",
                        dir_inode,
                        cache_err
                    );
                    cache_err.to_errno()
                })?;
        }

        Ok(dir_file)
    }

    /// Resolves `getattr` for `ino` without requiring a `fuser::ReplyAttr`.
    pub fn handle_getattr(&mut self, ino: u64) -> Result<FileAttr, libc::c_int> {
        self.drain_cache_evictions();
        match self.cache.get_by_inode(ino) {
            Ok(Some(file)) => {
                if file.is_google_native_doc()
                    && matches!(
                        self.google_docs_mode,
                        GoogleDocsMode::Pdf | GoogleDocsMode::Office
                    )
                {
                    let _ = self.ensure_google_doc_exported(&file)?;
                }
                Ok(self.file_to_attr(&file))
            }
            Ok(None) if ino == FUSE_ROOT_ID => {
                let fallback_root = CachedFile {
                    inode: ROOT_INODE,
                    drive_id: ROOT_DRIVE_ID.to_string(),
                    name: String::new(),
                    mime_type: FOLDER_MIME_TYPE.to_string(),
                    size: 0,
                    modified_time: 0,
                    is_dir: true,
                    last_synced_at: None,
                    web_view_link: None,
                    dirty: false,
                };
                Ok(self.file_to_attr(&fallback_root))
            }
            Ok(None) => Err(libc::ENOENT),
            Err(e) => {
                log::error!("Cache error in getattr for inode {}: {}", ino, e);
                Err(e.to_errno())
            }
        }
    }

    /// Computes the disambiguated POSIX display names for all cached children of `dir_inode`
    /// after applying Google Workspace file extensions and reserving `"Shared with me"` in root.
    fn disambiguated_children(
        &self,
        dir_inode: u64,
    ) -> Result<Vec<(String, CachedFile)>, libc::c_int> {
        let children = self.cache.list_children(dir_inode).map_err(|e| {
            log::error!("Cache error listing children for inode {}: {}", dir_inode, e);
            e.to_errno()
        })?;

        let raw_display_names: Vec<String> = children
            .iter()
            .map(|entry| {
                Self::display_name_for_file(
                    &entry.file.name,
                    &entry.file,
                    self.google_docs_mode,
                )
            })
            .collect();

        // Assign deduplicated display names in a deterministic order (base display name, then drive_id)
        // so that native Google Docs with appended extensions (e.g. `Plan` -> `Plan.gdoc`) cannot
        // collide with regular sibling files (`Plan.gdoc`) or the reserved root `Shared with me` folder.
        let mut naming_order: Vec<usize> = (0..children.len()).collect();
        naming_order.sort_by(|&a, &b| {
            (
                raw_display_names[a].as_str(),
                children[a].file.drive_id.as_str(),
            )
                .cmp(&(
                    raw_display_names[b].as_str(),
                    children[b].file.drive_id.as_str(),
                ))
        });

        let mut used_names = HashSet::with_capacity(children.len() + 1);
        if dir_inode == FUSE_ROOT_ID && self.shared_with_me {
            used_names.insert(SHARED_WITH_ME_NAME.to_string());
        }

        let mut final_names = vec![String::new(); children.len()];
        for idx in naming_order {
            final_names[idx] = disambiguate_filename(&raw_display_names[idx], &mut used_names);
        }

        Ok(final_names
            .into_iter()
            .zip(children.into_iter().map(|e| e.file))
            .collect())
    }

    fn find_child_by_display_name(
        &self,
        parent: u64,
        name: &str,
    ) -> Result<Option<CachedFile>, libc::c_int> {
        Ok(self
            .disambiguated_children(parent)?
            .into_iter()
            .find_map(|(display_name, file)| (display_name == name).then_some(file)))
    }

    /// Resolves `lookup` for a child `name` inside `parent` inode.
    pub fn handle_lookup(&mut self, parent: u64, name: &str) -> Result<FileAttr, libc::c_int> {
        self.drain_cache_evictions();
        if parent == FUSE_ROOT_ID && self.shared_with_me && name == SHARED_WITH_ME_NAME {
            return self.handle_getattr(SHARED_WITH_ME_INODE);
        }

        self.ensure_dir_synced(parent)?;

        match self.find_child_by_display_name(parent, name)? {
            Some(child) => {
                if child.is_google_native_doc()
                    && matches!(
                        self.google_docs_mode,
                        GoogleDocsMode::Pdf | GoogleDocsMode::Office
                    )
                {
                    let _ = self.ensure_google_doc_exported(&child)?;
                }
                Ok(self.file_to_attr(&child))
            }
            None => Err(libc::ENOENT),
        }
    }

    /// Resolves `readdir` for `ino`, returning `.` and `..` followed by all cached children.
    pub fn handle_readdir(&self, ino: u64) -> Result<Vec<(u64, FileType, String)>, libc::c_int> {
        if ino == SHARED_WITH_ME_INODE && !self.shared_with_me {
            return Err(libc::ENOENT);
        }

        self.ensure_dir_synced(ino)?;

        let parent_ino = self.cache.get_parent_inode(ino).map_err(|e| {
            log::error!("Cache error resolving parent for inode {}: {}", ino, e);
            e.to_errno()
        })?;

        let children = self.disambiguated_children(ino)?;

        let mut entries = Vec::with_capacity(children.len() + 3);
        entries.extend([
            (ino, FileType::Directory, ".".to_string()),
            (parent_ino, FileType::Directory, "..".to_string()),
        ]);

        if ino == FUSE_ROOT_ID && self.shared_with_me {
            entries.push((
                SHARED_WITH_ME_INODE,
                FileType::Directory,
                SHARED_WITH_ME_NAME.to_string(),
            ));
        }

        for (display_name, file) in children {
            let kind = if file.is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            entries.push((file.inode, kind, display_name));
        }

        Ok(entries)
    }

    /// Validates `access` permissions on `ino`, rejecting `W_OK` with `libc::EROFS` when mounted read-only.
    pub fn handle_access(&mut self, ino: u64, mask: i32) -> Result<(), libc::c_int> {
        if (mask & libc::W_OK) != 0 {
            if self.read_only {
                return Err(libc::EROFS);
            }
            if ino == SHARED_WITH_ME_INODE {
                return Err(libc::EACCES);
            }
        }
        self.handle_getattr(ino).map(|_| ())
    }

    /// Validates `open` flags and target inode type (read-only check + directory check).
    pub fn handle_open(&self, ino: u64, flags: i32) -> Result<(), libc::c_int> {
        if (flags & WRITE_OPEN_FLAGS) != 0 && self.read_only {
            log::debug!(
                "Rejecting open with write flags ({:#x}) on inode {} with EROFS",
                flags,
                ino
            );
            return Err(libc::EROFS);
        }

        match self.cache.get_by_inode(ino) {
            Ok(Some(file)) => {
                if file.is_dir {
                    Err(libc::EISDIR)
                } else if (flags & WRITE_OPEN_FLAGS) != 0 && file.is_google_native_doc() {
                    Err(libc::EACCES)
                } else {
                    Ok(())
                }
            }
            Ok(None) => Err(libc::ENOENT),
            Err(e) => {
                log::error!("Cache error in open for inode {}: {}", ino, e);
                Err(e.to_errno())
            }
        }
    }

    /// Opens an existing file for reading or writing, preparing the local staging buffer if opened with write/truncate flags.
    pub fn handle_open_with_fh(&mut self, ino: u64, flags: i32) -> Result<u64, libc::c_int> {
        self.handle_open(ino, flags)?;
        let fh = self.alloc_fh();

        if (flags & WRITE_OPEN_FLAGS) != 0 {
            let file = self
                .cache
                .get_by_inode(ino)
                .map_err(|e| e.to_errno())?
                .ok_or(libc::ENOENT)?;

            if !self.staged_files.contains_key(&ino)
                && self.staged_files.len() >= MAX_STAGED_WRITE_FILES
            {
                return Err(libc::ENOSPC);
            }

            let stage_path = self.staging_file_path(ino);
            if let Some(parent_dir) = stage_path.parent() {
                ensure_private_staging_dir(parent_dir).map_err(|_| libc::EIO)?;
            }

            let has_dirty_stage_on_disk =
                (file.dirty || file.is_pending_upload()) && stage_path.exists();
            if has_dirty_stage_on_disk {
                tighten_stage_file_permissions(&stage_path);
            }
            let truncate = (flags & libc::O_TRUNC) != 0;
            if truncate {
                create_truncated_stage_file(&stage_path).map_err(|e| {
                    log::error!(
                        "Failed to create/truncate staging file {}: {}",
                        stage_path.display(),
                        e
                    );
                    libc::EIO
                })?;
                let now_ts = chrono::Utc::now().timestamp();
                self.cache
                    .update_local_write(ino, 0, now_ts, true)
                    .map_err(|e| e.to_errno())?;
                self.chunk_buffer.evict(ino);
            } else if !self.staged_files.contains_key(&ino)
                && !has_dirty_stage_on_disk
                && file.size == 0
                && !file.is_pending_upload()
            {
                create_truncated_stage_file(&stage_path).map_err(|_| libc::EIO)?;
            }

            let entry = self
                .staged_files
                .entry(ino)
                .or_insert_with(|| StagedFileState {
                    ino,
                    path: stage_path,
                    dirty: truncate || file.dirty || file.is_pending_upload(),
                    populated_from_remote: truncate
                        || has_dirty_stage_on_disk
                        || file.size == 0
                        || file.is_pending_upload(),
                    open_handles: HashSet::new(),
                });
            if truncate || has_dirty_stage_on_disk {
                entry.dirty |= truncate || file.dirty || file.is_pending_upload();
                entry.populated_from_remote = true;
            }
            entry.open_handles.insert(fh);
        }

        self.track_open_handle(ino, fh, flags);
        Ok(fh)
    }

    /// Ensures the local staging file for `file` contains its existing remote bytes if a partial write or
    /// non-zero truncation occurs before the staging file has been populated.
    fn ensure_staging_populated(
        &mut self,
        file: &CachedFile,
        overwrite_all_from_zero: bool,
    ) -> Result<PathBuf, libc::c_int> {
        if !self.staged_files.contains_key(&file.inode)
            && self.staged_files.len() >= MAX_STAGED_WRITE_FILES
        {
            return Err(libc::ENOSPC);
        }

        let stage_path = self.staging_file_path(file.inode);
        if let Some(parent_dir) = stage_path.parent() {
            ensure_private_staging_dir(parent_dir).map_err(|_| libc::EIO)?;
        }

        let has_dirty_stage_on_disk =
            (file.dirty || file.is_pending_upload()) && stage_path.exists();
        let already_populated = self
            .staged_files
            .get(&file.inode)
            .is_some_and(|st| st.populated_from_remote);

        if !already_populated {
            if has_dirty_stage_on_disk && !overwrite_all_from_zero {
                // Treat an existing local staging file for a dirty/pending inode as authoritative
                // (e.g. after a restart or 410-token cache recovery) rather than overwriting it.
                tighten_stage_file_permissions(&stage_path);
            } else if !overwrite_all_from_zero && file.size > 0 && !file.is_pending_upload() {
                let downloaded = self
                    .rt_handle
                    .block_on(
                        self.api
                            .download_range(&file.drive_id, 0, file.size.saturating_sub(1)),
                    )
                    .map_err(|e| {
                        log::error!(
                            "Failed to pre-populate staging file from Drive for inode {} (drive_id={}): {}",
                            file.inode,
                            file.drive_id,
                            e
                        );
                        e.to_errno()
                    })?;

                let mut f = create_truncated_stage_file(&stage_path).map_err(|_| libc::EIO)?;
                f.write_all(&downloaded).map_err(|_| libc::EIO)?;
                f.flush().map_err(|_| libc::EIO)?;
            } else {
                create_truncated_stage_file(&stage_path).map_err(|_| libc::EIO)?;
            }
        } else if !stage_path.exists() {
            create_truncated_stage_file(&stage_path).map_err(|_| libc::EIO)?;
        } else {
            tighten_stage_file_permissions(&stage_path);
        }

        let entry = self
            .staged_files
            .entry(file.inode)
            .or_insert_with(|| StagedFileState {
                ino: file.inode,
                path: stage_path.clone(),
                dirty: file.dirty || file.is_pending_upload(),
                populated_from_remote: true,
                open_handles: HashSet::new(),
            });
        if has_dirty_stage_on_disk {
            entry.dirty = true;
        }
        entry.populated_from_remote = true;

        Ok(stage_path)
    }

    fn discard_staged_file(&mut self, ino: u64) {
        if let Some(st) = self.staged_files.remove(&ino) {
            let _ = std::fs::remove_file(&st.path);
        } else {
            let _ = std::fs::remove_file(self.staging_file_path(ino));
        }
    }

    /// Creates and opens a file in `parent` (`create` FUSE operation), staging it locally in `~/.cache/gdrive-fuse-rs/staging/`
    /// and marking its inode `dirty` for subsequent Resumable Upload on `flush`/`release`.
    pub fn handle_create(
        &mut self,
        parent: u64,
        name: &str,
        _mode: u32,
        flags: i32,
    ) -> Result<(FileAttr, u64), libc::c_int> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        if parent == SHARED_WITH_ME_INODE
            || (parent == FUSE_ROOT_ID && self.shared_with_me && name == SHARED_WITH_ME_NAME)
        {
            return Err(libc::EACCES);
        }
        if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\0')
        {
            return Err(libc::EINVAL);
        }

        self.ensure_dir_synced(parent)?;

        if let Some(existing) = self.find_child_by_display_name(parent, name)? {
            if (flags & libc::O_EXCL) != 0 {
                return Err(libc::EEXIST);
            }
            let open_flags = flags & !(libc::O_CREAT | libc::O_EXCL);
            let fh = self.handle_open_with_fh(existing.inode, open_flags)?;
            let attr = self.handle_getattr(existing.inode)?;
            return Ok((attr, fh));
        }

        if self.staged_files.len() >= MAX_STAGED_WRITE_FILES {
            return Err(libc::ENOSPC);
        }

        let now_ts = chrono::Utc::now().timestamp();
        let mime_type = guess_mime_type(name);
        let created_entry = self
            .cache
            .create_pending_file(parent, name, mime_type, now_ts)
            .map_err(|e| {
                log::error!(
                    "Failed to allocate pending file '{}' in cache under parent {}: {}",
                    name,
                    parent,
                    e
                );
                e.to_errno()
            })?;

        let ino = created_entry.inode;
        let stage_path = self.staging_file_path(ino);
        if let Some(parent_dir) = stage_path.parent() {
            if let Err(e) = ensure_private_staging_dir(parent_dir) {
                log::error!(
                    "Failed to create staging directory {}: {}",
                    parent_dir.display(),
                    e
                );
                let _ = self.cache.remove_file(ino);
                return Err(libc::EIO);
            }
        }

        if let Err(e) = create_truncated_stage_file(&stage_path) {
            log::error!(
                "Failed to create local staging file {}: {}",
                stage_path.display(),
                e
            );
            let _ = self.cache.remove_file(ino);
            return Err(libc::EIO);
        }

        let fh = self.alloc_fh();
        let mut open_handles = HashSet::new();
        open_handles.insert(fh);
        self.staged_files.insert(
            ino,
            StagedFileState {
                ino,
                path: stage_path,
                dirty: true,
                populated_from_remote: true,
                open_handles,
            },
        );
        let create_open_flags = if (flags & libc::O_ACCMODE) == libc::O_RDONLY {
            (flags & !libc::O_ACCMODE) | libc::O_WRONLY
        } else {
            flags
        };
        self.track_open_handle(ino, fh, create_open_flags);

        Ok((self.file_to_attr(&created_entry.file), fh))
    }

    /// Writes `data` at `offset` to the local staging file for `ino`, marks the inode `dirty`,
    /// and updates cached `size` and `mtime` in SQLite.
    pub fn handle_write(
        &mut self,
        ino: u64,
        _fh: u64,
        offset: u64,
        data: &[u8],
        flags: i32,
    ) -> Result<u32, libc::c_int> {
        if self.read_only {
            return Err(libc::EROFS);
        }

        let file = self
            .cache
            .get_by_inode(ino)
            .map_err(|e| e.to_errno())?
            .ok_or(libc::ENOENT)?;

        if file.is_dir {
            return Err(libc::EISDIR);
        }
        if file.is_google_native_doc() {
            return Err(libc::EACCES);
        }

        let overwrite_all = offset == 0 && (data.len() as u64) >= file.size && (flags & libc::O_APPEND) == 0;
        let stage_path = self.ensure_staging_populated(&file, overwrite_all)?;

        let mut stage_file = open_existing_stage_file_rw(&stage_path).map_err(|e| {
            log::error!(
                "Failed to open staging file {} for write: {}",
                stage_path.display(),
                e
            );
            libc::EIO
        })?;

        if (flags & libc::O_APPEND) != 0 {
            stage_file.seek(SeekFrom::End(0)).map_err(|_| libc::EIO)?;
        } else {
            stage_file
                .seek(SeekFrom::Start(offset))
                .map_err(|_| libc::EIO)?;
        }

        stage_file.write_all(data).map_err(|e| {
            log::error!(
                "Failed to write {} bytes to staging file {}: {}",
                data.len(),
                stage_path.display(),
                e
            );
            libc::EIO
        })?;

        let new_size = stage_file.metadata().map_err(|_| libc::EIO)?.len();
        let now_ts = chrono::Utc::now().timestamp();

        if let Some(st) = self.staged_files.get_mut(&ino) {
            st.dirty = true;
            st.populated_from_remote = true;
        }

        self.chunk_buffer.evict(ino);
        self.cache
            .update_local_write(ino, new_size, now_ts, true)
            .map_err(|e| e.to_errno())?;

        Ok(data.len() as u32)
    }

    /// Uploads a dirty staged file to Google Drive via the Resumable Upload API (`POST` for newly created files,
    /// `PATCH` for existing files), updates SQLite metadata with the returned Drive `id` and `modifiedTime`,
    /// and deletes the local staging file immediately upon upload completion.
    fn upload_staged_if_dirty(
        &mut self,
        ino: u64,
        remove_state_when_clean: bool,
    ) -> Result<(), libc::c_int> {
        if !self.staged_files.contains_key(&ino) {
            if let Ok(Some(file)) = self.cache.get_by_inode(ino) {
                let stage_path = self.staging_file_path(ino);
                if (file.dirty || file.is_pending_upload()) && stage_path.exists() {
                    tighten_stage_file_permissions(&stage_path);
                    self.staged_files.insert(
                        ino,
                        StagedFileState {
                            ino,
                            path: stage_path,
                            dirty: true,
                            populated_from_remote: true,
                            open_handles: HashSet::new(),
                        },
                    );
                }
            }
        }

        let has_open_handles = self.has_open_handles(ino);
        let (is_dirty, stage_path) = match self.staged_files.get(&ino) {
            Some(st) => (st.dirty, st.path.clone()),
            None => return Ok(()),
        };

        if is_dirty {
            let Some(file) = self.cache.get_by_inode(ino).map_err(|e| e.to_errno())? else {
                self.discard_staged_file(ino);
                return Ok(());
            };

            if !stage_path.exists() {
                create_truncated_stage_file(&stage_path).map_err(|_| libc::EIO)?;
            } else {
                tighten_stage_file_permissions(&stage_path);
            }

            let uploaded_res = if file.is_pending_upload() {
                let parent_ino = self.cache.get_parent_inode(ino).map_err(|e| e.to_errno())?;
                let parent_file = self
                    .cache
                    .get_by_inode(parent_ino)
                    .map_err(|e| e.to_errno())?
                    .ok_or(libc::ENOENT)?;

                self.rt_handle
                    .block_on(self.api.resumable_upload_new_file(
                        &parent_file.drive_id,
                        &file.name,
                        &file.mime_type,
                        Some(file.modified_time),
                        &stage_path,
                    ))
            } else {
                self.rt_handle
                    .block_on(self.api.resumable_upload_existing_file(
                        &file.drive_id,
                        &file.mime_type,
                        Some(file.modified_time),
                        &stage_path,
                    ))
            };

            let uploaded_drive_file = uploaded_res.map_err(|api_err| {
                log::error!(
                    "Resumable upload failed for inode {} (name='{}', drive_id='{}'): {}",
                    ino,
                    file.name,
                    file.drive_id,
                    api_err
                );
                api_err.to_errno()
            })?;

            self.cache
                .complete_file_upload(ino, &uploaded_drive_file)
                .map_err(|cache_err| {
                    log::error!(
                        "Failed to update SQLite cache after resumable upload for inode {}: {}",
                        ino,
                        cache_err
                    );
                    cache_err.to_errno()
                })?;

            self.chunk_buffer.evict(ino);
            self.quota_cache = None;

            // Delete the local staging file immediately upon upload completion.
            let _ = std::fs::remove_file(&stage_path);

            if let Some(st) = self.staged_files.get_mut(&ino) {
                st.dirty = false;
                st.populated_from_remote = false;
            }
        }

        if remove_state_when_clean && !has_open_handles {
            let _ = std::fs::remove_file(&stage_path);
            self.staged_files.remove(&ino);
        }

        Ok(())
    }

    /// Flushes any dirty staged writes for `ino` to Google Drive via Resumable Upload.
    pub fn handle_flush(&mut self, ino: u64, fh: u64) -> Result<(), libc::c_int> {
        if self.unlinked_inodes.contains(&ino) {
            return Ok(());
        }
        if let Some(flags) = self.open_handles.get(&ino).and_then(|h| h.get(&fh)) {
            if (*flags & WRITE_OPEN_FLAGS) == 0 {
                return Ok(());
            }
        }
        self.upload_staged_if_dirty(ino, false)
    }

    /// Releases an open file handle `fh` on `ino`, uploading any remaining dirty staged data
    /// and cleaning up the local staging file and read-ahead buffer.
    pub fn handle_release(&mut self, ino: u64, fh: u64) -> Result<(), libc::c_int> {
        let was_writable = self.untrack_open_handle(ino, fh);
        let still_open = self.has_open_handles(ino);
        if self.unlinked_inodes.contains(&ino) {
            if !still_open {
                self.unlinked_inodes.remove(&ino);
                self.discard_staged_file(ino);
                self.chunk_buffer.evict(ino);
                let _ = self.cache.remove_file(ino);
            }
            return Ok(());
        }
        if !was_writable || still_open {
            if !still_open && !self.staged_files.contains_key(&ino) {
                self.chunk_buffer.evict(ino);
            }
            return Ok(());
        }
        let res = self.upload_staged_if_dirty(ino, true);
        self.chunk_buffer.evict(ino);
        res
    }

    /// Updates file attributes (`size` truncation and/or `mtime` modification) on `ino`.
    pub fn handle_setattr(
        &mut self,
        ino: u64,
        size: Option<u64>,
        mtime: Option<SystemTime>,
        fh: Option<u64>,
    ) -> Result<FileAttr, libc::c_int> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        if ino == SHARED_WITH_ME_INODE {
            return Err(libc::EACCES);
        }

        let file = self
            .cache
            .get_by_inode(ino)
            .map_err(|e| e.to_errno())?
            .ok_or(libc::ENOENT)?;

        let target_mtime = match mtime {
            Some(sys_time) => system_time_to_unix_secs(sys_time),
            None if size.is_some() => chrono::Utc::now().timestamp(),
            None => file.modified_time,
        };

        if let Some(new_size) = size {
            if file.is_dir {
                return Err(libc::EISDIR);
            }
            if file.is_google_native_doc() {
                return Err(libc::EACCES);
            }
            if let Some(handle_id) = fh {
                if !self.is_handle_writable(ino, handle_id) {
                    return Err(libc::EINVAL);
                }
            }

            let stage_path = self.ensure_staging_populated(&file, new_size == 0)?;
            let stage_file = open_existing_stage_file_rw(&stage_path).map_err(|_| libc::EIO)?;
            stage_file.set_len(new_size).map_err(|_| libc::EIO)?;

            if let Some(st) = self.staged_files.get_mut(&ino) {
                st.dirty = true;
                st.populated_from_remote = true;
            }
            self.chunk_buffer.evict(ino);

            self.cache
                .update_local_write(ino, new_size, target_mtime, true)
                .map_err(|e| e.to_errno())?;

            let has_open_handles = self.has_open_handles(ino);
            if fh.is_none() && !has_open_handles && !self.unlinked_inodes.contains(&ino) {
                self.upload_staged_if_dirty(ino, true)?;
            }
        } else if mtime.is_some() {
            let is_dirty_or_pending = file.dirty
                || file.is_pending_upload()
                || self.staged_files.get(&ino).is_some_and(|st| st.dirty);
            if is_dirty_or_pending {
                self.cache
                    .update_local_write(ino, file.size, target_mtime, file.dirty)
                    .map_err(|e| e.to_errno())?;
            } else if ino > SHARED_WITH_ME_INODE {
                let updated = self
                    .rt_handle
                    .block_on(self.api.update_file_metadata(
                        &file.drive_id,
                        None,
                        Some(target_mtime),
                        None,
                        None,
                    ))
                    .map_err(|e| {
                        log::error!(
                            "Failed to update remote mtime for inode {} (drive_id={}): {}",
                            ino,
                            file.drive_id,
                            e
                        );
                        e.to_errno()
                    })?;
                self.cache.upsert_file(&updated).map_err(|e| e.to_errno())?;
            }
        }

        self.handle_getattr(ino)
    }

    /// Creates a new remote Google Drive folder (`mimeType = "application/vnd.google-apps.folder"`)
    /// inside `parent` and inserts it into the SQLite cache.
    pub fn handle_mkdir(
        &mut self,
        parent: u64,
        name: &str,
        _mode: u32,
    ) -> Result<FileAttr, libc::c_int> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        if parent == SHARED_WITH_ME_INODE
            || (parent == FUSE_ROOT_ID && self.shared_with_me && name == SHARED_WITH_ME_NAME)
        {
            return Err(libc::EACCES);
        }
        if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\0')
        {
            return Err(libc::EINVAL);
        }

        let parent_file = self.ensure_dir_synced(parent)?;
        if self.find_child_by_display_name(parent, name)?.is_some() {
            return Err(libc::EEXIST);
        }

        let created_folder = self
            .rt_handle
            .block_on(self.api.create_folder(&parent_file.drive_id, name))
            .map_err(|e| {
                log::error!(
                    "Failed to create folder '{}' under parent {} (drive_id={}): {}",
                    name,
                    parent,
                    parent_file.drive_id,
                    e
                );
                e.to_errno()
            })?;

        let entry = self
            .cache
            .insert_child_file(parent, name, &created_folder)
            .map_err(|e| {
                log::error!(
                    "Failed to insert created folder '{}' into SQLite cache: {}",
                    name,
                    e
                );
                e.to_errno()
            })?;

        Ok(self.file_to_attr(&entry.file))
    }

    /// Moves a file in `parent` to Google Drive Trash (`{"trashed": true}`) rather than permanently deleting it,
    /// and removes the entry from the SQLite cache and local staging directory (deferring inode/staging removal
    /// until the final `release` if the file still has open handles).
    pub fn handle_unlink(&mut self, parent: u64, name: &str) -> Result<(), libc::c_int> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        if parent == SHARED_WITH_ME_INODE {
            return Err(libc::EACCES);
        }
        if parent == FUSE_ROOT_ID && self.shared_with_me && name == SHARED_WITH_ME_NAME {
            return Err(libc::EISDIR);
        }

        self.ensure_dir_synced(parent)?;
        let child = self
            .find_child_by_display_name(parent, name)?
            .ok_or(libc::ENOENT)?;

        if child.is_dir {
            return Err(libc::EISDIR);
        }

        if !child.is_pending_upload() {
            self.rt_handle
                .block_on(self.api.trash_file(&child.drive_id))
                .map_err(|e| {
                    log::error!(
                        "Failed to move file '{}' (inode={}, drive_id={}) to Google Drive Trash: {}",
                        name,
                        child.inode,
                        child.drive_id,
                        e
                    );
                    e.to_errno()
                })?;
        }

        self.quota_cache = None;
        if self.has_open_handles(child.inode) {
            self.unlinked_inodes.insert(child.inode);
            self.cache
                .unlink_child_entry(parent, child.inode)
                .map_err(|e| e.to_errno())?;
        } else {
            self.discard_staged_file(child.inode);
            self.chunk_buffer.evict(child.inode);
            self.cache
                .remove_file(child.inode)
                .map_err(|e| e.to_errno())?;
        }
        Ok(())
    }

    /// Moves an empty folder in `parent` to Google Drive Trash (`{"trashed": true}`) and removes it from SQLite.
    pub fn handle_rmdir(&mut self, parent: u64, name: &str) -> Result<(), libc::c_int> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        if parent == SHARED_WITH_ME_INODE
            || (parent == FUSE_ROOT_ID && self.shared_with_me && name == SHARED_WITH_ME_NAME)
        {
            return Err(libc::EACCES);
        }

        self.ensure_dir_synced(parent)?;
        let child = self
            .find_child_by_display_name(parent, name)?
            .ok_or(libc::ENOENT)?;

        if !child.is_dir {
            return Err(libc::ENOTDIR);
        }

        self.ensure_dir_synced(child.inode)?;
        let grandchildren = self
            .cache
            .list_children(child.inode)
            .map_err(|e| e.to_errno())?;
        if !grandchildren.is_empty() {
            return Err(libc::ENOTEMPTY);
        }

        if !child.is_pending_upload() {
            self.rt_handle
                .block_on(self.api.trash_file(&child.drive_id))
                .map_err(|e| {
                    log::error!(
                        "Failed to move folder '{}' (inode={}, drive_id={}) to Google Drive Trash: {}",
                        name,
                        child.inode,
                        child.drive_id,
                        e
                    );
                    e.to_errno()
                })?;
        }

        self.cache
            .remove_file(child.inode)
            .map_err(|e| e.to_errno())?;
        Ok(())
    }

    /// Renames a file or folder in-directory (`PATCH name`) or moves it across directories
    /// (`addParents` / `removeParents`) atomically in both Google Drive API and SQLite `parent_child`.
    pub fn handle_rename(
        &mut self,
        parent: u64,
        name: &str,
        newparent: u64,
        newname: &str,
        flags: u32,
    ) -> Result<(), libc::c_int> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        if parent == SHARED_WITH_ME_INODE || newparent == SHARED_WITH_ME_INODE {
            return Err(libc::EACCES);
        }
        if (parent == FUSE_ROOT_ID && self.shared_with_me && name == SHARED_WITH_ME_NAME)
            || (newparent == FUSE_ROOT_ID && self.shared_with_me && newname == SHARED_WITH_ME_NAME)
        {
            return Err(libc::EACCES);
        }
        if name == "."
            || name == ".."
            || newname.is_empty()
            || newname == "."
            || newname == ".."
            || newname.contains('/')
            || newname.contains('\0')
        {
            return Err(libc::EINVAL);
        }
        if (flags & libc::RENAME_EXCHANGE) != 0 {
            return Err(libc::EINVAL);
        }

        let old_parent_file = self.ensure_dir_synced(parent)?;
        let new_parent_file = self.ensure_dir_synced(newparent)?;

        let src_file = self
            .find_child_by_display_name(parent, name)?
            .ok_or(libc::ENOENT)?;

        if parent == newparent && name == newname {
            return Ok(());
        }

        // Prevent moving a directory into itself or any of its own descendants (`EINVAL`).
        if src_file.is_dir && parent != newparent {
            let mut cur = newparent;
            for _ in 0..256 {
                if cur == src_file.inode {
                    return Err(libc::EINVAL);
                }
                if cur <= SHARED_WITH_ME_INODE {
                    break;
                }
                let next_parent = self.cache.get_parent_inode(cur).map_err(|e| e.to_errno())?;
                if next_parent == cur {
                    break;
                }
                cur = next_parent;
            }
        }

        let mut overwrite_dst: Option<CachedFile> = None;
        if let Some(dst_file) = self.find_child_by_display_name(newparent, newname)? {
            if dst_file.inode == src_file.inode {
                return Ok(());
            }
            if (flags & libc::RENAME_NOREPLACE) != 0 {
                return Err(libc::EEXIST);
            }
            if src_file.is_dir && !dst_file.is_dir {
                return Err(libc::ENOTDIR);
            }
            if !src_file.is_dir && dst_file.is_dir {
                return Err(libc::EISDIR);
            }
            if dst_file.is_dir {
                self.ensure_dir_synced(dst_file.inode)?;
                if !self
                    .cache
                    .list_children(dst_file.inode)
                    .map_err(|e| e.to_errno())?
                    .is_empty()
                {
                    return Err(libc::ENOTEMPTY);
                }
            }

            // Native Google Workspace documents coexist with regular files via display-name
            // disambiguation in `rename_entry` / `disambiguated_children`; do not trash them.
            if !dst_file.is_google_native_doc() {
                overwrite_dst = Some(dst_file);
            }
        }

        // For Google Workspace native documents, strip the synthesized virtual extension
        // (e.g. `.gdoc`, `.desktop`, `.pdf`, `.docx`) if the original Drive title did not include it.
        let effective_target_name = if src_file.is_google_native_doc() {
            let ext = Self::google_doc_extension(&src_file.mime_type, self.google_docs_mode);
            if !ext.is_empty() && !src_file.name.ends_with(ext) {
                newname
                    .strip_suffix(ext)
                    .filter(|stem| !stem.is_empty())
                    .unwrap_or(newname)
            } else {
                newname
            }
        } else {
            newname
        };

        // 1. Perform the source rename/move in Google Drive FIRST before trashing any overwritten destination,
        // so that if the source update fails, the pre-existing destination remains untouched.
        let updated_drive_file = if !src_file.is_pending_upload() {
            let (add_parents, remove_parents) = if parent != newparent {
                (
                    Some(new_parent_file.drive_id.as_str()),
                    Some(old_parent_file.drive_id.as_str()),
                )
            } else {
                (None, None)
            };

            let updated = self
                .rt_handle
                .block_on(self.api.update_file_metadata(
                    &src_file.drive_id,
                    Some(effective_target_name),
                    None,
                    add_parents,
                    remove_parents,
                ))
                .map_err(|e| {
                    log::error!(
                        "Failed to rename/move inode {} (drive_id={}) to '{}': {}",
                        src_file.inode,
                        src_file.drive_id,
                        effective_target_name,
                        e
                    );
                    e.to_errno()
                })?;
            Some(updated)
        } else {
            None
        };

        // 2. Now that the source operation succeeded, trash and remove any overwritten non-native destination.
        if let Some(dst_file) = overwrite_dst {
            if !dst_file.is_pending_upload() {
                if let Err(trash_err) = self
                    .rt_handle
                    .block_on(self.api.trash_file(&dst_file.drive_id))
                {
                    log::error!(
                        "Failed to move overwritten destination '{}' (inode={}, drive_id={}) to Trash during rename: {}",
                        newname,
                        dst_file.inode,
                        dst_file.drive_id,
                        trash_err
                    );
                    if !src_file.is_pending_upload() {
                        let (rollback_add, rollback_remove) = if parent != newparent {
                            (
                                Some(old_parent_file.drive_id.as_str()),
                                Some(new_parent_file.drive_id.as_str()),
                            )
                        } else {
                            (None, None)
                        };
                        let _ = self.rt_handle.block_on(self.api.update_file_metadata(
                            &src_file.drive_id,
                            Some(&src_file.name),
                            None,
                            rollback_add,
                            rollback_remove,
                        ));
                    }
                    return Err(trash_err.to_errno());
                }
            }

            if self.has_open_handles(dst_file.inode) {
                self.unlinked_inodes.insert(dst_file.inode);
                self.cache
                    .unlink_child_entry(newparent, dst_file.inode)
                    .map_err(|e| e.to_errno())?;
            } else {
                self.discard_staged_file(dst_file.inode);
                self.chunk_buffer.evict(dst_file.inode);
                self.cache
                    .remove_file(dst_file.inode)
                    .map_err(|e| e.to_errno())?;
            }
        }

        // 3. Commit the rename/move in SQLite.
        self.cache
            .rename_entry(
                parent,
                src_file.inode,
                newparent,
                effective_target_name,
                updated_drive_file.as_ref(),
            )
            .map_err(|e| e.to_errno())?;

        Ok(())
    }

    /// Reads up to `size` bytes from `ino` starting at `offset`, serving from the local staging file
    /// if the file is actively staged for writing, or from the 4 MB read-ahead chunk buffer otherwise.
    pub fn handle_read(&mut self, ino: u64, offset: u64, size: u32) -> Result<Bytes, libc::c_int> {
        self.drain_cache_evictions();
        let file = match self.cache.get_by_inode(ino) {
            Ok(Some(file)) => file,
            Ok(None) => return Err(libc::ENOENT),
            Err(e) => {
                log::error!("Cache error in read for inode {}: {}", ino, e);
                return Err(e.to_errno());
            }
        };

        if file.is_dir {
            return Err(libc::EISDIR);
        }

        if !self.staged_files.contains_key(&ino) && (file.dirty || file.is_pending_upload()) {
            let stage_path = self.staging_file_path(ino);
            if stage_path.exists() {
                tighten_stage_file_permissions(&stage_path);
                self.staged_files.insert(
                    ino,
                    StagedFileState {
                        ino,
                        path: stage_path,
                        dirty: true,
                        populated_from_remote: true,
                        open_handles: HashSet::new(),
                    },
                );
            }
        }

        // If this inode is actively staged on disk with local data, read directly from the staging file.
        if let Some(st) = self.staged_files.get(&ino) {
            if st.populated_from_remote && st.path.exists() {
                let mut stage_file = File::open(&st.path).map_err(|_| libc::EIO)?;
                let stage_len = stage_file.metadata().map_err(|_| libc::EIO)?.len();
                if size == 0 || offset >= stage_len {
                    return Ok(Bytes::new());
                }
                let to_read = std::cmp::min(size as u64, stage_len - offset) as usize;
                stage_file
                    .seek(SeekFrom::Start(offset))
                    .map_err(|_| libc::EIO)?;
                let mut buf = vec![0u8; to_read];
                stage_file.read_exact(&mut buf).map_err(|_| libc::EIO)?;
                return Ok(Bytes::from(buf));
            }
        }

        if file.is_google_native_doc() {
            let doc_bytes = match self.google_docs_mode {
                GoogleDocsMode::Browser | GoogleDocsMode::Desktop => {
                    Self::synthesize_google_doc_shortcut(&file, self.google_docs_mode)
                }
                GoogleDocsMode::Pdf | GoogleDocsMode::Office => {
                    self.ensure_google_doc_exported(&file)?
                }
            };

            if size == 0 || offset >= doc_bytes.len() as u64 {
                return Ok(Bytes::new());
            }
            let start = offset as usize;
            let end = std::cmp::min(start.saturating_add(size as usize), doc_bytes.len());
            return Ok(doc_bytes.slice(start..end));
        }

        if file.size == 0 || offset >= file.size || size == 0 {
            return Ok(Bytes::new());
        }

        let remaining = file.size - offset;
        let expected_len = std::cmp::min(size as u64, remaining) as usize;

        if let Some(cached_slice) = self.chunk_buffer.get_slice_for_mtime(
            ino,
            offset,
            expected_len,
            file.modified_time,
        ) {
            log::trace!(
                "Read-ahead buffer HIT for inode {} at offset {} (len={})",
                ino,
                offset,
                expected_len
            );
            return Ok(cached_slice);
        }

        let fetch_len = std::cmp::max(size as usize, self.chunk_size);
        let chunk_end = std::cmp::min(
            offset.saturating_add(fetch_len as u64).saturating_sub(1),
            file.size.saturating_sub(1),
        );

        log::debug!(
            "Read-ahead buffer MISS for inode {} (drive_id={}): fetching bytes {}-{} (file size={})",
            ino,
            file.drive_id,
            offset,
            chunk_end,
            file.size
        );

        let downloaded = self
            .rt_handle
            .block_on(self.api.download_range(&file.drive_id, offset, chunk_end))
            .map_err(|api_err| {
                log::error!(
                    "Failed to download range {}-{} for inode {} (drive_id={}): {}",
                    offset,
                    chunk_end,
                    ino,
                    file.drive_id,
                    api_err
                );
                api_err.to_errno()
            })?;

        let actual_len = std::cmp::min(expected_len, downloaded.len());
        let result_slice = downloaded.slice(0..actual_len);
        self.chunk_buffer
            .insert_with_mtime(ino, offset, file.modified_time, downloaded);

        Ok(result_slice)
    }

    /// Resolves `statfs` storage quota metrics from Google Drive's `about.get` endpoint,
    /// caching the result in memory for `QUOTA_CACHE_TTL` (5 minutes).
    ///
    /// For unlimited Google Workspace accounts (`quota.limit == 0`), `blocks`, `bfree`, and
    /// `bavail` are reported as `0` (standard POSIX `statfs` sentinel for unbounded/unknown
    /// filesystem capacity) rather than fabricating an arbitrary finite quota.
    pub fn handle_statfs(&mut self) -> Result<StatfsSnapshot, libc::c_int> {
        let quota = match self.quota_cache {
            Some((cached_quota, fetched_at)) if fetched_at.elapsed() < QUOTA_CACHE_TTL => {
                cached_quota
            }
            _ => {
                let fresh = self
                    .rt_handle
                    .block_on(self.api.get_storage_quota())
                    .map_err(|api_err| {
                        log::warn!("Failed to query Google Drive storage quota: {}", api_err);
                        api_err.to_errno()
                    })?;
                self.quota_cache = Some((fresh, Instant::now()));
                fresh
            }
        };

        const STATFS_BSIZE: u64 = 4096;
        let used_blocks = quota.usage.div_ceil(STATFS_BSIZE);
        let (total_blocks, free_blocks) = if quota.limit > 0 {
            let total = quota.limit.div_ceil(STATFS_BSIZE).max(used_blocks);
            let free = total.saturating_sub(used_blocks);
            (total, free)
        } else {
            // Unlimited Google Workspace account (`limit == 0`): report 0 (unbounded quota sentinel)
            // rather than fabricating an arbitrary finite capacity.
            (0, 0)
        };

        Ok(StatfsSnapshot {
            blocks: total_blocks,
            bfree: free_blocks,
            bavail: free_blocks,
            bsize: STATFS_BSIZE as u32,
            namelen: 255,
            frsize: STATFS_BSIZE as u32,
        })
    }
}

fn unix_secs_to_system_time(timestamp_secs: i64) -> SystemTime {
    if timestamp_secs >= 0 {
        UNIX_EPOCH
            .checked_add(Duration::from_secs(timestamp_secs as u64))
            .unwrap_or(UNIX_EPOCH)
    } else {
        UNIX_EPOCH
            .checked_sub(Duration::from_secs(timestamp_secs.unsigned_abs()))
            .unwrap_or(UNIX_EPOCH)
    }
}

fn system_time_to_unix_secs(sys_time: SystemTime) -> i64 {
    match sys_time.duration_since(UNIX_EPOCH) {
        Ok(dur) => dur.as_secs() as i64,
        Err(err) => -(err.duration().as_secs() as i64),
    }
}

fn time_or_now_to_system_time(time_or_now: TimeOrNow) -> SystemTime {
    match time_or_now {
        TimeOrNow::SpecificTime(st) => st,
        TimeOrNow::Now => SystemTime::now(),
    }
}

impl Filesystem for GDriveFS {
    fn init(
        &mut self,
        _req: &Request<'_>,
        _config: &mut KernelConfig,
    ) -> Result<(), libc::c_int> {
        log::info!(
            "Initializing gdrive-fuse-rs FUSE filesystem (uid={}, gid={}, chunk_size={} bytes, google_docs_mode={:?}, shared_with_me={}, read_only={}, staging_dir={})",
            self.uid,
            self.gid,
            self.chunk_size,
            self.google_docs_mode,
            self.shared_with_me,
            self.read_only,
            self.staging_dir.display()
        );
        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let Some(name_str) = name.to_str() else {
            log::warn!("Rejecting non-UTF8 filename in lookup under parent {}", parent);
            reply.error(libc::ENOENT);
            return;
        };

        match self.handle_lookup(parent, name_str) {
            Ok(attr) => reply.entry(&self.attr_ttl, &attr, 0),
            Err(errno) => reply.error(errno),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match self.handle_getattr(ino) {
            Ok(attr) => reply.attr(&self.attr_ttl, &attr),
            Err(errno) => reply.error(errno),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }

        match self.handle_readdir(ino) {
            Ok(entries) => {
                for (idx, (child_ino, kind, name)) in
                    entries.into_iter().enumerate().skip(offset as usize)
                {
                    let next_offset = (idx + 1) as i64;
                    if reply.add(child_ino, next_offset, kind, name) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(errno) => reply.error(errno),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        match self.handle_open_with_fh(ino, flags) {
            Ok(fh) => reply.opened(fh, 0),
            Err(errno) => reply.error(errno),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }

        match self.handle_read(ino, offset as u64, size) {
            Ok(bytes) => reply.data(&bytes),
            Err(errno) => reply.error(errno),
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        match self.handle_flush(ino, fh) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.handle_flush(ino, fh) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.handle_release(ino, fh) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        if (flags & WRITE_OPEN_FLAGS) != 0 {
            reply.error(libc::EROFS);
            return;
        }
        match self.cache.get_by_inode(ino) {
            Ok(Some(file)) if file.is_dir => reply.opened(0, 0),
            Ok(Some(_)) => reply.error(libc::ENOTDIR),
            Ok(None) => reply.error(libc::ENOENT),
            Err(e) => {
                log::error!("Cache error in opendir for inode {}: {}", ino, e);
                reply.error(e.to_errno());
            }
        }
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        match self.handle_statfs() {
            Ok(snap) => reply.statfs(
                snap.blocks,
                snap.bfree,
                snap.bavail,
                0,
                0,
                snap.bsize,
                snap.namelen,
                snap.frsize,
            ),
            Err(_) => reply.statfs(0, 0, 0, 0, 0, 4096, 255, 4096),
        }
    }

    fn access(&mut self, _req: &Request<'_>, ino: u64, mask: i32, reply: ReplyEmpty) {
        match self.handle_access(ino, mask) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let mtime_sys = mtime.map(time_or_now_to_system_time);
        match self.handle_setattr(ino, size, mtime_sys, fh) {
            Ok(attr) => reply.attr(&self.attr_ttl, &attr),
            Err(errno) => reply.error(errno),
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let Some(name_str) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };
        match self.handle_mkdir(parent, name_str, mode) {
            Ok(attr) => reply.entry(&self.attr_ttl, &attr, 0),
            Err(errno) => reply.error(errno),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let Some(name_str) = name.to_str() else {
            reply.error(libc::ENOENT);
            return;
        };
        match self.handle_unlink(parent, name_str) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let Some(name_str) = name.to_str() else {
            reply.error(libc::ENOENT);
            return;
        };
        match self.handle_rmdir(parent, name_str) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    fn symlink(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        _link_name: &OsStr,
        _target: &Path,
        reply: ReplyEntry,
    ) {
        log::debug!("Rejecting symlink under parent {} (unsupported)", parent);
        reply.error(if self.read_only {
            libc::EROFS
        } else {
            libc::EOPNOTSUPP
        });
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        let (Some(name_str), Some(newname_str)) = (name.to_str(), newname.to_str()) else {
            reply.error(libc::EINVAL);
            return;
        };
        match self.handle_rename(parent, name_str, newparent, newname_str, flags) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _newparent: u64,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        log::debug!("Rejecting link for inode {} (unsupported)", ino);
        reply.error(if self.read_only {
            libc::EROFS
        } else {
            libc::EOPNOTSUPP
        });
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        match self.handle_write(ino, fh, offset as u64, data, flags) {
            Ok(written) => reply.written(written),
            Err(errno) => reply.error(errno),
        }
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let Some(name_str) = name.to_str() else {
            reply.error(libc::EINVAL);
            return;
        };
        match self.handle_create(parent, name_str, mode, flags) {
            Ok((attr, fh)) => reply.created(&self.attr_ttl, &attr, 0, fh, 0),
            Err(errno) => reply.error(errno),
        }
    }

    fn fallocate(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _offset: i64,
        _length: i64,
        _mode: i32,
        reply: ReplyEmpty,
    ) {
        log::debug!("Rejecting fallocate on inode {} (unsupported)", ino);
        reply.error(if self.read_only {
            libc::EROFS
        } else {
            libc::EOPNOTSUPP
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthManager;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::runtime::Runtime;

    fn make_test_fs(base_url: &str, rt_handle: Handle, chunk_size: usize) -> GDriveFS {
        make_test_fs_with_options(
            base_url,
            rt_handle,
            chunk_size,
            GoogleDocsMode::Browser,
            true,
        )
    }

    fn make_test_fs_with_options(
        base_url: &str,
        rt_handle: Handle,
        chunk_size: usize,
        google_docs_mode: GoogleDocsMode,
        shared_with_me: bool,
    ) -> GDriveFS {
        let auth = Arc::new(AuthManager::new_static_for_test("test-token"));
        let api = Arc::new(
            DriveClient::with_base_url(auth, base_url).expect("Failed to create test DriveClient"),
        );
        let cache =
            Arc::new(MetadataCache::open_in_memory(60).expect("Failed to create test cache"));
        GDriveFS::with_full_config(
            api,
            cache,
            rt_handle,
            DEFAULT_ATTR_TTL,
            chunk_size,
            google_docs_mode,
            shared_with_me,
        )
    }

    #[test]
    fn test_getattr_root_inode_and_file_to_attr() {
        let rt = Runtime::new().unwrap();
        let mut fs =
            make_test_fs("http://127.0.0.1:9", rt.handle().clone(), DEFAULT_READ_CHUNK_SIZE);

        let root_attr = fs
            .handle_getattr(FUSE_ROOT_ID)
            .expect("getattr on root inode 1 must succeed");
        assert_eq!(root_attr.ino, FUSE_ROOT_ID);
        assert_eq!(root_attr.kind, FileType::Directory);
        assert_eq!(root_attr.perm, 0o755);
        assert_eq!(root_attr.nlink, 2);

        let sample_file = CachedFile {
            inode: 42,
            drive_id: "file_42".to_string(),
            name: "video.mp4".to_string(),
            mime_type: "video/mp4".to_string(),
            size: 1025,
            modified_time: 1_700_000_100,
            is_dir: false,
            last_synced_at: None,
            web_view_link: None,
            dirty: false,
        };
        let attr = fs.file_to_attr(&sample_file);
        assert_eq!(attr.ino, 42);
        assert_eq!(attr.size, 1025);
        assert_eq!(attr.blocks, 3); // ceil(1025 / 512) = 3
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o644);
        assert_eq!(attr.nlink, 1);
        assert_eq!(
            attr.mtime,
            UNIX_EPOCH + Duration::from_secs(1_700_000_100)
        );
    }

    #[test]
    fn test_lookup_and_readdir_with_mock_server_and_clickable_google_docs() {
        let rt = Runtime::new().unwrap();

        let (addr, server_handle) = rt.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let handle = tokio::spawn(async move {
                if let Ok((mut stream, _)) = listener.accept().await {
                    let mut buf = vec![0u8; 4096];
                    let _ = stream.read(&mut buf).await;

                    let body = r#"{
                        "files": [
                            {
                                "id": "folder_1",
                                "name": "Photos",
                                "mimeType": "application/vnd.google-apps.folder",
                                "modifiedTime": "2025-01-01T00:00:00Z",
                                "parents": ["root"]
                            },
                            {
                                "id": "file_1",
                                "name": "report.pdf",
                                "mimeType": "application/pdf",
                                "size": "2048",
                                "modifiedTime": "2025-01-02T00:00:00Z",
                                "parents": ["root"]
                            },
                            {
                                "id": "file_2_dup",
                                "name": "report.pdf",
                                "mimeType": "application/pdf",
                                "size": "4096",
                                "modifiedTime": "2025-01-03T00:00:00Z",
                                "parents": ["root"]
                            },
                            {
                                "id": "gdoc_1",
                                "name": "Roadmap",
                                "mimeType": "application/vnd.google-apps.document",
                                "modifiedTime": "2025-01-04T00:00:00Z",
                                "parents": ["root"],
                                "webViewLink": "https://docs.google.com/document/d/gdoc_1/edit?usp=drivesdk"
                            }
                        ]
                    }"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
            });

            (addr, handle)
        });

        let base_url = format!("http://{}/drive/v3", addr);
        let mut fs = make_test_fs(&base_url, rt.handle().clone(), DEFAULT_READ_CHUNK_SIZE);

        let entries = fs
            .handle_readdir(FUSE_ROOT_ID)
            .expect("readdir on root must succeed");
        assert_eq!(entries.len(), 7); // ".", "..", "Shared with me", plus 4 Drive items
        assert_eq!(entries[0], (FUSE_ROOT_ID, FileType::Directory, ".".to_string()));
        assert_eq!(entries[1], (FUSE_ROOT_ID, FileType::Directory, "..".to_string()));
        assert_eq!(
            entries[2],
            (
                SHARED_WITH_ME_INODE,
                FileType::Directory,
                SHARED_WITH_ME_NAME.to_string()
            )
        );
        assert_eq!(entries[3].2, "Photos");
        assert_eq!(entries[3].1, FileType::Directory);
        assert_eq!(entries[4].2, "report.pdf");
        assert_eq!(entries[5].2, "report (1).pdf");
        assert_eq!(entries[6].2, "Roadmap.gdoc");

        // Subsequent lookups hit SQLite cache within TTL without needing another HTTP request
        let pdf_attr = fs
            .handle_lookup(FUSE_ROOT_ID, "report.pdf")
            .expect("lookup report.pdf must succeed");
        assert_eq!(pdf_attr.size, 2048);
        assert_eq!(pdf_attr.kind, FileType::RegularFile);

        let dup_pdf_attr = fs
            .handle_lookup(FUSE_ROOT_ID, "report (1).pdf")
            .expect("lookup deduplicated report (1).pdf must succeed");
        assert_eq!(dup_pdf_attr.size, 4096);

        let gdoc_attr = fs
            .handle_lookup(FUSE_ROOT_ID, "Roadmap.gdoc")
            .expect("lookup Google Doc by Roadmap.gdoc must succeed");
        assert!(gdoc_attr.size > 0);

        // Reading a Google Doc in Browser mode returns the .gdoc JSON shortcut with zero HTTP calls
        let gdoc_bytes = fs
            .handle_read(gdoc_attr.ino, 0, 4096)
            .expect("reading Google Doc shortcut should succeed");
        let json_str = std::str::from_utf8(&gdoc_bytes).unwrap();
        assert_eq!(gdoc_bytes.len() as u64, gdoc_attr.size);
        assert!(json_str.contains("https://docs.google.com/document/d/gdoc_1/edit?usp=drivesdk"));
        assert!(json_str.contains("\"doc_id\": \"gdoc_1\""));

        rt.block_on(server_handle).unwrap();
    }

    #[test]
    fn test_google_docs_desktop_and_pdf_export_modes() {
        let rt = Runtime::new().unwrap();
        let fake_pdf_bytes = b"%PDF-1.7\nFake exported Google Doc PDF payload\n%%EOF";

        let (addr, server_handle) = rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let handle = tokio::spawn(async move {
                // Request 1: list_children, Request 2: export_file
                for req_idx in 0..2 {
                    if let Ok((mut stream, _)) = listener.accept().await {
                        let mut buf = vec![0u8; 4096];
                        let n = stream.read(&mut buf).await.unwrap_or(0);
                        let req_str = String::from_utf8_lossy(&buf[..n]);

                        if req_idx == 0 {
                            let body = r#"{
                                "files": [
                                    {
                                        "id": "gdoc_pdf_1",
                                        "name": "Quarterly Spec",
                                        "mimeType": "application/vnd.google-apps.document",
                                        "modifiedTime": "2025-01-05T00:00:00Z",
                                        "parents": ["root"]
                                    }
                                ]
                            }"#;
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                body.len(),
                                body
                            );
                            let _ = stream.write_all(resp.as_bytes()).await;
                        } else {
                            assert!(req_str.contains("/files/gdoc_pdf_1/export"));
                            assert!(req_str.contains("application%2Fpdf"));
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/pdf\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                fake_pdf_bytes.len()
                            );
                            let _ = stream.write_all(resp.as_bytes()).await;
                            let _ = stream.write_all(fake_pdf_bytes).await;
                        }
                    }
                }
            });

            (addr, handle)
        });

        // Verify Desktop launcher synthesis
        let sample_doc = CachedFile {
            inode: 77,
            drive_id: "sheet_77".to_string(),
            name: "Budget 2026".to_string(),
            mime_type: "application/vnd.google-apps.spreadsheet".to_string(),
            size: 0,
            modified_time: 1_700_000_000,
            is_dir: false,
            last_synced_at: None,
            web_view_link: None,
            dirty: false,
        };
        assert_eq!(
            GDriveFS::display_name_for_file("Budget 2026", &sample_doc, GoogleDocsMode::Desktop),
            "Budget 2026.desktop"
        );
        assert_eq!(
            GDriveFS::display_name_for_file("Budget 2026", &sample_doc, GoogleDocsMode::Office),
            "Budget 2026.xlsx"
        );
        let desktop_bytes =
            GDriveFS::synthesize_google_doc_shortcut(&sample_doc, GoogleDocsMode::Desktop);
        let desktop_str = std::str::from_utf8(&desktop_bytes).unwrap();
        assert!(desktop_str.contains("[Desktop Entry]"));
        assert!(desktop_str.contains("Type=Link"));
        assert!(desktop_str.contains("URL=https://docs.google.com/spreadsheets/d/sheet_77/edit"));

        // Verify Pdf export mode end-to-end
        let base_url = format!("http://{}/drive/v3", addr);
        let mut fs = make_test_fs_with_options(
            &base_url,
            rt.handle().clone(),
            DEFAULT_READ_CHUNK_SIZE,
            GoogleDocsMode::Pdf,
            false,
        );

        let entries = fs.handle_readdir(FUSE_ROOT_ID).unwrap();
        assert_eq!(entries.len(), 3); // ".", "..", "Quarterly Spec.pdf"
        assert_eq!(entries[2].2, "Quarterly Spec.pdf");

        let attr = fs
            .handle_lookup(FUSE_ROOT_ID, "Quarterly Spec.pdf")
            .expect("lookup Quarterly Spec.pdf must export and return exact PDF size");
        assert_eq!(attr.size, fake_pdf_bytes.len() as u64);

        let read_data = fs
            .handle_read(attr.ino, 0, 4096)
            .expect("reading exported PDF must serve from chunk buffer");
        assert_eq!(&read_data[..], fake_pdf_bytes);

        rt.block_on(server_handle).unwrap();
    }

    #[test]
    fn test_statfs_storage_quota_and_caching() {
        let rt = Runtime::new().unwrap();
        let about_calls = Arc::new(AtomicUsize::new(0));
        let about_calls_clone = Arc::clone(&about_calls);

        let (addr, server_handle) = rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let handle = tokio::spawn(async move {
                if let Ok((mut stream, _)) = listener.accept().await {
                    let mut buf = vec![0u8; 4096];
                    let _ = stream.read(&mut buf).await;
                    about_calls_clone.fetch_add(1, Ordering::SeqCst);

                    // 15 GiB limit, 5 GiB usage
                    let body = r#"{
                        "storageQuota": {
                            "limit": "16106127360",
                            "usage": "5368709120"
                        }
                    }"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
            });

            (addr, handle)
        });

        let base_url = format!("http://{}/drive/v3", addr);
        let mut fs = make_test_fs(&base_url, rt.handle().clone(), DEFAULT_READ_CHUNK_SIZE);

        let snap1 = fs.handle_statfs().expect("first statfs should succeed");
        let snap2 = fs.handle_statfs().expect("second statfs should hit cache");
        assert_eq!(snap1, snap2);
        assert_eq!(about_calls.load(Ordering::SeqCst), 1);

        assert_eq!(snap1.bsize, 4096);
        assert_eq!(snap1.blocks, 16_106_127_360 / 4096);
        assert_eq!(snap1.bfree, (16_106_127_360 - 5_368_709_120) / 4096);
        assert_eq!(snap1.bavail, snap1.bfree);

        rt.block_on(server_handle).unwrap();
    }

    #[test]
    fn test_shared_with_me_virtual_directory() {
        let rt = Runtime::new().unwrap();

        let (addr, server_handle) = rt.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let handle = tokio::spawn(async move {
                if let Ok((mut stream, _)) = listener.accept().await {
                    let mut buf = vec![0u8; 4096];
                    let n = stream.read(&mut buf).await.unwrap_or(0);
                    let req_str = String::from_utf8_lossy(&buf[..n]);
                    assert!(req_str.contains("sharedWithMe"));

                    let body = r#"{
                        "files": [
                            {
                                "id": "shared_1",
                                "name": "Team Notes.txt",
                                "mimeType": "text/plain",
                                "size": "512",
                                "modifiedTime": "2025-02-01T00:00:00Z"
                            }
                        ]
                    }"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
            });

            (addr, handle)
        });

        let base_url = format!("http://{}/drive/v3", addr);
        let mut fs = make_test_fs(&base_url, rt.handle().clone(), DEFAULT_READ_CHUNK_SIZE);

        let shared_attr = fs
            .handle_lookup(FUSE_ROOT_ID, SHARED_WITH_ME_NAME)
            .expect("lookup 'Shared with me' under root must succeed");
        assert_eq!(shared_attr.ino, SHARED_WITH_ME_INODE);
        assert_eq!(shared_attr.kind, FileType::Directory);

        let shared_entries = fs
            .handle_readdir(SHARED_WITH_ME_INODE)
            .expect("readdir on SHARED_WITH_ME_INODE must succeed");
        assert_eq!(shared_entries.len(), 3);
        assert_eq!(
            shared_entries[0],
            (SHARED_WITH_ME_INODE, FileType::Directory, ".".to_string())
        );
        assert_eq!(
            shared_entries[1],
            (FUSE_ROOT_ID, FileType::Directory, "..".to_string())
        );
        assert_eq!(shared_entries[2].2, "Team Notes.txt");

        let child_attr = fs
            .handle_lookup(SHARED_WITH_ME_INODE, "Team Notes.txt")
            .expect("lookup shared child must succeed");
        assert_eq!(child_attr.size, 512);

        rt.block_on(server_handle).unwrap();
    }

    #[tokio::test]
    async fn test_sync_changes_once_initializes_and_applies_changes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            for req_idx in 0..2 {
                if let Ok((mut stream, _)) = listener.accept().await {
                    let mut buf = vec![0u8; 4096];
                    let _ = stream.read(&mut buf).await;

                    let body = if req_idx == 0 {
                        r#"{"startPageToken": "tok_100"}"#
                    } else {
                        r#"{
                            "newStartPageToken": "tok_101",
                            "changes": [
                                {
                                    "fileId": "f_changed",
                                    "removed": false,
                                    "file": {
                                        "id": "f_changed",
                                        "name": "fresh.txt",
                                        "mimeType": "text/plain",
                                        "size": "99",
                                        "modifiedTime": "2025-04-01T00:00:00Z",
                                        "parents": ["root"],
                                        "trashed": false
                                    }
                                }
                            ]
                        }"#
                    };
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
            }
        });

        let auth = Arc::new(AuthManager::new_static_for_test("test-token"));
        let api = DriveClient::with_base_url(auth, format!("http://{}/drive/v3", addr)).unwrap();
        let cache = MetadataCache::open_in_memory(60).unwrap();

        // First call initializes startPageToken
        let count1 = sync_changes_once(&api, &cache).await.unwrap();
        assert_eq!(count1, 0);
        assert_eq!(
            cache.get_change_page_token().unwrap().as_deref(),
            Some("tok_100")
        );

        // Second call polls changes and applies them
        let count2 = sync_changes_once(&api, &cache).await.unwrap();
        assert_eq!(count2, 1);
        assert_eq!(
            cache.get_change_page_token().unwrap().as_deref(),
            Some("tok_101")
        );
        let updated = cache.get_by_drive_id("f_changed").unwrap().unwrap();
        assert_eq!(updated.name, "fresh.txt");
        assert_eq!(updated.size, 99);

        server_handle.await.unwrap();
    }

    #[test]
    fn test_read_ahead_chunk_buffer_single_http_call_for_sequential_reads() {
        let rt = Runtime::new().unwrap();
        let http_download_count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&http_download_count);

        // Prepare a 16 KB synthetic file payload
        let payload: Vec<u8> = (0..16_384).map(|i| (i % 251) as u8).collect();
        let payload_server = payload.clone();

        let (addr, server_handle) = rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let handle = tokio::spawn(async move {
                // Accept 1 request for list_children and 1 request for download_range
                for req_idx in 0..2 {
                    if let Ok((mut stream, _)) = listener.accept().await {
                        let mut buf = vec![0u8; 4096];
                        let n = stream.read(&mut buf).await.unwrap_or(0);
                        let req_str = String::from_utf8_lossy(&buf[..n]);

                        if req_idx == 0 {
                            let body = r#"{
                                "files": [
                                    {
                                        "id": "big_file_1",
                                        "name": "data.bin",
                                        "mimeType": "application/octet-stream",
                                        "size": "16384",
                                        "modifiedTime": "2025-02-01T00:00:00Z",
                                        "parents": ["root"]
                                    }
                                ]
                            }"#;
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                body.len(),
                                body
                            );
                            let _ = stream.write_all(resp.as_bytes()).await;
                        } else {
                            count_clone.fetch_add(1, Ordering::SeqCst);
                            assert!(
                                req_str.contains("Range: bytes=0-16383")
                                    || req_str.contains("range: bytes=0-16383")
                            );
                            let resp = format!(
                                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                payload_server.len()
                            );
                            let _ = stream.write_all(resp.as_bytes()).await;
                            let _ = stream.write_all(&payload_server).await;
                        }
                    }
                }
            });

            (addr, handle)
        });

        let base_url = format!("http://{}/drive/v3", addr);
        let mut fs = make_test_fs(&base_url, rt.handle().clone(), DEFAULT_READ_CHUNK_SIZE);

        let attr = fs
            .handle_lookup(FUSE_ROOT_ID, "data.bin")
            .expect("lookup data.bin failed");
        assert_eq!(attr.size, 16_384);

        // Perform 4 sequential 1 KB reads (0..1024, 1024..2048, 2048..3072, 3072..4096)
        for i in 0..4 {
            let offset = (i * 1024) as u64;
            let chunk = fs
                .handle_read(attr.ino, offset, 1024)
                .expect("sequential read must succeed");
            assert_eq!(&chunk[..], &payload[offset as usize..(offset as usize + 1024)]);
        }

        // Only 1 HTTP download request must have been made because the 4 MB read-ahead buffer served reads 2..=4
        assert_eq!(http_download_count.load(Ordering::SeqCst), 1);

        rt.block_on(server_handle).unwrap();
    }

    #[test]
    fn test_posix_error_conditions() {
        let rt = Runtime::new().unwrap();

        let (addr, server_handle) = rt.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let handle = tokio::spawn(async move {
                if let Ok((mut stream, _)) = listener.accept().await {
                    let mut buf = vec![0u8; 4096];
                    let _ = stream.read(&mut buf).await;
                    let body = r#"{"error": {"code": 401, "message": "Invalid Credentials"}}"#;
                    let resp = format!(
                        "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
            });

            (addr, handle)
        });

        let base_url = format!("http://{}/drive/v3", addr);
        let mut fs = make_test_fs(&base_url, rt.handle().clone(), DEFAULT_READ_CHUNK_SIZE);

        // 1. Missing inode -> ENOENT
        assert_eq!(fs.handle_getattr(999_999), Err(libc::ENOENT));
        assert_eq!(fs.handle_read(999_999, 0, 1024), Err(libc::ENOENT));

        // 2. Reading a directory -> EISDIR
        assert_eq!(fs.handle_read(FUSE_ROOT_ID, 0, 1024), Err(libc::EISDIR));
        assert_eq!(fs.handle_open(FUSE_ROOT_ID, libc::O_RDONLY), Err(libc::EISDIR));

        // 3. Opening with write flags -> EROFS
        assert_eq!(fs.handle_open(FUSE_ROOT_ID, libc::O_WRONLY), Err(libc::EROFS));
        assert_eq!(fs.handle_open(FUSE_ROOT_ID, libc::O_RDWR), Err(libc::EROFS));

        // 4. Lookup inside a regular file -> ENOTDIR
        let reg_file = crate::api::DriveFile {
            id: "reg_1".to_string(),
            name: "plain.txt".to_string(),
            mime_type: "text/plain".to_string(),
            size: 10,
            modified_time: 1_700_000_000,
            parents: vec!["root".to_string()],
            trashed: false,
            web_view_link: None,
        };
        let cached_reg = fs.cache.upsert_file(&reg_file).unwrap();
        assert_eq!(
            fs.handle_lookup(cached_reg.inode, "child"),
            Err(libc::ENOTDIR)
        );
        assert_eq!(fs.handle_readdir(cached_reg.inode), Err(libc::ENOTDIR));

        // 5. HTTP 401 from Drive API -> EACCES
        assert_eq!(fs.handle_readdir(FUSE_ROOT_ID), Err(libc::EACCES));

        rt.block_on(server_handle).unwrap();
    }

    #[test]
    fn test_desktop_launcher_executable_perm_and_unlimited_statfs_and_cache_eviction() {
        let rt = Runtime::new().unwrap();
        let export_calls = Arc::new(AtomicUsize::new(0));
        let export_calls_clone = Arc::clone(&export_calls);

        let (addr, server_handle) = rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let handle = tokio::spawn(async move {
                // Request 1: about (unlimited quota), Request 2: export v1, Request 3: export v2
                for req_idx in 0..3 {
                    if let Ok((mut stream, _)) = listener.accept().await {
                        let mut buf = vec![0u8; 4096];
                        let _ = stream.read(&mut buf).await;

                        if req_idx == 0 {
                            let body = r#"{"storageQuota": {"usage": "10737418240"}}"#;
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                body.len(),
                                body
                            );
                            let _ = stream.write_all(resp.as_bytes()).await;
                        } else {
                            let call_num = export_calls_clone.fetch_add(1, Ordering::SeqCst) + 1;
                            let pdf_payload = format!("%PDF-1.7 version {}", call_num);
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/pdf\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                pdf_payload.len(),
                                pdf_payload
                            );
                            let _ = stream.write_all(resp.as_bytes()).await;
                        }
                    }
                }
            });

            (addr, handle)
        });

        let base_url = format!("http://{}/drive/v3", addr);

        // 1. Desktop mode marks only Google Workspace native launchers executable (0o755)
        let fs_desktop = make_test_fs_with_options(
            &base_url,
            rt.handle().clone(),
            DEFAULT_READ_CHUNK_SIZE,
            GoogleDocsMode::Desktop,
            false,
        );
        let native_doc = CachedFile {
            inode: 50,
            drive_id: "doc_50".to_string(),
            name: "Notes".to_string(),
            mime_type: "application/vnd.google-apps.document".to_string(),
            size: 0,
            modified_time: 1_700_000_000,
            is_dir: false,
            last_synced_at: None,
            web_view_link: None,
            dirty: false,
        };
        let regular_file = CachedFile {
            inode: 51,
            drive_id: "file_51".to_string(),
            name: "notes.txt".to_string(),
            mime_type: "text/plain".to_string(),
            size: 100,
            modified_time: 1_700_000_000,
            is_dir: false,
            last_synced_at: None,
            web_view_link: None,
            dirty: false,
        };
        assert_eq!(fs_desktop.file_to_attr(&native_doc).perm, 0o755);
        assert_eq!(fs_desktop.file_to_attr(&regular_file).perm, 0o644);

        // 2. Unlimited storage quota (`limit == 0`) reports 0 blocks instead of a fabricated 1 TiB quota
        let mut fs_pdf = make_test_fs_with_options(
            &base_url,
            rt.handle().clone(),
            DEFAULT_READ_CHUNK_SIZE,
            GoogleDocsMode::Pdf,
            false,
        );
        let snap = fs_pdf.handle_statfs().expect("statfs for unlimited account must succeed");
        assert_eq!(snap.blocks, 0);
        assert_eq!(snap.bfree, 0);
        assert_eq!(snap.bavail, 0);

        // 3. Remote change application evicts cached exported Google Doc bytes
        let doc_v1 = crate::api::DriveFile {
            id: "doc_export_1".to_string(),
            name: "Spec".to_string(),
            mime_type: "application/vnd.google-apps.document".to_string(),
            size: 0,
            modified_time: 1_700_000_000,
            parents: vec!["root".to_string()],
            trashed: false,
            web_view_link: None,
        };
        let cached_doc = fs_pdf.cache.upsert_file(&doc_v1).unwrap();

        let bytes_v1 = fs_pdf.handle_read(cached_doc.inode, 0, 4096).unwrap();
        assert_eq!(&bytes_v1[..], b"%PDF-1.7 version 1");
        assert_eq!(export_calls.load(Ordering::SeqCst), 1);

        // Apply incremental change for doc_export_1 and verify stale export buffer is evicted
        let mut doc_v2 = doc_v1.clone();
        doc_v2.modified_time = 1_700_000_100;
        fs_pdf
            .cache
            .apply_changes(
                &[crate::api::ChangeItem {
                    file_id: "doc_export_1".to_string(),
                    removed: false,
                    file: Some(doc_v2),
                }],
                Some("tok_next"),
            )
            .unwrap();

        let bytes_v2 = fs_pdf.handle_read(cached_doc.inode, 0, 4096).unwrap();
        assert_eq!(&bytes_v2[..], b"%PDF-1.7 version 2");
        assert_eq!(export_calls.load(Ordering::SeqCst), 2);

        rt.block_on(server_handle).unwrap();
    }

    #[test]
    fn test_display_name_disambiguation_and_access_w_ok_rejection() {
        let rt = Runtime::new().unwrap();
        let mut fs = make_test_fs(
            "http://127.0.0.1:9",
            rt.handle().clone(),
            DEFAULT_READ_CHUNK_SIZE,
        );

        // Populate root in cache with:
        // 1. A real folder named "Shared with me" (collides with virtual SHARED_WITH_ME_NAME)
        // 2. A native Google Doc named "Plan" (whose display name becomes "Plan.gdoc")
        // 3. A regular binary file named "Plan.gdoc"
        let real_shared_folder = crate::api::DriveFile {
            id: "real_shared_folder".to_string(),
            name: "Shared with me".to_string(),
            mime_type: FOLDER_MIME_TYPE.to_string(),
            size: 0,
            modified_time: 1_700_000_000,
            parents: vec!["root".to_string()],
            trashed: false,
            web_view_link: None,
        };
        let native_plan_doc = crate::api::DriveFile {
            id: "doc_plan_a".to_string(),
            name: "Plan".to_string(),
            mime_type: "application/vnd.google-apps.document".to_string(),
            size: 0,
            modified_time: 1_700_000_000,
            parents: vec!["root".to_string()],
            trashed: false,
            web_view_link: None,
        };
        let regular_plan_file = crate::api::DriveFile {
            id: "file_plan_b".to_string(),
            name: "Plan.gdoc".to_string(),
            mime_type: "application/octet-stream".to_string(),
            size: 321,
            modified_time: 1_700_000_000,
            parents: vec!["root".to_string()],
            trashed: false,
            web_view_link: None,
        };

        fs.cache
            .sync_directory_children(
                FUSE_ROOT_ID,
                &[real_shared_folder, native_plan_doc, regular_plan_file],
            )
            .unwrap();

        let entries = fs.handle_readdir(FUSE_ROOT_ID).unwrap();
        let names: Vec<&str> = entries.iter().map(|(_, _, n)| n.as_str()).collect();
        assert_eq!(
            names,
            vec![
                ".",
                "..",
                "Shared with me",
                "Shared with me (1)",
                "Plan.gdoc",
                "Plan (1).gdoc"
            ]
        );

        // Virtual "Shared with me" resolves to SHARED_WITH_ME_INODE (2), while "Shared with me (1)" resolves to the real folder
        let virt_attr = fs.handle_lookup(FUSE_ROOT_ID, "Shared with me").unwrap();
        assert_eq!(virt_attr.ino, SHARED_WITH_ME_INODE);

        let real_attr = fs
            .handle_lookup(FUSE_ROOT_ID, "Shared with me (1)")
            .unwrap();
        assert!(real_attr.ino > SHARED_WITH_ME_INODE);
        assert_eq!(real_attr.kind, FileType::Directory);

        // Both "Plan.gdoc" and "Plan (1).gdoc" are uniquely addressable via handle_lookup
        let plan_1 = fs.handle_lookup(FUSE_ROOT_ID, "Plan.gdoc").unwrap();
        let plan_2 = fs.handle_lookup(FUSE_ROOT_ID, "Plan (1).gdoc").unwrap();
        assert_ne!(plan_1.ino, plan_2.ino);

        // Verify handle_access rejects W_OK with EROFS while allowing R_OK
        assert_eq!(fs.handle_access(FUSE_ROOT_ID, libc::R_OK), Ok(()));
        assert_eq!(fs.handle_access(FUSE_ROOT_ID, libc::W_OK), Err(libc::EROFS));
        assert_eq!(
            fs.handle_access(FUSE_ROOT_ID, libc::R_OK | libc::W_OK),
            Err(libc::EROFS)
        );
    }

    #[tokio::test]
    async fn test_sync_changes_once_recovers_from_expired_page_token_410() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            // Request 1: list_changes returns 410 Gone, Request 2: get_start_page_token returns fresh token
            for req_idx in 0..2 {
                if let Ok((mut stream, _)) = listener.accept().await {
                    let mut buf = vec![0u8; 4096];
                    let _ = stream.read(&mut buf).await;

                    if req_idx == 0 {
                        let body = r#"{"error": {"code": 410, "message": "pageTokenExpired"}}"#;
                        let resp = format!(
                            "HTTP/1.1 410 Gone\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = stream.write_all(resp.as_bytes()).await;
                    } else {
                        let body = r#"{"startPageToken": "fresh_tok_500"}"#;
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = stream.write_all(resp.as_bytes()).await;
                    }
                }
            }
        });

        let auth = Arc::new(AuthManager::new_static_for_test("test-token"));
        let api = DriveClient::with_base_url(auth, format!("http://{}/drive/v3", addr)).unwrap();
        let cache = MetadataCache::open_in_memory(300).unwrap();
        cache.set_change_page_token("expired_tok_1").unwrap();
        cache.sync_directory_children(FUSE_ROOT_ID, &[]).unwrap();
        assert!(!cache.is_dir_stale(FUSE_ROOT_ID).unwrap());

        let count = sync_changes_once(&api, &cache)
            .await
            .expect("sync_changes_once must recover from 410 Gone");
        assert_eq!(count, 0);
        assert_eq!(
            cache.get_change_page_token().unwrap().as_deref(),
            Some("fresh_tok_500")
        );
        assert!(cache.is_dir_stale(FUSE_ROOT_ID).unwrap());

        server_handle.await.unwrap();
    }

    #[test]
    fn test_phase3_create_write_read_flush_and_release_resumable_upload() {
        let rt = Runtime::new().unwrap();

        let (addr, server_handle) = rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let handle = tokio::spawn(async move {
                // Request 1: POST /upload/drive/v3/files?uploadType=resumable (new file session)
                // Request 2: PUT /upload/drive/v3/session/new_1 (new file payload)
                // Request 3: PATCH /upload/drive/v3/files/drive_created_100?uploadType=resumable (existing file update session)
                // Request 4: PUT /upload/drive/v3/session/existing_1 (updated payload)
                for req_idx in 0..4 {
                    if let Ok((mut stream, _)) = listener.accept().await {
                        let mut buf = vec![0u8; 8192];
                        let n = stream.read(&mut buf).await.unwrap_or(0);
                        let req_str = String::from_utf8_lossy(&buf[..n]);

                        match req_idx {
                            0 => {
                                assert!(req_str.starts_with("POST /upload/drive/v3/files?uploadType=resumable"));
                                assert!(req_str.contains("\"name\":\"hello.txt\""));
                                let session_url =
                                    format!("http://{}/upload/drive/v3/session/new_1", addr);
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nLocation: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                    session_url
                                );
                                let _ = stream.write_all(resp.as_bytes()).await;
                            }
                            1 => {
                                assert!(req_str.starts_with("PUT /upload/drive/v3/session/new_1"));
                                assert!(req_str.contains("Hello Phase 3!"));
                                let body = r#"{
                                    "id": "drive_created_100",
                                    "name": "hello.txt",
                                    "mimeType": "text/plain",
                                    "size": "14",
                                    "modifiedTime": "2026-03-01T12:00:00Z",
                                    "parents": ["root"],
                                    "trashed": false
                                }"#;
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                    body.len(),
                                    body
                                );
                                let _ = stream.write_all(resp.as_bytes()).await;
                            }
                            2 => {
                                assert!(req_str.starts_with(
                                    "PATCH /upload/drive/v3/files/drive_created_100?uploadType=resumable"
                                ));
                                let session_url =
                                    format!("http://{}/upload/drive/v3/session/existing_1", addr);
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nLocation: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                    session_url
                                );
                                let _ = stream.write_all(resp.as_bytes()).await;
                            }
                            3 => {
                                assert!(
                                    req_str.starts_with("PUT /upload/drive/v3/session/existing_1")
                                );
                                assert!(req_str.contains("Updated!"));
                                let body = r#"{
                                    "id": "drive_created_100",
                                    "name": "hello.txt",
                                    "mimeType": "text/plain",
                                    "size": "8",
                                    "modifiedTime": "2026-03-01T12:05:00Z",
                                    "parents": ["root"],
                                    "trashed": false
                                }"#;
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                    body.len(),
                                    body
                                );
                                let _ = stream.write_all(resp.as_bytes()).await;
                            }
                            _ => unreachable!(),
                        }
                    }
                }
            });

            (addr, handle)
        });

        let staging_dir = std::env::temp_dir().join(format!(
            "gdrive-fuse-rs-fs-stage-test-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let base_url = format!("http://{}/drive/v3", addr);
        let mut fs = make_test_fs(&base_url, rt.handle().clone(), DEFAULT_READ_CHUNK_SIZE)
            .with_read_only(false)
            .with_staging_dir(staging_dir.clone());

        // Mark root synced so create doesn't trigger a listing call
        fs.cache.sync_directory_children(FUSE_ROOT_ID, &[]).unwrap();

        // 1. Create a new file under root
        let (created_attr, fh) = fs
            .handle_create(FUSE_ROOT_ID, "hello.txt", 0o644, libc::O_CREAT | libc::O_RDWR)
            .expect("handle_create must succeed in read-write mode");
        assert_eq!(created_attr.size, 0);
        let ino = created_attr.ino;
        let expected_stage_path = staging_dir.join(format!("inode_{}.stage", ino));
        assert!(expected_stage_path.exists());

        // 2. Write two chunks to the staged file
        let w1 = fs
            .handle_write(ino, fh, 0, b"Hello ", 0)
            .expect("first write must succeed");
        assert_eq!(w1, 6);
        let w2 = fs
            .handle_write(ino, fh, 6, b"Phase 3!", 0)
            .expect("second write must succeed");
        assert_eq!(w2, 8);

        // 3. Read back while staged (served directly from local staging file)
        let staged_read = fs
            .handle_read(ino, 0, 1024)
            .expect("reading staged file must succeed");
        assert_eq!(&staged_read[..], b"Hello Phase 3!");

        // 4. Flush -> triggers resumable upload (POST + PUT), updates Drive ID in SQLite, and deletes staging file immediately
        fs.handle_flush(ino, fh)
            .expect("handle_flush must upload via resumable API");
        assert!(
            !expected_stage_path.exists(),
            "Local staging file must be deleted immediately upon upload completion"
        );
        let uploaded_meta = fs.cache.get_by_inode(ino).unwrap().unwrap();
        assert_eq!(uploaded_meta.drive_id, "drive_created_100");
        assert!(!uploaded_meta.dirty);
        assert_eq!(uploaded_meta.size, 14);

        // Release handle
        fs.handle_release(ino, fh).expect("handle_release must succeed");

        // 5. Re-open existing file with O_RDWR | O_TRUNC, write new content, and release
        let fh2 = fs
            .handle_open_with_fh(ino, libc::O_RDWR | libc::O_TRUNC)
            .expect("open existing file with O_RDWR | O_TRUNC must succeed");
        assert_eq!(fs.handle_getattr(ino).unwrap().size, 0);
        fs.handle_write(ino, fh2, 0, b"Updated!", 0).unwrap();
        fs.handle_release(ino, fh2)
            .expect("release must upload modified existing file via PATCH resumable session");
        assert!(!expected_stage_path.exists());

        let updated_meta = fs.cache.get_by_inode(ino).unwrap().unwrap();
        assert_eq!(updated_meta.size, 8);
        assert!(!updated_meta.dirty);

        let _ = std::fs::remove_dir_all(&staging_dir);
        rt.block_on(server_handle).unwrap();
    }

    #[test]
    fn test_phase3_mkdir_rmdir_unlink_rename_and_setattr() {
        let rt = Runtime::new().unwrap();

        let (addr, server_handle) = rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let handle = tokio::spawn(async move {
                // Request 1: POST /drive/v3/files (mkdir "Projects")
                // Request 2: PATCH /drive/v3/files/file_to_move (rename/move across folders with addParents & removeParents)
                // Request 3: PATCH /drive/v3/files/file_to_move (setattr mtime update)
                // Request 4: PATCH /drive/v3/files/file_to_move (unlink -> {"trashed": true})
                // Request 5: PATCH /drive/v3/files/folder_projects_1 (rmdir -> {"trashed": true})
                for req_idx in 0..5 {
                    if let Ok((mut stream, _)) = listener.accept().await {
                        let mut buf = vec![0u8; 8192];
                        let n = stream.read(&mut buf).await.unwrap_or(0);
                        let req_str = String::from_utf8_lossy(&buf[..n]);

                        match req_idx {
                            0 => {
                                assert!(req_str.starts_with("POST /drive/v3/files"));
                                assert!(req_str.contains("application/vnd.google-apps.folder"));
                                assert!(req_str.contains("\"name\":\"Projects\""));
                                let body = r#"{
                                    "id": "folder_projects_1",
                                    "name": "Projects",
                                    "mimeType": "application/vnd.google-apps.folder",
                                    "modifiedTime": "2026-03-01T00:00:00Z",
                                    "parents": ["root"],
                                    "trashed": false
                                }"#;
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                    body.len(),
                                    body
                                );
                                let _ = stream.write_all(resp.as_bytes()).await;
                            }
                            1 => {
                                assert!(req_str.starts_with("PATCH /drive/v3/files/file_to_move"));
                                assert!(req_str.contains("addParents=folder_projects_1"));
                                assert!(req_str.contains("removeParents=root"));
                                assert!(req_str.contains("\"name\":\"moved.txt\""));
                                let body = r#"{
                                    "id": "file_to_move",
                                    "name": "moved.txt",
                                    "mimeType": "text/plain",
                                    "size": "12",
                                    "modifiedTime": "2026-03-01T01:00:00Z",
                                    "parents": ["folder_projects_1"],
                                    "trashed": false
                                }"#;
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                    body.len(),
                                    body
                                );
                                let _ = stream.write_all(resp.as_bytes()).await;
                            }
                            2 => {
                                assert!(req_str.starts_with("PATCH /drive/v3/files/file_to_move"));
                                assert!(req_str.contains("modifiedTime"));
                                let body = r#"{
                                    "id": "file_to_move",
                                    "name": "moved.txt",
                                    "mimeType": "text/plain",
                                    "size": "12",
                                    "modifiedTime": "2026-03-02T00:00:00Z",
                                    "parents": ["folder_projects_1"],
                                    "trashed": false
                                }"#;
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                    body.len(),
                                    body
                                );
                                let _ = stream.write_all(resp.as_bytes()).await;
                            }
                            3 => {
                                assert!(req_str.starts_with("PATCH /drive/v3/files/file_to_move"));
                                assert!(req_str.contains("\"trashed\":true"));
                                let body = r#"{
                                    "id": "file_to_move",
                                    "name": "moved.txt",
                                    "mimeType": "text/plain",
                                    "trashed": true
                                }"#;
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                    body.len(),
                                    body
                                );
                                let _ = stream.write_all(resp.as_bytes()).await;
                            }
                            4 => {
                                assert!(
                                    req_str.starts_with("PATCH /drive/v3/files/folder_projects_1")
                                );
                                assert!(req_str.contains("\"trashed\":true"));
                                let body = r#"{
                                    "id": "folder_projects_1",
                                    "name": "Projects",
                                    "mimeType": "application/vnd.google-apps.folder",
                                    "trashed": true
                                }"#;
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                    body.len(),
                                    body
                                );
                                let _ = stream.write_all(resp.as_bytes()).await;
                            }
                            _ => unreachable!(),
                        }
                    }
                }
            });

            (addr, handle)
        });

        let staging_dir = std::env::temp_dir().join(format!(
            "gdrive-fuse-rs-fs-ops-test-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let base_url = format!("http://{}/drive/v3", addr);
        let mut fs = make_test_fs(&base_url, rt.handle().clone(), DEFAULT_READ_CHUNK_SIZE)
            .with_read_only(false)
            .with_staging_dir(staging_dir.clone());

        // Seed root with `draft.txt`
        let draft_file = crate::api::DriveFile {
            id: "file_to_move".to_string(),
            name: "draft.txt".to_string(),
            mime_type: "text/plain".to_string(),
            size: 12,
            modified_time: 1_700_000_000,
            parents: vec!["root".to_string()],
            trashed: false,
            web_view_link: None,
        };
        fs.cache
            .sync_directory_children(FUSE_ROOT_ID, &[draft_file])
            .unwrap();

        // 1. mkdir "Projects" under root
        let proj_attr = fs
            .handle_mkdir(FUSE_ROOT_ID, "Projects", 0o755)
            .expect("mkdir Projects must succeed");
        assert_eq!(proj_attr.kind, FileType::Directory);
        fs.cache
            .sync_directory_children(proj_attr.ino, &[])
            .unwrap();

        // 2. rename "draft.txt" in root -> "moved.txt" inside "Projects"
        fs.handle_rename(FUSE_ROOT_ID, "draft.txt", proj_attr.ino, "moved.txt", 0)
            .expect("cross-directory rename must succeed");
        assert_eq!(
            fs.handle_lookup(FUSE_ROOT_ID, "draft.txt"),
            Err(libc::ENOENT)
        );
        let moved_attr = fs
            .handle_lookup(proj_attr.ino, "moved.txt")
            .expect("moved.txt must exist under Projects");

        // 3. rmdir on non-empty "Projects" must fail with ENOTEMPTY
        assert_eq!(
            fs.handle_rmdir(FUSE_ROOT_ID, "Projects"),
            Err(libc::ENOTEMPTY)
        );

        // 4. setattr mtime update on "moved.txt"
        let new_mtime = UNIX_EPOCH + Duration::from_secs(1_772_409_600);
        let updated_attr = fs
            .handle_setattr(moved_attr.ino, None, Some(new_mtime), None)
            .expect("setattr mtime must succeed");
        assert_eq!(updated_attr.mtime, new_mtime);

        // 5. unlink "moved.txt" inside "Projects" (safe Trash)
        fs.handle_unlink(proj_attr.ino, "moved.txt")
            .expect("unlink moved.txt must succeed");
        assert_eq!(
            fs.handle_lookup(proj_attr.ino, "moved.txt"),
            Err(libc::ENOENT)
        );

        // 6. rmdir now-empty "Projects" (safe Trash)
        fs.handle_rmdir(FUSE_ROOT_ID, "Projects")
            .expect("rmdir empty Projects folder must succeed");
        assert_eq!(
            fs.handle_lookup(FUSE_ROOT_ID, "Projects"),
            Err(libc::ENOENT)
        );

        let _ = std::fs::remove_dir_all(&staging_dir);
        rt.block_on(server_handle).unwrap();
    }

    #[test]
    fn test_phase3_read_only_mode_rejects_all_mutations_with_erofs() {
        let rt = Runtime::new().unwrap();
        let mut fs = make_test_fs(
            "http://127.0.0.1:9",
            rt.handle().clone(),
            DEFAULT_READ_CHUNK_SIZE,
        );
        assert!(fs.read_only);

        assert_eq!(
            fs.handle_create(FUSE_ROOT_ID, "a.txt", 0o644, libc::O_CREAT | libc::O_RDWR),
            Err(libc::EROFS)
        );
        assert_eq!(
            fs.handle_write(FUSE_ROOT_ID, 1, 0, b"abc", 0),
            Err(libc::EROFS)
        );
        assert_eq!(
            fs.handle_setattr(FUSE_ROOT_ID, Some(0), None, None),
            Err(libc::EROFS)
        );
        assert_eq!(
            fs.handle_mkdir(FUSE_ROOT_ID, "dir", 0o755),
            Err(libc::EROFS)
        );
        assert_eq!(fs.handle_unlink(FUSE_ROOT_ID, "a.txt"), Err(libc::EROFS));
        assert_eq!(fs.handle_rmdir(FUSE_ROOT_ID, "dir"), Err(libc::EROFS));
        assert_eq!(
            fs.handle_rename(FUSE_ROOT_ID, "a.txt", FUSE_ROOT_ID, "b.txt", 0),
            Err(libc::EROFS)
        );
    }

    #[test]
    fn test_phase3_google_doc_coexistence_create_without_trunc_and_rename_cycle_check() {
        let rt = Runtime::new().unwrap();
        let staging_dir = std::env::temp_dir().join(format!(
            "gdrive-fuse-rs-fs-edge-test-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let mut fs = make_test_fs(
            "http://127.0.0.1:9",
            rt.handle().clone(),
            DEFAULT_READ_CHUNK_SIZE,
        )
        .with_read_only(false)
        .with_staging_dir(staging_dir.clone());

        // Seed root with a native Google Doc named "Plan" (FUSE display name: "Plan.gdoc"),
        // a folder "Parent", and a subfolder "Child" inside "Parent".
        let gdoc = crate::api::DriveFile {
            id: "gdoc_plan_1".to_string(),
            name: "Plan".to_string(),
            mime_type: "application/vnd.google-apps.document".to_string(),
            size: 0,
            modified_time: 1_700_000_000,
            parents: vec!["root".to_string()],
            trashed: false,
            web_view_link: None,
        };
        let parent_dir = crate::api::DriveFile {
            id: "dir_parent_1".to_string(),
            name: "Parent".to_string(),
            mime_type: FOLDER_MIME_TYPE.to_string(),
            size: 0,
            modified_time: 1_700_000_000,
            parents: vec!["root".to_string()],
            trashed: false,
            web_view_link: None,
        };
        fs.cache
            .sync_directory_children(FUSE_ROOT_ID, &[gdoc, parent_dir])
            .unwrap();

        let parent_attr = fs.handle_lookup(FUSE_ROOT_ID, "Parent").unwrap();
        let child_dir = crate::api::DriveFile {
            id: "dir_child_1".to_string(),
            name: "Child".to_string(),
            mime_type: FOLDER_MIME_TYPE.to_string(),
            size: 0,
            modified_time: 1_700_000_000,
            parents: vec!["dir_parent_1".to_string()],
            trashed: false,
            web_view_link: None,
        };
        fs.cache
            .sync_directory_children(parent_attr.ino, &[child_dir])
            .unwrap();
        let child_attr = fs.handle_lookup(parent_attr.ino, "Child").unwrap();
        fs.cache
            .sync_directory_children(child_attr.ino, &[])
            .unwrap();

        // 1. Creating a regular file named "Plan" (no extension) must NOT clobber "Plan.gdoc"
        let (plan_reg_attr, fh1) = fs
            .handle_create(FUSE_ROOT_ID, "Plan", 0o644, libc::O_CREAT | libc::O_RDWR)
            .expect("creating 'Plan' alongside 'Plan.gdoc' must succeed");
        fs.handle_write(plan_reg_attr.ino, fh1, 0, b"12345", 0)
            .unwrap();

        assert!(
            fs.handle_lookup(FUSE_ROOT_ID, "Plan.gdoc").is_ok(),
            "Plan.gdoc must still exist in root alongside Plan"
        );
        assert_eq!(
            fs.handle_lookup(FUSE_ROOT_ID, "Plan").unwrap().size,
            5
        );

        // 2. Calling handle_create on existing "Plan" WITHOUT O_TRUNC or O_EXCL must preserve its 5-byte content
        let (reopened_attr, _fh2) = fs
            .handle_create(FUSE_ROOT_ID, "Plan", 0o644, libc::O_CREAT | libc::O_RDWR)
            .expect("handle_create without O_TRUNC/O_EXCL on existing file must succeed");
        assert_eq!(
            reopened_attr.size, 5,
            "handle_create without O_TRUNC must not truncate existing file"
        );

        // 3. Moving "Parent" into its own descendant "Child" must fail with EINVAL
        assert_eq!(
            fs.handle_rename(FUSE_ROOT_ID, "Parent", child_attr.ino, "MovedParent", 0),
            Err(libc::EINVAL)
        );

        // 4. clear_all() must preserve dirty/pending files during 410 token recovery
        fs.cache.clear_all().unwrap();
        assert!(
            fs.cache.get_by_inode(plan_reg_attr.ino).unwrap().is_some(),
            "dirty/pending file must survive clear_all()"
        );

        let _ = std::fs::remove_dir_all(&staging_dir);
    }

    #[test]
    fn test_phase3_review_edge_cases_unlink_open_setattr_retry_rename_and_stage_recovery() {
        let rt = Runtime::new().unwrap();
        let staging_dir = std::env::temp_dir().join(format!(
            "gdrive-fuse-rs-fs-review-test-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));

        let gdoc = crate::api::DriveFile {
            id: "gdoc_plan_1".to_string(),
            name: "Plan".to_string(),
            mime_type: "application/vnd.google-apps.document".to_string(),
            size: 0,
            modified_time: 1_700_000_000,
            parents: vec!["root".to_string()],
            trashed: false,
            web_view_link: None,
        };
        let remote_a = crate::api::DriveFile {
            id: "remote_a_1".to_string(),
            name: "alpha.txt".to_string(),
            mime_type: "text/plain".to_string(),
            size: 10,
            modified_time: 1_700_000_000,
            parents: vec!["root".to_string()],
            trashed: false,
            web_view_link: None,
        };
        let remote_b = crate::api::DriveFile {
            id: "remote_b_1".to_string(),
            name: "beta.txt".to_string(),
            mime_type: "text/plain".to_string(),
            size: 10,
            modified_time: 1_700_000_000,
            parents: vec!["root".to_string()],
            trashed: false,
            web_view_link: None,
        };

        let mut fs = make_test_fs(
            "http://127.0.0.1:9",
            rt.handle().clone(),
            DEFAULT_READ_CHUNK_SIZE,
        )
        .with_read_only(false)
        .with_staging_dir(staging_dir.clone());

        fs.cache
            .sync_directory_children(FUSE_ROOT_ID, &[gdoc, remote_a, remote_b])
            .unwrap();

        // 1. `handle_create` with `O_CREAT | O_RDONLY` on an existing Google Workspace document
        // must succeed for read-only access while rejecting write/truncate flags with EACCES.
        let (gdoc_attr, gdoc_fh) = fs
            .handle_create(FUSE_ROOT_ID, "Plan.gdoc", 0o644, libc::O_CREAT | libc::O_RDONLY)
            .expect("open(O_CREAT | O_RDONLY) on existing Google Doc must succeed");
        fs.handle_release(gdoc_attr.ino, gdoc_fh).unwrap();
        assert_eq!(
            fs.handle_create(FUSE_ROOT_ID, "Plan.gdoc", 0o644, libc::O_CREAT | libc::O_WRONLY),
            Err(libc::EACCES)
        );

        // 2. Renaming a pending regular file to `Plan.gdoc` when native Google Doc `Plan` exists
        // must NOT trash or remove the native Google Doc; both must coexist via disambiguation.
        let (pending_attr, pending_fh) = fs
            .handle_create(FUSE_ROOT_ID, "notes.txt", 0o644, libc::O_CREAT | libc::O_RDWR)
            .unwrap();
        fs.handle_write(pending_attr.ino, pending_fh, 0, b"local notes", 0)
            .unwrap();
        fs.handle_rename(FUSE_ROOT_ID, "notes.txt", FUSE_ROOT_ID, "Plan.gdoc", 0)
            .expect("renaming pending regular file to Plan.gdoc must succeed without trashing native doc");
        assert!(
            fs.cache.get_by_drive_id("gdoc_plan_1").unwrap().is_some(),
            "Native Google Doc must not be removed when a regular file is renamed to Plan.gdoc"
        );
        assert!(fs.handle_lookup(FUSE_ROOT_ID, "Plan.gdoc").is_ok());
        assert!(fs.handle_lookup(FUSE_ROOT_ID, "Plan (1).gdoc").is_ok());

        // 3. Overwrite rename where the source remote update fails (unreachable server `127.0.0.1:9`)
        // must NOT trash or remove the existing destination (`beta.txt`).
        let rename_err = fs.handle_rename(FUSE_ROOT_ID, "alpha.txt", FUSE_ROOT_ID, "beta.txt", 0);
        assert!(rename_err.is_err());
        assert!(
            fs.handle_lookup(FUSE_ROOT_ID, "beta.txt").is_ok(),
            "Destination file beta.txt must remain intact when source rename fails"
        );
        assert!(
            fs.handle_lookup(FUSE_ROOT_ID, "alpha.txt").is_ok(),
            "Source file alpha.txt must remain intact when source rename fails"
        );

        // 4. `handle_unlink` on a file with an open handle must remove the directory entry immediately
        // (`lookup` -> ENOENT) while preserving the inode and staged data until the final `handle_release`.
        let (temp_attr, temp_fh) = fs
            .handle_create(FUSE_ROOT_ID, "temp_open.txt", 0o644, libc::O_CREAT | libc::O_RDWR)
            .unwrap();
        fs.handle_write(temp_attr.ino, temp_fh, 0, b"open-unlink", 0)
            .unwrap();
        fs.handle_unlink(FUSE_ROOT_ID, "temp_open.txt")
            .expect("unlink of open file must succeed");
        assert_eq!(
            fs.handle_lookup(FUSE_ROOT_ID, "temp_open.txt"),
            Err(libc::ENOENT),
            "Directory entry must be gone immediately after unlink"
        );
        // Reading, writing, and flushing via the still-open handle must succeed:
        fs.handle_write(temp_attr.ino, temp_fh, 11, b" works", 0)
            .expect("write on unlinked open handle must succeed");
        let read_open = fs
            .handle_read(temp_attr.ino, 0, 64)
            .expect("read on unlinked open handle must succeed");
        assert_eq!(&read_open[..], b"open-unlink works");
        fs.handle_flush(temp_attr.ino, temp_fh)
            .expect("flush on unlinked open file must succeed without uploading");
        // Final release removes the staging file and the inode from SQLite:
        fs.handle_release(temp_attr.ino, temp_fh)
            .expect("final release on unlinked file must succeed");
        assert!(fs.cache.get_by_inode(temp_attr.ino).unwrap().is_none());
        assert!(!fs.staging_file_path(temp_attr.ino).exists());

        // 5. `handle_setattr` size truncation with no open handle when the upload fails must preserve
        // the staged file and state so subsequent reads see the new local bytes rather than losing them.
        let alpha_attr = fs.handle_lookup(FUSE_ROOT_ID, "alpha.txt").unwrap();
        let setattr_err = fs.handle_setattr(alpha_attr.ino, Some(0), None, None);
        assert!(setattr_err.is_err());
        assert!(
            fs.staging_file_path(alpha_attr.ino).exists(),
            "Staged file must be preserved when immediate setattr upload fails"
        );
        let alpha_bytes = fs.handle_read(alpha_attr.ino, 0, 64).unwrap();
        assert!(alpha_bytes.is_empty());

        // 6. Restart / 410-recovery staging preservation: simulate a new GDriveFS instance attached to
        // the same cache and staging_dir while `pending_attr.ino` is dirty on disk.
        let auth = Arc::new(AuthManager::new_static_for_test("test-token"));
        let api = Arc::new(
            DriveClient::with_base_url(auth, "http://127.0.0.1:9".to_string()).unwrap(),
        );
        let mut restarted_fs = GDriveFS::with_phase3_config(
            api,
            fs.cache.clone(),
            rt.handle().clone(),
            DEFAULT_ATTR_TTL,
            DEFAULT_READ_CHUNK_SIZE,
            GoogleDocsMode::Browser,
            true,
            false,
            staging_dir.clone(),
        );
        let recovered_bytes = restarted_fs
            .handle_read(pending_attr.ino, 0, 64)
            .expect("reading recovered dirty file after restart must succeed");
        assert_eq!(&recovered_bytes[..], b"local notes");
        // Partial write at offset 6 must modify the recovered staging file without truncating bytes 0..6:
        restarted_fs
            .handle_write(pending_attr.ino, 1, 6, b"NOTES!", 0)
            .expect("partial write on recovered dirty file must succeed");
        let after_partial_write = restarted_fs
            .handle_read(pending_attr.ino, 0, 64)
            .unwrap();
        assert_eq!(&after_partial_write[..], b"local NOTES!");

        // 7. Verify staging directory (`0700`) and staging file (`0600`) owner-only permissions on Unix.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let dir_mode = std::fs::metadata(&staging_dir).unwrap().permissions().mode() & 0o777;
            assert_eq!(dir_mode, 0o700, "staging directory must be mode 0700");
            let file_mode = std::fs::metadata(restarted_fs.staging_file_path(pending_attr.ino))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(file_mode, 0o600, "staging file must be mode 0600");
        }

        // 8. Size-changing `handle_setattr` (`ftruncate`) with an `O_RDONLY` file handle must fail
        // with `EINVAL`, while an `O_RDWR` file handle succeeds.
        let ro_fh = restarted_fs
            .handle_open_with_fh(pending_attr.ino, libc::O_RDONLY)
            .unwrap();
        assert_eq!(
            restarted_fs.handle_setattr(pending_attr.ino, Some(5), None, Some(ro_fh)),
            Err(libc::EINVAL),
            "ftruncate via an O_RDONLY handle must be rejected with EINVAL"
        );
        restarted_fs.handle_release(pending_attr.ino, ro_fh).unwrap();

        let rw_fh = restarted_fs
            .handle_open_with_fh(pending_attr.ino, libc::O_RDWR)
            .unwrap();
        let trunc_attr = restarted_fs
            .handle_setattr(pending_attr.ino, Some(5), None, Some(rw_fh))
            .expect("ftruncate via an O_RDWR handle must succeed");
        assert_eq!(trunc_attr.size, 5);

        // 9. Renaming a Google Workspace native document to `Foo.gdoc` (whose stripped effective name is `Foo`)
        // when a regular sibling named `Foo` exists must NOT delete the regular sibling `Foo` from SQLite.
        let (foo_reg_attr, _foo_fh) = restarted_fs
            .handle_create(FUSE_ROOT_ID, "Foo", 0o644, libc::O_CREAT | libc::O_RDWR)
            .unwrap();
        let gdoc_ino = restarted_fs
            .cache
            .get_by_drive_id("gdoc_plan_1")
            .unwrap()
            .unwrap()
            .inode;
        let renamed_gdoc_drive = crate::api::DriveFile {
            id: "gdoc_plan_1".to_string(),
            name: "Foo".to_string(),
            mime_type: "application/vnd.google-apps.document".to_string(),
            size: 0,
            modified_time: 1_700_000_100,
            parents: vec!["root".to_string()],
            trashed: false,
            web_view_link: None,
        };
        restarted_fs
            .cache
            .rename_entry(
                FUSE_ROOT_ID,
                gdoc_ino,
                FUSE_ROOT_ID,
                "Foo",
                Some(&renamed_gdoc_drive),
            )
            .expect("renaming native doc to Foo alongside regular file Foo must succeed");
        assert_eq!(
            restarted_fs.handle_lookup(FUSE_ROOT_ID, "Foo").unwrap().ino,
            foo_reg_attr.ino,
            "Regular sibling 'Foo' must remain present after native doc is renamed to 'Foo.gdoc'"
        );
        assert_eq!(
            restarted_fs
                .handle_lookup(FUSE_ROOT_ID, "Foo.gdoc")
                .unwrap()
                .ino,
            gdoc_ino,
            "Renamed native doc must be addressable as 'Foo.gdoc'"
        );

        let _ = std::fs::remove_dir_all(&staging_dir);
    }
}
