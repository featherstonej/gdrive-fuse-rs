use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
    TimeOrNow, FUSE_ROOT_ID,
};
use tokio::runtime::Handle;

use crate::api::{ApiError, DriveClient, StorageQuota, FOLDER_MIME_TYPE};
use crate::auth::GoogleDocsMode;
use crate::cache::{
    CachedFile, MetadataCache, ROOT_DRIVE_ID, ROOT_INODE, SHARED_WITH_ME_DRIVE_ID,
    SHARED_WITH_ME_INODE, SHARED_WITH_ME_NAME,
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

/// Write-related POSIX open flags that must be rejected on a read-only mount.
const WRITE_OPEN_FLAGS: i32 =
    libc::O_WRONLY | libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC | libc::O_APPEND;

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

    /// Returns the length of a cached chunk starting at `offset == 0` for `ino` without mutating access timestamps.
    pub fn peek_zero_offset_len(&self, ino: u64) -> Option<u64> {
        self.chunks
            .iter()
            .find(|c| c.ino == ino && c.offset == 0)
            .map(|c| c.data.len() as u64)
    }

    /// Inserts or replaces the read-ahead chunk for `ino`, evicting the least-recently accessed chunk if full.
    pub fn insert(&mut self, ino: u64, offset: u64, data: Bytes) {
        let now = Instant::now();
        if let Some(existing) = self.chunks.iter_mut().find(|c| c.ino == ino) {
            existing.offset = offset;
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

    let (changes, new_token) = api.list_changes(&token).await?;
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

/// Read-only Google Drive FUSE filesystem implementation.
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

    /// Creates a new `GDriveFS` with full Phase 2 configuration options.
    pub fn with_full_config(
        api: Arc<DriveClient>,
        cache: Arc<MetadataCache>,
        rt_handle: Handle,
        attr_ttl: Duration,
        chunk_size: usize,
        google_docs_mode: GoogleDocsMode,
        shared_with_me: bool,
    ) -> Self {
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        Self {
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
        }
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

    fn ensure_google_doc_exported(&mut self, file: &CachedFile) -> Result<Bytes, libc::c_int> {
        if let Some(existing) = self.chunk_buffer.get_zero_offset_chunk(file.inode) {
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

        self.chunk_buffer.insert(file.inode, 0, exported.clone());
        Ok(exported)
    }

    /// Converts a `CachedFile` record into a `fuser::FileAttr` structure.
    pub fn file_to_attr(&self, file: &CachedFile) -> FileAttr {
        let mtime = unix_secs_to_system_time(file.modified_time);
        let (kind, perm, nlink) = if file.is_dir {
            (FileType::Directory, 0o755, 2)
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
                    .peek_zero_offset_len(file.inode)
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

    /// Resolves `lookup` for a child `name` inside `parent` inode.
    pub fn handle_lookup(&mut self, parent: u64, name: &str) -> Result<FileAttr, libc::c_int> {
        if parent == FUSE_ROOT_ID && self.shared_with_me && name == SHARED_WITH_ME_NAME {
            return self.handle_getattr(SHARED_WITH_ME_INODE);
        }

        self.ensure_dir_synced(parent)?;

        let direct = self.cache.lookup_child(parent, name).map_err(|e| {
            log::error!(
                "Cache error looking up child '{}' in parent inode {}: {}",
                name,
                parent,
                e
            );
            e.to_errno()
        })?;

        let resolved = if let Some(child) = direct {
            Some(child)
        } else {
            self.lookup_google_doc_by_display_name(parent, name)?
        };

        match resolved {
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

    fn lookup_google_doc_by_display_name(
        &self,
        parent: u64,
        name: &str,
    ) -> Result<Option<CachedFile>, libc::c_int> {
        let candidate_exts: &[&str] = match self.google_docs_mode {
            GoogleDocsMode::Browser => &[".gdoc", ".gsheet", ".gslides", ".gdrive"],
            GoogleDocsMode::Desktop => &[".desktop"],
            GoogleDocsMode::Pdf => &[".pdf"],
            GoogleDocsMode::Office => &[".docx", ".xlsx", ".pptx", ".pdf"],
        };

        for ext in candidate_exts {
            if let Some(stripped) = name.strip_suffix(ext) {
                let maybe_child = self.cache.lookup_child(parent, stripped).map_err(|e| {
                    log::error!(
                        "Cache error looking up stripped child '{}' in parent inode {}: {}",
                        stripped,
                        parent,
                        e
                    );
                    e.to_errno()
                })?;

                if let Some(child) = maybe_child {
                    if child.is_google_native_doc()
                        && Self::display_name_for_file(stripped, &child, self.google_docs_mode)
                            == name
                    {
                        return Ok(Some(child));
                    }
                }
            }
        }

        Ok(None)
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

        let children = self.cache.list_children(ino).map_err(|e| {
            log::error!("Cache error listing children for inode {}: {}", ino, e);
            e.to_errno()
        })?;

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

        for entry in children {
            let kind = if entry.file.is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            let display_name = Self::display_name_for_file(
                &entry.effective_name,
                &entry.file,
                self.google_docs_mode,
            );
            entries.push((entry.inode, kind, display_name));
        }

        Ok(entries)
    }

    /// Validates `open` flags and target inode type.
    pub fn handle_open(&self, ino: u64, flags: i32) -> Result<(), libc::c_int> {
        if (flags & WRITE_OPEN_FLAGS) != 0 {
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

    /// Reads up to `size` bytes from `ino` starting at `offset`, utilizing the 4 MB read-ahead chunk buffer.
    pub fn handle_read(&mut self, ino: u64, offset: u64, size: u32) -> Result<Bytes, libc::c_int> {
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

        if let Some(cached_slice) = self.chunk_buffer.get_slice(ino, offset, expected_len) {
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
        self.chunk_buffer.insert(ino, offset, downloaded);

        Ok(result_slice)
    }

    /// Resolves `statfs` storage quota metrics from Google Drive's `about.get` endpoint,
    /// caching the result in memory for `QUOTA_CACHE_TTL` (5 minutes).
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
            // Unlimited Google Workspace account (`limit == 0`): report 1 TiB available
            let free = (1024u64 * 1024 * 1024 * 1024) / STATFS_BSIZE;
            let total = used_blocks.saturating_add(free);
            (total, free)
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

impl Filesystem for GDriveFS {
    fn init(
        &mut self,
        _req: &Request<'_>,
        _config: &mut KernelConfig,
    ) -> Result<(), libc::c_int> {
        log::info!(
            "Initializing gdrive-fuse-rs FUSE filesystem (uid={}, gid={}, chunk_size={} bytes, google_docs_mode={:?}, shared_with_me={})",
            self.uid,
            self.gid,
            self.chunk_size,
            self.google_docs_mode,
            self.shared_with_me
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
        match self.handle_open(ino, flags) {
            Ok(()) => reply.opened(0, 0),
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

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.chunk_buffer.evict(ino);
        reply.ok();
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

    fn access(&mut self, _req: &Request<'_>, ino: u64, _mask: i32, reply: ReplyEmpty) {
        match self.handle_getattr(ino) {
            Ok(_) => reply.ok(),
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
        _size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        log::debug!("Rejecting setattr on inode {} (read-only filesystem)", ino);
        reply.error(libc::EROFS);
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        log::debug!("Rejecting mkdir under parent {} (read-only filesystem)", parent);
        reply.error(libc::EROFS);
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        log::debug!("Rejecting unlink under parent {} (read-only filesystem)", parent);
        reply.error(libc::EROFS);
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        log::debug!("Rejecting rmdir under parent {} (read-only filesystem)", parent);
        reply.error(libc::EROFS);
    }

    fn symlink(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        _link_name: &OsStr,
        _target: &Path,
        reply: ReplyEntry,
    ) {
        log::debug!("Rejecting symlink under parent {} (read-only filesystem)", parent);
        reply.error(libc::EROFS);
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        _name: &OsStr,
        _newparent: u64,
        _newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        log::debug!("Rejecting rename under parent {} (read-only filesystem)", parent);
        reply.error(libc::EROFS);
    }

    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _newparent: u64,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        log::debug!("Rejecting link for inode {} (read-only filesystem)", ino);
        reply.error(libc::EROFS);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _offset: i64,
        _data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        log::debug!("Rejecting write on inode {} (read-only filesystem)", ino);
        reply.error(libc::EROFS);
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        log::debug!("Rejecting create under parent {} (read-only filesystem)", parent);
        reply.error(libc::EROFS);
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
        log::debug!("Rejecting fallocate on inode {} (read-only filesystem)", ino);
        reply.error(libc::EROFS);
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
}
