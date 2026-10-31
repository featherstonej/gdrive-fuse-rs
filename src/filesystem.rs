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

use crate::api::{DriveClient, FOLDER_MIME_TYPE};
use crate::cache::{CachedFile, MetadataCache, ROOT_DRIVE_ID, ROOT_INODE};

/// Default time-to-live for FUSE attribute and directory entry responses.
pub const DEFAULT_ATTR_TTL: Duration = Duration::from_secs(60);

/// Default read-ahead chunk size in bytes (4 MB).
pub const DEFAULT_READ_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Maximum read-ahead chunk size in bytes (16 MB).
pub const MAX_READ_CHUNK_SIZE: usize = 16 * 1024 * 1024;

/// Standard 512-byte POSIX block size used for reporting `FileAttr::blocks`.
pub const BLOCK_SIZE: u32 = 512;

/// Maximum number of active read-ahead chunks retained in memory simultaneously.
const MAX_CACHED_CHUNKS: usize = 4;

/// Write-related POSIX open flags that must be rejected on a read-only mount.
const WRITE_OPEN_FLAGS: i32 =
    libc::O_WRONLY | libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC | libc::O_APPEND;

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
        }
    }

    /// Converts a `CachedFile` record into a `fuser::FileAttr` structure.
    pub fn file_to_attr(&self, file: &CachedFile) -> FileAttr {
        let mtime = unix_secs_to_system_time(file.modified_time);
        let (kind, perm, nlink) = if file.is_dir {
            (FileType::Directory, 0o555, 2)
        } else {
            (FileType::RegularFile, 0o444, 1)
        };
        let blocks = file.size.div_ceil(BLOCK_SIZE as u64);

        FileAttr {
            ino: file.inode,
            size: file.size,
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
            let children = self
                .rt_handle
                .block_on(self.api.list_children(&dir_file.drive_id))
                .map_err(|api_err| {
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
    pub fn handle_getattr(&self, ino: u64) -> Result<FileAttr, libc::c_int> {
        match self.cache.get_by_inode(ino) {
            Ok(Some(file)) => Ok(self.file_to_attr(&file)),
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
    pub fn handle_lookup(&self, parent: u64, name: &str) -> Result<FileAttr, libc::c_int> {
        self.ensure_dir_synced(parent)?;

        match self.cache.lookup_child(parent, name) {
            Ok(Some(child)) => Ok(self.file_to_attr(&child)),
            Ok(None) => Err(libc::ENOENT),
            Err(e) => {
                log::error!(
                    "Cache error looking up child '{}' in parent inode {}: {}",
                    name,
                    parent,
                    e
                );
                Err(e.to_errno())
            }
        }
    }

    /// Resolves `readdir` for `ino`, returning `.` and `..` followed by all cached children.
    pub fn handle_readdir(&self, ino: u64) -> Result<Vec<(u64, FileType, String)>, libc::c_int> {
        self.ensure_dir_synced(ino)?;

        let parent_ino = self.cache.get_parent_inode(ino).map_err(|e| {
            log::error!("Cache error resolving parent for inode {}: {}", ino, e);
            e.to_errno()
        })?;

        let children = self.cache.list_children(ino).map_err(|e| {
            log::error!("Cache error listing children for inode {}: {}", ino, e);
            e.to_errno()
        })?;

        let mut entries = Vec::with_capacity(children.len() + 2);
        entries.extend([
            (ino, FileType::Directory, ".".to_string()),
            (parent_ino, FileType::Directory, "..".to_string()),
        ]);

        for entry in children {
            let kind = if entry.file.is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            entries.push((entry.inode, kind, entry.effective_name));
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

        // Google Workspace native documents have size == 0 (Option B) and return immediate EOF.
        if file.is_google_native_doc() || file.size == 0 || offset >= file.size || size == 0 {
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
            "Initializing gdrive-fuse-rs FUSE filesystem (uid={}, gid={}, chunk_size={} bytes)",
            self.uid,
            self.gid,
            self.chunk_size
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
        reply.statfs(0, 0, 0, 0, 0, 4096, 255, 0);
    }

    fn access(&mut self, _req: &Request<'_>, ino: u64, mask: i32, reply: ReplyEmpty) {
        if (mask & libc::W_OK) != 0 {
            reply.error(libc::EROFS);
            return;
        }

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
        let auth = Arc::new(AuthManager::new_static_for_test("test-token"));
        let api = Arc::new(
            DriveClient::with_base_url(auth, base_url).expect("Failed to create test DriveClient"),
        );
        let cache =
            Arc::new(MetadataCache::open_in_memory(60).expect("Failed to create test cache"));
        GDriveFS::with_config(api, cache, rt_handle, DEFAULT_ATTR_TTL, chunk_size)
    }

    #[test]
    fn test_getattr_root_inode_and_file_to_attr() {
        let rt = Runtime::new().unwrap();
        let fs = make_test_fs("http://127.0.0.1:9", rt.handle().clone(), DEFAULT_READ_CHUNK_SIZE);

        let root_attr = fs
            .handle_getattr(FUSE_ROOT_ID)
            .expect("getattr on root inode 1 must succeed");
        assert_eq!(root_attr.ino, FUSE_ROOT_ID);
        assert_eq!(root_attr.kind, FileType::Directory);
        assert_eq!(root_attr.perm, 0o555);
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
        };
        let attr = fs.file_to_attr(&sample_file);
        assert_eq!(attr.ino, 42);
        assert_eq!(attr.size, 1025);
        assert_eq!(attr.blocks, 3); // ceil(1025 / 512) = 3
        assert_eq!(attr.kind, FileType::RegularFile);
        assert_eq!(attr.perm, 0o444);
        assert_eq!(attr.nlink, 1);
        assert_eq!(
            attr.mtime,
            UNIX_EPOCH + Duration::from_secs(1_700_000_100)
        );
    }

    #[test]
    fn test_lookup_and_readdir_with_mock_server() {
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
                }
            });

            (addr, handle)
        });

        let base_url = format!("http://{}/drive/v3", addr);
        let mut fs = make_test_fs(&base_url, rt.handle().clone(), DEFAULT_READ_CHUNK_SIZE);

        let entries = fs
            .handle_readdir(FUSE_ROOT_ID)
            .expect("readdir on root must succeed");
        assert_eq!(entries.len(), 6); // ".", "..", plus 4 Drive items
        assert_eq!(entries[0], (FUSE_ROOT_ID, FileType::Directory, ".".to_string()));
        assert_eq!(entries[1], (FUSE_ROOT_ID, FileType::Directory, "..".to_string()));
        assert_eq!(entries[2].2, "Photos");
        assert_eq!(entries[2].1, FileType::Directory);
        assert_eq!(entries[3].2, "report.pdf");
        assert_eq!(entries[4].2, "report (1).pdf");
        assert_eq!(entries[5].2, "Roadmap");

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
            .handle_lookup(FUSE_ROOT_ID, "Roadmap")
            .expect("lookup Google Doc must succeed");
        assert_eq!(gdoc_attr.size, 0);

        // Reading a Google Doc returns 0 bytes (EOF) with zero HTTP calls
        let gdoc_bytes = fs
            .handle_read(gdoc_attr.ino, 0, 4096)
            .expect("reading Google Doc should return EOF");
        assert!(gdoc_bytes.is_empty());

        rt.block_on(server_handle).unwrap();
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
