# `gdrive-fuse-rs` Project Roadmap

This document tracks the development phases, architectural milestones, and feature specifications for `gdrive-fuse-rs` -- a lightweight, memory-safe Google Drive FUSE filesystem written in Rust for Linux and GNOME.

---

## Phase 1: Read-Only MVP (Completed)

**Goal:** Deliver a rock-solid, memory-safe, read-only FUSE mount that streams files on-demand in RAM without consuming local disk space.

- [x] **Layered OAuth 2.0 PKCE Authentication ([`src/auth.rs`](src/auth.rs))**
  - [x] Support CLI flags (`--client-id`, `--client-secret`), environment variables (`GDRIVE_CLIENT_ID`, `GDRIVE_CLIENT_SECRET`), `~/.config/gdrive-fuse-rs/config.toml`, system keyring fallback, and compile-time defaults (`GDRIVE_DEFAULT_CLIENT_ID`).
  - [x] Interactive **"Sign in with Google"** browser auto-launch (`open`) with an ephemeral loopback listener (`127.0.0.1:0`) and CSRF state validation.
  - [x] Secure refresh token persistence in **GNOME Keyring** (`keyring` crate) and automatic in-memory access token refreshing (`60s` expiry margin).
- [x] **Google Drive REST API v3 Client ([`src/api.rs`](src/api.rs))**
  - [x] Directory listing (`pageSize = 1000`, `trashed = false`) with full `nextPageToken` pagination.
  - [x] Preserve Google Workspace native documents (`application/vnd.google-apps.*`) in directory listings (Option B).
  - [x] On-demand byte-range streaming (`alt=media` with `Range: bytes=start-end`) returning `bytes::Bytes`.
- [x] **Embedded SQLite Metadata & Inode Cache ([`src/cache.rs`](src/cache.rs))**
  - [x] Persistent `u64` inode to Drive `file_id` bidirectional mapping in WAL mode (`inode = 1` pinned to `"root"`).
  - [x] Deterministic sibling filename collision disambiguation (`report (1).pdf`, `Notes (1)`).
  - [x] Transactional directory child updates and configurable directory TTL tracking.
- [x] **Read-Only FUSE Filesystem ([`src/filesystem.rs`](src/filesystem.rs) & [`src/main.rs`](src/main.rs))**
  - [x] Pure-Rust Linux `fuser` session with clean `Ctrl+C` (`SIGINT`) auto-unmount without requiring `/etc/fuse.conf` `user_allow_other`.
  - [x] **4 MB In-Memory Read-Ahead Chunk Buffer** (`ReadChunkBuffer`, LRU across open files) to serve sequential kernel reads with minimal HTTP requests.
  - [x] Strict POSIX error code mapping (`libc::ENOENT`, `libc::EACCES`, `libc::EAGAIN`, `libc::EISDIR`, `libc::ENOTDIR`, `libc::EROFS`, `libc::EIO`).

---

## Phase 2: GNOME Desktop Integration & Smart Sync (Completed)

**Goal:** Elevate the read-only mount into a seamless Linux/GNOME desktop experience with clickable Google Docs, accurate disk quota reporting, real-time change polling, "Shared with me" access, and background service management.

### 2.1 Clickable Google Workspace Native Files (Docs, Sheets, Slides)
- [x] **Browser Shortcut Mode (Default -- `--google-docs-mode=browser` / `.desktop` or `.html`):**
  - Synthesize lightweight, clickable launcher content (`webViewLink` / `https://docs.google.com/...`) for native `application/vnd.google-apps.*` files so double-clicking a Google Doc, Sheet, or Slide in **GNOME Files (Nautilus)** opens it directly in the user's default web browser (matching the Windows Google Drive app behavior).
  - Automatically report the exact byte length of the synthesized shortcut in `getattr` (`FileAttr::size`) so kernel `read()` calls stream the shortcut cleanly from memory.
- [x] **On-the-Fly Export Mode (Optional -- `--google-docs-mode=pdf|office`):**
  - Support exporting Google Docs/Sheets/Slides via `GET /drive/v3/files/{fileId}/export` as `.pdf` or Office formats (`.docx`, `.xlsx`, `.pptx`), caching exported bytes in the in-memory chunk buffer.

### 2.2 Real Drive Storage Quota (`statfs`)
- [x] Query `GET /drive/v3/about?fields=storageQuota` (`limit`, `usage`, `usageInDrive`, `usageInDriveTrash`) in [`src/api.rs`](src/api.rs).
- [x] Cache storage quota metrics in memory/SQLite with a configurable TTL (e.g., 5 minutes).
- [x] Populate `fuser::ReplyStatfs` in [`src/filesystem.rs`](src/filesystem.rs) with real `blocks`, `bfree`, and `bavail` counts (using `4096`-byte block size) so `df -h ~/GoogleDrive` and GNOME Files display accurate Total, Used, and Available Drive space.

### 2.3 Background Incremental Change Sync (`changes.list` API)
- [x] Fetch and store the initial `startPageToken` (`GET /drive/v3/changes/startPageToken`) in SQLite.
- [x] Spawn a lightweight background Tokio task that periodically polls `GET /drive/v3/changes` (`pageToken`, `includeItemsFromAllDrives=true`, `supportsAllDrives=true`).
- [x] When remote changes are detected (file created, renamed, modified, moved, or trashed), atomically update or invalidate the affected inodes and parent directories in SQLite so local directory listings stay fresh even with longer directory TTLs.

### 2.4 "Shared with me" & Shared Drive Request Compatibility
- [x] Add `supportsAllDrives=true` and `includeItemsFromAllDrives=true` to Drive API v3 requests so shared items residing on Shared Drives can be listed, exported, and downloaded.
- [x] Expose an optional virtual top-level directory (e.g., `Shared with me` at a reserved virtual inode) populated via `q = "sharedWithMe = true and trashed = false"`.

### 2.5 Systemd User Service & Lifecycle CLI Utilities
- [x] Add CLI utility flags/actions:
  - `--clear-cache`: Wipes `~/.cache/gdrive-fuse-rs/metadata.db` before mounting (or exits).
  - `--logout`: Removes saved OAuth tokens from GNOME Keyring.
  - `--install-systemd` / systemd user service unit (`~/.config/systemd/user/gdrive-fuse-rs.service`) so Google Drive mounts automatically on GNOME login.

---

## Phase 3: Read-Write Support (`create`, `write`, `mkdir`, `unlink`, `rename`) (Completed)

**Goal:** Enable full POSIX file creation, editing, folder management, and safe deletion while preserving memory safety and low disk footprint.

### 3.1 OAuth Scope Upgrade
- [x] Support upgrading OAuth2 scope from `drive.readonly` to `https://www.googleapis.com/auth/drive` when mounted without `--read-only` (while retaining `--read-only` flag for strict read-only mounts).

### 3.2 Write-Back Staging & Resumable Uploads (`create`, `open`, `write`, `flush`, `release`, `setattr`)
- [x] Maintain a bounded temporary staging directory (`~/.cache/gdrive-fuse-rs/staging/`) only for files actively open for writing.
- [x] Implement `create` and `write`: write incoming byte buffers to a local sparse/staging file, mark the inode as `dirty`, and update cached `size` and `mtime`.
- [x] Implement `flush` / `release`: upload dirty staged files to Google Drive using the **Google Drive Resumable Upload API** (`POST/PATCH https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable`), update SQLite metadata with the returned Drive `id`/`modifiedTime`, and delete the local staging file immediately upon upload completion.
- [x] Implement `setattr`: support `size` truncation (`truncate`) and `mtime` updates.

### 3.3 Directory & File Operations (`mkdir`, `rmdir`, `unlink`, `rename`)
- [x] **`mkdir`**: Create remote folder (`mimeType = "application/vnd.google-apps.folder"`) via `POST /drive/v3/files` and insert into SQLite cache.
- [x] **`unlink` & `rmdir` (Safe Trash by Default)**:
  - Move deleted files and folders to Google Drive Trash (`PATCH /drive/v3/files/{fileId}` with `{"trashed": true}`) rather than permanent deletion (`DELETE`), preventing accidental data loss.
- [x] **`rename`**:
  - Support in-directory renames (`PATCH` `name`) and cross-directory moves (`addParents` / `removeParents`) atomically in both Drive API and SQLite `parent_child`.

---

## Phase 4: Reliability, Security & Performance (Completed)

**Goal:** Protect pending writes and credentials, make synchronization failures visible, and improve responsiveness and diagnostics for large Drive accounts.

Prioritize sections 4.1-4.3 before performance tuning.

### 4.1 Protect Pending Writes
- [x] Fail and report an appropriate error if a dirty staging file is missing or unreadable; never replace it with an empty file and upload that in its place.
- [x] Make `--clear-cache` and account-switch cleanup protect pending staged writes and avoid modifying cache files used by an active mount. Require explicit user confirmation before any destructive cleanup.
- [x] Test missing staging files, pending uploads, active mounts, and cleanup failures without losing staged data.

### 4.2 Harden Setup Security
- [x] Do not render stored client secrets in the browser setup form. Authenticate access to the local setup session with a cryptographically random, session-specific secret and validate requests so other local clients cannot retrieve or modify configuration.
- [x] Create credential-bearing configuration files with restrictive permissions from the outset and surface permission failures.
- [x] Test setup access controls, secret handling, and configuration-file permissions.

### 4.3 Make Change Sync Recovery Reliable
- [x] Propagate cache and token persistence failures instead of reporting successful change polling.
- [x] Do not advance the change token when expired-token cache recovery fails; preserve pending writes and retry safely.
- [x] Prevent a directory refresh from overwriting an invalidation that occurred while the Drive listing was in flight.
- [x] Test recovery failures and concurrent refresh/invalidation scenarios.

### 4.4 Improve Folder Responsiveness
- [x] Investigate background refresh of previously cached directory listings so a slow Drive request need not block browsing. Define and gate any stale-data behavior, preserve write and error semantics, and do not expose incomplete listings as complete.
- [x] Use targeted directory-listing invalidation where practical instead of rebuilding unrelated cached listings after every cache mutation.
- [x] Measure cold and warm folder navigation with representative large directories, and test refresh, invalidation, and concurrent changes.

### 4.5 Improve Diagnostics
- [x] Add end-to-end timings for FUSE operations and file reads, distinguishing time spent waiting for Drive, the local cache, and request processing.
- [x] Keep diagnostics opt-in and avoid logging credentials, tokens, file contents, or unnecessary identifying information.
- [x] Test timing and error reporting without requiring a live Drive account.

### 4.6 Documentation & Package Safeguards
- [x] Correct the documented cache TTL default and keep CLI help, configuration documentation, and implementation consistent.
- [x] Prevent unintended embedding of client secrets in release binaries and `.deb` / `.rpm` packages; document safe build and credential configuration.
- [x] Verify package contents and document any intentional effects on per-user GNOME settings.
