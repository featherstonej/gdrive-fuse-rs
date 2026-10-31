# gdrive-fuse-rs

A lightweight, memory-safe, read-only Google Drive FUSE filesystem written in Rust for Linux and GNOME. Designed as a modern, secure alternative to legacy integrations like `libgdata` and GVFS.

## Tech Stack

- **Language:** Rust (2021 Edition)
- **Filesystem Interface:** `fuser` (Pure-Rust Linux FUSE implementation)
- **API & Async Runtime:** `reqwest` & `tokio`
- **Authentication:** `oauth2` (PKCE), `open` (Browser Sign-In), & system `keyring` (GNOME Keyring / Secret Service)
- **Metadata Caching:** `rusqlite` (Embedded SQLite with WAL mode)

---

## Prerequisites

Before running `gdrive-fuse-rs`, ensure your Linux system has the `fuse3` runtime utilities (`fusermount3`) installed:

* **Debian / Ubuntu / Mint:**
  
  ```bash
  sudo apt install fuse3 build-essential pkg-config libssl-dev
  ```

* **Fedora / RHEL / CentOS:**
  
  ```bash
  sudo dnf install fuse3 pkg-config openssl-devel gcc
  ```

* **Arch Linux:**
  
  ```bash
  sudo pacman -S fuse3 base-devel
  ```

Additionally, ensure a Secret Service provider (such as **GNOME Keyring** or **KeePassXC**) is active on your session so the app can securely store your OAuth2 refresh tokens.

---

## Building from Source

1. Clone the repository:
   
   ```bash
   git clone https://github.com/your-username/gdrive-fuse-rs.git
   cd gdrive-fuse-rs
   ```

2. Build the release binary using Cargo:
   
   ```bash
   cargo build --release
   ```

The compiled binary will be available at `target/release/gdrive-fuse-rs`.

---

## Configuration & Authentication

`gdrive-fuse-rs` uses Google's **OAuth 2.0 PKCE** flow with an automatic **"Sign in with Google"** browser launch and local loopback redirect listener (`127.0.0.1`).

You can supply your Google Cloud Desktop OAuth2 `client_id` (and optional `client_secret`) using any of the following methods (resolved in priority order):

1. **CLI Flags:**
   ```bash
   ./target/release/gdrive-fuse-rs --client-id "<CLIENT_ID>" --client-secret "<CLIENT_SECRET>" --mount-point ~/GoogleDrive
   ```
2. **Environment Variables:**
   ```bash
   export GDRIVE_CLIENT_ID="<CLIENT_ID>"
   export GDRIVE_CLIENT_SECRET="<CLIENT_SECRET>"
   ```
3. **Configuration File (`~/.config/gdrive-fuse-rs/config.toml`):**
   ```toml
   client_id = "your-client-id.apps.googleusercontent.com"
   client_secret = "your-client-secret"
   cache_ttl_secs = 60
   chunk_size_bytes = 4194304
   ```
4. **System Keyring Fallback:**
   Once you authenticate once using CLI flags, environment variables, or `config.toml`, `gdrive-fuse-rs` caches both your client configuration and OAuth refresh token in your system keyring (**GNOME Keyring**), so future mounts only require `--mount-point`.

---

## Usage & Mounting

1. Create a local mount point directory:
   
   ```bash
   mkdir -p ~/GoogleDrive
   ```

2. Run the binary to authenticate and mount your Google Drive:
   
   ```bash
   ./target/release/gdrive-fuse-rs --mount-point ~/GoogleDrive
   ```
   
   *(On first run, this will automatically open your default web browser to sign in with your Google Account).*

3. To cleanly unmount the filesystem when finished:
   
   ```bash
   fusermount3 -u ~/GoogleDrive || fusermount -u ~/GoogleDrive
   ```

### CLI Options

```text
Usage: gdrive-fuse-rs [OPTIONS] --mount-point <PATH>

Options:
  -m, --mount-point <PATH>      Local directory path where Google Drive will be mounted
      --client-id <ID>          Google OAuth2 Client ID [env: GDRIVE_CLIENT_ID=]
      --client-secret <SECRET>  Google OAuth2 Client Secret [env: GDRIVE_CLIENT_SECRET=]
      --config <FILE>           Path to user configuration TOML file [env: GDRIVE_CONFIG_PATH=]
      --cache-db <FILE>         Path to the SQLite metadata cache database [env: GDRIVE_CACHE_DB=]
      --cache-ttl <SECONDS>     Directory metadata cache TTL in seconds (default: 60)
      --chunk-size <BYTES>      Read-ahead chunk buffer size in bytes (default: 4 MB)
      --no-browser              Do not automatically launch the web browser during interactive OAuth2 login
      --allow-other             Allow other system users to access the FUSE mount
  -h, --help                    Print help
  -V, --version                 Print version
```

---

## Project Status

**Phase 1: Read-Only MVP (Complete)**
- Layered OAuth2 PKCE authentication with browser auto-launch & GNOME Keyring storage.
- Google Drive API v3 directory listing (`pageSize = 1000` with full pagination) & HTTP `Range` streaming downloads.
- Embedded SQLite metadata & persistent inode cache (`inode 1 == root`) with automatic sibling duplicate filename disambiguation (`filename (1).ext`).
- Read-only FUSE filesystem with a **4 MB in-memory Read-Ahead Chunk Buffer** to minimize HTTP requests during sequential file reads.

## License

Licensed under the [GNU General Public License v3.0](LICENSE).
