# gdrive-fuse-rs

A lightweight, memory-safe Google Drive FUSE filesystem written in Rust for Linux and GNOME. Designed as a modern, secure alternative to legacy integrations like `libgdata` and GVFS.

## Tech Stack

- **Language:** Rust (2021 Edition)
- **Filesystem Interface:** `fuser` (Pure-Rust Linux FUSE implementation)
- **API & Async Runtime:** `reqwest` (`rustls` pure-Rust TLS) & `tokio`
- **Authentication:** `oauth2` (PKCE), `open` (Browser Sign-In), & system `keyring` (GNOME Keyring / Secret Service via pure-Rust `zbus`)
- **Metadata Caching:** `rusqlite` (Bundled embedded SQLite with WAL mode)

---

## Installation

### Option 1: Install Native `.deb` or `.rpm` Package (Recommended)

Download the latest `.deb` or `.rpm` package from the [Releases](https://github.com/featherstonej/gdrive-fuse-rs/releases) page. Both SQLite and TLS (`rustls`) are statically bundled inside the binary; the package manager will automatically install `fuse3` (`fusermount3`) if it isn't already present.

* **Debian / Ubuntu / Linux Mint / Pop!_OS (`.deb`):**
  ```bash
  sudo apt install ./gdrive-fuse-rs_*_amd64.deb
  ```

* **Fedora / RHEL / CentOS / openSUSE (`.rpm`):**
  ```bash
  sudo dnf install ./gdrive-fuse-rs-*.x86_64.rpm
  ```

Once installed, you can either launch **Google Drive (gdrive-fuse-rs)** directly from the GNOME Applications menu, or run:

```bash
gdrive-fuse-rs --setup
```

The setup page asks for a one-time code from a private file whose path is shown in the
browser. Open that file with your file manager or `cat`, then paste its contents into
the page. Do not share the code. The optional GNOME Files performance setting affects
all folders. The application does not modify Tracker's ignored-directory settings automatically.

**Warning:** `--clear-cache` requires interactive confirmation (or `--yes`) and refuses
to run while a matching FUSE mount or pending staged write is active. The setup wizard's
account-switch action is explicitly destructive and applies the same staged-write and
exclusive-cache protections.

This signs you in with Google in your browser, enables the background systemd user service (`gdrive-fuse-rs.service`) to auto-mount `~/GoogleDrive` on login, and opens `~/GoogleDrive` in GNOME Files.

The setup wizard offers an optional **Reduce GNOME Files background loading** setting.
It disables folder item counts and thumbnails for **all** folders in GNOME Files for the
current user, not just Google Drive. The change persists after uninstalling the package;
you can change it later in GNOME Files preferences. Existing users can open the wizard
with `gdrive-fuse-rs --configure`. Package installation alone does not change these settings.

---

### Option 2: Building from Source

#### Prerequisites

Before building or running `gdrive-fuse-rs` from source, ensure your Linux system has the `fuse3` runtime utilities (`fusermount3`) installed:

* **Debian / Ubuntu / Mint:**
  
  ```bash
  sudo apt install fuse3 build-essential
  ```

* **Fedora / RHEL / CentOS:**
  
  ```bash
  sudo dnf install fuse3 gcc
  ```

* **Arch Linux:**
  
  ```bash
  sudo pacman -S fuse3 base-devel
  ```

Additionally, ensure a Secret Service provider (such as **GNOME Keyring** or **KeePassXC**) is active on your session so the app can securely store your OAuth2 refresh tokens.

#### Build Binary & Optional `.deb` / `.rpm` Packages

1. Clone the repository:
   
   ```bash
   git clone https://github.com/featherstonej/gdrive-fuse-rs.git
   cd gdrive-fuse-rs
   ```

2. Build the release binary using Cargo:
   
   ```bash
   cargo build --release
   ```
   The compiled binary will be available at `target/release/gdrive-fuse-rs`.

3. *(Optional)* Build redistributable `.deb` and `.rpm` packages locally:
   
   ```bash
   cargo install cargo-deb cargo-generate-rpm
   cargo deb
   cargo generate-rpm
   ```

   Builds may embed a default OAuth client ID from `.env.build` or the
   `GDRIVE_DEFAULT_CLIENT_ID` build environment. Client secrets are not embedded;
   if your OAuth client requires a secret, supply the client ID and secret together
   at runtime through configuration, environment, or keyring.
   Before distributing packages, inspect their contents with `dpkg-deb -c <package.deb>`
   or `rpm -qpl <package.rpm>`. Never add local credential files to package assets.

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
   chunk_size_bytes = 4194304 # maximum: 16777216 (16 MB)
   ```
4. **System Keyring Fallback:**
   Once you authenticate once using CLI flags, environment variables, or `config.toml`, `gdrive-fuse-rs` caches both your client configuration and OAuth refresh token in your system keyring (**GNOME Keyring**), so future mounts only require `--mount-point`.

---

## Usage & Mounting

### Quick Start (Default `~/GoogleDrive`)

Running `gdrive-fuse-rs` with no arguments automatically creates `~/GoogleDrive` (if it doesn't exist) and mounts your Google Drive there:

```bash
gdrive-fuse-rs
```

*(On first run, this will automatically open your default web browser to sign in with your Google Account).*

Or, to sign in, configure automatic background mounting on login via `systemd --user`, and open `~/GoogleDrive` in your file manager in one step:

```bash
gdrive-fuse-rs --setup
```

### Custom Mount Point & Advanced CLI Usage

1. Mount at a custom directory path:
   
   ```bash
   mkdir -p ~/MyCustomDrive
   gdrive-fuse-rs --mount-point ~/MyCustomDrive
   ```

2. To cleanly unmount the filesystem when finished:
   
   ```bash
   fusermount3 -u ~/GoogleDrive || fusermount -u ~/GoogleDrive
   ```

For slow directory listings, enable filesystem debug logging with
`RUST_LOG=gdrive_fuse_rs::filesystem=debug`. After a directory refresh, the log reports
the number of entries and time spent fetching from Drive, updating SQLite, and preparing
display names. Logs can contain Drive identifiers or other sensitive information; review
them before sharing.

### CLI Options

```text
Usage: gdrive-fuse-rs [OPTIONS]

Options:
  -m, --mount-point <PATH>         Local directory path where Google Drive will be mounted (defaults to ~/GoogleDrive)
      --client-id <ID>             Google OAuth2 Client ID [env: GDRIVE_CLIENT_ID=]
      --client-secret <SECRET>     Google OAuth2 Client Secret [env: GDRIVE_CLIENT_SECRET=]
      --config <FILE>              Path to user configuration TOML file [env: GDRIVE_CONFIG_PATH=]
      --cache-db <FILE>            Path to the SQLite metadata cache database [env: GDRIVE_CACHE_DB=]
      --cache-ttl <SECONDS>        Directory metadata cache TTL in seconds (default: 60)
      --chunk-size <BYTES>         Read-ahead chunk buffer size in bytes (default: 4 MB; maximum: 16 MB)
      --google-docs-mode <MODE>    How Google Docs/Sheets/Slides are presented: browser, desktop, pdf, office (default: browser)
      --change-poll-secs <SECONDS> Background changes.list poll interval in seconds (default: 30; 0 to disable)
      --no-shared-with-me          Disable the top-level "Shared with me" virtual directory
      --read-only                  Mount in strict read-only mode (uses `drive.readonly` OAuth scope and rejects writes with EROFS)
      --staging-dir <DIR>          Path to temporary write-back staging directory [env: GDRIVE_STAGING_DIR=] (default: ~/.cache/gdrive-fuse-rs/staging)
      --clear-cache                Safely remove the local SQLite metadata cache after confirmation
      --yes                        Confirm destructive cache cleanup without an interactive prompt
      --stale-while-revalidate     Serve a complete expired cached directory while refreshing it in the background
      --diagnostics                Log opt-in FUSE, cache, Drive, and processing timing metrics
      --logout                     Remove saved OAuth2 tokens and client credentials from the system keyring
      --install-systemd            Install a systemd user service at ~/.config/systemd/user/gdrive-fuse-rs.service
      --setup                      Sign in with Google, enable and start the background systemd user service, and open the mount directory
      --configure                  Open the local browser configuration wizard to customize settings in ~/.config/gdrive-fuse-rs/config.toml
      --open-doc <FILE>            Open a .gdoc/.gsheet/.gslides/.gdrive shortcut file in the default web browser and exit
      --no-browser                 Do not automatically launch the web browser during interactive OAuth2 login
      --allow-other                Allow other system users to access the FUSE mount
  -h, --help                       Print help
  -V, --version                    Print version
```

---

## Project Status

**Phase 1: Read-Only MVP (Complete)**
- Layered OAuth2 PKCE authentication with browser auto-launch & GNOME Keyring storage.
- Google Drive API v3 directory listing (`pageSize = 1000` with full pagination) & HTTP `Range` streaming downloads.
- Embedded SQLite metadata & persistent inode cache (`inode 1 == root`) with automatic sibling duplicate filename disambiguation (`filename (1).ext`).
- Read-only FUSE filesystem with a **4 MB in-memory Read-Ahead Chunk Buffer** to minimize HTTP requests during sequential file reads.

**Phase 2: GNOME Desktop Integration & Smart Sync (Complete)**
- Clickable Google Workspace native files (`--google-docs-mode=browser|desktop|pdf|office`) so double-clicking Google Docs/Sheets/Slides opens them in your browser or exports them as PDF/Office files on the fly.
- Real Google Drive storage quota reporting in `df -h` and GNOME Files (`statfs` via `/drive/v3/about`).
- Background incremental change polling (`/drive/v3/changes`) for automatic SQLite cache invalidation and updates.
- Virtual `"Shared with me"` folder (`inode = 2`) and Shared Drive item request compatibility (`supportsAllDrives=true` / `includeItemsFromAllDrives=true`).
- Lifecycle CLI utilities (`--clear-cache`, `--logout`, `--install-systemd`).

**Phase 3: Read-Write Support (Complete)**
- Automatic OAuth2 scope upgrade to `https://www.googleapis.com/auth/drive` when mounted in read-write mode (with `--read-only` flag preserved for strict read-only mounts).
- Bounded write-back staging directory (`~/.cache/gdrive-fuse-rs/staging/`) for active writes (`create`, `open`, `write`, `setattr` truncation/mtime), uploaded via the **Google Drive Resumable Upload API** (`uploadType=resumable`) on `flush`/`fsync`/`release` and deleted immediately upon upload completion.
- Full directory and file management (`mkdir`, `rmdir`, `unlink` to Google Drive Trash `{"trashed": true}` by default, and atomic in-directory/cross-directory `rename` via `addParents`/`removeParents`).

## License

Licensed under the [GNU General Public License v3.0](LICENSE).
