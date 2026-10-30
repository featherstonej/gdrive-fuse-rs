# gdrive-fuse-rs

A lightweight, memory-safe, read-only Google Drive FUSE filesystem written in Rust for Linux and GNOME. Designed as a modern, secure alternative to legacy integrations like `libgdata` and GVFS.

## Tech Stack

- **Language:** Rust (2021 Edition)
- **Filesystem Interface:** `fuser` (Rust FUSE bindings)
- **API & Async Runtime:** `reqwest` & `tokio`
- **Authentication:** `oauth2` & system `keyring` (GNOME Keyring / Secret Service)
- **Metadata Caching:** `rusqlite` (Embedded SQLite)

---

## Prerequisites

Before building or running `gdrive-fuse-rs`, ensure your Linux system has the necessary FUSE development libraries installed:

* **Debian / Ubuntu / Mint:**
  
  ```bash
  sudo apt install libfuse3-dev pkg-config build-essential
  ```

* **Fedora / RHEL / CentOS:**
  
  ```bash
  sudo dnf install fuse3-devel pkg-config gcc
  ```

* **Arch Linux:**
  
  ```bash
  sudo pacman -S fuse3 base-devel
  ```

Additionally, ensure a Secret Service provider (such as **GNOME Keyring** or **KeePassXC**) is active on your session so the app can securely store your OAuth2 tokens.

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

## Usage & Mounting

1. Create a local mount point directory:
   
   ```bash
   mkdir -p ~/GoogleDrive
   ```

2. Run the binary to authenticate and mount your Google Drive:
   
   ```bash
   ./target/release/gdrive-fuse-rs --mount-point ~/GoogleDrive
   ```
   
   *(On first run, this will prompt an OAuth2 login flow via your default web browser).*

3. To cleanly unmount the filesystem when finished:
   
   ```bash
   fusermount -u ~/GoogleDrive
   ```

---

## Project Status

Currently under active development (**Phase 1: Read-Only MVP**). Contributions, feedback, and issue reports are welcome!

## License

Licensed under the [GNU General Public License v3.0](LICENSE).