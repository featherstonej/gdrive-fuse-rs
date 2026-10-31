# Privacy Policy for gdrive-fuse-rs

**Last updated:** October 30, 2026

`gdrive-fuse-rs` is an open-source, local FUSE filesystem utility designed to let users mount their Google Drive on Linux. 

### 1. Information We Collect
`gdrive-fuse-rs` operates entirely locally on your machine. When you authenticate:
- The application requests read-only access to your Google Drive files solely to display them locally via your file manager.
- OAuth2 authentication tokens (refresh and access tokens) are handled securely through your system's native credential manager (e.g., GNOME Keyring / Secret Service API) and are **never** transmitted to any third-party servers.

### 2. Data Storage and Security
- All file metadata caching happens locally in an embedded SQLite database on your machine.
- We do not collect, store, log, or transmit any of your personal files, Google Drive content, or telemetry data to any external server.

### 3. Contact Us
If you have any questions about this Privacy Policy, please open an issue on our [GitHub Repository](https://github.com/featherstonej/gdrive-fuse-rs).
