#[allow(dead_code)]
mod api;
#[allow(dead_code)]
mod auth;
#[allow(dead_code)]
mod cache;
#[allow(dead_code)]
mod filesystem;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use fuser::{MountOption, Session};

use crate::api::DriveClient;
use crate::auth::{
    clear_keyring_credentials, resolve_credentials, AppConfig, AuthManager, GoogleDocsMode,
};
use crate::cache::MetadataCache;
use crate::filesystem::{
    sync_changes_once, GDriveFS, DEFAULT_READ_CHUNK_SIZE, MAX_READ_CHUNK_SIZE,
};

/// Default polling interval (in seconds) for Google Drive `changes.list` incremental sync.
pub const DEFAULT_CHANGE_POLL_SECS: u64 = 30;

/// A lightweight, memory-safe, read-only Google Drive FUSE filesystem for Linux and GNOME.
#[derive(Parser, Debug, Clone)]
#[command(name = "gdrive-fuse-rs", version, about, long_about = None)]
pub struct CliArgs {
    /// Local directory path where Google Drive will be mounted.
    #[arg(short = 'm', long = "mount-point", value_name = "PATH")]
    pub mount_point: Option<PathBuf>,

    /// Google OAuth2 Client ID (overrides config file and system keyring).
    #[arg(long = "client-id", env = "GDRIVE_CLIENT_ID", value_name = "ID")]
    pub client_id: Option<String>,

    /// Google OAuth2 Client Secret (overrides config file and system keyring).
    #[arg(
        long = "client-secret",
        env = "GDRIVE_CLIENT_SECRET",
        value_name = "SECRET"
    )]
    pub client_secret: Option<String>,

    /// Path to user configuration TOML file (defaults to ~/.config/gdrive-fuse-rs/config.toml).
    #[arg(long = "config", env = "GDRIVE_CONFIG_PATH", value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Path to the SQLite metadata cache database (defaults to ~/.cache/gdrive-fuse-rs/metadata.db).
    #[arg(long = "cache-db", env = "GDRIVE_CACHE_DB", value_name = "FILE")]
    pub cache_db: Option<PathBuf>,

    /// Directory metadata cache TTL in seconds.
    #[arg(long = "cache-ttl", value_name = "SECONDS")]
    pub cache_ttl: Option<u64>,

    /// Read-ahead chunk buffer size in bytes (defaults to 4 MB; maximum 16 MB).
    #[arg(
        long = "chunk-size",
        value_name = "BYTES",
        value_parser = parse_chunk_size
    )]
    pub chunk_size: Option<usize>,

    /// Presentation mode for Google Workspace native documents (browser, desktop, pdf, office).
    #[arg(long = "google-docs-mode", value_enum, value_name = "MODE")]
    pub google_docs_mode: Option<GoogleDocsMode>,

    /// Background polling interval in seconds for incremental Drive changes (0 to disable; default 30).
    #[arg(long = "change-poll-secs", value_name = "SECONDS")]
    pub change_poll_secs: Option<u64>,

    /// Disable the virtual top-level "Shared with me" directory.
    #[arg(long = "no-shared-with-me", default_value_t = false)]
    pub no_shared_with_me: bool,

    /// Delete the local SQLite metadata cache database (and WAL/SHM files) before exiting or mounting.
    #[arg(long = "clear-cache", default_value_t = false)]
    pub clear_cache: bool,

    /// Remove stored OAuth2 tokens and client credentials from the system keyring.
    #[arg(long = "logout", default_value_t = false)]
    pub logout: bool,

    /// Generate and install a user systemd service unit at ~/.config/systemd/user/gdrive-fuse-rs.service.
    #[arg(long = "install-systemd", default_value_t = false)]
    pub install_systemd: bool,

    /// Open a `.gdoc`, `.gsheet`, `.gslides`, or `.gdrive` shortcut file in the default web browser.
    #[arg(long = "open-doc", value_name = "FILE")]
    pub open_doc: Option<PathBuf>,

    /// Do not automatically launch the web browser during interactive OAuth2 login.
    #[arg(long = "no-browser", default_value_t = false)]
    pub no_browser: bool,

    /// Allow other system users to access the FUSE mount (requires `user_allow_other` in /etc/fuse.conf).
    #[arg(long = "allow-other", default_value_t = false)]
    pub allow_other: bool,
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = CliArgs::parse();

    if let Some(ref doc_path) = args.open_doc {
        return open_google_doc_shortcut(doc_path);
    }

    let config = match &args.config {
        Some(path) => {
            if !path.exists() {
                bail!("Configuration file does not exist: {}", path.display());
            }
            AppConfig::load_from_path(path)
                .with_context(|| format!("Failed to load configuration from {}", path.display()))?
        }
        None => AppConfig::load_default().context("Failed to load default configuration")?,
    };

    if args.logout {
        clear_keyring_credentials()
            .context("Failed to clear OAuth2 credentials from system keyring")?;
        log::info!("Cleared stored Google Drive OAuth2 credentials from system keyring.");
        if args.mount_point.is_none() && !args.clear_cache && !args.install_systemd {
            return Ok(());
        }
    }

    if args.clear_cache {
        let db_path = args
            .cache_db
            .clone()
            .or_else(|| config.cache_db_path.clone())
            .unwrap_or_else(MetadataCache::default_db_path);
        MetadataCache::remove_db_files(&db_path).with_context(|| {
            format!(
                "Failed to remove SQLite metadata cache at {}",
                db_path.display()
            )
        })?;
        log::info!("Removed SQLite metadata cache at {}.", db_path.display());
        if args.mount_point.is_none() && !args.install_systemd {
            return Ok(());
        }
    }

    if args.install_systemd {
        let exe_path = std::env::current_exe()
            .unwrap_or_else(|_| PathBuf::from("/usr/local/bin/gdrive-fuse-rs"));
        let unit_mount_point = args.mount_point.clone().unwrap_or_else(|| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join("GoogleDrive")
        });
        let unit_contents = generate_systemd_unit(&exe_path, &unit_mount_point);
        let config_dir = dirs::config_dir()
            .context("Could not determine user ~/.config directory for systemd unit")?;
        let unit_dir = config_dir.join("systemd").join("user");
        std::fs::create_dir_all(&unit_dir).with_context(|| {
            format!(
                "Failed to create systemd user unit directory {}",
                unit_dir.display()
            )
        })?;
        let unit_path = unit_dir.join("gdrive-fuse-rs.service");
        std::fs::write(&unit_path, unit_contents).with_context(|| {
            format!(
                "Failed to write systemd unit file to {}",
                unit_path.display()
            )
        })?;
        log::info!(
            "Installed systemd user unit to {}. Run:\n  systemctl --user daemon-reload && systemctl --user enable --now gdrive-fuse-rs.service",
            unit_path.display()
        );
        if args.mount_point.is_none() {
            return Ok(());
        }
    }

    let mount_point = args
        .mount_point
        .context("Missing required --mount-point <PATH> argument")?;

    if !mount_point.exists() {
        bail!(
            "Mount point directory does not exist: {}",
            mount_point.display()
        );
    }
    if !mount_point.is_dir() {
        bail!(
            "Mount point path is not a directory: {}",
            mount_point.display()
        );
    }

    let credentials =
        resolve_credentials(args.client_id.clone(), args.client_secret.clone(), &config)
            .context("Failed to resolve Google OAuth2 client credentials")?;

    let cache_ttl_secs = args.cache_ttl.or(config.cache_ttl_secs).unwrap_or(60);
    let chunk_size_bytes = resolve_chunk_size(args.chunk_size, config.chunk_size_bytes)?;
    let google_docs_mode = args
        .google_docs_mode
        .or(config.google_docs_mode)
        .unwrap_or_default();
    let shared_with_me = if args.no_shared_with_me {
        false
    } else {
        config.shared_with_me.unwrap_or(true)
    };
    let change_poll_secs = args
        .change_poll_secs
        .or(config.change_poll_secs)
        .unwrap_or(DEFAULT_CHANGE_POLL_SECS);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Failed to initialize Tokio async runtime")?;

    log::info!("Authenticating with Google Drive...");
    let auth_manager = rt
        .block_on(AuthManager::authenticate(credentials, !args.no_browser))
        .context("Google OAuth2 authentication failed")?;
    let auth_manager = Arc::new(auth_manager);

    let cache = match args.cache_db.as_ref().or(config.cache_db_path.as_ref()) {
        Some(db_path) => MetadataCache::open(db_path, cache_ttl_secs).with_context(|| {
            format!(
                "Failed to open SQLite metadata cache at {}",
                db_path.display()
            )
        })?,
        None => MetadataCache::open_default(cache_ttl_secs)
            .context("Failed to open default SQLite metadata cache")?,
    };
    let cache = Arc::new(cache);

    let api_client =
        Arc::new(DriveClient::new(auth_manager).context("Failed to initialize Drive API client")?);

    if change_poll_secs > 0 {
        let api_bg = Arc::clone(&api_client);
        let cache_bg = Arc::clone(&cache);
        rt.spawn(async move {
            if let Err(e) = sync_changes_once(&api_bg, &cache_bg).await {
                log::warn!("Initial Drive changes.list token sync failed: {}", e);
            }
            let mut ticker = tokio::time::interval(Duration::from_secs(change_poll_secs));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if let Err(e) = sync_changes_once(&api_bg, &cache_bg).await {
                    log::warn!("Background Drive changes.list poll failed: {}", e);
                }
            }
        });
    }

    if google_docs_mode == GoogleDocsMode::Browser {
        if let Ok(exe_path) = std::env::current_exe() {
            if let Err(e) = install_gdrive_mime_handlers(&exe_path) {
                log::debug!("Could not register user XDG MIME handler for .gdoc/.gsheet/.gslides: {}", e);
            }
        }
    }

    let fs = GDriveFS::with_full_config(
        api_client,
        cache,
        rt.handle().clone(),
        Duration::from_secs(cache_ttl_secs),
        chunk_size_bytes,
        google_docs_mode,
        shared_with_me,
    );

    let mut mount_options = vec![
        MountOption::FSName("gdrive".to_string()),
        MountOption::Subtype("gdrive-fuse-rs".to_string()),
    ];
    if args.allow_other {
        mount_options.push(MountOption::AllowOther);
        mount_options.push(MountOption::AutoUnmount);
    }

    log::info!(
        "Mounting Google Drive (read-only) at {}... (Press Ctrl+C to unmount)",
        mount_point.display()
    );
    let mut session = Session::new(fs, &mount_point, &mount_options).with_context(|| {
        format!(
            "Failed to mount FUSE filesystem at {}",
            mount_point.display()
        )
    })?;

    let mut unmounter = session.unmount_callable();
    rt.spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            log::info!("Received Ctrl+C; unmounting filesystem...");
            if let Err(e) = unmounter.unmount() {
                log::warn!("Error while unmounting on Ctrl+C: {}", e);
            }
        }
    });

    session.run().with_context(|| {
        format!(
            "Error running FUSE session at {}",
            mount_point.display()
        )
    })?;

    log::info!("Unmounted {} cleanly.", mount_point.display());
    Ok(())
}

/// Parses a `.gdoc`, `.gsheet`, `.gslides`, or `.gdrive` shortcut file and opens its `"url"` in the default browser.
pub fn extract_url_from_shortcut(content: &str) -> Result<String> {
    let parsed: serde_json::Value =
        serde_json::from_str(content).context("Invalid JSON in Google Drive shortcut file")?;
    let url = parsed
        .get("url")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| s.starts_with("https://") || s.starts_with("http://"))
        .context("Google Drive shortcut file is missing a valid 'url' field")?;
    Ok(url.to_string())
}

fn open_google_doc_shortcut(path: &Path) -> Result<()> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read shortcut file {}", path.display()))?;
    let url = extract_url_from_shortcut(&content)
        .with_context(|| format!("Failed to parse URL from {}", path.display()))?;
    log::info!("Opening Google Workspace document in browser: {}", url);
    open::that(&url).with_context(|| format!("Failed to launch default browser for {}", url))?;
    Ok(())
}

/// Installs per-user XDG MIME definitions (`~/.local/share/mime/packages/gdrive-fuse-rs.xml`)
/// and a desktop handler (`~/.local/share/applications/gdrive-fuse-rs-open.desktop`) so double-clicking
/// `.gdoc`, `.gsheet`, `.gslides`, and `.gdrive` files in GNOME Files opens the user's web browser
/// rather than a text/code editor.
fn install_gdrive_mime_handlers(exe_path: &Path) -> Result<()> {
    let data_dir = dirs::data_dir().context("Could not determine XDG data directory")?;
    let mime_dir = data_dir.join("mime");
    let mime_pkg_dir = mime_dir.join("packages");
    let apps_dir = data_dir.join("applications");

    std::fs::create_dir_all(&mime_pkg_dir)?;
    std::fs::create_dir_all(&apps_dir)?;

    let mime_xml_path = mime_pkg_dir.join("gdrive-fuse-rs.xml");
    let mime_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<mime-info xmlns="http://www.freedesktop.org/standards/shared-mime-info">
  <mime-type type="application/x-gdrive-doc">
    <comment>Google Docs Document</comment>
    <generic-icon name="x-office-document"/>
    <glob pattern="*.gdoc"/>
  </mime-type>
  <mime-type type="application/x-gdrive-sheet">
    <comment>Google Sheets Spreadsheet</comment>
    <generic-icon name="x-office-spreadsheet"/>
    <glob pattern="*.gsheet"/>
  </mime-type>
  <mime-type type="application/x-gdrive-slides">
    <comment>Google Slides Presentation</comment>
    <generic-icon name="x-office-presentation"/>
    <glob pattern="*.gslides"/>
  </mime-type>
  <mime-type type="application/x-gdrive-link">
    <comment>Google Drive Web Shortcut</comment>
    <generic-icon name="text-html"/>
    <glob pattern="*.gdrive"/>
  </mime-type>
</mime-info>
"#;

    let desktop_path = apps_dir.join("gdrive-fuse-rs-open.desktop");
    let exe_str = exe_path.display();
    let desktop_entry = format!(
        "[Desktop Entry]\n\
         Version=1.0\n\
         Type=Application\n\
         Name=Google Drive Web Opener\n\
         Comment=Open Google Docs, Sheets, and Slides in your default web browser\n\
         Exec=\"{exe_str}\" --open-doc %f\n\
         Icon=web-browser\n\
         Terminal=false\n\
         NoDisplay=true\n\
         MimeType=application/x-gdrive-doc;application/x-gdrive-sheet;application/x-gdrive-slides;application/x-gdrive-link;\n"
    );

    let needs_mime_update = std::fs::read_to_string(&mime_xml_path).ok().as_deref() != Some(mime_xml);
    let needs_desktop_update =
        std::fs::read_to_string(&desktop_path).ok().as_deref() != Some(desktop_entry.as_str());

    if needs_mime_update {
        std::fs::write(&mime_xml_path, mime_xml)?;
        let _ = std::process::Command::new("update-mime-database")
            .arg(&mime_dir)
            .status();
    }

    if needs_desktop_update {
        std::fs::write(&desktop_path, &desktop_entry)?;
        let _ = std::process::Command::new("update-desktop-database")
            .arg(&apps_dir)
            .status();
        let _ = std::process::Command::new("xdg-mime")
            .args([
                "default",
                "gdrive-fuse-rs-open.desktop",
                "application/x-gdrive-doc",
                "application/x-gdrive-sheet",
                "application/x-gdrive-slides",
                "application/x-gdrive-link",
            ])
            .status();
    }

    Ok(())
}

/// Generates a user-level systemd service unit file (`gdrive-fuse-rs.service`) for auto-mounting on login.
pub fn generate_systemd_unit(exe_path: &Path, mount_point: &Path) -> String {
    let exe = exe_path.display();
    let mp = mount_point.display();
    format!(
        "[Unit]\n\
         Description=Google Drive FUSE Filesystem (gdrive-fuse-rs)\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStartPre=/bin/mkdir -p \"{mp}\"\n\
         ExecStart=\"{exe}\" --mount-point \"{mp}\" --no-browser\n\
         ExecStop=/bin/sh -c 'fusermount3 -u \"{mp}\" || fusermount -u \"{mp}\"'\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n"
    )
}

fn resolve_chunk_size(cli_size: Option<usize>, config_size: Option<usize>) -> Result<usize> {
    let chunk_size = cli_size.or(config_size).unwrap_or(DEFAULT_READ_CHUNK_SIZE);
    if chunk_size > MAX_READ_CHUNK_SIZE {
        bail!(
            "Read-ahead chunk size exceeds the maximum of {} bytes",
            MAX_READ_CHUNK_SIZE
        );
    }
    Ok(chunk_size)
}

fn parse_chunk_size(value: &str) -> Result<usize, String> {
    let chunk_size = value
        .parse::<usize>()
        .map_err(|e| format!("invalid chunk size: {e}"))?;
    if chunk_size > MAX_READ_CHUNK_SIZE {
        return Err(format!(
            "chunk size exceeds the maximum of {} bytes",
            MAX_READ_CHUNK_SIZE
        ));
    }
    Ok(chunk_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_args_parsing_readme_invocation() {
        let args = CliArgs::try_parse_from([
            "gdrive-fuse-rs",
            "--mount-point",
            "/tmp/gdrive-mount",
            "--client-id",
            "my-client-id.apps.googleusercontent.com",
            "--client-secret",
            "my-secret",
            "--cache-ttl",
            "120",
            "--chunk-size",
            "8388608",
            "--no-browser",
            "--allow-other",
        ])
        .expect("Failed to parse valid CLI args");

        assert_eq!(args.mount_point, Some(PathBuf::from("/tmp/gdrive-mount")));
        assert_eq!(
            args.client_id.as_deref(),
            Some("my-client-id.apps.googleusercontent.com")
        );
        assert_eq!(args.client_secret.as_deref(), Some("my-secret"));
        assert_eq!(args.cache_ttl, Some(120));
        assert_eq!(args.chunk_size, Some(8_388_608));
        assert!(args.no_browser);
        assert!(args.allow_other);
    }

    #[test]
    fn test_cli_args_short_mount_point_flag() {
        let args = CliArgs::try_parse_from(["gdrive-fuse-rs", "-m", "/mnt/drive"])
            .expect("Failed to parse short -m flag");
        assert_eq!(args.mount_point, Some(PathBuf::from("/mnt/drive")));
        assert!(!args.no_browser);
        assert!(!args.allow_other);
        assert!(args.cache_ttl.is_none());
    }

    #[test]
    fn test_cli_args_phase2_flags_and_standalone_utilities() {
        let args = CliArgs::try_parse_from([
            "gdrive-fuse-rs",
            "--google-docs-mode",
            "pdf",
            "--change-poll-secs",
            "45",
            "--no-shared-with-me",
            "--clear-cache",
            "--logout",
            "--install-systemd",
        ])
        .expect("Phase 2 utility flags must parse without --mount-point");

        assert!(args.mount_point.is_none());
        assert_eq!(args.google_docs_mode, Some(GoogleDocsMode::Pdf));
        assert_eq!(args.change_poll_secs, Some(45));
        assert!(args.no_shared_with_me);
        assert!(args.clear_cache);
        assert!(args.logout);
        assert!(args.install_systemd);
    }

    #[test]
    fn test_generate_systemd_unit_contents() {
        let unit = generate_systemd_unit(
            Path::new("/usr/local/bin/gdrive-fuse-rs"),
            Path::new("/home/alice/GoogleDrive"),
        );
        assert!(unit.contains("[Unit]"));
        assert!(unit.contains("[Service]"));
        assert!(unit.contains(
            "ExecStart=\"/usr/local/bin/gdrive-fuse-rs\" --mount-point \"/home/alice/GoogleDrive\" --no-browser"
        ));
        assert!(unit.contains("fusermount3 -u \"/home/alice/GoogleDrive\""));
        assert!(unit.contains("Restart=on-failure"));
        assert!(unit.contains("WantedBy=default.target"));
    }

    #[test]
    fn test_cli_rejects_chunk_size_above_maximum() {
        let args = CliArgs::try_parse_from([
            "gdrive-fuse-rs",
            "--mount-point",
            "/mnt/drive",
            "--chunk-size",
            &(MAX_READ_CHUNK_SIZE + 1).to_string(),
        ]);
        assert!(args.is_err());
    }

    #[test]
    fn test_config_rejects_chunk_size_above_maximum() {
        let err = resolve_chunk_size(None, Some(MAX_READ_CHUNK_SIZE + 1)).unwrap_err();
        assert!(err.to_string().contains("exceeds the maximum"));
    }

    #[test]
    fn test_chunk_size_accepts_maximum() {
        assert_eq!(
            resolve_chunk_size(Some(MAX_READ_CHUNK_SIZE), None).unwrap(),
            MAX_READ_CHUNK_SIZE
        );
    }

    #[test]
    fn test_extract_url_from_shortcut() {
        let json = r#"{
            "url": "https://docs.google.com/document/d/abc123/edit",
            "doc_id": "abc123",
            "title": "Project Plan",
            "mime_type": "application/vnd.google-apps.document"
        }"#;
        let url = extract_url_from_shortcut(json).expect("Should extract valid URL");
        assert_eq!(url, "https://docs.google.com/document/d/abc123/edit");

        assert!(extract_url_from_shortcut(r#"{"url": "javascript:alert(1)"}"#).is_err());
        assert!(extract_url_from_shortcut(r#"{"doc_id": "abc"}"#).is_err());
    }
}
