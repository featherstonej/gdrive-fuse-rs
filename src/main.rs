#[allow(dead_code)]
mod api;
#[allow(dead_code)]
mod auth;
#[allow(dead_code)]
mod cache;
#[allow(dead_code)]
mod filesystem;
#[allow(dead_code)]
mod setup_ui;

use std::fs::{File, OpenOptions};
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use fuser::{MountOption, Session};

use crate::api::DriveClient;
use crate::auth::{
    clear_keyring_client_credentials, clear_keyring_credentials, resolve_credentials,
    validate_credentials, AppConfig, AuthManager, GoogleDocsMode,
};
use crate::cache::MetadataCache;
use crate::filesystem::{
    sync_changes_once, GDriveFS, DEFAULT_READ_CHUNK_SIZE, MAX_READ_CHUNK_SIZE,
};

/// Default polling interval (in seconds) for Google Drive `changes.list` incremental sync.
pub const DEFAULT_CHANGE_POLL_SECS: u64 = 30;

/// A lightweight, memory-safe Google Drive FUSE filesystem for Linux and GNOME.
#[derive(Parser, Debug, Clone)]
#[command(name = "gdrive-fuse-rs", version, about, long_about = None)]
pub struct CliArgs {
    /// Local directory path where Google Drive will be mounted (defaults to ~/GoogleDrive).
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

    /// Directory metadata cache TTL in seconds (default: 60).
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

    /// Mount the filesystem in strict read-only mode (uses `drive.readonly` OAuth scope and rejects writes with EROFS).
    #[arg(long = "read-only", default_value_t = false)]
    pub read_only: bool,

    /// Path to the temporary write-back staging directory (defaults to ~/.cache/gdrive-fuse-rs/staging).
    #[arg(long = "staging-dir", env = "GDRIVE_STAGING_DIR", value_name = "DIR")]
    pub staging_dir: Option<PathBuf>,

    /// Delete the local SQLite metadata cache database (and WAL/SHM files) before exiting or mounting.
    #[arg(long = "clear-cache", default_value_t = false)]
    pub clear_cache: bool,

    /// Confirm destructive cache cleanup without an interactive prompt.
    #[arg(long = "yes", default_value_t = false)]
    pub yes: bool,

    /// Serve expired complete directory listings while refreshing them in the background.
    #[arg(long = "stale-while-revalidate", default_value_t = false)]
    pub stale_while_revalidate: bool,

    /// Enable timing diagnostics for FUSE, cache, and Drive operations (no file identifiers or contents).
    #[arg(long = "diagnostics", default_value_t = false)]
    pub diagnostics: bool,

    /// Remove stored OAuth2 tokens and client credentials from the system keyring.
    #[arg(long = "logout", default_value_t = false)]
    pub logout: bool,

    /// Generate and install a user systemd service unit at ~/.config/systemd/user/gdrive-fuse-rs.service.
    #[arg(long = "install-systemd", default_value_t = false)]
    pub install_systemd: bool,

    /// Sign in with Google, enable and start the background systemd user service, and open the mount directory.
    #[arg(long = "setup", default_value_t = false)]
    pub setup: bool,

    /// Open the local browser configuration wizard to customize settings in ~/.config/gdrive-fuse-rs/config.toml.
    #[arg(long = "configure", default_value_t = false)]
    pub configure: bool,

    /// Open an existing configured mount, or run setup when no connected account is available.
    #[arg(
        long = "launch",
        default_value_t = false,
        conflicts_with_all = ["setup", "configure", "install_systemd", "logout", "clear_cache"]
    )]
    pub launch: bool,

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

    if (args.install_systemd || args.setup) && args.client_secret.is_some() {
        bail!(
            "--client-secret cannot be stored in a systemd unit; configure it in the config file or system keyring"
        );
    }

    if let Some(ref doc_path) = args.open_doc {
        return open_google_doc_shortcut(doc_path);
    }

    let config = match &args.config {
        Some(path) => {
            if !path.exists() && !args.setup && !args.configure && !args.launch {
                bail!("Configuration file does not exist: {}", path.display());
            }
            AppConfig::load_from_path(path)
                .with_context(|| format!("Failed to load configuration from {}", path.display()))?
        }
        None => AppConfig::load_default().context("Failed to load default configuration")?,
    };

    if args.clear_cache {
        confirm_cache_cleanup(args.yes)?;
    }

    if (args.logout || args.clear_cache) && (args.setup || args.configure) {
        let output = std::process::Command::new("systemctl")
            .args([
                "--user",
                "show",
                "--property=LoadState",
                "--value",
                "gdrive-fuse-rs.service",
            ])
            .output()
            .context("Failed to check gdrive-fuse-rs.service before cleanup")?;
        let load_state = std::str::from_utf8(&output.stdout)
            .context("Invalid systemd service load state")?
            .trim();
        if load_state != "not-found" {
            if !output.status.success() || load_state.is_empty() {
                bail!(
                    "Failed to check gdrive-fuse-rs.service before cleanup: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            let status = std::process::Command::new("systemctl")
                .args(["--user", "stop", "gdrive-fuse-rs.service"])
                .status()
                .context("Failed to stop gdrive-fuse-rs.service before cleanup")?;
            if !status.success() {
                bail!("Failed to stop gdrive-fuse-rs.service before cleanup");
            }
        }
    }

    if args.logout {
        clear_keyring_credentials()
            .context("Failed to clear OAuth2 credentials from system keyring")?;
        log::info!("Cleared stored Google Drive OAuth2 credentials from system keyring.");
        if args.mount_point.is_none()
            && !args.clear_cache
            && !args.install_systemd
            && !args.setup
            && !args.configure
        {
            return Ok(());
        }
    }

    if args.clear_cache {
        clear_local_cache_and_staging(&args, &config)?;
        if args.mount_point.is_none() && !args.install_systemd && !args.setup && !args.configure {
            return Ok(());
        }
    }

    if args.launch {
        return launch_desktop_app(&args, &config);
    }

    if args.setup || args.configure {
        return run_guided_setup(&args, &config);
    }

    if args.install_systemd {
        let _ = install_user_systemd_unit(&args, &config)?;
        if args.mount_point.is_none() {
            return Ok(());
        }
    }

    let (mount_point, auto_create_mount_point) =
        resolve_mount_point(args.mount_point.clone(), config.mount_point_path.clone());
    cleanup_stale_fuse_mount(&mount_point);
    if auto_create_mount_point && !mount_point.exists() {
        std::fs::create_dir_all(&mount_point).with_context(|| {
            format!(
                "Failed to create default mount point directory {}",
                mount_point.display()
            )
        })?;
    }

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
    let read_only = args.read_only || config.read_only.unwrap_or(false);
    let resolved_db_path = args
        .cache_db
        .clone()
        .or_else(|| config.cache_db_path.clone());
    let staging_dir = args
        .staging_dir
        .clone()
        .or_else(|| config.staging_dir_path.clone())
        .unwrap_or_else(|| match resolved_db_path.as_deref() {
            Some(db_path) => MetadataCache::default_staging_dir_for_db(db_path),
            None => MetadataCache::default_staging_dir(),
        });

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Failed to initialize Tokio async runtime")?;

    log::info!(
        "Authenticating with Google Drive (mode={})...",
        if read_only { "read-only" } else { "read-write" }
    );
    let auth_manager = rt
        .block_on(AuthManager::authenticate_with_mode(
            credentials,
            !args.no_browser,
            read_only,
        ))
        .context("Google OAuth2 authentication failed")?;
    let auth_manager = Arc::new(auth_manager);

    let cache_db_path = args
        .cache_db
        .clone()
        .or_else(|| config.cache_db_path.clone())
        .unwrap_or_else(MetadataCache::default_db_path);
    let cache_lock = acquire_cache_lock(&cache_db_path)?;
    let cache = MetadataCache::open(&cache_db_path, cache_ttl_secs).with_context(|| {
        format!(
            "Failed to open SQLite metadata cache at {}",
            cache_db_path.display()
        )
    })?;
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
                log::debug!(
                    "Could not register user XDG MIME handler for .gdoc/.gsheet/.gslides: {}",
                    e
                );
            }
        }
    }

    let fs = GDriveFS::with_phase4_config(
        api_client,
        cache,
        rt.handle().clone(),
        Duration::from_secs(cache_ttl_secs),
        chunk_size_bytes,
        google_docs_mode,
        shared_with_me,
        read_only,
        staging_dir,
        args.stale_while_revalidate,
        args.diagnostics,
    );
    let _cache_lock = cache_lock;

    let mount_options = build_mount_options(read_only, args.allow_other);

    log::info!(
        "Mounting Google Drive ({}) at {}... (Press Ctrl+C to unmount)",
        if read_only { "read-only" } else { "read-write" },
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

    session
        .run()
        .with_context(|| format!("Error running FUSE session at {}", mount_point.display()))?;

    log::info!("Unmounted {} cleanly.", mount_point.display());
    Ok(())
}

/// Builds the FUSE mount options for `Session::new`, always enabling `MountOption::DefaultPermissions`
/// so the kernel enforces the `0644`/`0755` `uid`/`gid` ownership attributes returned by `GDriveFS`
/// (especially critical when `--allow-other` is enabled in read-write mode).
fn build_mount_options(read_only: bool, allow_other: bool) -> Vec<MountOption> {
    let mut mount_options = vec![
        MountOption::FSName("gdrive".to_string()),
        MountOption::Subtype("gdrive-fuse-rs".to_string()),
        MountOption::DefaultPermissions,
    ];
    if read_only {
        mount_options.push(MountOption::RO);
    }
    if allow_other {
        mount_options.push(MountOption::AllowOther);
        mount_options.push(MountOption::AutoUnmount);
    }
    mount_options
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
    let exe_str = escape_desktop_entry_double_quoted(&exe_path.display().to_string());
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

    let needs_mime_update =
        std::fs::read_to_string(&mime_xml_path).ok().as_deref() != Some(mime_xml);
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
        for mime_type in [
            "application/x-gdrive-doc",
            "application/x-gdrive-sheet",
            "application/x-gdrive-slides",
            "application/x-gdrive-link",
        ] {
            let _ = std::process::Command::new("xdg-mime")
                .args(["default", "gdrive-fuse-rs-open.desktop", mime_type])
                .status();
        }
    }

    Ok(())
}

/// Escapes a path string for safe inclusion inside a double-quoted systemd command argument,
/// doubling `%` specifiers (`%%`) and `$` characters (`$$`), and escaping `\`, `"`, and `` ` ``.
fn escape_systemd_double_quoted(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 8);
    for ch in raw.chars() {
        match ch {
            '%' => out.push_str("%%"),
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '$' => out.push_str("$$"),
            '`' => out.push_str("\\`"),
            '\n' | '\r' => out.push(' '),
            _ => out.push(ch),
        }
    }
    out
}

fn escape_desktop_entry_double_quoted(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 8);
    for ch in raw.chars() {
        match ch {
            '%' => out.push_str("%%"),
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '$' => out.push_str("\\$"),
            '`' => out.push_str("\\`"),
            '\n' | '\r' => out.push(' '),
            _ => out.push(ch),
        }
    }
    out
}

/// Generates a user-level systemd service unit file (`gdrive-fuse-rs.service`) for auto-mounting on login.
pub fn generate_systemd_unit(exe_path: &Path, mount_point: &Path, args: &[String]) -> String {
    let exe = escape_systemd_double_quoted(&exe_path.display().to_string());
    let mp = escape_systemd_double_quoted(&mount_point.display().to_string());
    let args = args
        .iter()
        .map(|arg| format!(" \"{}\"", escape_systemd_double_quoted(arg)))
        .collect::<String>();
    format!(
        "[Unit]\n\
         Description=Google Drive FUSE Filesystem (gdrive-fuse-rs)\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStartPre=/bin/mkdir -p \"{mp}\"\n\
         ExecStart=\"{exe}\" --mount-point \"{mp}\" --no-browser{args}\n\
         ExecStop=-/bin/sh -c 'fusermount3 -u \"$1\" || fusermount -u \"$1\"' sh \"{mp}\"\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n"
    )
}

fn systemd_mount_args(args: &CliArgs) -> Vec<String> {
    let mut unit_args = Vec::new();
    for (option, value) in [
        (
            "--config",
            args.config.as_ref().map(|path| path.display().to_string()),
        ),
        ("--client-id", args.client_id.clone()),
        (
            "--cache-db",
            args.cache_db
                .as_ref()
                .map(|path| path.display().to_string()),
        ),
        ("--cache-ttl", args.cache_ttl.map(|value| value.to_string())),
        (
            "--chunk-size",
            args.chunk_size.map(|value| value.to_string()),
        ),
        (
            "--google-docs-mode",
            args.google_docs_mode.map(|mode| {
                match mode {
                    GoogleDocsMode::Browser => "browser",
                    GoogleDocsMode::Desktop => "desktop",
                    GoogleDocsMode::Pdf => "pdf",
                    GoogleDocsMode::Office => "office",
                }
                .to_owned()
            }),
        ),
        (
            "--change-poll-secs",
            args.change_poll_secs.map(|value| value.to_string()),
        ),
        (
            "--staging-dir",
            args.staging_dir
                .as_ref()
                .map(|path| path.display().to_string()),
        ),
    ] {
        if let Some(value) = value {
            unit_args.push(option.to_owned());
            unit_args.push(value);
        }
    }
    if args.no_shared_with_me {
        unit_args.push("--no-shared-with-me".to_owned());
    }
    if args.read_only {
        unit_args.push("--read-only".to_owned());
    }
    if args.allow_other {
        unit_args.push("--allow-other".to_owned());
    }
    if args.stale_while_revalidate {
        unit_args.push("--stale-while-revalidate".to_owned());
    }
    if args.diagnostics {
        unit_args.push("--diagnostics".to_owned());
    }
    unit_args
}

/// Returns the default user mount point (`~/GoogleDrive`).
pub fn default_mount_point() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join("GoogleDrive")
}

/// Resolves the target mount point in priority order:
/// 1. CLI `--mount-point <PATH>` (`auto_create = false`)
/// 2. `config.toml` `mount_point_path` (`auto_create = false`)
/// 3. Default `~/GoogleDrive` (`auto_create = true`)
pub fn resolve_mount_point(
    cli_mount: Option<PathBuf>,
    config_mount: Option<PathBuf>,
) -> (PathBuf, bool) {
    if let Some(path) = cli_mount {
        (path, false)
    } else if let Some(path) = config_mount {
        (path, false)
    } else {
        (default_mount_point(), true)
    }
}

fn install_user_systemd_unit(args: &CliArgs, config: &AppConfig) -> Result<PathBuf> {
    let exe_path =
        std::env::current_exe().unwrap_or_else(|_| PathBuf::from("/usr/bin/gdrive-fuse-rs"));
    let cwd = std::env::current_dir().context("Failed to resolve paths for systemd unit")?;
    let absolute_path = |path: PathBuf| {
        if path.is_absolute() {
            path
        } else {
            cwd.join(path)
        }
    };
    let (raw_mount, _) =
        resolve_mount_point(args.mount_point.clone(), config.mount_point_path.clone());
    let unit_mount_point = absolute_path(raw_mount);
    let mut unit_cli_args = args.clone();
    unit_cli_args.config = unit_cli_args.config.map(&absolute_path);
    unit_cli_args.cache_db = unit_cli_args.cache_db.map(&absolute_path);
    unit_cli_args.staging_dir = unit_cli_args.staging_dir.map(&absolute_path);
    let unit_args = systemd_mount_args(&unit_cli_args);
    let unit_contents = generate_systemd_unit(&exe_path, &unit_mount_point, &unit_args);
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
    Ok(unit_mount_point)
}

fn cache_cleanup_paths(args: &CliArgs, config: &AppConfig) -> Result<(PathBuf, PathBuf)> {
    let db_path = args
        .cache_db
        .clone()
        .or_else(|| config.cache_db_path.clone())
        .unwrap_or_else(MetadataCache::default_db_path);
    let db_path = MetadataCache::normalize_db_path_for_namespace(&db_path);
    let stage_dir = args
        .staging_dir
        .clone()
        .or_else(|| config.staging_dir_path.clone())
        .unwrap_or_else(|| MetadataCache::default_staging_dir_for_db(&db_path));
    let stage_dir = if stage_dir == MetadataCache::default_staging_dir() {
        MetadataCache::default_staging_dir_for_db(&db_path)
    } else {
        stage_dir
    };
    Ok((db_path, stage_dir))
}

fn clear_local_cache_and_staging(args: &CliArgs, config: &AppConfig) -> Result<()> {
    let (mount_point, _) =
        resolve_mount_point(args.mount_point.clone(), config.mount_point_path.clone());
    let proc_mounts = std::fs::read_to_string("/proc/mounts")
        .context("Failed to inspect active FUSE mounts before cache cleanup")?;
    if is_fuse_mounted_in_proc_mounts(&proc_mounts, &mount_point) {
        bail!(
            "Refusing to clear cache because {} is an active FUSE mount; unmount it first",
            mount_point.display()
        );
    }
    let (db_path, stage_dir) = cache_cleanup_paths(args, config)?;
    let _lock = acquire_cache_lock(&db_path)?;
    if MetadataCache::has_pending_staging_files(&stage_dir).with_context(|| {
        format!(
            "Failed to inspect staging directory {}",
            stage_dir.display()
        )
    })? {
        bail!(
            "Refusing to clear cache while pending writes exist in {}. Upload or recover those files first.",
            stage_dir.display()
        );
    }
    if MetadataCache::database_has_pending_writes(&db_path).with_context(|| {
        format!(
            "Failed to inspect pending writes in cache database {}",
            db_path.display()
        )
    })? {
        bail!(
            "Refusing to clear cache at {} while its database records pending writes. Upload or recover those files first.",
            db_path.display()
        );
    }
    MetadataCache::clear_staging_dir(&stage_dir)
        .with_context(|| format!("Failed to clear staging directory {}", stage_dir.display()))?;
    MetadataCache::remove_db_files(&db_path).with_context(|| {
        format!(
            "Failed to remove SQLite metadata cache at {}",
            db_path.display()
        )
    })?;
    log::info!("Removed SQLite metadata cache at {}.", db_path.display());
    Ok(())
}

fn confirm_cache_cleanup(assume_yes: bool) -> Result<()> {
    if assume_yes {
        return Ok(());
    }
    let mut stderr = io::stderr().lock();
    if !stderr.is_terminal() {
        bail!("--clear-cache requires explicit confirmation: rerun interactively or pass --yes");
    }
    write!(
        stderr,
        "This deletes the local metadata cache and may require Drive to be listed again. Pending staged writes are never deleted. Continue? [y/N] "
    )?;
    stderr.flush()?;
    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .context("Failed to read cleanup confirmation")?;
    if !matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
        bail!("Cache cleanup cancelled");
    }
    Ok(())
}

#[derive(Debug)]
struct CacheLock {
    _file: File,
}

fn acquire_cache_lock(db_path: &Path) -> Result<CacheLock> {
    if let Some(parent) = db_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("Failed to create cache lock directory {}", parent.display())
            })?;
        }
    }
    let resolved_db_path = resolve_db_path_for_lock(db_path)?;
    let mut lock_path = resolved_db_path.as_os_str().to_os_string();
    lock_path.push(".lock");
    let lock_path = PathBuf::from(lock_path);
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(&lock_path)
        .with_context(|| format!("Failed to open cache lock {}", lock_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("Failed to secure cache lock {}", lock_path.display()))?;
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                bail!(
                    "Cache at {} is in use by an active gdrive-fuse-rs process; stop/unmount it before cleanup",
                    resolved_db_path.display()
                );
            }
            return Err(err).with_context(|| {
                format!("Failed to lock cache at {}", resolved_db_path.display())
            });
        }
    }
    Ok(CacheLock { _file: file })
}

fn resolve_db_path_for_lock(db_path: &Path) -> Result<PathBuf> {
    let normalized = MetadataCache::normalize_db_path_for_namespace(db_path);
    let parent = normalized
        .parent()
        .context("Cache database path has no parent directory")?;
    std::fs::canonicalize(parent)
        .with_context(|| format!("Failed to resolve cache directory {}", parent.display()))?;
    Ok(normalized)
}

fn stop_existing_service(systemctl: &std::ffi::OsStr, existing_unit: bool) -> Result<()> {
    if existing_unit {
        let status = std::process::Command::new(systemctl)
            .args(["--user", "stop", "gdrive-fuse-rs.service"])
            .status()
            .context("Failed to stop existing gdrive-fuse-rs.service")?;
        if !status.success() {
            bail!("Failed to stop existing gdrive-fuse-rs.service");
        }
    }
    Ok(())
}

fn launch_desktop_app(args: &CliArgs, initial_config: &AppConfig) -> Result<()> {
    let (raw_mount_point, _) = resolve_mount_point(
        args.mount_point.clone(),
        initial_config.mount_point_path.clone(),
    );
    let mount_point = if raw_mount_point.is_absolute() {
        raw_mount_point
    } else {
        std::env::current_dir()
            .context("Failed to resolve mount point for desktop launch")?
            .join(raw_mount_point)
    };
    cleanup_stale_fuse_mount(&mount_point);
    let proc_mounts =
        std::fs::read_to_string("/proc/mounts").context("Failed to inspect active FUSE mounts")?;
    if is_fuse_mounted_in_proc_mounts(&proc_mounts, &mount_point) {
        log::info!(
            "Google Drive is already mounted at {}; opening it.",
            mount_point.display()
        );
        if !args.no_browser {
            let _ = open::that(&mount_point);
        }
        return Ok(());
    }

    let config_file_path = args.config.clone().or_else(AppConfig::default_path);
    let config_exists = config_file_path.as_ref().is_some_and(|path| path.exists());
    let has_credentials = validate_credentials(
        args.client_id.clone(),
        args.client_secret.clone(),
        initial_config,
    )
    .is_ok();
    if !config_exists || !has_credentials {
        let mut setup_args = args.clone();
        setup_args.launch = false;
        setup_args.setup = true;
        return run_guided_setup(&setup_args, initial_config);
    }

    cleanup_stale_fuse_mount(&mount_point);
    let service_output = std::process::Command::new("systemctl")
        .args([
            "--user",
            "show",
            "--property=LoadState",
            "--value",
            "gdrive-fuse-rs.service",
        ])
        .output()
        .context("Failed to check gdrive-fuse-rs.service")?;
    let load_state = std::str::from_utf8(&service_output.stdout)
        .context("Invalid systemd service load state")?
        .trim();
    if load_state == "not-found" {
        let mut setup_args = args.clone();
        setup_args.launch = false;
        setup_args.setup = true;
        return run_guided_setup(&setup_args, initial_config);
    }
    if !service_output.status.success() || load_state.is_empty() {
        bail!(
            "Failed to check gdrive-fuse-rs.service: {}",
            String::from_utf8_lossy(&service_output.stderr).trim()
        );
    }

    let stop = std::process::Command::new("systemctl")
        .args(["--user", "stop", "gdrive-fuse-rs.service"])
        .status()
        .context("Failed to stop gdrive-fuse-rs.service before authentication")?;
    if !stop.success() {
        bail!("Failed to stop gdrive-fuse-rs.service before authentication");
    }

    let credentials = resolve_credentials(
        args.client_id.clone(),
        args.client_secret.clone(),
        initial_config,
    )
    .context("Failed to resolve Google OAuth2 client credentials")?;
    let read_only = args.read_only || initial_config.read_only.unwrap_or(false);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Failed to initialize Tokio async runtime")?;
    log::info!("Checking the existing Google Drive sign-in...");
    rt.block_on(AuthManager::authenticate_with_mode(
        credentials,
        !args.no_browser,
        read_only,
    ))
    .context("Google OAuth2 authentication failed while launching Google Drive")?;

    install_user_systemd_unit(args, initial_config)?;
    let reload = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status()
        .context("Failed to reload systemd user units")?;
    if !reload.success() {
        bail!("Failed to reload systemd user units");
    }

    let start = std::process::Command::new("systemctl")
        .args(["--user", "start", "gdrive-fuse-rs.service"])
        .status()
        .context("Failed to start gdrive-fuse-rs.service")?;
    if !start.success() {
        bail!(
            "Failed to start gdrive-fuse-rs.service; run 'gdrive-fuse-rs --configure' to repair the connection"
        );
    }
    if !wait_for_fuse_mount(&mount_point, Duration::from_secs(15)) {
        bail!(
            "Google Drive did not mount at {} in time; check the user service journal or run 'gdrive-fuse-rs --configure'",
            mount_point.display()
        );
    }

    if !args.no_browser {
        let _ = open::that(&mount_point);
    }
    Ok(())
}

fn run_guided_setup(args: &CliArgs, initial_config: &AppConfig) -> Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Failed to initialize Tokio async runtime")?;

    let config_file_path = args.config.clone().or_else(AppConfig::default_path);
    let config_exists = config_file_path.as_ref().is_some_and(|p| p.exists());
    let has_resolvable_creds = validate_credentials(
        args.client_id.clone(),
        args.client_secret.clone(),
        initial_config,
    )
    .is_ok();

    let mut active_config = initial_config.clone();
    let service_state = std::process::Command::new("systemctl")
        .args([
            "--user",
            "show",
            "--property=LoadState",
            "--value",
            "gdrive-fuse-rs.service",
        ])
        .output()
        .context("Failed to check existing gdrive-fuse-rs.service")?;
    let load_state = std::str::from_utf8(&service_state.stdout)
        .context("Invalid systemd service load state")?
        .trim();
    if load_state != "not-found" && (!service_state.status.success() || load_state.is_empty()) {
        bail!(
            "Failed to check existing gdrive-fuse-rs.service: {}",
            String::from_utf8_lossy(&service_state.stderr).trim()
        );
    }
    let existing_unit = load_state != "not-found";
    let mut enable_autostart = if config_exists && existing_unit {
        systemd_autostart_state()?.0
    } else {
        true
    };
    let mut switch_account = false;
    let mut clear_custom_credentials = false;

    // Prepare defaults in memory; persist only after the wizard is submitted.
    if !config_exists {
        if active_config.mount_point_path.is_none() {
            active_config.mount_point_path =
                Some(args.mount_point.clone().unwrap_or_else(default_mount_point));
        }
        if active_config.google_docs_mode.is_none() {
            active_config.google_docs_mode = Some(GoogleDocsMode::Browser);
        }
        if active_config.shared_with_me.is_none() {
            active_config.shared_with_me = Some(true);
        }
        if active_config.read_only.is_none() {
            active_config.read_only = Some(args.read_only);
        }
        if let Ok(existing_creds) = validate_credentials(
            args.client_id.clone(),
            args.client_secret.clone(),
            &active_config,
        ) {
            let is_builtin = auth::builtin_default_credentials()
                .as_ref()
                .is_some_and(|b| b.client_id == existing_creds.client_id);
            if !is_builtin {
                active_config.client_id = Some(existing_creds.client_id);
                active_config.client_secret = args.client_secret.clone();
            }
        } else {
            active_config.client_id = None;
            active_config.client_secret = None;
        }
    }

    let wizard_ran = args.configure || !config_exists || !has_resolvable_creds;
    if wizard_ran {
        active_config.google_docs_mode = args.google_docs_mode.or(active_config.google_docs_mode);
        active_config.read_only = Some(args.read_only || active_config.read_only.unwrap_or(false));
        active_config.shared_with_me =
            Some(!args.no_shared_with_me && active_config.shared_with_me.unwrap_or(true));
        if active_config.client_id.is_none() {
            if let Ok(existing_creds) = validate_credentials(
                args.client_id.clone(),
                args.client_secret.clone(),
                &active_config,
            ) {
                if auth::builtin_default_credentials()
                    .as_ref()
                    .is_none_or(|builtin| builtin.client_id != existing_creds.client_id)
                {
                    active_config.client_id = Some(existing_creds.client_id);
                }
            }
        }
        if let Some(client_id) = args
            .client_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if active_config.client_id.as_deref().map(str::trim) != Some(client_id) {
                active_config.client_secret = None;
            }
            active_config.client_id = Some(client_id.to_owned());
            if let Some(client_secret) = args
                .client_secret
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                active_config.client_secret = Some(client_secret.to_owned());
            }
        }
        let outcome = rt.block_on(setup_ui::run_browser_setup_wizard(
            &active_config,
            args.mount_point.clone(),
            args.client_id.clone(),
            args.client_secret.clone(),
            !args.no_browser,
            enable_autostart,
        ))?;
        active_config = outcome.config;
        enable_autostart = outcome.enable_autostart;
        switch_account = outcome.switch_account;
        clear_custom_credentials = outcome.clear_custom_credentials;

        if outcome.switch_account {
            confirm_cache_cleanup(args.yes)?;
        }
    }

    let mut unit_args = args.clone();
    if wizard_ran {
        unit_args.mount_point = active_config.mount_point_path.clone();
        unit_args.client_id = active_config.client_id.clone();
        unit_args.client_secret = active_config.client_secret.clone();
        unit_args.google_docs_mode = active_config.google_docs_mode;
        unit_args.read_only = active_config.read_only.unwrap_or(false);
        unit_args.no_shared_with_me = !active_config.shared_with_me.unwrap_or(true);
    }
    let args = &unit_args;
    let credentials = if clear_custom_credentials {
        auth::builtin_default_credentials().context(
            "No built-in OAuth2 client credentials are available after clearing custom credentials",
        )?
    } else {
        resolve_credentials(
            args.client_id.clone(),
            args.client_secret.clone(),
            &active_config,
        )
        .context("Failed to resolve Google OAuth2 client credentials")?
    };
    let read_only = args.read_only || active_config.read_only.unwrap_or(false);

    log::info!(
        "Completing Google Drive OAuth2 sign-in (mode={})...",
        if read_only { "read-only" } else { "read-write" }
    );
    rt.block_on(AuthManager::authenticate_with_account_selection(
        credentials,
        !args.no_browser,
        read_only,
        switch_account,
    ))
    .context("Google OAuth2 authentication failed during setup")?;

    if clear_custom_credentials {
        clear_keyring_client_credentials()
            .context("Failed to clear custom OAuth2 client credentials from system keyring")?;
    }
    stop_existing_service(std::ffi::OsStr::new("systemctl"), existing_unit)?;
    if switch_account {
        clear_local_cache_and_staging(args, &active_config)?;
        log::info!("Cleared previous Google account session and metadata cache.");
    }
    if wizard_ran {
        if let Some(ref save_path) = config_file_path {
            active_config.save_to_path(save_path).with_context(|| {
                format!("Failed to save configuration to {}", save_path.display())
            })?;
            log::info!("Saved configuration to {}.", save_path.display());
        }
    }

    let mut service_args = (*args).clone();
    if wizard_ran && config_file_path.is_some() {
        service_args.client_id = None;
        service_args.client_secret = None;
        service_args.google_docs_mode = None;
        service_args.read_only = false;
        service_args.no_shared_with_me = false;
    }
    let unit_mount_point = install_user_systemd_unit(&service_args, &active_config)?;
    cleanup_stale_fuse_mount(&unit_mount_point);
    std::fs::create_dir_all(&unit_mount_point).with_context(|| {
        format!(
            "Failed to create mount point directory {}",
            unit_mount_point.display()
        )
    })?;
    let mounts = std::fs::read_to_string("/proc/mounts").context("Failed to inspect mounts")?;
    if is_fuse_mounted_in_proc_mounts(&mounts, &unit_mount_point) {
        bail!(
            "A Google Drive mount is still active at {}; unmount it before setup",
            unit_mount_point.display()
        );
    }

    if let Ok(exe_path) = std::env::current_exe() {
        let _ = install_gdrive_mime_handlers(&exe_path);
    }

    let reload_ok = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status()
        .context("Failed to reload systemd user units")?;
    if !reload_ok.success() {
        bail!("Failed to reload systemd user units");
    }

    let action = if enable_autostart {
        "enable"
    } else {
        "disable"
    };
    let status = std::process::Command::new("systemctl")
        .args(["--user", action, "gdrive-fuse-rs.service"])
        .status()
        .with_context(|| format!("Failed to run systemctl --user {action}"))?;
    if !status.success() {
        bail!("Failed to {action} gdrive-fuse-rs.service for login auto-mount");
    }
    cleanup_stale_fuse_mount(&unit_mount_point);

    let restart_ok = std::process::Command::new("systemctl")
        .args(["--user", "start", "gdrive-fuse-rs.service"])
        .status()
        .context("Failed to start gdrive-fuse-rs.service")?;
    if !restart_ok.success() {
        bail!("Failed to start gdrive-fuse-rs.service");
    }
    log::info!(
        "Started gdrive-fuse-rs.service (waiting for mount at {}...).",
        unit_mount_point.display()
    );
    if !wait_for_fuse_mount(&unit_mount_point, Duration::from_secs(15)) {
        bail!(
            "gdrive-fuse-rs.service did not mount {} in time; check systemd user logs",
            unit_mount_point.display()
        );
    }

    if !args.no_browser {
        let _ = open::that(&unit_mount_point);
    }

    Ok(())
}

fn systemd_autostart_state() -> Result<(bool, bool)> {
    let output = std::process::Command::new("systemctl")
        .args(["--user", "is-enabled", "gdrive-fuse-rs.service"])
        .output()
        .context("Failed to check gdrive-fuse-rs.service auto-mount state")?;
    let state = std::str::from_utf8(&output.stdout)
        .context("Invalid systemctl auto-mount state")?
        .trim();
    match state {
        "enabled" | "enabled-runtime" => Ok((true, true)),
        "disabled" | "masked" | "masked-runtime" | "static" | "indirect" => Ok((false, true)),
        "not-found" => Ok((true, false)),
        _ => bail!(
            "Failed to check gdrive-fuse-rs.service auto-mount state: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
    }
}

/// Lazily unmounts `mount_point` if a previous FUSE session left it in a stale `ENOTCONN`
/// (`Transport endpoint is not connected`, errno 107) state.
fn cleanup_stale_fuse_mount(mount_point: &Path) {
    let is_stale_enotconn = match std::fs::metadata(mount_point) {
        Ok(_) => false,
        Err(err) => err.raw_os_error() == Some(libc::ENOTCONN),
    };
    if is_stale_enotconn {
        log::warn!(
            "Detected stale FUSE mount at {} (ENOTCONN); unmounting cleanly...",
            mount_point.display()
        );
        let mp_str = mount_point.to_string_lossy();
        let succeeded = std::process::Command::new("fusermount3")
            .args(["-uz", mp_str.as_ref()])
            .status()
            .is_ok_and(|status| status.success());
        if !succeeded {
            let _ = std::process::Command::new("fusermount")
                .args(["-uz", mp_str.as_ref()])
                .status();
        }
    }
}

/// Checks `/proc/mounts` content to determine if `mount_point` is currently mounted by FUSE.
pub fn is_fuse_mounted_in_proc_mounts(proc_mounts: &str, mount_point: &Path) -> bool {
    let canonical =
        std::fs::canonicalize(mount_point).unwrap_or_else(|_| mount_point.to_path_buf());
    let target_raw = mount_point.to_string_lossy();
    let target_canon = canonical.to_string_lossy();

    for line in proc_mounts.lines() {
        let mut cols = line.split_whitespace();
        let _device = cols.next();
        let Some(mp_escaped) = cols.next() else {
            continue;
        };
        let Some(fstype) = cols.next() else {
            continue;
        };
        if !matches!(_device, Some("gdrive") | Some("gdrive-fuse-rs"))
            || !matches!(fstype, "fuse" | "fuse.gdrive-fuse-rs")
        {
            continue;
        }
        let mp = unescape_proc_mounts_path(mp_escaped);
        if mp == target_raw.as_ref() || mp == target_canon.as_ref() {
            return true;
        }
    }
    false
}

fn unescape_proc_mounts_path(escaped: &str) -> String {
    escaped
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

fn wait_for_fuse_mount(mount_point: &Path, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Ok(proc_mounts) = std::fs::read_to_string("/proc/mounts") {
            if is_fuse_mounted_in_proc_mounts(&proc_mounts, mount_point) {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
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
    fn test_setup_stops_existing_service_before_applying_requested_options() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!(
            "gdrive-stop-service-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        std::fs::create_dir(&dir).unwrap();
        let command = dir.join("systemctl");
        let called = dir.join("called");
        std::fs::write(
            &command,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\n",
                called.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&command, std::fs::Permissions::from_mode(0o700)).unwrap();
        stop_existing_service(command.as_os_str(), false).unwrap();
        assert!(!called.exists());
        stop_existing_service(command.as_os_str(), true).unwrap();
        assert_eq!(
            std::fs::read_to_string(&called).unwrap(),
            "--user\nstop\ngdrive-fuse-rs.service\n"
        );
        let args = CliArgs::try_parse_from(["gdrive-fuse-rs", "--setup", "--read-only"]).unwrap();
        assert!(systemd_mount_args(&args).contains(&"--read-only".to_owned()));
        std::fs::write(&command, "#!/bin/sh\nexit 1\n").unwrap();
        assert!(stop_existing_service(command.as_os_str(), true).is_err());
        std::fs::remove_file(command).unwrap();
        std::fs::remove_file(called).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn test_cache_cleanup_keeps_database_if_staging_cleanup_fails() {
        let dir = std::env::temp_dir().join(format!(
            "gdrive-cleanup-error-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        std::fs::create_dir(&dir).unwrap();
        let db = dir.join("cache.db");
        let stage = dir.join("not-a-directory");
        std::fs::write(&db, b"metadata").unwrap();
        std::fs::write(&stage, b"staged").unwrap();
        let args = CliArgs::try_parse_from([
            "gdrive-fuse-rs",
            "--mount-point",
            dir.to_str().unwrap(),
            "--cache-db",
            db.to_str().unwrap(),
            "--staging-dir",
            stage.to_str().unwrap(),
        ])
        .unwrap();
        assert!(clear_local_cache_and_staging(&args, &AppConfig::default()).is_err());
        assert_eq!(std::fs::read(&db).unwrap(), b"metadata");
        assert_eq!(std::fs::read(&stage).unwrap(), b"staged");
        std::fs::remove_file(dir.join("cache.db.lock")).unwrap();
        std::fs::remove_file(db).unwrap();
        std::fs::remove_file(stage).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn test_cache_cleanup_refuses_pending_staging_data() {
        let dir = std::env::temp_dir().join(format!(
            "gdrive-cleanup-pending-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        std::fs::create_dir(&dir).unwrap();
        let db = dir.join("cache.db");
        let stage = dir.join("staging");
        std::fs::write(&db, b"metadata").unwrap();
        std::fs::create_dir(&stage).unwrap();
        let staged = stage.join("inode_42.stage");
        std::fs::write(&staged, b"pending write").unwrap();
        let args = CliArgs::try_parse_from([
            "gdrive-fuse-rs",
            "--mount-point",
            dir.to_str().unwrap(),
            "--cache-db",
            db.to_str().unwrap(),
            "--staging-dir",
            stage.to_str().unwrap(),
        ])
        .unwrap();

        let error = clear_local_cache_and_staging(&args, &AppConfig::default()).unwrap_err();
        assert!(error.to_string().contains("pending writes"));
        assert_eq!(std::fs::read(&db).unwrap(), b"metadata");
        assert_eq!(std::fs::read(&staged).unwrap(), b"pending write");

        std::fs::remove_file(staged).unwrap();
        std::fs::remove_file(db).unwrap();
        std::fs::remove_file(dir.join("cache.db.lock")).unwrap();
        std::fs::remove_dir(stage).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn test_cache_cleanup_refuses_database_pending_write_without_stage() {
        let dir = std::env::temp_dir().join(format!(
            "gdrive-cleanup-db-pending-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        std::fs::create_dir(&dir).unwrap();
        let db = dir.join("cache.db");
        let stage = dir.join("staging");
        let cache = MetadataCache::open(&db, 60).unwrap();
        cache
            .create_pending_file(crate::cache::ROOT_INODE, "unsent.txt", "text/plain", 1)
            .unwrap();
        drop(cache);
        let args = CliArgs::try_parse_from([
            "gdrive-fuse-rs",
            "--mount-point",
            dir.to_str().unwrap(),
            "--cache-db",
            db.to_str().unwrap(),
            "--staging-dir",
            stage.to_str().unwrap(),
        ])
        .unwrap();

        let error = clear_local_cache_and_staging(&args, &AppConfig::default()).unwrap_err();
        assert!(error
            .to_string()
            .contains("database records pending writes"));
        assert!(db.exists());
        assert!(!stage.exists());

        std::fs::remove_file(dir.join("cache.db.lock")).unwrap();
        std::fs::remove_file(dir.join("cache.db-shm")).unwrap();
        std::fs::remove_file(dir.join("cache.db-wal")).unwrap();
        std::fs::remove_file(db).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn test_cache_lock_rejects_concurrent_cleanup() {
        use std::os::unix::fs::symlink;
        let dir = std::env::temp_dir().join(format!(
            "gdrive-cache-lock-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        std::fs::create_dir(&dir).unwrap();
        let db = dir.join("cache.db");
        let alias = dir.join("cache-alias.db");
        std::fs::write(&db, b"metadata").unwrap();
        symlink(&db, &alias).unwrap();
        let active = acquire_cache_lock(&alias).unwrap();
        let error = acquire_cache_lock(&db).unwrap_err();
        assert!(error.to_string().contains("active gdrive-fuse-rs process"));
        drop(active);
        acquire_cache_lock(&db).unwrap();
        std::fs::remove_file(dir.join("cache.db.lock")).unwrap();
        std::fs::remove_file(alias).unwrap();
        std::fs::remove_file(db).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn test_cache_lock_resolves_dangling_database_symlink() {
        use std::os::unix::fs::symlink;
        let dir = std::env::temp_dir().join(format!(
            "gdrive-cache-lock-dangling-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        std::fs::create_dir(&dir).unwrap();
        let target = dir.join("target.db");
        let alias = dir.join("alias.db");
        symlink(&target, &alias).unwrap();

        assert_eq!(
            resolve_db_path_for_lock(&alias).unwrap(),
            std::fs::canonicalize(&dir)
                .unwrap()
                .join(target.file_name().unwrap())
        );
        let active = acquire_cache_lock(&alias).unwrap();
        let error = acquire_cache_lock(&target).unwrap_err();
        assert!(error.to_string().contains("active gdrive-fuse-rs process"));
        drop(active);
        std::fs::remove_file(dir.join("target.db.lock")).unwrap();
        std::fs::remove_file(alias).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn test_cache_cleanup_resolves_database_symlink() {
        use std::os::unix::fs::symlink;
        let dir = std::env::temp_dir().join(format!(
            "gdrive-cleanup-symlink-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        std::fs::create_dir(&dir).unwrap();
        let db = dir.join("target.db");
        let link = dir.join("link.db");
        let stage = dir.join("staging");
        let cache = MetadataCache::open(&db, 60).unwrap();
        drop(cache);
        std::fs::write(dir.join("target.db-wal"), []).unwrap();
        std::fs::create_dir(&stage).unwrap();
        std::fs::write(stage.join("inode_1.stage"), b"staged").unwrap();
        symlink(&db, &link).unwrap();
        assert_eq!(
            MetadataCache::default_staging_dir_for_db(&db),
            MetadataCache::default_staging_dir_for_db(&link)
        );
        let default_args =
            CliArgs::try_parse_from(["gdrive-fuse-rs", "--cache-db", link.to_str().unwrap()])
                .unwrap();
        let (resolved_db, resolved_stage) =
            cache_cleanup_paths(&default_args, &AppConfig::default()).unwrap();
        assert_eq!(resolved_db, std::fs::canonicalize(&db).unwrap());
        assert_eq!(
            resolved_stage,
            MetadataCache::default_staging_dir_for_db(&db)
        );
        let args = CliArgs::try_parse_from([
            "gdrive-fuse-rs",
            "--mount-point",
            dir.to_str().unwrap(),
            "--cache-db",
            link.to_str().unwrap(),
            "--staging-dir",
            stage.to_str().unwrap(),
        ])
        .unwrap();
        assert!(clear_local_cache_and_staging(&args, &AppConfig::default()).is_err());
        assert!(db.exists());
        assert!(stage.join("inode_1.stage").exists());
        std::fs::remove_file(stage.join("inode_1.stage")).unwrap();
        clear_local_cache_and_staging(&args, &AppConfig::default()).unwrap();
        assert!(!db.exists());
        assert!(!dir.join("target.db-wal").exists());
        assert!(std::fs::symlink_metadata(&link).is_ok());
        assert!(!stage.join("inode_1.stage").exists());
        std::fs::remove_file(dir.join("target.db.lock")).unwrap();
        std::fs::remove_file(link).unwrap();
        std::fs::remove_dir(stage).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }

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
            &[],
        );
        assert!(unit.contains("[Unit]"));
        assert!(unit.contains("[Service]"));
        assert!(unit.contains(
            "ExecStart=\"/usr/local/bin/gdrive-fuse-rs\" --mount-point \"/home/alice/GoogleDrive\" --no-browser"
        ));
        assert!(unit.contains(
            "ExecStop=-/bin/sh -c 'fusermount3 -u \"$1\" || fusermount -u \"$1\"' sh \"/home/alice/GoogleDrive\""
        ));
        assert!(unit.contains("Restart=on-failure"));
        assert!(unit.contains("WantedBy=default.target"));

        // Verify special characters (%, ", ') are safely escaped for systemd and sh
        let escaped_unit = generate_systemd_unit(
            Path::new("/opt/bin%/gdrive\"bin$HOME"),
            Path::new("/home/alice/100% 'Drive' \"Test\"/$DRIVE"),
            &[],
        );
        assert!(escaped_unit.contains("\"/opt/bin%%/gdrive\\\"bin$$HOME\""));
        assert!(escaped_unit.contains("\"/home/alice/100%% 'Drive' \\\"Test\\\"/$$DRIVE\""));
    }

    #[test]
    fn test_desktop_entry_path_escaping() {
        assert_eq!(
            escape_desktop_entry_double_quoted("/opt/bin%/gdrive\"bin$HOME"),
            "/opt/bin%%/gdrive\\\"bin\\$HOME"
        );
    }

    #[test]
    fn test_systemd_unit_preserves_mount_options() {
        let args = CliArgs::try_parse_from([
            "gdrive-fuse-rs",
            "--config",
            "/tmp/gdrive config.toml",
            "--client-id",
            "client-id",
            "--cache-db",
            "/tmp/cache.db",
            "--cache-ttl",
            "120",
            "--chunk-size",
            "8388608",
            "--google-docs-mode",
            "pdf",
            "--change-poll-secs",
            "45",
            "--no-shared-with-me",
            "--allow-other",
            "--stale-while-revalidate",
            "--diagnostics",
        ])
        .expect("Mount options must parse");
        let unit = generate_systemd_unit(
            Path::new("/usr/local/bin/gdrive-fuse-rs"),
            Path::new("/home/alice/GoogleDrive"),
            &systemd_mount_args(&args),
        );

        assert!(unit.contains(
            "ExecStart=\"/usr/local/bin/gdrive-fuse-rs\" --mount-point \"/home/alice/GoogleDrive\" --no-browser \"--config\" \"/tmp/gdrive config.toml\" \"--client-id\" \"client-id\" \"--cache-db\" \"/tmp/cache.db\" \"--cache-ttl\" \"120\" \"--chunk-size\" \"8388608\" \"--google-docs-mode\" \"pdf\" \"--change-poll-secs\" \"45\" \"--no-shared-with-me\" \"--allow-other\" \"--stale-while-revalidate\" \"--diagnostics\""
        ));
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

    #[test]
    fn test_cli_args_phase3_read_only_and_staging_dir() {
        let args = CliArgs::try_parse_from([
            "gdrive-fuse-rs",
            "--mount-point",
            "/mnt/drive",
            "--read-only",
            "--staging-dir",
            "/tmp/gdrive-staging",
        ])
        .expect("Phase 3 flags must parse");

        assert!(args.read_only);
        assert_eq!(args.staging_dir, Some(PathBuf::from("/tmp/gdrive-staging")));

        let unit_args = systemd_mount_args(&args);
        assert!(unit_args.contains(&"--read-only".to_string()));
        assert!(unit_args.contains(&"--staging-dir".to_string()));
        assert!(unit_args.contains(&"/tmp/gdrive-staging".to_string()));
    }

    #[test]
    fn test_build_mount_options_includes_default_permissions() {
        let rw_opts = build_mount_options(false, false);
        assert!(rw_opts.contains(&MountOption::DefaultPermissions));
        assert!(!rw_opts.contains(&MountOption::RO));
        assert!(!rw_opts.contains(&MountOption::AllowOther));

        let ro_allow_other_opts = build_mount_options(true, true);
        assert!(ro_allow_other_opts.contains(&MountOption::DefaultPermissions));
        assert!(ro_allow_other_opts.contains(&MountOption::RO));
        assert!(ro_allow_other_opts.contains(&MountOption::AllowOther));
        assert!(ro_allow_other_opts.contains(&MountOption::AutoUnmount));
    }

    #[test]
    fn test_resolve_mount_point_priority_and_default_auto_create_flag() {
        let (cli_mp, cli_auto) = resolve_mount_point(
            Some(PathBuf::from("/cli/mount")),
            Some(PathBuf::from("/cfg/mount")),
        );
        assert_eq!(cli_mp, PathBuf::from("/cli/mount"));
        assert!(!cli_auto);

        let (cfg_mp, cfg_auto) = resolve_mount_point(None, Some(PathBuf::from("/cfg/mount")));
        assert_eq!(cfg_mp, PathBuf::from("/cfg/mount"));
        assert!(!cfg_auto);

        let (def_mp, def_auto) = resolve_mount_point(None, None);
        assert_eq!(def_mp, default_mount_point());
        assert!(def_mp.ends_with("GoogleDrive"));
        assert!(def_auto);
    }

    #[test]
    fn test_cli_setup_flag_and_zero_arg_parsing() {
        let zero_args =
            CliArgs::try_parse_from(["gdrive-fuse-rs"]).expect("Zero arguments must parse");
        assert!(zero_args.mount_point.is_none());
        assert!(!zero_args.setup);
        assert!(!zero_args.launch);

        let setup_args = CliArgs::try_parse_from(["gdrive-fuse-rs", "--setup"])
            .expect("--setup flag must parse");
        assert!(setup_args.setup);

        let launch_args = CliArgs::try_parse_from(["gdrive-fuse-rs", "--launch"])
            .expect("--launch flag must parse");
        assert!(launch_args.launch);
        assert!(CliArgs::try_parse_from(["gdrive-fuse-rs", "--launch", "--setup"]).is_err());

        let configure_args = CliArgs::try_parse_from(["gdrive-fuse-rs", "--configure"])
            .expect("--configure flag must parse");
        assert!(configure_args.configure);
    }

    #[test]
    fn test_is_fuse_mounted_in_proc_mounts_detection() {
        let sample_mounts = "\
sysfs /sys sysfs rw,nosuid,nodev,noexec,relatime 0 0
gdrive-fuse-rs /home/alice/GoogleDrive fuse.gdrive-fuse-rs rw,nosuid,nodev,relatime,user_id=1000,group_id=1000,default_permissions 0 0
gdrive-fuse-rs /home/alice/My\\040Drive fuse rw,nosuid,nodev 0 0
";
        assert!(is_fuse_mounted_in_proc_mounts(
            sample_mounts,
            Path::new("/home/alice/GoogleDrive")
        ));
        assert!(is_fuse_mounted_in_proc_mounts(
            sample_mounts,
            Path::new("/home/alice/My Drive")
        ));
        assert!(!is_fuse_mounted_in_proc_mounts(
            sample_mounts,
            Path::new("/home/alice/Other")
        ));
    }
}
