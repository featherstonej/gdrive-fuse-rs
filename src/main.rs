#[allow(dead_code)]
mod api;
#[allow(dead_code)]
mod auth;
#[allow(dead_code)]
mod cache;
#[allow(dead_code)]
mod filesystem;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use fuser::{MountOption, Session};

use crate::api::DriveClient;
use crate::auth::{resolve_credentials, AppConfig, AuthManager};
use crate::cache::MetadataCache;
use crate::filesystem::{GDriveFS, DEFAULT_READ_CHUNK_SIZE, MAX_READ_CHUNK_SIZE};

/// A lightweight, memory-safe, read-only Google Drive FUSE filesystem for Linux and GNOME.
#[derive(Parser, Debug, Clone)]
#[command(name = "gdrive-fuse-rs", version, about, long_about = None)]
pub struct CliArgs {
    /// Local directory path where Google Drive will be mounted.
    #[arg(short = 'm', long = "mount-point", value_name = "PATH")]
    pub mount_point: PathBuf,

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

    if !args.mount_point.exists() {
        bail!(
            "Mount point directory does not exist: {}",
            args.mount_point.display()
        );
    }
    if !args.mount_point.is_dir() {
        bail!(
            "Mount point path is not a directory: {}",
            args.mount_point.display()
        );
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

    let credentials = resolve_credentials(args.client_id.clone(), args.client_secret.clone(), &config)
        .context("Failed to resolve Google OAuth2 client credentials")?;

    let cache_ttl_secs = args.cache_ttl.or(config.cache_ttl_secs).unwrap_or(60);
    let chunk_size_bytes = resolve_chunk_size(args.chunk_size, config.chunk_size_bytes)?;

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

    let fs = GDriveFS::with_config(
        api_client,
        cache,
        rt.handle().clone(),
        Duration::from_secs(cache_ttl_secs),
        chunk_size_bytes,
    );

    let mut mount_options = vec![
        MountOption::RO,
        MountOption::FSName("gdrive".to_string()),
        MountOption::Subtype("gdrive-fuse-rs".to_string()),
    ];
    if args.allow_other {
        mount_options.push(MountOption::AllowOther);
        mount_options.push(MountOption::AutoUnmount);
    }

    log::info!(
        "Mounting Google Drive (read-only) at {}... (Press Ctrl+C to unmount)",
        args.mount_point.display()
    );
    let mut session = Session::new(fs, &args.mount_point, &mount_options).with_context(|| {
        format!(
            "Failed to mount FUSE filesystem at {}",
            args.mount_point.display()
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
            args.mount_point.display()
        )
    })?;

    log::info!("Unmounted {} cleanly.", args.mount_point.display());
    Ok(())
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

        assert_eq!(args.mount_point, PathBuf::from("/tmp/gdrive-mount"));
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
        assert_eq!(args.mount_point, PathBuf::from("/mnt/drive"));
        assert!(!args.no_browser);
        assert!(!args.allow_other);
        assert!(args.cache_ttl.is_none());
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
}
