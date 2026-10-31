use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oauth2::basic::BasicClient;
use oauth2::reqwest::async_http_client;
use oauth2::{
    AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken, PkceCodeChallenge, RedirectUrl,
    RefreshToken, Scope, TokenResponse, TokenUrl,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use url::Url;

pub const GOOGLE_AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
pub const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
pub const GOOGLE_DRIVE_READONLY_SCOPE: &str = "https://www.googleapis.com/auth/drive.readonly";

pub const KEYRING_SERVICE: &str = "gdrive-fuse-rs";
pub const KEYRING_CLIENT_CONFIG_USER: &str = "oauth_client_config";
pub const KEYRING_REFRESH_TOKEN_USER: &str = "oauth_refresh_token";

const TOKEN_EXPIRY_MARGIN: Duration = Duration::from_secs(60);
const DEFAULT_TOKEN_LIFETIME: Duration = Duration::from_secs(3600);

/// Errors that can occur during configuration loading, credential resolution, or OAuth2 flows.
#[derive(Debug, thiserror::Error)]
#[allow(clippy::enum_variant_names)]
pub enum AuthError {
    #[error("Missing OAuth2 client credentials: {0}")]
    MissingCredentials(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Keyring error: {0}")]
    KeyringError(String),

    #[error("OAuth2 protocol error: {0}")]
    OAuthError(String),

    #[error("I/O error during authentication: {0}")]
    IoError(#[from] std::io::Error),

    #[error("OAuth2 token is expired and cannot be refreshed")]
    TokenExpired,

    #[error("Browser launch error: {0}")]
    BrowserLaunchError(String),

    #[error("Invalid OAuth2 callback received: {0}")]
    InvalidCallback(String),
}

impl AuthError {
    /// Maps authentication errors to standard POSIX error codes via `libc`.
    pub fn to_errno(&self) -> libc::c_int {
        match self {
            AuthError::MissingCredentials(_)
            | AuthError::OAuthError(_)
            | AuthError::TokenExpired
            | AuthError::InvalidCallback(_) => libc::EACCES,
            AuthError::ConfigError(_)
            | AuthError::KeyringError(_)
            | AuthError::IoError(_)
            | AuthError::BrowserLaunchError(_) => libc::EIO,
        }
    }
}

/// User configuration loaded from `~/.config/gdrive-fuse-rs/config.toml`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppConfig {
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub cache_ttl_secs: Option<u64>,
    pub cache_db_path: Option<PathBuf>,
    pub chunk_size_bytes: Option<usize>,
}

impl AppConfig {
    /// Returns the default configuration file path (`~/.config/gdrive-fuse-rs/config.toml`).
    pub fn default_path() -> Option<PathBuf> {
        dirs::config_dir().map(|dir| dir.join("gdrive-fuse-rs").join("config.toml"))
    }

    /// Loads `AppConfig` from a specific path. If the file does not exist, returns `Ok(AppConfig::default())`.
    pub fn load_from_path(path: &Path) -> Result<Self, AuthError> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(path).map_err(|e| {
            AuthError::ConfigError(format!("Failed to read config file {}: {}", path.display(), e))
        })?;
        toml::from_str(&content).map_err(|e| {
            AuthError::ConfigError(format!("Failed to parse TOML in {}: {}", path.display(), e))
        })
    }

    /// Loads `AppConfig` from `~/.config/gdrive-fuse-rs/config.toml`, or returns `AppConfig::default()` if absent.
    pub fn load_default() -> Result<Self, AuthError> {
        match Self::default_path() {
            Some(path) => Self::load_from_path(&path),
            None => Ok(Self::default()),
        }
    }
}

/// Resolved OAuth2 client credentials used to initiate the Google OAuth2 flow.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientCredentials {
    pub client_id: String,
    pub client_secret: Option<String>,
}

/// Resolves OAuth2 client credentials using a layered approach:
/// 1. CLI flags / Environment variables (`cli_id`, `cli_secret`)
/// 2. User configuration file (`config.toml`)
/// 3. System keyring fallback (`service = "gdrive-fuse-rs"`, `user = "oauth_client_config"`)
pub fn resolve_credentials(
    cli_id: Option<String>,
    cli_secret: Option<String>,
    config: &AppConfig,
) -> Result<ClientCredentials, AuthError> {
    let clean_opt = |opt: Option<String>| -> Option<String> {
        opt.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
    };

    let cli_id = clean_opt(cli_id);
    let cli_secret = clean_opt(cli_secret);
    let cfg_id = clean_opt(config.client_id.clone());
    let cfg_secret = clean_opt(config.client_secret.clone());

    if let Some(client_id) = cli_id {
        let creds = ClientCredentials {
            client_id,
            client_secret: cli_secret,
        };
        persist_credentials_to_keyring(&creds);
        return Ok(creds);
    }

    if let Some(client_id) = cfg_id {
        let creds = ClientCredentials {
            client_id,
            client_secret: cfg_secret,
        };
        persist_credentials_to_keyring(&creds);
        return Ok(creds);
    }

    if let Some(creds) = load_credentials_from_keyring() {
        return Ok(creds);
    }

    if let Some(default_id) = clean_opt(option_env!("GDRIVE_DEFAULT_CLIENT_ID").map(String::from)) {
        let default_secret =
            clean_opt(option_env!("GDRIVE_DEFAULT_CLIENT_SECRET").map(String::from));
        return Ok(ClientCredentials {
            client_id: default_id,
            client_secret: default_secret,
        });
    }

    Err(AuthError::MissingCredentials(
        "No OAuth2 client_id found via CLI flags (--client-id / GDRIVE_CLIENT_ID), \
         ~/.config/gdrive-fuse-rs/config.toml, or system keyring."
            .to_string(),
    ))
}

fn persist_credentials_to_keyring(creds: &ClientCredentials) {
    match keyring::Entry::new(KEYRING_SERVICE, KEYRING_CLIENT_CONFIG_USER) {
        Ok(entry) => {
            if let Ok(json) = serde_json::to_string(creds) {
                if let Err(e) = entry.set_password(&json) {
                    log::debug!("Unable to store client credentials in system keyring: {}", e);
                }
            }
        }
        Err(e) => {
            log::debug!("Unable to initialize keyring entry for client config: {}", e);
        }
    }
}

fn load_credentials_from_keyring() -> Option<ClientCredentials> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, KEYRING_CLIENT_CONFIG_USER).ok()?;
    let json = entry.get_password().ok()?;
    let creds: ClientCredentials = serde_json::from_str(&json).ok()?;
    if creds.client_id.trim().is_empty() {
        return None;
    }
    Some(creds)
}

fn persist_refresh_token_to_keyring(refresh_token: &str) {
    match keyring::Entry::new(KEYRING_SERVICE, KEYRING_REFRESH_TOKEN_USER) {
        Ok(entry) => {
            if let Err(e) = entry.set_password(refresh_token) {
                log::warn!("Failed to store OAuth2 refresh token in system keyring: {}", e);
            } else {
                log::info!("Saved OAuth2 refresh token to system keyring.");
            }
        }
        Err(e) => {
            log::warn!("Failed to access system keyring for refresh token: {}", e);
        }
    }
}

fn load_refresh_token_from_keyring() -> Option<String> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, KEYRING_REFRESH_TOKEN_USER).ok()?;
    let token = entry.get_password().ok()?;
    let trimmed = token.trim().to_string();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

/// Represents an in-memory cached OAuth2 access token with its expiration timestamp.
#[derive(Debug, Clone)]
pub struct CachedToken {
    pub access_token: String,
    pub expires_at: Option<Instant>,
}

impl CachedToken {
    /// Returns true if the token is still valid and not within `margin` of expiring.
    pub fn is_valid(&self, margin: Duration) -> bool {
        match self.expires_at {
            Some(expires_at) => Instant::now() + margin < expires_at,
            None => true,
        }
    }
}

/// Manages Google OAuth2 PKCE authentication, keyring persistence, and automatic access token refresh.
#[derive(Debug, Clone)]
pub struct AuthManager {
    creds: ClientCredentials,
    refresh_token: Arc<RwLock<Option<String>>>,
    cached_token: Arc<RwLock<CachedToken>>,
    static_mode: bool,
}

impl AuthManager {
    /// Creates an `AuthManager` backed by a static, non-expiring token for unit and integration tests.
    pub fn new_static_for_test(token: impl Into<String>) -> Self {
        Self {
            creds: ClientCredentials {
                client_id: String::new(),
                client_secret: None,
            },
            refresh_token: Arc::new(RwLock::new(None)),
            cached_token: Arc::new(RwLock::new(CachedToken {
                access_token: token.into(),
                expires_at: None,
            })),
            static_mode: true,
        }
    }

    /// Authenticates the user with Google Drive.
    ///
    /// 1. Checks the system keyring for an existing refresh token and attempts to exchange it for an access token.
    /// 2. If no refresh token exists or refreshing fails, runs an interactive OAuth2 PKCE browser flow
    ///    using a local loopback listener (`127.0.0.1:0`) and stores the resulting refresh token in the keyring.
    pub async fn authenticate(
        creds: ClientCredentials,
        open_browser: bool,
    ) -> Result<Self, AuthError> {
        if let Some(saved_refresh) = load_refresh_token_from_keyring() {
            log::info!("Found existing refresh token in system keyring; refreshing access token...");
            match Self::exchange_refresh_token(&creds, &saved_refresh).await {
                Ok((cached_token, new_refresh)) => {
                    let effective_refresh = new_refresh.unwrap_or(saved_refresh);
                    persist_refresh_token_to_keyring(&effective_refresh);
                    return Ok(Self {
                        creds,
                        refresh_token: Arc::new(RwLock::new(Some(effective_refresh))),
                        cached_token: Arc::new(RwLock::new(cached_token)),
                        static_mode: false,
                    });
                }
                Err(e) => {
                    log::warn!(
                        "Stored refresh token could not be exchanged ({}); falling back to interactive browser login.",
                        e
                    );
                }
            }
        }

        let (cached_token, refresh_token) =
            Self::run_interactive_pkce_flow(&creds, open_browser).await?;

        if let Some(ref rt) = refresh_token {
            persist_refresh_token_to_keyring(rt);
        }

        Ok(Self {
            creds,
            refresh_token: Arc::new(RwLock::new(refresh_token)),
            cached_token: Arc::new(RwLock::new(cached_token)),
            static_mode: false,
        })
    }

    /// Returns a valid OAuth2 access token, refreshing it automatically if it is within 60 seconds of expiry.
    pub async fn get_access_token(&self) -> Result<String, AuthError> {
        {
            let guard = self.cached_token.read().await;
            if self.static_mode || guard.is_valid(TOKEN_EXPIRY_MARGIN) {
                return Ok(guard.access_token.clone());
            }
        }
        self.force_refresh().await
    }

    /// Forces an immediate refresh of the OAuth2 access token using the stored refresh token.
    pub async fn force_refresh(&self) -> Result<String, AuthError> {
        if self.static_mode {
            let guard = self.cached_token.read().await;
            return Ok(guard.access_token.clone());
        }

        let mut token_guard = self.cached_token.write().await;
        // Double-check after acquiring write lock in case another task already refreshed
        if token_guard.is_valid(TOKEN_EXPIRY_MARGIN) {
            return Ok(token_guard.access_token.clone());
        }

        let refresh_tok = {
            let rt_guard = self.refresh_token.read().await;
            rt_guard.clone().ok_or(AuthError::TokenExpired)?
        };

        let (new_cached, maybe_new_refresh) =
            Self::exchange_refresh_token(&self.creds, &refresh_tok).await?;
        let access_token = new_cached.access_token.clone();
        *token_guard = new_cached;

        if let Some(new_rt) = maybe_new_refresh {
            persist_refresh_token_to_keyring(&new_rt);
            let mut rt_guard = self.refresh_token.write().await;
            *rt_guard = Some(new_rt);
        }

        Ok(access_token)
    }

    fn build_oauth_client(
        creds: &ClientCredentials,
        redirect_url: Option<RedirectUrl>,
    ) -> Result<BasicClient, AuthError> {
        let client_id = ClientId::new(creds.client_id.clone());
        let client_secret = creds
            .client_secret
            .as_ref()
            .map(|s| ClientSecret::new(s.clone()));
        let auth_url = AuthUrl::new(GOOGLE_AUTH_URL.to_string())
            .map_err(|e| AuthError::OAuthError(format!("Invalid auth URL: {}", e)))?;
        let token_url = TokenUrl::new(GOOGLE_TOKEN_URL.to_string())
            .map_err(|e| AuthError::OAuthError(format!("Invalid token URL: {}", e)))?;

        let mut client = BasicClient::new(client_id, client_secret, auth_url, Some(token_url));
        if let Some(redirect) = redirect_url {
            client = client.set_redirect_uri(redirect);
        }
        Ok(client)
    }

    async fn exchange_refresh_token(
        creds: &ClientCredentials,
        refresh_token: &str,
    ) -> Result<(CachedToken, Option<String>), AuthError> {
        let client = Self::build_oauth_client(creds, None)?;
        let rt = RefreshToken::new(refresh_token.to_string());

        let token_res = client
            .exchange_refresh_token(&rt)
            .request_async(async_http_client)
            .await
            .map_err(|e| AuthError::OAuthError(format!("Refresh token exchange failed: {}", e)))?;

        let lifetime = token_res.expires_in().unwrap_or(DEFAULT_TOKEN_LIFETIME);
        let cached = CachedToken {
            access_token: token_res.access_token().secret().clone(),
            expires_at: Some(Instant::now() + lifetime),
        };
        let new_refresh = token_res.refresh_token().map(|r| r.secret().clone());
        Ok((cached, new_refresh))
    }

    async fn run_interactive_pkce_flow(
        creds: &ClientCredentials,
        open_browser: bool,
    ) -> Result<(CachedToken, Option<String>), AuthError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        let redirect_uri_str = format!("http://127.0.0.1:{}", local_addr.port());
        let redirect_url = RedirectUrl::new(redirect_uri_str)
            .map_err(|e| AuthError::OAuthError(format!("Invalid redirect URL: {}", e)))?;

        let client = Self::build_oauth_client(creds, Some(redirect_url))?;
        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

        let (auth_url, csrf_token) = client
            .authorize_url(CsrfToken::new_random)
            .add_scope(Scope::new(GOOGLE_DRIVE_READONLY_SCOPE.to_string()))
            .add_extra_param("access_type", "offline")
            .add_extra_param("prompt", "consent")
            .set_pkce_challenge(pkce_challenge)
            .url();

        println!(
            "\nPlease authenticate with Google Drive by visiting the following URL:\n{}\n",
            auth_url
        );
        log::info!("Initiating OAuth2 PKCE login at {}", auth_url);

        if open_browser {
            if let Err(e) = open::that(auth_url.as_str()) {
                log::warn!(
                    "Failed to automatically launch browser ({}). Please open the URL above manually.",
                    e
                );
            }
        }

        let code = wait_for_oauth_callback(&listener, csrf_token.secret()).await?;

        let token_res = client
            .exchange_code(AuthorizationCode::new(code))
            .set_pkce_verifier(pkce_verifier)
            .request_async(async_http_client)
            .await
            .map_err(|e| {
                AuthError::OAuthError(format!("Authorization code exchange failed: {}", e))
            })?;

        let lifetime = token_res.expires_in().unwrap_or(DEFAULT_TOKEN_LIFETIME);
        let cached = CachedToken {
            access_token: token_res.access_token().secret().clone(),
            expires_at: Some(Instant::now() + lifetime),
        };
        let refresh_token = token_res.refresh_token().map(|r| r.secret().clone());

        Ok((cached, refresh_token))
    }
}

/// Waits for a single HTTP GET request on the loopback listener, verifies the CSRF state,
/// sends an HTML confirmation response, and returns the authorization code.
pub async fn wait_for_oauth_callback(
    listener: &TcpListener,
    expected_state: &str,
) -> Result<String, AuthError> {
    let (mut stream, _) = listener.accept().await?;
    let mut reader = BufReader::new(&mut stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).await?;

    let result = parse_oauth_callback_request_line(&request_line, expected_state);

    let (status_line, body) = match &result {
        Ok(_) => (
            "HTTP/1.1 200 OK",
            "<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>gdrive-fuse-rs</title></head>\
             <body style=\"font-family: sans-serif; text-align: center; padding-top: 3rem;\">\
             <h2>Authentication complete.</h2>\
             <p>You may close this window and return to <code>gdrive-fuse-rs</code>.</p>\
             </body></html>",
        ),
        Err(_) => (
            "HTTP/1.1 400 Bad Request",
            "<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>gdrive-fuse-rs</title></head>\
             <body style=\"font-family: sans-serif; text-align: center; padding-top: 3rem;\">\
             <h2>Authentication failed.</h2>\
             <p>Please check the terminal logs for details.</p>\
             </body></html>",
        ),
    };

    let response = format!(
        "{status_line}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;

    result
}

/// Parses the first line of an HTTP GET callback request (e.g., `GET /?code=abc&state=xyz HTTP/1.1`),
/// validates the CSRF `state`, and extracts the OAuth2 `code`.
pub fn parse_oauth_callback_request_line(
    request_line: &str,
    expected_state: &str,
) -> Result<String, AuthError> {
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 || parts[0] != "GET" {
        return Err(AuthError::InvalidCallback(format!(
            "Expected HTTP GET request, got: {}",
            request_line.trim()
        )));
    }

    let path_and_query = parts[1];
    let full_url = format!("http://127.0.0.1{}", path_and_query);
    let parsed_url = Url::parse(&full_url).map_err(|e| {
        AuthError::InvalidCallback(format!("Failed to parse callback URL '{}': {}", full_url, e))
    })?;

    let mut code: Option<String> = None;
    let mut state: Option<String> = None;
    let mut oauth_error: Option<String> = None;

    for (k, v) in parsed_url.query_pairs() {
        match k.as_ref() {
            "code" => code = Some(v.into_owned()),
            "state" => state = Some(v.into_owned()),
            "error" => oauth_error = Some(v.into_owned()),
            _ => {}
        }
    }

    if let Some(err) = oauth_error {
        return Err(AuthError::OAuthError(format!(
            "OAuth provider returned error: {}",
            err
        )));
    }

    let received_state = state.ok_or_else(|| {
        AuthError::InvalidCallback("Missing 'state' parameter in OAuth callback".to_string())
    })?;

    if received_state != expected_state {
        return Err(AuthError::InvalidCallback(
            "CSRF state mismatch in OAuth callback".to_string(),
        ));
    }

    let auth_code = code.filter(|c| !c.is_empty()).ok_or_else(|| {
        AuthError::InvalidCallback("Missing 'code' parameter in OAuth callback".to_string())
    })?;

    Ok(auth_code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    #[test]
    fn test_app_config_toml_deserialization_and_missing_file() {
        let raw_toml = r#"
            client_id = "placeholder-id.apps.googleusercontent.com"
            client_secret = "placeholder-secret"
            cache_ttl_secs = 120
            cache_db_path = "/tmp/gdrive-test-cache.db"
            chunk_size_bytes = 4194304
        "#;
        let parsed: AppConfig = toml::from_str(raw_toml).expect("valid TOML should parse");
        assert_eq!(
            parsed.client_id.as_deref(),
            Some("placeholder-id.apps.googleusercontent.com")
        );
        assert_eq!(parsed.client_secret.as_deref(), Some("placeholder-secret"));
        assert_eq!(parsed.cache_ttl_secs, Some(120));
        assert_eq!(
            parsed.cache_db_path,
            Some(PathBuf::from("/tmp/gdrive-test-cache.db"))
        );
        assert_eq!(parsed.chunk_size_bytes, Some(4_194_304));

        // Non-existent path should cleanly return Default
        let missing = Path::new("/tmp/non_existent_gdrive_fuse_rs_config_12345.toml");
        let default_cfg = AppConfig::load_from_path(missing).expect("missing file returns default");
        assert_eq!(default_cfg, AppConfig::default());
    }

    #[test]
    fn test_layered_credential_resolution_priority() {
        let cfg = AppConfig {
            client_id: Some("config-id-placeholder".to_string()),
            client_secret: Some("config-secret-placeholder".to_string()),
            ..Default::default()
        };

        // 1. CLI overrides config
        let resolved_cli = resolve_credentials(
            Some("cli-id-placeholder".to_string()),
            Some("cli-secret-placeholder".to_string()),
            &cfg,
        )
        .expect("CLI credentials should resolve");
        assert_eq!(resolved_cli.client_id, "cli-id-placeholder");
        assert_eq!(
            resolved_cli.client_secret.as_deref(),
            Some("cli-secret-placeholder")
        );

        // 2. Config fallback when CLI is None
        let resolved_cfg =
            resolve_credentials(None, None, &cfg).expect("Config credentials should resolve");
        assert_eq!(resolved_cfg.client_id, "config-id-placeholder");
        assert_eq!(
            resolved_cfg.client_secret.as_deref(),
            Some("config-secret-placeholder")
        );
    }

    #[test]
    fn test_parse_oauth_callback_request_line_valid_and_csrf_validation() {
        let req_line = "GET /?code=4/0AdQt8q_fake_code&state=csrf_state_123 HTTP/1.1\r\n";
        let code = parse_oauth_callback_request_line(req_line, "csrf_state_123")
            .expect("valid callback should succeed");
        assert_eq!(code, "4/0AdQt8q_fake_code");

        // CSRF mismatch must fail
        let bad_state = parse_oauth_callback_request_line(req_line, "wrong_state");
        assert!(matches!(bad_state, Err(AuthError::InvalidCallback(_))));

        // Provider error parameter must fail with OAuthError
        let err_line = "GET /?error=access_denied&state=csrf_state_123 HTTP/1.1\r\n";
        let err_res = parse_oauth_callback_request_line(err_line, "csrf_state_123");
        assert!(matches!(err_res, Err(AuthError::OAuthError(_))));

        // Missing code parameter must fail
        let no_code_line = "GET /?state=csrf_state_123 HTTP/1.1\r\n";
        let no_code_res = parse_oauth_callback_request_line(no_code_line, "csrf_state_123");
        assert!(matches!(no_code_res, Err(AuthError::InvalidCallback(_))));
    }

    #[tokio::test]
    async fn test_loopback_listener_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            wait_for_oauth_callback(&listener, "expected_state_token").await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(
                b"GET /?code=loopback_code_xyz&state=expected_state_token HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            )
            .await
            .unwrap();

        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("Authentication complete."));

        let extracted_code = server_task.await.unwrap().unwrap();
        assert_eq!(extracted_code, "loopback_code_xyz");
    }

    #[tokio::test]
    async fn test_static_auth_manager_and_token_expiry() {
        let auth = AuthManager::new_static_for_test("fake-static-token");
        assert_eq!(auth.get_access_token().await.unwrap(), "fake-static-token");
        assert_eq!(auth.force_refresh().await.unwrap(), "fake-static-token");

        let valid_token = CachedToken {
            access_token: "t1".to_string(),
            expires_at: Some(Instant::now() + Duration::from_secs(300)),
        };
        assert!(valid_token.is_valid(Duration::from_secs(60)));

        let expiring_soon = CachedToken {
            access_token: "t2".to_string(),
            expires_at: Some(Instant::now() + Duration::from_secs(30)),
        };
        assert!(!expiring_soon.is_valid(Duration::from_secs(60)));
    }

    #[test]
    fn test_auth_error_to_errno_mapping() {
        assert_eq!(
            AuthError::MissingCredentials("none".into()).to_errno(),
            libc::EACCES
        );
        assert_eq!(
            AuthError::OAuthError("denied".into()).to_errno(),
            libc::EACCES
        );
        assert_eq!(AuthError::TokenExpired.to_errno(), libc::EACCES);
        assert_eq!(
            AuthError::InvalidCallback("bad state".into()).to_errno(),
            libc::EACCES
        );
        assert_eq!(
            AuthError::ConfigError("invalid toml".into()).to_errno(),
            libc::EIO
        );
        assert_eq!(
            AuthError::KeyringError("dbus error".into()).to_errno(),
            libc::EIO
        );
        assert_eq!(
            AuthError::BrowserLaunchError("no display".into()).to_errno(),
            libc::EIO
        );
    }
}
