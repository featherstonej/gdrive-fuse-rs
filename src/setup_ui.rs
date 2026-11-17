use std::collections::HashMap;
use std::ffi::OsStr;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use oauth2::CsrfToken;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use url::form_urlencoded;

use crate::auth::{builtin_default_credentials, validate_credentials, AppConfig, GoogleDocsMode};
use crate::default_mount_point;

const SETUP_UI_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_SETUP_HTTP_REQUEST_BYTES: usize = 32 * 1024;

struct SetupCodeFile {
    dir: PathBuf,
    path: PathBuf,
}

impl SetupCodeFile {
    fn create(code: &str) -> Result<Self> {
        let dir = std::env::temp_dir().join(format!(
            "gdrive-fuse-rs-setup-{}",
            CsrfToken::new_random_len(24).secret()
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir)
            .context("Failed to create private setup directory")?;
        let code_file = Self {
            path: dir.join("code"),
            dir,
        };
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&code_file.path)
            .context("Failed to create private setup code file")?;
        file.write_all(code.as_bytes())
            .context("Failed to write private setup code")?;
        Ok(code_file)
    }
}

impl Drop for SetupCodeFile {
    fn drop(&mut self) {
        if let Err(err) = std::fs::remove_file(&self.path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                log::warn!("Failed to remove setup code file: {}", err);
            }
        }
        if let Err(err) = std::fs::remove_dir(&self.dir) {
            log::warn!("Failed to remove private setup directory: {}", err);
        }
    }
}

/// Output returned after the user completes and saves the Local Browser Setup Wizard.
#[derive(Debug, Clone)]
pub struct SetupWizardOutcome {
    pub config: AppConfig,
    pub enable_autostart: bool,
    pub switch_account: bool,
    pub optimize_nautilus: bool,
}

/// Launches a local loopback HTTP configuration wizard (`http://127.0.0.1:<port>`) in the user's browser,
/// waits for the user to save their settings, and returns the resulting `SetupWizardOutcome`.
pub async fn run_browser_setup_wizard(
    initial_config: &AppConfig,
    cli_mount: Option<PathBuf>,
    cli_client_id: Option<String>,
    cli_client_secret: Option<String>,
    open_browser: bool,
    enable_autostart: bool,
) -> Result<SetupWizardOutcome> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("Failed to bind local loopback server for setup wizard")?;
    let local_addr = listener
        .local_addr()
        .context("Failed to read setup wizard local address")?;
    let csrf_token = CsrfToken::new_random_len(32).secret().to_owned();
    let setup_code = SetupCodeFile::create(&csrf_token)?;
    let setup_url = format!("http://127.0.0.1:{}/", local_addr.port());

    let existing_creds = validate_credentials(
        cli_client_id.clone(),
        cli_client_secret.clone(),
        initial_config,
    )
    .ok();
    let builtin_creds = builtin_default_credentials();
    let has_available_creds = existing_creds.is_some() || builtin_creds.is_some();
    let using_only_builtin = builtin_creds.is_some()
        && cli_client_id.is_none()
        && initial_config.client_id.is_none()
        && existing_creds.as_ref().map(|c| &c.client_id)
            == builtin_creds.as_ref().map(|b| &b.client_id);

    let default_mp = cli_mount
        .or_else(|| initial_config.mount_point_path.clone())
        .unwrap_or_else(default_mount_point);
    let default_client_id = if using_only_builtin {
        String::new()
    } else {
        existing_creds
            .as_ref()
            .map(|c| c.client_id.clone())
            .unwrap_or_default()
    };
    let default_docs_mode = initial_config.google_docs_mode.unwrap_or_default();
    let default_read_only = initial_config.read_only.unwrap_or(false);
    let default_shared_with_me = initial_config.shared_with_me.unwrap_or(true);

    println!("\n======================================================================");
    println!("  gdrive-fuse-rs Initial Setup & Configuration");
    println!("======================================================================");
    println!("Please configure your Google Drive mount in your browser:");
    println!("\n  {}\n", setup_url);
    println!("Read the setup code from: {}", setup_code.path.display());
    println!("Waiting for configuration to be saved...");
    println!("======================================================================\n");

    let _ = std::process::Command::new("notify-send")
        .args([
            "--app-name=Google Drive (gdrive-fuse-rs)",
            "--icon=folder-remote",
            "Google Drive Setup",
            "Opening configuration wizard in your web browser...",
        ])
        .status();

    if open_browser {
        open_setup_window(&setup_url);
    }

    serve_setup_wizard(
        listener,
        initial_config,
        &csrf_token,
        &setup_code.path,
        default_mp,
        default_client_id,
        has_available_creds,
        default_docs_mode,
        default_read_only,
        default_shared_with_me,
        enable_autostart,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn serve_setup_wizard(
    listener: TcpListener,
    initial_config: &AppConfig,
    csrf_token: &str,
    code_path: &Path,
    default_mp: PathBuf,
    default_client_id: String,
    has_available_creds: bool,
    default_docs_mode: GoogleDocsMode,
    default_read_only: bool,
    default_shared_with_me: bool,
    default_autostart: bool,
) -> Result<SetupWizardOutcome> {
    let local_addr = listener.local_addr()?;
    let deadline = tokio::time::Instant::now() + SETUP_UI_TIMEOUT;
    let mut error_banner: Option<String> = None;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            bail!("Timed out waiting for setup wizard completion in the browser");
        }

        let (mut stream, _) = tokio::time::timeout(remaining, listener.accept())
            .await
            .context("Timed out waiting for setup wizard connection")?
            .context("Failed to accept connection on setup wizard listener")?;

        // Chromium-based browsers (Vivaldi, Chrome, Brave) open speculative idle preconnect
        // sockets alongside the real HTTP request. Bound each socket read to 1.5s so an idle
        // preconnect socket never blocks the real GET/POST request.
        let raw_req =
            match tokio::time::timeout(Duration::from_millis(1500), read_http_request(&mut stream))
                .await
            {
                Ok(Ok(bytes)) if !bytes.is_empty() => bytes,
                _ => continue,
            };
        let req_str = String::from_utf8_lossy(&raw_req);
        let first_line = req_str.lines().next().unwrap_or("");
        let mut parts = first_line.split_whitespace();
        let method = parts.next().unwrap_or("");
        let path = parts.next().unwrap_or("");

        if !valid_setup_headers(&req_str, local_addr.port()) {
            send_html_response(&mut stream, "403 Forbidden", "Forbidden").await;
            continue;
        }

        if method == "GET" && path == "/" {
            let html = format!(
                r#"<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><title>Google Drive Setup</title></head><body>
                <h1>Google Drive Setup</h1><p>Open <code>{}</code> and paste its contents below.</p>
                <form method="POST" action="/unlock"><label>Setup code <input name="csrf_token" type="password" required autofocus></label>
                <button type="submit">Continue</button></form></body></html>"#,
                escape_html_attr(&code_path.display().to_string())
            );
            send_html_response(&mut stream, "200 OK", &html).await;
        } else if method == "POST" && path == "/unlock" {
            let body = req_str.split("\r\n\r\n").nth(1).unwrap_or("");
            if !form_has_valid_token(body, csrf_token) {
                send_html_response(&mut stream, "403 Forbidden", "Forbidden").await;
                continue;
            }
            let html = render_setup_form_html(
                csrf_token,
                &default_mp.display().to_string(),
                &default_client_id,
                has_available_creds,
                default_docs_mode,
                default_read_only,
                default_shared_with_me,
                default_autostart,
                false,
                false,
                error_banner.as_deref(),
            );
            send_html_response(&mut stream, "200 OK", &html).await;
            error_banner = None;
        } else if method == "POST" && path == "/save" {
            let body = req_str.split("\r\n\r\n").nth(1).unwrap_or("");
            if !form_has_valid_token(body, csrf_token) {
                send_html_response(&mut stream, "403 Forbidden", "Forbidden").await;
                continue;
            }
            let optimize_nautilus = form_urlencoded::parse(body.as_bytes()).any(|(key, value)| {
                key == "optimize_nautilus" && (value == "on" || value == "true")
            });
            match parse_setup_form_submission(body, csrf_token, initial_config).and_then(
                |outcome| {
                    if outcome.optimize_nautilus {
                        apply_nautilus_performance_settings(OsStr::new("gsettings"))?;
                    }
                    Ok(outcome)
                },
            ) {
                Ok(outcome) => {
                    let success_html = render_setup_saved_html(
                        &outcome
                            .config
                            .mount_point_path
                            .as_ref()
                            .unwrap_or(&default_mp)
                            .display()
                            .to_string(),
                    );
                    send_html_response(&mut stream, "200 OK", &success_html).await;
                    return Ok(outcome);
                }
                Err(validation_err) => {
                    error_banner = Some(validation_err.to_string());
                    let fields: HashMap<String, String> = form_urlencoded::parse(body.as_bytes())
                        .into_owned()
                        .collect();
                    let checked =
                        |name: &str| fields.get(name).is_some_and(|v| v == "on" || v == "true");
                    let docs_mode = match fields.get("google_docs_mode").map(String::as_str) {
                        Some("desktop") => GoogleDocsMode::Desktop,
                        Some("pdf") => GoogleDocsMode::Pdf,
                        Some("office") => GoogleDocsMode::Office,
                        _ => GoogleDocsMode::Browser,
                    };
                    let html = render_setup_form_html(
                        csrf_token,
                        fields.get("mount_point").map_or("", String::as_str),
                        fields.get("client_id").map_or("", String::as_str),
                        has_available_creds,
                        docs_mode,
                        checked("read_only"),
                        checked("shared_with_me"),
                        checked("enable_autostart"),
                        optimize_nautilus,
                        checked("switch_account"),
                        error_banner.as_deref(),
                    );
                    send_html_response(&mut stream, "400 Bad Request", &html).await;
                }
            }
        } else {
            send_html_response(
                &mut stream,
                "404 Not Found",
                "<html><body>Not Found</body></html>",
            )
            .await;
        }
    }
}

fn valid_setup_headers(request: &str, port: u16) -> bool {
    let mut hosts = Vec::new();
    let mut origins = Vec::new();
    for line in request
        .split("\r\n")
        .skip(1)
        .take_while(|line| !line.is_empty())
    {
        let Some((name, value)) = line.split_once(':') else {
            return false;
        };
        if name.eq_ignore_ascii_case("host") {
            hosts.push(value.trim());
        } else if name.eq_ignore_ascii_case("origin") {
            origins.push(value.trim());
        }
    }
    let expected_host = format!("127.0.0.1:{port}");
    let expected_origin = format!("http://{expected_host}");
    hosts.as_slice() == [expected_host.as_str()]
        && (origins.is_empty() || origins.as_slice() == [expected_origin.as_str()])
}

fn form_has_valid_token(body: &str, expected: &str) -> bool {
    let mut tokens = form_urlencoded::parse(body.as_bytes())
        .filter_map(|(key, value)| (key == "csrf_token").then_some(value));
    !expected.is_empty()
        && tokens.next().is_some_and(|token| token == expected)
        && tokens.next().is_none()
}

/// Parses and validates a `application/x-www-form-urlencoded` submission from the Setup Wizard.
pub fn parse_setup_form_submission(
    form_body: &str,
    expected_csrf: &str,
    base_config: &AppConfig,
) -> Result<SetupWizardOutcome> {
    let fields: HashMap<String, String> = form_urlencoded::parse(form_body.as_bytes())
        .into_owned()
        .collect();

    if !form_has_valid_token(form_body, expected_csrf) {
        bail!("Invalid security token (CSRF mismatch). Please refresh and try again.");
    }

    let raw_mount = fields
        .get("mount_point")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(expand_tilde_path)
        .unwrap_or_else(default_mount_point);

    let client_id = fields
        .get("client_id")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let client_secret = fields
        .get("client_secret")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let mut test_cfg = base_config.clone();
    if client_id.is_some() && client_id != base_config.client_id {
        test_cfg.client_secret = None;
    }
    if client_id.is_some() {
        test_cfg.client_id = client_id.clone();
    }
    if client_secret.is_some() {
        test_cfg.client_secret = client_secret.clone();
    }

    if validate_credentials(client_id.clone(), client_secret.clone(), &test_cfg).is_err() {
        bail!(
            "A Google OAuth2 Client ID is required. Please enter your Desktop OAuth2 Client ID below."
        );
    }

    let google_docs_mode = match fields.get("google_docs_mode").map(|s| s.as_str()) {
        Some("desktop") => GoogleDocsMode::Desktop,
        Some("pdf") => GoogleDocsMode::Pdf,
        Some("office") => GoogleDocsMode::Office,
        _ => GoogleDocsMode::Browser,
    };

    let read_only = fields
        .get("read_only")
        .is_some_and(|v| v == "on" || v == "true");
    let shared_with_me = fields
        .get("shared_with_me")
        .is_some_and(|v| v == "on" || v == "true");
    let enable_autostart = fields
        .get("enable_autostart")
        .is_some_and(|v| v == "on" || v == "true");
    let switch_account = fields
        .get("switch_account")
        .is_some_and(|v| v == "on" || v == "true");
    let optimize_nautilus = fields
        .get("optimize_nautilus")
        .is_some_and(|v| v == "on" || v == "true");

    let mut updated = test_cfg;
    updated.mount_point_path = Some(raw_mount);
    if client_id.is_some() {
        let credentials = validate_credentials(client_id, client_secret, &updated)?;
        updated.client_id = Some(credentials.client_id);
        updated.client_secret = credentials.client_secret;
    }
    updated.google_docs_mode = Some(google_docs_mode);
    updated.read_only = Some(read_only);
    updated.shared_with_me = Some(shared_with_me);

    Ok(SetupWizardOutcome {
        config: updated,
        enable_autostart,
        switch_account,
        optimize_nautilus,
    })
}

fn apply_nautilus_performance_settings(gsettings: &OsStr) -> Result<()> {
    const SCHEMA: &str = "org.gnome.nautilus.preferences";
    for key in ["show-directory-item-counts", "show-image-thumbnails"] {
        let output = Command::new(gsettings)
            .args(["set", SCHEMA, key, "never"])
            .output()
            .with_context(|| {
                format!("Could not run gsettings to update {key}; earlier settings may already have changed")
            })?;
        if !output.status.success() {
            bail!(
                "Could not update GNOME Files setting {key}: {}. Earlier settings may already have changed.",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let output = Command::new(gsettings)
            .args(["get", SCHEMA, key])
            .output()
            .with_context(|| {
                format!("Could not verify GNOME Files setting {key}; earlier settings may already have changed")
            })?;
        if !output.status.success() {
            bail!(
                "Could not verify GNOME Files setting {key}: {}. Earlier settings may already have changed.",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        if String::from_utf8_lossy(&output.stdout).trim() != "'never'" {
            bail!(
                "GNOME Files setting {key} did not change to 'never'. Earlier settings may already have changed."
            );
        }
    }
    Ok(())
}

fn expand_tilde_path(raw: &str) -> PathBuf {
    if raw == "~" {
        return dirs::home_dir().unwrap_or_else(|| PathBuf::from(raw));
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(raw)
}

/// Opens the setup wizard URL as a standalone focused desktop app window (`--app=...`) when the
/// user's default browser is Chromium-based (Vivaldi, Chrome, Chromium, Brave, Edge), falling back
/// to `open::that`.
fn open_setup_window(setup_url: &str) {
    let default_desktop = std::process::Command::new("xdg-settings")
        .args(["get", "default-web-browser"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    let candidate_bin = if default_desktop.contains("vivaldi") {
        Some("vivaldi-stable")
    } else if default_desktop.contains("google-chrome") {
        Some("google-chrome")
    } else if default_desktop.contains("chromium") {
        Some("chromium")
    } else if default_desktop.contains("brave") {
        Some("brave-browser")
    } else if default_desktop.contains("microsoft-edge") {
        Some("microsoft-edge")
    } else {
        None
    };

    if let Some(bin) = candidate_bin {
        if std::process::Command::new(bin)
            .arg(format!("--app={setup_url}"))
            .spawn()
            .is_ok()
        {
            return;
        }
    }

    if let Err(e) = open::that(setup_url) {
        log::warn!(
            "Could not automatically launch web browser ({}). Please open {} manually.",
            e,
            setup_url
        );
    }
}

async fn read_http_request(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut request = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = stream.read(&mut chunk).await.unwrap_or(0);
        if n == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..n]);
        if request.len() >= MAX_SETUP_HTTP_REQUEST_BYTES {
            break;
        }
        if let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if request.len() >= header_end + 4 + content_length {
                break;
            }
        }
    }
    Ok(request)
}

async fn send_html_response(stream: &mut TcpStream, status: &str, html: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: text/html; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Cache-Control: no-store\r\n\
         Referrer-Policy: no-referrer\r\n\
         Content-Security-Policy: default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'\r\n\
         Connection: close\r\n\r\n\
         {html}",
        html.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
}

fn escape_html_attr(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(ch),
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn render_setup_form_html(
    csrf_token: &str,
    mount_point: &str,
    client_id: &str,
    has_available_creds: bool,
    docs_mode: GoogleDocsMode,
    read_only: bool,
    shared_with_me: bool,
    enable_autostart: bool,
    optimize_nautilus: bool,
    switch_account: bool,
    error_message: Option<&str>,
) -> String {
    let error_block = match error_message {
        Some(msg) => format!(
            "<div class=\"error-banner\">{}</div>",
            escape_html_attr(msg)
        ),
        None => String::new(),
    };

    let sel_browser = if docs_mode == GoogleDocsMode::Browser {
        "selected"
    } else {
        ""
    };
    let sel_desktop = if docs_mode == GoogleDocsMode::Desktop {
        "selected"
    } else {
        ""
    };
    let sel_pdf = if docs_mode == GoogleDocsMode::Pdf {
        "selected"
    } else {
        ""
    };
    let sel_office = if docs_mode == GoogleDocsMode::Office {
        "selected"
    } else {
        ""
    };

    let chk_ro = if read_only { "checked" } else { "" };
    let chk_shared = if shared_with_me { "checked" } else { "" };
    let chk_auto = if enable_autostart { "checked" } else { "" };
    let chk_optimize = if optimize_nautilus { "checked" } else { "" };
    let chk_switch = if switch_account { "checked" } else { "" };

    let (oauth_details_attr, oauth_summary, cid_required, cid_placeholder) = if has_available_creds
    {
        (
            "",
            "Advanced: Use custom Google Cloud OAuth2 Client ID (Optional)",
            "",
            "Leave blank to use default OAuth2 Client ID",
        )
    } else {
        (
            "open",
            "Google Cloud OAuth2 Client Credentials (Required)",
            "required",
            "123456789-abc.apps.googleusercontent.com",
        )
    };

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Google Drive (gdrive-fuse-rs) Setup</title>
<style>
  :root {{
    color-scheme: light dark;
    --bg: #f6f8fc;
    --card: #ffffff;
    --text: #1f1f1f;
    --muted: #5f6368;
    --border: #dadce0;
    --primary: #1a73e8;
    --primary-hover: #1557b0;
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{
      --bg: #131314;
      --card: #1e1f20;
      --text: #e3e3e3;
      --muted: #9aa0a6;
      --border: #3c4043;
      --primary: #8ab4f8;
      --primary-hover: #aecbfa;
    }}
  }}
  body {{
    font-family: system-ui, -apple-system, 'Segoe UI', Roboto, Ubuntu, Cantarell, sans-serif;
    background: var(--bg);
    color: var(--text);
    margin: 0;
    padding: 2rem 1rem;
    display: flex;
    justify-content: center;
  }}
  .card {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 16px;
    max-width: 540px;
    width: 100%;
    padding: 2rem;
    box-shadow: 0 8px 24px rgba(0,0,0,0.08);
  }}
  h1 {{
    margin: 0 0 0.4rem 0;
    font-size: 1.45rem;
  }}
  p.subtitle {{
    margin: 0 0 1.5rem 0;
    color: var(--muted);
    font-size: 0.95rem;
    line-height: 1.45;
  }}
  .error-banner {{
    background: rgba(217, 48, 37, 0.12);
    border: 1px solid #d93025;
    color: #d93025;
    padding: 0.75rem 1rem;
    border-radius: 8px;
    margin-bottom: 1.25rem;
    font-size: 0.9rem;
  }}
  .field {{
    margin-bottom: 1.15rem;
  }}
  label.block {{
    display: block;
    font-weight: 600;
    font-size: 0.9rem;
    margin-bottom: 0.35rem;
  }}
  .hint {{
    color: var(--muted);
    font-size: 0.8rem;
    margin-top: 0.25rem;
  }}
  input[type="text"], input[type="password"], select {{
    width: 100%;
    box-sizing: border-box;
    padding: 0.65rem 0.75rem;
    border-radius: 8px;
    border: 1px solid var(--border);
    background: transparent;
    color: var(--text);
    font-size: 0.95rem;
  }}
  .checkbox-row {{
    display: flex;
    align-items: flex-start;
    gap: 0.65rem;
    margin-bottom: 0.85rem;
  }}
  .checkbox-row input {{
    margin-top: 0.25rem;
    transform: scale(1.15);
  }}
  details {{
    margin-bottom: 1.2rem;
    font-size: 0.85rem;
    color: var(--muted);
    border: 1px dashed var(--border);
    border-radius: 8px;
    padding: 0.65rem 0.85rem;
  }}
  summary {{
    cursor: pointer;
    font-weight: 600;
    color: var(--text);
  }}
  button {{
    width: 100%;
    padding: 0.8rem 1.2rem;
    border: none;
    border-radius: 10px;
    background: var(--primary);
    color: #fff;
    font-weight: 600;
    font-size: 1rem;
    cursor: pointer;
    margin-top: 0.5rem;
  }}
  @media (prefers-color-scheme: dark) {{
    button {{ color: #131314; }}
  }}
  button:hover {{
    background: var(--primary-hover);
  }}
  button.btn-secondary {{
    background: transparent;
    color: var(--primary);
    border: 1px solid var(--border);
    margin-top: 0.65rem;
  }}
  button.btn-secondary:hover {{
    background: rgba(128, 128, 128, 0.1);
  }}
</style>
</head>
<body>
  <div class="card">
    <h1>Google Drive Setup</h1>
    <p class="subtitle">Configure how <code>gdrive-fuse-rs</code> mounts your Google Drive in GNOME Files. Settings are saved to <code>~/.config/gdrive-fuse-rs/config.toml</code>.</p>
    {error_block}
    <form method="POST" action="/save">
      <input type="hidden" name="csrf_token" value="{csrf}">
      <div class="field">
        <label class="block" for="mount_point">Mount Folder Path</label>
        <input type="text" id="mount_point" name="mount_point" value="{mp}" required>
        <div class="hint">Created automatically if it does not exist yet.</div>
      </div>

      <div class="field">
        <label class="block" for="google_docs_mode">Google Docs / Sheets / Slides Mode</label>
        <select id="google_docs_mode" name="google_docs_mode">
          <option value="browser" {sel_browser}>Browser Shortcut (.gdoc / .gsheet / .gslides)</option>
          <option value="desktop" {sel_desktop}>GNOME Desktop Launcher (.desktop)</option>
          <option value="pdf" {sel_pdf}>Export on-the-fly as PDF (.pdf)</option>
          <option value="office" {sel_office}>Export on-the-fly as Office (.docx / .xlsx / .pptx)</option>
        </select>
      </div>

      <div class="checkbox-row">
        <input type="checkbox" id="shared_with_me" name="shared_with_me" {chk_shared}>
        <label for="shared_with_me">
          <strong>Show "Shared with me" folder</strong><br>
          <span class="hint">Includes a top-level virtual folder with items shared with your account.</span>
        </label>
      </div>

      <div class="checkbox-row">
        <input type="checkbox" id="read_only" name="read_only" {chk_ro}>
        <label for="read_only">
          <strong>Strict Read-Only Mode</strong><br>
          <span class="hint">Blocks local writes/deletes. New sign-ins request <code>drive.readonly</code>, but an existing full-access grant may be reused.</span>
        </label>
      </div>

      <div class="checkbox-row">
        <input type="checkbox" id="enable_autostart" name="enable_autostart" {chk_auto}>
        <label for="enable_autostart">
          <strong>Auto-mount in the background on login</strong><br>
          <span class="hint">Enables and starts the <code>gdrive-fuse-rs.service</code> user systemd service.</span>
        </label>
      </div>

      <div class="checkbox-row">
        <input type="checkbox" id="optimize_nautilus" name="optimize_nautilus" {chk_optimize}>
        <label for="optimize_nautilus">
          <strong>Reduce GNOME Files background loading</strong><br>
          <span class="hint">Disable folder item counts and thumbnails for all folders in GNOME Files, not just Google Drive. This persists for your user until you change these preferences.</span>
        </label>
      </div>

      <div class="checkbox-row">
        <input type="checkbox" id="switch_account" name="switch_account" {chk_switch}>
        <label for="switch_account">
          <strong>Switch Google Account</strong><br>
          <span class="hint">Signs out of the current Google account, clears the local metadata cache, and prompts you to sign in with a different account.</span>
        </label>
      </div>

      <details {oauth_details_attr}>
        <summary>{oauth_summary}</summary>
        <div style="margin-top:0.85rem;">
          <div class="field">
            <label class="block" for="client_id">Google OAuth2 Client ID</label>
            <input type="text" id="client_id" name="client_id" value="{cid}" placeholder="{cid_placeholder}" {cid_required}>
          </div>

          <div class="field">
            <label class="block" for="client_secret">Google OAuth2 Client Secret</label>
            <input type="password" id="client_secret" name="client_secret" value="" placeholder="GOCSPX-..." autocomplete="new-password">
            <div class="hint">Leave blank to keep the existing secret for this Client ID.</div>
          </div>

          <ol style="margin:0.5rem 0 0 1.2rem;padding:0;line-height:1.5;">
            <li>Open the <strong>Google Cloud Console</strong> &rarr; <em>APIs &amp; Services</em> &rarr; <em>Library</em> and enable the <strong>Google Drive API</strong>.</li>
            <li>Go to <em>Credentials</em> &rarr; <em>Create Credentials</em> &rarr; <em>OAuth client ID</em>.</li>
            <li>Choose Application type: <strong>Desktop app</strong>, then copy the Client ID and Client Secret here.</li>
          </ol>
        </div>
      </details>

      <button type="submit">Sign in with Google &amp; Mount</button>
      <button type="submit" name="switch_account" value="on" class="btn-secondary">Switch Google Account &amp; Sign In</button>
    </form>
  </div>
</body>
</html>"#,
        csrf = escape_html_attr(csrf_token),
        mp = escape_html_attr(mount_point),
        cid = escape_html_attr(client_id),
    )
}

fn render_setup_saved_html(mount_point: &str) -> String {
    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Configuration Submitted - gdrive-fuse-rs</title>
<style>
  :root {{ color-scheme: light dark; }}
  body {{
    font-family: system-ui, -apple-system, sans-serif;
    display: flex;
    justify-content: center;
    align-items: center;
    min-height: 85vh;
    margin: 0;
  }}
  .card {{
    max-width: 460px;
    padding: 2rem;
    border-radius: 16px;
    border: 1px solid rgba(128,128,128,0.3);
    text-align: center;
  }}
</style>
</head>
<body>
  <div class="card">
    <h2>Configuration Submitted</h2>
    <p>Setup will attempt to mount Google Drive at <code>{}</code>.</p>
    <p style="opacity:0.75;font-size:0.9rem;">Complete Google Sign-In if prompted. Check the application logs for the final setup result.</p>
  </div>
</body>
</html>"#,
        escape_html_attr(mount_point)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_setup_code_file_is_private_and_removed() {
        let code = SetupCodeFile::create("private-code").unwrap();
        let path = code.path.clone();
        let dir = code.dir.clone();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "private-code");
        assert_eq!(
            std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        drop(code);
        assert!(!path.exists());
        assert!(!dir.exists());
    }

    #[test]
    fn test_setup_request_host_origin_and_token_validation() {
        assert!(valid_setup_headers(
            "POST / HTTP/1.1\r\nHost: 127.0.0.1:1234\r\nOrigin: http://127.0.0.1:1234\r\n\r\n",
            1234
        ));
        assert!(!valid_setup_headers(
            "GET / HTTP/1.1\r\nHost: attacker.invalid:1234\r\n\r\n",
            1234
        ));
        assert!(!valid_setup_headers(
            "POST / HTTP/1.1\r\nHost: 127.0.0.1:1234\r\nOrigin: https://attacker.invalid\r\n\r\n",
            1234
        ));
        assert!(!form_has_valid_token(
            "csrf_token=good&csrf_token=bad",
            "good"
        ));
    }

    #[test]
    fn test_changing_client_id_does_not_keep_previous_secret() {
        let initial = AppConfig {
            client_id: Some("old-client".into()),
            client_secret: Some("old-secret".into()),
            ..Default::default()
        };
        let result =
            parse_setup_form_submission("csrf_token=good&client_id=new-client", "good", &initial)
                .unwrap();
        assert_eq!(result.config.client_id.as_deref(), Some("new-client"));
        assert_eq!(result.config.client_secret, None);
    }

    #[tokio::test]
    async fn test_setup_session_requires_secret_and_never_renders_client_secret() {
        async fn request(addr: std::net::SocketAddr, request: &str) -> String {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            response
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let initial = AppConfig {
                client_id: Some("example-id".into()),
                client_secret: Some("private-value".into()),
                ..Default::default()
            };
            serve_setup_wizard(
                listener,
                &initial,
                "session-token",
                Path::new("/private/setup-code"),
                PathBuf::from("/drive"),
                "example-id".into(),
                true,
                GoogleDocsMode::Browser,
                false,
                true,
                true,
            )
            .await
        });
        let host = format!("127.0.0.1:{}", addr.port());
        let login = request(addr, &format!("GET / HTTP/1.1\r\nHost: {host}\r\n\r\n")).await;
        assert!(login.starts_with("HTTP/1.1 200"));
        assert!(!login.contains("private-value"));
        assert!(!login.contains("session-token"));
        assert!(
            request(addr, "GET / HTTP/1.1\r\nHost: attacker.invalid\r\n\r\n")
                .await
                .starts_with("HTTP/1.1 403")
        );
        let body = "csrf_token=session-token";
        let response = request(
            addr,
            &format!(
                "POST /unlock HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            ),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"));
        assert!(!response.contains("private-value"));
        assert!(response.contains(r#"name="client_secret" value="""#));
        let body = "csrf_token=wrong";
        let unlock = format!(
            "POST /unlock HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        assert!(request(addr, &unlock).await.starts_with("HTTP/1.1 403"));
        let post = format!(
            "POST /save HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        assert!(request(addr, &post).await.starts_with("HTTP/1.1 403"));
        let post = format!("POST /save HTTP/1.1\r\nHost: {host}\r\nOrigin: https://attacker.invalid\r\nContent-Length: {}\r\n\r\n{body}", body.len());
        assert!(request(addr, &post).await.starts_with("HTTP/1.1 403"));
        let body = "csrf_token=session-token&client_id=example-id&mount_point=%2Fdrive";
        let post = format!(
            "POST /save HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        assert!(request(addr, &post).await.starts_with("HTTP/1.1 200"));
        assert_eq!(
            server
                .await
                .unwrap()
                .unwrap()
                .config
                .client_secret
                .as_deref(),
            Some("private-value")
        );
    }

    #[tokio::test]
    async fn test_validation_error_preserves_submitted_fields() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            serve_setup_wizard(
                listener,
                &AppConfig::default(),
                "session",
                Path::new("/private/setup-code"),
                PathBuf::from("/old"),
                String::new(),
                false,
                GoogleDocsMode::Browser,
                false,
                true,
                true,
            )
            .await
        });
        let body =
            "csrf_token=session&mount_point=%2Fnew&client_id=&google_docs_mode=pdf&read_only=on";
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let request = format!(
            "POST /save HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nContent-Length: {}\r\n\r\n{body}",
            addr.port(),
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 400"));
        assert!(response.contains(r#"name="mount_point" value="/new""#));
        assert!(response.contains(r#"value="pdf" selected"#));
        assert!(response.contains(r#"name="read_only" checked"#));
        assert!(!response.contains(r#"name="enable_autostart" checked"#));
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn test_setup_wizard_survives_idle_preconnect_socket() {
        let initial = AppConfig {
            client_id: Some("prefilled-id.apps.googleusercontent.com".to_string()),
            client_secret: Some("prefilled-secret".to_string()),
            ..Default::default()
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let csrf = "csrf-test-1";

        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let raw_req = match tokio::time::timeout(
                    Duration::from_millis(200),
                    read_http_request(&mut stream),
                )
                .await
                {
                    Ok(Ok(bytes)) if !bytes.is_empty() => bytes,
                    _ => continue,
                };
                let req_str = String::from_utf8_lossy(&raw_req);
                if req_str.starts_with("POST /save") {
                    let body = req_str.split("\r\n\r\n").nth(1).unwrap_or("");
                    let outcome = parse_setup_form_submission(body, csrf, &initial).unwrap();
                    send_html_response(&mut stream, "200 OK", "saved").await;
                    return outcome;
                }
            }
        });

        // 1. Simulate Chromium idle preconnect socket (connects and sends nothing)
        let _idle_socket = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;

        // 2. Send real POST /save request on second connection
        let mut client = TcpStream::connect(addr).await.unwrap();
        let form = "csrf_token=csrf-test-1&mount_point=%2Ftmp%2Ftest-drive&client_id=prefilled-id.apps.googleusercontent.com&google_docs_mode=browser&shared_with_me=on&enable_autostart=on";
        let req = format!(
            "POST /save HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\n\r\n{}",
            form.len(),
            form
        );
        client.write_all(req.as_bytes()).await.unwrap();

        let outcome = server.await.unwrap();
        assert_eq!(
            outcome.config.mount_point_path,
            Some(PathBuf::from("/tmp/test-drive"))
        );
    }

    #[test]
    fn test_parse_setup_form_submission_valid_and_csrf_enforcement() {
        let base = AppConfig::default();
        let body = "csrf_token=tok_123&mount_point=%2Ftmp%2Fmy-drive&client_id=my-id.apps.googleusercontent.com&client_secret=my-sec&google_docs_mode=pdf&shared_with_me=on&enable_autostart=on&switch_account=on";

        let outcome =
            parse_setup_form_submission(body, "tok_123", &base).expect("Valid form should parse");
        assert_eq!(
            outcome.config.mount_point_path,
            Some(PathBuf::from("/tmp/my-drive"))
        );
        assert_eq!(
            outcome.config.client_id.as_deref(),
            Some("my-id.apps.googleusercontent.com")
        );
        assert_eq!(outcome.config.client_secret.as_deref(), Some("my-sec"));
        assert_eq!(outcome.config.google_docs_mode, Some(GoogleDocsMode::Pdf));
        assert_eq!(outcome.config.read_only, Some(false));
        assert_eq!(outcome.config.shared_with_me, Some(true));
        assert!(outcome.enable_autostart);
        assert!(outcome.switch_account);
        assert!(!outcome.optimize_nautilus);

        let opted_in = format!("{body}&optimize_nautilus=on");
        assert!(
            parse_setup_form_submission(&opted_in, "tok_123", &base)
                .unwrap()
                .optimize_nautilus
        );

        // Verify CSRF mismatch is rejected
        assert!(parse_setup_form_submission(body, "wrong_token", &base).is_err());
    }

    #[test]
    fn test_nautilus_option_is_unchecked_by_default() {
        let html = render_setup_form_html(
            "token",
            "/tmp/drive",
            "",
            true,
            GoogleDocsMode::Browser,
            false,
            true,
            true,
            false,
            false,
            None,
        );
        assert!(html.contains(r#"id="optimize_nautilus" name="optimize_nautilus" >"#));
        assert!(html.contains("for all folders in GNOME Files"));
    }

    #[test]
    fn test_apply_nautilus_settings_and_report_failure() {
        let test_dir = std::env::temp_dir().join(format!(
            "gdrive-fuse-nautilus-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&test_dir).unwrap();
        let script = test_dir.join("gsettings");
        let log = test_dir.join("gsettings.log");
        std::fs::write(
            &script,
            r#"#!/bin/sh
printf '%s\n' "$*" >> "$0.log"
case "$*" in
  "set org.gnome.nautilus.preferences show-directory-item-counts never"|"set org.gnome.nautilus.preferences show-image-thumbnails never") exit 0 ;;
  "get org.gnome.nautilus.preferences show-directory-item-counts"|"get org.gnome.nautilus.preferences show-image-thumbnails") printf "'never'\n" ;;
  *) exit 1 ;;
esac
"#,
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();

        apply_nautilus_performance_settings(script.as_os_str()).unwrap();
        assert_eq!(
            std::fs::read_to_string(&log)
                .unwrap()
                .lines()
                .collect::<Vec<_>>(),
            vec![
                "set org.gnome.nautilus.preferences show-directory-item-counts never",
                "get org.gnome.nautilus.preferences show-directory-item-counts",
                "set org.gnome.nautilus.preferences show-image-thumbnails never",
                "get org.gnome.nautilus.preferences show-image-thumbnails",
            ]
        );
        assert!(apply_nautilus_performance_settings(OsStr::new("/bin/false")).is_err());
        assert!(apply_nautilus_performance_settings(OsStr::new("/bin/true")).is_err());
        std::fs::remove_file(log).unwrap();
        std::fs::remove_file(script).unwrap();
        std::fs::remove_dir(test_dir).unwrap();
    }
}
