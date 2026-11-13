use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, LOCATION, RANGE};
use reqwest::StatusCode;
use serde::{Deserialize, Deserializer, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::auth::{AuthError, AuthManager};

pub const DRIVE_API_BASE_URL: &str = "https://www.googleapis.com/drive/v3";
pub const DRIVE_UPLOAD_BASE_URL: &str = "https://www.googleapis.com/upload/drive/v3";
pub const FOLDER_MIME_TYPE: &str = "application/vnd.google-apps.folder";
pub const GOOGLE_APPS_MIME_PREFIX: &str = "application/vnd.google-apps.";

/// Default chunk size (8 MB = 32 * 256 KB) for Google Drive Resumable Uploads.
/// Google Drive requires intermediate resumable upload chunks to be multiples of 256 KB (262,144 bytes).
pub const RESUMABLE_UPLOAD_CHUNK_SIZE: usize = 8 * 1024 * 1024;

const LIST_PAGE_SIZE: &str = "1000";
const LIST_FIELDS: &str =
    "nextPageToken,files(id,name,mimeType,size,modifiedTime,parents,trashed,webViewLink)";
const FILE_FIELDS: &str = "id,name,mimeType,size,modifiedTime,parents,trashed,webViewLink";
const CHANGES_FIELDS: &str = "nextPageToken,newStartPageToken,changes(fileId,removed,file(id,name,mimeType,size,modifiedTime,parents,trashed,webViewLink))";

/// Errors returned by the Google Drive REST API v3 client.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("Authentication error: {0}")]
    Auth(#[from] AuthError),

    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Drive file or resource not found: {0}")]
    NotFound(String),

    #[error("Unauthorized or permission denied: {0}")]
    Unauthorized(String),

    #[error("Rate limited or service temporarily unavailable: {0}")]
    RateLimited(String),

    #[error("Unexpected HTTP status {status}: {body}")]
    UnexpectedStatus { status: u16, body: String },

    #[error("Invalid byte range requested: start ({start}) > end ({end})")]
    InvalidRange { start: u64, end: u64 },

    #[error("JSON serialization/deserialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("I/O error during local file upload: {0}")]
    Io(#[from] std::io::Error),
}

impl ApiError {
    /// Maps `ApiError` into standard POSIX `libc` error codes according to AGENTS.md.
    pub fn to_errno(&self) -> libc::c_int {
        match self {
            ApiError::NotFound(_) => libc::ENOENT,
            ApiError::Unauthorized(_) => libc::EACCES,
            ApiError::Auth(auth_err) => auth_err.to_errno(),
            ApiError::RateLimited(_) => libc::EAGAIN,
            ApiError::InvalidRange { .. } => libc::EINVAL,
            ApiError::Http(err) => {
                if err.is_timeout() {
                    libc::EAGAIN
                } else {
                    libc::EIO
                }
            }
            ApiError::UnexpectedStatus { .. }
            | ApiError::Serialization(_)
            | ApiError::Io(_) => libc::EIO,
        }
    }

    /// Returns `true` if this error indicates an expired or invalid `changes.list` page token
    /// (HTTP `410 Gone`, or HTTP `400 Bad Request` referencing `pageToken` / `Invalid Value`).
    pub fn is_expired_page_token(&self) -> bool {
        match self {
            ApiError::UnexpectedStatus { status: 410, .. } => true,
            ApiError::UnexpectedStatus { status: 400, body } => {
                body.contains("pageToken") || body.contains("Invalid Value")
            }
            _ => false,
        }
    }
}

/// Represents a file or folder returned by Google Drive API v3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriveFile {
    pub id: String,
    pub name: String,
    #[serde(rename = "mimeType")]
    pub mime_type: String,
    #[serde(default, deserialize_with = "deserialize_u64_or_zero")]
    pub size: u64,
    #[serde(
        rename = "modifiedTime",
        default,
        deserialize_with = "deserialize_rfc3339_or_zero"
    )]
    pub modified_time: i64,
    #[serde(default)]
    pub parents: Vec<String>,
    #[serde(default)]
    pub trashed: bool,
    #[serde(rename = "webViewLink", default)]
    pub web_view_link: Option<String>,
}

impl DriveFile {
    /// Returns `true` if this entry is a Google Drive folder.
    pub fn is_dir(&self) -> bool {
        self.mime_type == FOLDER_MIME_TYPE
    }

    /// Returns `true` if this entry is a Google Workspace native document
    /// (e.g. Google Docs, Sheets, Slides) rather than a folder or binary file.
    pub fn is_google_native_doc(&self) -> bool {
        self.mime_type.starts_with(GOOGLE_APPS_MIME_PREFIX) && !self.is_dir()
    }

    /// Returns the `webViewLink` if present and non-empty, or synthesizes a canonical
    /// Google Workspace / Drive URL based on `mime_type` and `id`.
    pub fn web_url(&self) -> String {
        if let Some(ref link) = self.web_view_link {
            if !link.trim().is_empty() {
                return link.clone();
            }
        }
        match self.mime_type.as_str() {
            "application/vnd.google-apps.document" => {
                format!("https://docs.google.com/document/d/{}/edit", self.id)
            }
            "application/vnd.google-apps.spreadsheet" => {
                format!("https://docs.google.com/spreadsheets/d/{}/edit", self.id)
            }
            "application/vnd.google-apps.presentation" => {
                format!("https://docs.google.com/presentation/d/{}/edit", self.id)
            }
            _ => format!("https://drive.google.com/open?id={}", self.id),
        }
    }
}

/// Paginated response returned by `GET /drive/v3/files`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileListResponse {
    #[serde(rename = "nextPageToken")]
    pub next_page_token: Option<String>,
    #[serde(default)]
    pub files: Vec<DriveFile>,
}

/// User storage quota returned by `GET /drive/v3/about?fields=storageQuota`.
/// Note: `limit` is `0` on unlimited Google Workspace accounts where Google omits the field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StorageQuota {
    #[serde(default, deserialize_with = "deserialize_u64_or_zero")]
    pub limit: u64,
    #[serde(default, deserialize_with = "deserialize_u64_or_zero")]
    pub usage: u64,
    #[serde(
        rename = "usageInDrive",
        default,
        deserialize_with = "deserialize_u64_or_zero"
    )]
    pub usage_in_drive: u64,
    #[serde(
        rename = "usageInDriveTrash",
        default,
        deserialize_with = "deserialize_u64_or_zero"
    )]
    pub usage_in_drive_trash: u64,
}

/// Response wrapper for `GET /drive/v3/about`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AboutResponse {
    #[serde(rename = "storageQuota", default)]
    pub storage_quota: StorageQuota,
}

/// Response returned by `GET /drive/v3/changes/startPageToken`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartPageTokenResponse {
    #[serde(rename = "startPageToken")]
    pub start_page_token: String,
}

/// Represents a single change entry from `GET /drive/v3/changes`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeItem {
    #[serde(rename = "fileId", default)]
    pub file_id: String,
    #[serde(default)]
    pub removed: bool,
    #[serde(default)]
    pub file: Option<DriveFile>,
}

/// Paginated response returned by `GET /drive/v3/changes`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeListResponse {
    #[serde(rename = "nextPageToken")]
    pub next_page_token: Option<String>,
    #[serde(rename = "newStartPageToken")]
    pub new_start_page_token: Option<String>,
    #[serde(default)]
    pub changes: Vec<ChangeItem>,
}

fn deserialize_u64_or_zero<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrU64 {
        Str(String),
        Num(u64),
        Signed(i64),
    }

    let opt = Option::<StringOrU64>::deserialize(deserializer)?;
    match opt {
        None => Ok(0),
        Some(StringOrU64::Num(n)) => Ok(n),
        Some(StringOrU64::Signed(n)) => Ok(u64::try_from(n).unwrap_or(0)),
        Some(StringOrU64::Str(s)) => {
            if s.trim().is_empty() {
                Ok(0)
            } else {
                s.trim().parse::<u64>().map_err(serde::de::Error::custom)
            }
        }
    }
}

fn deserialize_rfc3339_or_zero<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrInt {
        Str(String),
        Int(i64),
    }

    let opt = Option::<StringOrInt>::deserialize(deserializer)?;
    match opt {
        None => Ok(0),
        Some(StringOrInt::Int(ts)) => Ok(ts),
        Some(StringOrInt::Str(s)) => {
            if s.trim().is_empty() {
                Ok(0)
            } else {
                chrono::DateTime::parse_from_rfc3339(s.trim())
                    .map(|dt| dt.timestamp())
                    .map_err(serde::de::Error::custom)
            }
        }
    }
}

/// Escapes single quotes and backslashes for safe embedding in a Drive API `q` literal.
pub fn escape_drive_query_literal(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch == '\\' || ch == '\'' {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

/// Formats a Unix timestamp (seconds) as an RFC 3339 UTC timestamp string (e.g. `"2025-05-01T12:00:00Z"`).
pub fn unix_secs_to_rfc3339(timestamp_secs: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp_secs, 0)
        .unwrap_or_else(chrono::Utc::now)
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Infers a standard MIME type from a filename's extension, defaulting to `"application/octet-stream"`.
pub fn guess_mime_type(filename: &str) -> &'static str {
    let ext = filename
        .rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase());
    match ext.as_deref() {
        Some("txt" | "log") => "text/plain",
        Some("md" | "markdown") => "text/markdown",
        Some("csv") => "text/csv",
        Some("html" | "htm") => "text/html",
        Some("css") => "text/css",
        Some("js" | "mjs") => "application/javascript",
        Some("ts" | "tsx") => "application/typescript",
        Some("json") => "application/json",
        Some("xml") => "application/xml",
        Some("toml") => "application/toml",
        Some("yaml" | "yml") => "application/x-yaml",
        Some("rs") => "text/x-rust",
        Some("py") => "text/x-python",
        Some("sh" | "bash") => "application/x-sh",
        Some("pdf") => "application/pdf",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("svg") => "image/svg+xml",
        Some("mp3") => "audio/mpeg",
        Some("wav") => "audio/wav",
        Some("mp4") => "video/mp4",
        Some("webm") => "video/webm",
        Some("zip") => "application/zip",
        Some("gz" | "tgz") => "application/gzip",
        Some("tar") => "application/x-tar",
        Some("doc") => "application/msword",
        Some("docx") => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        Some("xls") => "application/vnd.ms-excel",
        Some("xlsx") => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        Some("ppt") => "application/vnd.ms-powerpoint",
        Some("pptx") => {
            "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        }
        _ => "application/octet-stream",
    }
}

/// Google Drive REST API v3 client.
#[derive(Clone)]
pub struct DriveClient {
    http: reqwest::Client,
    auth: Arc<AuthManager>,
    base_url: String,
    upload_base_url: String,
    upload_chunk_size: usize,
}

impl DriveClient {
    /// Creates a new `DriveClient` pointed at `DRIVE_API_BASE_URL` and `DRIVE_UPLOAD_BASE_URL`.
    pub fn new(auth: Arc<AuthManager>) -> Result<Self, ApiError> {
        Self::with_base_url(auth, DRIVE_API_BASE_URL)
    }

    /// Creates a new `DriveClient` with a custom base URL (useful for local mock testing).
    pub fn with_base_url(
        auth: Arc<AuthManager>,
        base_url: impl Into<String>,
    ) -> Result<Self, ApiError> {
        let redirect_policy = reqwest::redirect::Policy::custom(|attempt| {
            // Preserve HTTP 308 Resume Incomplete responses during Google Drive Resumable Uploads
            if attempt.status() == StatusCode::PERMANENT_REDIRECT {
                attempt.stop()
            } else {
                reqwest::redirect::Policy::default().redirect(attempt)
            }
        });

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
            .redirect(redirect_policy)
            .build()?;

        let normalized_base = base_url.into().trim_end_matches('/').to_string();
        let upload_base_url = if let Some(prefix) = normalized_base.strip_suffix("/drive/v3") {
            format!("{}/upload/drive/v3", prefix)
        } else {
            format!("{}/upload", normalized_base)
        };

        Ok(Self {
            http,
            auth,
            base_url: normalized_base,
            upload_base_url,
            upload_chunk_size: RESUMABLE_UPLOAD_CHUNK_SIZE,
        })
    }

    /// Overrides the resumable upload chunk size (useful for testing multi-chunk `308 Resume Incomplete` uploads).
    pub fn with_upload_chunk_size(mut self, chunk_size: usize) -> Self {
        self.upload_chunk_size = chunk_size.max(1);
        self
    }

    /// Returns the configured upload base URL (`https://www.googleapis.com/upload/drive/v3` by default).
    pub fn upload_base_url(&self) -> &str {
        &self.upload_base_url
    }

    /// Lists all non-trashed children inside `parent_drive_id` (use `"root"` for the root folder).
    /// Transparently follows `nextPageToken` until all pages are retrieved.
    /// Google Workspace native documents (`application/vnd.google-apps.*`) are preserved with `size = 0`.
    pub async fn list_children(&self, parent_drive_id: &str) -> Result<Vec<DriveFile>, ApiError> {
        let query = format!(
            "'{}' in parents and trashed = false",
            escape_drive_query_literal(parent_drive_id)
        );
        self.list_files_with_query(&query).await
    }

    /// Lists all non-trashed files explicitly shared with the user ("Shared with me").
    /// Transparently follows `nextPageToken` until all pages are retrieved.
    pub async fn list_shared_with_me(&self) -> Result<Vec<DriveFile>, ApiError> {
        let query = "sharedWithMe = true and trashed = false";
        self.list_files_with_query(query).await
    }

    async fn list_files_with_query(&self, query: &str) -> Result<Vec<DriveFile>, ApiError> {
        let url = format!("{}/files", self.base_url);

        let mut all_files = Vec::new();
        let mut page_token: Option<String> = None;

        loop {
            let token = self.auth.get_access_token().await?;

            // Enable request compatibility for shared items/folders residing on Shared Drives.
            let mut params = vec![
                ("q", query),
                ("pageSize", LIST_PAGE_SIZE),
                ("fields", LIST_FIELDS),
                ("supportsAllDrives", "true"),
                ("includeItemsFromAllDrives", "true"),
            ];
            if let Some(ref pt) = page_token {
                params.push(("pageToken", pt.as_str()));
            }

            let response = self
                .http
                .get(&url)
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .query(&params)
                .send()
                .await?;

            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(Self::map_status_error(status, body));
            }

            let body_bytes = response.bytes().await?;
            let page: FileListResponse = serde_json::from_slice(&body_bytes)?;
            all_files.extend(page.files);

            match page.next_page_token {
                Some(next_token) if !next_token.is_empty() => {
                    page_token = Some(next_token);
                }
                _ => break,
            }
        }

        Ok(all_files)
    }

    /// Fetches metadata for a single file or folder by its Google Drive `file_id`.
    pub async fn get_file_metadata(&self, drive_id: &str) -> Result<DriveFile, ApiError> {
        let token = self.auth.get_access_token().await?;
        let url = format!("{}/files/{}", self.base_url, drive_id);

        let response = self
            .http
            .get(&url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .query(&[("fields", FILE_FIELDS), ("supportsAllDrives", "true")])
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Self::map_status_error(status, body));
        }

        let body_bytes = response.bytes().await?;
        let file: DriveFile = serde_json::from_slice(&body_bytes)?;
        Ok(file)
    }

    /// Downloads an inclusive byte range `[start, end]` from a file using `?alt=media` and `Range: bytes=start-end`.
    pub async fn download_range(
        &self,
        file_id: &str,
        start: u64,
        end: u64,
    ) -> Result<Bytes, ApiError> {
        if start > end {
            return Err(ApiError::InvalidRange { start, end });
        }

        let token = self.auth.get_access_token().await?;
        let url = format!("{}/files/{}", self.base_url, file_id);
        let range_header = format!("bytes={}-{}", start, end);

        let response = self
            .http
            .get(&url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .header(RANGE, range_header)
            .query(&[("alt", "media"), ("supportsAllDrives", "true")])
            .send()
            .await?;

        let status = response.status();
        match status {
            StatusCode::PARTIAL_CONTENT => Ok(response.bytes().await?),
            // A 200 means the server ignored the Range header and sent the whole file,
            // so the body does not start at `start` and must not be used.
            StatusCode::OK => Err(ApiError::UnexpectedStatus {
                status: status.as_u16(),
                body: format!("Server ignored Range request bytes={}-{}", start, end),
            }),
            _ => {
                let body = response.text().await.unwrap_or_default();
                Err(Self::map_status_error(status, body))
            }
        }
    }

    /// Exports a Google Workspace native document (`application/vnd.google-apps.*`) into the requested
    /// target MIME type (e.g. `application/pdf`, `application/vnd.openxmlformats-officedocument.wordprocessingml.document`).
    pub async fn export_file(
        &self,
        file_id: &str,
        export_mime_type: &str,
    ) -> Result<Bytes, ApiError> {
        let token = self.auth.get_access_token().await?;
        let url = format!("{}/files/{}/export", self.base_url, file_id);

        let response = self
            .http
            .get(&url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .query(&[("mimeType", export_mime_type)])
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Self::map_status_error(status, body));
        }

        Ok(response.bytes().await?)
    }

    /// Queries the user's Google Drive storage quota via `GET /drive/v3/about?fields=storageQuota`.
    pub async fn get_storage_quota(&self) -> Result<StorageQuota, ApiError> {
        let token = self.auth.get_access_token().await?;
        let url = format!("{}/about", self.base_url);

        let response = self
            .http
            .get(&url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .query(&[("fields", "storageQuota")])
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Self::map_status_error(status, body));
        }

        let body_bytes = response.bytes().await?;
        let about: AboutResponse = serde_json::from_slice(&body_bytes)?;
        Ok(about.storage_quota)
    }

    /// Retrieves the initial `startPageToken` for polling incremental Drive changes.
    pub async fn get_start_page_token(&self) -> Result<String, ApiError> {
        let token = self.auth.get_access_token().await?;
        let url = format!("{}/changes/startPageToken", self.base_url);

        let response = self
            .http
            .get(&url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .query(&[("supportsAllDrives", "true")])
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Self::map_status_error(status, body));
        }

        let body_bytes = response.bytes().await?;
        let parsed: StartPageTokenResponse = serde_json::from_slice(&body_bytes)?;
        Ok(parsed.start_page_token)
    }

    /// Polls incremental changes starting from `page_token` via `GET /drive/v3/changes`.
    /// Transparently follows `nextPageToken` until `newStartPageToken` is reached, returning
    /// `(all_changes, final_new_start_page_token)`.
    pub async fn list_changes(
        &self,
        page_token: &str,
    ) -> Result<(Vec<ChangeItem>, String), ApiError> {
        let url = format!("{}/changes", self.base_url);
        let mut all_changes = Vec::new();
        let mut current_token = page_token.to_string();
        let mut final_new_start_token = page_token.to_string();

        loop {
            let token = self.auth.get_access_token().await?;

            let params = [
                ("pageToken", current_token.as_str()),
                ("pageSize", LIST_PAGE_SIZE),
                ("includeItemsFromAllDrives", "true"),
                ("supportsAllDrives", "true"),
                ("fields", CHANGES_FIELDS),
            ];

            let response = self
                .http
                .get(&url)
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .query(&params)
                .send()
                .await?;

            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(Self::map_status_error(status, body));
            }

            let body_bytes = response.bytes().await?;
            let page: ChangeListResponse = serde_json::from_slice(&body_bytes)?;
            all_changes.extend(page.changes);

            if let Some(new_start) = page.new_start_page_token {
                if !new_start.is_empty() {
                    final_new_start_token = new_start;
                }
            }

            match page.next_page_token {
                Some(next_token) if !next_token.is_empty() => {
                    current_token = next_token;
                }
                _ => break,
            }
        }

        Ok((all_changes, final_new_start_token))
    }

    /// Creates a new remote folder (`mimeType = "application/vnd.google-apps.folder"`) inside `parent_drive_id`.
    pub async fn create_folder(
        &self,
        parent_drive_id: &str,
        name: &str,
    ) -> Result<DriveFile, ApiError> {
        let token = self.auth.get_access_token().await?;
        let url = format!("{}/files", self.base_url);
        let payload = serde_json::json!({
            "name": name,
            "mimeType": FOLDER_MIME_TYPE,
            "parents": [parent_drive_id],
        });

        let response = self
            .http
            .post(&url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .query(&[("fields", FILE_FIELDS), ("supportsAllDrives", "true")])
            .json(&payload)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Self::map_status_error(status, body));
        }

        let body_bytes = response.bytes().await?;
        let folder: DriveFile = serde_json::from_slice(&body_bytes)?;
        Ok(folder)
    }

    /// Safely moves a remote file or folder to Google Drive Trash (`PATCH /drive/v3/files/{fileId}` with `{"trashed": true}`)
    /// rather than permanently deleting it (`DELETE`).
    pub async fn trash_file(&self, file_id: &str) -> Result<DriveFile, ApiError> {
        let token = self.auth.get_access_token().await?;
        let url = format!("{}/files/{}", self.base_url, file_id);
        let payload = serde_json::json!({
            "trashed": true,
        });

        let response = self
            .http
            .patch(&url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .query(&[("fields", FILE_FIELDS), ("supportsAllDrives", "true")])
            .json(&payload)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Self::map_status_error(status, body));
        }

        let body_bytes = response.bytes().await?;
        let trashed: DriveFile = serde_json::from_slice(&body_bytes)?;
        Ok(trashed)
    }

    /// Updates a file or folder's metadata (`name`, `modifiedTime`, and/or parent directories via `addParents`/`removeParents`).
    pub async fn update_file_metadata(
        &self,
        file_id: &str,
        new_name: Option<&str>,
        modified_time: Option<i64>,
        add_parents: Option<&str>,
        remove_parents: Option<&str>,
    ) -> Result<DriveFile, ApiError> {
        let token = self.auth.get_access_token().await?;
        let url = format!("{}/files/{}", self.base_url, file_id);

        let mut query_params: Vec<(&str, &str)> =
            vec![("fields", FILE_FIELDS), ("supportsAllDrives", "true")];
        if let Some(add_p) = add_parents {
            if !add_p.is_empty() {
                query_params.push(("addParents", add_p));
            }
        }
        if let Some(rem_p) = remove_parents {
            if !rem_p.is_empty() {
                query_params.push(("removeParents", rem_p));
            }
        }

        let mut body_map = serde_json::Map::new();
        if let Some(name) = new_name {
            body_map.insert(
                "name".to_string(),
                serde_json::Value::String(name.to_string()),
            );
        }
        if let Some(mtime) = modified_time {
            body_map.insert(
                "modifiedTime".to_string(),
                serde_json::Value::String(unix_secs_to_rfc3339(mtime)),
            );
        }

        let response = self
            .http
            .patch(&url)
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .query(&query_params)
            .json(&serde_json::Value::Object(body_map))
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Self::map_status_error(status, body));
        }

        let body_bytes = response.bytes().await?;
        let updated: DriveFile = serde_json::from_slice(&body_bytes)?;
        Ok(updated)
    }

    /// Uploads a newly created file from `staging_path` using the Google Drive Resumable Upload API
    /// (`POST /upload/drive/v3/files?uploadType=resumable` followed by chunked `PUT` to the session `Location`).
    pub async fn resumable_upload_new_file(
        &self,
        parent_drive_id: &str,
        name: &str,
        mime_type: &str,
        modified_time: Option<i64>,
        staging_path: &Path,
    ) -> Result<DriveFile, ApiError> {
        let mut file = tokio::fs::File::open(staging_path).await?;
        let total_size = file.metadata().await?.len();

        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "name".to_string(),
            serde_json::Value::String(name.to_string()),
        );
        metadata.insert(
            "mimeType".to_string(),
            serde_json::Value::String(mime_type.to_string()),
        );
        metadata.insert(
            "parents".to_string(),
            serde_json::Value::Array(vec![serde_json::Value::String(
                parent_drive_id.to_string(),
            )]),
        );
        if let Some(mtime) = modified_time {
            metadata.insert(
                "modifiedTime".to_string(),
                serde_json::Value::String(unix_secs_to_rfc3339(mtime)),
            );
        }

        let session_uri = self
            .initiate_resumable_session(
                None,
                mime_type,
                total_size,
                &serde_json::Value::Object(metadata),
            )
            .await?;

        self.upload_file_to_session_uri(&session_uri, &mut file, total_size, mime_type)
            .await
    }

    /// Uploads modified content for an existing Drive file `file_id` from `staging_path` using the
    /// Google Drive Resumable Upload API (`PATCH /upload/drive/v3/files/{fileId}?uploadType=resumable`).
    pub async fn resumable_upload_existing_file(
        &self,
        file_id: &str,
        mime_type: &str,
        modified_time: Option<i64>,
        staging_path: &Path,
    ) -> Result<DriveFile, ApiError> {
        let mut file = tokio::fs::File::open(staging_path).await?;
        let total_size = file.metadata().await?.len();

        let mut metadata = serde_json::Map::new();
        if let Some(mtime) = modified_time {
            metadata.insert(
                "modifiedTime".to_string(),
                serde_json::Value::String(unix_secs_to_rfc3339(mtime)),
            );
        }

        let session_uri = self
            .initiate_resumable_session(
                Some(file_id),
                mime_type,
                total_size,
                &serde_json::Value::Object(metadata),
            )
            .await?;

        self.upload_file_to_session_uri(&session_uri, &mut file, total_size, mime_type)
            .await
    }

    /// Initiates a Google Drive Resumable Upload session (`POST` if `file_id` is `None`, `PATCH` if `Some`)
    /// and returns the resolved session upload URI from the `Location` response header.
    async fn initiate_resumable_session(
        &self,
        file_id: Option<&str>,
        mime_type: &str,
        total_size: u64,
        metadata_json: &serde_json::Value,
    ) -> Result<String, ApiError> {
        let token = self.auth.get_access_token().await?;
        let url = match file_id {
            Some(id) => format!("{}/files/{}", self.upload_base_url, id),
            None => format!("{}/files", self.upload_base_url),
        };

        let builder = match file_id {
            Some(_) => self.http.patch(&url),
            None => self.http.post(&url),
        };

        let response = builder
            .header(AUTHORIZATION, format!("Bearer {}", token))
            .header("X-Upload-Content-Type", mime_type)
            .header("X-Upload-Content-Length", total_size.to_string())
            .query(&[
                ("uploadType", "resumable"),
                ("fields", FILE_FIELDS),
                ("supportsAllDrives", "true"),
            ])
            .json(metadata_json)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Self::map_status_error(status, body));
        }

        let location = response
            .headers()
            .get(LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ApiError::UnexpectedStatus {
                status: status.as_u16(),
                body: "Missing Location header in resumable upload initiation response".to_string(),
            })?;

        Ok(self.resolve_session_uri(location))
    }

    fn resolve_session_uri(&self, location: &str) -> String {
        if location.starts_with("http://") || location.starts_with("https://") {
            location.to_string()
        } else if let Ok(base) = url::Url::parse(&self.upload_base_url) {
            base.join(location)
                .map(|u| u.to_string())
                .unwrap_or_else(|_| location.to_string())
        } else {
            location.to_string()
        }
    }

    async fn upload_file_to_session_uri(
        &self,
        session_uri: &str,
        file: &mut tokio::fs::File,
        total_size: u64,
        mime_type: &str,
    ) -> Result<DriveFile, ApiError> {
        if total_size == 0 {
            let token = self.auth.get_access_token().await?;
            let response = self
                .http
                .put(session_uri)
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .header(CONTENT_TYPE, mime_type)
                .header(CONTENT_LENGTH, "0")
                .body(Vec::<u8>::new())
                .send()
                .await?;

            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(Self::map_status_error(status, body));
            }
            let body_bytes = response.bytes().await?;
            let uploaded: DriveFile = serde_json::from_slice(&body_bytes)?;
            return Ok(uploaded);
        }

        let mut offset: u64 = 0;
        let chunk_cap = self.upload_chunk_size.max(1);
        let mut stalled_308_retries: u8 = 0;
        const MAX_STALLED_308_RETRIES: u8 = 3;

        while offset < total_size {
            let remaining = (total_size - offset) as usize;
            let to_read = remaining.min(chunk_cap);

            file.seek(std::io::SeekFrom::Start(offset)).await?;
            let mut buf = vec![0u8; to_read];
            file.read_exact(&mut buf).await?;

            let chunk_end = offset + (to_read as u64) - 1;
            let content_range = format!("bytes {}-{}/{}", offset, chunk_end, total_size);
            let token = self.auth.get_access_token().await?;

            let response = self
                .http
                .put(session_uri)
                .header(AUTHORIZATION, format!("Bearer {}", token))
                .header(CONTENT_TYPE, mime_type)
                .header(CONTENT_LENGTH, to_read.to_string())
                .header(CONTENT_RANGE, content_range)
                .body(buf)
                .send()
                .await?;

            let status = response.status();
            if status == StatusCode::PERMANENT_REDIRECT {
                // HTTP 308 Resume Incomplete: only advance offset from an acknowledged Range upper bound.
                // When Range is absent, the server accepted 0 bytes, so retry from the current offset.
                let acknowledged_next = response
                    .headers()
                    .get(RANGE)
                    .and_then(|v| v.to_str().ok())
                    .and_then(parse_resumable_range_upper)
                    .map(|upper| upper.saturating_add(1))
                    .filter(|&next| next <= total_size);

                if let Some(next_offset) = acknowledged_next {
                    if next_offset > offset {
                        offset = next_offset;
                        stalled_308_retries = 0;
                        continue;
                    }
                    offset = next_offset;
                }

                stalled_308_retries += 1;
                if stalled_308_retries > MAX_STALLED_308_RETRIES {
                    return Err(ApiError::UnexpectedStatus {
                        status: status.as_u16(),
                        body: "Resumable upload stalled: 308 Resume Incomplete did not advance acknowledged byte Range".to_string(),
                    });
                }
                continue;
            }

            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(Self::map_status_error(status, body));
            }

            let body_bytes = response.bytes().await?;
            let uploaded: DriveFile = serde_json::from_slice(&body_bytes)?;
            return Ok(uploaded);
        }

        Err(ApiError::UnexpectedStatus {
            status: 500,
            body: "Resumable upload loop ended without final DriveFile response".to_string(),
        })
    }

    fn map_status_error(status: StatusCode, body: String) -> ApiError {
        match status {
            StatusCode::FORBIDDEN if body.to_ascii_lowercase().contains("ratelimitexceeded") => {
                ApiError::RateLimited(body)
            }
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => ApiError::Unauthorized(body),
            StatusCode::NOT_FOUND => ApiError::NotFound(body),
            StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE => {
                ApiError::RateLimited(body)
            }
            _ => ApiError::UnexpectedStatus {
                status: status.as_u16(),
                body,
            },
        }
    }
}

/// Parses the upper inclusive byte offset from a Google Drive `308 Resume Incomplete` `Range` header
/// (e.g. `"bytes=0-262143"` -> `Some(262143)`).
fn parse_resumable_range_upper(range_header: &str) -> Option<u64> {
    let trimmed = range_header.trim();
    let without_prefix = trimmed.strip_prefix("bytes=").unwrap_or(trimmed);
    let (_, upper_str) = without_prefix.split_once('-')?;
    upper_str.trim().parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn test_deserialize_binary_file() {
        let raw_json = r#"{
            "id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
            "name": "archive.tar.gz",
            "mimeType": "application/gzip",
            "size": "1048576",
            "modifiedTime": "2025-02-20T15:04:05.000Z",
            "parents": ["root"]
        }"#;

        let file: DriveFile = serde_json::from_str(raw_json).expect("Failed to parse binary file");
        assert_eq!(file.id, "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms");
        assert_eq!(file.name, "archive.tar.gz");
        assert_eq!(file.mime_type, "application/gzip");
        assert_eq!(file.size, 1_048_576);
        assert_eq!(file.modified_time, 1_740_063_845);
        assert_eq!(file.parents, vec!["root".to_string()]);
        assert!(!file.trashed);
        assert_eq!(file.web_view_link, None);
        assert!(!file.is_dir());
        assert!(!file.is_google_native_doc());
    }

    #[test]
    fn test_deserialize_folder_without_size() {
        let raw_json = r#"{
            "id": "folder_123",
            "name": "Projects",
            "mimeType": "application/vnd.google-apps.folder",
            "modifiedTime": "2025-01-01T00:00:00Z",
            "parents": ["root"]
        }"#;

        let folder: DriveFile = serde_json::from_str(raw_json).expect("Failed to parse folder");
        assert_eq!(folder.id, "folder_123");
        assert_eq!(folder.name, "Projects");
        assert_eq!(folder.size, 0);
        assert_eq!(folder.modified_time, 1_735_689_600);
        assert!(folder.is_dir());
        assert!(!folder.is_google_native_doc());
    }

    #[test]
    fn test_deserialize_google_doc_native_file_option_b() {
        let raw_json = r#"{
            "id": "doc_456",
            "name": "Architecture Notes",
            "mimeType": "application/vnd.google-apps.document",
            "modifiedTime": "2025-03-10T12:30:00Z",
            "parents": ["folder_123"],
            "trashed": false,
            "webViewLink": "https://docs.google.com/document/d/doc_456/edit?usp=drivesdk"
        }"#;

        let doc: DriveFile = serde_json::from_str(raw_json).expect("Failed to parse Google Doc");
        assert_eq!(doc.id, "doc_456");
        assert_eq!(doc.name, "Architecture Notes");
        assert_eq!(doc.size, 0);
        assert!(!doc.trashed);
        assert_eq!(
            doc.web_view_link.as_deref(),
            Some("https://docs.google.com/document/d/doc_456/edit?usp=drivesdk")
        );
        assert!(!doc.is_dir());
        assert!(doc.is_google_native_doc());
    }

    #[test]
    fn test_drive_file_web_url_resolution() {
        let mut doc = DriveFile {
            id: "doc_123".to_string(),
            name: "Doc".to_string(),
            mime_type: "application/vnd.google-apps.document".to_string(),
            size: 0,
            modified_time: 0,
            parents: vec![],
            trashed: false,
            web_view_link: Some("https://custom.link/doc_123".to_string()),
        };
        assert_eq!(doc.web_url(), "https://custom.link/doc_123");

        doc.web_view_link = None;
        assert_eq!(
            doc.web_url(),
            "https://docs.google.com/document/d/doc_123/edit"
        );

        doc.mime_type = "application/vnd.google-apps.spreadsheet".to_string();
        assert_eq!(
            doc.web_url(),
            "https://docs.google.com/spreadsheets/d/doc_123/edit"
        );

        doc.mime_type = "application/vnd.google-apps.presentation".to_string();
        assert_eq!(
            doc.web_url(),
            "https://docs.google.com/presentation/d/doc_123/edit"
        );

        doc.mime_type = "application/vnd.google-apps.drawing".to_string();
        assert_eq!(doc.web_url(), "https://drive.google.com/open?id=doc_123");
    }

    #[test]
    fn test_deserialize_storage_quota_and_unlimited() {
        let finite_json = r#"{
            "storageQuota": {
                "limit": "16106127360",
                "usage": "5368709120",
                "usageInDrive": "4294967296",
                "usageInDriveTrash": "1073741824"
            }
        }"#;
        let about: AboutResponse =
            serde_json::from_str(finite_json).expect("Failed to parse finite storageQuota");
        assert_eq!(about.storage_quota.limit, 16_106_127_360);
        assert_eq!(about.storage_quota.usage, 5_368_709_120);
        assert_eq!(about.storage_quota.usage_in_drive, 4_294_967_296);
        assert_eq!(about.storage_quota.usage_in_drive_trash, 1_073_741_824);

        let unlimited_json = r#"{
            "storageQuota": {
                "usage": "999999"
            }
        }"#;
        let unlimited: AboutResponse =
            serde_json::from_str(unlimited_json).expect("Failed to parse unlimited storageQuota");
        assert_eq!(unlimited.storage_quota.limit, 0);
        assert_eq!(unlimited.storage_quota.usage, 999_999);
        assert_eq!(unlimited.storage_quota.usage_in_drive, 0);
        assert_eq!(unlimited.storage_quota.usage_in_drive_trash, 0);
    }

    #[test]
    fn test_deserialize_change_list_response() {
        let raw_json = r#"{
            "nextPageToken": "changes_page_2",
            "changes": [
                {
                    "fileId": "file_abc",
                    "removed": false,
                    "file": {
                        "id": "file_abc",
                        "name": "updated.txt",
                        "mimeType": "text/plain",
                        "size": "128",
                        "modifiedTime": "2025-04-01T00:00:00Z",
                        "parents": ["root"],
                        "trashed": false
                    }
                },
                {
                    "fileId": "file_deleted",
                    "removed": true
                }
            ]
        }"#;

        let resp: ChangeListResponse =
            serde_json::from_str(raw_json).expect("Failed to parse ChangeListResponse");
        assert_eq!(resp.next_page_token.as_deref(), Some("changes_page_2"));
        assert!(resp.new_start_page_token.is_none());
        assert_eq!(resp.changes.len(), 2);
        assert_eq!(resp.changes[0].file_id, "file_abc");
        assert!(!resp.changes[0].removed);
        assert_eq!(
            resp.changes[0].file.as_ref().unwrap().name,
            "updated.txt"
        );
        assert_eq!(resp.changes[1].file_id, "file_deleted");
        assert!(resp.changes[1].removed);
        assert!(resp.changes[1].file.is_none());
    }

    #[test]
    fn test_deserialize_file_list_response_with_pagination() {
        let raw_json = r#"{
            "nextPageToken": "token_page_2",
            "files": [
                {
                    "id": "f1",
                    "name": "hello.txt",
                    "mimeType": "text/plain",
                    "size": "42",
                    "modifiedTime": "2025-01-01T00:00:00Z",
                    "parents": ["root"]
                }
            ]
        }"#;

        let resp: FileListResponse =
            serde_json::from_str(raw_json).expect("Failed to parse FileListResponse");
        assert_eq!(resp.next_page_token.as_deref(), Some("token_page_2"));
        assert_eq!(resp.files.len(), 1);
        assert_eq!(resp.files[0].size, 42);
    }

    #[test]
    fn test_api_error_to_errno_mapping() {
        assert_eq!(ApiError::NotFound("id".into()).to_errno(), libc::ENOENT);
        assert_eq!(
            ApiError::Unauthorized("denied".into()).to_errno(),
            libc::EACCES
        );
        assert_eq!(
            ApiError::Auth(AuthError::TokenExpired).to_errno(),
            libc::EACCES
        );
        assert_eq!(
            ApiError::RateLimited("slow down".into()).to_errno(),
            libc::EAGAIN
        );
        assert_eq!(
            ApiError::InvalidRange { start: 10, end: 5 }.to_errno(),
            libc::EINVAL
        );
        assert_eq!(
            ApiError::UnexpectedStatus {
                status: 500,
                body: "internal".into()
            }
            .to_errno(),
            libc::EIO
        );
    }

    #[tokio::test]
    async fn test_list_children_pagination_and_download_range_mock_server() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind mock server");
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            // Request 1: First page of list_children
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str.contains("GET /drive/v3/files?"));
                assert!(req_str.contains("supportsAllDrives=true"));
                assert!(req_str.contains("includeItemsFromAllDrives=true"));
                assert!(req_str.contains("Bearer test-access-token"));

                let body = r#"{
                    "nextPageToken": "page_2_tok",
                    "files": [
                        {
                            "id": "file_1",
                            "name": "first.bin",
                            "mimeType": "application/octet-stream",
                            "size": "100",
                            "modifiedTime": "2025-01-01T00:00:00Z",
                            "parents": ["root"]
                        }
                    ]
                }"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }

            // Request 2: Second page of list_children
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str.contains("pageToken=page_2_tok"));

                let body = r#"{
                    "files": [
                        {
                            "id": "doc_2",
                            "name": "Notes",
                            "mimeType": "application/vnd.google-apps.document",
                            "modifiedTime": "2025-01-02T00:00:00Z",
                            "parents": ["root"]
                        }
                    ]
                }"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }

            // Request 3: Byte-range download
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str.contains("GET /drive/v3/files/file_1?alt=media"));
                assert!(req_str.contains("supportsAllDrives=true"));
                assert!(
                    req_str.contains("Range: bytes=4-9") || req_str.contains("range: bytes=4-9")
                );

                let payload = b"456789";
                let resp = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    payload.len()
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.write_all(payload).await;
            }
        });

        let auth = Arc::new(AuthManager::new_static_for_test("test-access-token"));
        let base_url = format!("http://{}/drive/v3", addr);
        let client = DriveClient::with_base_url(auth, base_url).expect("Client init failed");

        let files = client
            .list_children("root")
            .await
            .expect("list_children failed");
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].id, "file_1");
        assert_eq!(files[0].size, 100);
        assert_eq!(files[1].id, "doc_2");
        assert!(files[1].is_google_native_doc());
        assert_eq!(files[1].size, 0);

        let chunk = client
            .download_range("file_1", 4, 9)
            .await
            .expect("download_range failed");
        assert_eq!(&chunk[..], b"456789");

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_phase2_endpoints_mock_server() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind mock server");
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            // Request 1: get_storage_quota
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str.contains("GET /drive/v3/about?fields=storageQuota"));

                let body = r#"{
                    "storageQuota": {
                        "limit": "10737418240",
                        "usage": "2147483648",
                        "usageInDrive": "2000000000",
                        "usageInDriveTrash": "147483648"
                    }
                }"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }

            // Request 2: get_start_page_token
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(
                    req_str.contains("GET /drive/v3/changes/startPageToken?supportsAllDrives=true")
                );

                let body = r#"{"startPageToken": "start_tok_100"}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }

            // Request 3: list_changes (page 1)
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str.contains("GET /drive/v3/changes?"));
                assert!(req_str.contains("pageToken=start_tok_100"));

                let body = r#"{
                    "nextPageToken": "changes_p2",
                    "changes": [
                        {
                            "fileId": "f_changed",
                            "removed": false,
                            "file": {
                                "id": "f_changed",
                                "name": "report.pdf",
                                "mimeType": "application/pdf",
                                "size": "2048",
                                "modifiedTime": "2025-05-01T00:00:00Z",
                                "parents": ["root"],
                                "trashed": false
                            }
                        }
                    ]
                }"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }

            // Request 4: list_changes (page 2 with newStartPageToken)
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str.contains("pageToken=changes_p2"));

                let body = r#"{
                    "newStartPageToken": "start_tok_101",
                    "changes": [
                        {
                            "fileId": "f_removed",
                            "removed": true
                        }
                    ]
                }"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }

            // Request 5: list_shared_with_me
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str.contains("sharedWithMe"));

                let body = r#"{
                    "files": [
                        {
                            "id": "shared_1",
                            "name": "Team Roadmap",
                            "mimeType": "application/vnd.google-apps.document",
                            "modifiedTime": "2025-05-02T00:00:00Z",
                            "trashed": false,
                            "webViewLink": "https://docs.google.com/document/d/shared_1/edit"
                        }
                    ]
                }"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }

            // Request 6: export_file
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str
                    .contains("GET /drive/v3/files/shared_1/export?mimeType=application%2Fpdf"));

                let payload = b"%PDF-1.7 mock exported content";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/pdf\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    payload.len()
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.write_all(payload).await;
            }
        });

        let auth = Arc::new(AuthManager::new_static_for_test("test-access-token"));
        let base_url = format!("http://{}/drive/v3", addr);
        let client = DriveClient::with_base_url(auth, base_url).expect("Client init failed");

        let quota = client
            .get_storage_quota()
            .await
            .expect("get_storage_quota failed");
        assert_eq!(quota.limit, 10_737_418_240);
        assert_eq!(quota.usage, 2_147_483_648);

        let start_tok = client
            .get_start_page_token()
            .await
            .expect("get_start_page_token failed");
        assert_eq!(start_tok, "start_tok_100");

        let (changes, new_tok) = client
            .list_changes(&start_tok)
            .await
            .expect("list_changes failed");
        assert_eq!(new_tok, "start_tok_101");
        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].file_id, "f_changed");
        assert!(!changes[0].removed);
        assert_eq!(changes[1].file_id, "f_removed");
        assert!(changes[1].removed);

        let shared = client
            .list_shared_with_me()
            .await
            .expect("list_shared_with_me failed");
        assert_eq!(shared.len(), 1);
        assert_eq!(shared[0].id, "shared_1");
        assert_eq!(
            shared[0].web_url(),
            "https://docs.google.com/document/d/shared_1/edit"
        );

        let exported = client
            .export_file("shared_1", "application/pdf")
            .await
            .expect("export_file failed");
        assert_eq!(&exported[..], b"%PDF-1.7 mock exported content");

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_phase3_create_folder_trash_rename_and_resumable_upload_mock_server() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind mock server");
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            // Request 1: create_folder (POST /drive/v3/files)
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str.starts_with("POST /drive/v3/files?"));
                assert!(req_str.contains("application/vnd.google-apps.folder"));
                assert!(req_str.contains("\"NewDocs\""));

                let body = r#"{
                    "id": "folder_new_1",
                    "name": "NewDocs",
                    "mimeType": "application/vnd.google-apps.folder",
                    "modifiedTime": "2025-06-01T00:00:00Z",
                    "parents": ["root"]
                }"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }

            // Request 2: resumable_upload_new_file initiation (POST /upload/drive/v3/files?uploadType=resumable)
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str.starts_with("POST /upload/drive/v3/files?uploadType=resumable"));
                assert!(req_str.to_ascii_lowercase().contains("x-upload-content-length: 12"));

                let session_url = format!(
                    "http://{}/upload/drive/v3/files?uploadType=resumable&upload_id=sess_1",
                    addr
                );
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nLocation: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    session_url
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }

            // Request 3a: resumable_upload_new_file chunk 1 attempt 1 (PUT bytes 0-7/12 -> 308 without Range header)
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str.starts_with("PUT /upload/drive/v3/files?uploadType=resumable&upload_id=sess_1"));
                assert!(req_str.to_ascii_lowercase().contains("content-range: bytes 0-7/12"));
                assert!(req_str.ends_with("Hello, G"));

                let resp = "HTTP/1.1 308 Permanent Redirect\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                let _ = stream.write_all(resp.as_bytes()).await;
            }

            // Request 3b: resumable_upload_new_file chunk 1 retry (PUT bytes 0-7/12 -> 308 with Range: bytes=0-7)
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str.starts_with("PUT /upload/drive/v3/files?uploadType=resumable&upload_id=sess_1"));
                assert!(req_str.to_ascii_lowercase().contains("content-range: bytes 0-7/12"));
                assert!(req_str.ends_with("Hello, G"));

                let resp = "HTTP/1.1 308 Permanent Redirect\r\nRange: bytes=0-7\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                let _ = stream.write_all(resp.as_bytes()).await;
            }

            // Request 4: resumable_upload_new_file chunk 2 (PUT bytes 8-11/12 -> 200 OK with DriveFile)
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str.to_ascii_lowercase().contains("content-range: bytes 8-11/12"));
                assert!(req_str.ends_with("oogl"));

                let body = r#"{
                    "id": "uploaded_file_1",
                    "name": "hello.txt",
                    "mimeType": "text/plain",
                    "size": "12",
                    "modifiedTime": "2025-06-01T12:00:00Z",
                    "parents": ["folder_new_1"]
                }"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }

            // Request 5: update_file_metadata (PATCH /drive/v3/files/uploaded_file_1 with addParents/removeParents)
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str.starts_with("PATCH /drive/v3/files/uploaded_file_1?"));
                assert!(req_str.contains("addParents=root"));
                assert!(req_str.contains("removeParents=folder_new_1"));
                assert!(req_str.contains("\"hello_moved.txt\""));

                let body = r#"{
                    "id": "uploaded_file_1",
                    "name": "hello_moved.txt",
                    "mimeType": "text/plain",
                    "size": "12",
                    "modifiedTime": "2025-06-01T12:05:00Z",
                    "parents": ["root"]
                }"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }

            // Request 6: trash_file (PATCH /drive/v3/files/uploaded_file_1 with {"trashed": true})
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(req_str.starts_with("PATCH /drive/v3/files/uploaded_file_1?"));
                assert!(req_str.contains("\"trashed\":true"));

                let body = r#"{
                    "id": "uploaded_file_1",
                    "name": "hello_moved.txt",
                    "mimeType": "text/plain",
                    "size": "12",
                    "modifiedTime": "2025-06-01T12:10:00Z",
                    "parents": ["root"],
                    "trashed": true
                }"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }

            // Request 7 & 8: 0-byte resumable upload initiation + PUT finalization
            for empty_step in 0..2 {
                if let Ok((mut stream, _)) = listener.accept().await {
                    let mut buf = vec![0u8; 4096];
                    let n = stream.read(&mut buf).await.unwrap_or(0);
                    let req_str = String::from_utf8_lossy(&buf[..n]);
                    if empty_step == 0 {
                        assert!(req_str.starts_with("POST /upload/drive/v3/files?"));
                        assert!(
                            req_str.contains("X-Upload-Content-Length: 0")
                                || req_str.contains("x-upload-content-length: 0")
                        );
                        let session_url = format!("http://{}/upload/drive/v3/session/empty_1", addr);
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nLocation: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                            session_url
                        );
                        let _ = stream.write_all(resp.as_bytes()).await;
                    } else {
                        assert!(req_str.starts_with("PUT /upload/drive/v3/session/empty_1"));
                        assert!(
                            req_str.contains("Content-Length: 0")
                                || req_str.contains("content-length: 0")
                        );
                        let body = r#"{
                            "id": "uploaded_empty_1",
                            "name": "empty.txt",
                            "mimeType": "text/plain",
                            "size": "0",
                            "modifiedTime": "2025-06-01T12:15:00Z",
                            "parents": ["root"],
                            "trashed": false
                        }"#;
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = stream.write_all(resp.as_bytes()).await;
                    }
                }
            }
        });

        let auth = Arc::new(AuthManager::new_static_for_test("test-access-token"));
        let base_url = format!("http://{}/drive/v3", addr);
        let client = DriveClient::with_base_url(auth, base_url)
            .expect("Client init failed")
            .with_upload_chunk_size(8);

        let folder = client
            .create_folder("root", "NewDocs")
            .await
            .expect("create_folder failed");
        assert_eq!(folder.id, "folder_new_1");
        assert!(folder.is_dir());

        let stage_path = std::env::temp_dir().join(format!(
            "gdrive_fuse_api_test_{}.stage",
            std::process::id()
        ));
        std::fs::write(&stage_path, b"Hello, Googl").unwrap();

        let uploaded = client
            .resumable_upload_new_file(
                "folder_new_1",
                "hello.txt",
                guess_mime_type("hello.txt"),
                Some(1_748_779_200),
                &stage_path,
            )
            .await
            .expect("resumable_upload_new_file failed");
        let _ = std::fs::remove_file(&stage_path);
        assert_eq!(uploaded.id, "uploaded_file_1");
        assert_eq!(uploaded.size, 12);

        let moved = client
            .update_file_metadata(
                "uploaded_file_1",
                Some("hello_moved.txt"),
                None,
                Some("root"),
                Some("folder_new_1"),
            )
            .await
            .expect("update_file_metadata failed");
        assert_eq!(moved.name, "hello_moved.txt");
        assert_eq!(moved.parents, vec!["root".to_string()]);

        let trashed = client
            .trash_file("uploaded_file_1")
            .await
            .expect("trash_file failed");
        assert!(trashed.trashed);

        // Verify 0-byte resumable upload
        std::fs::write(&stage_path, b"").unwrap();
        let empty_uploaded = client
            .resumable_upload_new_file(
                "root",
                "empty.txt",
                guess_mime_type("empty.txt"),
                None,
                &stage_path,
            )
            .await
            .expect("0-byte resumable upload failed");
        let _ = std::fs::remove_file(&stage_path);
        assert_eq!(empty_uploaded.id, "uploaded_empty_1");
        assert_eq!(empty_uploaded.size, 0);

        server_handle.await.unwrap();
    }
}
