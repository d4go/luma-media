use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::{Url, header::HeaderMap};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchMode {
    Http,
    Browser,
    Auto,
}

impl FetchMode {
    pub fn parse(value: Option<&str>) -> Self {
        match value.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            Some("browser") => Self::Browser,
            Some("auto") => Self::Auto,
            _ => Self::Http,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchMethod {
    Get,
    Post,
}

#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub provider_key: String,
    pub url: Url,
    pub method: FetchMethod,
    pub headers: HeaderMap,
    pub referer: Option<Url>,
    pub body: Option<String>,
    pub timeout: Duration,
}

impl FetchRequest {
    pub fn get(provider_key: impl Into<String>, url: Url) -> Self {
        Self {
            provider_key: provider_key.into(),
            url,
            method: FetchMethod::Get,
            headers: HeaderMap::new(),
            referer: None,
            body: None,
            timeout: Duration::from_secs(20),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FetchTransport {
    pub proxy_url: Option<String>,
    pub user_agent: Option<String>,
    pub cookie: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FetchResponse {
    pub final_url: Url,
    pub status: Option<u16>,
    pub content_type: Option<String>,
    pub headers: HeaderMap,
    pub body: String,
    pub fetched_at: DateTime<Utc>,
    pub fetch_mode: FetchMode,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FetchFailureKind {
    Network,
    Timeout,
    RateLimited,
    TemporaryUnavailable,
    SessionExpired,
    InteractionRequired,
    AccessDenied,
    InvalidContent,
    BrowserUnavailable,
    Unknown,
}

impl FetchFailureKind {
    pub fn retryable(self) -> bool {
        matches!(
            self,
            Self::Network | Self::Timeout | Self::TemporaryUnavailable
        )
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{provider_key} fetch failed ({kind:?}): {message}")]
pub struct FetchError {
    pub provider_key: String,
    pub kind: FetchFailureKind,
    pub message: String,
    pub status: Option<u16>,
}

impl FetchError {
    pub fn new(
        provider_key: impl Into<String>,
        kind: FetchFailureKind,
        message: impl Into<String>,
        status: Option<u16>,
    ) -> Self {
        Self {
            provider_key: provider_key.into(),
            kind,
            message: message.into(),
            status,
        }
    }
}
