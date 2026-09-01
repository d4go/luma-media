use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::{Url, header::HeaderMap};
use serde::{Deserialize, Serialize};

use super::classifier::{PageKind, classify_transport};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchDecision {
    /// Return the current result to the caller unchanged.
    Return,
    /// Do not fall back; the caller should retry/backoff (network, timeout, plain 5xx).
    Retry,
    /// The HTTP result is unusable (blocked/challenge/invalid); retry through the browser.
    FallbackToBrowser,
    /// The provider is rate limited; the caller should cool down, never fall back.
    Cooldown,
    /// The browser path needs human interaction; do not retry automatically.
    #[allow(dead_code)] // consumed by the circuit-breaker integration (master plan Phase 2)
    InteractionRequired,
}

#[derive(Debug, Clone)]
pub struct AutoFetchPolicy {
    /// How long a provider sticks to Browser after an HTTP failure + browser success.
    pub sticky_ttl: Duration,
}

impl Default for AutoFetchPolicy {
    fn default() -> Self {
        Self {
            sticky_ttl: Duration::from_secs(10 * 60),
        }
    }
}

impl AutoFetchPolicy {
    /// Decide what an Auto fetch should do based on the HTTP attempt.
    ///
    /// `looks_like_challenge` lets the transport layer recognise access/challenge
    /// pages that are served with a 2xx/5xx status without depending on a specific
    /// provider parser.
    pub fn decide(
        &self,
        http_result: Result<&FetchResponse, &FetchError>,
        looks_like_challenge: impl Fn(&str) -> bool,
    ) -> FetchDecision {
        match http_result {
            Ok(response) => match classify_transport(response) {
                Some(PageKind::RateLimited) => FetchDecision::Cooldown,
                Some(PageKind::TemporaryUnavailable) => {
                    if looks_like_challenge(&response.body) {
                        FetchDecision::FallbackToBrowser
                    } else {
                        FetchDecision::Retry
                    }
                }
                Some(PageKind::AccessDenied)
                | Some(PageKind::LoginRequired)
                | Some(PageKind::AgeGate)
                | Some(PageKind::InteractionRequired)
                | Some(PageKind::InvalidContent) => FetchDecision::FallbackToBrowser,
                Some(PageKind::ValidContent) | None => {
                    // A 2xx/3xx body can still be a challenge or access-denied page.
                    if looks_like_challenge(&response.body) {
                        FetchDecision::FallbackToBrowser
                    } else {
                        FetchDecision::Return
                    }
                }
            },
            Err(error) => match error.kind {
                FetchFailureKind::AccessDenied
                | FetchFailureKind::InvalidContent
                | FetchFailureKind::SessionExpired
                | FetchFailureKind::InteractionRequired => FetchDecision::FallbackToBrowser,
                FetchFailureKind::RateLimited => FetchDecision::Cooldown,
                FetchFailureKind::Network
                | FetchFailureKind::Timeout
                | FetchFailureKind::TemporaryUnavailable => FetchDecision::Retry,
                FetchFailureKind::BrowserUnavailable | FetchFailureKind::Unknown => {
                    FetchDecision::Return
                }
            },
        }
    }
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
