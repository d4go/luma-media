use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;

use crate::fetch::{FetchMode, FetchTransport};

#[derive(Debug, Clone)]
pub struct SourceProviderConfig {
    pub key: String,
    pub display_name: String,
    pub base_url: String,
    pub secret: String,
    pub adapter: String,
    pub proxy_url: String,
    pub user_agent: String,
    pub fetch_mode: FetchMode,
    pub sync_enabled: bool,
    pub sync_interval_minutes: i64,
    pub sync_overlap_days: i64,
    pub sync_detail_limit: usize,
    pub resource_cache_ttl_hours: i64,
    pub resource_hydration_recent_days: i64,
}

impl SourceProviderConfig {
    pub fn from_row(row: &sqlx::sqlite::SqliteRow) -> Self {
        let config = serde_json::from_str::<Value>(&row.get::<String, _>("config_json"))
            .unwrap_or_else(|_| json!({}));
        let sync = config.get("sync").and_then(Value::as_object);
        Self {
            key: row.get("provider_key"),
            display_name: row.get("display_name"),
            base_url: row.get("base_url"),
            secret: row.get("secret"),
            adapter: config
                .get("adapter")
                .and_then(Value::as_str)
                .unwrap_or("javbus")
                .to_owned(),
            proxy_url: config
                .get("proxyUrl")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_owned(),
            user_agent: config
                .get("userAgent")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_owned(),
            fetch_mode: FetchMode::parse(config.get("fetchMode").and_then(Value::as_str)),
            sync_enabled: sync
                .and_then(|value| value.get("enabled"))
                .and_then(Value::as_bool)
                .or_else(|| config.get("syncEnabled").and_then(Value::as_bool))
                .unwrap_or(true),
            sync_interval_minutes: sync
                .and_then(|value| value.get("intervalMinutes"))
                .and_then(Value::as_i64)
                .or_else(|| config.get("syncIntervalMinutes").and_then(Value::as_i64))
                .unwrap_or(1440)
                .clamp(60, 10080),
            sync_overlap_days: sync
                .and_then(|value| value.get("overlapDays"))
                .and_then(Value::as_i64)
                .or_else(|| config.get("syncOverlapDays").and_then(Value::as_i64))
                .unwrap_or(3)
                .clamp(1, 30),
            sync_detail_limit: config
                .get("syncDetailLimit")
                .and_then(Value::as_u64)
                .unwrap_or(8)
                .min(40) as usize,
            resource_cache_ttl_hours: config
                .get("resourceCacheTtlHours")
                .and_then(Value::as_i64)
                .unwrap_or(72)
                .clamp(1, 24 * 30),
            resource_hydration_recent_days: config
                .get("resourceHydrationRecentDays")
                .and_then(Value::as_i64)
                .unwrap_or(180)
                .clamp(1, 3650),
        }
    }

    pub fn transport(&self) -> FetchTransport {
        FetchTransport {
            proxy_url: (!self.proxy_url.is_empty()).then(|| self.proxy_url.clone()),
            user_agent: (!self.user_agent.is_empty()).then(|| self.user_agent.clone()),
            cookie: (!self.secret.trim().is_empty()).then(|| self.secret.trim().to_owned()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceMedia {
    pub provider_id: String,
    pub code: String,
    pub title: String,
    pub poster_url: Option<String>,
    pub source_url: String,
    pub release_date: Option<String>,
}

#[derive(Clone)]
pub struct ProviderContext {
    pub state: crate::AppState,
    pub provider: SourceProviderConfig,
    pub media_id: i64,
    pub raw_document: Option<RawProviderDocument>,
    pub allow_resource_endpoint: bool,
}

#[derive(Debug, Clone, Default)]
pub struct DiscoverRequest {
    pub cursor: Value,
}

#[derive(Debug, Clone, Default)]
pub struct DiscoverPage {
    pub items: Vec<ProviderMediaCandidate>,
    pub next_cursor: Option<Value>,
}

#[derive(Debug, Clone, Default)]
pub struct ProviderMediaCandidate {
    pub provider_id: String,
    pub code: Option<String>,
    pub source_url: String,
}

#[derive(Debug, Clone, Default)]
pub struct ProviderMediaRef {
    pub provider_id: String,
    pub source_url: String,
}

#[derive(Debug, Clone)]
pub struct RawProviderDocument {
    pub source_url: String,
    pub body: String,
}

#[derive(Debug, Clone, Default)]
pub struct ResourceCandidate {
    pub provider_resource_id: Option<String>,
    pub download_url: String,
    pub title: String,
    pub info_hash: Option<String>,
    pub size_bytes: Option<i64>,
    pub resolution: Option<String>,
    pub subtitle_languages: Vec<String>,
    pub trackers: Vec<String>,
    pub published_at: Option<String>,
    pub codec: Option<String>,
    pub source_url: String,
    pub raw_json: Value,
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    #[error("fetch failed: {0}")]
    Fetch(String),
    #[error("provider returned invalid content: {0}")]
    InvalidContent(String),
    #[error("provider capability is not implemented: {0}")]
    Unsupported(String),
}
