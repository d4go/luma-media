use serde::Serialize;
use sqlx::{Row, SqlitePool};

use crate::fetch::{FetchFailureKind, FetchMode, PageKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRuntimeState {
    Ready,
    Degraded,
    Cooldown,
    InteractionRequired,
    Unavailable,
}

impl ProviderRuntimeState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Degraded => "degraded",
            Self::Cooldown => "cooldown",
            Self::InteractionRequired => "interaction_required",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderRuntimeView {
    pub provider_key: String,
    pub state: ProviderRuntimeState,
    pub active_fetch_mode: FetchMode,
    pub last_success_at: Option<String>,
    pub last_failure_at: Option<String>,
    pub last_failure_kind: Option<String>,
    pub last_failure_message: Option<String>,
    pub failure_count: i64,
    pub cooldown_until: Option<String>,
    pub browser_enabled: bool,
    pub browser_executable: String,
    pub browser_profile_path: String,
}

pub async fn ensure(pool: &SqlitePool, provider_key: &str, mode: FetchMode) -> sqlx::Result<()> {
    sqlx::query("INSERT INTO provider_runtime_state(provider_key,active_fetch_mode) VALUES (?,?) ON CONFLICT(provider_key) DO UPDATE SET active_fetch_mode=excluded.active_fetch_mode,updated_at=datetime('now')")
        .bind(provider_key)
        .bind(mode_name(mode))
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn record_success(
    pool: &SqlitePool,
    provider_key: &str,
    mode: FetchMode,
) -> sqlx::Result<()> {
    ensure(pool, provider_key, mode).await?;
    sqlx::query("UPDATE provider_runtime_state SET runtime_state='ready',active_fetch_mode=?,last_success_at=datetime('now'),last_failure_kind=NULL,last_failure_message=NULL,failure_count=0,cooldown_until=NULL,updated_at=datetime('now') WHERE provider_key=?")
        .bind(mode_name(mode))
        .bind(provider_key)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn record_page_failure(
    pool: &SqlitePool,
    provider_key: &str,
    mode: FetchMode,
    kind: PageKind,
    message: &str,
) -> sqlx::Result<()> {
    let (state, cooldown) = match kind {
        PageKind::AgeGate | PageKind::LoginRequired | PageKind::InteractionRequired => {
            (ProviderRuntimeState::InteractionRequired, None)
        }
        PageKind::RateLimited => (ProviderRuntimeState::Cooldown, Some("+1 hour")),
        PageKind::TemporaryUnavailable => (ProviderRuntimeState::Cooldown, Some("+30 minutes")),
        PageKind::AccessDenied | PageKind::InvalidContent => (ProviderRuntimeState::Degraded, None),
        PageKind::ValidContent => return record_success(pool, provider_key, mode).await,
    };
    record_failure(
        pool,
        provider_key,
        mode,
        state,
        page_kind_name(kind),
        message,
        cooldown,
    )
    .await
}

pub async fn record_fetch_failure(
    pool: &SqlitePool,
    provider_key: &str,
    mode: FetchMode,
    kind: FetchFailureKind,
    message: &str,
) -> sqlx::Result<()> {
    let (state, cooldown) = match kind {
        FetchFailureKind::RateLimited => (ProviderRuntimeState::Cooldown, Some("+1 hour")),
        FetchFailureKind::TemporaryUnavailable => {
            (ProviderRuntimeState::Cooldown, Some("+30 minutes"))
        }
        FetchFailureKind::InteractionRequired | FetchFailureKind::SessionExpired => {
            (ProviderRuntimeState::InteractionRequired, None)
        }
        FetchFailureKind::BrowserUnavailable => (ProviderRuntimeState::Unavailable, None),
        FetchFailureKind::Network | FetchFailureKind::Timeout => {
            (ProviderRuntimeState::Degraded, None)
        }
        FetchFailureKind::AccessDenied
        | FetchFailureKind::InvalidContent
        | FetchFailureKind::Unknown => (ProviderRuntimeState::Degraded, None),
    };
    record_failure(
        pool,
        provider_key,
        mode,
        state,
        failure_kind_name(kind),
        message,
        cooldown,
    )
    .await
}

async fn record_failure(
    pool: &SqlitePool,
    provider_key: &str,
    mode: FetchMode,
    state: ProviderRuntimeState,
    kind: &str,
    message: &str,
    cooldown: Option<&str>,
) -> sqlx::Result<()> {
    ensure(pool, provider_key, mode).await?;
    let message = message.chars().take(1000).collect::<String>();
    sqlx::query("UPDATE provider_runtime_state SET runtime_state=?,active_fetch_mode=?,last_failure_at=datetime('now'),last_failure_kind=?,last_failure_message=?,failure_count=failure_count+1,cooldown_until=CASE WHEN ? IS NULL THEN NULL ELSE datetime('now',?) END,updated_at=datetime('now') WHERE provider_key=?")
        .bind(state.as_str())
        .bind(mode_name(mode))
        .bind(kind)
        .bind(message)
        .bind(cooldown)
        .bind(cooldown)
        .bind(provider_key)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn load(
    pool: &SqlitePool,
    provider_key: &str,
    browser_enabled: bool,
    browser_executable: &str,
    profile_path: &str,
) -> sqlx::Result<Option<ProviderRuntimeView>> {
    let row = sqlx::query("SELECT * FROM provider_runtime_state WHERE provider_key=?")
        .bind(provider_key)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|row| ProviderRuntimeView {
        provider_key: row.get("provider_key"),
        state: parse_state(&row.get::<String, _>("runtime_state")),
        active_fetch_mode: FetchMode::parse(Some(&row.get::<String, _>("active_fetch_mode"))),
        last_success_at: row.get("last_success_at"),
        last_failure_at: row.get("last_failure_at"),
        last_failure_kind: row.get("last_failure_kind"),
        last_failure_message: row.get("last_failure_message"),
        failure_count: row.get("failure_count"),
        cooldown_until: row.get("cooldown_until"),
        browser_enabled,
        browser_executable: browser_executable.to_owned(),
        browser_profile_path: profile_path.to_owned(),
    }))
}

fn parse_state(value: &str) -> ProviderRuntimeState {
    match value {
        "ready" => ProviderRuntimeState::Ready,
        "degraded" => ProviderRuntimeState::Degraded,
        "cooldown" => ProviderRuntimeState::Cooldown,
        "interaction_required" => ProviderRuntimeState::InteractionRequired,
        _ => ProviderRuntimeState::Unavailable,
    }
}

fn mode_name(mode: FetchMode) -> &'static str {
    match mode {
        FetchMode::Http => "http",
        FetchMode::Browser => "browser",
        FetchMode::Auto => "auto",
    }
}

fn page_kind_name(kind: PageKind) -> &'static str {
    match kind {
        PageKind::ValidContent => "valid_content",
        PageKind::AgeGate => "age_gate",
        PageKind::LoginRequired => "login_required",
        PageKind::InteractionRequired => "interaction_required",
        PageKind::AccessDenied => "access_denied",
        PageKind::RateLimited => "rate_limited",
        PageKind::TemporaryUnavailable => "temporary_unavailable",
        PageKind::InvalidContent => "invalid_content",
    }
}

fn failure_kind_name(kind: FetchFailureKind) -> &'static str {
    match kind {
        FetchFailureKind::Network => "network",
        FetchFailureKind::Timeout => "timeout",
        FetchFailureKind::RateLimited => "rate_limited",
        FetchFailureKind::TemporaryUnavailable => "temporary_unavailable",
        FetchFailureKind::SessionExpired => "session_expired",
        FetchFailureKind::InteractionRequired => "interaction_required",
        FetchFailureKind::AccessDenied => "access_denied",
        FetchFailureKind::InvalidContent => "invalid_content",
        FetchFailureKind::BrowserUnavailable => "browser_unavailable",
        FetchFailureKind::Unknown => "unknown",
    }
}
