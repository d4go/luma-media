use axum::{
    Json, Router,
    extract::{Path, State},
    routing::{get, post},
};
use serde::Serialize;

use crate::{
    AppState,
    error::{AppError, AppResult},
    fetch::{
        BrowserSessionView, FetchError, FetchFailureKind, FetchMethod, FetchMode, FetchRequest,
        FetchResponse, PageKind,
    },
};

use super::{SourceProviderConfig, runtime};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/providers/{key}/runtime", get(get_runtime))
        .route("/providers/{key}/diagnose", post(diagnose))
        .route(
            "/providers/{key}/browser-session",
            post(start_browser_session),
        )
        .route(
            "/providers/{key}/browser-session/{session_id}",
            get(get_browser_session).delete(cancel_browser_session),
        )
        .route(
            "/providers/{key}/browser-session/{session_id}/complete",
            post(complete_browser_session),
        )
        .route(
            "/providers/{key}/browser-profile",
            axum::routing::delete(clear_browser_profile),
        )
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnoseAttempt {
    attempted: bool,
    success: bool,
    status: Option<u16>,
    page_kind: Option<PageKind>,
    final_url: Option<String>,
    elapsed_ms: Option<u64>,
    error: Option<String>,
    #[serde(skip)]
    failure_kind: Option<FetchFailureKind>,
}

impl DiagnoseAttempt {
    fn skipped() -> Self {
        Self {
            attempted: false,
            success: false,
            status: None,
            page_kind: None,
            final_url: None,
            elapsed_ms: None,
            error: None,
            failure_kind: None,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DiagnoseResponse {
    provider: String,
    http: DiagnoseAttempt,
    browser: DiagnoseAttempt,
    runtime: runtime::ProviderRuntimeView,
}

async fn get_runtime(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> AppResult<Json<runtime::ProviderRuntimeView>> {
    let provider = load_source_config(&state, &key)
        .await?
        .ok_or(AppError::NotFound)?;
    runtime::ensure(&state.pool, &key, provider.fetch_mode).await?;
    Ok(Json(runtime_view(&state, &provider).await?))
}

async fn diagnose(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> AppResult<Json<DiagnoseResponse>> {
    let provider = load_source_config(&state, &key)
        .await?
        .ok_or(AppError::NotFound)?;
    let url = reqwest::Url::parse(&provider.base_url)
        .map_err(|error| AppError::BadRequest(error.to_string()))?;
    let http = if matches!(provider.fetch_mode, FetchMode::Http | FetchMode::Auto) {
        diagnose_mode(&state, &provider, FetchMode::Http, url.clone()).await
    } else {
        DiagnoseAttempt::skipped()
    };
    let browser = if matches!(provider.fetch_mode, FetchMode::Browser | FetchMode::Auto) {
        diagnose_mode(&state, &provider, FetchMode::Browser, url).await
    } else {
        DiagnoseAttempt::skipped()
    };
    let selected = if browser.attempted { &browser } else { &http };
    if selected.success {
        runtime::record_success(&state.pool, &key, provider.fetch_mode).await?;
    } else if let Some(kind) = selected.failure_kind {
        runtime::record_fetch_failure(
            &state.pool,
            &key,
            provider.fetch_mode,
            kind,
            selected.error.as_deref().unwrap_or("provider fetch failed"),
        )
        .await?;
    } else if let Some(kind) = selected.page_kind {
        runtime::record_page_failure(
            &state.pool,
            &key,
            provider.fetch_mode,
            kind,
            selected
                .error
                .as_deref()
                .unwrap_or("provider page was not valid"),
        )
        .await?;
    }
    let runtime = runtime_view(&state, &provider).await?;
    Ok(Json(DiagnoseResponse {
        provider: key,
        http,
        browser,
        runtime,
    }))
}

async fn start_browser_session(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> AppResult<Json<BrowserSessionView>> {
    let provider = load_source_config(&state, &key)
        .await?
        .ok_or(AppError::NotFound)?;
    let url = reqwest::Url::parse(&provider.base_url)
        .map_err(|error| AppError::BadRequest(error.to_string()))?;
    let session = state
        .fetch_manager
        .browser()
        .start_interactive(&key, &url)
        .await
        .map_err(fetch_bad_request)?;
    Ok(Json(session))
}

async fn get_browser_session(
    State(state): State<AppState>,
    Path((key, session_id)): Path<(String, String)>,
) -> AppResult<Json<BrowserSessionView>> {
    load_source_config(&state, &key)
        .await?
        .ok_or(AppError::NotFound)?;
    let session = state
        .fetch_manager
        .browser()
        .interactive_status(&key, &session_id)
        .await
        .map_err(fetch_bad_request)?;
    Ok(Json(session))
}

async fn complete_browser_session(
    State(state): State<AppState>,
    Path((key, session_id)): Path<(String, String)>,
) -> AppResult<Json<BrowserSessionView>> {
    load_source_config(&state, &key)
        .await?
        .ok_or(AppError::NotFound)?;
    let session = state
        .fetch_manager
        .browser()
        .complete_interactive(&key, &session_id)
        .await
        .map_err(fetch_bad_request)?;
    Ok(Json(session))
}

async fn cancel_browser_session(
    State(state): State<AppState>,
    Path((key, session_id)): Path<(String, String)>,
) -> AppResult<Json<BrowserSessionView>> {
    load_source_config(&state, &key)
        .await?
        .ok_or(AppError::NotFound)?;
    let session = state
        .fetch_manager
        .browser()
        .cancel_interactive(&key, &session_id)
        .await
        .map_err(fetch_bad_request)?;
    Ok(Json(session))
}

async fn clear_browser_profile(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> AppResult<Json<serde_json::Value>> {
    let provider = load_source_config(&state, &key)
        .await?
        .ok_or(AppError::NotFound)?;
    state
        .fetch_manager
        .browser()
        .clear_profile(&key)
        .await
        .map_err(fetch_bad_request)?;
    runtime::record_fetch_failure(
        &state.pool,
        &key,
        provider.fetch_mode,
        FetchFailureKind::SessionExpired,
        "browser profile was cleared; establish a new session if this provider requires one",
    )
    .await?;
    Ok(Json(serde_json::json!({ "cleared": true })))
}

fn fetch_bad_request(error: crate::fetch::FetchError) -> AppError {
    AppError::BadRequest(error.to_string())
}

async fn diagnose_mode(
    state: &AppState,
    provider: &SourceProviderConfig,
    mode: FetchMode,
    url: reqwest::Url,
) -> DiagnoseAttempt {
    match fetch_for_diagnose(state, provider, mode, url).await {
        Ok(response) => {
            let page_kind = state
                .provider_registry
                .classify(&provider.adapter, &response);
            DiagnoseAttempt {
                attempted: true,
                success: page_kind == PageKind::ValidContent,
                status: response.status,
                page_kind: Some(page_kind),
                final_url: Some(response.final_url.to_string()),
                elapsed_ms: Some(response.elapsed_ms),
                error: (page_kind != PageKind::ValidContent)
                    .then(|| format!("provider returned {page_kind:?}")),
                failure_kind: None,
            }
        }
        Err(error) => {
            let page_kind = match error.kind {
                FetchFailureKind::RateLimited => PageKind::RateLimited,
                FetchFailureKind::TemporaryUnavailable => PageKind::TemporaryUnavailable,
                FetchFailureKind::SessionExpired => PageKind::LoginRequired,
                FetchFailureKind::InteractionRequired => PageKind::InteractionRequired,
                FetchFailureKind::AccessDenied => PageKind::AccessDenied,
                _ => PageKind::InvalidContent,
            };
            DiagnoseAttempt {
                attempted: true,
                success: false,
                status: error.status,
                page_kind: Some(page_kind),
                final_url: None,
                elapsed_ms: None,
                error: Some(error.to_string()),
                failure_kind: Some(error.kind),
            }
        }
    }
}

async fn fetch_for_diagnose(
    state: &AppState,
    provider: &SourceProviderConfig,
    mode: FetchMode,
    url: reqwest::Url,
) -> Result<FetchResponse, FetchError> {
    let mut transport = provider.transport();
    if provider.adapter != "javbus" || mode != FetchMode::Http {
        return state
            .fetch_manager
            .fetch(mode, FetchRequest::get(&provider.key, url), &transport)
            .await;
    }

    transport.cookie = Some(javbus_cookie(transport.cookie.as_deref(), &[]));
    let response = state
        .fetch_manager
        .fetch(
            mode,
            FetchRequest::get(&provider.key, url.clone()),
            &transport,
        )
        .await?;
    if state
        .provider_registry
        .classify(&provider.adapter, &response)
        != PageKind::AgeGate
    {
        return Ok(response);
    }

    let mut verify_url =
        reqwest::Url::parse(provider.base_url.trim_end_matches('/')).map_err(|error| {
            FetchError::new(
                &provider.key,
                FetchFailureKind::InvalidContent,
                error.to_string(),
                None,
            )
        })?;
    verify_url.set_path("/doc/driver-verify");
    verify_url
        .query_pairs_mut()
        .append_pair("referer", url.path());
    let verification = state
        .fetch_manager
        .fetch(
            mode,
            FetchRequest {
                provider_key: provider.key.clone(),
                url: verify_url,
                method: FetchMethod::Post,
                headers: reqwest::header::HeaderMap::new(),
                referer: Some(url.clone()),
                body: Some("Submit=confirm".into()),
                timeout: std::time::Duration::from_secs(20),
            },
            &transport,
        )
        .await?;
    let session = verification
        .headers
        .get_all(reqwest::header::SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .filter_map(|value| value.split(';').next())
        .collect::<Vec<_>>();
    transport.cookie = Some(javbus_cookie(
        provider.transport().cookie.as_deref(),
        &session,
    ));
    state
        .fetch_manager
        .fetch(mode, FetchRequest::get(&provider.key, url), &transport)
        .await
}

fn javbus_cookie(configured: Option<&str>, session: &[&str]) -> String {
    let mut cookies = vec!["age=verified", "existmag=all"];
    if let Some(configured) = configured.filter(|value| !value.trim().is_empty()) {
        cookies.push(configured.trim());
    }
    cookies.extend(session.iter().copied());
    cookies.join("; ")
}

#[cfg(test)]
mod tests {
    use super::javbus_cookie;

    #[test]
    fn javbus_diagnose_keeps_age_and_session_cookies() {
        assert_eq!(
            javbus_cookie(Some("PHPSESSID=stored"), &["PHPSESSID=fresh"]),
            "age=verified; existmag=all; PHPSESSID=stored; PHPSESSID=fresh"
        );
    }
}

async fn runtime_view(
    state: &AppState,
    provider: &SourceProviderConfig,
) -> AppResult<runtime::ProviderRuntimeView> {
    let browser = state.fetch_manager.browser();
    let profile = browser
        .profile_path(&provider.key)
        .map_err(|error| AppError::BadRequest(error.to_string()))?;
    runtime::load(
        &state.pool,
        &provider.key,
        browser.enabled(),
        &browser.executable().display().to_string(),
        &profile.display().to_string(),
    )
    .await?
    .ok_or(AppError::NotFound)
}

async fn load_source_config(
    state: &AppState,
    key: &str,
) -> AppResult<Option<SourceProviderConfig>> {
    let row = sqlx::query(
        "SELECT * FROM provider_config WHERE provider_key=? AND provider_type='source'",
    )
    .bind(key)
    .fetch_optional(&state.pool)
    .await?;
    Ok(row.as_ref().map(SourceProviderConfig::from_row))
}
