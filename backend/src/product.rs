use std::{
    collections::HashMap,
    convert::Infallible,
    path::{Component, Path, PathBuf},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post, put},
};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Row, Sqlite, Transaction};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use walkdir::WalkDir;

use crate::{
    AppState,
    error::{AppError, AppResult},
    fetch::{FetchError, FetchMethod, FetchRequest, FetchResponse, PageKind},
    ingestion::{
        DiscoveryJobPayload, EnqueueJob, HydrationJobPayload, PRIORITY_DAILY_INCREMENTAL,
        PRIORITY_HISTORICAL_BOOTSTRAP, PRIORITY_USER_ON_DEMAND, ResourceRefreshJobPayload,
        SnapshotInput, SyncMode,
    },
    metadata::{LocalizedAlias, MetadataSourceInput, SourceActor},
    pagination::{Paged, PageParams},
    providers::{
        ProviderContext, ProviderMediaRef, RawProviderDocument, ResourceCandidate, SourceMedia,
        SourceProviderConfig,
        runtime::{GateBlocked, ProviderExecutionGuard},
    },
    qbittorrent::{QBittorrentClient, magnet_hash, normalize_hash},
    storage,
};

const ACTIVE_STATES: &str = "'REQUESTED','RESOURCE_RESOLVING','QUEUED','DOWNLOADING','DOWNLOADED','PROCESSING','METADATA','LIBRARY_COMMIT'";
const RESOLVE_QBIT_ATTENTION: &str = "UPDATE attention_item SET status = 'resolved', resolved_at = datetime('now'), updated_at = datetime('now'), resolution_json = '{\"action\":\"qbit_reconciled\"}' WHERE acquisition_id = ? AND status = 'open' AND kind IN ('provider_unavailable','qbit_task_missing')";
const VIDEO_EXTENSIONS: &[&str] = &["mp4", "mkv", "avi", "mov", "wmv", "m4v", "ts", "webm"];

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/home", get(home))
        .route("/events", get(events))
        .route("/search", get(search))
        .route("/catalog/resolve", post(resolve_catalog_code))
        .route("/catalog/media", get(list_media))
        .route("/catalog/media/{id}", get(media_detail))
        .route(
            "/catalog/media/{id}/resources",
            get(media_resources).post(refresh_media_resources),
        )
        .route(
            "/catalog/media/{id}/resources/refresh",
            post(refresh_media_resources),
        )
        .route("/catalog/media/{id}/acquire", post(acquire_media))
        .route("/actors", get(list_actors))
        .route("/actors/{id}", get(actor_detail))
        .route(
            "/actors/{id}/follow",
            post(follow_actor).delete(unfollow_actor),
        )
        .route(
            "/acquisitions",
            get(list_acquisitions).post(create_acquisition),
        )
        .route(
            "/acquisitions/{id}",
            get(acquisition_detail).delete(delete_acquisition),
        )
        .route("/acquisitions/{id}/pause", post(pause_acquisition))
        .route("/acquisitions/{id}/resume", post(resume_acquisition))
        .route("/acquisitions/{id}/retry", post(retry_acquisition))
        .route("/acquisitions/{id}/cancel", post(cancel_acquisition))
        .route("/library", get(list_library))
        .route("/library/{id}", get(library_detail))
        .route("/library/{id}/reorganize", post(reorganize_library))
        .route("/library/{id}/nfo", post(regenerate_library_nfo))
        .route("/library/{id}/artwork", post(sync_library_artwork))
        .route("/library/{id}/rematch", post(rematch_library_item))
        .route("/attention", get(list_attention))
        .route("/attention/{id}", get(attention_detail))
        .route("/attention/{id}/action", post(attention_action))
        .route(
            "/automations",
            get(list_automations).post(create_automation),
        )
        .route(
            "/automations/{id}",
            put(update_automation).delete(delete_automation),
        )
        .route("/automations/{id}/enabled", post(set_automation_enabled))
        .route("/providers", get(list_providers).post(create_provider))
        .route(
            "/providers/{key}",
            put(update_provider).delete(delete_provider),
        )
        .route("/providers/{key}/enabled", post(set_provider_enabled))
        .route("/providers/{key}/test", post(test_provider))
        .route("/providers/{key}/sync", post(sync_provider))
        .route(
            "/providers/{key}/sync/incremental",
            post(sync_provider_incremental),
        )
        .route("/providers/{key}/bootstrap", post(bootstrap_provider))
        .route(
            "/providers/{key}/bootstrap/pause",
            post(pause_provider_bootstrap),
        )
        .route(
            "/providers/{key}/bootstrap/resume",
            post(resume_provider_bootstrap),
        )
        .route(
            "/product-settings",
            get(get_product_settings).put(update_product_settings),
        )
}

async fn events(
    State(state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let stream = BroadcastStream::new(state.events.subscribe())
        .filter_map(|message| message.ok().map(|data| Ok(Event::default().data(data))));
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Media {
    id: i64,
    code: String,
    title: String,
    original_title: Option<String>,
    summary: String,
    release_date: Option<String>,
    duration_minutes: Option<i64>,
    poster_url: Option<String>,
    backdrop_url: Option<String>,
    media_type: String,
    metadata_status: String,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Actor {
    id: i64,
    name: String,
    aliases: Vec<String>,
    avatar_url: Option<String>,
    followed: bool,
    media_count: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Resource {
    id: i64,
    media_id: i64,
    provider_key: String,
    title: String,
    download_url: String,
    info_hash: Option<String>,
    size_bytes: Option<i64>,
    resolution: Option<String>,
    subtitle_languages: Vec<String>,
    trackers: Vec<String>,
    published_at: Option<String>,
    score: f64,
    score_reasons: Vec<String>,
    available: bool,
    availability_status: String,
    codec: Option<String>,
    source_count: i64,
    first_seen_at: Option<String>,
    last_seen_at: Option<String>,
    last_verified_at: Option<String>,
    qbit_hash: Option<String>,
    qbit_state: Option<String>,
    qbit_sync_status: String,
    acquisition_id: Option<i64>,
    acquisition_state: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Acquisition {
    pub id: i64,
    media_id: i64,
    resource_id: Option<i64>,
    requested_by: String,
    state: String,
    state_message: String,
    qbit_hash: Option<String>,
    qbit_state: Option<String>,
    progress: f64,
    download_speed: i64,
    eta_seconds: Option<i64>,
    download_path: Option<String>,
    library_item_id: Option<i64>,
    last_error: Option<String>,
    retry_count: i64,
    created_at: String,
    updated_at: String,
    completed_at: Option<String>,
    media: Media,
    resource: Option<Resource>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquireInput {
    pub media_id: Option<i64>,
    pub resource_id: Option<i64>,
    #[serde(default = "manual_requester")]
    pub requested_by: String,
}

fn manual_requester() -> String {
    "manual".into()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchQuery {
    #[serde(default)]
    q: String,
    #[serde(default)]
    page: u32,
    #[serde(default)]
    page_size: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListQuery {
    #[serde(default)]
    status: String,
    #[serde(default)]
    q: String,
    #[serde(default)]
    page: u32,
    #[serde(default)]
    page_size: u32,
}

impl ListQuery {
    fn page_params(&self) -> PageParams {
        PageParams {
            page: self.page,
            page_size: self.page_size,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SearchResponse {
    query: String,
    media: Paged<Media>,
    actors: Paged<Actor>,
    provider_reports: Vec<ProviderReport>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProviderReport {
    provider_key: String,
    ok: bool,
    message: String,
    result_count: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CatalogResolveJobPayload {
    pub code: String,
    pub include_resources: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CatalogResolveInput {
    code: String,
    #[serde(default = "default_true")]
    include_resources: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogResolveResponse {
    code: String,
    media_id: Option<i64>,
    status: String,
    job_ids: Vec<i64>,
}

async fn home(
    State(state): State<AppState>,
    Query(params): Query<PageParams>,
) -> AppResult<Json<Value>> {
    let active: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM acquisition WHERE state IN ({ACTIVE_STATES})"
    ))
    .fetch_one(&state.pool)
    .await?;
    let library: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM library_item")
        .fetch_one(&state.pool)
        .await?;
    let attention: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM attention_item WHERE status = 'open'")
            .fetch_one(&state.pool)
            .await?;
    let followed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM actor WHERE followed = 1")
        .fetch_one(&state.pool)
        .await?;
    let recent_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM acquisition")
        .fetch_one(&state.pool)
        .await?;
    let recent_rows = sqlx::query(&format!(
        "{ACQUISITION_SELECT} ORDER BY a.id DESC LIMIT ? OFFSET ?"
    ))
        .bind(params.limit())
        .bind(params.offset())
        .fetch_all(&state.pool)
        .await?;
    let recent = Paged::new(
        recent_rows.iter().map(acquisition_from_row).collect(),
        recent_total,
        params,
    );
    Ok(Json(json!({
        "activeAcquisitions": active,
        "libraryCount": library,
        "attentionCount": attention,
        "followedActors": followed,
        "recentAcquisitions": recent,
        "quickStarts": [
            {"title":"搜索媒体", "description":"查找作品、演员和可获取资源", "to":"/resources"},
            {"title":"查看获取进度", "description":"跟踪从排队到入库的完整阶段", "to":"/downloads"},
            {"title":"处理需要关注", "description":"修复连接、路径或文件冲突", "to":"/downloads?attention=1"}
        ]
    })))
}

async fn search(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> AppResult<Json<SearchResponse>> {
    let params = PageParams {
        page: query.page,
        page_size: query.page_size,
    };
    let term = query.q.trim();
    if term.is_empty() {
        return Ok(Json(SearchResponse {
            query: String::new(),
            media: Paged::new(Vec::new(), 0, params),
            actors: Paged::new(Vec::new(), 0, params),
            provider_reports: Vec::new(),
        }));
    }
    // User-facing search is deliberately local-only. External providers are
    // refreshed by the background catalogue synchronizer, so a slow or blocked
    // provider can never hold this request open.
    let normalized_code = extract_media_code(term)
        .map(|code| normalize_code(&code))
        .unwrap_or_else(|| normalize_code(term));
    let normalized_alias = normalize_alias(term);
    let (media, actors) = if term.chars().count() >= 3 {
        let phrase = fts_phrase(term);
        let media_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM (WITH matches AS (SELECT rowid AS media_id, bm25(media_search_fts) AS rank FROM media_search_fts WHERE media_search_fts MATCH ?) SELECT m.id FROM media m LEFT JOIN matches x ON x.media_id=m.id WHERE m.normalized_code=? OR EXISTS(SELECT 1 FROM media_title_alias mta WHERE mta.media_id=m.id AND mta.normalized_alias=?) OR x.media_id IS NOT NULL)")
            .bind(&phrase).bind(&normalized_code).bind(&normalized_alias).fetch_one(&state.pool).await?;
        let media_rows = sqlx::query("WITH matches AS (SELECT rowid AS media_id, bm25(media_search_fts) AS rank FROM media_search_fts WHERE media_search_fts MATCH ?) SELECT m.*, (SELECT li.legacy_media_item_id FROM library_item li WHERE li.media_id=m.id AND li.legacy_media_item_id IS NOT NULL ORDER BY li.id DESC LIMIT 1) AS legacy_media_item_id, (SELECT mi.filename FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_filename, (SELECT mi.provider_id FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_provider_id FROM media m LEFT JOIN matches x ON x.media_id=m.id WHERE m.normalized_code=? OR EXISTS(SELECT 1 FROM media_title_alias mta WHERE mta.media_id=m.id AND mta.normalized_alias=?) OR x.media_id IS NOT NULL ORDER BY CASE WHEN m.normalized_code=? THEN 0 WHEN EXISTS(SELECT 1 FROM media_title_alias mta WHERE mta.media_id=m.id AND mta.normalized_alias=?) THEN 1 ELSE 2 END, COALESCE(x.rank,0), m.updated_at DESC LIMIT ? OFFSET ?")
            .bind(&phrase).bind(&normalized_code).bind(&normalized_alias).bind(&normalized_code).bind(&normalized_alias).bind(params.limit()).bind(params.offset()).fetch_all(&state.pool).await?;
        let actor_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM (WITH matches AS (SELECT rowid AS actor_id, bm25(actor_search_fts) AS rank FROM actor_search_fts WHERE actor_search_fts MATCH ?) SELECT a.id FROM actor a LEFT JOIN matches x ON x.actor_id=a.id WHERE a.normalized_name=? OR EXISTS(SELECT 1 FROM actor_name_alias ana WHERE ana.actor_id=a.id AND ana.normalized_alias=?) OR x.actor_id IS NOT NULL)")
            .bind(&phrase).bind(&normalized_alias).bind(&normalized_alias).fetch_one(&state.pool).await?;
        let actor_rows = sqlx::query("WITH matches AS (SELECT rowid AS actor_id, bm25(actor_search_fts) AS rank FROM actor_search_fts WHERE actor_search_fts MATCH ?) SELECT a.*, (SELECT COUNT(*) FROM media_actor ma WHERE ma.actor_id=a.id) AS media_count FROM actor a LEFT JOIN matches x ON x.actor_id=a.id WHERE a.normalized_name=? OR EXISTS(SELECT 1 FROM actor_name_alias ana WHERE ana.actor_id=a.id AND ana.normalized_alias=?) OR x.actor_id IS NOT NULL ORDER BY CASE WHEN a.normalized_name=? THEN 0 WHEN EXISTS(SELECT 1 FROM actor_name_alias ana WHERE ana.actor_id=a.id AND ana.normalized_alias=?) THEN 1 ELSE 2 END, a.followed DESC, COALESCE(x.rank,0), media_count DESC LIMIT ? OFFSET ?")
            .bind(&phrase).bind(&normalized_alias).bind(&normalized_alias).bind(&normalized_alias).bind(&normalized_alias).bind(params.limit()).bind(params.offset()).fetch_all(&state.pool).await?;
        (
            Paged::new(
                media_rows.iter().map(media_from_row).collect(),
                media_total,
                params,
            ),
            Paged::new(
                actor_rows.iter().map(actor_from_row).collect(),
                actor_total,
                params,
            ),
        )
    } else {
        // FTS5 trigram has no tokens for one- or two-character queries. Keep
        // this bounded fallback for short names while exact aliases still use
        // their B-tree indexes.
        let pattern = format!("%{}%", term.to_lowercase());
        let media_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM (SELECT m.id FROM media m JOIN media_search_document d ON d.media_id=m.id WHERE m.normalized_code=? OR EXISTS(SELECT 1 FROM media_title_alias mta WHERE mta.media_id=m.id AND mta.normalized_alias=?) OR lower(d.title) LIKE ? OR lower(d.original_title) LIKE ? OR lower(d.aliases) LIKE ? OR lower(d.actors) LIKE ? OR lower(d.resources) LIKE ?)")
            .bind(&normalized_code).bind(&normalized_alias).bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).fetch_one(&state.pool).await?;
        let media_rows = sqlx::query("SELECT m.*, (SELECT li.legacy_media_item_id FROM library_item li WHERE li.media_id=m.id AND li.legacy_media_item_id IS NOT NULL ORDER BY li.id DESC LIMIT 1) AS legacy_media_item_id, (SELECT mi.filename FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_filename, (SELECT mi.provider_id FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_provider_id FROM media m JOIN media_search_document d ON d.media_id=m.id WHERE m.normalized_code=? OR EXISTS(SELECT 1 FROM media_title_alias mta WHERE mta.media_id=m.id AND mta.normalized_alias=?) OR lower(d.title) LIKE ? OR lower(d.original_title) LIKE ? OR lower(d.aliases) LIKE ? OR lower(d.actors) LIKE ? OR lower(d.resources) LIKE ? ORDER BY CASE WHEN m.normalized_code=? THEN 0 ELSE 1 END, m.updated_at DESC LIMIT ? OFFSET ?")
            .bind(&normalized_code).bind(&normalized_alias).bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).bind(&normalized_code).bind(params.limit()).bind(params.offset()).fetch_all(&state.pool).await?;
        let actor_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM (SELECT a.id FROM actor a JOIN actor_search_document d ON d.actor_id=a.id WHERE a.normalized_name=? OR EXISTS(SELECT 1 FROM actor_name_alias ana WHERE ana.actor_id=a.id AND ana.normalized_alias=?) OR lower(d.name) LIKE ? OR lower(d.aliases) LIKE ?)")
            .bind(&normalized_alias).bind(&normalized_alias).bind(&pattern).bind(&pattern).fetch_one(&state.pool).await?;
        let actor_rows = sqlx::query("SELECT a.*, (SELECT COUNT(*) FROM media_actor ma WHERE ma.actor_id=a.id) AS media_count FROM actor a JOIN actor_search_document d ON d.actor_id=a.id WHERE a.normalized_name=? OR EXISTS(SELECT 1 FROM actor_name_alias ana WHERE ana.actor_id=a.id AND ana.normalized_alias=?) OR lower(d.name) LIKE ? OR lower(d.aliases) LIKE ? ORDER BY CASE WHEN a.normalized_name=? THEN 0 ELSE 1 END, a.followed DESC, media_count DESC LIMIT ? OFFSET ?")
            .bind(&normalized_alias).bind(&normalized_alias).bind(&pattern).bind(&pattern).bind(&normalized_alias).bind(params.limit()).bind(params.offset()).fetch_all(&state.pool).await?;
        (
            Paged::new(
                media_rows.iter().map(media_from_row).collect(),
                media_total,
                params,
            ),
            Paged::new(
                actor_rows.iter().map(actor_from_row).collect(),
                actor_total,
                params,
            ),
        )
    };
    Ok(Json(SearchResponse {
        query: term.into(),
        media,
        actors,
        provider_reports: Vec::new(),
    }))
}

async fn resolve_catalog_code(
    State(state): State<AppState>,
    Json(input): Json<CatalogResolveInput>,
) -> AppResult<Json<CatalogResolveResponse>> {
    let code = extract_media_code(&input.code)
        .map(|value| normalize_code(&value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AppError::BadRequest("请输入完整番号，例如 ABC-123".into()))?;
    let media_id = sqlx::query_scalar::<_, i64>("SELECT id FROM media WHERE normalized_code=?")
        .bind(&code)
        .fetch_optional(&state.pool)
        .await?;
    let rows = sqlx::query("SELECT * FROM provider_config WHERE provider_type='source' AND enabled=1 ORDER BY COALESCE(json_extract(config_json,'$.metadataPriority'),100) DESC,provider_key")
        .fetch_all(&state.pool)
        .await?;
    let providers = rows
        .iter()
        .map(source_provider_from_row)
        .filter(|provider| state.provider_registry.contains(&provider.adapter))
        .collect::<Vec<_>>();
    if providers.is_empty() {
        return Err(AppError::BadRequest(
            "没有已启用且支持按番号查找的数据源".into(),
        ));
    }
    let mut job_ids = Vec::with_capacity(providers.len());
    for provider in providers {
        let dedupe_key = format!("catalog-resolve:{}:{code}", provider.key);
        let job = state
            .ingestion_queue
            .enqueue(EnqueueJob {
                provider_key: &provider.key,
                job_type: "catalog_resolve",
                priority: PRIORITY_USER_ON_DEMAND,
                payload: serde_json::to_value(CatalogResolveJobPayload {
                    code: code.clone(),
                    include_resources: input.include_resources,
                })
                .map_err(anyhow::Error::from)?,
                max_attempts: 2,
                dedupe_key: Some(&dedupe_key),
            })
            .await?;
        job_ids.push(job.id);
    }
    emit(
        &state,
        "catalog-resolve",
        json!({"code":code,"status":"queued","jobIds":job_ids}),
    );
    Ok(Json(CatalogResolveResponse {
        code,
        media_id,
        status: "queued".into(),
        job_ids,
    }))
}

fn fts_phrase(value: &str) -> String {
    format!("\"{}\"", value.trim().replace('"', "\"\""))
}

#[cfg(test)]
fn push_search_term(terms: &mut Vec<String>, value: &str) {
    let value = value.trim();
    let normalized = normalize_alias(value);
    if !value.is_empty()
        && !normalized.is_empty()
        && !terms
            .iter()
            .any(|existing| normalize_alias(existing) == normalized)
    {
        terms.push(value.to_owned());
    }
}

async fn list_media(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> AppResult<Json<Paged<Media>>> {
    let params = query.page_params();
    let pattern = format!("%{}%", query.q.trim().to_lowercase());
    let alias_pattern = format!("%{}%", normalize_alias(query.q.trim()));
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media m WHERE (? = '%%' OR lower(m.title) LIKE ? OR lower(m.normalized_code) LIKE ? OR lower(COALESCE((SELECT mi.filename FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1),'')) LIKE ? OR lower(COALESCE((SELECT mi.provider_id FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1),'')) LIKE ? OR EXISTS(SELECT 1 FROM media_title_alias mta WHERE mta.media_id=m.id AND mta.normalized_alias LIKE ?))")
        .bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).bind(&alias_pattern).fetch_one(&state.pool).await?;
    let rows = sqlx::query("SELECT m.*, (SELECT li.legacy_media_item_id FROM library_item li WHERE li.media_id=m.id AND li.legacy_media_item_id IS NOT NULL ORDER BY li.id DESC LIMIT 1) AS legacy_media_item_id, (SELECT mi.filename FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_filename, (SELECT mi.provider_id FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_provider_id FROM media m WHERE (? = '%%' OR lower(m.title) LIKE ? OR lower(m.normalized_code) LIKE ? OR lower(COALESCE(legacy_filename,'')) LIKE ? OR lower(COALESCE(legacy_provider_id,'')) LIKE ? OR EXISTS(SELECT 1 FROM media_title_alias mta WHERE mta.media_id=m.id AND mta.normalized_alias LIKE ?)) ORDER BY m.updated_at DESC LIMIT ? OFFSET ?")
        .bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).bind(&alias_pattern).bind(params.limit()).bind(params.offset()).fetch_all(&state.pool).await?;
    Ok(Json(Paged::new(
        rows.iter().map(media_from_row).collect(),
        total,
        params,
    )))
}

async fn media_detail(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
    Query(query): Query<PageParams>,
) -> AppResult<Json<Value>> {
    let row = sqlx::query("SELECT m.*, (SELECT li.legacy_media_item_id FROM library_item li WHERE li.media_id=m.id AND li.legacy_media_item_id IS NOT NULL ORDER BY li.id DESC LIMIT 1) AS legacy_media_item_id, (SELECT mi.filename FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_filename, (SELECT mi.provider_id FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_provider_id FROM media m WHERE m.id = ?")
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or(AppError::NotFound)?;
    let media = media_from_row(&row);
    let (resource_items, resource_total) = resources_for_media_paged(&state, id, query).await?;
    let resources = Paged::new(resource_items, resource_total, query);
    let actor_rows = sqlx::query("SELECT a.*, (SELECT COUNT(*) FROM media_actor x WHERE x.actor_id = a.id) AS media_count, ma.role, ma.billing_order FROM actor a JOIN media_actor ma ON ma.actor_id = a.id WHERE ma.media_id = ? ORDER BY ma.billing_order, a.name")
        .bind(id).fetch_all(&state.pool).await?;
    let actors = actor_rows.iter().map(actor_from_row).collect::<Vec<_>>();
    let acquisition_id: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM acquisition WHERE media_id = ? ORDER BY id DESC LIMIT 1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .flatten();
    let library_id: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM library_item WHERE media_id = ? ORDER BY id DESC LIMIT 1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?
    .flatten();
    let source_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM metadata_source_record WHERE media_id=?")
        .bind(id)
        .fetch_one(&state.pool)
        .await?;
    let source_rows = sqlx::query("SELECT id,provider_key,provider_entity_id,source_url,record_kind,evidence_level,priority,title,original_title,summary,release_date,duration_minutes,poster_url,backdrop_url,actors_json,aliases_json,tags_json,first_seen_at,last_seen_at,updated_at FROM metadata_source_record WHERE media_id=? ORDER BY priority DESC,provider_key LIMIT ? OFFSET ?")
        .bind(id)
        .bind(query.limit())
        .bind(query.offset())
        .fetch_all(&state.pool)
        .await?;
    let metadata_sources = Paged::new(
        source_rows
            .iter()
            .map(|row| {
            json!({
                "id": row.get::<i64, _>("id"),
                "providerKey": row.get::<String, _>("provider_key"),
                "providerEntityId": row.get::<String, _>("provider_entity_id"),
                "sourceUrl": row.get::<Option<String>, _>("source_url"),
                "recordKind": row.get::<String, _>("record_kind"),
                "evidenceLevel": row.get::<i64, _>("evidence_level"),
                "priority": row.get::<i64, _>("priority"),
                "title": row.get::<Option<String>, _>("title"),
                "originalTitle": row.get::<Option<String>, _>("original_title"),
                "summary": row.get::<Option<String>, _>("summary"),
                "releaseDate": row.get::<Option<String>, _>("release_date"),
                "durationMinutes": row.get::<Option<i64>, _>("duration_minutes"),
                "posterUrl": row.get::<Option<String>, _>("poster_url"),
                "backdropUrl": row.get::<Option<String>, _>("backdrop_url"),
                "actors": parse_json(&row.get::<String, _>("actors_json"), json!([])),
                "aliases": parse_json(&row.get::<String, _>("aliases_json"), json!([])),
                "tags": parse_json(&row.get::<String, _>("tags_json"), json!([])),
                "firstSeenAt": row.get::<String, _>("first_seen_at"),
                "lastSeenAt": row.get::<String, _>("last_seen_at"),
                "updatedAt": row.get::<String, _>("updated_at"),
            })
            })
            .collect::<Vec<_>>(),
        source_total,
        query,
    );
    let provenance_rows = sqlx::query("SELECT field_name,provider_key,source_record_id,priority,value_json,source_updated_at,selected_at FROM metadata_field_provenance WHERE media_id=? ORDER BY field_name")
        .bind(id)
        .fetch_all(&state.pool)
        .await?;
    let field_provenance = provenance_rows
        .iter()
        .map(|row| {
            json!({
                "field": row.get::<String, _>("field_name"),
                "providerKey": row.get::<String, _>("provider_key"),
                "sourceRecordId": row.get::<i64, _>("source_record_id"),
                "priority": row.get::<i64, _>("priority"),
                "value": parse_json(&row.get::<String, _>("value_json"), Value::Null),
                "sourceUpdatedAt": row.get::<String, _>("source_updated_at"),
                "selectedAt": row.get::<String, _>("selected_at"),
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(
        json!({"media": media, "actors": actors, "resources": resources, "metadataSources": metadata_sources, "fieldProvenance": field_provenance, "latestAcquisitionId": acquisition_id, "libraryItemId": library_id}),
    ))
}

async fn media_resources(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
    Query(params): Query<PageParams>,
) -> AppResult<Json<Paged<Resource>>> {
    media_exists(&state, id).await?;
    let (items, total) = resources_for_media_paged(&state, id, params).await?;
    Ok(Json(Paged::new(items, total, params)))
}

async fn refresh_media_resources(
    State(state): State<AppState>,
    AxumPath(media_id): AxumPath<i64>,
) -> AppResult<Json<Value>> {
    media_exists(&state, media_id).await?;
    let providers = resource_provider_keys_for_media(&state, media_id).await?;
    if providers.is_empty() {
        return Err(AppError::BadRequest(
            "当前媒体没有支持磁链刷新的来源映射".into(),
        ));
    }
    let mut jobs = Vec::new();
    for provider_key in providers {
        jobs.push(
            enqueue_resource_refresh_job(
                &state,
                ResourceRefreshJobPayload {
                    media_id,
                    provider_key,
                    force: true,
                },
                PRIORITY_USER_ON_DEMAND,
            )
            .await?,
        );
    }
    Ok(Json(json!({
        "mediaId": media_id,
        "status": "queued",
        "jobIds": jobs,
    })))
}

async fn acquire_media(
    State(state): State<AppState>,
    AxumPath(media_id): AxumPath<i64>,
    Json(mut input): Json<AcquireInput>,
) -> AppResult<Json<Acquisition>> {
    input.media_id = Some(media_id);
    let acquisition = request_acquisition(&state, input).await?;
    Ok(Json(acquisition))
}

async fn create_acquisition(
    State(state): State<AppState>,
    Json(input): Json<AcquireInput>,
) -> AppResult<Json<Acquisition>> {
    Ok(Json(request_acquisition(&state, input).await?))
}

pub async fn request_acquisition(state: &AppState, input: AcquireInput) -> AppResult<Acquisition> {
    let media_id = match (input.media_id, input.resource_id) {
        (Some(id), _) => id,
        (None, Some(resource_id)) => {
            sqlx::query_scalar("SELECT media_id FROM resource WHERE id = ?")
                .bind(resource_id)
                .fetch_optional(&state.pool)
                .await?
                .ok_or(AppError::NotFound)?
        }
        _ => {
            return Err(AppError::BadRequest(
                "mediaId 或 resourceId 至少需要一个".into(),
            ));
        }
    };
    media_exists(state, media_id).await?;
    if let Some(id) = sqlx::query_scalar::<_, i64>(&format!("SELECT id FROM acquisition WHERE media_id = ? AND state IN ({ACTIVE_STATES}) ORDER BY id DESC LIMIT 1"))
        .bind(media_id).fetch_optional(&state.pool).await? {
        return acquisition_by_id(state, id).await;
    }
    let resource_id = match input.resource_id {
        Some(id) => Some(id),
        None => sqlx::query_scalar("SELECT id FROM resource WHERE media_id = ? AND available = 1 ORDER BY score DESC, published_at DESC LIMIT 1")
            .bind(media_id).fetch_optional(&state.pool).await?,
    };
    let resource_id =
        resource_id.ok_or_else(|| AppError::BadRequest("当前媒体没有可获取资源".into()))?;
    let belongs: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM resource WHERE id = ? AND media_id = ? AND available=1)",
    )
    .bind(resource_id)
    .bind(media_id)
    .fetch_one(&state.pool)
    .await?;
    if belongs == 0 {
        return Err(AppError::BadRequest("资源不属于该媒体或当前不可用".into()));
    }

    let mut tx = state.pool.begin().await?;
    let id = match sqlx::query(
        "INSERT INTO acquisition(media_id, resource_id, requested_by) VALUES (?, ?, ?)",
    )
    .bind(media_id)
    .bind(resource_id)
    .bind(input.requested_by.trim())
    .execute(&mut *tx)
    .await
    {
        Ok(result) => result.last_insert_rowid(),
        Err(sqlx::Error::Database(error)) if error.is_unique_violation() => {
            tx.rollback().await?;
            let id = sqlx::query_scalar(&format!("SELECT id FROM acquisition WHERE media_id = ? AND state IN ({ACTIVE_STATES}) ORDER BY id DESC LIMIT 1"))
                .bind(media_id).fetch_one(&state.pool).await?;
            return acquisition_by_id(state, id).await;
        }
        Err(error) => return Err(error.into()),
    };
    insert_event(
        &mut tx,
        id,
        "request",
        None,
        "REQUESTED",
        "获取请求已创建",
        json!({"requestedBy": input.requested_by}),
    )
    .await?;
    tx.commit().await?;
    emit(
        state,
        "acquisition.created",
        json!({"acquisitionId": id, "mediaId": media_id}),
    );
    let work_state = state.clone();
    tokio::spawn(async move {
        submit_acquisition(&work_state, id).await;
    });
    acquisition_by_id(state, id).await
}

async fn submit_acquisition(state: &AppState, id: i64) {
    if let Err(error) = submit_acquisition_inner(state, id).await {
        let _ = needs_attention(
            state,
            id,
            "provider_unavailable",
            "无法提交到 qBittorrent",
            &error.to_string(),
            vec!["retry", "cancel"],
        )
        .await;
    }
}

async fn submit_acquisition_inner(state: &AppState, id: i64) -> AppResult<()> {
    transition(
        state,
        id,
        "RESOURCE_RESOLVING",
        "正在确认首选资源",
        json!({}),
    )
    .await?;
    let row = sqlx::query("SELECT r.download_url, r.trackers_json FROM acquisition a JOIN resource r ON r.id = a.resource_id WHERE a.id = ?")
        .bind(id).fetch_optional(&state.pool).await?.ok_or(AppError::NotFound)?;
    let url: String = row.get("download_url");
    let trackers: Vec<String> =
        serde_json::from_str(&row.get::<String, _>("trackers_json")).unwrap_or_default();
    transition(state, id, "QUEUED", "正在提交到 qBittorrent", json!({})).await?;
    let settings = storage::load_settings(&state.pool).await?;
    let save_path = setting(state, "qbittorrent_save_path", "/downloads").await?;
    let category = setting(state, "qbittorrent_category", "luma").await?;
    let tags = setting(state, "qbittorrent_tags", "luma").await?;
    let hash = QBittorrentClient::new(&settings)?
        .add_download_with_options(
            &url,
            &trackers,
            Some(&save_path),
            Some(&category),
            Some(&tags),
        )
        .await?;
    sqlx::query("UPDATE acquisition SET qbit_hash = ?, updated_at = datetime('now') WHERE id = ?")
        .bind(hash.as_deref())
        .bind(id)
        .execute(&state.pool)
        .await?;
    transition(
        state,
        id,
        "DOWNLOADING",
        "qBittorrent 已接收，等待下载",
        json!({"qbitHash": hash}),
    )
    .await?;
    Ok(())
}

async fn list_acquisitions(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> AppResult<Json<Paged<Acquisition>>> {
    let params = query.page_params();
    if let Err(error) = reconcile_active(&state).await {
        tracing::warn!(%error, "acquisition list reconciliation failed");
    }
    let status = query.status.trim();
    let total: i64 = if status.is_empty() {
        sqlx::query_scalar("SELECT COUNT(*) FROM acquisition a")
            .fetch_one(&state.pool)
            .await?
    } else {
        sqlx::query_scalar("SELECT COUNT(*) FROM acquisition a WHERE a.state = ?")
            .bind(status)
            .fetch_one(&state.pool)
            .await?
    };
    let rows = if status.is_empty() {
        sqlx::query(&format!(
            "{ACQUISITION_SELECT} ORDER BY a.id DESC LIMIT ? OFFSET ?"
        ))
            .bind(params.limit())
            .bind(params.offset())
            .fetch_all(&state.pool)
            .await?
    } else {
        sqlx::query(&format!(
            "{ACQUISITION_SELECT} WHERE a.state = ? ORDER BY a.id DESC LIMIT ? OFFSET ?"
        ))
        .bind(status)
        .bind(params.limit())
        .bind(params.offset())
        .fetch_all(&state.pool)
        .await?
    };
    Ok(Json(Paged::new(
        rows.iter().map(acquisition_from_row).collect(),
        total,
        params,
    )))
}

async fn acquisition_detail(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
    Query(query): Query<PageParams>,
) -> AppResult<Json<Value>> {
    if let Err(error) = reconcile_active(&state).await {
        tracing::warn!(%error, acquisition_id = id, "acquisition detail reconciliation failed");
    }
    let acquisition = acquisition_by_id(&state, id).await?;
    let event_total: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM acquisition_event WHERE acquisition_id = ?")
            .bind(id)
            .fetch_one(&state.pool)
            .await?;
    let event_rows = sqlx::query(
        "SELECT * FROM acquisition_event WHERE acquisition_id = ? ORDER BY id LIMIT ? OFFSET ?",
    )
    .bind(id)
    .bind(query.limit())
    .bind(query.offset())
    .fetch_all(&state.pool)
    .await?;
    let events = Paged::new(
        event_rows
            .iter()
            .map(|row| {
                json!({
                    "id": row.get::<i64,_>("id"), "eventKey": row.get::<String,_>("event_key"),
                    "fromState": row.get::<Option<String>,_>("from_state"), "toState": row.get::<String,_>("to_state"),
                    "message": row.get::<String,_>("message"), "payload": parse_json(&row.get::<String,_>("payload_json"), json!({})),
                    "createdAt": row.get::<String,_>("created_at")
                })
            })
            .collect::<Vec<_>>(),
        event_total,
        query,
    );
    Ok(Json(json!({"acquisition": acquisition, "events": events})))
}

async fn pause_acquisition(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Acquisition>> {
    let item = acquisition_by_id(&state, id).await?;
    let hash = item
        .qbit_hash
        .ok_or_else(|| AppError::BadRequest("该获取尚未提交到 qBittorrent".into()))?;
    QBittorrentClient::new(&storage::load_settings(&state.pool).await?)?
        .pause(&hash)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    sqlx::query("UPDATE acquisition SET state_message = '已暂停', updated_at = datetime('now') WHERE id = ?").bind(id).execute(&state.pool).await?;
    emit(&state, "acquisition.updated", json!({"acquisitionId": id}));
    Ok(Json(acquisition_by_id(&state, id).await?))
}

async fn resume_acquisition(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Acquisition>> {
    let item = acquisition_by_id(&state, id).await?;
    let hash = item
        .qbit_hash
        .ok_or_else(|| AppError::BadRequest("该获取尚未提交到 qBittorrent".into()))?;
    QBittorrentClient::new(&storage::load_settings(&state.pool).await?)?
        .resume(&hash)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    sqlx::query("UPDATE acquisition SET state_message = '已继续', updated_at = datetime('now') WHERE id = ?").bind(id).execute(&state.pool).await?;
    emit(&state, "acquisition.updated", json!({"acquisitionId": id}));
    Ok(Json(acquisition_by_id(&state, id).await?))
}

async fn retry_acquisition(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Acquisition>> {
    if let Err(error) = reconcile_active(&state).await {
        tracing::warn!(%error, acquisition_id = id, "acquisition retry reconciliation failed");
    }
    let item = acquisition_by_id(&state, id).await?;
    if item.state != "NEEDS_ATTENTION" {
        if acquisition_has_live_qbit_task(item.qbit_hash.as_deref(), item.qbit_state.as_deref()) {
            sqlx::query(RESOLVE_QBIT_ATTENTION)
                .bind(id)
                .execute(&state.pool)
                .await?;
            return Ok(Json(item));
        }
        return Err(AppError::BadRequest("只有需要关注的获取可以重试".into()));
    }
    sqlx::query("UPDATE attention_item SET status = 'resolved', resolved_at = datetime('now'), updated_at = datetime('now'), resolution_json = '{\"action\":\"retry\"}' WHERE acquisition_id = ? AND status = 'open'")
        .bind(id).execute(&state.pool).await?;
    let resume_processing =
        item.qbit_hash.is_some() && item.download_path.is_some() && item.progress >= 0.999;
    if resume_processing {
        sqlx::query("UPDATE acquisition SET state = 'DOWNLOADED', state_message = '准备重试媒体处理', last_error = NULL, retry_count = retry_count + 1, updated_at = datetime('now') WHERE id = ?").bind(id).execute(&state.pool).await?;
        let work_state = state.clone();
        tokio::spawn(async move {
            process_acquisition(&work_state, id).await;
        });
    } else {
        sqlx::query("UPDATE acquisition SET state = 'REQUESTED', state_message = '准备重试', last_error = NULL, retry_count = retry_count + 1, qbit_hash = NULL, qbit_missing_since = NULL, updated_at = datetime('now') WHERE id = ?").bind(id).execute(&state.pool).await?;
        let work_state = state.clone();
        tokio::spawn(async move {
            submit_acquisition(&work_state, id).await;
        });
    }
    Ok(Json(acquisition_by_id(&state, id).await?))
}

async fn cancel_acquisition(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Acquisition>> {
    let item = acquisition_by_id(&state, id).await?;
    if matches!(item.state.as_str(), "COMPLETED" | "CANCELLED") {
        return Err(AppError::BadRequest("该获取已经结束".into()));
    }
    if let Some(hash) = &item.qbit_hash {
        let client = QBittorrentClient::new(&storage::load_settings(&state.pool).await?)?;
        client
            .remove(hash)
            .await
            .map_err(|e| AppError::BadRequest(e.to_string()))?;
    }
    transition(&state, id, "CANCELLED", "获取已取消", json!({})).await?;
    Ok(Json(acquisition_by_id(&state, id).await?))
}

async fn delete_acquisition(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<StatusCode> {
    let item = acquisition_by_id(&state, id).await?;
    let cancelled = item.state == "CANCELLED";
    let qbit_missing = item.qbit_state.as_deref() == Some("missing");
    if !cancelled && !qbit_missing {
        return Err(AppError::BadRequest(
            "只有已取消或 qBittorrent 中已删除的获取记录可以删除".into(),
        ));
    }
    let mut tx = state.pool.begin().await?;
    sqlx::query("DELETE FROM acquisition_event WHERE acquisition_id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM attention_item WHERE acquisition_id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE library_item SET acquisition_id = NULL WHERE acquisition_id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE automation_execution SET acquisition_id = NULL WHERE acquisition_id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    let result = sqlx::query("DELETE FROM acquisition WHERE id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound);
    }
    tx.commit().await?;
    emit(
        &state,
        "acquisition.deleted",
        json!({"acquisitionId": id}),
    );
    storage::log(
        &state.pool,
        "info",
        "acquisition",
        &format!("Deleted acquisition {}", id),
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn reconcile_active(state: &AppState) -> anyhow::Result<()> {
    let rows = sqlx::query("SELECT a.id, a.media_id, a.qbit_hash, a.state, r.info_hash AS resource_info_hash, r.download_url AS resource_download_url FROM acquisition a LEFT JOIN resource r ON r.id = a.resource_id ORDER BY a.id DESC")
        .fetch_all(&state.pool).await?;
    if rows.is_empty() {
        return Ok(());
    }
    let settings = storage::load_settings(&state.pool).await?;
    let torrents = match QBittorrentClient::new(&settings)?.torrents().await {
        Ok(items) => items,
        Err(error) => {
            tracing::warn!(%error, "acquisition reconciliation could not reach qBittorrent");
            return Ok(());
        }
    };
    for row in rows {
        let id: i64 = row.get("id");
        let media_id: i64 = row.get("media_id");
        let qbit_hash: Option<String> = row.get("qbit_hash");
        let state_name: String = row.get("state");
        let resource_info_hash: Option<String> = row.get("resource_info_hash");
        let resource_download_url: Option<String> = row.get("resource_download_url");
        let candidate_hashes = [
            qbit_hash.as_deref().and_then(normalize_hash),
            resource_info_hash.as_deref().and_then(normalize_hash),
            resource_download_url.as_deref().and_then(magnet_hash),
        ];
        let torrent = torrents.iter().find(|item| {
            normalize_hash(&item.hash).is_some_and(|hash| {
                candidate_hashes
                    .iter()
                    .flatten()
                    .any(|candidate| candidate == &hash)
            })
        });
        if let Some(torrent) = torrent {
            let complete =
                torrent_state_is_complete(torrent.progress, torrent.completion_on, &torrent.state);
            let mut target_state = reconciled_acquisition_state(&state_name, complete);
            if target_state.is_some() {
                let conflict: Option<i64> = sqlx::query_scalar("SELECT id FROM acquisition WHERE media_id = ? AND id != ? AND state NOT IN ('COMPLETED','CANCELLED','NEEDS_ATTENTION') ORDER BY id DESC LIMIT 1")
                    .bind(media_id).bind(id).fetch_optional(&state.pool).await?;
                if conflict.is_some() {
                    target_state = None;
                }
            }
            let download_path = Path::new(&torrent.save_path)
                .join(&torrent.name)
                .to_string_lossy()
                .to_string();
            let mut tx = state.pool.begin().await?;
            sqlx::query("UPDATE acquisition SET qbit_hash = ?, progress = ?, download_speed = ?, eta_seconds = ?, download_path = ?, qbit_state = ?, qbit_missing_since = NULL, updated_at = datetime('now') WHERE id = ?")
                .bind(&torrent.hash).bind(torrent.progress).bind(torrent.download_speed).bind(torrent.eta).bind(download_path).bind(&torrent.state).bind(id).execute(&mut *tx).await?;
            sqlx::query(RESOLVE_QBIT_ATTENTION)
                .bind(id)
                .execute(&mut *tx)
                .await?;
            let mut changed_to = None;
            if let Some(target) = target_state {
                let message = if target == "DOWNLOADED" {
                    "qBittorrent 下载已完成，准备整理".to_string()
                } else {
                    format!("已按 qBittorrent 状态恢复同步：{}", torrent.state)
                };
                let changed = sqlx::query("UPDATE acquisition SET state = ?, state_message = ?, last_error = NULL, updated_at = datetime('now') WHERE id = ? AND state = ?")
                    .bind(target).bind(&message).bind(id).bind(&state_name).execute(&mut *tx).await?;
                if changed.rows_affected() != 0 {
                    let key = format!(
                        "qbit-reconciled-{}-{}",
                        target.to_lowercase(),
                        chrono_like_nonce()
                    );
                    insert_event(
                        &mut tx,
                        id,
                        &key,
                        Some(&state_name),
                        target,
                        &message,
                        json!({"qbitHash": &torrent.hash, "qbitState": &torrent.state}),
                    )
                    .await?;
                    changed_to = Some((target.to_string(), message));
                }
            }
            tx.commit().await?;
            emit(
                state,
                "acquisition.progress",
                json!({"acquisitionId": id, "progress": torrent.progress, "downloadSpeed": torrent.download_speed, "etaSeconds": torrent.eta, "qbitState": torrent.state}),
            );
            if let Some((target, message)) = changed_to {
                emit(
                    state,
                    "acquisition.state",
                    json!({"acquisitionId": id, "from": state_name, "to": target, "message": message}),
                );
                if target == "DOWNLOADED" {
                    let state = state.clone();
                    tokio::spawn(async move {
                        process_acquisition(&state, id).await;
                    });
                }
            }
        } else if qbit_hash.is_some() && state_name != "COMPLETED" {
            sqlx::query("UPDATE acquisition SET qbit_state = 'missing', download_speed = 0, eta_seconds = NULL, qbit_missing_since = COALESCE(qbit_missing_since, datetime('now')), updated_at = datetime('now') WHERE id = ?")
                .bind(id).execute(&state.pool).await?;
            emit(
                state,
                "acquisition.progress",
                json!({"acquisitionId": id, "qbitState": "missing"}),
            );
            if state_name == "DOWNLOADING" {
                let missing_too_long: i64 = sqlx::query_scalar("SELECT CASE WHEN (julianday('now') - julianday(qbit_missing_since)) * 86400 >= 90 THEN 1 ELSE 0 END FROM acquisition WHERE id = ?")
                    .bind(id).fetch_one(&state.pool).await?;
                if missing_too_long != 0 {
                    let _ = needs_attention(
                        state,
                        id,
                        "qbit_task_missing",
                        "qBittorrent 中找不到任务",
                        "下载任务已连续 90 秒不在 qBittorrent 列表中，请重试或取消。",
                        vec!["retry", "cancel"],
                    )
                    .await;
                }
            }
        }
    }
    Ok(())
}

fn torrent_state_is_complete(progress: f64, completion_on: i64, state: &str) -> bool {
    progress >= 0.9999
        || completion_on > 0
        || matches!(
            state,
            "uploading" | "stalledUP" | "pausedUP" | "queuedUP" | "forcedUP"
        )
}

fn reconciled_acquisition_state(current: &str, complete: bool) -> Option<&'static str> {
    if matches!(
        current,
        "COMPLETED" | "DOWNLOADED" | "PROCESSING" | "METADATA" | "LIBRARY_COMMIT"
    ) {
        return None;
    }
    let target = if complete {
        "DOWNLOADED"
    } else {
        "DOWNLOADING"
    };
    (current != target).then_some(target)
}

fn acquisition_has_live_qbit_task(qbit_hash: Option<&str>, qbit_state: Option<&str>) -> bool {
    qbit_hash.is_some() && qbit_state.is_some_and(|state| !state.is_empty() && state != "missing")
}

pub async fn recover(state: &AppState) -> anyhow::Result<()> {
    let submit_ids = sqlx::query_scalar::<_, i64>("SELECT id FROM acquisition WHERE state IN ('REQUESTED','RESOURCE_RESOLVING','QUEUED') AND qbit_hash IS NULL")
        .fetch_all(&state.pool).await?;
    for id in submit_ids {
        sqlx::query("UPDATE acquisition SET state='REQUESTED', state_message='服务重启，准备恢复提交', updated_at=datetime('now') WHERE id=?")
            .bind(id).execute(&state.pool).await?;
        let state = state.clone();
        tokio::spawn(async move {
            submit_acquisition(&state, id).await;
        });
    }
    let process_ids = sqlx::query_scalar::<_, i64>("SELECT id FROM acquisition WHERE state IN ('DOWNLOADED','PROCESSING','METADATA','LIBRARY_COMMIT')")
        .fetch_all(&state.pool).await?;
    for id in process_ids {
        sqlx::query("UPDATE acquisition SET state='DOWNLOADED', state_message='服务重启，准备恢复媒体处理', updated_at=datetime('now') WHERE id=?")
            .bind(id).execute(&state.pool).await?;
        let state = state.clone();
        tokio::spawn(async move {
            process_acquisition(&state, id).await;
        });
    }
    Ok(())
}

pub async fn run_automations(state: &AppState) -> anyhow::Result<()> {
    let rules = sqlx::query("SELECT * FROM automation_rule WHERE enabled = 1 ORDER BY id")
        .fetch_all(&state.pool)
        .await?;
    for rule in rules {
        let rule_id: i64 = rule.get("id");
        let trigger_type: String = rule.get("trigger_type");
        let last_run: Option<String> = rule.get("last_run_at");
        let conditions = parse_json(&rule.get::<String, _>("conditions_json"), json!({}));
        let mode: String = rule.get("mode");
        let rows = if trigger_type == "FOLLOWED_ACTOR_UPDATE" {
            sqlx::query("SELECT DISTINCT r.id AS resource_id, r.media_id, r.score, r.provider_key, r.title, r.subtitle_languages_json FROM resource r JOIN media_actor ma ON ma.media_id = r.media_id JOIN actor a ON a.id = ma.actor_id WHERE a.followed = 1 AND r.created_at > COALESCE(?, '1970-01-01') ORDER BY r.id LIMIT 100")
                .bind(&last_run).fetch_all(&state.pool).await?
        } else {
            sqlx::query("SELECT r.id AS resource_id, r.media_id, r.score, r.provider_key, r.title, r.subtitle_languages_json FROM resource r WHERE r.created_at > COALESCE(?, '1970-01-01') ORDER BY r.id LIMIT 100")
                .bind(&last_run).fetch_all(&state.pool).await?
        };
        for item in rows {
            if !automation_matches(&conditions, &item) {
                continue;
            }
            let resource_id: i64 = item.get("resource_id");
            let media_id: i64 = item.get("media_id");
            let event_key = format!("resource:{resource_id}");
            let explanation = format!(
                "WHEN {trigger_type} 匹配，IF 条件通过，THEN {} ({mode})",
                rule.get::<String, _>("action_type")
            );
            let execution = sqlx::query("INSERT OR IGNORE INTO automation_execution(rule_id,event_key,status,explanation,media_id,resource_id,input_json) VALUES (?,?,'running',?,?,?,?)")
                .bind(rule_id).bind(&event_key).bind(&explanation).bind(media_id).bind(resource_id).bind(conditions.to_string()).execute(&state.pool).await?;
            if execution.rows_affected() == 0 {
                continue;
            }
            let execution_id: i64 = sqlx::query_scalar(
                "SELECT id FROM automation_execution WHERE rule_id = ? AND event_key = ?",
            )
            .bind(rule_id)
            .bind(&event_key)
            .fetch_one(&state.pool)
            .await?;
            match mode.as_str() {
                "AUTO" => match request_acquisition(
                    state,
                    AcquireInput {
                        media_id: Some(media_id),
                        resource_id: Some(resource_id),
                        requested_by: format!("automation:{rule_id}"),
                    },
                )
                .await
                {
                    Ok(acquisition) => {
                        sqlx::query("UPDATE automation_execution SET status='success', acquisition_id=?, output_json=?, finished_at=datetime('now') WHERE id=?").bind(acquisition.id).bind(json!({"action":"acquire","acquisitionId":acquisition.id}).to_string()).bind(execution_id).execute(&state.pool).await?;
                    }
                    Err(error) => {
                        sqlx::query("UPDATE automation_execution SET status='failed', error_message=?, finished_at=datetime('now') WHERE id=?").bind(error.to_string()).bind(execution_id).execute(&state.pool).await?;
                    }
                },
                "CONFIRM" => {
                    sqlx::query("INSERT INTO attention_item(kind,severity,title,message,media_id,actions_json) VALUES ('automation_confirm','info','自动化等待确认',?,?, '[\"acquire\",\"dismiss\"]')")
                        .bind(&explanation).bind(media_id).execute(&state.pool).await?;
                    sqlx::query("UPDATE automation_execution SET status='waiting_confirmation', output_json=?, finished_at=datetime('now') WHERE id=?").bind(json!({"action":"confirm"}).to_string()).bind(execution_id).execute(&state.pool).await?;
                }
                "NOTIFY" => {
                    sqlx::query("INSERT INTO attention_item(kind,severity,title,message,media_id,actions_json) VALUES ('notification','info','自动化发现新资源',?,?, '[\"dismiss\"]')")
                        .bind(&explanation).bind(media_id).execute(&state.pool).await?;
                    sqlx::query("UPDATE automation_execution SET status='notified', output_json=?, finished_at=datetime('now') WHERE id=?").bind(json!({"action":"notify"}).to_string()).bind(execution_id).execute(&state.pool).await?;
                }
                _ => {}
            }
        }
        sqlx::query("UPDATE automation_rule SET last_run_at=datetime('now'), next_run_at=datetime('now','+5 minutes'), updated_at=datetime('now') WHERE id=?").bind(rule_id).execute(&state.pool).await?;
    }
    Ok(())
}

fn automation_matches(conditions: &Value, row: &sqlx::sqlite::SqliteRow) -> bool {
    let object = conditions.as_object().cloned().unwrap_or_default();
    let score: f64 = row.get("score");
    let provider: String = row.get("provider_key");
    let title: String = row.get("title");
    let subtitles: Vec<String> = parse_string_vec(&row.get::<String, _>("subtitle_languages_json"));
    if object
        .get("minScore")
        .and_then(Value::as_f64)
        .is_some_and(|minimum| score < minimum)
    {
        return false;
    }
    if object
        .get("providerKey")
        .and_then(Value::as_str)
        .is_some_and(|expected| !expected.is_empty() && expected != provider)
    {
        return false;
    }
    if object
        .get("keyword")
        .and_then(Value::as_str)
        .is_some_and(|keyword| {
            !keyword.is_empty() && !title.to_lowercase().contains(&keyword.to_lowercase())
        })
    {
        return false;
    }
    if object
        .get("requireSubtitle")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && subtitles.is_empty()
        && !title.contains("字幕")
    {
        return false;
    }
    true
}

async fn process_acquisition(state: &AppState, id: i64) {
    if let Err(error) = process_acquisition_inner(state, id).await {
        let _ = needs_attention(
            state,
            id,
            "processing_failed",
            "媒体整理未完成",
            &error.to_string(),
            vec!["retry", "cancel"],
        )
        .await;
    }
}

async fn process_acquisition_inner(state: &AppState, id: i64) -> AppResult<()> {
    transition(state, id, "PROCESSING", "正在整理媒体文件", json!({})).await?;
    let acquisition = acquisition_by_id(state, id).await?;
    let download_path = acquisition
        .download_path
        .clone()
        .ok_or_else(|| AppError::BadRequest("qBittorrent 没有返回下载路径".into()))?;
    let qbit_save = setting(state, "qbittorrent_save_path", "/downloads").await?;
    let download_root = PathBuf::from(setting(state, "download_root", "/downloads").await?);
    let media_root = PathBuf::from(setting(state, "media_root", "/media").await?);
    let mapped = map_download_path(
        Path::new(&download_path),
        Path::new(&qbit_save),
        &download_root,
    )?;
    let source = select_video(&mapped).ok_or_else(|| {
        AppError::BadRequest(format!(
            "下载目录中没有支持的视频文件：{}",
            mapped.display()
        ))
    })?;
    ensure_within(&source, &download_root)?;
    let extension = source
        .extension()
        .and_then(|v| v.to_str())
        .unwrap_or("mkv")
        .to_ascii_lowercase();
    let code = safe_segment(&acquisition.media.code);
    let destination = media_root.join(&code).join(format!("{code}.{extension}"));
    ensure_lexically_within(&destination, &media_root)?;
    if destination.exists() {
        return Err(AppError::BadRequest(format!(
            "目标文件已经存在：{}",
            destination.display()
        )));
    }
    tokio::fs::create_dir_all(
        destination
            .parent()
            .ok_or_else(|| AppError::BadRequest("目标路径无效".into()))?,
    )
    .await
    .map_err(anyhow::Error::from)?;
    let mode = setting(state, "organizer_mode", "hardlink").await?;
    if mode == "hardlink" {
        if tokio::fs::hard_link(&source, &destination).await.is_err() {
            tokio::fs::copy(&source, &destination)
                .await
                .map_err(anyhow::Error::from)?;
        }
    } else {
        tokio::fs::copy(&source, &destination)
            .await
            .map_err(anyhow::Error::from)?;
    }
    transition(
        state,
        id,
        "METADATA",
        "正在从 Luma 数据库生成本地元数据",
        json!({"videoPath": destination}),
    )
    .await?;
    if canonical_metadata_needs_fallback(state, acquisition.media_id).await? {
        match enrich_with_metatube(state, acquisition.media_id, &acquisition.media.code).await {
            Ok((_, _, Some(poster_path))) => {
                crate::export::artwork::register_cached_asset(
                    &state.pool,
                    acquisition.media_id,
                    "poster",
                    acquisition.media.poster_url.as_deref(),
                    &poster_path,
                    None,
                )
                .await?;
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(%error, media_id = acquisition.media_id, "MetaTube fallback failed; canonical Luma metadata will still be exported");
                sqlx::query("INSERT INTO metadata_record_v2(media_id, provider_key, status, raw_json, error_message) VALUES (?, 'metatube', 'failed', '{}', ?)")
                    .bind(acquisition.media_id)
                    .bind(error.to_string())
                    .execute(&state.pool)
                    .await?;
            }
        }
    }
    let export = crate::export::write_sidecars_for_media(
        &state.pool,
        acquisition.media_id,
        &destination,
        crate::export::ExportOptions {
            overwrite_nfo: true,
            overwrite_artwork: false,
        },
    )
    .await?;
    transition(state, id, "LIBRARY_COMMIT", "正在提交媒体库", json!({})).await?;
    let file_size = tokio::fs::metadata(&destination)
        .await
        .ok()
        .map(|m| m.len() as i64);
    let legacy_id = sqlx::query("INSERT INTO media_item(folder_id, path, filename, hash, title, media_type, status) VALUES (NULL, ?, ?, ?, ?, ?, 'ready') ON CONFLICT(path) DO UPDATE SET title = excluded.title, status = 'ready', updated_at = datetime('now') RETURNING id")
        .bind(destination.to_string_lossy().to_string()).bind(destination.file_name().and_then(|v| v.to_str()).unwrap_or_default()).bind(format!("luma-{id}"))
        .bind(&acquisition.media.title).bind(&acquisition.media.media_type).fetch_one(&state.pool).await?.get::<i64,_>("id");
    let library_id = sqlx::query("INSERT INTO library_item(media_id, acquisition_id, legacy_media_item_id, video_path, nfo_path, poster_path, status, file_size) VALUES (?, ?, ?, ?, ?, ?, 'ready', ?)")
        .bind(acquisition.media_id).bind(id).bind(legacy_id).bind(destination.to_string_lossy().to_string()).bind(export.nfo_path.to_string_lossy().to_string()).bind(export.poster_path.as_ref().map(|path| path.to_string_lossy().to_string())).bind(file_size).execute(&state.pool).await?.last_insert_rowid();
    sqlx::query("INSERT INTO library_export_state(library_item_id,media_id,nfo_path,poster_path,metadata_updated_at,status,last_exported_at) VALUES (?,?,?,?,?,'success',datetime('now')) ON CONFLICT(library_item_id) DO UPDATE SET nfo_path=excluded.nfo_path,poster_path=COALESCE(excluded.poster_path,library_export_state.poster_path),metadata_updated_at=excluded.metadata_updated_at,status='success',last_error=NULL,last_exported_at=datetime('now'),updated_at=datetime('now')")
        .bind(library_id)
        .bind(acquisition.media_id)
        .bind(export.nfo_path.to_string_lossy().to_string())
        .bind(export.poster_path.as_ref().map(|path| path.to_string_lossy().to_string()))
        .bind(export.metadata_updated_at)
        .execute(&state.pool)
        .await?;
    sqlx::query("UPDATE acquisition SET library_item_id = ? WHERE id = ?")
        .bind(library_id)
        .bind(id)
        .execute(&state.pool)
        .await?;
    transition(
        state,
        id,
        "COMPLETED",
        "已进入媒体库",
        json!({"libraryItemId": library_id}),
    )
    .await?;
    Ok(())
}

async fn canonical_metadata_needs_fallback(state: &AppState, media_id: i64) -> AppResult<bool> {
    let needs: i64 = sqlx::query_scalar("SELECT CASE WHEN trim(title)='' OR (lower(trim(title))=lower(trim(normalized_code)) AND trim(summary)='' AND release_date IS NULL AND poster_url IS NULL) THEN 1 ELSE 0 END FROM media WHERE id=?")
        .bind(media_id)
        .fetch_one(&state.pool)
        .await?;
    Ok(needs != 0)
}

async fn enrich_with_metatube(
    state: &AppState,
    media_id: i64,
    code: &str,
) -> anyhow::Result<(Value, String, Option<PathBuf>)> {
    if !provider_enabled(state, "metatube").await? {
        anyhow::bail!("MetaTube Provider 已停用");
    }
    let settings = storage::load_settings(&state.pool).await?;
    let client = crate::provider::MetaTubeClient::new(&settings)?;
    let match_item = client.search_movie(code).await?;
    let remote = client
        .movie_info(&match_item.provider, &match_item.id)
        .await?;
    let title = first_json_string(&remote, &["title"]).unwrap_or(code);
    let original_title = first_json_string(&remote, &["original_title", "number"]);
    let summary =
        first_json_string(&remote, &["summary", "plot", "description"]).unwrap_or_default();
    let release_date = first_json_string(&remote, &["release_date", "premiered"]);
    let poster_url = first_json_string(&remote, &["big_cover_url", "cover_url", "poster_url"]);
    let backdrop_url = first_json_string(&remote, &["big_thumb_url", "thumb_url", "backdrop_url"]);
    let provider_entity_id = format!("{}:{}", match_item.provider, match_item.id);
    let mut source =
        MetadataSourceInput::catalogue(media_id, "metatube", &provider_entity_id, code);
    source.record_kind = "detail".into();
    source.evidence_level = 2;
    source.priority = 25;
    source.title = Some(title.to_owned());
    source.original_title = original_title.map(str::to_owned);
    source.summary = (!summary.trim().is_empty()).then(|| summary.to_owned());
    source.release_date = release_date.map(str::to_owned);
    source.poster_url = poster_url.map(str::to_owned);
    source.backdrop_url = backdrop_url.map(str::to_owned);
    source.aliases = localized_media_titles(&remote)
        .into_iter()
        .map(|(alias, locale)| LocalizedAlias::new(alias, locale))
        .collect();
    source.actors = source_actors_from_json(&remote);
    source.tags = json_string_array(remote.get("genres"));
    source.raw_json = remote.clone();
    crate::metadata::record_and_resolve(&state.pool, &source).await?;
    sqlx::query("INSERT INTO provider_entity_mapping(provider_key,entity_type,provider_entity_id,media_id,raw_json) VALUES ('metatube','media',?,?,?) ON CONFLICT(provider_key,entity_type,provider_entity_id) DO UPDATE SET media_id=excluded.media_id,raw_json=excluded.raw_json,last_seen_at=datetime('now')")
        .bind(&provider_entity_id).bind(media_id).bind(remote.to_string()).execute(&state.pool).await?;
    let poster_path = if let Some(url) = poster_url {
        match client.download_image(url).await {
            Ok(image) => {
                let directory = PathBuf::from(setting(state, "media_root", "/media").await?)
                    .join(safe_segment(code));
                let path =
                    directory.join(format!("{}-poster.{}", safe_segment(code), image.extension));
                tokio::fs::write(&path, image.bytes).await?;
                Some(path)
            }
            Err(error) => {
                tracing::warn!(%error, media_id, "MetaTube poster download failed");
                None
            }
        }
    } else {
        None
    };
    Ok((remote, "metatube".into(), poster_path))
}

fn source_actors_from_json(remote: &Value) -> Vec<SourceActor> {
    remote
        .get("actors")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| {
            let (name, avatar) = match item {
                Value::String(name) => (name.as_str(), None),
                Value::Object(map) => (
                    map.get("name").and_then(Value::as_str).unwrap_or_default(),
                    map.get("image_url")
                        .or_else(|| map.get("thumb_url"))
                        .and_then(Value::as_str),
                ),
                _ => ("", None),
            };
            if name.is_empty() {
                return None;
            }
            let provider_actor_id = item
                .get("id")
                .or_else(|| item.get("actor_id"))
                .or_else(|| item.get("provider_actor_id"))
                .and_then(json_value_string);
            let mut aliases = localized_actor_names(item)
                .into_iter()
                .map(|(alias, locale)| LocalizedAlias::new(alias, locale))
                .collect::<Vec<_>>();
            aliases.extend(
                item.get("aliases")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(|alias| LocalizedAlias::new(alias, "und")),
            );
            Some(SourceActor {
                provider_actor_id,
                name: name.to_owned(),
                aliases,
                avatar_url: avatar.map(str::to_owned),
            })
        })
        .collect()
}

fn json_string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

async fn transition(
    state: &AppState,
    id: i64,
    to: &str,
    message: &str,
    payload: Value,
) -> AppResult<()> {
    let mut tx = state.pool.begin().await?;
    let from: String = sqlx::query_scalar("SELECT state FROM acquisition WHERE id = ?")
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(AppError::NotFound)?;
    if from == to {
        tx.rollback().await?;
        return Ok(());
    }
    if !legal_transition(&from, to) {
        return Err(AppError::BadRequest(format!(
            "非法获取状态转换：{from} -> {to}"
        )));
    }
    sqlx::query("UPDATE acquisition SET state = ?, state_message = ?, last_error = CASE WHEN ? = 'NEEDS_ATTENTION' THEN last_error ELSE NULL END, updated_at = datetime('now'), completed_at = CASE WHEN ? = 'COMPLETED' THEN datetime('now') ELSE completed_at END WHERE id = ?")
        .bind(to).bind(message).bind(to).bind(to).bind(id).execute(&mut *tx).await?;
    let key = format!(
        "{}-{}-{}",
        from.to_lowercase(),
        to.to_lowercase(),
        chrono_like_nonce()
    );
    insert_event(&mut tx, id, &key, Some(&from), to, message, payload).await?;
    tx.commit().await?;
    emit(
        state,
        "acquisition.state",
        json!({"acquisitionId": id, "from": from, "to": to, "message": message}),
    );
    Ok(())
}

fn legal_transition(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        ("REQUESTED", "RESOURCE_RESOLVING")
            | ("RESOURCE_RESOLVING", "QUEUED")
            | ("QUEUED", "DOWNLOADING")
            | ("DOWNLOADING", "DOWNLOADED")
            | ("DOWNLOADED", "PROCESSING")
            | ("PROCESSING", "METADATA")
            | ("METADATA", "LIBRARY_COMMIT")
            | ("LIBRARY_COMMIT", "COMPLETED")
            | ("NEEDS_ATTENTION", "REQUESTED")
    ) || (to == "NEEDS_ATTENTION" && !matches!(from, "COMPLETED" | "CANCELLED"))
        || (to == "CANCELLED" && !matches!(from, "COMPLETED" | "CANCELLED"))
}

async fn needs_attention(
    state: &AppState,
    id: i64,
    kind: &str,
    title: &str,
    message: &str,
    actions: Vec<&str>,
) -> AppResult<()> {
    let media_id: i64 = sqlx::query_scalar("SELECT media_id FROM acquisition WHERE id = ?")
        .bind(id)
        .fetch_one(&state.pool)
        .await?;
    sqlx::query("UPDATE acquisition SET state = 'NEEDS_ATTENTION', state_message = ?, last_error = ?, updated_at = datetime('now') WHERE id = ?")
        .bind(title).bind(message).bind(id).execute(&state.pool).await?;
    sqlx::query("INSERT INTO attention_item(kind, title, message, acquisition_id, media_id, actions_json) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(acquisition_id, kind) WHERE status = 'open' AND acquisition_id IS NOT NULL DO UPDATE SET title = excluded.title, message = excluded.message, actions_json = excluded.actions_json, updated_at = datetime('now')")
        .bind(kind).bind(title).bind(message).bind(id).bind(media_id).bind(serde_json::to_string(&actions).unwrap_or_else(|_| "[]".into())).execute(&state.pool).await?;
    emit(
        state,
        "attention.created",
        json!({"acquisitionId": id, "kind": kind, "title": title}),
    );
    Ok(())
}

async fn insert_event(
    tx: &mut Transaction<'_, Sqlite>,
    acquisition_id: i64,
    event_key: &str,
    from: Option<&str>,
    to: &str,
    message: &str,
    payload: Value,
) -> AppResult<()> {
    sqlx::query("INSERT OR IGNORE INTO acquisition_event(acquisition_id, event_key, from_state, to_state, message, payload_json) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(acquisition_id).bind(event_key).bind(from).bind(to).bind(message).bind(payload.to_string()).execute(&mut **tx).await?;
    Ok(())
}

async fn list_actors(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> AppResult<Json<Paged<Actor>>> {
    let params = query.page_params();
    let pattern = format!("%{}%", query.q.trim().to_lowercase());
    let alias_pattern = format!("%{}%", normalize_alias(query.q.trim()));
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM actor a WHERE (? = '%%' OR lower(a.name) LIKE ? OR lower(a.aliases_json) LIKE ? OR EXISTS(SELECT 1 FROM actor_name_alias ana WHERE ana.actor_id=a.id AND ana.normalized_alias LIKE ?))")
        .bind(&pattern).bind(&pattern).bind(&pattern).bind(&alias_pattern).fetch_one(&state.pool).await?;
    let rows = sqlx::query("SELECT a.*, (SELECT COUNT(*) FROM media_actor ma WHERE ma.actor_id = a.id) AS media_count FROM actor a WHERE (? = '%%' OR lower(a.name) LIKE ? OR lower(a.aliases_json) LIKE ? OR EXISTS(SELECT 1 FROM actor_name_alias ana WHERE ana.actor_id=a.id AND ana.normalized_alias LIKE ?)) ORDER BY a.followed DESC, media_count DESC, a.name LIMIT ? OFFSET ?")
        .bind(&pattern).bind(&pattern).bind(&pattern).bind(&alias_pattern).bind(params.limit()).bind(params.offset()).fetch_all(&state.pool).await?;
    Ok(Json(Paged::new(
        rows.iter().map(actor_from_row).collect(),
        total,
        params,
    )))
}

async fn actor_detail(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
    Query(query): Query<PageParams>,
) -> AppResult<Json<Value>> {
    let row = sqlx::query("SELECT a.*, (SELECT COUNT(*) FROM media_actor ma WHERE ma.actor_id = a.id) AS media_count FROM actor a WHERE id = ?").bind(id).fetch_optional(&state.pool).await?.ok_or(AppError::NotFound)?;
    let media_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media m JOIN media_actor ma ON ma.media_id = m.id WHERE ma.actor_id = ?")
        .bind(id)
        .fetch_one(&state.pool)
        .await?;
    let media_rows = sqlx::query("SELECT m.*, (SELECT li.legacy_media_item_id FROM library_item li WHERE li.media_id=m.id AND li.legacy_media_item_id IS NOT NULL ORDER BY li.id DESC LIMIT 1) AS legacy_media_item_id, (SELECT mi.filename FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_filename, (SELECT mi.provider_id FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_provider_id FROM media m JOIN media_actor ma ON ma.media_id = m.id WHERE ma.actor_id = ? ORDER BY m.release_date DESC, m.updated_at DESC LIMIT ? OFFSET ?")
        .bind(id)
        .bind(query.limit())
        .bind(query.offset())
        .fetch_all(&state.pool)
        .await?;
    let media = Paged::new(
        media_rows.iter().map(media_from_row).collect(),
        media_total,
        query,
    );
    Ok(Json(json!({"actor": actor_from_row(&row), "media": media})))
}

async fn follow_actor(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Actor>> {
    set_follow(&state, id, true).await
}
async fn unfollow_actor(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Actor>> {
    set_follow(&state, id, false).await
}
async fn set_follow(state: &AppState, id: i64, followed: bool) -> AppResult<Json<Actor>> {
    let updated =
        sqlx::query("UPDATE actor SET followed = ?, updated_at = datetime('now') WHERE id = ?")
            .bind(followed)
            .bind(id)
            .execute(&state.pool)
            .await?;
    if updated.rows_affected() == 0 {
        return Err(AppError::NotFound);
    }
    let row = sqlx::query("SELECT a.*, (SELECT COUNT(*) FROM media_actor ma WHERE ma.actor_id = a.id) AS media_count FROM actor a WHERE id = ?").bind(id).fetch_one(&state.pool).await?;
    Ok(Json(actor_from_row(&row)))
}

async fn list_library(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> AppResult<Json<Value>> {
    let params = query.page_params();
    let pattern = format!("%{}%", query.q.trim().to_lowercase());
    let alias_pattern = format!("%{}%", normalize_alias(query.q.trim()));
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM library_item li JOIN media m ON m.id = li.media_id LEFT JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE (? = '%%' OR lower(m.title) LIKE ? OR lower(m.normalized_code) LIKE ? OR lower(COALESCE(mi.filename,'')) LIKE ? OR lower(COALESCE(mi.provider_id,'')) LIKE ? OR EXISTS(SELECT 1 FROM media_title_alias mta WHERE mta.media_id=m.id AND mta.normalized_alias LIKE ?))")
        .bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).bind(&alias_pattern).fetch_one(&state.pool).await?;
    let rows = sqlx::query("SELECT li.*, m.normalized_code, m.title, m.poster_url, m.release_date, m.metadata_status, mi.filename AS legacy_filename, mi.provider_id AS legacy_provider_id FROM library_item li JOIN media m ON m.id = li.media_id LEFT JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE (? = '%%' OR lower(m.title) LIKE ? OR lower(m.normalized_code) LIKE ? OR lower(COALESCE(mi.filename,'')) LIKE ? OR lower(COALESCE(mi.provider_id,'')) LIKE ? OR EXISTS(SELECT 1 FROM media_title_alias mta WHERE mta.media_id=m.id AND mta.normalized_alias LIKE ?)) ORDER BY li.added_at DESC LIMIT ? OFFSET ?")
        .bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).bind(&alias_pattern).bind(params.limit()).bind(params.offset()).fetch_all(&state.pool).await?;
    let values = rows.iter().map(library_json).collect::<Vec<_>>();
    Ok(Json(json!({
        "items": values,
        "total": total,
        "page": params.page(),
        "pageSize": params.page_size(),
        "totalPages": Paged::<Value>::total_pages(total, params.page_size()),
    })))
}

async fn library_detail(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Value>> {
    let row = sqlx::query("SELECT li.*, m.normalized_code, m.title, m.poster_url, m.release_date, m.metadata_status, mi.filename AS legacy_filename, mi.provider_id AS legacy_provider_id FROM library_item li JOIN media m ON m.id = li.media_id LEFT JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.id = ?").bind(id).fetch_optional(&state.pool).await?.ok_or(AppError::NotFound)?;
    Ok(Json(library_json(&row)))
}

async fn reorganize_library(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Value>> {
    crate::export::regenerate_library_item(
        &state.pool,
        id,
        crate::export::ExportOptions::default(),
    )
    .await?;
    let row = sqlx::query("SELECT li.*, m.normalized_code, m.title, m.poster_url, m.release_date, m.metadata_status, mi.filename AS legacy_filename, mi.provider_id AS legacy_provider_id FROM library_item li JOIN media m ON m.id = li.media_id LEFT JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.id = ?").bind(id).fetch_optional(&state.pool).await?.ok_or(AppError::NotFound)?;
    Ok(Json(
        json!({"item": library_json(&row), "message": "已按当前 Luma 数据重新生成 NFO"}),
    ))
}

async fn regenerate_library_nfo(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Value>> {
    let report = crate::export::regenerate_library_item(
        &state.pool,
        id,
        crate::export::ExportOptions {
            overwrite_nfo: true,
            overwrite_artwork: false,
        },
    )
    .await?;
    Ok(Json(
        json!({"export": report, "message": "NFO 已从 Luma 数据库重新生成"}),
    ))
}

async fn sync_library_artwork(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Value>> {
    let report = crate::export::regenerate_library_item(
        &state.pool,
        id,
        crate::export::ExportOptions {
            overwrite_nfo: false,
            overwrite_artwork: true,
        },
    )
    .await?;
    let message = if report.poster_path.is_some() {
        "本地缓存图片已重新同步"
    } else {
        "暂无可离线同步的本地图片缓存"
    };
    Ok(Json(json!({"export": report, "message": message})))
}

#[derive(Debug, Default, Deserialize)]
struct LibraryRematchInput {
    code: Option<String>,
}

async fn rematch_library_item(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
    input: Option<Json<LibraryRematchInput>>,
) -> AppResult<Json<Value>> {
    let row = sqlx::query("SELECT video_path,legacy_media_item_id FROM library_item WHERE id=?")
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or(AppError::NotFound)?;
    let video_path: String = row.get("video_path");
    let requested = input
        .and_then(|Json(value)| value.code)
        .and_then(|value| extract_media_code(&value))
        .or_else(|| {
            Path::new(&video_path)
                .file_name()
                .and_then(|value| value.to_str())
                .and_then(extract_media_code)
        })
        .or_else(|| extract_media_code(&video_path))
        .map(|value| normalize_code(&value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AppError::BadRequest("无法从文件名识别番号，请手动填写番号".into()))?;
    let media_id = sqlx::query_scalar::<_, i64>("SELECT id FROM media WHERE normalized_code=?")
        .bind(&requested)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| {
            AppError::BadRequest(format!(
                "本地数据库尚未收录 {}，请先从数据源查找该番号",
                requested.to_ascii_uppercase()
            ))
        })?;
    let legacy_media_item_id: Option<i64> = row.get("legacy_media_item_id");
    let mut transaction = state.pool.begin().await?;
    sqlx::query(
        "UPDATE library_item SET media_id=?,status='ready',updated_at=datetime('now') WHERE id=?",
    )
    .bind(media_id)
    .bind(id)
    .execute(&mut *transaction)
    .await?;
    if let Some(legacy_media_item_id) = legacy_media_item_id {
        sqlx::query("UPDATE media_item SET provider_id=?,title=(SELECT title FROM media WHERE id=?),status='ready',updated_at=datetime('now') WHERE id=?")
            .bind(format!("luma:{}", requested.to_ascii_uppercase()))
            .bind(media_id)
            .bind(legacy_media_item_id)
            .execute(&mut *transaction)
            .await?;
    }
    transaction.commit().await?;
    crate::export::regenerate_library_item(
        &state.pool,
        id,
        crate::export::ExportOptions {
            overwrite_nfo: true,
            overwrite_artwork: false,
        },
    )
    .await?;
    let item = sqlx::query("SELECT li.*, m.normalized_code, m.title, m.poster_url, m.release_date, m.metadata_status, mi.filename AS legacy_filename, mi.provider_id AS legacy_provider_id FROM library_item li JOIN media m ON m.id = li.media_id LEFT JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.id = ?")
        .bind(id)
        .fetch_one(&state.pool)
        .await?;
    Ok(Json(json!({
        "item": library_json(&item),
        "message": format!("已按 {} 重新匹配并生成 NFO", requested.to_ascii_uppercase()),
    })))
}

async fn list_attention(
    State(state): State<AppState>,
    Query(params): Query<PageParams>,
) -> AppResult<Json<Paged<Value>>> {
    if let Err(error) = reconcile_active(&state).await {
        tracing::warn!(%error, "attention list reconciliation failed");
    }
    let total: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM attention_item ai WHERE ai.status = 'open'")
            .fetch_one(&state.pool)
            .await?;
    let rows = sqlx::query("SELECT ai.*, m.title AS media_title, m.normalized_code FROM attention_item ai LEFT JOIN media m ON m.id = ai.media_id WHERE ai.status = 'open' ORDER BY CASE ai.severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END, ai.id DESC LIMIT ? OFFSET ?")
        .bind(params.limit())
        .bind(params.offset())
        .fetch_all(&state.pool)
        .await?;
    Ok(Json(Paged::new(
        rows.iter().map(attention_json).collect(),
        total,
        params,
    )))
}

async fn attention_detail(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Value>> {
    let row = sqlx::query("SELECT ai.*, m.title AS media_title, m.normalized_code FROM attention_item ai LEFT JOIN media m ON m.id = ai.media_id WHERE ai.id = ?").bind(id).fetch_optional(&state.pool).await?.ok_or(AppError::NotFound)?;
    Ok(Json(attention_json(&row)))
}

#[derive(Debug, Deserialize)]
struct AttentionAction {
    action: String,
}
async fn attention_action(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
    Json(input): Json<AttentionAction>,
) -> AppResult<Json<Value>> {
    let row =
        sqlx::query("SELECT acquisition_id, status, actions_json FROM attention_item WHERE id = ?")
            .bind(id)
            .fetch_optional(&state.pool)
            .await?
            .ok_or(AppError::NotFound)?;
    if row.get::<String, _>("status") != "open" {
        return Ok(Json(
            json!({"id": id, "resolved": true, "alreadyResolved": true, "action": input.action}),
        ));
    }
    let allowed: Vec<String> =
        serde_json::from_str(&row.get::<String, _>("actions_json")).unwrap_or_default();
    if !allowed.iter().any(|action| action == &input.action) {
        return Err(AppError::BadRequest("该问题不支持此操作".into()));
    }
    let acquisition_id: Option<i64> = row.get("acquisition_id");
    match (input.action.as_str(), acquisition_id) {
        ("retry", Some(acquisition_id)) => {
            let _ = retry_acquisition(State(state.clone()), AxumPath(acquisition_id)).await?;
        }
        ("cancel", Some(acquisition_id)) => {
            let _ = cancel_acquisition(State(state.clone()), AxumPath(acquisition_id)).await?;
        }
        ("acquire", _) => {
            let media_id = sqlx::query_scalar::<_, Option<i64>>(
                "SELECT media_id FROM attention_item WHERE id = ?",
            )
            .bind(id)
            .fetch_one(&state.pool)
            .await?
            .ok_or_else(|| AppError::BadRequest("缺少媒体记录".into()))?;
            let acquisition = request_acquisition(
                &state,
                AcquireInput {
                    media_id: Some(media_id),
                    resource_id: None,
                    requested_by: "automation-confirm".into(),
                },
            )
            .await?;
            sqlx::query("UPDATE attention_item SET status = 'resolved', resolution_json = ?, resolved_at = datetime('now'), updated_at = datetime('now') WHERE id = ?").bind(json!({"action":"acquire","acquisitionId":acquisition.id}).to_string()).bind(id).execute(&state.pool).await?;
        }
        ("dismiss", _) => {
            sqlx::query("UPDATE attention_item SET status = 'resolved', resolution_json = '{\"action\":\"dismiss\"}', resolved_at = datetime('now'), updated_at = datetime('now') WHERE id = ?").bind(id).execute(&state.pool).await?;
        }
        _ => return Err(AppError::BadRequest("缺少可操作的获取记录".into())),
    }
    Ok(Json(
        json!({"id": id, "resolved": true, "action": input.action}),
    ))
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct AutomationInput {
    name: String,
    #[serde(default = "enabled_default")]
    enabled: bool,
    trigger_type: String,
    #[serde(default)]
    trigger_config: Value,
    #[serde(default)]
    conditions: Value,
    #[serde(default = "acquire_action")]
    action_type: String,
    #[serde(default)]
    action_config: Value,
    #[serde(default = "confirm_mode")]
    mode: String,
}
fn enabled_default() -> bool {
    true
}
fn acquire_action() -> String {
    "ACQUIRE".into()
}
fn confirm_mode() -> String {
    "CONFIRM".into()
}

async fn list_automations(
    State(state): State<AppState>,
    Query(params): Query<PageParams>,
) -> AppResult<Json<Paged<Value>>> {
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM automation_rule ar")
        .fetch_one(&state.pool)
        .await?;
    let rows = sqlx::query("SELECT ar.*, (SELECT status FROM automation_execution ae WHERE ae.rule_id = ar.id ORDER BY ae.id DESC LIMIT 1) AS last_status, (SELECT explanation FROM automation_execution ae WHERE ae.rule_id = ar.id ORDER BY ae.id DESC LIMIT 1) AS last_explanation FROM automation_rule ar ORDER BY ar.enabled DESC, ar.id DESC LIMIT ? OFFSET ?")
        .bind(params.limit())
        .bind(params.offset())
        .fetch_all(&state.pool)
        .await?;
    Ok(Json(Paged::new(
        rows.iter().map(automation_json).collect(),
        total,
        params,
    )))
}

async fn create_automation(
    State(state): State<AppState>,
    Json(input): Json<AutomationInput>,
) -> AppResult<Json<Value>> {
    validate_automation(&input)?;
    let id = sqlx::query("INSERT INTO automation_rule(name, enabled, trigger_type, trigger_config_json, conditions_json, action_type, action_config_json, mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(input.name.trim()).bind(input.enabled).bind(&input.trigger_type).bind(input.trigger_config.to_string()).bind(input.conditions.to_string()).bind(&input.action_type).bind(input.action_config.to_string()).bind(&input.mode).execute(&state.pool).await?.last_insert_rowid();
    automation_by_id(&state, id).await
}

async fn update_automation(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
    Json(input): Json<AutomationInput>,
) -> AppResult<Json<Value>> {
    validate_automation(&input)?;
    let result = sqlx::query("UPDATE automation_rule SET name = ?, enabled = ?, trigger_type = ?, trigger_config_json = ?, conditions_json = ?, action_type = ?, action_config_json = ?, mode = ?, updated_at = datetime('now') WHERE id = ?")
        .bind(input.name.trim()).bind(input.enabled).bind(&input.trigger_type).bind(input.trigger_config.to_string()).bind(input.conditions.to_string()).bind(&input.action_type).bind(input.action_config.to_string()).bind(&input.mode).bind(id).execute(&state.pool).await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound);
    }
    automation_by_id(&state, id).await
}

async fn delete_automation(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Value>> {
    let result = sqlx::query("DELETE FROM automation_rule WHERE id = ?")
        .bind(id)
        .execute(&state.pool)
        .await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound);
    }
    Ok(Json(json!({"deleted": true})))
}

#[derive(Debug, Deserialize)]
struct EnabledInput {
    enabled: bool,
}
async fn set_automation_enabled(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
    Json(input): Json<EnabledInput>,
) -> AppResult<Json<Value>> {
    let result = sqlx::query(
        "UPDATE automation_rule SET enabled = ?, updated_at = datetime('now') WHERE id = ?",
    )
    .bind(input.enabled)
    .bind(id)
    .execute(&state.pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound);
    }
    automation_by_id(&state, id).await
}

fn validate_automation(input: &AutomationInput) -> AppResult<()> {
    if input.name.trim().is_empty() {
        return Err(AppError::BadRequest("规则名称不能为空".into()));
    }
    if !matches!(input.mode.as_str(), "AUTO" | "CONFIRM" | "NOTIFY") {
        return Err(AppError::BadRequest(
            "mode 必须是 AUTO、CONFIRM 或 NOTIFY".into(),
        ));
    }
    if input.trigger_type.trim().is_empty() {
        return Err(AppError::BadRequest("WHEN 触发器不能为空".into()));
    }
    Ok(())
}

async fn automation_by_id(state: &AppState, id: i64) -> AppResult<Json<Value>> {
    let row = sqlx::query("SELECT ar.*, NULL AS last_status, NULL AS last_explanation FROM automation_rule ar WHERE id = ?").bind(id).fetch_optional(&state.pool).await?.ok_or(AppError::NotFound)?;
    Ok(Json(automation_json(&row)))
}

async fn list_providers(
    State(state): State<AppState>,
    Query(params): Query<PageParams>,
) -> AppResult<Json<Paged<Value>>> {
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_config pc")
        .fetch_one(&state.pool)
        .await?;
    let rows = sqlx::query("SELECT pc.*,ss.status AS sync_status,ss.active_mode AS sync_active_mode,ss.bootstrap_paused AS sync_bootstrap_paused,ss.bootstrap_from AS sync_bootstrap_from,ss.bootstrap_to AS sync_bootstrap_to,ss.cursor_json AS sync_cursor_json,ss.last_started_at AS sync_last_started_at,ss.last_finished_at AS sync_last_finished_at,ss.last_success_at AS sync_last_success_at,ss.next_run_at AS sync_next_run_at,ss.last_message AS sync_last_message,ss.failure_count AS sync_failure_count,ss.item_count AS sync_item_count,ss.inserted_count AS sync_inserted_count,ss.updated_count AS sync_updated_count,ss.discovery_count AS sync_discovery_count,ss.hydrated_count AS sync_hydrated_count,ss.hydration_failed_count AS sync_hydration_failed_count,ss.pending_count AS sync_pending_count FROM provider_config pc LEFT JOIN source_sync_state ss ON ss.provider_key=pc.provider_key ORDER BY pc.provider_type,pc.provider_key LIMIT ? OFFSET ?")
        .bind(params.limit())
        .bind(params.offset())
        .fetch_all(&state.pool)
        .await?;
    Ok(Json(Paged::new(
        rows.iter().map(provider_json).collect(),
        total,
        params,
    )))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProviderInput {
    #[serde(default)]
    display_name: String,
    base_url: String,
    #[serde(default)]
    secret: String,
    #[serde(default)]
    config: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateProviderInput {
    display_name: String,
    base_url: String,
    #[serde(default)]
    secret: String,
    #[serde(default = "default_source_adapter")]
    adapter: String,
    #[serde(default)]
    config: Value,
}

fn default_source_adapter() -> String {
    "jav321".into()
}

async fn create_provider(
    State(state): State<AppState>,
    Json(input): Json<CreateProviderInput>,
) -> AppResult<Json<Value>> {
    if input.display_name.trim().is_empty() {
        return Err(AppError::BadRequest("来源名称不能为空".into()));
    }
    let adapter = input.adapter.trim().to_ascii_lowercase();
    if !state.provider_registry.contains(&adapter) {
        return Err(AppError::BadRequest(format!(
            "不支持的来源适配器：{}",
            input.adapter
        )));
    }
    validate_http_url(&input.base_url)?;
    validate_source_transport_config(&input.config)?;
    let mut config = input.config.as_object().cloned().unwrap_or_default();
    config.insert("adapter".into(), Value::String(adapter.clone()));
    config.entry("fetchMode").or_insert_with(|| {
        Value::String(
            match state.provider_registry.default_fetch_mode(&adapter) {
                Some(crate::fetch::FetchMode::Browser) => "browser",
                Some(crate::fetch::FetchMode::Auto) => "auto",
                _ => "http",
            }
            .into(),
        )
    });
    let mut suffix = 1_i64;
    let key = loop {
        let candidate = if suffix == 1 {
            adapter.clone()
        } else {
            format!("{adapter}-{suffix}")
        };
        let exists: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM provider_config WHERE provider_key = ?)",
        )
        .bind(&candidate)
        .fetch_one(&state.pool)
        .await?;
        if exists == 0 {
            break candidate;
        }
        suffix += 1;
    };
    sqlx::query("INSERT INTO provider_config(provider_key, provider_type, display_name, enabled, base_url, secret, config_json) VALUES (?, 'source', ?, 1, ?, ?, ?)")
        .bind(&key)
        .bind(input.display_name.trim())
        .bind(input.base_url.trim())
        .bind(input.secret.trim())
        .bind(Value::Object(config).to_string())
        .execute(&state.pool)
        .await?;
    sqlx::query("INSERT OR IGNORE INTO source_sync_state(provider_key) VALUES (?)")
        .bind(&key)
        .execute(&state.pool)
        .await?;
    provider_by_key(&state, &key).await
}

async fn update_provider(
    State(state): State<AppState>,
    AxumPath(key): AxumPath<String>,
    Json(input): Json<ProviderInput>,
) -> AppResult<Json<Value>> {
    validate_http_url(&input.base_url)?;
    validate_source_transport_config(&input.config)?;
    let display_name = input.display_name.trim();
    let result = if input.secret.trim().is_empty() {
        sqlx::query("UPDATE provider_config SET display_name = CASE WHEN ? = '' THEN display_name ELSE ? END, base_url = ?, config_json = ?, updated_at = datetime('now') WHERE provider_key = ?").bind(display_name).bind(display_name).bind(input.base_url.trim()).bind(input.config.to_string()).bind(&key).execute(&state.pool).await?
    } else {
        sqlx::query("UPDATE provider_config SET display_name = CASE WHEN ? = '' THEN display_name ELSE ? END, base_url = ?, secret = ?, config_json = ?, updated_at = datetime('now') WHERE provider_key = ?").bind(display_name).bind(display_name).bind(input.base_url.trim()).bind(&input.secret).bind(input.config.to_string()).bind(&key).execute(&state.pool).await?
    };
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound);
    }
    sync_provider_legacy_setting(&state, &key, input.base_url.trim(), input.secret.trim()).await?;
    provider_by_key(&state, &key).await
}

async fn delete_provider(
    State(state): State<AppState>,
    AxumPath(key): AxumPath<String>,
) -> AppResult<Json<Value>> {
    let result = sqlx::query(
        "DELETE FROM provider_config WHERE provider_key = ? AND provider_type = 'source'",
    )
    .bind(&key)
    .execute(&state.pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound);
    }
    Ok(Json(json!({"deleted": true})))
}

async fn set_provider_enabled(
    State(state): State<AppState>,
    AxumPath(key): AxumPath<String>,
    Json(input): Json<EnabledInput>,
) -> AppResult<Json<Value>> {
    let result = sqlx::query("UPDATE provider_config SET enabled = ?, updated_at = datetime('now') WHERE provider_key = ?").bind(input.enabled).bind(&key).execute(&state.pool).await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound);
    }
    provider_by_key(&state, &key).await
}

async fn test_provider(
    State(state): State<AppState>,
    AxumPath(key): AxumPath<String>,
) -> AppResult<Json<Value>> {
    let started = std::time::Instant::now();
    let source = source_provider_by_key(&state, &key).await?;
    let outcome: anyhow::Result<String> = if let Some(provider) = source {
        source_probe(&state, &provider)
            .await
            .map(|_| format!("{} 可访问", provider.display_name))
    } else {
        match key.as_str() {
            "metatube" => {
                let settings = storage::load_settings(&state.pool).await?;
                crate::provider::MetaTubeClient::new(&settings)?
                    .test_connection()
                    .await
                    .map(|count| format!("MetaTube 已连接，{count} 个元数据来源"))
            }
            "qbittorrent" => {
                let settings = storage::load_settings(&state.pool).await?;
                QBittorrentClient::new(&settings)?
                    .version()
                    .await
                    .map(|version| format!("qBittorrent {version}"))
            }
            _ => return Err(AppError::NotFound),
        }
    };
    let (status, message) = match outcome {
        Ok(message) => ("online", message),
        Err(error) => ("offline", error.to_string()),
    };
    sqlx::query("UPDATE provider_config SET last_status = ?, last_message = ?, last_checked_at = datetime('now'), updated_at = datetime('now') WHERE provider_key = ?")
        .bind(status).bind(&message).bind(&key).execute(&state.pool).await?;
    Ok(Json(
        json!({"providerKey": key, "connected": status == "online", "message": message, "latencyMs": started.elapsed().as_millis()}),
    ))
}

async fn sync_provider(
    State(state): State<AppState>,
    AxumPath(key): AxumPath<String>,
) -> AppResult<Json<Value>> {
    start_incremental_run(&state, &key, true).await?;
    provider_by_key(&state, &key).await
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BootstrapInput {
    from: String,
    to: String,
    #[serde(default = "default_true")]
    include_resources: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SyncRunResponse {
    run_id: i64,
    mode: String,
    status: String,
}

fn default_true() -> bool {
    true
}

async fn sync_provider_incremental(
    State(state): State<AppState>,
    AxumPath(key): AxumPath<String>,
) -> AppResult<Json<SyncRunResponse>> {
    let run_id = start_incremental_run(&state, &key, true).await?;
    Ok(Json(SyncRunResponse {
        run_id,
        mode: SyncMode::Incremental.as_str().into(),
        status: "running".into(),
    }))
}

async fn bootstrap_provider(
    State(state): State<AppState>,
    AxumPath(key): AxumPath<String>,
    Json(input): Json<BootstrapInput>,
) -> AppResult<Json<SyncRunResponse>> {
    validate_sync_window(&input.from, &input.to)?;
    let provider = source_provider_by_key(&state, &key)
        .await?
        .ok_or(AppError::NotFound)?;
    let run_id = start_discovery_run(
        &state,
        &provider,
        SyncMode::Bootstrap,
        &input.from,
        &input.to,
        input.include_resources,
        &provider.base_url,
        1,
        5,
        true,
    )
    .await?;
    let status: String = sqlx::query_scalar("SELECT status FROM source_sync_run WHERE id=?")
        .bind(run_id)
        .fetch_one(&state.pool)
        .await?;
    Ok(Json(SyncRunResponse {
        run_id,
        mode: SyncMode::Bootstrap.as_str().into(),
        status,
    }))
}

async fn pause_provider_bootstrap(
    State(state): State<AppState>,
    AxumPath(key): AxumPath<String>,
) -> AppResult<Json<SyncRunResponse>> {
    source_provider_by_key(&state, &key)
        .await?
        .ok_or(AppError::NotFound)?;
    let run_id = sqlx::query_scalar::<_, i64>("SELECT id FROM source_sync_run WHERE provider_key=? AND sync_mode='bootstrap' AND status='running' ORDER BY id DESC LIMIT 1")
        .bind(&key)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| AppError::BadRequest("当前没有正在运行的历史回填".into()))?;
    sqlx::query("UPDATE source_sync_state SET bootstrap_paused=1,last_message='历史回填将在当前批次后暂停',updated_at=datetime('now') WHERE provider_key=?")
        .bind(&key)
        .execute(&state.pool)
        .await?;
    Ok(Json(SyncRunResponse {
        run_id,
        mode: SyncMode::Bootstrap.as_str().into(),
        status: "pausing".into(),
    }))
}

async fn resume_provider_bootstrap(
    State(state): State<AppState>,
    AxumPath(key): AxumPath<String>,
) -> AppResult<Json<SyncRunResponse>> {
    let provider = source_provider_by_key(&state, &key)
        .await?
        .ok_or(AppError::NotFound)?;
    let row = sqlx::query("SELECT bootstrap_from,bootstrap_to,cursor_json FROM source_sync_state WHERE provider_key=?")
        .bind(&key)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| AppError::BadRequest("这个来源还没有历史回填 checkpoint".into()))?;
    let from = row
        .get::<Option<String>, _>("bootstrap_from")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AppError::BadRequest("这个来源还没有历史回填范围".into()))?;
    let to = row
        .get::<Option<String>, _>("bootstrap_to")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AppError::BadRequest("这个来源还没有历史回填范围".into()))?;
    let cursor = serde_json::from_str::<Value>(&row.get::<String, _>("cursor_json"))
        .unwrap_or_else(|_| json!({}));
    let next_url = cursor
        .get("nextUrl")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let has_more = cursor
        .get("hasMore")
        .and_then(Value::as_bool)
        .unwrap_or(next_url.is_some());
    let page_url = next_url
        .clone()
        .unwrap_or_else(|| provider.base_url.clone());
    let page = cursor.get("page").and_then(Value::as_i64).unwrap_or(1);
    let include_resources = cursor
        .get("includeResources")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let active_run = sqlx::query_scalar::<_, i64>("SELECT id FROM source_sync_run WHERE provider_key=? AND sync_mode='bootstrap' AND status='running' ORDER BY id DESC LIMIT 1")
        .bind(&key)
        .fetch_optional(&state.pool)
        .await?;
    let run_id = if let Some(run_id) = active_run {
        sqlx::query("UPDATE source_sync_state SET bootstrap_paused=0,last_message='历史回填已继续',updated_at=datetime('now') WHERE provider_key=?")
            .bind(&key)
            .execute(&state.pool)
            .await?;
        if has_more {
            enqueue_discovery_job(
                &state,
                &provider,
                DiscoveryJobPayload {
                    run_id,
                    mode: SyncMode::Bootstrap,
                    page_url,
                    page,
                    window_from: from,
                    window_to: to,
                    include_resources,
                    pages_remaining: 5,
                },
            )
            .await?;
        } else {
            settle_source_sync_run(&state, run_id).await?;
        }
        run_id
    } else {
        if !has_more {
            return Err(AppError::BadRequest(
                "历史回填已到达当前 checkpoint 末尾".into(),
            ));
        }
        start_discovery_run(
            &state,
            &provider,
            SyncMode::Bootstrap,
            &from,
            &to,
            include_resources,
            &page_url,
            page,
            5,
            false,
        )
        .await?
    };
    let status: String = sqlx::query_scalar("SELECT status FROM source_sync_run WHERE id=?")
        .bind(run_id)
        .fetch_one(&state.pool)
        .await?;
    Ok(Json(SyncRunResponse {
        run_id,
        mode: SyncMode::Bootstrap.as_str().into(),
        status,
    }))
}

pub async fn schedule_source_sync(state: &AppState) -> anyhow::Result<()> {
    let rows = sqlx::query("SELECT pc.* FROM provider_config pc JOIN source_sync_state ss ON ss.provider_key=pc.provider_key WHERE pc.provider_type='source' AND pc.enabled=1 AND ss.status!='running' AND ss.next_run_at<=datetime('now') ORDER BY ss.next_run_at,pc.provider_key LIMIT 8")
        .fetch_all(&state.pool)
        .await?;
    for provider in rows.iter().map(source_provider_from_row) {
        if provider.sync_enabled {
            if let Err(error) = start_incremental_run(state, &provider.key, false).await {
                if !matches!(&error, AppError::BadRequest(message) if message.contains("正在运行"))
                {
                    return Err(error.into());
                }
            }
        }
    }
    Ok(())
}

pub(crate) async fn recover_interrupted_source_syncs(state: &AppState) -> anyhow::Result<()> {
    let mut transaction = state.pool.begin().await?;
    sqlx::query("UPDATE source_sync_run SET status='failed',error_message='worker interrupted by service restart',finished_at=datetime('now') WHERE status='running' AND ingestion_job_id IS NOT NULL")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("UPDATE source_sync_state SET status='idle',last_finished_at=datetime('now'),last_message='上次同步被服务重启中断，已重新排队',lease_owner=NULL,lease_expires_at=NULL,updated_at=datetime('now') WHERE status='running' AND NOT EXISTS(SELECT 1 FROM source_sync_run run WHERE run.provider_key=source_sync_state.provider_key AND run.status='running')")
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;

    let stranded = sqlx::query("SELECT id,provider_key FROM source_sync_run run WHERE status='running' AND ingestion_job_id IS NULL AND NOT EXISTS(SELECT 1 FROM ingestion_job job WHERE job.status IN ('pending','running') AND job.job_type IN ('discovery','hydrate') AND json_extract(job.payload_json,'$.runId')=run.id)")
        .fetch_all(&state.pool)
        .await?;
    for row in stranded {
        let run_id: i64 = row.get("id");
        settle_source_sync_run(state, run_id).await?;
    }
    Ok(())
}

async fn start_incremental_run(state: &AppState, key: &str, manual: bool) -> AppResult<i64> {
    let provider = source_provider_by_key(state, key)
        .await?
        .ok_or(AppError::NotFound)?;
    if !manual && !provider.sync_enabled {
        return Ok(0);
    }
    let today = chrono::Utc::now().date_naive();
    let from = today - chrono::Duration::days(provider.sync_overlap_days);
    start_discovery_run(
        state,
        &provider,
        SyncMode::Incremental,
        &from.format("%Y-%m-%d").to_string(),
        &today.format("%Y-%m-%d").to_string(),
        true,
        &provider.base_url,
        1,
        5,
        true,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn start_discovery_run(
    state: &AppState,
    provider: &SourceProviderConfig,
    mode: SyncMode,
    window_from: &str,
    window_to: &str,
    include_resources: bool,
    page_url: &str,
    page: i64,
    pages_remaining: i64,
    reset_cursor: bool,
) -> AppResult<i64> {
    validate_sync_window(window_from, window_to)?;
    validate_provider_page_url(provider, page_url)?;
    sqlx::query("INSERT OR IGNORE INTO source_sync_state(provider_key) VALUES (?)")
        .bind(&provider.key)
        .execute(&state.pool)
        .await?;
    let mut transaction = state.pool.begin().await?;
    let active: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM source_sync_run WHERE provider_key=? AND status='running')",
    )
    .bind(&provider.key)
    .fetch_one(&mut *transaction)
    .await?;
    if active != 0 {
        return Err(AppError::BadRequest(
            "这个来源已有正在运行的同步批次".into(),
        ));
    }
    let cursor_before: String =
        sqlx::query_scalar("SELECT cursor_json FROM source_sync_state WHERE provider_key=?")
            .bind(&provider.key)
            .fetch_one(&mut *transaction)
            .await?;
    let initial_cursor = json!({
        "mode": mode.as_str(),
        "nextUrl": page_url,
        "page": page,
        "from": window_from,
        "to": window_to,
        "includeResources": include_resources,
        "hasMore": true,
    })
    .to_string();
    if mode == SyncMode::Bootstrap {
        sqlx::query("UPDATE source_sync_state SET status='running',active_mode='bootstrap',bootstrap_paused=0,bootstrap_from=?,bootstrap_to=?,last_started_at=datetime('now'),last_finished_at=NULL,last_message='正在发现历史目录',discovery_count=0,hydrated_count=0,hydration_failed_count=0,pending_count=0,cursor_json=CASE WHEN ? THEN ? ELSE cursor_json END,updated_at=datetime('now') WHERE provider_key=?")
            .bind(window_from)
            .bind(window_to)
            .bind(reset_cursor)
            .bind(&initial_cursor)
            .bind(&provider.key)
            .execute(&mut *transaction)
            .await?;
    } else {
        sqlx::query("UPDATE source_sync_state SET status='running',active_mode='incremental',overlap_days=?,last_started_at=datetime('now'),last_finished_at=NULL,last_message='正在发现增量目录',discovery_count=0,hydrated_count=0,hydration_failed_count=0,pending_count=0,updated_at=datetime('now') WHERE provider_key=?")
            .bind(provider.sync_overlap_days)
            .bind(&provider.key)
            .execute(&mut *transaction)
            .await?;
    }
    let run_id: i64 = sqlx::query("INSERT INTO source_sync_run(provider_key,status,cursor_before_json,watermark_before_json,sync_mode,window_from,window_to) SELECT provider_key,'running',?,watermark_json,?,?,? FROM source_sync_state WHERE provider_key=? RETURNING id")
        .bind(&cursor_before)
        .bind(mode.as_str())
        .bind(window_from)
        .bind(window_to)
        .bind(&provider.key)
        .fetch_one(&mut *transaction)
        .await?
        .get("id");
    transaction.commit().await?;

    let payload = DiscoveryJobPayload {
        run_id,
        mode,
        page_url: page_url.to_owned(),
        page,
        window_from: window_from.to_owned(),
        window_to: window_to.to_owned(),
        include_resources,
        pages_remaining: pages_remaining.max(1),
    };
    if let Err(error) = enqueue_discovery_job(state, provider, payload).await {
        let message = error.to_string();
        let _ = sqlx::query("UPDATE source_sync_run SET status='failed',error_message=?,finished_at=datetime('now') WHERE id=?")
            .bind(&message)
            .bind(run_id)
            .execute(&state.pool)
            .await;
        let _ = sqlx::query("UPDATE source_sync_state SET status='failed',last_finished_at=datetime('now'),last_message=?,updated_at=datetime('now') WHERE provider_key=?")
            .bind(&message)
            .bind(&provider.key)
            .execute(&state.pool)
            .await;
        return Err(error);
    }
    Ok(run_id)
}

async fn enqueue_discovery_job(
    state: &AppState,
    provider: &SourceProviderConfig,
    payload: DiscoveryJobPayload,
) -> AppResult<i64> {
    validate_provider_page_url(provider, &payload.page_url)?;
    let dedupe_key = format!("discovery:{}:{}", payload.run_id, payload.page);
    let priority = match payload.mode {
        SyncMode::Bootstrap => PRIORITY_HISTORICAL_BOOTSTRAP,
        SyncMode::Incremental => PRIORITY_DAILY_INCREMENTAL,
        SyncMode::OnDemand => PRIORITY_USER_ON_DEMAND,
    };
    let enqueued = state
        .ingestion_queue
        .enqueue(EnqueueJob {
            provider_key: &provider.key,
            job_type: "discovery",
            priority,
            payload: serde_json::to_value(&payload).map_err(anyhow::Error::from)?,
            max_attempts: 3,
            dedupe_key: Some(&dedupe_key),
        })
        .await?;
    Ok(enqueued.id)
}

fn validate_sync_window(from: &str, to: &str) -> AppResult<()> {
    let from_date = chrono::NaiveDate::parse_from_str(from, "%Y-%m-%d")
        .map_err(|_| AppError::BadRequest("from 必须是 YYYY-MM-DD".into()))?;
    let to_date = chrono::NaiveDate::parse_from_str(to, "%Y-%m-%d")
        .map_err(|_| AppError::BadRequest("to 必须是 YYYY-MM-DD".into()))?;
    if from_date > to_date {
        return Err(AppError::BadRequest("from 不能晚于 to".into()));
    }
    Ok(())
}

fn resource_date_is_recent(release_date: &str, recent_days: i64) -> bool {
    chrono::NaiveDate::parse_from_str(release_date, "%Y-%m-%d")
        .ok()
        .is_some_and(|date| {
            date >= chrono::Utc::now().date_naive()
                - chrono::Duration::days(recent_days.clamp(1, 3650))
        })
}

fn validate_provider_page_url(
    provider: &SourceProviderConfig,
    page_url: &str,
) -> AppResult<reqwest::Url> {
    let base = reqwest::Url::parse(&provider.base_url)
        .map_err(|error| AppError::BadRequest(error.to_string()))?;
    let url = reqwest::Url::parse(page_url)
        .or_else(|_| base.join(page_url))
        .map_err(|error| AppError::BadRequest(error.to_string()))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str() != base.host_str()
        || url.port_or_known_default() != base.port_or_known_default()
    {
        return Err(AppError::BadRequest(
            "目录分页 URL 必须与来源使用同一站点".into(),
        ));
    }
    Ok(url)
}

#[derive(Debug)]
struct SourceSyncStats {
    item_count: i64,
    inserted_count: i64,
    updated_count: i64,
    detail_count: usize,
    detail_failures: usize,
}

pub(crate) async fn execute_source_sync_job(
    state: &AppState,
    key: &str,
    manual: bool,
    ingestion_job_id: i64,
) -> anyhow::Result<()> {
    let provider = source_provider_by_key(state, key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("source provider {key} no longer exists"))?;
    if !manual && !provider.sync_enabled {
        return Ok(());
    }
    let mut transaction = state.pool.begin().await?;
    let source_lease_owner = format!("ingestion-job:{ingestion_job_id}");
    sqlx::query("UPDATE source_sync_run SET status='failed',error_message='previous attempt did not finish',finished_at=datetime('now') WHERE status='running' AND ingestion_job_id=?")
        .bind(ingestion_job_id)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("UPDATE source_sync_state SET status='idle',last_message='正在恢复未完成的同步任务',lease_owner=NULL,lease_expires_at=NULL,updated_at=datetime('now') WHERE provider_key=? AND status='running' AND lease_owner=?")
        .bind(key)
        .bind(&source_lease_owner)
        .execute(&mut *transaction)
        .await?;
    let claimed = sqlx::query("UPDATE source_sync_state SET status='running',last_started_at=datetime('now'),last_finished_at=NULL,last_message='正在采集最新目录',lease_owner=?,lease_expires_at=datetime('now','+2 minutes'),updated_at=datetime('now') WHERE provider_key=? AND status!='running'")
        .bind(source_lease_owner)
        .bind(key)
        .execute(&mut *transaction)
        .await?;
    anyhow::ensure!(
        claimed.rows_affected() == 1,
        "source provider {key} is already synchronizing"
    );
    let run_id: i64 = sqlx::query("INSERT INTO source_sync_run(provider_key,status,cursor_before_json,watermark_before_json,ingestion_job_id) SELECT provider_key,'running',cursor_json,watermark_json,? FROM source_sync_state WHERE provider_key=? RETURNING id")
        .bind(ingestion_job_id)
        .bind(key)
        .fetch_one(&mut *transaction)
        .await?
        .get("id");
    transaction.commit().await?;

    let permit = match state.crawler_limiter.clone().acquire_owned().await {
        Ok(permit) => permit,
        Err(error) => {
            finish_source_sync_failure(state, &provider, run_id, &error.to_string()).await?;
            return Err(error.into());
        }
    };
    let outcome = synchronize_source_catalogue(state, &provider).await;
    drop(permit);
    match outcome {
        Ok(stats) => finish_source_sync_success(state, &provider, run_id, &stats).await,
        Err(error) => {
            tracing::warn!(%error, provider = provider.key, "source catalogue sync failed");
            if let Err(finish_error) =
                finish_source_sync_failure(state, &provider, run_id, &error.to_string()).await
            {
                tracing::error!(%finish_error, provider = provider.key, "could not persist source sync failure");
            }
            Err(error)
        }
    }
}

pub(crate) async fn execute_discovery_job(
    state: &AppState,
    payload: &DiscoveryJobPayload,
) -> anyhow::Result<()> {
    let run_status =
        sqlx::query_scalar::<_, String>("SELECT status FROM source_sync_run WHERE id=?")
            .bind(payload.run_id)
            .fetch_optional(&state.pool)
            .await?;
    if run_status.as_deref() != Some("running") {
        return Ok(());
    }
    let page_completed: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM source_sync_page WHERE run_id=? AND page_number=?)",
    )
    .bind(payload.run_id)
    .bind(payload.page)
    .fetch_one(&state.pool)
    .await?;
    if page_completed != 0 {
        return Ok(());
    }
    if payload.mode == SyncMode::Bootstrap {
        let paused: i64 = sqlx::query_scalar(
            "SELECT bootstrap_paused FROM source_sync_state WHERE provider_key=?",
        )
        .bind(
            sqlx::query_scalar::<_, String>("SELECT provider_key FROM source_sync_run WHERE id=?")
                .bind(payload.run_id)
                .fetch_one(&state.pool)
                .await?,
        )
        .fetch_one(&state.pool)
        .await?;
        if paused != 0 {
            return Ok(());
        }
    }
    let provider_key: String =
        sqlx::query_scalar("SELECT provider_key FROM source_sync_run WHERE id=?")
            .bind(payload.run_id)
            .fetch_one(&state.pool)
            .await?;
    let provider = source_provider_by_key(state, &provider_key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("source provider {provider_key} no longer exists"))?;
    let page = fetch_source_catalogue_page(state, &provider, &payload.page_url).await?;
    let reached_start = crate::ingestion::reached_window_start(&page.items, &payload.window_from);
    let candidates = crate::ingestion::upsert_candidates(
        &state.pool,
        &provider.key,
        page.items
            .into_iter()
            .filter(|item| {
                crate::ingestion::within_window(item, &payload.window_from, &payload.window_to)
            })
            .collect(),
    )
    .await?;
    let priority = match payload.mode {
        SyncMode::Bootstrap => PRIORITY_HISTORICAL_BOOTSTRAP,
        SyncMode::Incremental => PRIORITY_DAILY_INCREMENTAL,
        SyncMode::OnDemand => PRIORITY_USER_ON_DEMAND,
    };
    let mut pending_count = 0_i64;
    for candidate in &candidates {
        if !candidate.should_hydrate {
            if payload.mode == SyncMode::Incremental
                && let Some(media_id) = candidate.media_id
                && state
                    .provider_registry
                    .resource_provider(&provider.adapter)
                    .is_some()
                && crate::resource::cache_needs_refresh(&state.pool, media_id, &provider.key)
                    .await?
            {
                enqueue_resource_refresh_job(
                    state,
                    ResourceRefreshJobPayload {
                        media_id,
                        provider_key: provider.key.clone(),
                        force: false,
                    },
                    PRIORITY_DAILY_INCREMENTAL,
                )
                .await?;
            }
            continue;
        }
        let hydration = HydrationJobPayload {
            run_id: payload.run_id,
            discovery_item_id: candidate.id,
            content_hash: candidate.content_hash.clone(),
            mode: payload.mode,
            include_resources: payload.include_resources,
        };
        let dedupe_key = format!(
            "hydrate:{}:{}:{}",
            provider.key, candidate.id, candidate.content_hash
        );
        state
            .ingestion_queue
            .enqueue(EnqueueJob {
                provider_key: &provider.key,
                job_type: "hydrate",
                priority,
                payload: serde_json::to_value(hydration)?,
                max_attempts: 3,
                dedupe_key: Some(&dedupe_key),
            })
            .await?;
        pending_count += 1;
    }
    let inserted_count = candidates.iter().filter(|item| item.inserted).count() as i64;
    let updated_count = candidates.len() as i64 - inserted_count;
    let actual_next = if !reached_start { page.next_url } else { None };
    let next_cursor = json!({
        "mode": payload.mode.as_str(),
        "nextUrl": actual_next.clone(),
        "page": payload.page + 1,
        "from": payload.window_from,
        "to": payload.window_to,
        "includeResources": payload.include_resources,
        "hasMore": actual_next.is_some(),
    });
    let watermark_after = json!({
        "from": payload.window_from,
        "to": payload.window_to,
    });
    if let Some(next_url) = actual_next.as_deref()
        && payload.pages_remaining > 1
    {
        let can_continue = if payload.mode == SyncMode::Bootstrap {
            sqlx::query_scalar::<_, i64>(
                "SELECT bootstrap_paused FROM source_sync_state WHERE provider_key=?",
            )
            .bind(&provider.key)
            .fetch_one(&state.pool)
            .await?
                == 0
        } else {
            true
        };
        if can_continue {
            enqueue_discovery_job(
                state,
                &provider,
                DiscoveryJobPayload {
                    run_id: payload.run_id,
                    mode: payload.mode,
                    page_url: next_url.to_owned(),
                    page: payload.page + 1,
                    window_from: payload.window_from.clone(),
                    window_to: payload.window_to.clone(),
                    include_resources: payload.include_resources,
                    pages_remaining: payload.pages_remaining - 1,
                },
            )
            .await?;
        }
    }
    let mut transaction = state.pool.begin().await?;
    let recorded = sqlx::query("INSERT OR IGNORE INTO source_sync_page(run_id,page_number,page_url,item_count,inserted_count,updated_count,cursor_after_json) VALUES (?,?,?,?,?,?,?)")
        .bind(payload.run_id)
        .bind(payload.page)
        .bind(&payload.page_url)
        .bind(candidates.len() as i64)
        .bind(inserted_count)
        .bind(updated_count)
        .bind(next_cursor.to_string())
        .execute(&mut *transaction)
        .await?;
    if recorded.rows_affected() == 0 {
        transaction.rollback().await?;
        return Ok(());
    }
    sqlx::query("UPDATE source_sync_run SET item_count=item_count+?,inserted_count=inserted_count+?,updated_count=updated_count+?,discovery_count=discovery_count+?,pending_count=pending_count+?,cursor_after_json=?,watermark_after_json=? WHERE id=? AND status='running'")
        .bind(candidates.len() as i64)
        .bind(inserted_count)
        .bind(updated_count)
        .bind(candidates.len() as i64)
        .bind(pending_count)
        .bind(next_cursor.to_string())
        .bind(watermark_after.to_string())
        .bind(payload.run_id)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("UPDATE source_sync_state SET cursor_json=?,last_message=?,discovery_count=discovery_count+?,pending_count=pending_count+?,updated_at=datetime('now') WHERE provider_key=?")
        .bind(next_cursor.to_string())
        .bind(format!("已发现 {} 项，等待详情补全", candidates.len()))
        .bind(candidates.len() as i64)
        .bind(pending_count)
        .bind(&provider.key)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn execute_hydration_job(
    state: &AppState,
    payload: &HydrationJobPayload,
) -> anyhow::Result<()> {
    let item = crate::ingestion::start_hydration(&state.pool, payload.discovery_item_id).await?;
    if item.content_hash != payload.content_hash {
        sqlx::query("UPDATE provider_discovery_item SET hydration_status='pending' WHERE id=?")
            .bind(item.id)
            .execute(&state.pool)
            .await?;
        return Ok(());
    }
    let outcome = async {
        let provider = source_provider_by_key(state, &item.provider_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("source provider {} no longer exists", item.provider_key))?;
        let source = SourceMedia {
            provider_id: item.provider_entity_id.clone(),
            code: item.normalized_code.clone(),
            title: item.title_hint.clone(),
            poster_url: item.poster_hint.clone(),
            source_url: item.source_url.clone(),
            release_date: item.release_date.clone(),
        };
        let media_id = persist_source_media(state, &provider, &source).await?;
        let allow_resource_endpoint = payload.mode != SyncMode::Bootstrap
            || item.release_date.as_deref().is_some_and(|release_date| {
                resource_date_is_recent(release_date, provider.resource_hydration_recent_days)
            });
        match provider.adapter.as_str() {
            "javbus" => {
                refresh_javbus_media(
                    state,
                    media_id,
                    &provider,
                    &item.provider_entity_id,
                    &item.source_url,
                    payload.include_resources,
                    allow_resource_endpoint,
                )
                .await?;
            }
            "javdb" | "jav321" | "javlibrary" => {
                refresh_generic_source_media(
                    state,
                    media_id,
                    &provider,
                    &item.provider_entity_id,
                    &item.source_url,
                    payload.include_resources,
                    allow_resource_endpoint,
                )
                .await?;
            }
            adapter => anyhow::bail!("不支持的来源适配器：{adapter}"),
        }
        crate::ingestion::finish_hydration(
            &state.pool,
            item.id,
            media_id,
            &item.content_hash,
        )
        .await?;
        sqlx::query("UPDATE source_sync_run SET hydrated_count=hydrated_count+1,pending_count=MAX(pending_count-1,0) WHERE id=?")
            .bind(payload.run_id)
            .execute(&state.pool)
            .await?;
        sqlx::query("UPDATE source_sync_state SET hydrated_count=hydrated_count+1,pending_count=MAX(pending_count-1,0),updated_at=datetime('now') WHERE provider_key=?")
            .bind(&item.provider_key)
            .execute(&state.pool)
            .await?;
        anyhow::Ok(())
    }
    .await;
    if let Err(error) = &outcome {
        crate::ingestion::fail_hydration(&state.pool, item.id, &error.to_string()).await?;
    }
    outcome
}

pub(crate) async fn execute_catalog_resolve_job(
    state: &AppState,
    provider_key: &str,
    payload: &CatalogResolveJobPayload,
) -> anyhow::Result<()> {
    let outcome = async {
        let provider = source_provider_by_key(state, provider_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("source provider {provider_key} no longer exists"))?;
        let (source, cached_detail) =
            fetch_provider_code_candidate(state, &provider, &payload.code).await?;
        let discovered =
            crate::ingestion::upsert_candidates(&state.pool, &provider.key, vec![source.clone()])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("provider returned no exact code candidate"))?;
        let media_id = persist_source_media(state, &provider, &source).await?;
        if let Some(detail) = cached_detail {
            let detail_source_url = detail.source_url.clone();
            persist_source_detail_html(
                state,
                media_id,
                &provider,
                &source.provider_id,
                &detail_source_url,
                &detail.body,
                "/star/",
            )
            .await?;
            if payload.include_resources {
                fetch_and_persist_resources(
                    state,
                    media_id,
                    &provider,
                    &source.provider_id,
                    &detail_source_url,
                    Some(detail),
                    true,
                )
                .await?;
            }
        } else {
            match provider.adapter.as_str() {
                "javbus" => {
                    refresh_javbus_media(
                        state,
                        media_id,
                        &provider,
                        &source.provider_id,
                        &source.source_url,
                        payload.include_resources,
                        true,
                    )
                    .await?;
                }
                "javdb" | "jav321" | "javlibrary" => {
                    refresh_generic_source_media(
                        state,
                        media_id,
                        &provider,
                        &source.provider_id,
                        &source.source_url,
                        payload.include_resources,
                        true,
                    )
                    .await?;
                }
                adapter => anyhow::bail!("unsupported source adapter: {adapter}"),
            }
        }
        crate::ingestion::finish_hydration(
            &state.pool,
            discovered.id,
            media_id,
            &discovered.content_hash,
        )
        .await?;
        refresh_media_search_document(state, media_id).await?;
        anyhow::Ok(media_id)
    }
    .await;
    match outcome {
        Ok(media_id) => {
            emit(
                state,
                "catalog-resolve",
                json!({"code":payload.code,"providerKey":provider_key,"mediaId":media_id,"status":"success"}),
            );
            Ok(())
        }
        Err(error) => {
            emit(
                state,
                "catalog-resolve",
                json!({"code":payload.code,"providerKey":provider_key,"status":"failed","message":error.to_string()}),
            );
            Err(error)
        }
    }
}

async fn fetch_provider_code_candidate(
    state: &AppState,
    provider: &SourceProviderConfig,
    code: &str,
) -> anyhow::Result<(SourceMedia, Option<RawProviderDocument>)> {
    let base = reqwest::Url::parse(provider.base_url.trim_end_matches('/'))?;
    if provider.adapter == "javbus" {
        let url = base.join(&format!("/{code}"))?;
        let response = javbus_request(state, provider, url).await?;
        ensure_provider_content(state, provider, &response, "resolve", Some(code), None).await?;
        let source_url = response.final_url.to_string();
        let provider_id = response
            .final_url
            .path_segments()
            .and_then(Iterator::last)
            .filter(|value| !value.is_empty())
            .unwrap_or(code)
            .to_owned();
        let source = SourceMedia {
            provider_id,
            code: code.to_owned(),
            title: meta_content(&response.body, "og:title").unwrap_or_else(|| code.to_owned()),
            poster_url: meta_content(&response.body, "og:image"),
            source_url: source_url.clone(),
            release_date: extract_iso_date(&response.body),
        };
        return Ok((
            source,
            Some(RawProviderDocument {
                source_url,
                body: response.body,
            }),
        ));
    }

    let mut url = match provider.adapter.as_str() {
        "javdb" => base.join("/search")?,
        "jav321" => base.join("/search")?,
        "javlibrary" => base.join("/cn/vl_searchbyword.php")?,
        adapter => anyhow::bail!("unsupported source adapter: {adapter}"),
    };
    match provider.adapter.as_str() {
        "javdb" => {
            url.query_pairs_mut()
                .append_pair("q", code)
                .append_pair("f", "all");
        }
        "jav321" => {
            url.query_pairs_mut().append_pair("sn", code);
        }
        "javlibrary" => {
            url.query_pairs_mut().append_pair("keyword", code);
        }
        _ => {}
    }
    let response = source_fetch(state, provider, url).await?;
    ensure_provider_content(state, provider, &response, "resolve", Some(code), None).await?;
    let candidates = match provider.adapter.as_str() {
        "javdb" => parse_javdb_search_html(&response.body, code, &provider.base_url),
        "jav321" => parse_jav321_html(
            &response.body,
            code,
            &provider.base_url,
            &response.final_url,
        ),
        "javlibrary" => parse_javlibrary_html(
            &response.body,
            code,
            &provider.base_url,
            &response.final_url,
        ),
        _ => Vec::new(),
    };
    let requested = normalize_code(code);
    let candidate = candidates
        .into_iter()
        .find(|candidate| {
            normalize_code(&candidate.code) == requested
                || extract_media_code(&candidate.title)
                    .map(|value| normalize_code(&value) == requested)
                    .unwrap_or(false)
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "{} did not return an exact match for {code}",
                provider.display_name
            )
        })?;
    Ok((candidate, None))
}

pub(crate) async fn settle_ingestion_job(
    state: &AppState,
    job: &crate::ingestion::IngestionJob,
) -> anyhow::Result<()> {
    if !matches!(job.job_type.as_str(), "discovery" | "hydrate") {
        return Ok(());
    }
    let Some(run_id) = job.payload.get("runId").and_then(Value::as_i64) else {
        return Ok(());
    };
    settle_source_sync_run(state, run_id).await
}

async fn settle_source_sync_run(state: &AppState, run_id: i64) -> anyhow::Result<()> {
    let run = sqlx::query("SELECT provider_key,sync_mode,status,cursor_after_json,watermark_after_json FROM source_sync_run WHERE id=?")
        .bind(run_id)
        .fetch_optional(&state.pool)
        .await?;
    let Some(run) = run else {
        return Ok(());
    };
    if run.get::<String, _>("status") != "running" {
        return Ok(());
    }
    let active_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM ingestion_job WHERE status IN ('pending','running') AND job_type IN ('discovery','hydrate') AND json_extract(payload_json,'$.runId')=?")
        .bind(run_id)
        .fetch_one(&state.pool)
        .await?;
    if active_count > 0 {
        return Ok(());
    }
    let provider_key: String = run.get("provider_key");
    if run.get::<String, _>("sync_mode") == "bootstrap" {
        let paused: i64 = sqlx::query_scalar(
            "SELECT bootstrap_paused FROM source_sync_state WHERE provider_key=?",
        )
        .bind(&provider_key)
        .fetch_one(&state.pool)
        .await?;
        if paused != 0 {
            sqlx::query("UPDATE source_sync_state SET last_message='历史回填已暂停，可从 checkpoint 继续',updated_at=datetime('now') WHERE provider_key=?")
                .bind(&provider_key)
                .execute(&state.pool)
                .await?;
            return Ok(());
        }
    }
    let failed_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM ingestion_job WHERE status='failed' AND job_type IN ('discovery','hydrate') AND json_extract(payload_json,'$.runId')=?")
        .bind(run_id)
        .fetch_one(&state.pool)
        .await?;
    let provider = source_provider_by_key(state, &provider_key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("source provider {provider_key} no longer exists"))?;
    let stats = sqlx::query("SELECT item_count,inserted_count,updated_count,discovery_count,hydrated_count,pending_count FROM source_sync_run WHERE id=?")
        .bind(run_id)
        .fetch_one(&state.pool)
        .await?;
    let mut transaction = state.pool.begin().await?;
    if failed_count > 0 {
        sqlx::query("UPDATE source_sync_run SET status='failed',hydration_failed_count=?,pending_count=0,error_message='one or more ingestion jobs exhausted their retries',finished_at=datetime('now') WHERE id=? AND status='running'")
            .bind(failed_count)
            .bind(run_id)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("UPDATE source_sync_state SET status='failed',last_finished_at=datetime('now'),last_message='部分详情补全失败，保留已完成数据与 checkpoint',hydration_failed_count=?,pending_count=0,updated_at=datetime('now') WHERE provider_key=?")
            .bind(failed_count)
            .bind(&provider_key)
            .execute(&mut *transaction)
            .await?;
    } else {
        let cursor_after: String = run.get("cursor_after_json");
        let watermark_after: String = run.get("watermark_after_json");
        let next_modifier = format!("+{} minutes", provider.sync_interval_minutes);
        sqlx::query("UPDATE source_sync_run SET status='success',pending_count=0,finished_at=datetime('now') WHERE id=? AND status='running'")
            .bind(run_id)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("UPDATE source_sync_state SET status='success',last_finished_at=datetime('now'),last_success_at=datetime('now'),next_run_at=datetime('now',?),last_message='同步批次完成',failure_count=0,item_count=?,inserted_count=?,updated_count=?,discovery_count=?,hydrated_count=?,hydration_failed_count=0,pending_count=0,cursor_json=?,watermark_json=?,updated_at=datetime('now') WHERE provider_key=?")
            .bind(next_modifier)
            .bind(stats.get::<i64, _>("item_count"))
            .bind(stats.get::<i64, _>("inserted_count"))
            .bind(stats.get::<i64, _>("updated_count"))
            .bind(stats.get::<i64, _>("discovery_count"))
            .bind(stats.get::<i64, _>("hydrated_count"))
            .bind(cursor_after)
            .bind(watermark_after)
            .bind(&provider_key)
            .execute(&mut *transaction)
            .await?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn synchronize_source_catalogue(
    state: &AppState,
    provider: &SourceProviderConfig,
) -> anyhow::Result<SourceSyncStats> {
    let items = fetch_source_catalogue_with_retry(state, provider).await?;
    anyhow::ensure!(
        !items.is_empty(),
        "{} 首页没有解析出作品，已保留旧索引并停止本轮同步",
        provider.display_name
    );
    let mut stats = SourceSyncStats {
        item_count: 0,
        inserted_count: 0,
        updated_count: 0,
        detail_count: 0,
        detail_failures: 0,
    };
    let mut details = Vec::new();
    for item in items {
        let existed: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM provider_entity_mapping WHERE provider_key=? AND entity_type='media' AND provider_entity_id=?)")
            .bind(&provider.key)
            .bind(&item.provider_id)
            .fetch_one(&state.pool)
            .await?;
        match persist_source_media(state, provider, &item).await {
            Ok(media_id) => {
                stats.item_count += 1;
                if existed == 0 {
                    stats.inserted_count += 1;
                } else {
                    stats.updated_count += 1;
                }
                if details.len() < provider.sync_detail_limit {
                    details.push((media_id, item));
                }
            }
            Err(error) => tracing::warn!(
                %error,
                provider = provider.key,
                provider_id = item.provider_id,
                "source item was not canonical enough to index"
            ),
        }
    }
    anyhow::ensure!(
        stats.item_count > 0,
        "{} 最新目录中没有可识别番号的作品，已保留旧索引",
        provider.display_name
    );
    for (index, (media_id, item)) in details.iter().enumerate() {
        let detail = match provider.adapter.as_str() {
            "javbus" => {
                refresh_javbus_media(
                    state,
                    *media_id,
                    provider,
                    &item.provider_id,
                    &item.source_url,
                    true,
                    true,
                )
                .await
            }
            "javdb" | "jav321" | "javlibrary" => {
                refresh_generic_source_media(
                    state,
                    *media_id,
                    provider,
                    &item.provider_id,
                    &item.source_url,
                    true,
                    true,
                )
                .await
            }
            adapter => Err(anyhow::anyhow!("不支持的来源适配器：{adapter}")),
        };
        match detail {
            Ok(_) => stats.detail_count += 1,
            Err(error) => {
                stats.detail_failures += 1;
                tracing::warn!(%error, provider = provider.key, media_id, "source detail refresh failed");
            }
        }
        if index + 1 < details.len() {
            tokio::time::sleep(Duration::from_millis(900)).await;
        }
    }
    Ok(stats)
}

async fn fetch_source_catalogue_with_retry(
    state: &AppState,
    provider: &SourceProviderConfig,
) -> anyhow::Result<Vec<SourceMedia>> {
    let mut last_error = None;
    for attempt in 0..3 {
        match fetch_source_catalogue(state, provider).await {
            Ok(items) => return Ok(items),
            Err(error) => {
                let retryable = error
                    .downcast_ref::<FetchError>()
                    .is_some_and(|error| error.kind.retryable());
                if !retryable || attempt == 2 {
                    return Err(error);
                }
                last_error = Some(error);
                tokio::time::sleep(Duration::from_secs(if attempt == 0 { 2 } else { 5 })).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("来源同步失败")))
}

async fn fetch_source_catalogue(
    state: &AppState,
    provider: &SourceProviderConfig,
) -> anyhow::Result<Vec<SourceMedia>> {
    Ok(
        fetch_source_catalogue_page(state, provider, &provider.base_url)
            .await?
            .items,
    )
}

struct SourceCataloguePage {
    items: Vec<SourceMedia>,
    next_url: Option<String>,
}

async fn fetch_source_catalogue_page(
    state: &AppState,
    provider: &SourceProviderConfig,
    page_url: &str,
) -> anyhow::Result<SourceCataloguePage> {
    gate_provider(state, provider).await?;
    let url = validate_provider_page_url(provider, page_url)?;
    let response = match provider.adapter.as_str() {
        "javbus" => {
            let response = javbus_request(state, provider, url).await?;
            ensure_provider_content(state, provider, &response, "catalogue", None, None).await?;
            response
        }
        "javdb" => {
            let response = source_fetch(state, provider, url).await?;
            ensure_provider_content(state, provider, &response, "catalogue", None, None).await?;
            response
        }
        "jav321" => {
            let response = source_fetch(state, provider, url).await?;
            ensure_provider_content(state, provider, &response, "catalogue", None, None).await?;
            response
        }
        "javlibrary" => {
            let response = source_fetch(state, provider, url).await?;
            ensure_provider_content(state, provider, &response, "catalogue", None, None).await?;
            response
        }
        adapter => anyhow::bail!("不支持的来源适配器：{adapter}"),
    };
    let items = match provider.adapter.as_str() {
        "javbus" => parse_javbus_search_html(&response.body, "", &provider.base_url),
        "javdb" => parse_javdb_search_html(&response.body, "", &provider.base_url),
        "jav321" => parse_jav321_html(&response.body, "", &provider.base_url, &response.final_url),
        "javlibrary" => {
            parse_javlibrary_html(&response.body, "", &provider.base_url, &response.final_url)
        }
        _ => Vec::new(),
    };
    let next_url = extract_next_page_url(provider, &response);
    Ok(SourceCataloguePage { items, next_url })
}

fn extract_next_page_url(
    provider: &SourceProviderConfig,
    response: &FetchResponse,
) -> Option<String> {
    let document = Html::parse_document(&response.body);
    for selector in [
        "a[rel='next']",
        "a.next",
        "li.next a",
        "a.pagination-next",
        ".pagination a[aria-label='Next']",
    ] {
        let Ok(selector) = Selector::parse(selector) else {
            continue;
        };
        for link in document.select(&selector) {
            let Some(href) = link.value().attr("href") else {
                continue;
            };
            let Ok(url) = response.final_url.join(href) else {
                continue;
            };
            if url != response.final_url
                && validate_provider_page_url(provider, url.as_str()).is_ok()
            {
                return Some(url.into());
            }
        }
    }
    None
}

async fn finish_source_sync_success(
    state: &AppState,
    provider: &SourceProviderConfig,
    run_id: i64,
    stats: &SourceSyncStats,
) -> anyhow::Result<()> {
    let message = if stats.detail_failures == 0 {
        format!(
            "目录同步完成，收录 {} 项；更新 {} 项详情",
            stats.item_count, stats.detail_count
        )
    } else {
        format!(
            "目录同步完成，收录 {} 项；{} 项详情暂时失败",
            stats.item_count, stats.detail_failures
        )
    };
    let modifier = format!("+{} minutes", provider.sync_interval_minutes);
    let mut transaction = state.pool.begin().await?;
    sqlx::query("UPDATE source_sync_run SET status='success',item_count=?,inserted_count=?,updated_count=?,finished_at=datetime('now') WHERE id=?")
        .bind(stats.item_count).bind(stats.inserted_count).bind(stats.updated_count).bind(run_id).execute(&mut *transaction).await?;
    sqlx::query("UPDATE source_sync_state SET status='success',last_finished_at=datetime('now'),last_success_at=datetime('now'),next_run_at=datetime('now',?),last_message=?,failure_count=0,item_count=?,inserted_count=?,updated_count=?,lease_owner=NULL,lease_expires_at=NULL,updated_at=datetime('now') WHERE provider_key=?")
        .bind(&modifier).bind(&message).bind(stats.item_count).bind(stats.inserted_count).bind(stats.updated_count).bind(&provider.key).execute(&mut *transaction).await?;
    transaction.commit().await?;
    storage::log(
        &state.pool,
        "info",
        "source-sync",
        &format!("{}: {message}", provider.display_name),
    )
    .await;
    emit(
        state,
        "source-sync",
        json!({"providerKey":provider.key,"status":"success","message":message}),
    );
    Ok(())
}

async fn finish_source_sync_failure(
    state: &AppState,
    provider: &SourceProviderConfig,
    run_id: i64,
    error: &str,
) -> anyhow::Result<()> {
    let failure_count: i64 =
        sqlx::query_scalar("SELECT failure_count FROM source_sync_state WHERE provider_key=?")
            .bind(&provider.key)
            .fetch_optional(&state.pool)
            .await
            .ok()
            .flatten()
            .unwrap_or(0)
            + 1;
    let retry_minutes = match failure_count {
        0 | 1 => 60,
        2 => 180,
        3 => 360,
        _ => 720,
    };
    let modifier = format!("+{retry_minutes} minutes");
    let mut transaction = state.pool.begin().await?;
    sqlx::query("UPDATE source_sync_run SET status='failed',error_message=?,finished_at=datetime('now') WHERE id=?")
        .bind(error).bind(run_id).execute(&mut *transaction).await?;
    sqlx::query("UPDATE source_sync_state SET status='failed',last_finished_at=datetime('now'),next_run_at=datetime('now',?),last_message=?,failure_count=?,lease_owner=NULL,lease_expires_at=NULL,updated_at=datetime('now') WHERE provider_key=?")
        .bind(&modifier).bind(error).bind(failure_count).bind(&provider.key).execute(&mut *transaction).await?;
    transaction.commit().await?;
    storage::log(
        &state.pool,
        "warn",
        "source-sync",
        &format!("{}: {error}", provider.display_name),
    )
    .await;
    emit(
        state,
        "source-sync",
        json!({"providerKey":provider.key,"status":"failed","message":error}),
    );
    Ok(())
}

async fn provider_by_key(state: &AppState, key: &str) -> AppResult<Json<Value>> {
    let row = sqlx::query("SELECT pc.*,ss.status AS sync_status,ss.active_mode AS sync_active_mode,ss.bootstrap_paused AS sync_bootstrap_paused,ss.bootstrap_from AS sync_bootstrap_from,ss.bootstrap_to AS sync_bootstrap_to,ss.cursor_json AS sync_cursor_json,ss.last_started_at AS sync_last_started_at,ss.last_finished_at AS sync_last_finished_at,ss.last_success_at AS sync_last_success_at,ss.next_run_at AS sync_next_run_at,ss.last_message AS sync_last_message,ss.failure_count AS sync_failure_count,ss.item_count AS sync_item_count,ss.inserted_count AS sync_inserted_count,ss.updated_count AS sync_updated_count,ss.discovery_count AS sync_discovery_count,ss.hydrated_count AS sync_hydrated_count,ss.hydration_failed_count AS sync_hydration_failed_count,ss.pending_count AS sync_pending_count FROM provider_config pc LEFT JOIN source_sync_state ss ON ss.provider_key=pc.provider_key WHERE pc.provider_key=?")
        .bind(key)
        .fetch_optional(&state.pool)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(Json(provider_json(&row)))
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProductSettings {
    download_root: String,
    media_root: String,
    qbittorrent_save_path: String,
    qbittorrent_category: String,
    qbittorrent_tags: String,
    organizer_mode: String,
    organizer_movie_template: String,
    organizer_conflict_policy: String,
}

async fn get_product_settings(State(state): State<AppState>) -> AppResult<Json<ProductSettings>> {
    Ok(Json(load_product_settings(&state).await?))
}

async fn update_product_settings(
    State(state): State<AppState>,
    Json(input): Json<ProductSettings>,
) -> AppResult<Json<ProductSettings>> {
    if !matches!(input.organizer_mode.as_str(), "hardlink" | "copy") {
        return Err(AppError::BadRequest(
            "整理模式必须是 hardlink 或 copy".into(),
        ));
    }
    if !input.organizer_movie_template.contains("{code}") {
        return Err(AppError::BadRequest("电影命名模板必须包含 {code}".into()));
    }
    for (key, value) in [
        ("download_root", &input.download_root),
        ("media_root", &input.media_root),
        ("qbittorrent_save_path", &input.qbittorrent_save_path),
        ("qbittorrent_category", &input.qbittorrent_category),
        ("qbittorrent_tags", &input.qbittorrent_tags),
        ("organizer_mode", &input.organizer_mode),
        ("organizer_movie_template", &input.organizer_movie_template),
        (
            "organizer_conflict_policy",
            &input.organizer_conflict_policy,
        ),
    ] {
        save_setting(&state, key, value).await?;
    }
    Ok(Json(load_product_settings(&state).await?))
}

async fn load_product_settings(state: &AppState) -> AppResult<ProductSettings> {
    Ok(ProductSettings {
        download_root: setting(state, "download_root", "/downloads").await?,
        media_root: setting(state, "media_root", "/media").await?,
        qbittorrent_save_path: setting(state, "qbittorrent_save_path", "/downloads").await?,
        qbittorrent_category: setting(state, "qbittorrent_category", "luma").await?,
        qbittorrent_tags: setting(state, "qbittorrent_tags", "luma").await?,
        organizer_mode: setting(state, "organizer_mode", "hardlink").await?,
        organizer_movie_template: setting(state, "organizer_movie_template", "{code}/{code}.{ext}")
            .await?,
        organizer_conflict_policy: setting(state, "organizer_conflict_policy", "attention").await?,
    })
}

async fn source_provider_by_key(
    state: &AppState,
    key: &str,
) -> AppResult<Option<SourceProviderConfig>> {
    let row = sqlx::query(
        "SELECT * FROM provider_config WHERE provider_key = ? AND provider_type = 'source'",
    )
    .bind(key)
    .fetch_optional(&state.pool)
    .await?;
    Ok(row.as_ref().map(SourceProviderConfig::from_row))
}

fn source_provider_from_row(row: &sqlx::sqlite::SqliteRow) -> SourceProviderConfig {
    SourceProviderConfig::from_row(row)
}

async fn source_probe(state: &AppState, provider: &SourceProviderConfig) -> anyhow::Result<()> {
    let base = reqwest::Url::parse(&provider.base_url)?;
    let response = if provider.adapter == "javbus" {
        javbus_request(state, provider, base).await?
    } else {
        source_fetch(state, provider, base).await?
    };
    ensure_provider_content(state, provider, &response, "probe", None, None).await
}

async fn source_fetch(
    state: &AppState,
    provider: &SourceProviderConfig,
    url: reqwest::Url,
) -> anyhow::Result<FetchResponse> {
    let guard = ProviderExecutionGuard::new(&state.pool, &provider.key, provider.fetch_mode);
    match state
        .fetch_manager
        .fetch(
            provider.fetch_mode,
            FetchRequest::get(&provider.key, url),
            &provider.transport(),
        )
        .await
    {
        Ok(response) => {
            let _ = guard.after_success().await;
            Ok(response)
        }
        Err(error) => {
            let _ = guard.after_fetch_failure(error.kind, &error.message).await;
            Err(error.into())
        }
    }
}

async fn ensure_provider_content(
    state: &AppState,
    provider: &SourceProviderConfig,
    response: &FetchResponse,
    entity_type: &str,
    provider_entity_id: Option<&str>,
    media_id: Option<i64>,
) -> anyhow::Result<()> {
    let kind = state
        .provider_registry
        .classify(&provider.adapter, response);
    if kind != PageKind::ValidContent {
        let message = format!(
            "{} returned {kind:?} instead of valid provider content",
            provider.display_name
        );
        let guard = ProviderExecutionGuard::new(&state.pool, &provider.key, provider.fetch_mode);
        let _ = guard.after_page_failure(kind, &message).await;
        anyhow::bail!(message);
    }
    state
        .snapshot_repository
        .store(SnapshotInput {
            provider_key: &provider.key,
            entity_type,
            provider_entity_id,
            media_id,
            source_url: response.final_url.as_str(),
            response,
            raw_json: json!({ "pageKind": "valid_content" }),
            parser_version: "1",
        })
        .await?;
    Ok(())
}

fn parse_javdb_search_html(html: &str, fallback: &str, base_url: &str) -> Vec<SourceMedia> {
    let mut items = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = html[cursor..].find("href=\"") {
        let start = cursor + offset + 6;
        let Some(end_offset) = html[start..].find('"') else {
            break;
        };
        let href = &html[start..start + end_offset];
        cursor = start + end_offset + 1;
        if !(href.starts_with("/v/") || href.contains("/v/")) {
            continue;
        }
        let provider_id = href
            .split('?')
            .next()
            .unwrap_or(href)
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_owned();
        if provider_id.is_empty()
            || items
                .iter()
                .any(|item: &SourceMedia| item.provider_id == provider_id)
        {
            continue;
        }
        let tail = &html[cursor..char_boundary_before(html, cursor + 3000)];
        let code = extract_tag_text_after(tail, "uid").unwrap_or_else(|| fallback.to_owned());
        let title = extract_tag_text_after(tail, "video-title")
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| code.clone());
        let poster_url = extract_attribute(tail, "data-src=")
            .or_else(|| extract_attribute(tail, "src="))
            .and_then(|value| absolute_url(base_url, &value));
        items.push(SourceMedia {
            provider_id,
            code: normalize_code(&code),
            title,
            poster_url,
            source_url: absolute_url(base_url, href).unwrap_or_else(|| href.to_owned()),
            release_date: extract_iso_date(tail),
        });
        if items.len() >= 40 {
            break;
        }
    }
    items
}

fn parse_jav321_html(
    html: &str,
    fallback: &str,
    base_url: &str,
    final_url: &reqwest::Url,
) -> Vec<SourceMedia> {
    if final_url.path().contains("/video/") {
        let provider_id = final_url
            .path_segments()
            .and_then(Iterator::last)
            .unwrap_or(fallback)
            .to_owned();
        let code = text_after_label(html, "<b>品番</b>").unwrap_or_else(|| fallback.to_owned());
        let title = extract_tag_text_after(html, "panel-heading")
            .map(|value| {
                value
                    .split(&code)
                    .next()
                    .unwrap_or(&value)
                    .trim()
                    .to_owned()
            })
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| code.clone());
        let detail_html = &html[html.find("panel-heading").unwrap_or(0)..];
        let poster_url = extract_attribute(detail_html, "poster=")
            .or_else(|| extract_attribute(detail_html, "src="))
            .map(|value| value.replace("http://pics.dmm.co.jp", "https://pics.dmm.co.jp"))
            .and_then(|value| absolute_url(base_url, &value));
        return vec![SourceMedia {
            provider_id,
            code: normalize_code(&code),
            title,
            poster_url,
            source_url: final_url.to_string(),
            release_date: extract_iso_date(html),
        }];
    }

    parse_video_links(html, fallback, base_url, "/video/")
}

fn parse_javlibrary_html(
    html: &str,
    fallback: &str,
    base_url: &str,
    final_url: &reqwest::Url,
) -> Vec<SourceMedia> {
    if final_url
        .query()
        .is_some_and(|query| query.contains("v=jav"))
    {
        let provider_id = final_url
            .query_pairs()
            .find(|(key, _)| key == "v")
            .map(|(_, value)| value.into_owned())
            .unwrap_or_else(|| fallback.to_owned());
        let title = extract_tag_text_after(html, "video_title")
            .or_else(|| meta_content(html, "og:title"))
            .unwrap_or_else(|| fallback.to_owned());
        return vec![SourceMedia {
            provider_id,
            code: extract_media_code(&title)
                .or_else(|| extract_media_code(fallback))
                .map(|code| normalize_code(&code))
                .unwrap_or_default(),
            title,
            poster_url: meta_content(html, "og:image")
                .and_then(|value| absolute_url(base_url, &value)),
            source_url: final_url.to_string(),
            release_date: extract_iso_date(html),
        }];
    }
    parse_video_links(html, fallback, base_url, "?v=jav")
}

fn parse_video_links(html: &str, fallback: &str, base_url: &str, marker: &str) -> Vec<SourceMedia> {
    let mut items = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = html[cursor..].find(marker) {
        let at = cursor + offset;
        let anchor_start = html[..at].rfind("<a").unwrap_or(at);
        let anchor_end = html[at..]
            .find("</a>")
            .map(|offset| at + offset + 4)
            .unwrap_or(at);
        cursor = at + marker.len();
        if anchor_end <= anchor_start {
            continue;
        }
        let anchor = &html[anchor_start..anchor_end];
        let Some(href) = extract_attribute(anchor, "href=") else {
            continue;
        };
        let provider_id = href
            .split(['?', '&', '/'])
            .filter(|part| !part.is_empty() && *part != "video" && *part != "v=jav")
            .next_back()
            .unwrap_or(fallback)
            .trim_start_matches("v=")
            .to_owned();
        if items
            .iter()
            .any(|item: &SourceMedia| item.provider_id == provider_id)
        {
            continue;
        }
        let title = extract_attribute(anchor, "title=")
            .unwrap_or_else(|| strip_tags(anchor))
            .trim()
            .to_owned();
        let poster_url = extract_attribute(anchor, "data-original=")
            .or_else(|| extract_attribute(anchor, "src="))
            .and_then(|value| absolute_url(base_url, &value));
        let code = extract_media_code(&title)
            .or_else(|| extract_media_code(&provider_id))
            .or_else(|| extract_media_code(fallback))
            .map(|code| normalize_code(&code))
            .unwrap_or_default();
        items.push(SourceMedia {
            provider_id,
            code,
            title: if title.is_empty() {
                fallback.to_owned()
            } else {
                title
            },
            poster_url,
            source_url: absolute_url(base_url, &href).unwrap_or(href),
            release_date: extract_iso_date(anchor),
        });
        if items.len() >= 40 {
            break;
        }
    }
    items
}

fn text_after_label(html: &str, label: &str) -> Option<String> {
    let at = html.find(label)? + label.len();
    let tail = &html[at..char_boundary_before(html, at + 300)];
    let end = tail
        .find("<br")
        .or_else(|| tail.find("</p>"))
        .unwrap_or(tail.len());
    let text = strip_tags(&tail[..end])
        .trim_start_matches([':', '：', ' '])
        .trim()
        .to_owned();
    (!text.is_empty()).then_some(text)
}

fn extract_tag_text_after(html: &str, marker: &str) -> Option<String> {
    let at = html.find(marker)?;
    let tail = &html[at..char_boundary_before(html, at + 2000)];
    let start = tail.find('>')? + 1;
    let body = &tail[start..];
    let end = ["</h3>", "</strong>", "</div>", "</a>"]
        .iter()
        .filter_map(|closing| body.find(closing))
        .min()
        .unwrap_or(body.len());
    let value = strip_tags(&body[..end]).trim().to_owned();
    (!value.is_empty()).then_some(value)
}

fn javbus_cookie(provider: &SourceProviderConfig, session: &[String]) -> String {
    let mut parts = vec!["age=verified".to_owned(), "existmag=all".to_owned()];
    if !provider.secret.trim().is_empty() {
        parts.push(provider.secret.trim().to_owned());
    }
    parts.extend(session.iter().cloned());
    parts.join("; ")
}

async fn gate_provider(state: &AppState, provider: &SourceProviderConfig) -> anyhow::Result<()> {
    let guard = ProviderExecutionGuard::new(&state.pool, &provider.key, provider.fetch_mode);
    let decision = guard.before_fetch().await?;
    if decision.allows_request() {
        Ok(())
    } else {
        Err(anyhow::Error::new(GateBlocked {
            provider_key: provider.key.clone(),
            decision,
        }))
    }
}

async fn record_javbus_fetch(
    state: &AppState,
    provider: &SourceProviderConfig,
    result: &Result<FetchResponse, FetchError>,
) {
    let guard = ProviderExecutionGuard::new(&state.pool, &provider.key, provider.fetch_mode);
    match result {
        Ok(_) => {
            let _ = guard.after_success().await;
        }
        Err(error) => {
            let _ = guard.after_fetch_failure(error.kind, &error.message).await;
        }
    }
}

async fn javbus_request(
    state: &AppState,
    provider: &SourceProviderConfig,
    url: reqwest::Url,
) -> anyhow::Result<FetchResponse> {
    gate_provider(state, provider).await?;
    let initial_cookie = javbus_cookie(provider, &[]);
    let mut transport = provider.transport();
    transport.cookie = Some(initial_cookie.clone());
    let first_result = state
        .fetch_manager
        .fetch(
            provider.fetch_mode,
            FetchRequest::get(&provider.key, url.clone()),
            &transport,
        )
        .await;
    record_javbus_fetch(state, provider, &first_result).await;
    let response = first_result?;
    if !is_javbus_age_page(&response.body) {
        return Ok(response);
    }

    let mut verify_url = reqwest::Url::parse(provider.base_url.trim_end_matches('/'))?;
    verify_url.set_path("/doc/driver-verify");
    verify_url
        .query_pairs_mut()
        .append_pair("referer", url.path());
    let verification = state
        .fetch_manager
        .fetch(
            provider.fetch_mode,
            FetchRequest {
                provider_key: provider.key.clone(),
                url: verify_url,
                method: FetchMethod::Post,
                headers: reqwest::header::HeaderMap::new(),
                referer: Some(url.clone()),
                body: Some("Submit=confirm".into()),
                timeout: Duration::from_secs(20),
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
        .map(str::to_owned)
        .collect::<Vec<_>>();
    transport.cookie = Some(javbus_cookie(provider, &session));
    let retried_result = state
        .fetch_manager
        .fetch(
            provider.fetch_mode,
            FetchRequest::get(&provider.key, url),
            &transport,
        )
        .await;
    record_javbus_fetch(state, provider, &retried_result).await;
    let retried = retried_result?;
    if is_javbus_age_page(&retried.body) {
        anyhow::bail!("JavBus 要求年龄验证，请在该来源中填写可用 Cookie 或更换镜像");
    }
    Ok(retried)
}

fn is_javbus_age_page(html: &str) -> bool {
    html.contains("driver-verify")
        && (html.contains("Age Verification") || html.contains("你是否已經成年"))
}

fn parse_javbus_search_html(html: &str, fallback: &str, base_url: &str) -> Vec<SourceMedia> {
    let mut items = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = html[cursor..].find("movie-box") {
        let marker = cursor + offset;
        let anchor_start = html[..marker].rfind("<a").unwrap_or(marker);
        let Some(open_end_offset) = html[anchor_start..].find('>') else {
            break;
        };
        if anchor_start + open_end_offset < marker {
            cursor = marker + "movie-box".len();
            continue;
        }
        let Some(anchor_end_offset) = html[marker..].find("</a>") else {
            break;
        };
        let anchor_end = marker + anchor_end_offset + 4;
        let card = &html[anchor_start..anchor_end];
        cursor = anchor_end;
        let Some(href) = extract_attribute(card, "href=") else {
            continue;
        };
        let provider_id = href
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_owned();
        if provider_id.is_empty()
            || items
                .iter()
                .any(|item: &SourceMedia| item.provider_id == provider_id)
        {
            continue;
        }
        let code = if provider_id.is_empty() {
            fallback.to_owned()
        } else {
            provider_id.clone()
        };
        let title = extract_attribute(card, "title=")
            .or_else(|| extract_attribute(card, "alt="))
            .map(|value| strip_tags(&value))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| code.clone());
        let poster_url = extract_attribute(card, "data-original=")
            .or_else(|| extract_attribute(card, "src="))
            .and_then(|value| absolute_url(base_url, &value));
        let source_url = absolute_url(base_url, &href).unwrap_or(href);
        items.push(SourceMedia {
            provider_id,
            code: normalize_code(&code),
            title,
            poster_url,
            source_url,
            release_date: extract_iso_date(card),
        });
        if items.len() >= 40 {
            break;
        }
    }
    items
}

fn absolute_url(base_url: &str, value: &str) -> Option<String> {
    reqwest::Url::parse(value)
        .or_else(|_| reqwest::Url::parse(base_url)?.join(value))
        .ok()
        .map(Into::into)
}

fn extract_iso_date(text: &str) -> Option<String> {
    text.as_bytes().windows(10).find_map(|window| {
        let value = std::str::from_utf8(window).ok()?;
        (value.as_bytes()[4] == b'-'
            && value.as_bytes()[7] == b'-'
            && value
                .bytes()
                .enumerate()
                .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
            && chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok())
        .then(|| value.to_owned())
    })
}

async fn persist_source_media(
    state: &AppState,
    provider: &SourceProviderConfig,
    item: &SourceMedia,
) -> anyhow::Result<i64> {
    let code = extract_media_code(&item.code)
        .or_else(|| extract_media_code(&item.title))
        .or_else(|| extract_media_code(&item.provider_id))
        .map(|code| normalize_code(&code))
        .filter(|code| !code.is_empty())
        .ok_or_else(|| anyhow::anyhow!("无法从来源条目提取真实番号"))?;
    let row = sqlx::query("INSERT INTO media(normalized_code,title,poster_url,release_date) VALUES (?,?,?,?) ON CONFLICT(normalized_code) DO UPDATE SET normalized_code=excluded.normalized_code RETURNING id")
        .bind(&code)
        .bind(&item.title)
        .bind(&item.poster_url)
        .bind(&item.release_date)
        .fetch_one(&state.pool)
        .await?;
    let id: i64 = row.get("id");
    sqlx::query("INSERT INTO provider_entity_mapping(provider_key, entity_type, provider_entity_id, media_id, source_url) VALUES (?, 'media', ?, ?, ?) ON CONFLICT(provider_key, entity_type, provider_entity_id) DO UPDATE SET media_id = excluded.media_id, source_url = excluded.source_url, last_seen_at = datetime('now')")
        .bind(&provider.key).bind(&item.provider_id).bind(id).bind(&item.source_url).execute(&state.pool).await?;
    let mut source = MetadataSourceInput::catalogue(id, &provider.key, &item.provider_id, &code);
    source.source_url = Some(item.source_url.clone());
    source.priority = provider.metadata_priority;
    source.title = Some(item.title.clone());
    source.release_date = item.release_date.clone();
    source.poster_url = item.poster_url.clone();
    source.aliases = vec![LocalizedAlias::new(&item.title, "und")];
    source.raw_json = json!({
        "code": code,
        "title": item.title,
        "posterUrl": item.poster_url,
        "releaseDate": item.release_date,
    });
    crate::metadata::record_and_resolve(&state.pool, &source).await?;
    Ok(id)
}

pub async fn ingest_crawler_result(state: &AppState, result_id: i64) -> AppResult<(i64, i64)> {
    let result = crate::crawler::result_by_id(&state.pool, result_id).await?;
    let code = [
        "code",
        "number",
        "mediaCode",
        "media_code",
        "videoId",
        "video_id",
    ]
    .into_iter()
    .find_map(|key| result.raw.get(key))
    .and_then(json_value_string)
    .and_then(|value| extract_media_code(&value))
    .or_else(|| extract_media_code(&result.title))
    .map(|code| normalize_code(&code))
    .filter(|code| !code.is_empty())
    .ok_or_else(|| {
        AppError::BadRequest(
            "爬虫结果缺少可识别的番号；请让脚本输出 code 或在标题中包含番号".into(),
        )
    })?;
    let media_title = ["mediaTitle", "media_title", "title"]
        .into_iter()
        .find_map(|key| result.raw.get(key))
        .and_then(json_value_string)
        .filter(|title| !title.trim().is_empty())
        .unwrap_or_else(|| result.title.clone());
    let original_title = ["originalTitle", "original_title", "titleJa", "title_ja"]
        .into_iter()
        .find_map(|key| result.raw.get(key))
        .and_then(json_value_string);
    let poster_url = ["posterUrl", "poster_url", "coverUrl", "cover_url"]
        .into_iter()
        .find_map(|key| result.raw.get(key))
        .and_then(json_value_string);
    let media_id: i64 = sqlx::query("INSERT INTO media(normalized_code,title,original_title,poster_url) VALUES (?,?,?,?) ON CONFLICT(normalized_code) DO UPDATE SET normalized_code=excluded.normalized_code RETURNING id")
        .bind(&code).bind(&media_title).bind(&original_title).bind(&poster_url).fetch_one(&state.pool).await?.get("id");
    let info_hash = magnet_hash(&result.download_url);
    let provider_key = format!("crawler:{}", result.script_id);
    let provider_entity_id = [
        "mediaId",
        "media_id",
        "videoId",
        "video_id",
        "externalMediaId",
        "external_media_id",
    ]
    .into_iter()
    .find_map(|key| result.raw.get(key))
    .and_then(json_value_string)
    .filter(|value| !value.trim().is_empty())
    .unwrap_or_else(|| code.clone());
    let mut aliases = vec![LocalizedAlias::new(&media_title, "und")];
    if let Some(original_title) = original_title.as_deref() {
        aliases.push(LocalizedAlias::new(original_title, "ja"));
    }
    aliases.extend(
        localized_media_titles(&result.raw)
            .into_iter()
            .map(|(alias, locale)| LocalizedAlias::new(alias, locale)),
    );
    let mut source =
        MetadataSourceInput::catalogue(media_id, &provider_key, &provider_entity_id, &code);
    source.source_url = Some(result.source.clone());
    source.record_kind = "crawler".into();
    source.evidence_level = 2;
    source.priority = result
        .raw
        .get("metadataPriority")
        .and_then(Value::as_i64)
        .unwrap_or(50)
        .clamp(-1000, 1000);
    source.title = Some(media_title.clone());
    source.original_title = original_title.clone();
    source.summary =
        first_json_string(&result.raw, &["summary", "plot", "description"]).map(str::to_owned);
    source.release_date =
        first_json_string(&result.raw, &["releaseDate", "release_date", "premiered"])
            .map(str::to_owned);
    source.duration_minutes = result
        .raw
        .get("durationMinutes")
        .or_else(|| result.raw.get("duration_minutes"))
        .or_else(|| result.raw.get("runtime"))
        .and_then(Value::as_i64);
    source.poster_url = poster_url.clone();
    source.backdrop_url = first_json_string(
        &result.raw,
        &["backdropUrl", "backdrop_url", "thumbUrl", "thumb_url"],
    )
    .map(str::to_owned);
    source.actors = source_actors_from_json(&result.raw);
    source.aliases = aliases;
    source.tags = result
        .raw
        .get("tags")
        .or_else(|| result.raw.get("genres"))
        .map(|value| json_string_array(Some(value)))
        .unwrap_or_default();
    source.raw_json = result.raw.clone();
    crate::metadata::record_and_resolve(&state.pool, &source).await?;
    sqlx::query("INSERT INTO provider_entity_mapping(provider_key,entity_type,provider_entity_id,media_id,source_url,raw_json) VALUES (?,'media',?,?,?,?) ON CONFLICT(provider_key,entity_type,provider_entity_id) DO UPDATE SET media_id=excluded.media_id,source_url=excluded.source_url,raw_json=excluded.raw_json,last_seen_at=datetime('now')")
        .bind(&provider_key)
        .bind(&provider_entity_id)
        .bind(media_id)
        .bind(&result.source)
        .bind(result.raw.to_string())
        .execute(&state.pool)
        .await?;
    let provider_resource_id = [
        "resourceId",
        "resource_id",
        "externalId",
        "external_id",
        "providerResourceId",
        "provider_resource_id",
        "guid",
        "id",
    ]
    .into_iter()
    .find_map(|key| result.raw.get(key))
    .and_then(json_value_string)
    .filter(|value| !value.trim().is_empty())
    .unwrap_or_else(|| {
        info_hash
            .clone()
            .map(|hash| format!("btih:{hash}"))
            .unwrap_or_else(|| format!("result:{}", result.id))
    });
    let resource_id = crate::resource::upsert_candidate(
        &state.pool,
        media_id,
        &provider_key,
        &ResourceCandidate {
            provider_resource_id: Some(provider_resource_id),
            download_url: result.download_url.clone(),
            title: result.title.clone(),
            info_hash,
            trackers: result.trackers.clone(),
            published_at: (!result.published_at.is_empty()).then(|| result.published_at.clone()),
            source_url: result.source.clone(),
            raw_json: result.raw.clone(),
            ..ResourceCandidate::default()
        },
    )
    .await?
    .id;
    sqlx::query("UPDATE crawler_result SET media_id=?,resource_id=? WHERE id=?")
        .bind(media_id)
        .bind(resource_id)
        .bind(result.id)
        .execute(&state.pool)
        .await?;
    refresh_media_search_document(state, media_id).await?;
    Ok((media_id, resource_id))
}

fn json_value_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) if !value.trim().is_empty() => Some(value.trim().to_owned()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

async fn resources_for_media_paged(
    state: &AppState,
    media_id: i64,
    params: PageParams,
) -> AppResult<(Vec<Resource>, i64)> {
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM resource r WHERE r.media_id = ?")
        .bind(media_id)
        .fetch_one(&state.pool)
        .await?;
    let rows = sqlx::query("SELECT r.*, (SELECT a.id FROM acquisition a WHERE a.resource_id=r.id ORDER BY a.id DESC LIMIT 1) AS acquisition_id, (SELECT a.state FROM acquisition a WHERE a.resource_id=r.id ORDER BY a.id DESC LIMIT 1) AS acquisition_state, (SELECT a.qbit_hash FROM acquisition a WHERE a.resource_id=r.id ORDER BY a.id DESC LIMIT 1) AS acquisition_qbit_hash FROM resource r WHERE r.media_id = ? ORDER BY r.available DESC, r.score DESC, r.published_at DESC, r.id DESC LIMIT ? OFFSET ?")
        .bind(media_id)
        .bind(params.limit())
        .bind(params.offset())
        .fetch_all(&state.pool)
        .await?;
    let mut resources = rows.iter().map(resource_from_row).collect::<Vec<_>>();
    enrich_resources_with_qbit(state, &mut resources).await;
    Ok((resources, total))
}

async fn enrich_resources_with_qbit(state: &AppState, resources: &mut [Resource]) {
    let settings = match storage::load_settings(&state.pool).await {
        Ok(settings) => settings,
        Err(error) => {
            tracing::warn!(%error, "resource status has invalid qBittorrent settings");
            for resource in resources {
                resource.qbit_sync_status = "unavailable".into();
            }
            return;
        }
    };
    match QBittorrentClient::new(&settings) {
        Ok(client) => match client.torrents().await {
            Ok(torrents) => {
                let torrents = torrents
                    .into_iter()
                    .filter_map(|torrent| normalize_hash(&torrent.hash).map(|hash| (hash, torrent)))
                    .collect::<HashMap<_, _>>();
                for resource in resources.iter_mut() {
                    let resource_hash = resource
                        .info_hash
                        .as_deref()
                        .and_then(normalize_hash)
                        .or_else(|| magnet_hash(&resource.download_url));
                    let acquisition_hash = resource.qbit_hash.as_deref().and_then(normalize_hash);
                    let torrent = resource_hash
                        .as_ref()
                        .and_then(|hash| torrents.get(hash))
                        .or_else(|| {
                            acquisition_hash
                                .as_ref()
                                .and_then(|hash| torrents.get(hash))
                        });
                    resource.qbit_sync_status = "synced".into();
                    if let Some(torrent) = torrent {
                        resource.qbit_hash = Some(torrent.hash.clone());
                        resource.qbit_state = Some(torrent.state.clone());
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, "resource status could not reach qBittorrent");
                for resource in resources.iter_mut() {
                    resource.qbit_sync_status = "unavailable".into();
                }
            }
        },
        Err(error) => {
            tracing::warn!(%error, "resource status has invalid qBittorrent settings");
            for resource in resources.iter_mut() {
                resource.qbit_sync_status = "unavailable".into();
            }
        }
    }
}

async fn resource_provider_keys_for_media(
    state: &AppState,
    media_id: i64,
) -> AppResult<Vec<String>> {
    let rows = sqlx::query("SELECT DISTINCT pc.* FROM provider_entity_mapping mapping JOIN provider_config pc ON pc.provider_key=mapping.provider_key WHERE mapping.entity_type='media' AND mapping.media_id=? AND pc.provider_type='source' AND pc.enabled=1 ORDER BY pc.provider_key")
        .bind(media_id)
        .fetch_all(&state.pool)
        .await?;
    Ok(rows
        .iter()
        .map(source_provider_from_row)
        .filter(|provider| {
            state
                .provider_registry
                .resource_provider(&provider.adapter)
                .is_some()
        })
        .map(|provider| provider.key)
        .collect())
}

async fn enqueue_resource_refresh_job(
    state: &AppState,
    payload: ResourceRefreshJobPayload,
    priority: i64,
) -> AppResult<i64> {
    let dedupe_key = format!(
        "resource-refresh:{}:{}",
        payload.media_id, payload.provider_key
    );
    let enqueued = state
        .ingestion_queue
        .enqueue(EnqueueJob {
            provider_key: &payload.provider_key,
            job_type: "resource_refresh",
            priority,
            payload: serde_json::to_value(&payload).map_err(anyhow::Error::from)?,
            max_attempts: 3,
            dedupe_key: Some(&dedupe_key),
        })
        .await?;
    Ok(enqueued.id)
}

pub(crate) async fn execute_resource_refresh_job(
    state: &AppState,
    payload: &ResourceRefreshJobPayload,
) -> anyhow::Result<()> {
    if !payload.force
        && !crate::resource::cache_needs_refresh(
            &state.pool,
            payload.media_id,
            &payload.provider_key,
        )
        .await?
    {
        return Ok(());
    }
    let row = sqlx::query("SELECT mapping.provider_entity_id,mapping.source_url,pc.* FROM provider_entity_mapping mapping JOIN provider_config pc ON pc.provider_key=mapping.provider_key WHERE mapping.entity_type='media' AND mapping.media_id=? AND mapping.provider_key=? AND pc.provider_type='source' AND pc.enabled=1")
        .bind(payload.media_id)
        .bind(&payload.provider_key)
        .fetch_optional(&state.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("media/provider mapping no longer exists"))?;
    let provider = source_provider_from_row(&row);
    let provider_id: String = row.get("provider_entity_id");
    let source_url = row
        .get::<Option<String>, _>("source_url")
        .unwrap_or_else(|| provider.base_url.clone());
    match fetch_and_persist_resources(
        state,
        payload.media_id,
        &provider,
        &provider_id,
        &source_url,
        None,
        true,
    )
    .await
    {
        Ok(resource_count) => {
            emit(
                state,
                "resource-refresh",
                json!({"mediaId":payload.media_id,"providerKey":payload.provider_key,"status":"success","resourceCount":resource_count}),
            );
            Ok(())
        }
        Err(error) => {
            crate::resource::record_refresh_failure(
                &state.pool,
                payload.media_id,
                &provider.key,
                &error.to_string(),
            )
            .await?;
            emit(
                state,
                "resource-refresh",
                json!({"mediaId":payload.media_id,"providerKey":payload.provider_key,"status":"failed","message":error.to_string()}),
            );
            Err(error)
        }
    }
}

async fn fetch_and_persist_resources(
    state: &AppState,
    media_id: i64,
    provider: &SourceProviderConfig,
    provider_id: &str,
    source_url: &str,
    raw_document: Option<RawProviderDocument>,
    allow_resource_endpoint: bool,
) -> anyhow::Result<usize> {
    gate_provider(state, provider).await?;
    let resource_provider = state
        .provider_registry
        .resource_provider(&provider.adapter)
        .ok_or_else(|| anyhow::anyhow!("{} does not provide resources", provider.adapter))?;
    let context = ProviderContext {
        state: state.clone(),
        provider: provider.clone(),
        media_id,
        raw_document,
        allow_resource_endpoint,
    };
    let media = ProviderMediaRef {
        provider_id: provider_id.to_owned(),
        source_url: source_url.to_owned(),
    };
    let candidates = resource_provider.fetch_resources(&context, &media).await?;
    for candidate in &candidates {
        crate::resource::upsert_candidate(&state.pool, media_id, &provider.key, candidate).await?;
    }
    crate::resource::record_refresh_success(
        &state.pool,
        media_id,
        &provider.key,
        candidates.len(),
        provider.resource_cache_ttl_hours,
    )
    .await?;
    refresh_media_search_document(state, media_id).await?;
    Ok(candidates.len())
}

async fn refresh_javbus_media(
    state: &AppState,
    media_id: i64,
    provider: &SourceProviderConfig,
    provider_id: &str,
    source_url: &str,
    include_resources: bool,
    allow_resource_endpoint: bool,
) -> anyhow::Result<usize> {
    gate_provider(state, provider).await?;
    let base = reqwest::Url::parse(provider.base_url.trim_end_matches('/'))?;
    let url = reqwest::Url::parse(source_url)
        .or_else(|_| base.join(source_url))
        .or_else(|_| base.join(provider_id))?;
    let response = javbus_request(state, provider, url.clone()).await?;
    ensure_provider_content(
        state,
        provider,
        &response,
        "detail",
        Some(provider_id),
        Some(media_id),
    )
    .await?;
    let final_url = response.final_url.to_string();
    let html = response.body;
    let title = meta_content(&html, "og:title");
    let poster = meta_content(&html, "og:image");
    let summary = meta_content(&html, "og:description").unwrap_or_default();
    let release_date = extract_iso_date(&html);
    let actors = extract_source_actors(&html, "/star/");
    persist_metadata_detail(
        state,
        media_id,
        provider,
        provider_id,
        &final_url,
        title,
        poster,
        summary,
        release_date,
        actors,
    )
    .await?;
    let resource_count = if include_resources {
        fetch_and_persist_resources(
            state,
            media_id,
            provider,
            provider_id,
            &final_url,
            Some(RawProviderDocument {
                source_url: final_url.clone(),
                body: html,
            }),
            allow_resource_endpoint,
        )
        .await?
    } else {
        refresh_media_search_document(state, media_id).await?;
        0
    };
    Ok(resource_count)
}

async fn refresh_generic_source_media(
    state: &AppState,
    media_id: i64,
    provider: &SourceProviderConfig,
    provider_id: &str,
    source_url: &str,
    include_resources: bool,
    allow_resource_endpoint: bool,
) -> anyhow::Result<usize> {
    gate_provider(state, provider).await?;
    let base = reqwest::Url::parse(provider.base_url.trim_end_matches('/'))?;
    let url = reqwest::Url::parse(source_url).or_else(|_| match provider.adapter.as_str() {
        "javdb" => base.join(&format!("/v/{provider_id}")),
        "jav321" => base.join(&format!("/video/{provider_id}")),
        _ => base.join(source_url),
    })?;
    let response = source_fetch(state, provider, url).await?;
    ensure_provider_content(
        state,
        provider,
        &response,
        "detail",
        Some(provider_id),
        Some(media_id),
    )
    .await?;
    let final_url = response.final_url.to_string();
    let html = response.body;
    let actor_marker = if provider.adapter == "javdb" {
        "/actors/"
    } else {
        "/star/"
    };
    persist_source_detail_html(
        state,
        media_id,
        provider,
        provider_id,
        &final_url,
        &html,
        actor_marker,
    )
    .await?;
    if include_resources
        && state
            .provider_registry
            .resource_provider(&provider.adapter)
            .is_some()
    {
        fetch_and_persist_resources(
            state,
            media_id,
            provider,
            provider_id,
            &final_url,
            Some(RawProviderDocument {
                source_url: final_url.clone(),
                body: html,
            }),
            allow_resource_endpoint,
        )
        .await
    } else {
        refresh_media_search_document(state, media_id).await?;
        Ok(0)
    }
}

async fn persist_source_detail_html(
    state: &AppState,
    media_id: i64,
    provider: &SourceProviderConfig,
    provider_id: &str,
    source_url: &str,
    html: &str,
    actor_marker: &str,
) -> anyhow::Result<()> {
    let title = meta_content(html, "og:title")
        .or_else(|| extract_tag_text_after(html, "panel-heading"))
        .or_else(|| extract_tag_text_after(html, "video_title"));
    let poster = meta_content(html, "og:image")
        .or_else(|| extract_attribute(html, "poster="))
        .map(|value| value.replace("http://pics.dmm.co.jp", "https://pics.dmm.co.jp"));
    let summary = meta_content(html, "og:description").unwrap_or_default();
    persist_metadata_detail(
        state,
        media_id,
        provider,
        provider_id,
        source_url,
        title,
        poster,
        summary,
        extract_iso_date(html),
        extract_source_actors(html, actor_marker),
    )
    .await?;
    Ok(())
}

async fn persist_metadata_detail(
    state: &AppState,
    media_id: i64,
    provider: &SourceProviderConfig,
    provider_id: &str,
    source_url: &str,
    title: Option<String>,
    poster_url: Option<String>,
    summary: String,
    release_date: Option<String>,
    actors: Vec<SourceActor>,
) -> anyhow::Result<()> {
    let normalized_code: String =
        sqlx::query_scalar("SELECT normalized_code FROM media WHERE id=?")
            .bind(media_id)
            .fetch_one(&state.pool)
            .await?;
    let aliases = title
        .iter()
        .map(|title| LocalizedAlias::new(title, "und"))
        .collect::<Vec<_>>();
    let mut source =
        MetadataSourceInput::catalogue(media_id, &provider.key, provider_id, &normalized_code);
    source.source_url = Some(source_url.to_owned());
    source.record_kind = "detail".into();
    source.evidence_level = 2;
    source.priority = provider.metadata_priority;
    source.title = title;
    source.summary = (!summary.trim().is_empty()).then_some(summary);
    source.release_date = release_date;
    source.poster_url = poster_url;
    source.actors = actors;
    source.aliases = aliases;
    source.raw_json = json!({
        "title": source.title,
        "summary": source.summary,
        "releaseDate": source.release_date,
        "posterUrl": source.poster_url,
        "actors": source.actors,
    });
    crate::metadata::record_and_resolve(&state.pool, &source).await?;
    Ok(())
}

fn extract_source_actors(html: &str, marker: &str) -> Vec<SourceActor> {
    let mut actors = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = html[cursor..].find(marker) {
        let start = cursor + offset;
        let id_end = html[start..].find(['\"', '\'', '?']).unwrap_or(64).min(64);
        let provider_id = html[start + marker.len()..start + id_end].trim_matches('/');
        let nearby_start = char_boundary_before(html, start.saturating_sub(100));
        let nearby_end = char_boundary_before(html, start + 300);
        let nearby = &html[nearby_start..nearby_end];
        let name = extract_link_text(nearby, marker).unwrap_or_default();
        cursor = start + id_end;
        if provider_id.is_empty() || name.is_empty() {
            continue;
        }
        if actors
            .iter()
            .any(|actor: &SourceActor| actor.provider_actor_id.as_deref() == Some(provider_id))
        {
            continue;
        }
        actors.push(SourceActor {
            provider_actor_id: Some(provider_id.to_owned()),
            name,
            aliases: Vec::new(),
            avatar_url: None,
        });
        if actors.len() >= 40 {
            break;
        }
    }
    actors
}

fn parse_magnets(html: &str) -> Vec<(String, String)> {
    crate::providers::parse_magnet_candidates(html, "资源", "")
        .into_iter()
        .map(|candidate| (candidate.download_url, candidate.title))
        .collect()
}

fn meta_content(html: &str, property: &str) -> Option<String> {
    let marker = format!("property=\"{property}\"");
    let at = html.find(&marker)?;
    let end = char_boundary_before(html, at + 600);
    extract_attribute(&html[at..end], "content=")
}

pub(crate) fn reparse_provider_snapshot(
    adapter: &str,
    entity_type: &str,
    final_url: &reqwest::Url,
    body: &str,
    page_kind: PageKind,
) -> serde_json::Value {
    if page_kind != PageKind::ValidContent && entity_type != "resource" {
        return json!({
            "pageKind": page_kind,
            "entityType": entity_type,
            "parsed": false,
        });
    }
    if entity_type == "resource" {
        let resources = parse_magnets(body);
        return json!({
            "pageKind": page_kind,
            "entityType": entity_type,
            "parsed": true,
            "resourceCount": resources.len(),
            "resourceHashes": resources
                .iter()
                .filter_map(|(url, _)| magnet_hash(url))
                .take(40)
                .collect::<Vec<_>>(),
        });
    }
    if entity_type == "detail" {
        let actor_marker = if adapter == "javdb" {
            "/actors/"
        } else {
            "/star/"
        };
        return json!({
            "pageKind": page_kind,
            "entityType": entity_type,
            "parsed": true,
            "title": meta_content(body, "og:title"),
            "posterUrl": meta_content(body, "og:image"),
            "actorReferenceCount": body.matches(actor_marker).count().min(40),
            "embeddedResourceCount": parse_magnets(body).len(),
        });
    }

    let base_url = final_url.as_str();
    let candidates = match adapter {
        "javbus" => parse_javbus_search_html(body, "", base_url),
        "javdb" => parse_javdb_search_html(body, "", base_url),
        "jav321" => parse_jav321_html(body, "", base_url, final_url),
        "javlibrary" => parse_javlibrary_html(body, "", base_url, final_url),
        _ => Vec::new(),
    };
    json!({
        "pageKind": page_kind,
        "entityType": entity_type,
        "parsed": true,
        "candidateCount": candidates.len(),
        "candidates": candidates
            .iter()
            .take(40)
            .map(|item| json!({
                "providerEntityId": item.provider_id,
                "code": item.code,
                "title": item.title,
                "posterUrl": item.poster_url,
                "sourceUrl": item.source_url,
            }))
            .collect::<Vec<_>>(),
    })
}

fn extract_link_text(html: &str, marker: &str) -> Option<String> {
    let at = html.find(marker)?;
    let tail = &html[at..];
    let start = tail.find('>')? + 1;
    let end = tail[start..].find('<')?;
    let text = strip_tags(&tail[start..start + end]);
    (!text.is_empty()).then_some(text)
}

async fn acquisition_by_id(state: &AppState, id: i64) -> AppResult<Acquisition> {
    let row = sqlx::query(&format!("{ACQUISITION_SELECT} WHERE a.id = ?"))
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(acquisition_from_row(&row))
}

const ACQUISITION_SELECT: &str = "SELECT a.*, m.normalized_code, m.title AS media_title, m.original_title, m.summary, m.release_date, m.duration_minutes, m.poster_url, m.backdrop_url, m.media_type, m.metadata_status, m.created_at AS media_created_at, m.updated_at AS media_updated_at, r.provider_key AS resource_provider_key, r.title AS resource_title, r.download_url, r.info_hash, r.size_bytes, r.resolution, r.subtitle_languages_json, r.trackers_json, r.published_at, r.score, r.score_reasons_json, r.available, r.availability_status, r.codec, r.source_count, r.first_seen_at, r.last_seen_at, r.last_verified_at FROM acquisition a JOIN media m ON m.id = a.media_id LEFT JOIN resource r ON r.id = a.resource_id";

fn acquisition_from_row(row: &sqlx::sqlite::SqliteRow) -> Acquisition {
    let resource_id: Option<i64> = row.get("resource_id");
    Acquisition {
        id: row.get("id"),
        media_id: row.get("media_id"),
        resource_id,
        requested_by: row.get("requested_by"),
        state: row.get("state"),
        state_message: row.get("state_message"),
        qbit_hash: row.get("qbit_hash"),
        qbit_state: row.get("qbit_state"),
        progress: row.get("progress"),
        download_speed: row.get("download_speed"),
        eta_seconds: row.get("eta_seconds"),
        download_path: row.get("download_path"),
        library_item_id: row.get("library_item_id"),
        last_error: row.get("last_error"),
        retry_count: row.get("retry_count"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        completed_at: row.get("completed_at"),
        media: Media {
            id: row.get("media_id"),
            code: row.get("normalized_code"),
            title: row.get("media_title"),
            original_title: row.get("original_title"),
            summary: row.get("summary"),
            release_date: row.get("release_date"),
            duration_minutes: row.get("duration_minutes"),
            poster_url: row.get("poster_url"),
            backdrop_url: row.get("backdrop_url"),
            media_type: row.get("media_type"),
            metadata_status: row.get("metadata_status"),
            created_at: row.get("media_created_at"),
            updated_at: row.get("media_updated_at"),
        },
        resource: resource_id.map(|id| Resource {
            id,
            media_id: row.get("media_id"),
            provider_key: row.get("resource_provider_key"),
            title: row.get("resource_title"),
            download_url: row.get("download_url"),
            info_hash: row.get("info_hash"),
            size_bytes: row.get("size_bytes"),
            resolution: row.get("resolution"),
            subtitle_languages: parse_string_vec(&row.get::<String, _>("subtitle_languages_json")),
            trackers: parse_string_vec(&row.get::<String, _>("trackers_json")),
            published_at: row.get("published_at"),
            score: row.get("score"),
            score_reasons: parse_string_vec(&row.get::<String, _>("score_reasons_json")),
            available: row.get::<i64, _>("available") != 0,
            availability_status: row.get("availability_status"),
            codec: row.get("codec"),
            source_count: row.get("source_count"),
            first_seen_at: row.get("first_seen_at"),
            last_seen_at: row.get("last_seen_at"),
            last_verified_at: row.get("last_verified_at"),
            qbit_hash: row.try_get("qbit_hash").unwrap_or(None),
            qbit_state: row.try_get("qbit_state").unwrap_or(None),
            qbit_sync_status: "unknown".into(),
            acquisition_id: Some(row.get("id")),
            acquisition_state: Some(row.get("state")),
        }),
    }
}

fn media_from_row(row: &sqlx::sqlite::SqliteRow) -> Media {
    let stored_code: String = row.get("normalized_code");
    let title: String = row.get("title");
    let legacy_filename: Option<String> = row.try_get("legacy_filename").unwrap_or(None);
    let legacy_provider_id: Option<String> = row.try_get("legacy_provider_id").unwrap_or(None);
    let poster_url: Option<String> = row.get("poster_url");
    let legacy_media_item_id: Option<i64> = row.try_get("legacy_media_item_id").unwrap_or(None);
    Media {
        id: row.get("id"),
        code: display_media_code(
            &stored_code,
            &title,
            legacy_filename.as_deref(),
            legacy_provider_id.as_deref(),
        ),
        title,
        original_title: row.get("original_title"),
        summary: row.get("summary"),
        release_date: row.get("release_date"),
        duration_minutes: row.get("duration_minutes"),
        poster_url: poster_url
            .or_else(|| legacy_media_item_id.map(|id| format!("/asset/cover/{id}"))),
        backdrop_url: row.get("backdrop_url"),
        media_type: row.get("media_type"),
        metadata_status: row.get("metadata_status"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn actor_from_row(row: &sqlx::sqlite::SqliteRow) -> Actor {
    Actor {
        id: row.get("id"),
        name: row.get("name"),
        aliases: parse_string_vec(&row.get::<String, _>("aliases_json")),
        avatar_url: row.get("avatar_url"),
        followed: row.get::<i64, _>("followed") != 0,
        media_count: row.get("media_count"),
    }
}

fn resource_from_row(row: &sqlx::sqlite::SqliteRow) -> Resource {
    Resource {
        id: row.get("id"),
        media_id: row.get("media_id"),
        provider_key: row.get("provider_key"),
        title: row.get("title"),
        download_url: row.get("download_url"),
        info_hash: row.get("info_hash"),
        size_bytes: row.get("size_bytes"),
        resolution: row.get("resolution"),
        subtitle_languages: parse_string_vec(&row.get::<String, _>("subtitle_languages_json")),
        trackers: parse_string_vec(&row.get::<String, _>("trackers_json")),
        published_at: row.get("published_at"),
        score: row.get("score"),
        score_reasons: parse_string_vec(&row.get::<String, _>("score_reasons_json")),
        available: row.get::<i64, _>("available") != 0,
        availability_status: row.get("availability_status"),
        codec: row.get("codec"),
        source_count: row.get("source_count"),
        first_seen_at: row.get("first_seen_at"),
        last_seen_at: row.get("last_seen_at"),
        last_verified_at: row.get("last_verified_at"),
        qbit_hash: row.try_get("acquisition_qbit_hash").unwrap_or(None),
        qbit_state: None,
        qbit_sync_status: "unknown".into(),
        acquisition_id: row.try_get("acquisition_id").unwrap_or(None),
        acquisition_state: row.try_get("acquisition_state").unwrap_or(None),
    }
}

fn library_json(row: &sqlx::sqlite::SqliteRow) -> Value {
    let stored_code: String = row.get("normalized_code");
    let title: String = row.get("title");
    let code = display_media_code(
        &stored_code,
        &title,
        row.try_get::<Option<String>, _>("legacy_filename")
            .unwrap_or(None)
            .as_deref(),
        row.try_get::<Option<String>, _>("legacy_provider_id")
            .unwrap_or(None)
            .as_deref(),
    );
    let poster_url = row.get::<Option<String>, _>("poster_url").or_else(|| {
        row.get::<Option<i64>, _>("legacy_media_item_id")
            .map(|id| format!("/asset/cover/{id}"))
    });
    json!({"id":row.get::<i64,_>("id"),"mediaId":row.get::<i64,_>("media_id"),"acquisitionId":row.get::<Option<i64>,_>("acquisition_id"),"videoPath":row.get::<String,_>("video_path"),"nfoPath":row.get::<Option<String>,_>("nfo_path"),"posterPath":row.get::<Option<String>,_>("poster_path"),"status":row.get::<String,_>("status"),"fileSize":row.get::<Option<i64>,_>("file_size"),"addedAt":row.get::<String,_>("added_at"),"media":{"code":code,"title":title,"posterUrl":poster_url,"releaseDate":row.get::<Option<String>,_>("release_date"),"metadataStatus":row.get::<String,_>("metadata_status")}})
}
fn attention_json(row: &sqlx::sqlite::SqliteRow) -> Value {
    json!({"id":row.get::<i64,_>("id"),"kind":row.get::<String,_>("kind"),"severity":row.get::<String,_>("severity"),"title":row.get::<String,_>("title"),"message":row.get::<String,_>("message"),"acquisitionId":row.get::<Option<i64>,_>("acquisition_id"),"mediaId":row.get::<Option<i64>,_>("media_id"),"mediaTitle":row.get::<Option<String>,_>("media_title"),"mediaCode":row.get::<Option<String>,_>("normalized_code"),"actions":parse_string_vec(&row.get::<String,_>("actions_json")),"createdAt":row.get::<String,_>("created_at")})
}
fn automation_json(row: &sqlx::sqlite::SqliteRow) -> Value {
    json!({"id":row.get::<i64,_>("id"),"name":row.get::<String,_>("name"),"enabled":row.get::<i64,_>("enabled") != 0,"triggerType":row.get::<String,_>("trigger_type"),"triggerConfig":parse_json(&row.get::<String,_>("trigger_config_json"),json!({})),"conditions":parse_json(&row.get::<String,_>("conditions_json"),json!([])),"actionType":row.get::<String,_>("action_type"),"actionConfig":parse_json(&row.get::<String,_>("action_config_json"),json!({})),"mode":row.get::<String,_>("mode"),"lastRunAt":row.get::<Option<String>,_>("last_run_at"),"nextRunAt":row.get::<Option<String>,_>("next_run_at"),"lastStatus":row.get::<Option<String>,_>("last_status"),"lastExplanation":row.get::<Option<String>,_>("last_explanation"),"createdAt":row.get::<String,_>("created_at")})
}
fn provider_json(row: &sqlx::sqlite::SqliteRow) -> Value {
    let secret: String = row.get("secret");
    let sync_cursor = row
        .try_get::<Option<String>, _>("sync_cursor_json")
        .ok()
        .flatten()
        .map(|value| parse_json(&value, json!({})))
        .unwrap_or_else(|| json!({}));
    json!({
        "key":row.get::<String,_>("provider_key"),
        "type":row.get::<String,_>("provider_type"),
        "displayName":row.get::<String,_>("display_name"),
        "enabled":row.get::<i64,_>("enabled") != 0,
        "baseUrl":row.get::<String,_>("base_url"),
        "hasSecret":!secret.is_empty(),
        "config":parse_json(&row.get::<String,_>("config_json"),json!({})),
        "lastStatus":row.get::<String,_>("last_status"),
        "lastMessage":row.get::<String,_>("last_message"),
        "lastCheckedAt":row.get::<Option<String>,_>("last_checked_at"),
        "syncStatus":row.try_get::<Option<String>,_>("sync_status").ok().flatten().unwrap_or_else(|| "idle".into()),
        "syncActiveMode":row.try_get::<Option<String>,_>("sync_active_mode").ok().flatten().unwrap_or_else(|| "incremental".into()),
        "syncBootstrapPaused":row.try_get::<Option<i64>,_>("sync_bootstrap_paused").ok().flatten().unwrap_or(0) != 0,
        "syncBootstrapFrom":row.try_get::<Option<String>,_>("sync_bootstrap_from").ok().flatten(),
        "syncBootstrapTo":row.try_get::<Option<String>,_>("sync_bootstrap_to").ok().flatten(),
        "syncCursor":sync_cursor,
        "syncLastStartedAt":row.try_get::<Option<String>,_>("sync_last_started_at").ok().flatten(),
        "syncLastFinishedAt":row.try_get::<Option<String>,_>("sync_last_finished_at").ok().flatten(),
        "syncLastSuccessAt":row.try_get::<Option<String>,_>("sync_last_success_at").ok().flatten(),
        "syncNextRunAt":row.try_get::<Option<String>,_>("sync_next_run_at").ok().flatten(),
        "syncLastMessage":row.try_get::<Option<String>,_>("sync_last_message").ok().flatten().unwrap_or_default(),
        "syncFailureCount":row.try_get::<Option<i64>,_>("sync_failure_count").ok().flatten().unwrap_or(0),
        "syncItemCount":row.try_get::<Option<i64>,_>("sync_item_count").ok().flatten().unwrap_or(0),
        "syncInsertedCount":row.try_get::<Option<i64>,_>("sync_inserted_count").ok().flatten().unwrap_or(0),
        "syncUpdatedCount":row.try_get::<Option<i64>,_>("sync_updated_count").ok().flatten().unwrap_or(0),
        "syncDiscoveryCount":row.try_get::<Option<i64>,_>("sync_discovery_count").ok().flatten().unwrap_or(0),
        "syncHydratedCount":row.try_get::<Option<i64>,_>("sync_hydrated_count").ok().flatten().unwrap_or(0),
        "syncHydrationFailedCount":row.try_get::<Option<i64>,_>("sync_hydration_failed_count").ok().flatten().unwrap_or(0),
        "syncPendingCount":row.try_get::<Option<i64>,_>("sync_pending_count").ok().flatten().unwrap_or(0),
    })
}

async fn provider_enabled(state: &AppState, key: &str) -> AppResult<bool> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE((SELECT enabled FROM provider_config WHERE provider_key = ?), 0)",
    )
    .bind(key)
    .fetch_one(&state.pool)
    .await?
        != 0)
}
async fn media_exists(state: &AppState, id: i64) -> AppResult<()> {
    if sqlx::query_scalar::<_, i64>("SELECT EXISTS(SELECT 1 FROM media WHERE id = ?)")
        .bind(id)
        .fetch_one(&state.pool)
        .await?
        == 0
    {
        Err(AppError::NotFound)
    } else {
        Ok(())
    }
}
async fn setting(state: &AppState, key: &str, fallback: &str) -> AppResult<String> {
    Ok(
        sqlx::query_scalar::<_, String>("SELECT value FROM app_setting WHERE key = ?")
            .bind(key)
            .fetch_optional(&state.pool)
            .await?
            .unwrap_or_else(|| fallback.into()),
    )
}
async fn save_setting(state: &AppState, key: &str, value: &str) -> AppResult<()> {
    sqlx::query("INSERT INTO app_setting(key,value,updated_at) VALUES(?,?,datetime('now')) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=datetime('now')").bind(key).bind(value).execute(&state.pool).await?;
    Ok(())
}
async fn sync_provider_legacy_setting(
    state: &AppState,
    key: &str,
    url: &str,
    secret: &str,
) -> AppResult<()> {
    match key {
        "metatube" => {
            save_setting(state, "metatube_url", url).await?;
            if !secret.is_empty() {
                save_setting(state, "metatube_token", secret).await?;
            }
        }
        "qbittorrent" => {
            save_setting(state, "qbittorrent_url", url).await?;
            if !secret.is_empty() {
                save_setting(state, "qbittorrent_password", secret).await?;
            }
        }
        _ => {}
    }
    Ok(())
}
fn validate_http_url(value: &str) -> AppResult<()> {
    let url = reqwest::Url::parse(value.trim())
        .map_err(|_| AppError::BadRequest("Provider 地址无效".into()))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(AppError::BadRequest(
            "Provider 地址必须使用 http 或 https".into(),
        ));
    }
    Ok(())
}

fn validate_source_transport_config(config: &Value) -> AppResult<()> {
    let Some(config) = config.as_object() else {
        return Ok(());
    };
    if config
        .get("fetchMode")
        .and_then(Value::as_str)
        .is_some_and(|value| !matches!(value, "auto" | "http" | "browser"))
    {
        return Err(AppError::BadRequest(
            "访问方式必须是 auto、http 或 browser".into(),
        ));
    }
    if let Some(proxy_url) = config
        .get("proxyUrl")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let url = reqwest::Url::parse(proxy_url)
            .map_err(|_| AppError::BadRequest("代理 URL 无效".into()))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(AppError::BadRequest(
                "代理 URL 必须使用 http 或 https".into(),
            ));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(AppError::BadRequest(
                "代理 URL 暂不允许包含账号密码，避免凭据随 Provider 配置返回".into(),
            ));
        }
    }
    if config
        .get("userAgent")
        .and_then(Value::as_str)
        .is_some_and(|value| value.contains(['\r', '\n']))
    {
        return Err(AppError::BadRequest("User-Agent 不能包含换行".into()));
    }
    if config
        .get("syncIntervalMinutes")
        .and_then(Value::as_i64)
        .is_some_and(|minutes| !(60..=10080).contains(&minutes))
    {
        return Err(AppError::BadRequest(
            "来源同步间隔必须在 60 到 10080 分钟之间".into(),
        ));
    }
    if config
        .get("syncDetailLimit")
        .and_then(Value::as_i64)
        .is_some_and(|limit| !(0..=40).contains(&limit))
    {
        return Err(AppError::BadRequest(
            "每轮详情数量必须在 0 到 40 之间".into(),
        ));
    }
    Ok(())
}
fn emit(state: &AppState, event: &str, data: Value) {
    let _ = state
        .events
        .send(json!({"event":event,"data":data,"at":now_iso()}).to_string());
}
fn parse_string_vec(value: &str) -> Vec<String> {
    serde_json::from_str(value).unwrap_or_default()
}
fn parse_json(value: &str, fallback: Value) -> Value {
    serde_json::from_str(value).unwrap_or(fallback)
}
fn normalize_alias(value: &str) -> String {
    value
        .trim()
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|character| !character.is_whitespace())
        .collect()
}
#[cfg(test)]
fn detect_alias_locale(value: &str) -> &'static str {
    let has_kana = value
        .chars()
        .any(|character| matches!(character, '\u{3040}'..='\u{30ff}' | '\u{31f0}'..='\u{31ff}'));
    if has_kana {
        return "ja";
    }
    if value
        .chars()
        .any(|character| matches!(character, '\u{3400}'..='\u{4dbf}' | '\u{4e00}'..='\u{9fff}'))
    {
        return "zh";
    }
    if value.chars().any(|character| character.is_alphabetic()) {
        return "en";
    }
    "und"
}

#[cfg(test)]
fn alias_locale<'a>(value: &str, explicit: Option<&'a str>) -> &'a str {
    explicit
        .filter(|locale| matches!(*locale, "ja" | "zh" | "en" | "und"))
        .unwrap_or_else(|| detect_alias_locale(value))
}

fn push_localized_json_string(
    aliases: &mut Vec<(String, &'static str)>,
    value: &Value,
    keys: &[&str],
    locale: &'static str,
) {
    if let Some(alias) = first_json_string(value, keys) {
        let normalized = normalize_alias(alias);
        if !normalized.is_empty()
            && !aliases
                .iter()
                .any(|(existing, _)| normalize_alias(existing) == normalized)
        {
            aliases.push((alias.trim().to_owned(), locale));
        }
    }
}

fn localized_media_titles(value: &Value) -> Vec<(String, &'static str)> {
    let mut aliases = Vec::new();
    push_localized_json_string(
        &mut aliases,
        value,
        &["title_ja", "title_jp", "japanese_title"],
        "ja",
    );
    push_localized_json_string(
        &mut aliases,
        value,
        &["title_zh", "title_cn", "chinese_title"],
        "zh",
    );
    push_localized_json_string(&mut aliases, value, &["title_en", "english_title"], "en");
    aliases
}

fn localized_actor_names(value: &Value) -> Vec<(String, &'static str)> {
    let mut aliases = Vec::new();
    push_localized_json_string(
        &mut aliases,
        value,
        &["name_ja", "name_jp", "japanese_name"],
        "ja",
    );
    push_localized_json_string(
        &mut aliases,
        value,
        &["name_zh", "name_cn", "chinese_name"],
        "zh",
    );
    push_localized_json_string(&mut aliases, value, &["name_en", "english_name"], "en");
    aliases
}

#[cfg(test)]
async fn upsert_media_title_alias(
    state: &AppState,
    media_id: i64,
    alias: &str,
    locale: Option<&str>,
    source_key: &str,
    is_primary: bool,
) -> anyhow::Result<()> {
    let alias = alias.trim();
    let normalized = normalize_alias(alias);
    if normalized.is_empty() {
        return Ok(());
    }
    sqlx::query("INSERT INTO media_title_alias(media_id,locale,alias,normalized_alias,source_key,is_primary) VALUES (?,?,?,?,?,?) ON CONFLICT(media_id,normalized_alias) DO UPDATE SET alias=excluded.alias,locale=CASE WHEN excluded.locale!='und' THEN excluded.locale ELSE media_title_alias.locale END,source_key=excluded.source_key,is_primary=MAX(media_title_alias.is_primary,excluded.is_primary),updated_at=datetime('now')")
        .bind(media_id).bind(alias_locale(alias, locale)).bind(alias).bind(normalized).bind(source_key).bind(is_primary).execute(&state.pool).await?;
    refresh_media_search_document(state, media_id).await?;
    Ok(())
}
#[cfg(test)]
async fn upsert_actor_name_alias(
    state: &AppState,
    actor_id: i64,
    alias: &str,
    locale: Option<&str>,
    source_key: &str,
    is_primary: bool,
) -> anyhow::Result<()> {
    let alias = alias.trim();
    let normalized = normalize_alias(alias);
    if normalized.is_empty() {
        return Ok(());
    }
    sqlx::query("INSERT INTO actor_name_alias(actor_id,locale,alias,normalized_alias,source_key,is_primary) VALUES (?,?,?,?,?,?) ON CONFLICT(actor_id,normalized_alias) DO UPDATE SET alias=excluded.alias,locale=CASE WHEN excluded.locale!='und' THEN excluded.locale ELSE actor_name_alias.locale END,source_key=excluded.source_key,is_primary=MAX(actor_name_alias.is_primary,excluded.is_primary),updated_at=datetime('now')")
        .bind(actor_id).bind(alias_locale(alias, locale)).bind(alias).bind(&normalized).bind(source_key).bind(is_primary).execute(&state.pool).await?;
    let aliases: String = sqlx::query_scalar("SELECT COALESCE(json_group_array(alias),'[]') FROM actor_name_alias WHERE actor_id=? AND is_primary=0")
        .bind(actor_id).fetch_one(&state.pool).await?;
    sqlx::query("UPDATE actor SET aliases_json=?,updated_at=datetime('now') WHERE id=?")
        .bind(aliases)
        .bind(actor_id)
        .execute(&state.pool)
        .await?;
    refresh_actor_search_document(state, actor_id).await?;
    refresh_media_documents_for_actor(state, actor_id).await?;
    Ok(())
}

async fn refresh_media_search_document(state: &AppState, media_id: i64) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO media_search_document(media_id,code,title,original_title,aliases,actors,resources,updated_at) SELECT m.id,m.normalized_code,m.title,COALESCE(m.original_title,''),COALESCE((SELECT group_concat(alias,' ') FROM media_title_alias WHERE media_id=m.id),''),COALESCE((SELECT group_concat(actor_text,' ') FROM (SELECT a.name || ' ' || COALESCE((SELECT group_concat(alias,' ') FROM actor_name_alias WHERE actor_id=a.id),'') AS actor_text FROM media_actor ma JOIN actor a ON a.id=ma.actor_id WHERE ma.media_id=m.id)),''),COALESCE((SELECT group_concat(title,' ') FROM resource WHERE media_id=m.id AND available=1),'') ,datetime('now') FROM media m WHERE m.id=? ON CONFLICT(media_id) DO UPDATE SET code=excluded.code,title=excluded.title,original_title=excluded.original_title,aliases=excluded.aliases,actors=excluded.actors,resources=excluded.resources,updated_at=datetime('now')")
        .bind(media_id)
        .execute(&state.pool)
        .await?;
    Ok(())
}

#[cfg(test)]
async fn refresh_actor_search_document(state: &AppState, actor_id: i64) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO actor_search_document(actor_id,name,aliases,updated_at) SELECT a.id,a.name,COALESCE((SELECT group_concat(alias,' ') FROM actor_name_alias WHERE actor_id=a.id),''),datetime('now') FROM actor a WHERE a.id=? ON CONFLICT(actor_id) DO UPDATE SET name=excluded.name,aliases=excluded.aliases,updated_at=datetime('now')")
        .bind(actor_id)
        .execute(&state.pool)
        .await?;
    Ok(())
}

#[cfg(test)]
async fn refresh_media_documents_for_actor(state: &AppState, actor_id: i64) -> anyhow::Result<()> {
    let media_ids = sqlx::query_scalar::<_, i64>(
        "SELECT media_id FROM media_actor WHERE actor_id=? ORDER BY media_id",
    )
    .bind(actor_id)
    .fetch_all(&state.pool)
    .await?;
    for media_id in media_ids {
        refresh_media_search_document(state, media_id).await?;
    }
    Ok(())
}
#[cfg(test)]
async fn upsert_actor_with_aliases(
    state: &AppState,
    primary_name: &str,
    aliases: &[String],
    avatar_url: Option<&str>,
    source_key: &str,
) -> anyhow::Result<i64> {
    let mut names = Vec::new();
    push_search_term(&mut names, primary_name);
    for alias in aliases {
        push_search_term(&mut names, alias);
    }
    let mut actor_id = None;
    for name in &names {
        actor_id = sqlx::query_scalar("SELECT actor_id FROM actor_name_alias WHERE normalized_alias=? ORDER BY is_primary DESC,id LIMIT 1")
            .bind(normalize_alias(name)).fetch_optional(&state.pool).await?;
        if actor_id.is_some() {
            break;
        }
    }
    let actor_id = match actor_id {
        Some(id) => {
            sqlx::query("UPDATE actor SET avatar_url=COALESCE(?,avatar_url),updated_at=datetime('now') WHERE id=?")
                .bind(avatar_url).bind(id).execute(&state.pool).await?;
            id
        }
        None => sqlx::query("INSERT INTO actor(normalized_name,name,avatar_url) VALUES (?,?,?) ON CONFLICT(normalized_name) DO UPDATE SET avatar_url=COALESCE(excluded.avatar_url,actor.avatar_url),updated_at=datetime('now') RETURNING id")
            .bind(normalize_alias(primary_name)).bind(primary_name.trim()).bind(avatar_url).fetch_one(&state.pool).await?.get("id"),
    };
    for (index, name) in names.iter().enumerate() {
        upsert_actor_name_alias(state, actor_id, name, None, source_key, index == 0).await?;
    }
    Ok(actor_id)
}
fn first_json_string<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| {
        value
            .get(key)
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
    })
}
fn display_media_code(
    stored_code: &str,
    title: &str,
    legacy_filename: Option<&str>,
    legacy_provider_id: Option<&str>,
) -> String {
    legacy_provider_id
        .filter(|value| !value.eq_ignore_ascii_case("local:nfo"))
        .and_then(extract_media_code)
        .or_else(|| legacy_filename.and_then(extract_media_code))
        .or_else(|| extract_media_code(stored_code))
        .or_else(|| extract_media_code(title))
        .unwrap_or_else(|| {
            let normalized_title = normalize_code(title);
            (!stored_code.eq_ignore_ascii_case(title) && stored_code != normalized_title)
                .then(|| stored_code.to_ascii_uppercase())
                .unwrap_or_default()
        })
}

pub(crate) fn extract_media_code(value: &str) -> Option<String> {
    let uppercase = value.to_ascii_uppercase();
    let bytes = uppercase.as_bytes();
    if let Some(fc2_at) = uppercase.find("FC2") {
        let tail = &uppercase[fc2_at + 3..];
        if let Some(digit_at) = tail.find(|character: char| character.is_ascii_digit()) {
            let digits = tail[digit_at..]
                .chars()
                .take_while(|character| character.is_ascii_digit())
                .collect::<String>();
            if digits.len() >= 5 {
                return Some(format!("FC2-PPV-{digits}"));
            }
        }
    }
    let mut index = 0;
    while index < bytes.len() {
        if !bytes[index].is_ascii_alphabetic() {
            index += 1;
            continue;
        }
        let prefix_start = index;
        while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
            index += 1;
        }
        let prefix = &uppercase[prefix_start..index];
        if !(2..=10).contains(&prefix.len()) {
            continue;
        }
        while index < bytes.len() && matches!(bytes[index], b'-' | b'_' | b'.' | b' ') {
            index += 1;
        }
        let digits_start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        let digits = &uppercase[digits_start..index];
        if (2..=8).contains(&digits.len())
            && !matches!(prefix, "HD" | "FHD" | "UHD" | "AVC" | "HEVC")
        {
            return Some(format!("{prefix}-{digits}"));
        }
    }
    None
}
pub(crate) fn normalize_code(value: &str) -> String {
    value
        .trim()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .map(|c| {
            if c == '_' {
                '-'
            } else {
                c.to_ascii_lowercase()
            }
        })
        .collect()
}
fn char_boundary_before(value: &str, index: usize) -> usize {
    let mut index = index.min(value.len());
    while index > 0 && !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}
fn strip_tags(value: &str) -> String {
    let mut out = String::new();
    let mut inside = false;
    for c in value.chars() {
        match c {
            '<' => inside = true,
            '>' => inside = false,
            _ if !inside => out.push(c),
            _ => {}
        }
    }
    html_unescape(out.trim())
}
fn extract_attribute(text: &str, marker: &str) -> Option<String> {
    let at = text.find(marker)? + marker.len();
    let quote = text
        .as_bytes()
        .get(at)
        .copied()
        .filter(|v| *v == b'\'' || *v == b'"');
    let start = if quote.is_some() { at + 1 } else { at };
    let end = text[start..].find(|c: char| {
        quote
            .map(|q| c as u8 == q)
            .unwrap_or(c.is_whitespace() || c == '>')
    })?;
    Some(html_unescape(&text[start..start + end]))
}
fn html_unescape(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
}
fn safe_segment(value: &str) -> String {
    let cleaned = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect::<String>();
    if cleaned.trim_matches(['.', '_']).is_empty() {
        "untitled".into()
    } else {
        cleaned
    }
}
fn map_download_path(path: &Path, qbit_root: &Path, local_root: &Path) -> AppResult<PathBuf> {
    if let Ok(relative) = path.strip_prefix(qbit_root) {
        Ok(local_root.join(relative))
    } else if path.starts_with(local_root) {
        Ok(path.to_path_buf())
    } else {
        Err(AppError::BadRequest(format!(
            "qBittorrent 路径 {} 无法映射到下载根目录 {}",
            path.display(),
            local_root.display()
        )))
    }
}
fn select_video(path: &Path) -> Option<PathBuf> {
    if path.is_file() {
        return is_video(path).then(|| path.to_path_buf());
    }
    WalkDir::new(path)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file() && is_video(e.path()))
        .filter_map(|e| e.metadata().ok().map(|m| (m.len(), e.into_path())))
        .max_by_key(|(size, _)| *size)
        .map(|(_, p)| p)
}
fn is_video(path: &Path) -> bool {
    path.extension()
        .and_then(|v| v.to_str())
        .is_some_and(|v| VIDEO_EXTENSIONS.contains(&v.to_ascii_lowercase().as_str()))
}
fn ensure_within(path: &Path, root: &Path) -> AppResult<()> {
    let canonical = std::fs::canonicalize(path).map_err(|e| AppError::BadRequest(e.to_string()))?;
    let root = std::fs::canonicalize(root).map_err(|e| AppError::BadRequest(e.to_string()))?;
    if canonical.starts_with(root) {
        Ok(())
    } else {
        Err(AppError::BadRequest("下载文件超出允许目录".into()))
    }
}
fn ensure_lexically_within(path: &Path, root: &Path) -> AppResult<()> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| AppError::BadRequest("目标文件超出媒体根目录".into()))?;
    if relative.components().any(|c| {
        matches!(
            c,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        Err(AppError::BadRequest("目标路径包含非法片段".into()))
    } else {
        Ok(())
    }
}
fn chrono_like_nonce() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
fn now_iso() -> String {
    format!(
        "{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_acquisition_state_machine() {
        assert!(legal_transition("REQUESTED", "RESOURCE_RESOLVING"));
        assert!(legal_transition("DOWNLOADING", "NEEDS_ATTENTION"));
        assert!(!legal_transition("REQUESTED", "COMPLETED"));
        assert!(!legal_transition("COMPLETED", "CANCELLED"));
    }

    #[test]
    fn qbit_reconciliation_revives_cancelled_or_attention_downloads() {
        assert_eq!(
            reconciled_acquisition_state("CANCELLED", false),
            Some("DOWNLOADING")
        );
        assert_eq!(
            reconciled_acquisition_state("NEEDS_ATTENTION", false),
            Some("DOWNLOADING")
        );
        assert_eq!(
            reconciled_acquisition_state("CANCELLED", true),
            Some("DOWNLOADED")
        );
        assert_eq!(reconciled_acquisition_state("DOWNLOADING", false), None);
        assert_eq!(reconciled_acquisition_state("PROCESSING", false), None);
        assert_eq!(reconciled_acquisition_state("COMPLETED", false), None);
    }

    #[test]
    fn qbit_completion_recognizes_seeding_states() {
        assert!(torrent_state_is_complete(0.5, 0, "stalledUP"));
        assert!(torrent_state_is_complete(1.0, 0, "pausedDL"));
        assert!(!torrent_state_is_complete(0.5, 0, "stalledDL"));
    }

    #[test]
    fn live_qbit_task_makes_retry_idempotent() {
        assert!(acquisition_has_live_qbit_task(
            Some("abcdef0123456789abcdef0123456789abcdef01"),
            Some("stalledDL")
        ));
        assert!(!acquisition_has_live_qbit_task(
            Some("abcdef0123456789abcdef0123456789abcdef01"),
            Some("missing")
        ));
        assert!(!acquisition_has_live_qbit_task(None, Some("downloading")));
    }

    #[tokio::test]
    async fn qbit_reconciliation_resolves_only_stale_qbit_attention() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let media_id: i64 = sqlx::query(
            "INSERT INTO media(normalized_code,title) VALUES ('stars-123','STARS-123') RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .get("id");
        let acquisition_id: i64 = sqlx::query(
            "INSERT INTO acquisition(media_id,state) VALUES (?,'DOWNLOADING') RETURNING id",
        )
        .bind(media_id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get("id");
        for kind in [
            "provider_unavailable",
            "qbit_task_missing",
            "organizer_conflict",
        ] {
            sqlx::query("INSERT INTO attention_item(kind,title,message,acquisition_id,media_id) VALUES (?,'problem','problem',?,?)")
                .bind(kind)
                .bind(acquisition_id)
                .bind(media_id)
                .execute(&pool)
                .await
                .unwrap();
        }

        sqlx::query(RESOLVE_QBIT_ATTENTION)
            .bind(acquisition_id)
            .execute(&pool)
            .await
            .unwrap();

        let rows = sqlx::query("SELECT kind,status FROM attention_item ORDER BY kind")
            .fetch_all(&pool)
            .await
            .unwrap();
        let statuses = rows
            .iter()
            .map(|row| (row.get::<String, _>("kind"), row.get::<String, _>("status")))
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(statuses["provider_unavailable"], "resolved");
        assert_eq!(statuses["qbit_task_missing"], "resolved");
        assert_eq!(statuses["organizer_conflict"], "open");
    }

    #[test]
    fn ranker_explains_why_resource_wins() {
        let (score, reasons) = crate::resource::rank_resource(
            "ABC-123 4K 中文字幕",
            Some("5 GB"),
            "2026-08-01",
            "mock",
        );
        assert!(score > 90.0);
        assert!(reasons.iter().any(|reason| reason.contains("中文字幕")));
        assert!(reasons.iter().any(|reason| reason.contains("4K")));
    }

    #[test]
    fn legacy_media_code_comes_from_provider_or_filename() {
        assert_eq!(
            display_media_code(
                "完整作品名称",
                "完整作品名称",
                Some("SSIS-123-CD1.mkv"),
                None
            ),
            "SSIS-123"
        );
        assert_eq!(
            display_media_code(
                "fc2ppv4792609",
                "完整作品名称",
                Some("movie.mp4"),
                Some("FC2-PPV-4792609")
            ),
            "FC2-PPV-4792609"
        );
        assert_eq!(
            display_media_code("完整作品名称", "完整作品名称", None, None),
            ""
        );
    }

    #[test]
    fn path_mapping_rejects_unshared_qbit_path() {
        assert!(
            map_download_path(
                Path::new("/volume/downloads/a.mkv"),
                Path::new("/downloads"),
                Path::new("/downloads")
            )
            .is_err()
        );
        assert_eq!(
            map_download_path(
                Path::new("/downloads/a.mkv"),
                Path::new("/downloads"),
                Path::new("/downloads")
            )
            .unwrap(),
            PathBuf::from("/downloads/a.mkv")
        );
    }

    #[test]
    fn javbus_parser_deduplicates_media_links() {
        let html = r#"<a class="movie-box" href="/ABC-123"><div class="photo-frame"><img src="/cover.jpg" title="ABC-123 Title"></div></a><a class="movie-box" href="/ABC-123">duplicate</a>"#;
        let items = parse_javbus_search_html(html, "ABC-123", "https://www.javbus.com");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].provider_id, "ABC-123");
        assert_eq!(items[0].title, "ABC-123 Title");
        assert_eq!(
            items[0].poster_url.as_deref(),
            Some("https://www.javbus.com/cover.jpg")
        );
    }

    #[test]
    fn cached_catalogue_can_be_reparsed_without_network_access() {
        let html = r#"<a class="movie-box" href="/ABC-123"><div class="photo-frame"><img src="/cover.jpg" title="ABC-123 Offline title"></div></a>"#;
        let final_url = reqwest::Url::parse("https://www.javbus.com/").unwrap();
        let output = reparse_provider_snapshot(
            "javbus",
            "catalogue",
            &final_url,
            html,
            PageKind::ValidContent,
        );
        assert_eq!(output["parsed"], true);
        assert_eq!(output["candidateCount"], 1);
        assert_eq!(output["candidates"][0]["code"], "abc-123");
        assert_eq!(output["candidates"][0]["title"], "ABC-123 Offline title");
    }

    #[test]
    fn javbus_age_gate_is_detected() {
        assert!(is_javbus_age_page(
            "<title>Age Verification JavBus</title><a href='/doc/driver-verify'>verify</a>"
        ));
    }

    #[tokio::test]
    async fn on_demand_resolve_queues_one_high_priority_deduplicated_job() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        sqlx::query(
            "UPDATE provider_config SET enabled=CASE WHEN provider_key='javbus' THEN 1 ELSE 0 END WHERE provider_type='source'",
        )
        .execute(&pool)
        .await
        .unwrap();
        let temp =
            std::env::temp_dir().join(format!("luma-catalog-resolve-{}", chrono_like_nonce()));
        let state = AppState {
            pool: pool.clone(),
            scrape_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            crawler_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            asset_root: temp.join("assets"),
            script_root: temp.join("scripts"),
            events: tokio::sync::broadcast::channel(32).0,
            fetch_manager: std::sync::Arc::new(crate::fetch::FetchManager::default()),
            provider_registry: std::sync::Arc::new(crate::providers::ProviderRegistry::default()),
            snapshot_repository: std::sync::Arc::new(crate::ingestion::SnapshotRepository::new(
                pool.clone(),
                temp.join("source-cache"),
            )),
            ingestion_queue: crate::ingestion::IngestionQueue::new(pool.clone()),
            task_engine: crate::task::TaskEngine::new(pool.clone()),
            handler_registry: std::sync::Arc::new(crate::task::handler::HandlerRegistry::new()),
        };

        let first = resolve_catalog_code(
            State(state.clone()),
            Json(CatalogResolveInput {
                code: "ABC 123".into(),
                include_resources: true,
            }),
        )
        .await
        .unwrap()
        .0;
        let second = resolve_catalog_code(
            State(state.clone()),
            Json(CatalogResolveInput {
                code: "abc-123".into(),
                include_resources: true,
            }),
        )
        .await
        .unwrap()
        .0;

        assert_eq!(first.code, "abc-123");
        assert_eq!(first.status, "queued");
        assert_eq!(first.job_ids.len(), 1);
        assert_eq!(second.job_ids, first.job_ids);
        let row = sqlx::query(
            "SELECT provider_key,job_type,priority,status,payload_json,max_attempts FROM ingestion_job",
        )
        .fetch_one(&state.pool)
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("provider_key"), "javbus");
        assert_eq!(row.get::<String, _>("job_type"), "catalog_resolve");
        assert_eq!(row.get::<i64, _>("priority"), PRIORITY_USER_ON_DEMAND);
        assert_eq!(row.get::<String, _>("status"), "pending");
        assert_eq!(row.get::<i64, _>("max_attempts"), 2);
        let payload: Value = serde_json::from_str(&row.get::<String, _>("payload_json")).unwrap();
        assert_eq!(payload["code"], "abc-123");
        assert_eq!(payload["includeResources"], true);
    }

    #[test]
    fn historical_bootstrap_only_calls_resource_endpoint_for_recent_titles() {
        let today = chrono::Utc::now().date_naive();
        let recent = (today - chrono::Duration::days(30))
            .format("%Y-%m-%d")
            .to_string();
        let historical = (today - chrono::Duration::days(365))
            .format("%Y-%m-%d")
            .to_string();
        assert!(resource_date_is_recent(&recent, 180));
        assert!(!resource_date_is_recent(&historical, 180));
    }

    #[test]
    fn javdb_parser_deduplicates_media_links() {
        let html = r#"<a href="/v/abc"><div class="video-title">ABC-123 Title</div><img data-src="/cover.jpg"></a><a href="/v/abc">duplicate</a>"#;
        let items = parse_javdb_search_html(html, "ABC-123", "https://javdb.com");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].provider_id, "abc");
        assert_eq!(items[0].title, "ABC-123 Title");
    }

    #[tokio::test]
    async fn bootstrap_can_pause_and_resume_from_its_checkpoint() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO provider_config(provider_key,provider_type,display_name,enabled,base_url,config_json) VALUES ('mock-bootstrap','source','Mock Bootstrap',1,'https://example.test','{\"adapter\":\"javbus\"}')")
            .execute(&pool)
            .await
            .unwrap();
        let temp = std::env::temp_dir().join(format!("luma-bootstrap-{}", chrono_like_nonce()));
        let state = AppState {
            pool: pool.clone(),
            scrape_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            crawler_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            asset_root: temp.join("assets"),
            script_root: temp.join("scripts"),
            events: tokio::sync::broadcast::channel(32).0,
            fetch_manager: std::sync::Arc::new(crate::fetch::FetchManager::default()),
            provider_registry: std::sync::Arc::new(crate::providers::ProviderRegistry::default()),
            snapshot_repository: std::sync::Arc::new(crate::ingestion::SnapshotRepository::new(
                pool.clone(),
                temp.join("source-cache"),
            )),
            ingestion_queue: crate::ingestion::IngestionQueue::new(pool.clone()),
            task_engine: crate::task::TaskEngine::new(pool.clone()),
            handler_registry: std::sync::Arc::new(crate::task::handler::HandlerRegistry::new()),
        };

        let started = bootstrap_provider(
            State(state.clone()),
            AxumPath("mock-bootstrap".into()),
            Json(BootstrapInput {
                from: "2024-01-01".into(),
                to: "2026-08-10".into(),
                include_resources: false,
            }),
        )
        .await
        .unwrap()
        .0;
        let paused =
            pause_provider_bootstrap(State(state.clone()), AxumPath("mock-bootstrap".into()))
                .await
                .unwrap()
                .0;
        assert_eq!(paused.run_id, started.run_id);
        assert_eq!(paused.status, "pausing");
        let paused_flag: i64 = sqlx::query_scalar(
            "SELECT bootstrap_paused FROM source_sync_state WHERE provider_key='mock-bootstrap'",
        )
        .fetch_one(&state.pool)
        .await
        .unwrap();
        assert_eq!(paused_flag, 1);

        let resumed =
            resume_provider_bootstrap(State(state.clone()), AxumPath("mock-bootstrap".into()))
                .await
                .unwrap()
                .0;
        assert_eq!(resumed.run_id, started.run_id);
        assert_eq!(resumed.status, "running");
        let state_row = sqlx::query("SELECT bootstrap_paused,cursor_json FROM source_sync_state WHERE provider_key='mock-bootstrap'")
            .fetch_one(&state.pool)
            .await
            .unwrap();
        assert_eq!(state_row.get::<i64, _>("bootstrap_paused"), 0);
        let cursor: Value =
            serde_json::from_str(&state_row.get::<String, _>("cursor_json")).unwrap();
        assert_eq!(cursor["includeResources"], false);
        let queued: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM ingestion_job WHERE job_type='discovery' AND status='pending'",
        )
        .fetch_one(&state.pool)
        .await
        .unwrap();
        assert_eq!(queued, 1, "resume must reuse the pending checkpoint job");

        state
            .ingestion_queue
            .claim("worker-before-restart", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        state.ingestion_queue.recover_startup().await.unwrap();
        recover_interrupted_source_syncs(&state).await.unwrap();
        let recovered_run_status: String =
            sqlx::query_scalar("SELECT status FROM source_sync_run WHERE id=?")
                .bind(started.run_id)
                .fetch_one(&state.pool)
                .await
                .unwrap();
        let recovered_job_status: String = sqlx::query_scalar(
            "SELECT status FROM ingestion_job WHERE job_type='discovery' LIMIT 1",
        )
        .fetch_one(&state.pool)
        .await
        .unwrap();
        assert_eq!(recovered_run_status, "running");
        assert_eq!(recovered_job_status, "pending");

        sqlx::query("UPDATE ingestion_job SET status='succeeded',finished_at=datetime('now') WHERE job_type='discovery'")
            .execute(&state.pool)
            .await
            .unwrap();
        sqlx::query("UPDATE source_sync_state SET bootstrap_paused=1,cursor_json='{\"mode\":\"bootstrap\",\"nextUrl\":null,\"page\":2,\"from\":\"2024-01-01\",\"to\":\"2026-08-10\",\"includeResources\":false,\"hasMore\":false}' WHERE provider_key='mock-bootstrap'")
            .execute(&state.pool)
            .await
            .unwrap();
        let completed =
            resume_provider_bootstrap(State(state.clone()), AxumPath("mock-bootstrap".into()))
                .await
                .unwrap()
                .0;
        assert_eq!(completed.status, "success");
        let final_job_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM ingestion_job WHERE job_type='discovery'")
                .fetch_one(&state.pool)
                .await
                .unwrap();
        assert_eq!(final_job_count, 1, "completed checkpoints must not refetch");
    }

    #[tokio::test]
    async fn persistent_worker_sync_populates_the_local_search_index() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = vec![0_u8; 4096];
                let read = socket.read(&mut buffer).await.unwrap();
                let request = String::from_utf8_lossy(&buffer[..read]);
                let body = if request.starts_with("GET /ABC-123 ") {
                    r#"<meta property="og:title" content="ABC-123 Local detail"><meta property="og:image" content="/poster-large.jpg"><div>ABC-123 uncensored <a href="magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567">download</a></div>"#
                } else {
                    r#"<a class="movie-box" href="/ABC-123"><div class="photo-frame"><img src="/poster.jpg" title="ABC-123 Local title"></div></a>"#
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });

        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO provider_config(provider_key,provider_type,display_name,enabled,base_url,config_json) VALUES ('mock-javbus','source','Mock JavBus',1,?,'{\"adapter\":\"javbus\"}')")
            .bind(&base_url)
            .execute(&pool)
            .await
            .unwrap();
        let temp = std::env::temp_dir().join(format!("luma-source-sync-{}", chrono_like_nonce()));
        let state = AppState {
            pool: pool.clone(),
            scrape_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            crawler_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            asset_root: temp.join("assets"),
            script_root: temp.join("scripts"),
            events: tokio::sync::broadcast::channel(32).0,
            fetch_manager: std::sync::Arc::new(crate::fetch::FetchManager::default()),
            provider_registry: std::sync::Arc::new(crate::providers::ProviderRegistry::default()),
            snapshot_repository: std::sync::Arc::new(crate::ingestion::SnapshotRepository::new(
                pool.clone(),
                temp.join("source-cache"),
            )),
            ingestion_queue: crate::ingestion::IngestionQueue::new(pool.clone()),
            task_engine: crate::task::TaskEngine::new(pool.clone()),
            handler_registry: std::sync::Arc::new(crate::task::handler::HandlerRegistry::new()),
        };
        crate::ingestion::start_workers(state.clone())
            .await
            .unwrap();
        let first_run_id = start_incremental_run(&state, "mock-javbus", true)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let status: String =
                    sqlx::query_scalar("SELECT status FROM source_sync_run WHERE id=?")
                        .bind(first_run_id)
                        .fetch_one(&state.pool)
                        .await
                        .unwrap();
                if status == "success" {
                    break;
                }
                assert_ne!(status, "failed");
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("persistent source sync job did not finish");
        let sync_counts = sqlx::query(
            "SELECT status,item_count,inserted_count FROM source_sync_state WHERE provider_key='mock-javbus'",
        )
        .fetch_one(&state.pool)
        .await
        .unwrap();
        assert_eq!(sync_counts.get::<String, _>("status"), "success");
        assert_eq!(sync_counts.get::<i64, _>("item_count"), 1);
        assert_eq!(sync_counts.get::<i64, _>("inserted_count"), 1);
        let resource_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM resource")
            .fetch_one(&state.pool)
            .await
            .unwrap();
        assert_eq!(resource_count, 1);
        let second_run_id = start_incremental_run(&state, "mock-javbus", true)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let status: String =
                    sqlx::query_scalar("SELECT status FROM source_sync_run WHERE id=?")
                        .bind(second_run_id)
                        .fetch_one(&state.pool)
                        .await
                        .unwrap();
                if status == "success" {
                    break;
                }
                assert_ne!(status, "failed");
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("repeated incremental source sync did not finish");
        server.await.unwrap();
        let media_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media")
            .fetch_one(&state.pool)
            .await
            .unwrap();
        let repeated_resource_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM resource")
            .fetch_one(&state.pool)
            .await
            .unwrap();
        let discovery_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_discovery_item")
                .fetch_one(&state.pool)
                .await
                .unwrap();
        assert_eq!(media_count, 1);
        assert_eq!(repeated_resource_count, 1);
        assert_eq!(discovery_count, 1);
        let indexed_resources: String =
            sqlx::query_scalar("SELECT resources FROM media_search_document WHERE code='abc-123'")
                .fetch_one(&state.pool)
                .await
                .unwrap();
        assert!(indexed_resources.contains("uncensored"));
        let response = search(
            State(state),
            Query(SearchQuery {
                q: "ABC 123".into(),
                page: 1,
                page_size: 20,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(response.media.items.len(), 1);
        assert!(response.provider_reports.is_empty());
    }

    #[test]
    fn aliases_are_normalized_and_language_tagged() {
        assert_eq!(normalize_alias("  Sakura Mana \t"), "sakuramana");
        assert_eq!(detect_alias_locale("さくらまな"), "ja");
        assert_eq!(detect_alias_locale("纱仓真菜"), "zh");
        assert_eq!(detect_alias_locale("Mana Sakura"), "en");
    }

    #[test]
    fn source_transport_config_validates_proxy_and_user_agent() {
        assert!(
            validate_source_transport_config(&json!({
                "proxyUrl": "http://192.168.5.1:7890",
                "userAgent": "Mozilla/5.0 test"
            }))
            .is_ok()
        );
        assert!(
            validate_source_transport_config(&json!({"proxyUrl": "socks5://127.0.0.1:1080"}))
                .is_err()
        );
        assert!(
            validate_source_transport_config(
                &json!({"proxyUrl": "http://user:password@192.168.5.1:7890"})
            )
            .is_err()
        );
        assert!(
            validate_source_transport_config(&json!({"userAgent": "invalid\nheader"})).is_err()
        );
    }

    #[test]
    fn jav321_detail_parser_extracts_media() {
        let html = r#"<div class="panel-heading"><h3>Example title <small>abp-123</small></h3></div><div><img src="http://pics.dmm.co.jp/cover.jpg"><b>品番</b>: abp-123<br></div>"#;
        let url = reqwest::Url::parse("https://www.jav321.com/video/118abp00123").unwrap();
        let items = parse_jav321_html(html, "ABP-123", "https://www.jav321.com", &url);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].code, "abp-123");
        assert_eq!(items[0].title, "Example title");
        assert_eq!(
            items[0].poster_url.as_deref(),
            Some("https://pics.dmm.co.jp/cover.jpg")
        );
    }

    #[test]
    fn magnet_parser_handles_multibyte_labels() {
        let prefix = "中文字幕作品".repeat(80);
        let html = format!(
            "{prefix}<a href=\"magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567\">下载</a>"
        );
        let magnets = parse_magnets(&html);
        assert_eq!(magnets.len(), 1);
    }

    #[tokio::test]
    async fn migration_seeds_multiple_source_adapters() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let rows = sqlx::query(
            "SELECT provider_key, config_json FROM provider_config WHERE provider_type='source'",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(rows.len(), 4);
        let adapters = rows
            .iter()
            .map(|row| {
                parse_json(&row.get::<String, _>("config_json"), json!({}))["adapter"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert!(adapters.contains(&"javbus".to_owned()));
        assert!(adapters.contains(&"javdb".to_owned()));
        assert!(adapters.contains(&"jav321".to_owned()));
        assert!(adapters.contains(&"javlibrary".to_owned()));
        let enabled_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_config WHERE provider_type='source' AND enabled=1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(enabled_count, 4);
        for table in ["media_title_alias", "actor_name_alias"] {
            let exists: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            )
            .bind(table)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(exists, 1);
        }
    }

    #[tokio::test]
    async fn local_search_uses_multilingual_aliases_without_network_requests() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let temp = std::env::temp_dir().join(format!("luma-alias-test-{}", chrono_like_nonce()));
        let state = AppState {
            pool: pool.clone(),
            scrape_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            crawler_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            asset_root: temp.join("assets"),
            script_root: temp.join("scripts"),
            events: tokio::sync::broadcast::channel(32).0,
            fetch_manager: std::sync::Arc::new(crate::fetch::FetchManager::default()),
            provider_registry: std::sync::Arc::new(crate::providers::ProviderRegistry::default()),
            snapshot_repository: std::sync::Arc::new(crate::ingestion::SnapshotRepository::new(
                pool.clone(),
                temp.join("source-cache"),
            )),
            ingestion_queue: crate::ingestion::IngestionQueue::new(pool.clone()),
            task_engine: crate::task::TaskEngine::new(pool.clone()),
            handler_registry: std::sync::Arc::new(crate::task::handler::HandlerRegistry::new()),
        };
        sqlx::query("UPDATE provider_config SET base_url='http://127.0.0.1:9',enabled=1 WHERE provider_type='source'")
            .execute(&state.pool)
            .await
            .unwrap();
        let media_id: i64 = sqlx::query("INSERT INTO media(normalized_code,title) VALUES ('stars-123','5年振り出勤') RETURNING id")
            .fetch_one(&state.pool).await.unwrap().get("id");
        upsert_media_title_alias(&state, media_id, "5年振り出勤", Some("ja"), "test", true)
            .await
            .unwrap();
        upsert_media_title_alias(
            &state,
            media_id,
            "时隔五年再次出勤",
            Some("zh"),
            "test",
            false,
        )
        .await
        .unwrap();
        let actor_id = upsert_actor_with_aliases(
            &state,
            "紗倉まな",
            &["纱仓真菜".into(), "Mana Sakura".into()],
            None,
            "test",
        )
        .await
        .unwrap();
        upsert_actor_name_alias(&state, actor_id, "紗倉まな", Some("ja"), "test", true)
            .await
            .unwrap();
        sqlx::query("INSERT INTO media_actor(media_id,actor_id,billing_order) VALUES (?,?,0)")
            .bind(media_id)
            .bind(actor_id)
            .execute(&state.pool)
            .await
            .unwrap();
        refresh_media_search_document(&state, media_id)
            .await
            .unwrap();

        let chinese = search(
            State(state.clone()),
            Query(SearchQuery {
                q: "时隔五年再次出勤".into(),
                page: 1,
                page_size: 20,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(chinese.media.items[0].id, media_id);
        assert!(chinese.provider_reports.is_empty());
        let english = search(
            State(state.clone()),
            Query(SearchQuery {
                q: "Mana Sakura".into(),
                page: 1,
                page_size: 20,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(english.actors.items[0].id, actor_id);
        assert_eq!(english.media.items[0].id, media_id);
        assert!(english.provider_reports.is_empty());
        let locale: String = sqlx::query_scalar(
            "SELECT locale FROM actor_name_alias WHERE actor_id=? AND normalized_alias=?",
        )
        .bind(actor_id)
        .bind(normalize_alias("紗倉まな"))
        .fetch_one(&state.pool)
        .await
        .unwrap();
        assert_eq!(locale, "ja");
    }

    #[tokio::test]
    async fn duplicate_manual_requests_reuse_active_acquisition() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let stalled_qbit = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let qbit_url = format!("http://{}", stalled_qbit.local_addr().unwrap());
        sqlx::query("UPDATE app_setting SET value = ? WHERE key = 'qbittorrent_url'")
            .bind(qbit_url)
            .execute(&pool)
            .await
            .unwrap();
        let media_id: i64 = sqlx::query(
            "INSERT INTO media(normalized_code,title) VALUES ('abc-123','ABC-123') RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .get("id");
        let resource_id: i64 = sqlx::query("INSERT INTO resource(media_id,provider_key,title,download_url,score) VALUES (?,'mock','ABC-123 1080p','magnet:?xt=urn:btih:ABC123',90) RETURNING id").bind(media_id).fetch_one(&pool).await.unwrap().get("id");
        let temp = std::env::temp_dir().join(format!("luma-product-test-{}", chrono_like_nonce()));
        let state = AppState {
            pool: pool.clone(),
            scrape_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            crawler_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            asset_root: temp.join("assets"),
            script_root: temp.join("scripts"),
            events: tokio::sync::broadcast::channel(32).0,
            fetch_manager: std::sync::Arc::new(crate::fetch::FetchManager::default()),
            provider_registry: std::sync::Arc::new(crate::providers::ProviderRegistry::default()),
            snapshot_repository: std::sync::Arc::new(crate::ingestion::SnapshotRepository::new(
                pool.clone(),
                temp.join("source-cache"),
            )),
            ingestion_queue: crate::ingestion::IngestionQueue::new(pool.clone()),
            task_engine: crate::task::TaskEngine::new(pool.clone()),
            handler_registry: std::sync::Arc::new(crate::task::handler::HandlerRegistry::new()),
        };
        let first = request_acquisition(
            &state,
            AcquireInput {
                media_id: Some(media_id),
                resource_id: Some(resource_id),
                requested_by: "manual".into(),
            },
        )
        .await
        .unwrap();
        let second = request_acquisition(
            &state,
            AcquireInput {
                media_id: Some(media_id),
                resource_id: Some(resource_id),
                requested_by: "manual".into(),
            },
        )
        .await
        .unwrap();
        assert_eq!(first.id, second.id);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM acquisition WHERE media_id = ?")
            .bind(media_id)
            .fetch_one(&state.pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
    }
}
