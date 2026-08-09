use std::{
    collections::HashMap,
    convert::Infallible,
    path::{Component, Path, PathBuf},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Path as AxumPath, Query, State},
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Row, Sqlite, Transaction};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use walkdir::WalkDir;

use crate::{
    AppState,
    error::{AppError, AppResult},
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
        .route("/catalog/media", get(list_media))
        .route("/catalog/media/{id}", get(media_detail))
        .route("/catalog/media/{id}/resources", get(media_resources))
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
        .route("/acquisitions/{id}", get(acquisition_detail))
        .route("/acquisitions/{id}/pause", post(pause_acquisition))
        .route("/acquisitions/{id}/resume", post(resume_acquisition))
        .route("/acquisitions/{id}/retry", post(retry_acquisition))
        .route("/acquisitions/{id}/cancel", post(cancel_acquisition))
        .route("/library", get(list_library))
        .route("/library/{id}", get(library_detail))
        .route("/library/{id}/reorganize", post(reorganize_library))
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
struct SearchQuery {
    #[serde(default)]
    q: String,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    #[serde(default)]
    status: String,
    #[serde(default)]
    q: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SearchResponse {
    query: String,
    media: Vec<Media>,
    actors: Vec<Actor>,
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

async fn home(State(state): State<AppState>) -> AppResult<Json<Value>> {
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
    let recent_rows = sqlx::query(&format!("{ACQUISITION_SELECT} ORDER BY a.id DESC"))
        .fetch_all(&state.pool)
        .await?;
    let recent = recent_rows
        .iter()
        .take(6)
        .map(acquisition_from_row)
        .collect::<Vec<_>>();
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
    let term = query.q.trim();
    if term.is_empty() {
        return Ok(Json(SearchResponse {
            query: String::new(),
            media: Vec::new(),
            actors: Vec::new(),
            provider_reports: Vec::new(),
        }));
    }
    let mut reports = Vec::new();
    let providers = enabled_source_providers(&state).await?;
    let mut searches = tokio::task::JoinSet::new();
    for provider in providers {
        let source_state = state.clone();
        let source_term = term.to_owned();
        searches.spawn(async move {
            let key = provider.key.clone();
            let result = source_search(&source_state, &provider, &source_term).await;
            (key, result)
        });
    }
    while let Some(result) = searches.join_next().await {
        match result {
            Ok((provider_key, Ok(count))) => reports.push(ProviderReport {
                provider_key,
                ok: true,
                message: "搜索完成".into(),
                result_count: count,
            }),
            Ok((provider_key, Err(error))) => reports.push(ProviderReport {
                provider_key,
                ok: false,
                message: error.to_string(),
                result_count: 0,
            }),
            Err(error) => reports.push(ProviderReport {
                provider_key: "source-runtime".into(),
                ok: false,
                message: error.to_string(),
                result_count: 0,
            }),
        }
    }
    reports.sort_by(|left, right| left.provider_key.cmp(&right.provider_key));
    let pattern = format!("%{}%", term.to_lowercase());
    let media_rows = sqlx::query("SELECT m.*, (SELECT li.legacy_media_item_id FROM library_item li WHERE li.media_id=m.id AND li.legacy_media_item_id IS NOT NULL ORDER BY li.id DESC LIMIT 1) AS legacy_media_item_id, (SELECT mi.filename FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_filename, (SELECT mi.provider_id FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_provider_id FROM media m WHERE lower(m.title) LIKE ? OR lower(m.normalized_code) LIKE ? OR lower(COALESCE(m.original_title,'')) LIKE ? OR lower(COALESCE(legacy_filename,'')) LIKE ? OR lower(COALESCE(legacy_provider_id,'')) LIKE ? ORDER BY m.updated_at DESC LIMIT 60")
        .bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).fetch_all(&state.pool).await?;
    let actor_rows = sqlx::query("SELECT a.*, (SELECT COUNT(*) FROM media_actor ma WHERE ma.actor_id = a.id) AS media_count FROM actor a WHERE lower(a.name) LIKE ? OR lower(a.aliases_json) LIKE ? ORDER BY a.followed DESC, media_count DESC LIMIT 30")
        .bind(&pattern).bind(&pattern).fetch_all(&state.pool).await?;
    Ok(Json(SearchResponse {
        query: term.into(),
        media: media_rows.iter().map(media_from_row).collect(),
        actors: actor_rows.iter().map(actor_from_row).collect(),
        provider_reports: reports,
    }))
}

async fn list_media(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> AppResult<Json<Vec<Media>>> {
    let pattern = format!("%{}%", query.q.trim().to_lowercase());
    let rows = sqlx::query("SELECT m.*, (SELECT li.legacy_media_item_id FROM library_item li WHERE li.media_id=m.id AND li.legacy_media_item_id IS NOT NULL ORDER BY li.id DESC LIMIT 1) AS legacy_media_item_id, (SELECT mi.filename FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_filename, (SELECT mi.provider_id FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_provider_id FROM media m WHERE (? = '%%' OR lower(m.title) LIKE ? OR lower(m.normalized_code) LIKE ? OR lower(COALESCE(legacy_filename,'')) LIKE ? OR lower(COALESCE(legacy_provider_id,'')) LIKE ?) ORDER BY m.updated_at DESC LIMIT 200")
        .bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).fetch_all(&state.pool).await?;
    Ok(Json(rows.iter().map(media_from_row).collect()))
}

async fn media_detail(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Value>> {
    let row = sqlx::query("SELECT m.*, (SELECT li.legacy_media_item_id FROM library_item li WHERE li.media_id=m.id AND li.legacy_media_item_id IS NOT NULL ORDER BY li.id DESC LIMIT 1) AS legacy_media_item_id, (SELECT mi.filename FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_filename, (SELECT mi.provider_id FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_provider_id FROM media m WHERE m.id = ?")
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or(AppError::NotFound)?;
    let media = media_from_row(&row);
    let resources = resources_for_media(&state, id).await?;
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
    Ok(Json(
        json!({"media": media, "actors": actors, "resources": resources, "latestAcquisitionId": acquisition_id, "libraryItemId": library_id}),
    ))
}

async fn media_resources(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Vec<Resource>>> {
    media_exists(&state, id).await?;
    Ok(Json(resources_for_media(&state, id).await?))
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
    let belongs: i64 =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM resource WHERE id = ? AND media_id = ?)")
            .bind(resource_id)
            .bind(media_id)
            .fetch_one(&state.pool)
            .await?;
    if belongs == 0 {
        return Err(AppError::BadRequest("资源不属于该媒体".into()));
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
) -> AppResult<Json<Vec<Acquisition>>> {
    if let Err(error) = reconcile_active(&state).await {
        tracing::warn!(%error, "acquisition list reconciliation failed");
    }
    let rows = if query.status.trim().is_empty() {
        sqlx::query(&format!("{ACQUISITION_SELECT} ORDER BY a.id DESC"))
            .fetch_all(&state.pool)
            .await?
    } else {
        sqlx::query(&format!(
            "{ACQUISITION_SELECT} WHERE a.state = ? ORDER BY a.id DESC"
        ))
        .bind(query.status.trim())
        .fetch_all(&state.pool)
        .await?
    };
    Ok(Json(rows.iter().map(acquisition_from_row).collect()))
}

async fn acquisition_detail(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Value>> {
    if let Err(error) = reconcile_active(&state).await {
        tracing::warn!(%error, acquisition_id = id, "acquisition detail reconciliation failed");
    }
    let acquisition = acquisition_by_id(&state, id).await?;
    let event_rows =
        sqlx::query("SELECT * FROM acquisition_event WHERE acquisition_id = ? ORDER BY id")
            .bind(id)
            .fetch_all(&state.pool)
            .await?;
    let events = event_rows.iter().map(|row| json!({
        "id": row.get::<i64,_>("id"), "eventKey": row.get::<String,_>("event_key"),
        "fromState": row.get::<Option<String>,_>("from_state"), "toState": row.get::<String,_>("to_state"),
        "message": row.get::<String,_>("message"), "payload": parse_json(&row.get::<String,_>("payload_json"), json!({})),
        "createdAt": row.get::<String,_>("created_at")
    })).collect::<Vec<_>>();
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
        "正在通过 MetaTube 补全元数据",
        json!({"videoPath": destination}),
    )
    .await?;
    let nfo = destination.with_extension("nfo");
    let metadata = enrich_with_metatube(state, acquisition.media_id, &acquisition.media.code).await;
    let (remote, metadata_provider, poster_path) = match metadata {
        Ok(value) => value,
        Err(error) => {
            sqlx::query("INSERT INTO metadata_record_v2(media_id, provider_key, status, raw_json, error_message) VALUES (?, 'metatube', 'failed', '{}', ?)")
                .bind(acquisition.media_id).bind(error.to_string()).execute(&state.pool).await?;
            (
                json!({"title": acquisition.media.title, "number": acquisition.media.code}),
                "luma".to_owned(),
                None,
            )
        }
    };
    let title = first_json_string(&remote, &["title"]).unwrap_or(&acquisition.media.title);
    let original_title = first_json_string(&remote, &["original_title", "number"])
        .unwrap_or(&acquisition.media.code);
    let summary =
        first_json_string(&remote, &["summary", "plot", "description"]).unwrap_or_default();
    let release_date =
        first_json_string(&remote, &["release_date", "premiered"]).unwrap_or_default();
    let nfo_body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<movie>\n  <title>{}</title>\n  <originaltitle>{}</originaltitle>\n  <plot>{}</plot>\n  <premiered>{}</premiered>\n  <uniqueid type=\"{}\" default=\"true\">{}</uniqueid>\n</movie>\n",
        xml_escape(title),
        xml_escape(original_title),
        xml_escape(summary),
        xml_escape(release_date),
        xml_escape(&metadata_provider),
        xml_escape(&acquisition.media.code)
    );
    tokio::fs::write(&nfo, nfo_body)
        .await
        .map_err(anyhow::Error::from)?;
    sqlx::query("INSERT INTO metadata_record_v2(media_id, provider_key, status, raw_json) VALUES (?, ?, 'complete', ?)")
        .bind(acquisition.media_id).bind(&metadata_provider).bind(remote.to_string()).execute(&state.pool).await?;
    sqlx::query("UPDATE media SET metadata_status = ?, updated_at = datetime('now') WHERE id = ?")
        .bind(if metadata_provider == "metatube" {
            "complete"
        } else {
            "partial"
        })
        .bind(acquisition.media_id)
        .execute(&state.pool)
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
        .bind(acquisition.media_id).bind(id).bind(legacy_id).bind(destination.to_string_lossy().to_string()).bind(nfo.to_string_lossy().to_string()).bind(poster_path.as_ref().map(|path| path.to_string_lossy().to_string())).bind(file_size).execute(&state.pool).await?.last_insert_rowid();
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
    sqlx::query("UPDATE media SET title=?, original_title=COALESCE(?,original_title), summary=CASE WHEN ?='' THEN summary ELSE ? END, release_date=COALESCE(?,release_date), poster_url=COALESCE(?,poster_url), backdrop_url=COALESCE(?,backdrop_url), updated_at=datetime('now') WHERE id=?")
        .bind(title).bind(original_title).bind(summary).bind(summary).bind(release_date).bind(poster_url).bind(backdrop_url).bind(media_id).execute(&state.pool).await?;
    sqlx::query("INSERT INTO provider_entity_mapping(provider_key,entity_type,provider_entity_id,media_id,raw_json) VALUES ('metatube','media',?,?,?) ON CONFLICT(provider_key,entity_type,provider_entity_id) DO UPDATE SET media_id=excluded.media_id,raw_json=excluded.raw_json,last_seen_at=datetime('now')")
        .bind(format!("{}:{}", match_item.provider, match_item.id)).bind(media_id).bind(remote.to_string()).execute(&state.pool).await?;
    persist_remote_actors(state, media_id, &remote).await?;
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

async fn persist_remote_actors(
    state: &AppState,
    media_id: i64,
    remote: &Value,
) -> anyhow::Result<()> {
    for (order, item) in remote
        .get("actors")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
    {
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
            continue;
        }
        let normalized = name.to_lowercase().replace(' ', "");
        let actor_id: i64 = sqlx::query("INSERT INTO actor(normalized_name,name,avatar_url) VALUES (?,?,?) ON CONFLICT(normalized_name) DO UPDATE SET name=excluded.name,avatar_url=COALESCE(excluded.avatar_url,actor.avatar_url),updated_at=datetime('now') RETURNING id").bind(&normalized).bind(name).bind(avatar).fetch_one(&state.pool).await?.get("id");
        sqlx::query(
            "INSERT OR IGNORE INTO media_actor(media_id,actor_id,billing_order) VALUES (?,?,?)",
        )
        .bind(media_id)
        .bind(actor_id)
        .bind(order as i64)
        .execute(&state.pool)
        .await?;
    }
    Ok(())
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
) -> AppResult<Json<Vec<Actor>>> {
    let pattern = format!("%{}%", query.q.trim().to_lowercase());
    let rows = sqlx::query("SELECT a.*, (SELECT COUNT(*) FROM media_actor ma WHERE ma.actor_id = a.id) AS media_count FROM actor a WHERE (? = '%%' OR lower(a.name) LIKE ? OR lower(a.aliases_json) LIKE ?) ORDER BY a.followed DESC, media_count DESC, a.name LIMIT 200")
        .bind(&pattern).bind(&pattern).bind(&pattern).fetch_all(&state.pool).await?;
    Ok(Json(rows.iter().map(actor_from_row).collect()))
}

async fn actor_detail(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> AppResult<Json<Value>> {
    let row = sqlx::query("SELECT a.*, (SELECT COUNT(*) FROM media_actor ma WHERE ma.actor_id = a.id) AS media_count FROM actor a WHERE id = ?").bind(id).fetch_optional(&state.pool).await?.ok_or(AppError::NotFound)?;
    let media_rows = sqlx::query("SELECT m.*, (SELECT li.legacy_media_item_id FROM library_item li WHERE li.media_id=m.id AND li.legacy_media_item_id IS NOT NULL ORDER BY li.id DESC LIMIT 1) AS legacy_media_item_id, (SELECT mi.filename FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_filename, (SELECT mi.provider_id FROM library_item li JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.media_id=m.id ORDER BY li.id DESC LIMIT 1) AS legacy_provider_id FROM media m JOIN media_actor ma ON ma.media_id = m.id WHERE ma.actor_id = ? ORDER BY m.release_date DESC, m.updated_at DESC").bind(id).fetch_all(&state.pool).await?;
    Ok(Json(
        json!({"actor": actor_from_row(&row), "media": media_rows.iter().map(media_from_row).collect::<Vec<_>>()}),
    ))
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
    let pattern = format!("%{}%", query.q.trim().to_lowercase());
    let rows = sqlx::query("SELECT li.*, m.normalized_code, m.title, m.poster_url, m.release_date, m.metadata_status, mi.filename AS legacy_filename, mi.provider_id AS legacy_provider_id FROM library_item li JOIN media m ON m.id = li.media_id LEFT JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE (? = '%%' OR lower(m.title) LIKE ? OR lower(m.normalized_code) LIKE ? OR lower(COALESCE(mi.filename,'')) LIKE ? OR lower(COALESCE(mi.provider_id,'')) LIKE ?) ORDER BY li.added_at DESC LIMIT 300")
        .bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).bind(&pattern).fetch_all(&state.pool).await?;
    let values = rows.iter().map(library_json).collect::<Vec<_>>();
    Ok(Json(json!({"items": values, "total": values.len()})))
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
    let row = sqlx::query("SELECT li.*, m.normalized_code, m.title, m.poster_url, m.release_date, m.metadata_status, mi.filename AS legacy_filename, mi.provider_id AS legacy_provider_id FROM library_item li JOIN media m ON m.id = li.media_id LEFT JOIN media_item mi ON mi.id=li.legacy_media_item_id WHERE li.id = ?").bind(id).fetch_optional(&state.pool).await?.ok_or(AppError::NotFound)?;
    Ok(Json(
        json!({"item": library_json(&row), "message": "当前文件已符合命名模板，无需移动"}),
    ))
}

async fn list_attention(State(state): State<AppState>) -> AppResult<Json<Vec<Value>>> {
    if let Err(error) = reconcile_active(&state).await {
        tracing::warn!(%error, "attention list reconciliation failed");
    }
    let rows = sqlx::query("SELECT ai.*, m.title AS media_title, m.normalized_code FROM attention_item ai LEFT JOIN media m ON m.id = ai.media_id WHERE ai.status = 'open' ORDER BY CASE ai.severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END, ai.id DESC").fetch_all(&state.pool).await?;
    Ok(Json(rows.iter().map(attention_json).collect()))
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

async fn list_automations(State(state): State<AppState>) -> AppResult<Json<Vec<Value>>> {
    let rows = sqlx::query("SELECT ar.*, (SELECT status FROM automation_execution ae WHERE ae.rule_id = ar.id ORDER BY ae.id DESC LIMIT 1) AS last_status, (SELECT explanation FROM automation_execution ae WHERE ae.rule_id = ar.id ORDER BY ae.id DESC LIMIT 1) AS last_explanation FROM automation_rule ar ORDER BY ar.enabled DESC, ar.id DESC").fetch_all(&state.pool).await?;
    Ok(Json(rows.iter().map(automation_json).collect()))
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

async fn list_providers(State(state): State<AppState>) -> AppResult<Json<Vec<Value>>> {
    let rows = sqlx::query("SELECT * FROM provider_config ORDER BY provider_type, provider_key")
        .fetch_all(&state.pool)
        .await?;
    Ok(Json(rows.iter().map(provider_json).collect()))
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
}

fn default_source_adapter() -> String {
    "jav321".into()
}

fn source_adapter_label(adapter: &str) -> Option<&'static str> {
    match adapter {
        "jav321" => Some("Jav321"),
        "javdb" => Some("JavDB"),
        "javbus" => Some("JavBus"),
        "javlibrary" => Some("JavLibrary"),
        _ => None,
    }
}

async fn create_provider(
    State(state): State<AppState>,
    Json(input): Json<CreateProviderInput>,
) -> AppResult<Json<Value>> {
    if input.display_name.trim().is_empty() {
        return Err(AppError::BadRequest("来源名称不能为空".into()));
    }
    let adapter = input.adapter.trim().to_ascii_lowercase();
    if source_adapter_label(&adapter).is_none() {
        return Err(AppError::BadRequest(format!(
            "不支持的来源适配器：{}",
            input.adapter
        )));
    }
    validate_http_url(&input.base_url)?;
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
        .bind(json!({"adapter": adapter}).to_string())
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
        source_probe(&provider)
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

async fn provider_by_key(state: &AppState, key: &str) -> AppResult<Json<Value>> {
    let row = sqlx::query("SELECT * FROM provider_config WHERE provider_key = ?")
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

#[derive(Debug, Clone)]
struct SourceProviderConfig {
    key: String,
    display_name: String,
    base_url: String,
    secret: String,
    adapter: String,
}

async fn enabled_source_providers(state: &AppState) -> AppResult<Vec<SourceProviderConfig>> {
    let rows = sqlx::query(
        "SELECT * FROM provider_config WHERE provider_type = 'source' AND enabled = 1 ORDER BY provider_key",
    )
    .fetch_all(&state.pool)
    .await?;
    Ok(rows.iter().map(source_provider_from_row).collect())
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
    Ok(row.as_ref().map(source_provider_from_row))
}

fn source_provider_from_row(row: &sqlx::sqlite::SqliteRow) -> SourceProviderConfig {
    let config = parse_json(&row.get::<String, _>("config_json"), json!({}));
    SourceProviderConfig {
        key: row.get("provider_key"),
        display_name: row.get("display_name"),
        base_url: row.get("base_url"),
        secret: row.get("secret"),
        adapter: config
            .get("adapter")
            .and_then(Value::as_str)
            .unwrap_or("javbus")
            .to_owned(),
    }
}

async fn source_search(
    state: &AppState,
    provider: &SourceProviderConfig,
    query: &str,
) -> anyhow::Result<usize> {
    match provider.adapter.as_str() {
        "javbus" => javbus_search(state, provider, query).await,
        "javdb" => javdb_search(state, provider, query).await,
        "jav321" => jav321_search(state, provider, query).await,
        "javlibrary" => javlibrary_search(state, provider, query).await,
        adapter => anyhow::bail!("不支持的来源适配器：{adapter}"),
    }
}

async fn source_probe(provider: &SourceProviderConfig) -> anyhow::Result<()> {
    match provider.adapter.as_str() {
        "javbus" => {
            let base = reqwest::Url::parse(&provider.base_url)?;
            let html = javbus_request_html(provider, base).await?;
            if !html.to_ascii_lowercase().contains("javbus") {
                anyhow::bail!("响应内容不是 JavBus 页面");
            }
            Ok(())
        }
        "javdb" => {
            let (_, html) =
                source_get_html(provider, reqwest::Url::parse(&provider.base_url)?).await?;
            reject_cloudflare(&html, "JavDB")?;
            if !html.to_ascii_lowercase().contains("javdb") {
                anyhow::bail!("响应内容不是 JavDB 页面");
            }
            Ok(())
        }
        "jav321" => {
            let (_, html) =
                source_get_html(provider, reqwest::Url::parse(&provider.base_url)?).await?;
            if !html.to_ascii_lowercase().contains("jav321") {
                anyhow::bail!("响应内容不是 Jav321 页面");
            }
            Ok(())
        }
        "javlibrary" => {
            let (_, html) =
                source_get_html(provider, reqwest::Url::parse(&provider.base_url)?).await?;
            reject_cloudflare(&html, "JavLibrary")?;
            if !html.to_ascii_lowercase().contains("javlibrary") {
                anyhow::bail!("响应内容不是 JavLibrary 页面");
            }
            Ok(())
        }
        adapter => anyhow::bail!("不支持的来源适配器：{adapter}"),
    }
}

fn source_client() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .user_agent(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/127 Safari/537.36 Luma/1.0",
        )
        .build()?)
}

async fn source_get_html(
    provider: &SourceProviderConfig,
    url: reqwest::Url,
) -> anyhow::Result<(reqwest::Url, String)> {
    let mut request = source_client()?.get(url);
    if !provider.secret.trim().is_empty() {
        request = request.header(reqwest::header::COOKIE, provider.secret.trim());
    }
    let response = request.send().await?.error_for_status()?;
    let final_url = response.url().clone();
    Ok((final_url, response.text().await?))
}

fn reject_cloudflare(html: &str, name: &str) -> anyhow::Result<()> {
    if html.contains("challenge-platform") || html.contains("Just a moment...") {
        anyhow::bail!(
            "{name} 触发 Cloudflare 验证，请填写浏览器中的 cf_clearance Cookie 或更换镜像"
        );
    }
    Ok(())
}

async fn javdb_search(
    state: &AppState,
    provider: &SourceProviderConfig,
    query: &str,
) -> anyhow::Result<usize> {
    let mut url = reqwest::Url::parse(provider.base_url.trim_end_matches('/'))?;
    url.set_path("/search");
    url.query_pairs_mut()
        .append_pair("q", query)
        .append_pair("f", "all");
    let (_, html) = source_get_html(provider, url).await?;
    reject_cloudflare(&html, "JavDB")?;
    let items = parse_javdb_search_html(&html, query, &provider.base_url);
    for item in &items {
        persist_source_media(state, &provider.key, item).await?;
    }
    Ok(items.len())
}

async fn jav321_search(
    state: &AppState,
    provider: &SourceProviderConfig,
    query: &str,
) -> anyhow::Result<usize> {
    let mut url = reqwest::Url::parse(provider.base_url.trim_end_matches('/'))?;
    url.set_path("/search");
    let mut encoded = reqwest::Url::parse("https://luma.invalid/")?;
    encoded.query_pairs_mut().append_pair("sn", query);
    let mut request = source_client()?
        .post(url)
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .body(encoded.query().unwrap_or_default().to_owned());
    if !provider.secret.trim().is_empty() {
        request = request.header(reqwest::header::COOKIE, provider.secret.trim());
    }
    let response = request.send().await?.error_for_status()?;
    let final_url = response.url().clone();
    let html = response.text().await?;
    let items = parse_jav321_html(&html, query, &provider.base_url, &final_url);
    for item in &items {
        let media_id = persist_source_media(state, &provider.key, item).await?;
        if final_url.path().contains("/video/") {
            persist_source_detail_html(
                state,
                media_id,
                provider,
                &item.provider_id,
                &html,
                "/star/",
            )
            .await?;
        }
    }
    Ok(items.len())
}

async fn javlibrary_search(
    state: &AppState,
    provider: &SourceProviderConfig,
    query: &str,
) -> anyhow::Result<usize> {
    let mut url = reqwest::Url::parse(provider.base_url.trim_end_matches('/'))?;
    url.set_path("/cn/vl_searchbyid.php");
    url.query_pairs_mut().append_pair("keyword", query);
    let (final_url, html) = source_get_html(provider, url).await?;
    reject_cloudflare(&html, "JavLibrary")?;
    let items = parse_javlibrary_html(&html, query, &provider.base_url, &final_url);
    for item in &items {
        persist_source_media(state, &provider.key, item).await?;
    }
    Ok(items.len())
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
            code: normalize_code(fallback),
            title,
            poster_url: meta_content(html, "og:image")
                .and_then(|value| absolute_url(base_url, &value)),
            source_url: final_url.to_string(),
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
        items.push(SourceMedia {
            provider_id,
            code: normalize_code(fallback),
            title: if title.is_empty() {
                fallback.to_owned()
            } else {
                title
            },
            poster_url,
            source_url: absolute_url(base_url, &href).unwrap_or(href),
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

async fn javbus_search(
    state: &AppState,
    provider: &SourceProviderConfig,
    query: &str,
) -> anyhow::Result<usize> {
    let mut url = reqwest::Url::parse(provider.base_url.trim_end_matches('/'))?;
    url.path_segments_mut()
        .map_err(|_| anyhow::anyhow!("JavBus 地址不能作为基础地址"))?
        .clear()
        .push("search")
        .push(query);
    let html = javbus_request_html(provider, url).await?;
    let items = parse_javbus_search_html(&html, query, &provider.base_url);
    for item in &items {
        persist_source_media(state, &provider.key, item).await?;
    }
    Ok(items.len())
}

fn javbus_cookie(provider: &SourceProviderConfig, session: &[String]) -> String {
    let mut parts = vec!["age=verified".to_owned(), "existmag=all".to_owned()];
    if !provider.secret.trim().is_empty() {
        parts.push(provider.secret.trim().to_owned());
    }
    parts.extend(session.iter().cloned());
    parts.join("; ")
}

async fn javbus_request_html(
    provider: &SourceProviderConfig,
    url: reqwest::Url,
) -> anyhow::Result<String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Luma/1.0")
        .build()?;
    let initial_cookie = javbus_cookie(provider, &[]);
    let html = client
        .get(url.clone())
        .header(reqwest::header::COOKIE, &initial_cookie)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    if !is_javbus_age_page(&html) {
        return Ok(html);
    }

    let mut verify_url = reqwest::Url::parse(provider.base_url.trim_end_matches('/'))?;
    verify_url.set_path("/doc/driver-verify");
    verify_url
        .query_pairs_mut()
        .append_pair("referer", url.path());
    let verification = client
        .post(verify_url)
        .header(reqwest::header::COOKIE, &initial_cookie)
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .body("Submit=confirm")
        .send()
        .await?
        .error_for_status()?;
    let session = verification
        .headers()
        .get_all(reqwest::header::SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .filter_map(|value| value.split(';').next())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let retried = client
        .get(url)
        .header(reqwest::header::COOKIE, javbus_cookie(provider, &session))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    if is_javbus_age_page(&retried) {
        anyhow::bail!("JavBus 要求年龄验证，请在该来源中填写可用 Cookie 或更换镜像");
    }
    Ok(retried)
}

fn is_javbus_age_page(html: &str) -> bool {
    html.contains("driver-verify")
        && (html.contains("Age Verification") || html.contains("你是否已經成年"))
}

#[derive(Debug)]
struct SourceMedia {
    provider_id: String,
    code: String,
    title: String,
    poster_url: Option<String>,
    source_url: String,
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

async fn persist_source_media(
    state: &AppState,
    provider_key: &str,
    item: &SourceMedia,
) -> anyhow::Result<i64> {
    let code = if item.code.is_empty() {
        normalize_code(&item.title)
    } else {
        item.code.clone()
    };
    let row = sqlx::query("INSERT INTO media(normalized_code, title, poster_url) VALUES (?, ?, ?) ON CONFLICT(normalized_code) DO UPDATE SET title = CASE WHEN length(excluded.title) > length(media.title) THEN excluded.title ELSE media.title END, poster_url = COALESCE(excluded.poster_url, media.poster_url), updated_at = datetime('now') RETURNING id")
        .bind(&code).bind(&item.title).bind(&item.poster_url).fetch_one(&state.pool).await?;
    let id: i64 = row.get("id");
    sqlx::query("INSERT INTO provider_entity_mapping(provider_key, entity_type, provider_entity_id, media_id, source_url) VALUES (?, 'media', ?, ?, ?) ON CONFLICT(provider_key, entity_type, provider_entity_id) DO UPDATE SET media_id = excluded.media_id, source_url = excluded.source_url, last_seen_at = datetime('now')")
        .bind(provider_key).bind(&item.provider_id).bind(id).bind(&item.source_url).execute(&state.pool).await?;
    Ok(id)
}

pub async fn ingest_crawler_result(state: &AppState, result_id: i64) -> AppResult<(i64, i64)> {
    let result = crate::crawler::result_by_id(&state.pool, result_id).await?;
    let code = normalize_code(&result.title);
    let media_id: i64 = sqlx::query("INSERT INTO media(normalized_code, title) VALUES (?, ?) ON CONFLICT(normalized_code) DO UPDATE SET title = excluded.title, updated_at = datetime('now') RETURNING id")
        .bind(&code).bind(&result.title).fetch_one(&state.pool).await?.get("id");
    let info_hash = magnet_hash(&result.download_url);
    let (score, reasons) = rank_resource(
        &result.title,
        result.size.as_deref(),
        &result.published_at,
        &result.source,
    );
    let resource_id: i64 = sqlx::query("INSERT INTO resource(media_id, provider_key, provider_resource_id, title, download_url, info_hash, trackers_json, published_at, score, score_reasons_json, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(info_hash) WHERE info_hash IS NOT NULL DO UPDATE SET media_id = excluded.media_id, title = excluded.title, trackers_json = excluded.trackers_json, score = excluded.score, score_reasons_json = excluded.score_reasons_json, updated_at = datetime('now') RETURNING id")
        .bind(media_id).bind(format!("crawler:{}", result.script_id)).bind(result.id.to_string()).bind(&result.title).bind(&result.download_url).bind(info_hash)
        .bind(serde_json::to_string(&result.trackers).unwrap_or_else(|_| "[]".into())).bind(&result.published_at).bind(score).bind(serde_json::to_string(&reasons).unwrap_or_else(|_| "[]".into())).bind(result.raw.to_string()).fetch_one(&state.pool).await?.get("id");
    Ok((media_id, resource_id))
}

fn rank_resource(
    title: &str,
    size: Option<&str>,
    published_at: &str,
    provider: &str,
) -> (f64, Vec<String>) {
    let lower = title.to_lowercase();
    let mut score = 50.0;
    let mut reasons = vec![format!("来源 {provider} 可用")];
    if lower.contains("中文字幕") || lower.contains("chinese") || lower.contains("-c") {
        score += 24.0;
        reasons.push("包含中文字幕标记".into());
    }
    if lower.contains("4k") || lower.contains("2160") {
        score += 16.0;
        reasons.push("4K 清晰度".into());
    } else if lower.contains("1080") {
        score += 10.0;
        reasons.push("1080p 清晰度".into());
    }
    if size.is_some() {
        score += 3.0;
        reasons.push("提供文件大小".into());
    }
    if !published_at.is_empty() {
        score += 2.0;
        reasons.push("提供发布时间".into());
    }
    (score, reasons)
}

async fn resources_for_media(state: &AppState, media_id: i64) -> AppResult<Vec<Resource>> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM resource WHERE media_id = ?")
        .bind(media_id)
        .fetch_one(&state.pool)
        .await?;
    if count == 0
        && let Err(error) = refresh_source_media(state, media_id).await
    {
        tracing::warn!(%error, media_id, "source detail refresh failed");
    }
    let rows = sqlx::query("SELECT r.*, (SELECT a.id FROM acquisition a WHERE a.resource_id=r.id ORDER BY a.id DESC LIMIT 1) AS acquisition_id, (SELECT a.state FROM acquisition a WHERE a.resource_id=r.id ORDER BY a.id DESC LIMIT 1) AS acquisition_state, (SELECT a.qbit_hash FROM acquisition a WHERE a.resource_id=r.id ORDER BY a.id DESC LIMIT 1) AS acquisition_qbit_hash FROM resource r WHERE r.media_id = ? ORDER BY r.available DESC, r.score DESC, r.published_at DESC, r.id DESC").bind(media_id).fetch_all(&state.pool).await?;
    let mut resources = rows.iter().map(resource_from_row).collect::<Vec<_>>();
    let settings = storage::load_settings(&state.pool).await?;
    match QBittorrentClient::new(&settings) {
        Ok(client) => match client.torrents().await {
            Ok(torrents) => {
                let torrents = torrents
                    .into_iter()
                    .filter_map(|torrent| normalize_hash(&torrent.hash).map(|hash| (hash, torrent)))
                    .collect::<HashMap<_, _>>();
                for resource in &mut resources {
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
                tracing::warn!(%error, media_id, "resource status could not reach qBittorrent");
                for resource in &mut resources {
                    resource.qbit_sync_status = "unavailable".into();
                }
            }
        },
        Err(error) => {
            tracing::warn!(%error, media_id, "resource status has invalid qBittorrent settings");
            for resource in &mut resources {
                resource.qbit_sync_status = "unavailable".into();
            }
        }
    }
    Ok(resources)
}

async fn refresh_source_media(state: &AppState, media_id: i64) -> anyhow::Result<usize> {
    let rows = sqlx::query("SELECT pem.provider_entity_id, pem.source_url, pc.* FROM provider_entity_mapping pem JOIN provider_config pc ON pc.provider_key = pem.provider_key WHERE pem.entity_type='media' AND pem.media_id=? AND pc.provider_type='source' AND pc.enabled=1 ORDER BY pc.provider_key")
        .bind(media_id).fetch_all(&state.pool).await?;
    if rows.is_empty() {
        anyhow::bail!("媒体没有启用的来源映射");
    }
    let mut total = 0;
    let mut errors = Vec::new();
    for row in &rows {
        let provider = source_provider_from_row(row);
        let provider_id: String = row.get("provider_entity_id");
        let source_url: Option<String> = row.get("source_url");
        let result = match provider.adapter.as_str() {
            "javbus" => {
                refresh_javbus_media(
                    state,
                    media_id,
                    &provider,
                    &provider_id,
                    source_url.as_deref().unwrap_or_default(),
                )
                .await
            }
            "javdb" | "jav321" | "javlibrary" => {
                refresh_generic_source_media(
                    state,
                    media_id,
                    &provider,
                    &provider_id,
                    source_url.as_deref().unwrap_or_default(),
                )
                .await
            }
            adapter => Err(anyhow::anyhow!("不支持的来源适配器：{adapter}")),
        };
        match result {
            Ok(count) => total += count,
            Err(error) => errors.push(format!("{}: {error}", provider.display_name)),
        }
    }
    if total == 0 && !errors.is_empty() {
        anyhow::bail!(errors.join("；"));
    }
    Ok(total)
}

async fn refresh_javbus_media(
    state: &AppState,
    media_id: i64,
    provider: &SourceProviderConfig,
    provider_id: &str,
    source_url: &str,
) -> anyhow::Result<usize> {
    let base = reqwest::Url::parse(provider.base_url.trim_end_matches('/'))?;
    let url = reqwest::Url::parse(source_url)
        .or_else(|_| base.join(source_url))
        .or_else(|_| base.join(provider_id))?;
    let html = javbus_request_html(provider, url.clone()).await?;
    let title = meta_content(&html, "og:title");
    let poster = meta_content(&html, "og:image");
    let summary = meta_content(&html, "og:description").unwrap_or_default();
    sqlx::query("UPDATE media SET title=COALESCE(?,title), poster_url=COALESCE(?,poster_url), summary=CASE WHEN ?='' THEN summary ELSE ? END, updated_at=datetime('now') WHERE id=?")
        .bind(title).bind(poster).bind(&summary).bind(&summary).bind(media_id).execute(&state.pool).await?;
    persist_source_actors(state, media_id, &provider.key, &html, "/star/").await?;
    let magnets = fetch_javbus_magnets(provider, &url, &html).await?;
    for (index, (url, label)) in magnets.iter().enumerate() {
        let info_hash = magnet_hash(url);
        let (score, reasons) = rank_resource(label, None, "", &provider.display_name);
        sqlx::query("INSERT INTO resource(media_id,provider_key,provider_resource_id,title,download_url,info_hash,score,score_reasons_json) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(info_hash) WHERE info_hash IS NOT NULL DO UPDATE SET media_id=excluded.media_id,title=excluded.title,score=excluded.score,score_reasons_json=excluded.score_reasons_json,updated_at=datetime('now')")
            .bind(media_id).bind(&provider.key).bind(format!("{provider_id}:{index}")).bind(label).bind(url).bind(info_hash).bind(score).bind(serde_json::to_string(&reasons)?).execute(&state.pool).await?;
    }
    Ok(magnets.len())
}

async fn refresh_generic_source_media(
    state: &AppState,
    media_id: i64,
    provider: &SourceProviderConfig,
    provider_id: &str,
    source_url: &str,
) -> anyhow::Result<usize> {
    let base = reqwest::Url::parse(provider.base_url.trim_end_matches('/'))?;
    let url = reqwest::Url::parse(source_url).or_else(|_| match provider.adapter.as_str() {
        "javdb" => base.join(&format!("/v/{provider_id}")),
        "jav321" => base.join(&format!("/video/{provider_id}")),
        _ => base.join(source_url),
    })?;
    let (_, html) = source_get_html(provider, url).await?;
    if provider.adapter == "javdb" {
        reject_cloudflare(&html, "JavDB")?;
    } else if provider.adapter == "javlibrary" {
        reject_cloudflare(&html, "JavLibrary")?;
    }
    let actor_marker = if provider.adapter == "javdb" {
        "/actors/"
    } else {
        "/star/"
    };
    persist_source_detail_html(state, media_id, provider, provider_id, &html, actor_marker).await
}

async fn persist_source_detail_html(
    state: &AppState,
    media_id: i64,
    provider: &SourceProviderConfig,
    provider_id: &str,
    html: &str,
    actor_marker: &str,
) -> anyhow::Result<usize> {
    let title = meta_content(html, "og:title")
        .or_else(|| extract_tag_text_after(html, "panel-heading"))
        .or_else(|| extract_tag_text_after(html, "video_title"));
    let poster = meta_content(html, "og:image")
        .or_else(|| extract_attribute(html, "poster="))
        .map(|value| value.replace("http://pics.dmm.co.jp", "https://pics.dmm.co.jp"));
    let summary = meta_content(html, "og:description").unwrap_or_default();
    sqlx::query("UPDATE media SET title=COALESCE(?,title), poster_url=COALESCE(?,poster_url), summary=CASE WHEN ?='' THEN summary ELSE ? END, updated_at=datetime('now') WHERE id=?")
        .bind(title).bind(poster).bind(&summary).bind(&summary).bind(media_id).execute(&state.pool).await?;
    persist_source_actors(state, media_id, &provider.key, html, actor_marker).await?;
    let magnets = parse_magnets(html);
    for (index, (url, label)) in magnets.iter().enumerate() {
        let info_hash = magnet_hash(url);
        let (score, reasons) = rank_resource(label, None, "", &provider.display_name);
        sqlx::query("INSERT INTO resource(media_id,provider_key,provider_resource_id,title,download_url,info_hash,score,score_reasons_json) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(info_hash) WHERE info_hash IS NOT NULL DO UPDATE SET media_id=excluded.media_id,title=excluded.title,score=excluded.score,score_reasons_json=excluded.score_reasons_json,updated_at=datetime('now')")
            .bind(media_id).bind(&provider.key).bind(format!("{provider_id}:{index}")).bind(label).bind(url).bind(info_hash).bind(score).bind(serde_json::to_string(&reasons)?).execute(&state.pool).await?;
    }
    Ok(magnets.len())
}

async fn fetch_javbus_magnets(
    provider: &SourceProviderConfig,
    detail_url: &reqwest::Url,
    html: &str,
) -> anyhow::Result<Vec<(String, String)>> {
    let embedded = parse_magnets(html);
    if !embedded.is_empty() {
        return Ok(embedded);
    }
    let Some(gid) = extract_js_value(html, "gid") else {
        return Ok(Vec::new());
    };
    let img = extract_js_value(html, "img").unwrap_or_default();
    let uc = extract_js_value(html, "uc").unwrap_or_else(|| "0".into());
    let mut ajax = reqwest::Url::parse(provider.base_url.trim_end_matches('/'))?;
    ajax.set_path("/ajax/uncledatoolsbyajax.php");
    ajax.query_pairs_mut()
        .append_pair("gid", &gid)
        .append_pair("lang", "zh")
        .append_pair("img", &img)
        .append_pair("uc", &uc)
        .append_pair("floor", "1");
    let body = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Luma/1.0")
        .build()?
        .get(ajax)
        .header(reqwest::header::COOKIE, javbus_cookie(provider, &[]))
        .header(reqwest::header::REFERER, detail_url.as_str())
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    Ok(parse_magnets(&body))
}

fn extract_js_value(html: &str, name: &str) -> Option<String> {
    for marker in [
        format!("var {name}"),
        format!("{name} ="),
        format!("{name}="),
    ] {
        let Some(at) = html.find(&marker) else {
            continue;
        };
        let tail = &html[at + marker.len()..char_boundary_before(html, at + marker.len() + 800)];
        let value = tail
            .trim_start_matches(|character: char| character.is_whitespace() || character == '=')
            .split([';', '\n', '\r', ','])
            .next()?
            .trim()
            .trim_matches(['\'', '"']);
        if !value.is_empty() {
            return Some(html_unescape(value));
        }
    }
    None
}

async fn persist_source_actors(
    state: &AppState,
    media_id: i64,
    provider_key: &str,
    html: &str,
    marker: &str,
) -> anyhow::Result<()> {
    let mut cursor = 0;
    let mut order = 0;
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
        let normalized = name.to_lowercase().replace(' ', "");
        let actor_id: i64 = sqlx::query("INSERT INTO actor(normalized_name,name) VALUES (?,?) ON CONFLICT(normalized_name) DO UPDATE SET name=excluded.name,updated_at=datetime('now') RETURNING id").bind(&normalized).bind(&name).fetch_one(&state.pool).await?.get("id");
        sqlx::query(
            "INSERT OR IGNORE INTO media_actor(media_id,actor_id,billing_order) VALUES (?,?,?)",
        )
        .bind(media_id)
        .bind(actor_id)
        .bind(order)
        .execute(&state.pool)
        .await?;
        sqlx::query("INSERT INTO provider_entity_mapping(provider_key,entity_type,provider_entity_id,actor_id) VALUES (?,'actor',?,?) ON CONFLICT(provider_key,entity_type,provider_entity_id) DO UPDATE SET actor_id=excluded.actor_id,last_seen_at=datetime('now')").bind(provider_key).bind(provider_id).bind(actor_id).execute(&state.pool).await?;
        order += 1;
        if order >= 40 {
            break;
        }
    }
    Ok(())
}

fn parse_magnets(html: &str) -> Vec<(String, String)> {
    let mut result: Vec<(String, String)> = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = html[cursor..].find("magnet:?") {
        let start = cursor + offset;
        let end = html[start..]
            .find(['\"', '\'', '<', ' '])
            .unwrap_or(html.len() - start);
        let url = html_unescape(&html[start..start + end]);
        cursor = start + end;
        if magnet_hash(&url).is_none()
            || result
                .iter()
                .any(|(existing, _)| magnet_hash(existing) == magnet_hash(&url))
        {
            continue;
        }
        let label_start = char_boundary_before(html, start.saturating_sub(500));
        let label = strip_tags(&html[label_start..start])
            .split_whitespace()
            .rev()
            .take(8)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join(" ");
        result.push((
            url,
            if label.is_empty() {
                "JavBus 资源".into()
            } else {
                label
            },
        ));
    }
    result
}

fn meta_content(html: &str, property: &str) -> Option<String> {
    let marker = format!("property=\"{property}\"");
    let at = html.find(&marker)?;
    let end = char_boundary_before(html, at + 600);
    extract_attribute(&html[at..end], "content=")
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

const ACQUISITION_SELECT: &str = "SELECT a.*, m.normalized_code, m.title AS media_title, m.original_title, m.summary, m.release_date, m.duration_minutes, m.poster_url, m.backdrop_url, m.media_type, m.metadata_status, m.created_at AS media_created_at, m.updated_at AS media_updated_at, r.provider_key AS resource_provider_key, r.title AS resource_title, r.download_url, r.info_hash, r.size_bytes, r.resolution, r.subtitle_languages_json, r.trackers_json, r.published_at, r.score, r.score_reasons_json, r.available FROM acquisition a JOIN media m ON m.id = a.media_id LEFT JOIN resource r ON r.id = a.resource_id";

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
    json!({"key":row.get::<String,_>("provider_key"),"type":row.get::<String,_>("provider_type"),"displayName":row.get::<String,_>("display_name"),"enabled":row.get::<i64,_>("enabled") != 0,"baseUrl":row.get::<String,_>("base_url"),"hasSecret":!secret.is_empty(),"config":parse_json(&row.get::<String,_>("config_json"),json!({})),"lastStatus":row.get::<String,_>("last_status"),"lastMessage":row.get::<String,_>("last_message"),"lastCheckedAt":row.get::<Option<String>,_>("last_checked_at")})
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

fn extract_media_code(value: &str) -> Option<String> {
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
fn normalize_code(value: &str) -> String {
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
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
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
        let (score, reasons) =
            rank_resource("ABC-123 4K 中文字幕", Some("5 GB"), "2026-08-01", "mock");
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
    fn javbus_age_gate_is_detected() {
        assert!(is_javbus_age_page(
            "<title>Age Verification JavBus</title><a href='/doc/driver-verify'>verify</a>"
        ));
    }

    #[test]
    fn javbus_javascript_values_are_parsed() {
        let html = "<script>var gid = 12345; var uc = 0; var img = '/cover.jpg';</script>";
        assert_eq!(extract_js_value(html, "gid").as_deref(), Some("12345"));
        assert_eq!(extract_js_value(html, "uc").as_deref(), Some("0"));
        assert_eq!(extract_js_value(html, "img").as_deref(), Some("/cover.jpg"));
    }

    #[test]
    fn javdb_parser_deduplicates_media_links() {
        let html = r#"<a href="/v/abc"><div class="video-title">ABC-123 Title</div><img data-src="/cover.jpg"></a><a href="/v/abc">duplicate</a>"#;
        let items = parse_javdb_search_html(html, "ABC-123", "https://javdb.com");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].provider_id, "abc");
        assert_eq!(items[0].title, "ABC-123 Title");
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
        let jav321_enabled: i64 =
            sqlx::query_scalar("SELECT enabled FROM provider_config WHERE provider_key='jav321'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(jav321_enabled, 1);
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
            pool,
            scrape_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            crawler_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            asset_root: temp.join("assets"),
            script_root: temp.join("scripts"),
            events: tokio::sync::broadcast::channel(32).0,
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
