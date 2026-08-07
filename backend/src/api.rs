use std::collections::{HashMap, HashSet};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Multipart, Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{delete, get, post, put},
};
use serde_json::json;
use sqlx::Row;

use crate::{
    AppState, asset, crawler,
    error::{AppError, AppResult},
    metadata::{self, WriteOptions},
    models::{
        BatchCrawlerResultInput, BatchScrapeInput, BatchScrapeResponse, BatchTaskInput,
        BatchTaskResponse, CrawlerResult, CrawlerRun, CrawlerScript, DashboardStats, DownloadItem,
        Folder, FolderInput, LogEntry, MediaItem, MetaTubeConnection, QBittorrentConnection,
        ScrapeOptions, ServiceHealth, ServiceStatus, Settings, Task, TaskDetail,
    },
    provider::MetaTubeClient,
    qbittorrent::QBittorrentClient,
    scanner,
    storage::{self, folder_from_row, media_from_row, task_from_row},
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/dashboard", get(dashboard))
        .route("/folders", get(list_folders).post(create_folder))
        .route("/folders/{id}", put(update_folder).delete(delete_folder))
        .route("/folders/{id}/scan", post(scan_folder))
        .route("/tasks", get(list_tasks))
        .route("/tasks/retry", post(retry_tasks))
        .route("/tasks/cancel", post(cancel_tasks))
        .route("/tasks/{id}", get(get_task))
        .route("/tasks/{id}/retry", post(retry_task))
        .route("/tasks/{id}/cancel", post(cancel_task))
        .route("/media", get(list_media))
        .route("/media/scrape", post(scrape_media_batch))
        .route("/media/{id}/scrape", post(scrape_media))
        .route("/settings", get(get_settings).put(update_settings))
        .route("/status", get(service_status))
        .route("/settings/metatube/test", post(test_metatube))
        .route("/settings/qbittorrent/test", post(test_qbittorrent))
        .route("/crawlers", get(list_crawlers).post(create_crawler))
        .route("/crawlers/{id}", put(update_crawler).delete(delete_crawler))
        .route("/crawlers/{id}/run", post(run_crawler))
        .route("/crawler-runs", get(list_crawler_runs))
        .route("/crawler-results", get(list_crawler_results))
        .route("/crawler-results/download", post(download_crawler_results))
        .route(
            "/crawler-results/{id}/download",
            post(download_crawler_result),
        )
        .route("/crawler-results/{id}/ignore", post(ignore_crawler_result))
        .route("/downloads", get(list_downloads))
        .route("/downloads/{hash}", delete(remove_download))
        .route("/downloads/{hash}/pause", post(pause_download))
        .route("/downloads/{hash}/resume", post(resume_download))
        .route("/logs", get(list_logs))
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
}

async fn dashboard(State(state): State<AppState>) -> AppResult<Json<DashboardStats>> {
    let media_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_item")
        .fetch_one(&state.pool)
        .await?;
    let task_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM scrape_task")
        .fetch_one(&state.pool)
        .await?;
    let success_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM scrape_task WHERE status = 'success'")
            .fetch_one(&state.pool)
            .await?;
    let failed_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM scrape_task WHERE status = 'failed'")
            .fetch_one(&state.pool)
            .await?;
    let candidate_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM crawler_result WHERE download_status != 'ignored'",
    )
    .fetch_one(&state.pool)
    .await?;
    let pending_scrape_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM media_item WHERE status = 'pending'")
            .fetch_one(&state.pool)
            .await?;
    let settings = storage::load_settings(&state.pool).await?;
    let download_count = match QBittorrentClient::new(&settings) {
        Ok(client) => client.torrents().await.map_or(0, |items| items.len()),
        Err(_) => 0,
    };
    let rows = sqlx::query(&format!(
        "{} ORDER BY st.updated_at DESC, st.id DESC LIMIT 8",
        storage::TASK_SELECT
    ))
    .fetch_all(&state.pool)
    .await?;
    let recent_activity = rows.iter().map(task_from_row).collect();
    Ok(Json(DashboardStats {
        media_count,
        task_count,
        success_count,
        failed_count,
        candidate_count,
        download_count,
        pending_scrape_count,
        recent_activity,
    }))
}

async fn list_folders(State(state): State<AppState>) -> AppResult<Json<Vec<Folder>>> {
    let rows = sqlx::query("SELECT * FROM media_config ORDER BY name ASC")
        .fetch_all(&state.pool)
        .await?;
    Ok(Json(rows.iter().map(folder_from_row).collect()))
}

async fn create_folder(
    State(state): State<AppState>,
    Json(input): Json<FolderInput>,
) -> AppResult<(StatusCode, Json<Folder>)> {
    validate_folder(&input)?;
    validate_folder_path(&input.path).await?;
    ensure_metatube_connected(&state).await?;
    let result = sqlx::query(
        "INSERT INTO media_config (name, path, media_type, output_format, scan_mode, enabled) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(input.name.trim())
    .bind(input.path.trim())
    .bind(&input.media_type)
    .bind(&input.output_format)
    .bind(&input.scan_mode)
    .bind(input.enabled)
    .execute(&state.pool).await?;
    let folder = storage::folder_by_id(&state.pool, result.last_insert_rowid()).await?;
    storage::log(
        &state.pool,
        "info",
        "folder",
        &format!("Added folder {}", folder.name),
    )
    .await;
    if folder.enabled {
        let run = storage::create_task_run(&state.pool, None, Some(folder.id), "scan").await?;
        tokio::spawn(run_scan_with_auto_scrape(
            state,
            folder.clone(),
            run.task.id,
            run.record_id,
        ));
    }
    Ok((StatusCode::CREATED, Json(folder)))
}

pub(crate) async fn run_scan_with_auto_scrape(
    state: AppState,
    folder: Folder,
    scan_task_id: i64,
    scan_record_id: i64,
) {
    scanner::run_scan(state.clone(), folder.clone(), scan_task_id, scan_record_id).await;
    let scan_status: Option<String> =
        sqlx::query_scalar("SELECT status FROM task_record WHERE id = ? AND task_id = ?")
            .bind(scan_record_id)
            .bind(scan_task_id)
            .fetch_optional(&state.pool)
            .await
            .ok()
            .flatten();
    if scan_status.as_deref() != Some("success") {
        return;
    }

    let media_ids = match sqlx::query_scalar::<_, i64>(
        "SELECT id FROM media_item WHERE folder_id = ? AND status = 'pending' ORDER BY id ASC",
    )
    .bind(folder.id)
    .fetch_all(&state.pool)
    .await
    {
        Ok(ids) => ids,
        Err(error) => {
            tracing::error!(%error, folder_id = folder.id, "failed to load media for automatic scrape");
            storage::log(
                &state.pool,
                "error",
                "folder",
                &format!("Could not start automatic scraping for {}", folder.name),
            )
            .await;
            return;
        }
    };

    if let Err(error) = queue_scrapes(&state, media_ids, ScrapeOptions::default()).await {
        tracing::error!(%error, folder_id = folder.id, "failed to queue automatic scrape tasks");
        storage::log(
            &state.pool,
            "error",
            "folder",
            &format!("Could not queue automatic scraping for {}", folder.name),
        )
        .await;
    }
}

async fn update_folder(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(input): Json<FolderInput>,
) -> AppResult<Json<Folder>> {
    validate_folder(&input)?;
    validate_folder_path(&input.path).await?;
    let result = sqlx::query(
        "UPDATE media_config SET name = ?, path = ?, media_type = ?, output_format = ?, scan_mode = ?, enabled = ?, updated_at = datetime('now') WHERE id = ?",
    )
    .bind(input.name.trim()).bind(input.path.trim()).bind(&input.media_type)
    .bind(&input.output_format).bind(&input.scan_mode).bind(input.enabled).bind(id)
    .execute(&state.pool).await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound);
    }
    Ok(Json(storage::folder_by_id(&state.pool, id).await?))
}

async fn delete_folder(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<StatusCode> {
    let folder = storage::folder_by_id(&state.pool, id).await?;
    sqlx::query("DELETE FROM media_config WHERE id = ?")
        .bind(id)
        .execute(&state.pool)
        .await?;
    storage::log(
        &state.pool,
        "info",
        "folder",
        &format!("Deleted folder {}", folder.name),
    )
    .await;
    Ok(StatusCode::NO_CONTENT)
}

async fn scan_folder(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<(StatusCode, Json<Task>)> {
    let folder = storage::folder_by_id(&state.pool, id).await?;
    if !folder.enabled {
        return Err(AppError::BadRequest("folder is disabled".into()));
    }
    let run = storage::create_task_run(&state.pool, None, Some(id), "scan").await?;
    tokio::spawn(run_scan_with_auto_scrape(
        state,
        folder,
        run.task.id,
        run.record_id,
    ));
    Ok((StatusCode::ACCEPTED, Json(run.task)))
}

async fn list_tasks(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> AppResult<Json<Vec<Task>>> {
    let rows = if let Some(status) = query.get("status").filter(|value| !value.is_empty()) {
        sqlx::query(&format!(
            "{} WHERE st.status = ? ORDER BY st.updated_at DESC, st.id DESC LIMIT 250",
            storage::TASK_SELECT
        ))
        .bind(status)
        .fetch_all(&state.pool)
        .await?
    } else {
        sqlx::query(&format!(
            "{} ORDER BY st.updated_at DESC, st.id DESC LIMIT 250",
            storage::TASK_SELECT
        ))
        .fetch_all(&state.pool)
        .await?
    };
    Ok(Json(rows.iter().map(task_from_row).collect()))
}

async fn get_task(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<Json<TaskDetail>> {
    Ok(Json(storage::task_detail_by_id(&state.pool, id).await?))
}

async fn retry_task(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<(StatusCode, Json<Task>)> {
    let task = retry_task_by_id(&state, id).await?;
    Ok((StatusCode::ACCEPTED, Json(task)))
}

async fn retry_tasks(
    State(state): State<AppState>,
    Json(input): Json<BatchTaskInput>,
) -> AppResult<(StatusCode, Json<BatchTaskResponse>)> {
    validate_batch_task_input(&input)?;
    let requested = input.task_ids.len();
    let mut seen = HashSet::new();
    let task_ids = input
        .task_ids
        .into_iter()
        .filter(|id| seen.insert(*id))
        .collect::<Vec<_>>();
    let mut skipped = requested.saturating_sub(task_ids.len());
    let mut tasks = Vec::new();
    for task_id in task_ids {
        match retry_task_by_id(&state, task_id).await {
            Ok(task) => tasks.push(task),
            Err(AppError::BadRequest(_) | AppError::NotFound) => skipped += 1,
            Err(error) => return Err(error),
        }
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(BatchTaskResponse {
            processed: tasks.len(),
            skipped,
            tasks,
        }),
    ))
}

async fn retry_task_by_id(state: &AppState, id: i64) -> AppResult<Task> {
    let previous = storage::task_by_id(&state.pool, id).await?;
    if !matches!(previous.status.as_str(), "failed" | "cancelled") {
        return Err(AppError::BadRequest(
            "only failed or cancelled tasks can be retried".into(),
        ));
    }

    enum RetryTarget {
        Scan(Folder),
        Scrape(i64),
    }
    let target = match previous.task_type.as_str() {
        "scan" => RetryTarget::Scan(
            storage::folder_by_id(&state.pool, previous.folder_id.ok_or(AppError::NotFound)?)
                .await?,
        ),
        "scrape" => {
            let media_id = previous.media_id.ok_or(AppError::NotFound)?;
            storage::media_by_id(&state.pool, media_id).await?;
            RetryTarget::Scrape(media_id)
        }
        _ => return Err(AppError::BadRequest("unknown task type".into())),
    };

    let run = storage::create_task_run(
        &state.pool,
        previous.media_id,
        previous.folder_id,
        &previous.task_type,
    )
    .await?;
    match target {
        RetryTarget::Scan(folder) => {
            tokio::spawn(run_scan_with_auto_scrape(
                state.clone(),
                folder,
                run.task.id,
                run.record_id,
            ));
        }
        RetryTarget::Scrape(media_id) => {
            tokio::spawn(run_scrape(
                state.clone(),
                media_id,
                run.task.id,
                run.record_id,
                ScrapeOptions::default(),
            ));
        }
    }
    Ok(run.task)
}

async fn cancel_task(State(state): State<AppState>, Path(id): Path<i64>) -> AppResult<Json<Task>> {
    Ok(Json(cancel_task_by_id(&state, id).await?))
}

async fn cancel_tasks(
    State(state): State<AppState>,
    Json(input): Json<BatchTaskInput>,
) -> AppResult<Json<BatchTaskResponse>> {
    validate_batch_task_input(&input)?;
    let requested = input.task_ids.len();
    let mut seen = HashSet::new();
    let task_ids = input
        .task_ids
        .into_iter()
        .filter(|id| seen.insert(*id))
        .collect::<Vec<_>>();
    let mut skipped = requested.saturating_sub(task_ids.len());
    let mut tasks = Vec::new();
    for task_id in task_ids {
        match cancel_task_by_id(&state, task_id).await {
            Ok(task) => tasks.push(task),
            Err(AppError::BadRequest(_) | AppError::NotFound) => skipped += 1,
            Err(error) => return Err(error),
        }
    }
    Ok(Json(BatchTaskResponse {
        processed: tasks.len(),
        skipped,
        tasks,
    }))
}

async fn cancel_task_by_id(state: &AppState, id: i64) -> AppResult<Task> {
    let record_id: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM task_record WHERE task_id = ? AND status IN ('pending', 'running') \
         ORDER BY created_at DESC, id DESC LIMIT 1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;
    let Some(record_id) = record_id else {
        let task = storage::task_by_id(&state.pool, id).await?;
        return Err(AppError::BadRequest(format!(
            "task is already {}",
            task.status
        )));
    };
    storage::finish_task_run(&state.pool, id, record_id, "cancelled", None).await?;
    storage::task_by_id(&state.pool, id).await
}

async fn list_media(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> AppResult<Json<Vec<MediaItem>>> {
    let search = query
        .get("search")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty());
    let status = query
        .get("status")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty());
    let media_id = query
        .get("mediaId")
        .map(|value| value.parse::<i64>())
        .transpose()
        .map_err(|_| AppError::BadRequest("invalid media id".into()))?;
    if let Some(status) = status
        && !matches!(status, "pending" | "ready" | "failed")
    {
        return Err(AppError::BadRequest("unknown media status".into()));
    }
    let rows = match (media_id, search, status) {
        (Some(media_id), _, _) => {
            sqlx::query(&format!("{} WHERE mi.id = ?", storage::MEDIA_SELECT))
                .bind(media_id)
                .fetch_all(&state.pool)
                .await?
        }
        (None, Some(search), Some(status)) => {
            let pattern = format!("%{search}%");
            sqlx::query(&format!("{} WHERE (mi.filename LIKE ? OR mi.title LIKE ?) AND mi.status = ? ORDER BY mi.updated_at DESC LIMIT 500", storage::MEDIA_SELECT))
                .bind(&pattern).bind(&pattern).bind(status).fetch_all(&state.pool).await?
        }
        (None, Some(search), None) => {
            let pattern = format!("%{search}%");
            sqlx::query(&format!("{} WHERE mi.filename LIKE ? OR mi.title LIKE ? ORDER BY mi.updated_at DESC LIMIT 500", storage::MEDIA_SELECT))
                .bind(&pattern).bind(&pattern).fetch_all(&state.pool).await?
        }
        (None, None, Some(status)) => {
            sqlx::query(&format!(
                "{} WHERE mi.status = ? ORDER BY mi.updated_at DESC LIMIT 500",
                storage::MEDIA_SELECT
            ))
            .bind(status)
            .fetch_all(&state.pool)
            .await?
        }
        (None, None, None) => {
            sqlx::query(&format!(
                "{} ORDER BY mi.updated_at DESC LIMIT 500",
                storage::MEDIA_SELECT
            ))
            .fetch_all(&state.pool)
            .await?
        }
    };
    Ok(Json(rows.iter().map(media_from_row).collect()))
}

async fn scrape_media_batch(
    State(state): State<AppState>,
    Json(input): Json<BatchScrapeInput>,
) -> AppResult<(StatusCode, Json<BatchScrapeResponse>)> {
    if input.media_ids.is_empty() {
        return Err(AppError::BadRequest(
            "select at least one media item".into(),
        ));
    }
    if input.media_ids.len() > 500 {
        return Err(AppError::BadRequest(
            "a batch can contain at most 500 media items".into(),
        ));
    }
    let response = queue_scrapes(&state, input.media_ids, input.options).await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn scrape_media(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(options): Json<ScrapeOptions>,
) -> AppResult<(StatusCode, Json<Task>)> {
    let media = storage::media_by_id(&state.pool, id).await?;
    let run = storage::create_task_run(&state.pool, Some(id), media.folder_id, "scrape").await?;
    tokio::spawn(run_scrape(state, id, run.task.id, run.record_id, options));
    Ok((StatusCode::ACCEPTED, Json(run.task)))
}

async fn queue_scrapes(
    state: &AppState,
    media_ids: Vec<i64>,
    options: ScrapeOptions,
) -> AppResult<BatchScrapeResponse> {
    let requested = media_ids.len();
    let mut seen = HashSet::new();
    let media_ids = media_ids
        .into_iter()
        .filter(|id| seen.insert(*id))
        .collect::<Vec<_>>();

    let mut folder_ids = HashMap::new();
    for media_id in &media_ids {
        let media = storage::media_by_id(&state.pool, *media_id).await?;
        folder_ids.insert(*media_id, media.folder_id);
    }

    let mut skipped = requested.saturating_sub(media_ids.len());
    let mut tasks = Vec::new();
    for media_id in media_ids {
        let active: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM scrape_task WHERE media_id = ? AND task_type = 'scrape' AND status IN ('pending', 'running'))",
        )
        .bind(media_id)
        .fetch_one(&state.pool)
        .await?;
        if active != 0 {
            skipped += 1;
            continue;
        }
        tasks.push(
            storage::create_task_run(
                &state.pool,
                Some(media_id),
                folder_ids.get(&media_id).copied().flatten(),
                "scrape",
            )
            .await?,
        );
    }

    let queued = tasks
        .iter()
        .filter_map(|run| {
            run.task
                .media_id
                .map(|media_id| (media_id, run.task.id, run.record_id))
        })
        .collect::<Vec<_>>();
    if !queued.is_empty() {
        for (media_id, task_id, record_id) in queued {
            tokio::spawn(run_scrape(
                state.clone(),
                media_id,
                task_id,
                record_id,
                options,
            ));
        }
    }

    Ok(BatchScrapeResponse {
        queued: tasks.len(),
        skipped,
        tasks: tasks.into_iter().map(|run| run.task).collect(),
    })
}

async fn run_scrape(
    state: AppState,
    media_id: i64,
    task_id: i64,
    record_id: i64,
    options: ScrapeOptions,
) {
    let Ok(_permit) = state.scrape_limiter.clone().acquire_owned().await else {
        return;
    };
    if !matches!(
        storage::start_task_run(&state.pool, task_id, record_id, 10).await,
        Ok(true)
    ) {
        return;
    }

    let media = match storage::media_by_id(&state.pool, media_id).await {
        Ok(media) => media,
        Err(error) => {
            fail_scrape(&state, media_id, task_id, record_id, &error.to_string()).await;
            return;
        }
    };

    let settings = match storage::load_settings(&state.pool).await {
        Ok(settings) => settings,
        Err(error) => {
            fail_scrape(&state, media_id, task_id, record_id, &error.to_string()).await;
            return;
        }
    };
    let client = match MetaTubeClient::new(&settings) {
        Ok(client) => client,
        Err(error) => {
            fail_scrape(&state, media_id, task_id, record_id, &error.to_string()).await;
            return;
        }
    };
    let matched = match client.search_movie(&media.title).await {
        Ok(matched) => matched,
        Err(error) => {
            fail_scrape(&state, media_id, task_id, record_id, &error.to_string()).await;
            return;
        }
    };
    let _ = storage::update_task_progress(&state.pool, task_id, record_id, 55).await;
    if !storage::task_run_is_running(&state.pool, task_id, record_id).await {
        return;
    }

    let remote_metadata = match client.movie_info(&matched.provider, &matched.id).await {
        Ok(metadata) => metadata,
        Err(error) => {
            fail_scrape(&state, media_id, task_id, record_id, &error.to_string()).await;
            return;
        }
    };
    let _ = storage::update_task_progress(&state.pool, task_id, record_id, 85).await;
    if !storage::task_run_is_running(&state.pool, task_id, record_id).await {
        return;
    }

    let provider_id = format!("{}:{}", matched.provider, matched.id);
    let remote_title = remote_metadata
        .get("title")
        .and_then(|value| value.as_str())
        .unwrap_or(&matched.title)
        .to_owned();
    let output_format = match media.folder_id {
        Some(folder_id) => match storage::folder_by_id(&state.pool, folder_id).await {
            Ok(folder) => folder.output_format,
            Err(error) => {
                fail_scrape(&state, media_id, task_id, record_id, &error.to_string()).await;
                return;
            }
        },
        None => settings.output_format.clone(),
    };
    let cached_poster = match asset::resolve_poster(
        &state,
        &client,
        &media,
        &media.title,
        Some((&matched, &remote_metadata)),
    )
    .await
    {
        Ok(poster) => poster,
        Err(error) => {
            tracing::error!(%error, media_id, "poster resolver failed; metadata will be kept");
            None
        }
    };
    let mut resolved_metadata = remote_metadata.clone();
    if let Some(object) = resolved_metadata.as_object_mut() {
        object.remove("big_cover_url");
        object.remove("cover_url");
        object.remove("poster_url");
    }
    let write_report = match metadata::write_sidecars(
        &client,
        &media,
        &resolved_metadata,
        WriteOptions {
            output_format: &output_format,
            overwrite_policy: &settings.overwrite_policy,
            overwrite_nfo: options.overwrite_nfo,
            overwrite_image: options.overwrite_image,
            provider: &matched.provider,
            remote_id: &matched.id,
            cached_poster: cached_poster.as_ref(),
        },
    )
    .await
    {
        Ok(report) => report,
        Err(error) => {
            fail_scrape(&state, media_id, task_id, record_id, &error.to_string()).await;
            return;
        }
    };
    let _ = storage::update_task_progress(&state.pool, task_id, record_id, 95).await;
    if !storage::task_run_is_running(&state.pool, task_id, record_id).await {
        return;
    }
    let written_files = write_report
        .written
        .iter()
        .map(|path| path.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    let skipped_files = write_report
        .skipped
        .iter()
        .map(|path| path.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    let metadata = json!({
        "metadata": resolved_metadata,
        "luma": {
            "providerId": provider_id,
            "posterUrl": cached_poster.as_ref().map(|_| format!("/asset/poster/{media_id}")),
            "coverUrl": cached_poster.as_ref().map(|_| format!("/asset/cover/{media_id}")),
            "posterSource": cached_poster.as_ref().map(|poster| poster.source.as_str()),
            "overwriteNfo": options.overwrite_nfo,
            "overwriteImage": options.overwrite_image,
            "writtenFiles": written_files,
            "skippedFiles": skipped_files
        }
    });
    let mut transaction = match state.pool.begin().await {
        Ok(transaction) => transaction,
        Err(_) => return,
    };
    let completed = sqlx::query(
        "UPDATE task_record SET status = 'success', progress = 100, finished_at = datetime('now') \
         WHERE id = ? AND task_id = ? AND status = 'running'",
    )
    .bind(record_id)
    .bind(task_id)
    .execute(&mut *transaction)
    .await;
    match completed {
        Ok(result) if result.rows_affected() == 1 => {}
        Ok(_) => {
            let _ = transaction.rollback().await;
            return;
        }
        Err(error) => {
            tracing::error!(%error, "could not complete scrape task record");
            let _ = transaction.rollback().await;
            fail_scrape(
                &state,
                media_id,
                task_id,
                record_id,
                "failed to save MetaTube metadata",
            )
            .await;
            return;
        }
    }
    let result = async {
        sqlx::query("UPDATE media_item SET provider_id = ?, title = ?, status = 'ready', updated_at = datetime('now') WHERE id = ?")
            .bind(&provider_id).bind(&remote_title).bind(media_id).execute(&mut *transaction).await?;
        sqlx::query("INSERT INTO metadata_record (media_id, provider, raw_json) VALUES (?, 'metatube', ?)")
            .bind(media_id).bind(metadata.to_string()).execute(&mut *transaction).await?;
        sqlx::query("UPDATE scrape_task SET status = 'success', progress = 100, error_message = NULL, finished_at = datetime('now'), updated_at = datetime('now') WHERE id = ?")
            .bind(task_id).execute(&mut *transaction).await?;
        transaction.commit().await
    }.await;
    if let Err(error) = result {
        tracing::error!(%error, "scrape task transaction failed");
        fail_scrape(
            &state,
            media_id,
            task_id,
            record_id,
            "failed to save MetaTube metadata",
        )
        .await;
    } else {
        storage::log(
            &state.pool,
            "info",
            "provider",
            &format!(
                "Fetched MetaTube metadata for {}; wrote {} sidecar files, skipped {} existing files",
                media.filename,
                write_report.written.len(),
                write_report.skipped.len()
            ),
        )
        .await;
    }
}

async fn fail_scrape(state: &AppState, media_id: i64, task_id: i64, record_id: i64, message: &str) {
    let _ =
        storage::finish_task_run(&state.pool, task_id, record_id, "failed", Some(message)).await;
    let _ = sqlx::query(
        "UPDATE media_item SET status = 'failed', updated_at = datetime('now') WHERE id = ?",
    )
    .bind(media_id)
    .execute(&state.pool)
    .await;
    storage::log(&state.pool, "error", "provider", message).await;
}

async fn list_crawlers(State(state): State<AppState>) -> AppResult<Json<Vec<CrawlerScript>>> {
    Ok(Json(crawler::list_scripts(&state.pool).await?))
}

struct CrawlerUpload {
    name: String,
    website_url: String,
    interval_minutes: u64,
    enabled: bool,
    auto_download: bool,
    file_name: Option<String>,
    script: Option<Vec<u8>>,
}

async fn read_crawler_upload(mut multipart: Multipart) -> AppResult<CrawlerUpload> {
    let mut name = None;
    let mut website_url = None;
    let mut interval_minutes = None;
    let mut enabled = None;
    let mut auto_download = None;
    let mut file_name = None;
    let mut script = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| AppError::BadRequest(format!("invalid upload: {error}")))?
    {
        let field_name = field.name().unwrap_or_default().to_owned();
        if field_name == "script" {
            let original = field
                .file_name()
                .and_then(|name| std::path::Path::new(name).file_name())
                .and_then(|name| name.to_str())
                .unwrap_or("crawler.py")
                .to_owned();
            let bytes = field
                .bytes()
                .await
                .map_err(|error| AppError::BadRequest(format!("invalid script upload: {error}")))?;
            file_name = Some(original);
            script = Some(bytes.to_vec());
            continue;
        }
        let value = field
            .text()
            .await
            .map_err(|error| AppError::BadRequest(format!("invalid form field: {error}")))?;
        match field_name.as_str() {
            "name" => name = Some(value),
            "websiteUrl" => website_url = Some(value),
            "intervalMinutes" => interval_minutes = value.parse().ok(),
            "enabled" => enabled = Some(matches!(value.as_str(), "true" | "1" | "on")),
            "autoDownload" => auto_download = Some(matches!(value.as_str(), "true" | "1" | "on")),
            _ => {}
        }
    }
    Ok(CrawlerUpload {
        name: name.unwrap_or_default(),
        website_url: website_url.unwrap_or_default(),
        interval_minutes: interval_minutes.unwrap_or(60),
        enabled: enabled.unwrap_or(true),
        auto_download: auto_download.unwrap_or(false),
        file_name,
        script,
    })
}

fn validate_crawler_upload(upload: &CrawlerUpload, script_required: bool) -> AppResult<()> {
    if upload.name.trim().is_empty() {
        return Err(AppError::BadRequest("crawler name is required".into()));
    }
    let website = reqwest::Url::parse(upload.website_url.trim())
        .map_err(|_| AppError::BadRequest("website URL is invalid".into()))?;
    if !matches!(website.scheme(), "http" | "https") {
        return Err(AppError::BadRequest(
            "website URL must use http or https".into(),
        ));
    }
    if !(1..=10080).contains(&upload.interval_minutes) {
        return Err(AppError::BadRequest(
            "crawler interval must be between 1 and 10080 minutes".into(),
        ));
    }
    if script_required && upload.script.is_none() {
        return Err(AppError::BadRequest("a Python script is required".into()));
    }
    if let Some(script) = &upload.script {
        if script.is_empty() || script.len() > 1024 * 1024 {
            return Err(AppError::BadRequest(
                "Python script must be between 1 byte and 1 MiB".into(),
            ));
        }
        if !upload
            .file_name
            .as_deref()
            .is_some_and(|name| name.to_ascii_lowercase().ends_with(".py"))
        {
            return Err(AppError::BadRequest(
                "script filename must end in .py".into(),
            ));
        }
    }
    Ok(())
}

async fn create_crawler(
    State(state): State<AppState>,
    multipart: Multipart,
) -> AppResult<(StatusCode, Json<CrawlerScript>)> {
    let upload = read_crawler_upload(multipart).await?;
    validate_crawler_upload(&upload, true)?;
    let mut transaction = state.pool.begin().await?;
    let result = sqlx::query(
        "INSERT INTO crawler_script (name, website_url, file_name, file_path, interval_minutes, enabled, auto_download, next_run_at) \
         VALUES (?, ?, ?, '', ?, ?, ?, CASE WHEN ? THEN datetime('now') ELSE NULL END)",
    )
    .bind(upload.name.trim())
    .bind(upload.website_url.trim())
    .bind(upload.file_name.as_deref().unwrap_or("crawler.py"))
    .bind(upload.interval_minutes as i64)
    .bind(upload.enabled)
    .bind(upload.auto_download)
    .bind(upload.enabled)
    .execute(&mut *transaction)
    .await?;
    let id = result.last_insert_rowid();
    let file_path = state.script_root.join("scripts").join(format!("{id}.py"));
    sqlx::query("UPDATE crawler_script SET file_path = ? WHERE id = ?")
        .bind(file_path.to_string_lossy().to_string())
        .bind(id)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    if let Err(error) = tokio::fs::write(&file_path, upload.script.unwrap_or_default()).await {
        let _ = sqlx::query("DELETE FROM crawler_script WHERE id = ?")
            .bind(id)
            .execute(&state.pool)
            .await;
        return Err(AppError::Internal(error.into()));
    }
    Ok((
        StatusCode::CREATED,
        Json(crawler::script_by_id(&state.pool, id).await?),
    ))
}

async fn update_crawler(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    multipart: Multipart,
) -> AppResult<Json<CrawlerScript>> {
    crawler::script_by_id(&state.pool, id).await?;
    let upload = read_crawler_upload(multipart).await?;
    validate_crawler_upload(&upload, false)?;
    let existing_path: String =
        sqlx::query_scalar("SELECT file_path FROM crawler_script WHERE id = ?")
            .bind(id)
            .fetch_one(&state.pool)
            .await?;
    if let Some(script) = upload.script {
        tokio::fs::write(&existing_path, script)
            .await
            .map_err(|error| AppError::Internal(error.into()))?;
    }
    sqlx::query(
        "UPDATE crawler_script SET name = ?, website_url = ?, file_name = COALESCE(?, file_name), \
         interval_minutes = ?, enabled = ?, auto_download = ?, \
         next_run_at = CASE WHEN ? THEN datetime('now', '+' || ? || ' minutes') ELSE NULL END, \
         updated_at = datetime('now') WHERE id = ?",
    )
    .bind(upload.name.trim())
    .bind(upload.website_url.trim())
    .bind(upload.file_name)
    .bind(upload.interval_minutes as i64)
    .bind(upload.enabled)
    .bind(upload.auto_download)
    .bind(upload.enabled)
    .bind(upload.interval_minutes as i64)
    .bind(id)
    .execute(&state.pool)
    .await?;
    Ok(Json(crawler::script_by_id(&state.pool, id).await?))
}

async fn delete_crawler(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<StatusCode> {
    let file_path: String = sqlx::query_scalar("SELECT file_path FROM crawler_script WHERE id = ?")
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or(AppError::NotFound)?;
    sqlx::query("DELETE FROM crawler_script WHERE id = ?")
        .bind(id)
        .execute(&state.pool)
        .await?;
    if !file_path.is_empty() {
        let _ = tokio::fs::remove_file(file_path).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn run_crawler(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<(StatusCode, Json<CrawlerRun>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(crawler::queue_run(&state, id).await?),
    ))
}

async fn list_crawler_runs(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> AppResult<Json<Vec<CrawlerRun>>> {
    let script_id = query.get("scriptId").and_then(|value| value.parse().ok());
    Ok(Json(crawler::list_runs(&state.pool, script_id).await?))
}

async fn list_crawler_results(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> AppResult<Json<Vec<CrawlerResult>>> {
    let script_id = query.get("scriptId").and_then(|value| value.parse().ok());
    Ok(Json(crawler::list_results(&state.pool, script_id).await?))
}

async fn download_crawler_result(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<(StatusCode, Json<CrawlerResult>)> {
    Ok((
        StatusCode::ACCEPTED,
        Json(crawler::download_result(&state, id).await?),
    ))
}

async fn download_crawler_results(
    State(state): State<AppState>,
    Json(input): Json<BatchCrawlerResultInput>,
) -> AppResult<Json<Vec<CrawlerResult>>> {
    if input.result_ids.is_empty() || input.result_ids.len() > 100 {
        return Err(AppError::BadRequest(
            "select between 1 and 100 results".into(),
        ));
    }
    let mut downloaded = Vec::new();
    for id in input.result_ids {
        downloaded.push(crawler::download_result(&state, id).await?);
    }
    Ok(Json(downloaded))
}

async fn ignore_crawler_result(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<Json<CrawlerResult>> {
    Ok(Json(crawler::ignore_result(&state.pool, id).await?))
}

async fn list_downloads(State(state): State<AppState>) -> AppResult<Json<Vec<DownloadItem>>> {
    let client = qbittorrent_client(&state).await?;
    let downloads = client
        .torrents()
        .await
        .map_err(|error| AppError::BadRequest(error.to_string()))?;
    Ok(Json(downloads))
}

async fn pause_download(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> AppResult<StatusCode> {
    validate_download_hash(&hash)?;
    qbittorrent_client(&state)
        .await?
        .pause(&hash)
        .await
        .map_err(|error| AppError::BadRequest(error.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn resume_download(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> AppResult<StatusCode> {
    validate_download_hash(&hash)?;
    qbittorrent_client(&state)
        .await?
        .resume(&hash)
        .await
        .map_err(|error| AppError::BadRequest(error.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn remove_download(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> AppResult<StatusCode> {
    validate_download_hash(&hash)?;
    qbittorrent_client(&state)
        .await?
        .remove(&hash)
        .await
        .map_err(|error| AppError::BadRequest(error.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn qbittorrent_client(state: &AppState) -> AppResult<QBittorrentClient> {
    let settings = storage::load_settings(&state.pool).await?;
    QBittorrentClient::new(&settings).map_err(|error| AppError::BadRequest(error.to_string()))
}

fn validate_download_hash(hash: &str) -> AppResult<()> {
    if !matches!(hash.len(), 40 | 64) || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(AppError::BadRequest("invalid torrent hash".into()));
    }
    Ok(())
}

async fn get_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> AppResult<Json<Settings>> {
    let configured: Option<String> =
        sqlx::query_scalar("SELECT value FROM app_setting WHERE key = 'metatube_url'")
            .fetch_optional(&state.pool)
            .await?;
    let environment_override = std::env::var("LUMA_METATUBE_URL")
        .ok()
        .is_some_and(|value| !value.trim().is_empty());
    if configured.as_deref().is_none_or(|value| value == "auto")
        && !environment_override
        && let Some(url) = inferred_metatube_url(&headers)
    {
        sqlx::query(
            "INSERT INTO app_setting (key, value, updated_at) VALUES ('deployment_metatube_url', ?, datetime('now')) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')",
        )
        .bind(url)
        .execute(&state.pool)
        .await?;
    }
    Ok(Json(storage::load_settings(&state.pool).await?))
}

async fn update_settings(
    State(state): State<AppState>,
    Json(settings): Json<Settings>,
) -> AppResult<Json<Settings>> {
    validate_settings(&settings)?;
    let values = [
        ("metatube_url", settings.metatube_url.clone()),
        ("metatube_token", settings.metatube_token.clone()),
        ("output_format", settings.output_format.clone()),
        ("scan_interval", settings.scan_interval.to_string()),
        ("overwrite_policy", settings.overwrite_policy.clone()),
        ("log_level", settings.log_level.clone()),
        ("qbittorrent_url", settings.qbittorrent_url.clone()),
        (
            "qbittorrent_username",
            settings.qbittorrent_username.clone(),
        ),
        (
            "qbittorrent_password",
            settings.qbittorrent_password.clone(),
        ),
        (
            "qbittorrent_auto_update_trackers",
            settings.qbittorrent_auto_update_trackers.to_string(),
        ),
        (
            "qbittorrent_tracker_source_url",
            settings.qbittorrent_tracker_source_url.clone(),
        ),
        (
            "qbittorrent_tracker_update_interval",
            settings.qbittorrent_tracker_update_interval.to_string(),
        ),
    ];
    let mut transaction = state.pool.begin().await?;
    for (key, value) in values {
        sqlx::query("INSERT INTO app_setting (key, value, updated_at) VALUES (?, ?, datetime('now')) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')")
            .bind(key).bind(value).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    storage::log(
        &state.pool,
        "info",
        "settings",
        "Updated application settings",
    )
    .await;
    Ok(Json(settings))
}

async fn service_status(State(state): State<AppState>) -> AppResult<Json<ServiceStatus>> {
    let settings = storage::load_settings(&state.pool).await?;
    let checked_at: String = sqlx::query_scalar("SELECT datetime('now')")
        .fetch_one(&state.pool)
        .await?;
    let (meta_tube, qbittorrent) = tokio::join!(
        probe_metatube(settings.clone()),
        probe_qbittorrent(settings)
    );
    Ok(Json(ServiceStatus {
        luma: ServiceHealth {
            connected: true,
            message: "Luma is running".into(),
            latency_ms: Some(0),
        },
        meta_tube,
        qbittorrent,
        checked_at,
    }))
}

async fn probe_metatube(settings: Settings) -> ServiceHealth {
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let client = MetaTubeClient::new(&settings)?;
        let provider_count = client.test_connection().await?;
        Ok::<_, anyhow::Error>(format!("{provider_count} movie providers"))
    })
    .await;
    health_from_probe(result, started)
}

async fn probe_qbittorrent(settings: Settings) -> ServiceHealth {
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let client = QBittorrentClient::new(&settings)?;
        let version = client.version().await?;
        Ok::<_, anyhow::Error>(version)
    })
    .await;
    health_from_probe(result, started)
}

fn health_from_probe(
    result: Result<Result<String, anyhow::Error>, tokio::time::error::Elapsed>,
    started: std::time::Instant,
) -> ServiceHealth {
    let latency_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    match result {
        Ok(Ok(message)) => ServiceHealth {
            connected: true,
            message,
            latency_ms: Some(latency_ms),
        },
        Ok(Err(error)) => ServiceHealth {
            connected: false,
            message: error.to_string(),
            latency_ms: Some(latency_ms),
        },
        Err(_) => ServiceHealth {
            connected: false,
            message: "connection check timed out after 5 seconds".into(),
            latency_ms: Some(latency_ms),
        },
    }
}

async fn test_metatube(Json(settings): Json<Settings>) -> AppResult<Json<MetaTubeConnection>> {
    validate_metatube_settings(&settings)?;
    let client =
        MetaTubeClient::new(&settings).map_err(|error| AppError::BadRequest(error.to_string()))?;
    let provider_count = client
        .test_connection()
        .await
        .map_err(|error| AppError::BadRequest(error.to_string()))?;
    Ok(Json(MetaTubeConnection {
        connected: true,
        provider_count,
        message: format!("Connected to MetaTube with {provider_count} movie providers"),
    }))
}

async fn test_qbittorrent(
    Json(settings): Json<Settings>,
) -> AppResult<Json<QBittorrentConnection>> {
    validate_qbittorrent_settings(&settings)?;
    let version = QBittorrentClient::new(&settings)
        .map_err(|error| AppError::BadRequest(error.to_string()))?
        .version()
        .await
        .map_err(|error| AppError::BadRequest(error.to_string()))?;
    Ok(Json(QBittorrentConnection {
        connected: true,
        message: format!("Connected to qBittorrent {version}"),
        version,
    }))
}

async fn ensure_metatube_connected(state: &AppState) -> AppResult<()> {
    let settings = storage::load_settings(&state.pool).await?;
    validate_metatube_settings(&settings)?;
    let client = MetaTubeClient::new(&settings)
        .map_err(|error| AppError::BadRequest(format!("MetaTube connection failed: {error}")))?;
    client
        .test_connection()
        .await
        .map_err(|error| AppError::BadRequest(format!("MetaTube connection failed: {error}")))?;
    Ok(())
}

async fn list_logs(State(state): State<AppState>) -> AppResult<Json<Vec<LogEntry>>> {
    let rows = sqlx::query("SELECT * FROM system_log ORDER BY created_at DESC, id DESC LIMIT 100")
        .fetch_all(&state.pool)
        .await?;
    Ok(Json(
        rows.iter()
            .map(|row| LogEntry {
                id: row.get("id"),
                level: row.get("level"),
                module: row.get("module"),
                message: row.get("message"),
                created_at: row.get("created_at"),
            })
            .collect(),
    ))
}

fn validate_folder(input: &FolderInput) -> AppResult<()> {
    if input.name.trim().is_empty() {
        return Err(AppError::BadRequest("folder name is required".into()));
    }
    if input.path.trim().is_empty() {
        return Err(AppError::BadRequest("folder path is required".into()));
    }
    if !matches!(input.media_type.as_str(), "movie" | "tv" | "mixed") {
        return Err(AppError::BadRequest(
            "media type must be movie, tv, or mixed".into(),
        ));
    }
    if !matches!(input.output_format.as_str(), "nfo" | "json" | "both") {
        return Err(AppError::BadRequest(
            "output format must be nfo, json, or both".into(),
        ));
    }
    if !matches!(input.scan_mode.as_str(), "manual" | "watch" | "interval") {
        return Err(AppError::BadRequest(
            "scan mode must be manual, watch, or interval".into(),
        ));
    }
    Ok(())
}

async fn validate_folder_path(path: &str) -> AppResult<()> {
    let path = path.trim();
    let metadata = tokio::fs::metadata(path).await.map_err(|error| {
        AppError::BadRequest(format!("folder path is not accessible: {path} ({error})"))
    })?;
    if !metadata.is_dir() {
        return Err(AppError::BadRequest(format!(
            "folder path is not a directory: {path}"
        )));
    }
    Ok(())
}

fn validate_batch_task_input(input: &BatchTaskInput) -> AppResult<()> {
    if input.task_ids.is_empty() {
        return Err(AppError::BadRequest("select at least one task".into()));
    }
    if input.task_ids.len() > 250 {
        return Err(AppError::BadRequest(
            "a batch can contain at most 250 tasks".into(),
        ));
    }
    Ok(())
}

fn validate_settings(settings: &Settings) -> AppResult<()> {
    validate_metatube_settings(settings)?;
    validate_qbittorrent_settings(settings)?;
    if settings.scan_interval == 0 {
        return Err(AppError::BadRequest(
            "scan interval must be greater than zero".into(),
        ));
    }
    if !matches!(settings.output_format.as_str(), "nfo" | "json" | "both") {
        return Err(AppError::BadRequest(
            "output format must be nfo, json, or both".into(),
        ));
    }
    if !matches!(
        settings.overwrite_policy.as_str(),
        "missing" | "always" | "never"
    ) {
        return Err(AppError::BadRequest(
            "overwrite policy must be missing, always, or never".into(),
        ));
    }
    Ok(())
}

fn validate_metatube_settings(settings: &Settings) -> AppResult<()> {
    if settings.metatube_url.trim().is_empty() {
        return Err(AppError::BadRequest("MetaTube URL is required".into()));
    }
    let metatube_url = reqwest::Url::parse(settings.metatube_url.trim())
        .map_err(|_| AppError::BadRequest("MetaTube URL is invalid".into()))?;
    if !matches!(metatube_url.scheme(), "http" | "https") {
        return Err(AppError::BadRequest(
            "MetaTube URL must use http or https".into(),
        ));
    }
    Ok(())
}

fn validate_qbittorrent_settings(settings: &Settings) -> AppResult<()> {
    let qbit_url = reqwest::Url::parse(settings.qbittorrent_url.trim())
        .map_err(|_| AppError::BadRequest("qBittorrent URL is invalid".into()))?;
    if !matches!(qbit_url.scheme(), "http" | "https") {
        return Err(AppError::BadRequest(
            "qBittorrent URL must use http or https".into(),
        ));
    }
    if settings.qbittorrent_tracker_update_interval == 0 {
        return Err(AppError::BadRequest(
            "tracker update interval must be greater than zero".into(),
        ));
    }
    if settings.qbittorrent_auto_update_trackers {
        let tracker_url = reqwest::Url::parse(settings.qbittorrent_tracker_source_url.trim())
            .map_err(|_| AppError::BadRequest("tracker source URL is invalid".into()))?;
        if !matches!(tracker_url.scheme(), "http" | "https") {
            return Err(AppError::BadRequest(
                "tracker source URL must use http or https".into(),
            ));
        }
    }
    Ok(())
}

fn inferred_metatube_url(headers: &HeaderMap) -> Option<String> {
    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get("host"))?
        .to_str()
        .ok()?
        .split(',')
        .next()?
        .trim();
    let hostname = if host.starts_with('[') {
        let end = host.find(']')?;
        &host[..=end]
    } else if let Some((name, port)) = host.rsplit_once(':') {
        if port.chars().all(|character| character.is_ascii_digit()) {
            name
        } else {
            host
        }
    } else {
        host
    };
    (!hostname.is_empty()).then(|| format!("http://{hostname}:8080"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infers_metatube_from_deployment_host() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "192.168.1.20:3000".parse().unwrap());
        assert_eq!(
            inferred_metatube_url(&headers).as_deref(),
            Some("http://192.168.1.20:8080")
        );
    }

    #[test]
    fn supports_forwarded_ipv6_host() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-host", "[2001:db8::1]:3000".parse().unwrap());
        assert_eq!(
            inferred_metatube_url(&headers).as_deref(),
            Some("http://[2001:db8::1]:8080")
        );
    }

    #[test]
    fn accepts_only_complete_hex_torrent_hashes() {
        assert!(validate_download_hash("0123456789abcdef0123456789abcdef01234567").is_ok());
        assert!(validate_download_hash(&"a".repeat(64)).is_ok());
        assert!(validate_download_hash("../../api/v2/app/shutdown").is_err());
        assert!(validate_download_hash("0123456789abcdef").is_err());
    }
}
