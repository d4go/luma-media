use std::collections::{HashMap, HashSet};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post, put},
};
use serde_json::json;
use sqlx::Row;

use crate::{
    AppState,
    error::{AppError, AppResult},
    metadata::{self, WriteOptions},
    models::{
        BatchScrapeInput, BatchScrapeResponse, BatchTaskInput, BatchTaskResponse, DashboardStats,
        Folder, FolderInput, LogEntry, MediaItem, MetaTubeConnection, ScrapeOptions, Settings,
        Task,
    },
    provider::MetaTubeClient,
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
        .route("/settings/metatube/test", post(test_metatube))
        .route("/logs", get(list_logs))
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
    let rows = sqlx::query("SELECT * FROM scrape_task ORDER BY created_at DESC, id DESC LIMIT 8")
        .fetch_all(&state.pool)
        .await?;
    let recent_activity = rows.iter().map(task_from_row).collect();
    Ok(Json(DashboardStats {
        media_count,
        task_count,
        success_count,
        failed_count,
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
        let task = storage::create_task(&state.pool, None, Some(folder.id), "scan").await?;
        tokio::spawn(run_scan_with_auto_scrape(state, folder.clone(), task.id));
    }
    Ok((StatusCode::CREATED, Json(folder)))
}

pub(crate) async fn run_scan_with_auto_scrape(state: AppState, folder: Folder, scan_task_id: i64) {
    scanner::run_scan(state.clone(), folder.clone(), scan_task_id).await;
    let scan_status: Option<String> =
        sqlx::query_scalar("SELECT status FROM scrape_task WHERE id = ?")
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
    let task = storage::create_task(&state.pool, None, Some(id), "scan").await?;
    tokio::spawn(run_scan_with_auto_scrape(state, folder, task.id));
    Ok((StatusCode::ACCEPTED, Json(task)))
}

async fn list_tasks(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> AppResult<Json<Vec<Task>>> {
    let rows = if let Some(status) = query.get("status").filter(|value| !value.is_empty()) {
        sqlx::query("SELECT * FROM scrape_task WHERE status = ? ORDER BY created_at DESC, id DESC LIMIT 250")
            .bind(status).fetch_all(&state.pool).await?
    } else {
        sqlx::query("SELECT * FROM scrape_task ORDER BY created_at DESC, id DESC LIMIT 250")
            .fetch_all(&state.pool)
            .await?
    };
    Ok(Json(rows.iter().map(task_from_row).collect()))
}

async fn get_task(State(state): State<AppState>, Path(id): Path<i64>) -> AppResult<Json<Task>> {
    Ok(Json(storage::task_by_id(&state.pool, id).await?))
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

    let active: i64 = match &target {
        RetryTarget::Scan(folder) => sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM scrape_task WHERE folder_id = ? AND task_type = 'scan' AND status IN ('pending', 'running'))",
        )
        .bind(folder.id)
        .fetch_one(&state.pool)
        .await?,
        RetryTarget::Scrape(media_id) => sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM scrape_task WHERE media_id = ? AND task_type = 'scrape' AND status IN ('pending', 'running'))",
        )
        .bind(media_id)
        .fetch_one(&state.pool)
        .await?,
    };
    if active != 0 {
        return Err(AppError::BadRequest(
            "a retry for this resource is already active".into(),
        ));
    }

    let task = storage::create_task(
        &state.pool,
        previous.media_id,
        previous.folder_id,
        &previous.task_type,
    )
    .await?;
    match target {
        RetryTarget::Scan(folder) => {
            tokio::spawn(run_scan_with_auto_scrape(state.clone(), folder, task.id));
        }
        RetryTarget::Scrape(media_id) => {
            tokio::spawn(run_scrape(
                state.clone(),
                media_id,
                task.id,
                ScrapeOptions::default(),
            ));
        }
    }
    Ok(task)
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
    let result = sqlx::query(
        "UPDATE scrape_task SET status = 'cancelled', finished_at = datetime('now') WHERE id = ? AND status IN ('pending', 'running')",
    ).bind(id).execute(&state.pool).await?;
    if result.rows_affected() == 0 {
        let task = storage::task_by_id(&state.pool, id).await?;
        return Err(AppError::BadRequest(format!(
            "task is already {}",
            task.status
        )));
    }
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
    if let Some(status) = status
        && !matches!(status, "pending" | "ready" | "failed")
    {
        return Err(AppError::BadRequest("unknown media status".into()));
    }
    let rows = match (search, status) {
        (Some(search), Some(status)) => {
            let pattern = format!("%{search}%");
            sqlx::query("SELECT * FROM media_item WHERE (filename LIKE ? OR title LIKE ?) AND status = ? ORDER BY updated_at DESC LIMIT 500")
                .bind(&pattern).bind(&pattern).bind(status).fetch_all(&state.pool).await?
        }
        (Some(search), None) => {
            let pattern = format!("%{search}%");
            sqlx::query("SELECT * FROM media_item WHERE filename LIKE ? OR title LIKE ? ORDER BY updated_at DESC LIMIT 500")
                .bind(&pattern).bind(&pattern).fetch_all(&state.pool).await?
        }
        (None, Some(status)) => {
            sqlx::query(
                "SELECT * FROM media_item WHERE status = ? ORDER BY updated_at DESC LIMIT 500",
            )
            .bind(status)
            .fetch_all(&state.pool)
            .await?
        }
        (None, None) => {
            sqlx::query("SELECT * FROM media_item ORDER BY updated_at DESC LIMIT 500")
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
    let task = storage::create_task(&state.pool, Some(id), media.folder_id, "scrape").await?;
    tokio::spawn(run_scrape(state, id, task.id, options));
    Ok((StatusCode::ACCEPTED, Json(task)))
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
            storage::create_task(
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
        .filter_map(|task| task.media_id.map(|media_id| (media_id, task.id)))
        .collect::<Vec<_>>();
    if !queued.is_empty() {
        let state = state.clone();
        tokio::spawn(async move {
            for (media_id, task_id) in queued {
                run_scrape(state.clone(), media_id, task_id, options).await;
            }
        });
    }

    Ok(BatchScrapeResponse {
        created: tasks.len(),
        skipped,
        tasks,
    })
}

async fn run_scrape(state: AppState, media_id: i64, task_id: i64, options: ScrapeOptions) {
    let started = sqlx::query(
        "UPDATE scrape_task SET status = 'running', progress = 10 WHERE id = ? AND status = 'pending'",
    )
    .bind(task_id)
    .execute(&state.pool)
    .await;
    if !matches!(started, Ok(result) if result.rows_affected() == 1) {
        return;
    }

    let media = match storage::media_by_id(&state.pool, media_id).await {
        Ok(media) => media,
        Err(error) => {
            fail_scrape(&state, media_id, task_id, &error.to_string()).await;
            return;
        }
    };

    let settings = match storage::load_settings(&state.pool).await {
        Ok(settings) => settings,
        Err(error) => {
            fail_scrape(&state, media_id, task_id, &error.to_string()).await;
            return;
        }
    };
    let client = match MetaTubeClient::new(&settings) {
        Ok(client) => client,
        Err(error) => {
            fail_scrape(&state, media_id, task_id, &error.to_string()).await;
            return;
        }
    };
    let matched = match client.search_movie(&media.title).await {
        Ok(matched) => matched,
        Err(error) => {
            fail_scrape(&state, media_id, task_id, &error.to_string()).await;
            return;
        }
    };
    let _ = sqlx::query("UPDATE scrape_task SET progress = 55 WHERE id = ? AND status = 'running'")
        .bind(task_id)
        .execute(&state.pool)
        .await;
    if !task_is_running(&state, task_id).await {
        return;
    }

    let remote_metadata = match client.movie_info(&matched.provider, &matched.id).await {
        Ok(metadata) => metadata,
        Err(error) => {
            fail_scrape(&state, media_id, task_id, &error.to_string()).await;
            return;
        }
    };
    let _ = sqlx::query("UPDATE scrape_task SET progress = 85 WHERE id = ? AND status = 'running'")
        .bind(task_id)
        .execute(&state.pool)
        .await;
    if !task_is_running(&state, task_id).await {
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
                fail_scrape(&state, media_id, task_id, &error.to_string()).await;
                return;
            }
        },
        None => settings.output_format.clone(),
    };
    let write_report = match metadata::write_sidecars(
        &client,
        &media,
        &remote_metadata,
        WriteOptions {
            output_format: &output_format,
            overwrite_policy: &settings.overwrite_policy,
            overwrite_nfo: options.overwrite_nfo,
            overwrite_image: options.overwrite_image,
            provider: &matched.provider,
            remote_id: &matched.id,
        },
    )
    .await
    {
        Ok(report) => report,
        Err(error) => {
            fail_scrape(&state, media_id, task_id, &error.to_string()).await;
            return;
        }
    };
    let _ = sqlx::query("UPDATE scrape_task SET progress = 95 WHERE id = ? AND status = 'running'")
        .bind(task_id)
        .execute(&state.pool)
        .await;
    if !task_is_running(&state, task_id).await {
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
        "metadata": remote_metadata,
        "luma": {
            "providerId": provider_id,
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
    let result = async {
        sqlx::query("UPDATE media_item SET provider_id = ?, title = ?, status = 'ready', updated_at = datetime('now') WHERE id = ?")
            .bind(&provider_id).bind(&remote_title).bind(media_id).execute(&mut *transaction).await?;
        sqlx::query("INSERT INTO metadata_record (media_id, provider, raw_json) VALUES (?, 'metatube', ?)")
            .bind(media_id).bind(metadata.to_string()).execute(&mut *transaction).await?;
        sqlx::query("UPDATE scrape_task SET status = 'success', progress = 100, finished_at = datetime('now') WHERE id = ?")
            .bind(task_id).execute(&mut *transaction).await?;
        transaction.commit().await
    }.await;
    if let Err(error) = result {
        tracing::error!(%error, "scrape task transaction failed");
        fail_scrape(
            &state,
            media_id,
            task_id,
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

async fn task_is_running(state: &AppState, task_id: i64) -> bool {
    let status: Option<String> = sqlx::query_scalar("SELECT status FROM scrape_task WHERE id = ?")
        .bind(task_id)
        .fetch_optional(&state.pool)
        .await
        .ok()
        .flatten();
    status.as_deref() == Some("running")
}

async fn fail_scrape(state: &AppState, media_id: i64, task_id: i64, message: &str) {
    let _ = sqlx::query(
        "UPDATE scrape_task SET status = 'failed', error_message = ?, finished_at = datetime('now') WHERE id = ? AND status = 'running'",
    )
    .bind(message)
    .bind(task_id)
    .execute(&state.pool)
    .await;
    let _ = sqlx::query(
        "UPDATE media_item SET status = 'failed', updated_at = datetime('now') WHERE id = ?",
    )
    .bind(media_id)
    .execute(&state.pool)
    .await;
    storage::log(&state.pool, "error", "provider", message).await;
}

async fn get_settings(State(state): State<AppState>) -> AppResult<Json<Settings>> {
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

async fn test_metatube(Json(settings): Json<Settings>) -> AppResult<Json<MetaTubeConnection>> {
    validate_settings(&settings)?;
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

async fn ensure_metatube_connected(state: &AppState) -> AppResult<()> {
    let settings = storage::load_settings(&state.pool).await?;
    validate_settings(&settings)?;
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
    if settings.metatube_url.trim().is_empty() {
        return Err(AppError::BadRequest("MetaTube URL is required".into()));
    }
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
