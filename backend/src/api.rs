use std::collections::HashMap;

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
        DashboardStats, Folder, FolderInput, LogEntry, MediaItem, MetaTubeConnection,
        ScrapeOptions, Settings, Task,
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
        .route("/tasks/{id}", get(get_task))
        .route("/tasks/{id}/retry", post(retry_task))
        .route("/tasks/{id}/cancel", post(cancel_task))
        .route("/media", get(list_media))
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
    Ok((StatusCode::CREATED, Json(folder)))
}

async fn update_folder(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(input): Json<FolderInput>,
) -> AppResult<Json<Folder>> {
    validate_folder(&input)?;
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
    tokio::spawn(scanner::run_scan(state, folder, task.id));
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
    let previous = storage::task_by_id(&state.pool, id).await?;
    if matches!(previous.status.as_str(), "pending" | "running") {
        return Err(AppError::BadRequest("active task cannot be retried".into()));
    }
    let task = storage::create_task(
        &state.pool,
        previous.media_id,
        previous.folder_id,
        &previous.task_type,
    )
    .await?;
    match previous.task_type.as_str() {
        "scan" => {
            let folder =
                storage::folder_by_id(&state.pool, previous.folder_id.ok_or(AppError::NotFound)?)
                    .await?;
            tokio::spawn(scanner::run_scan(state, folder, task.id));
        }
        "scrape" => {
            let media_id = previous.media_id.ok_or(AppError::NotFound)?;
            tokio::spawn(run_scrape(
                state,
                media_id,
                task.id,
                ScrapeOptions {
                    overwrite_nfo: false,
                    overwrite_image: false,
                },
            ));
        }
        _ => return Err(AppError::BadRequest("unknown task type".into())),
    }
    Ok((StatusCode::ACCEPTED, Json(task)))
}

async fn cancel_task(State(state): State<AppState>, Path(id): Path<i64>) -> AppResult<Json<Task>> {
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
    Ok(Json(storage::task_by_id(&state.pool, id).await?))
}

async fn list_media(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> AppResult<Json<Vec<MediaItem>>> {
    let rows = if let Some(search) = query.get("search").filter(|value| !value.is_empty()) {
        let pattern = format!("%{search}%");
        sqlx::query("SELECT * FROM media_item WHERE filename LIKE ? OR title LIKE ? ORDER BY updated_at DESC LIMIT 500")
            .bind(&pattern).bind(&pattern).fetch_all(&state.pool).await?
    } else {
        sqlx::query("SELECT * FROM media_item ORDER BY updated_at DESC LIMIT 500")
            .fetch_all(&state.pool)
            .await?
    };
    Ok(Json(rows.iter().map(media_from_row).collect()))
}

async fn scrape_media(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(options): Json<ScrapeOptions>,
) -> AppResult<(StatusCode, Json<Task>)> {
    storage::media_by_id(&state.pool, id).await?;
    let task = storage::create_task(&state.pool, Some(id), None, "scrape").await?;
    tokio::spawn(run_scrape(state, id, task.id, options));
    Ok((StatusCode::ACCEPTED, Json(task)))
}

async fn run_scrape(state: AppState, media_id: i64, task_id: i64, options: ScrapeOptions) {
    let _ = sqlx::query("UPDATE scrape_task SET status = 'running', progress = 10 WHERE id = ?")
        .bind(task_id)
        .execute(&state.pool)
        .await;

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
