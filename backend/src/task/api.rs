use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    AppState,
    error::{AppError, AppResult},
    pagination::{PageParams, Paged},
};

use super::model::{CreateJobRun, JobItemStatus, JobStatus};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/jobs", get(list_runs))
        .route("/jobs/bootstrap", post(create_bootstrap_run))
        .route("/jobs/incremental", post(create_incremental_run))
        .route("/jobs/{id}", get(run_detail))
        .route("/jobs/{id}/items", get(run_items))
        .route("/jobs/{id}/events", get(run_events))
        .route("/jobs/{id}/pause", post(pause_run))
        .route("/jobs/{id}/resume", post(resume_run))
        .route("/jobs/{id}/cancel", post(cancel_run))
        .route("/jobs/{id}/retry-failed", post(retry_failed))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunListQuery {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    page: u32,
    #[serde(default)]
    page_size: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateRunInput {
    provider_key: String,
    #[serde(default)]
    from: String,
    #[serde(default)]
    to: String,
    #[serde(default = "default_true")]
    include_resources: bool,
}

fn default_true() -> bool {
    true
}

async fn list_runs(
    State(state): State<AppState>,
    Query(query): Query<RunListQuery>,
) -> AppResult<Json<Paged<super::model::JobRun>>> {
    let status = query
        .status
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(JobStatus::parse);
    let params = PageParams {
        page: query.page,
        page_size: query.page_size,
    };
    let (items, total) = state
        .task_engine
        .list_runs(status, params.page() as i64, params.page_size() as i64)
        .await?;
    Ok(Json(Paged::new(items, total, params)))
}

async fn create_bootstrap_run(
    State(state): State<AppState>,
    Json(input): Json<CreateRunInput>,
) -> AppResult<(StatusCode, Json<Value>)> {
    create_run_with_mode(&state, input, "bootstrap").await
}

async fn create_incremental_run(
    State(state): State<AppState>,
    Json(input): Json<CreateRunInput>,
) -> AppResult<(StatusCode, Json<Value>)> {
    create_run_with_mode(&state, input, "incremental").await
}

async fn create_run_with_mode(
    state: &AppState,
    input: CreateRunInput,
    mode: &str,
) -> AppResult<(StatusCode, Json<Value>)> {
    let provider = crate::product::source_provider_by_key(state, &input.provider_key)
        .await?
        .ok_or_else(|| AppError::BadRequest("数据源不存在".into()))?;
    let (from, to) =
        if mode == "incremental" && (input.from.trim().is_empty() || input.to.trim().is_empty()) {
            let today = chrono::Utc::now().date_naive();
            (
                (today - chrono::Duration::days(7))
                    .format("%Y-%m-%d")
                    .to_string(),
                today.format("%Y-%m-%d").to_string(),
            )
        } else {
            (input.from.trim().to_owned(), input.to.trim().to_owned())
        };
    validate_date_range(&from, &to)?;
    // v2 deliberately invalidates historical runs created before catalogue
    // pagination/release-date ordering was fixed. Otherwise SQLite's unique
    // idempotency key would keep returning the old zero-result run.
    let idempotency_key = format!("{mode}:{}:{from}:{to}:v2", input.provider_key);
    let config = super::bootstrap::default_bootstrap_config(
        &input.provider_key,
        from.clone(),
        to.clone(),
        input.include_resources,
        mode,
    );
    let run = state
        .task_engine
        .create_run(CreateJobRun {
            job_type: "bootstrap",
            provider_key: Some(&input.provider_key),
            idempotency_key: &idempotency_key,
            priority: if mode == "incremental" { 800 } else { 100 },
            config: config.clone(),
            job_definition_id: None,
        })
        .await?;
    let page_url = crate::product::source_catalogue_start_url(&provider, mode == "bootstrap")?;
    let first_page = json!({
        "page": 1,
        "pageUrl": page_url,
        "from": from,
        "to": to,
        "includeResources": input.include_resources,
        "mode": mode,
        "maxPages": config.get("maxPages").and_then(Value::as_i64).unwrap_or(200_000),
    });
    state
        .task_engine
        .create_item(run.id, "page:1", first_page)
        .await?;
    Ok((StatusCode::CREATED, Json(json!({ "run": run }))))
}

fn validate_date_range(from: &str, to: &str) -> AppResult<()> {
    let valid = |value: &str| {
        value.len() == 10
            && value.as_bytes()[4] == b'-'
            && value.as_bytes()[7] == b'-'
            && value
                .bytes()
                .enumerate()
                .all(|(index, byte)| byte.is_ascii_digit() || index == 4 || index == 7)
    };
    if !valid(from) || !valid(to) {
        return Err(AppError::BadRequest("日期格式应为 YYYY-MM-DD".into()));
    }
    if from > to {
        return Err(AppError::BadRequest("开始日期不能晚于结束日期".into()));
    }
    Ok(())
}

async fn run_detail(State(state): State<AppState>, Path(id): Path<i64>) -> AppResult<Json<Value>> {
    let run = state.task_engine.run_by_id(id).await?;
    let stats = state.task_engine.run_stats(id).await?;
    Ok(Json(json!({ "run": run, "stats": stats })))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ItemListQuery {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    page: u32,
    #[serde(default)]
    page_size: u32,
}

async fn run_items(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(query): Query<ItemListQuery>,
) -> AppResult<Json<Paged<super::model::JobItem>>> {
    let status = query
        .status
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(JobItemStatus::parse);
    let params = PageParams {
        page: query.page,
        page_size: query.page_size,
    };
    let (items, total) = state
        .task_engine
        .list_items(id, status, params.page() as i64, params.page_size() as i64)
        .await?;
    Ok(Json(Paged::new(items, total, params)))
}

async fn run_events(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(params): Query<PageParams>,
) -> AppResult<Json<Paged<super::model::JobEvent>>> {
    let (items, total) = state
        .task_engine
        .list_events(id, params.page() as i64, params.page_size() as i64)
        .await?;
    Ok(Json(Paged::new(items, total, params)))
}

async fn pause_run(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<Json<super::model::JobRun>> {
    let run = state.task_engine.run_by_id(id).await?;
    if matches!(
        run.status,
        JobStatus::Success | JobStatus::Failed | JobStatus::Cancelled
    ) {
        return Err(AppError::BadRequest("该任务已结束，不能暂停".into()));
    }
    let stats = state.task_engine.run_stats(id).await?;
    let to = if stats.running == 0 {
        JobStatus::Paused
    } else {
        JobStatus::Pausing
    };
    Ok(Json(state.task_engine.transition(id, to, None).await?))
}

async fn resume_run(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<Json<super::model::JobRun>> {
    let run = state.task_engine.run_by_id(id).await?;
    if run.status != JobStatus::Paused {
        return Err(AppError::BadRequest("只有已暂停的任务可以继续".into()));
    }
    Ok(Json(
        state
            .task_engine
            .transition(id, JobStatus::Pending, None)
            .await?,
    ))
}

async fn cancel_run(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<Json<super::model::JobRun>> {
    let run = state.task_engine.run_by_id(id).await?;
    if matches!(
        run.status,
        JobStatus::Success | JobStatus::Failed | JobStatus::Cancelled
    ) {
        return Err(AppError::BadRequest("该任务已结束，不能取消".into()));
    }
    let stats = state.task_engine.run_stats(id).await?;
    let to = if stats.running == 0 {
        JobStatus::Cancelled
    } else {
        JobStatus::Cancelling
    };
    Ok(Json(state.task_engine.transition(id, to, None).await?))
}

async fn retry_failed(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> AppResult<Json<super::model::JobRun>> {
    let stats = state.task_engine.run_stats(id).await?;
    if stats.failed == 0 {
        return Err(AppError::BadRequest("该任务没有失败项".into()));
    }
    let requeued = state.task_engine.requeue_failed_items(id).await?;
    if requeued == 0 {
        return Err(AppError::BadRequest("该任务没有失败项".into()));
    }
    Ok(Json(
        state
            .task_engine
            .transition(id, JobStatus::Pending, None)
            .await?,
    ))
}
