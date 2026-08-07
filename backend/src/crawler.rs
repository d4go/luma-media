use std::{path::PathBuf, process::Stdio};

use anyhow::{Context, bail};
use serde_json::Value;
use sqlx::{Row, SqlitePool};
use tokio::{process::Command, time::timeout};

use crate::{
    AppState,
    error::{AppError, AppResult},
    models::{CrawlerResult, CrawlerRun, CrawlerScript},
    qbittorrent::QBittorrentClient,
    storage,
};

const MAX_CAPTURE_BYTES: usize = 1024 * 1024;

pub const SCRIPT_SELECT: &str = "SELECT cs.*, \
    (SELECT status FROM crawler_run cr WHERE cr.script_id = cs.id ORDER BY cr.id DESC LIMIT 1) AS last_run_status, \
    COALESCE((SELECT result_count FROM crawler_run cr WHERE cr.script_id = cs.id ORDER BY cr.id DESC LIMIT 1), 0) AS last_result_count \
    FROM crawler_script cs";

pub async fn list_scripts(pool: &SqlitePool) -> AppResult<Vec<CrawlerScript>> {
    let rows = sqlx::query(&format!("{SCRIPT_SELECT} ORDER BY cs.name, cs.id"))
        .fetch_all(pool)
        .await?;
    Ok(rows.iter().map(script_from_row).collect())
}

pub async fn script_by_id(pool: &SqlitePool, id: i64) -> AppResult<CrawlerScript> {
    let row = sqlx::query(&format!("{SCRIPT_SELECT} WHERE cs.id = ?"))
        .bind(id)
        .fetch_optional(pool)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(script_from_row(&row))
}

async fn script_file(pool: &SqlitePool, id: i64) -> AppResult<(CrawlerScript, PathBuf)> {
    let row = sqlx::query(&format!("{SCRIPT_SELECT} WHERE cs.id = ?"))
        .bind(id)
        .fetch_optional(pool)
        .await?
        .ok_or(AppError::NotFound)?;
    let path: String = row.get("file_path");
    Ok((script_from_row(&row), PathBuf::from(path)))
}

pub async fn list_runs(pool: &SqlitePool, script_id: Option<i64>) -> AppResult<Vec<CrawlerRun>> {
    let rows = if let Some(script_id) = script_id {
        sqlx::query("SELECT * FROM crawler_run WHERE script_id = ? ORDER BY id DESC LIMIT 100")
            .bind(script_id)
            .fetch_all(pool)
            .await?
    } else {
        sqlx::query("SELECT * FROM crawler_run ORDER BY id DESC LIMIT 100")
            .fetch_all(pool)
            .await?
    };
    Ok(rows.iter().map(run_from_row).collect())
}

pub async fn list_results(
    pool: &SqlitePool,
    script_id: Option<i64>,
) -> AppResult<Vec<CrawlerResult>> {
    let rows = if let Some(script_id) = script_id {
        sqlx::query(
            "SELECT cr.*, cs.name AS source, cs.website_url AS source_url \
             FROM crawler_result cr JOIN crawler_script cs ON cs.id = cr.script_id \
             WHERE cr.script_id = ? ORDER BY cr.id DESC LIMIT 500",
        )
        .bind(script_id)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            "SELECT cr.*, cs.name AS source, cs.website_url AS source_url \
             FROM crawler_result cr JOIN crawler_script cs ON cs.id = cr.script_id \
             ORDER BY cr.id DESC LIMIT 500",
        )
        .fetch_all(pool)
        .await?
    };
    Ok(rows.iter().map(result_from_row).collect())
}

pub async fn result_by_id(pool: &SqlitePool, id: i64) -> AppResult<CrawlerResult> {
    let row = sqlx::query(
        "SELECT cr.*, cs.name AS source, cs.website_url AS source_url \
         FROM crawler_result cr JOIN crawler_script cs ON cs.id = cr.script_id WHERE cr.id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?
    .ok_or(AppError::NotFound)?;
    Ok(result_from_row(&row))
}

pub async fn queue_run(state: &AppState, script_id: i64) -> AppResult<CrawlerRun> {
    let (script, path) = script_file(&state.pool, script_id).await?;
    if !path.is_file() {
        return Err(AppError::BadRequest(
            "uploaded Python script is missing".into(),
        ));
    }
    let mut transaction = state.pool.begin().await?;
    let active: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM crawler_run WHERE script_id = ? AND status IN ('pending', 'running'))",
    )
    .bind(script_id)
    .fetch_one(&mut *transaction)
    .await?;
    if active != 0 {
        return Err(AppError::BadRequest(
            "this crawler is already running".into(),
        ));
    }
    let id = match sqlx::query("INSERT INTO crawler_run (script_id) VALUES (?)")
        .bind(script_id)
        .execute(&mut *transaction)
        .await
    {
        Ok(result) => result.last_insert_rowid(),
        Err(sqlx::Error::Database(error)) if error.is_unique_violation() => {
            return Err(AppError::BadRequest(
                "this crawler is already running".into(),
            ));
        }
        Err(error) => return Err(error.into()),
    };
    sqlx::query(
        "UPDATE crawler_script SET next_run_at = datetime('now', '+' || interval_minutes || ' minutes'), updated_at = datetime('now') WHERE id = ?",
    )
    .bind(script_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    let run = run_by_id(&state.pool, id).await?;
    tokio::spawn(run_script(state.clone(), script, path, id));
    Ok(run)
}

pub async fn schedule_due(state: &AppState) -> anyhow::Result<()> {
    let ids = sqlx::query_scalar::<_, i64>(
        "SELECT cs.id FROM crawler_script cs \
         WHERE cs.enabled = 1 AND COALESCE(cs.next_run_at, datetime('now')) <= datetime('now') \
         AND NOT EXISTS (SELECT 1 FROM crawler_run cr WHERE cr.script_id = cs.id AND cr.status IN ('pending', 'running')) \
         ORDER BY cs.id",
    )
    .fetch_all(&state.pool)
    .await?;
    for id in ids {
        if let Err(error) = queue_run(state, id).await {
            tracing::warn!(%error, script_id = id, "could not queue scheduled crawler");
        }
    }
    Ok(())
}

async fn run_script(state: AppState, script: CrawlerScript, path: PathBuf, run_id: i64) {
    let _permit = match state.crawler_limiter.acquire().await {
        Ok(permit) => permit,
        Err(_) => return,
    };
    let started = sqlx::query(
        "UPDATE crawler_run SET status = 'running', started_at = datetime('now') WHERE id = ? AND status = 'pending'",
    )
    .bind(run_id)
    .execute(&state.pool)
    .await;
    if !started.is_ok_and(|result| result.rows_affected() == 1) {
        return;
    }
    let _ = sqlx::query(
        "UPDATE crawler_script SET last_started_at = datetime('now'), updated_at = datetime('now') WHERE id = ?",
    )
    .bind(script.id)
    .execute(&state.pool)
    .await;

    let result_path = state
        .script_root
        .join("runs")
        .join(format!("{run_id}.json"));
    let python = std::env::var("LUMA_PYTHON_BIN").unwrap_or_else(|_| {
        if cfg!(windows) {
            "python".into()
        } else {
            "python3".into()
        }
    });
    let mut command = Command::new(python);
    command
        .arg(&path)
        .arg(&script.website_url)
        .current_dir(path.parent().unwrap_or(&state.script_root))
        .env("LUMA_TARGET_WEBSITE", &script.website_url)
        .env("LUMA_RESULT_PATH", &result_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let output = timeout(std::time::Duration::from_secs(900), command.output()).await;
    let (stdout, stderr, execution_error) = match output {
        Ok(Ok(output)) => {
            let stdout = capture(&output.stdout);
            let stderr = capture(&output.stderr);
            let error = (!output.status.success()).then(|| {
                format!(
                    "Python exited with {}",
                    output
                        .status
                        .code()
                        .map_or_else(|| "a signal".into(), |code| format!("code {code}"))
                )
            });
            (stdout, stderr, error)
        }
        Ok(Err(error)) => (
            String::new(),
            String::new(),
            Some(format!("could not start Python: {error}")),
        ),
        Err(_) => (
            String::new(),
            String::new(),
            Some("Python execution timed out after 15 minutes".into()),
        ),
    };

    if let Some(message) = execution_error {
        let _ = tokio::fs::remove_file(&result_path).await;
        finish_failed(&state, script.id, run_id, &stdout, &stderr, &message).await;
        return;
    }

    let result_text = match tokio::fs::read_to_string(&result_path).await {
        Ok(text) => text,
        Err(_) => stdout.clone(),
    };
    let _ = tokio::fs::remove_file(&result_path).await;
    let parsed = match parse_results(&result_text) {
        Ok(results) => results,
        Err(error) => {
            finish_failed(
                &state,
                script.id,
                run_id,
                &stdout,
                &stderr,
                &error.to_string(),
            )
            .await;
            return;
        }
    };

    let mut result_ids = Vec::with_capacity(parsed.len());
    let mut transaction = match state.pool.begin().await {
        Ok(transaction) => transaction,
        Err(error) => {
            finish_failed(
                &state,
                script.id,
                run_id,
                &stdout,
                &stderr,
                &error.to_string(),
            )
            .await;
            return;
        }
    };
    for item in parsed {
        match sqlx::query(
            "INSERT INTO crawler_result (run_id, script_id, title, download_url, trackers_json, raw_json) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(run_id)
        .bind(script.id)
        .bind(&item.title)
        .bind(&item.download_url)
        .bind(serde_json::to_string(&item.trackers).unwrap_or_else(|_| "[]".into()))
        .bind(item.raw.to_string())
        .execute(&mut *transaction)
        .await
        {
            Ok(result) => result_ids.push(result.last_insert_rowid()),
            Err(error) => {
                let _ = transaction.rollback().await;
                finish_failed(&state, script.id, run_id, &stdout, &stderr, &error.to_string()).await;
                return;
            }
        }
    }
    if let Err(error) = sqlx::query(
        "UPDATE crawler_run SET status = 'success', stdout = ?, stderr = ?, result_count = ?, finished_at = datetime('now') WHERE id = ?",
    )
    .bind(&stdout)
    .bind(&stderr)
    .bind(result_ids.len() as i64)
    .bind(run_id)
    .execute(&mut *transaction)
    .await
    {
        tracing::error!(%error, run_id, "failed to store crawler results");
        return;
    }
    if let Err(error) = transaction.commit().await {
        tracing::error!(%error, run_id, "failed to commit crawler results");
        return;
    }
    let _ = sqlx::query(
        "UPDATE crawler_script SET last_finished_at = datetime('now'), updated_at = datetime('now') WHERE id = ?",
    )
    .bind(script.id)
    .execute(&state.pool)
    .await;
    storage::log(
        &state.pool,
        "info",
        "crawler",
        &format!(
            "Crawler {} produced {} results",
            script.name,
            result_ids.len()
        ),
    )
    .await;

    if script.auto_download {
        for result_id in result_ids {
            let _ = download_result(&state, result_id).await;
        }
    }
}

async fn finish_failed(
    state: &AppState,
    script_id: i64,
    run_id: i64,
    stdout: &str,
    stderr: &str,
    message: &str,
) {
    let _ = sqlx::query(
        "UPDATE crawler_run SET status = 'failed', stdout = ?, stderr = ?, error_message = ?, finished_at = datetime('now') WHERE id = ?",
    )
    .bind(stdout)
    .bind(stderr)
    .bind(message)
    .bind(run_id)
    .execute(&state.pool)
    .await;
    let _ = sqlx::query(
        "UPDATE crawler_script SET last_finished_at = datetime('now'), updated_at = datetime('now') WHERE id = ?",
    )
    .bind(script_id)
    .execute(&state.pool)
    .await;
    storage::log(&state.pool, "error", "crawler", message).await;
}

pub async fn download_result(state: &AppState, result_id: i64) -> AppResult<CrawlerResult> {
    let result = result_by_id(&state.pool, result_id).await?;
    if result.download_status == "downloading" || result.download_status == "success" {
        return Err(AppError::BadRequest(
            "result was already sent to qBittorrent".into(),
        ));
    }
    let updated = sqlx::query(
        "UPDATE crawler_result SET download_status = 'downloading', error_message = NULL WHERE id = ? AND download_status IN ('pending', 'failed')",
    )
    .bind(result_id)
    .execute(&state.pool)
    .await?;
    if updated.rows_affected() == 0 {
        return Err(AppError::BadRequest(
            "result is not available for download".into(),
        ));
    }

    let outcome: anyhow::Result<Option<String>> = async {
        let settings = storage::load_settings(&state.pool).await?;
        let client = QBittorrentClient::new(&settings)?;
        client
            .add_download(&result.download_url, &result.trackers)
            .await
    }
    .await;
    match outcome {
        Ok(hash) => {
            sqlx::query(
                "UPDATE crawler_result SET download_status = 'success', qbit_hash = ?, downloaded_at = datetime('now') WHERE id = ?",
            )
            .bind(hash)
            .bind(result_id)
            .execute(&state.pool)
            .await?;
        }
        Err(error) => {
            sqlx::query(
                "UPDATE crawler_result SET download_status = 'failed', error_message = ? WHERE id = ?",
            )
            .bind(error.to_string())
            .bind(result_id)
            .execute(&state.pool)
            .await?;
            return Err(AppError::BadRequest(error.to_string()));
        }
    }
    result_by_id(&state.pool, result_id).await
}

pub async fn ignore_result(pool: &SqlitePool, result_id: i64) -> AppResult<CrawlerResult> {
    result_by_id(pool, result_id).await?;
    let updated = sqlx::query(
        "UPDATE crawler_result SET download_status = 'ignored', error_message = NULL \
         WHERE id = ? AND download_status IN ('pending', 'failed')",
    )
    .bind(result_id)
    .execute(pool)
    .await?;
    if updated.rows_affected() == 0 {
        return Err(AppError::BadRequest(
            "only pending or failed results can be ignored".into(),
        ));
    }
    result_by_id(pool, result_id).await
}

async fn run_by_id(pool: &SqlitePool, id: i64) -> AppResult<CrawlerRun> {
    let row = sqlx::query("SELECT * FROM crawler_run WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(run_from_row(&row))
}

fn capture(bytes: &[u8]) -> String {
    let bytes = &bytes[..bytes.len().min(MAX_CAPTURE_BYTES)];
    String::from_utf8_lossy(bytes).into_owned()
}

struct ParsedResult {
    title: String,
    download_url: String,
    trackers: Vec<String>,
    raw: Value,
}

fn parse_results(text: &str) -> anyhow::Result<Vec<ParsedResult>> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let value: Value = serde_json::from_str(trimmed).or_else(|_| {
        trimmed
            .lines()
            .rev()
            .find(|line| !line.trim().is_empty())
            .context("crawler did not output JSON")
            .and_then(|line| {
                serde_json::from_str(line).context("crawler did not output valid JSON")
            })
    })?;
    let values = match &value {
        Value::Array(values) => values.clone(),
        Value::Object(map) if map.get("results").is_some_and(Value::is_array) => {
            map["results"].as_array().cloned().unwrap_or_default()
        }
        Value::Object(_) => vec![value],
        _ => bail!("crawler JSON must be an array or object"),
    };
    values
        .into_iter()
        .enumerate()
        .map(|(index, raw)| {
            let object = raw
                .as_object()
                .context("each crawler result must be an object")?;
            let download_url = ["downloadUrl", "magnet", "torrentUrl", "url"]
                .into_iter()
                .find_map(|key| object.get(key).and_then(Value::as_str))
                .map(str::trim)
                .filter(|url| !url.is_empty())
                .context("crawler result is missing downloadUrl, magnet, torrentUrl, or url")?
                .to_owned();
            let title = ["title", "name"]
                .into_iter()
                .find_map(|key| object.get(key).and_then(Value::as_str))
                .map(str::trim)
                .filter(|title| !title.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| format!("Result {}", index + 1));
            let trackers = match object.get("trackers") {
                Some(Value::Array(values)) => values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .collect(),
                Some(Value::String(value)) => value
                    .split(['\n', ','])
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .collect(),
                _ => Vec::new(),
            };
            Ok(ParsedResult {
                title,
                download_url,
                trackers,
                raw,
            })
        })
        .collect()
}

fn script_from_row(row: &sqlx::sqlite::SqliteRow) -> CrawlerScript {
    CrawlerScript {
        id: row.get("id"),
        name: row.get("name"),
        website_url: row.get("website_url"),
        file_name: row.get("file_name"),
        interval_minutes: row.get::<i64, _>("interval_minutes") as u64,
        enabled: row.get::<i64, _>("enabled") != 0,
        auto_download: row.get::<i64, _>("auto_download") != 0,
        last_started_at: row.get("last_started_at"),
        last_finished_at: row.get("last_finished_at"),
        next_run_at: row.get("next_run_at"),
        last_run_status: row.get("last_run_status"),
        last_result_count: row.get("last_result_count"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn run_from_row(row: &sqlx::sqlite::SqliteRow) -> CrawlerRun {
    CrawlerRun {
        id: row.get("id"),
        script_id: row.get("script_id"),
        status: row.get("status"),
        stdout: row.get("stdout"),
        stderr: row.get("stderr"),
        result_count: row.get("result_count"),
        error_message: row.get("error_message"),
        created_at: row.get("created_at"),
        started_at: row.get("started_at"),
        finished_at: row.get("finished_at"),
    }
}

fn result_from_row(row: &sqlx::sqlite::SqliteRow) -> CrawlerResult {
    let trackers_json: String = row.get("trackers_json");
    let raw_json: String = row.get("raw_json");
    let raw = serde_json::from_str(&raw_json).unwrap_or(Value::Null);
    let created_at: String = row.get("created_at");
    let size = ["size", "fileSize"]
        .into_iter()
        .find_map(|key| raw.get(key))
        .and_then(|value| match value {
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            _ => None,
        });
    let published_at = ["publishedAt", "publishDate", "date", "createdAt"]
        .into_iter()
        .find_map(|key| raw.get(key).and_then(Value::as_str))
        .unwrap_or(&created_at)
        .to_owned();
    CrawlerResult {
        id: row.get("id"),
        run_id: row.get("run_id"),
        script_id: row.get("script_id"),
        title: row.get("title"),
        source: row.try_get("source").unwrap_or_else(|_| "Crawler".into()),
        source_url: row.try_get("source_url").unwrap_or_default(),
        size,
        published_at,
        download_url: row.get("download_url"),
        trackers: serde_json::from_str(&trackers_json).unwrap_or_default(),
        raw,
        download_status: row.get("download_status"),
        qbit_hash: row.get("qbit_hash"),
        error_message: row.get("error_message"),
        created_at,
        downloaded_at: row.get("downloaded_at"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_result_envelope_and_aliases() {
        let results = parse_results(
            r#"{"results":[{"name":"One","magnet":"magnet:?xt=urn:btih:A","trackers":"udp://a\nhttps://b"}]}"#,
        )
        .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "One");
        assert_eq!(results[0].trackers.len(), 2);
    }

    #[test]
    fn accepts_json_on_last_stdout_line() {
        let results = parse_results("log line\n[{\"url\":\"https://example/t.torrent\"}]").unwrap();
        assert_eq!(results[0].title, "Result 1");
    }
}
