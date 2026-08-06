use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};

use crate::{
    error::{AppError, AppResult},
    models::{Folder, MediaItem, Settings, Task},
};

pub async fn connect(database_url: &str) -> anyhow::Result<SqlitePool> {
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect(database_url)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    Ok(pool)
}

pub async fn folder_by_id(pool: &SqlitePool, id: i64) -> AppResult<Folder> {
    let row = sqlx::query("SELECT * FROM media_config WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(folder_from_row(&row))
}

pub async fn task_by_id(pool: &SqlitePool, id: i64) -> AppResult<Task> {
    let row = sqlx::query("SELECT * FROM scrape_task WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(task_from_row(&row))
}

pub async fn media_by_id(pool: &SqlitePool, id: i64) -> AppResult<MediaItem> {
    let row = sqlx::query("SELECT * FROM media_item WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(media_from_row(&row))
}

pub async fn create_task(
    pool: &SqlitePool,
    media_id: Option<i64>,
    folder_id: Option<i64>,
    task_type: &str,
) -> AppResult<Task> {
    let id =
        sqlx::query("INSERT INTO scrape_task (media_id, folder_id, task_type) VALUES (?, ?, ?)")
            .bind(media_id)
            .bind(folder_id)
            .bind(task_type)
            .execute(pool)
            .await?
            .last_insert_rowid();
    task_by_id(pool, id).await
}

pub async fn log(pool: &SqlitePool, level: &str, module: &str, message: &str) {
    if let Err(error) =
        sqlx::query("INSERT INTO system_log (level, module, message) VALUES (?, ?, ?)")
            .bind(level)
            .bind(module)
            .bind(message)
            .execute(pool)
            .await
    {
        tracing::warn!(%error, "failed to persist log entry");
    }
}

pub async fn load_settings(pool: &SqlitePool) -> AppResult<Settings> {
    let rows = sqlx::query("SELECT key, value FROM app_setting")
        .fetch_all(pool)
        .await?;
    let mut settings = Settings {
        metatube_url: "http://metatube:8080".into(),
        metatube_token: String::new(),
        output_format: "nfo".into(),
        scan_interval: 60,
        overwrite_policy: "missing".into(),
        log_level: "info".into(),
    };
    for row in rows {
        let key: String = row.get("key");
        let value: String = row.get("value");
        match key.as_str() {
            "metatube_url" => settings.metatube_url = value,
            "metatube_token" => settings.metatube_token = value,
            "output_format" => settings.output_format = value,
            "scan_interval" => settings.scan_interval = value.parse().unwrap_or(60),
            "overwrite_policy" => settings.overwrite_policy = value,
            "log_level" => settings.log_level = value,
            _ => {}
        }
    }
    Ok(settings)
}

pub fn folder_from_row(row: &sqlx::sqlite::SqliteRow) -> Folder {
    Folder {
        id: row.get("id"),
        name: row.get("name"),
        path: row.get("path"),
        media_type: row.get("media_type"),
        output_format: row.get("output_format"),
        scan_mode: row.get("scan_mode"),
        enabled: row.get::<i64, _>("enabled") != 0,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

pub fn task_from_row(row: &sqlx::sqlite::SqliteRow) -> Task {
    Task {
        id: row.get("id"),
        media_id: row.get("media_id"),
        folder_id: row.get("folder_id"),
        task_type: row.get("task_type"),
        status: row.get("status"),
        progress: row.get("progress"),
        error_message: row.get("error_message"),
        created_at: row.get("created_at"),
        finished_at: row.get("finished_at"),
    }
}

pub fn media_from_row(row: &sqlx::sqlite::SqliteRow) -> MediaItem {
    MediaItem {
        id: row.get("id"),
        folder_id: row.get("folder_id"),
        path: row.get("path"),
        filename: row.get("filename"),
        hash: row.get("hash"),
        title: row.get("title"),
        media_type: row.get("media_type"),
        provider_id: row.get("provider_id"),
        status: row.get("status"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}
