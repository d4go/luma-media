use std::collections::HashSet;

use anyhow::{Context, bail};
use sqlx::{Row, SqlitePool, migrate::MigrateError, sqlite::SqlitePoolOptions};

use crate::{
    error::{AppError, AppResult},
    models::{Folder, MediaItem, Settings, Task},
};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub async fn connect(database_url: &str) -> anyhow::Result<SqlitePool> {
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect(database_url)
        .await?;
    run_migrations_with_legacy_repair(&pool).await?;
    Ok(pool)
}

async fn run_migrations_with_legacy_repair(pool: &SqlitePool) -> anyhow::Result<()> {
    match MIGRATOR.run(pool).await {
        Ok(()) => Ok(()),
        Err(MigrateError::VersionMismatch(version)) if matches!(version, 1 | 2) => {
            validate_legacy_schema(pool, version).await?;
            let migration = MIGRATOR
                .iter()
                .find(|migration| migration.version == version)
                .context("legacy migration is not embedded in the application")?;
            let result = sqlx::query(
                "UPDATE _sqlx_migrations SET checksum = ? WHERE version = ? AND success = 1",
            )
            .bind(migration.checksum.as_ref())
            .bind(version)
            .execute(pool)
            .await?;
            if result.rows_affected() != 1 {
                bail!("could not repair legacy migration {version} checksum");
            }
            tracing::warn!(
                version,
                "repaired a legacy migration checksum after validating the database schema"
            );
            MIGRATOR.run(pool).await?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

async fn validate_legacy_schema(pool: &SqlitePool, version: i64) -> anyhow::Result<()> {
    let required_tables = if version == 1 {
        vec![
            (
                "media_config",
                &[
                    "id",
                    "name",
                    "path",
                    "media_type",
                    "output_format",
                    "scan_mode",
                    "enabled",
                    "created_at",
                    "updated_at",
                ] as &[&str],
            ),
            (
                "media_item",
                &[
                    "id",
                    "folder_id",
                    "path",
                    "filename",
                    "hash",
                    "title",
                    "media_type",
                    "provider_id",
                    "status",
                    "created_at",
                    "updated_at",
                ],
            ),
            (
                "scrape_task",
                &[
                    "id",
                    "media_id",
                    "folder_id",
                    "task_type",
                    "status",
                    "progress",
                    "error_message",
                    "created_at",
                    "finished_at",
                ],
            ),
            (
                "metadata_record",
                &["id", "media_id", "provider", "raw_json", "created_at"],
            ),
            (
                "system_log",
                &["id", "level", "module", "message", "created_at"],
            ),
            ("app_setting", &["key", "value", "updated_at"]),
        ]
    } else {
        vec![("app_setting", &["key", "value", "updated_at"] as &[&str])]
    };

    for (table, expected_columns) in required_tables {
        let statement = format!("PRAGMA table_info({table})");
        let rows = sqlx::query(&statement).fetch_all(pool).await?;
        if rows.is_empty() {
            bail!("cannot repair migration {version}: required table {table} is missing");
        }
        let columns = rows
            .iter()
            .map(|row| row.get::<String, _>("name"))
            .collect::<HashSet<_>>();
        if let Some(missing) = expected_columns
            .iter()
            .find(|column| !columns.contains(**column))
        {
            bail!(
                "cannot repair migration {version}: required column {table}.{missing} is missing"
            );
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn repairs_valid_legacy_migration_checksum() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        MIGRATOR.run(&pool).await.unwrap();
        sqlx::query("UPDATE _sqlx_migrations SET checksum = X'00' WHERE version = 1")
            .execute(&pool)
            .await
            .unwrap();

        run_migrations_with_legacy_repair(&pool).await.unwrap();

        let actual: Vec<u8> =
            sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        let expected = MIGRATOR
            .iter()
            .find(|migration| migration.version == 1)
            .unwrap();
        assert_eq!(actual, expected.checksum.as_ref());
    }

    #[tokio::test]
    async fn refuses_checksum_repair_when_legacy_schema_is_incomplete() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        MIGRATOR.run(&pool).await.unwrap();
        sqlx::query("UPDATE _sqlx_migrations SET checksum = X'00' WHERE version = 1")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE app_setting")
            .execute(&pool)
            .await
            .unwrap();

        let error = run_migrations_with_legacy_repair(&pool)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("required table app_setting is missing"));
    }
}
