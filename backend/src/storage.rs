use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use anyhow::{Context, bail};
use sqlx::{Row, SqlitePool, migrate::MigrateError, sqlite::SqlitePoolOptions};

use crate::{
    error::{AppError, AppResult},
    models::{
        Folder, MediaItem, MediaResourceState, MediaResources, Settings, Task, TaskDetail,
        TaskFolder, TaskMedia, TaskRecord,
    },
};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub const TASK_SELECT: &str = "SELECT st.*, \
    mi.title AS media_title, mi.filename AS media_filename, mi.path AS media_path, mi.status AS media_status, \
    mc.name AS folder_name, mc.path AS folder_path, \
    (SELECT COUNT(*) FROM task_record tr WHERE tr.task_id = st.id) AS record_count \
    FROM scrape_task st \
    LEFT JOIN media_item mi ON mi.id = st.media_id \
    LEFT JOIN media_config mc ON mc.id = st.folder_id";

pub const MEDIA_SELECT: &str = "SELECT mi.*, st.id AS scrape_task_id, \
    st.status AS scrape_task_status, \
    COALESCE((SELECT COUNT(*) FROM task_record tr WHERE tr.task_id = st.id), 0) AS scrape_record_count, \
    ma.status AS poster_asset_status, ma.source AS poster_asset_source, \
    ma.local_path AS poster_asset_path, ma.checked_at AS poster_asset_checked_at \
    FROM media_item mi \
    LEFT JOIN scrape_task st ON st.media_id = mi.id AND st.task_type = 'scrape' \
    LEFT JOIN media_asset ma ON ma.id = ( \
        SELECT latest.id FROM media_asset latest \
        WHERE latest.media_id = mi.id AND latest.asset_type = 'poster' \
        ORDER BY latest.updated_at DESC, latest.id DESC LIMIT 1 \
    )";

#[derive(Debug)]
pub struct TaskRun {
    pub task: Task,
    pub record_id: i64,
}

pub async fn connect(database_url: &str) -> anyhow::Result<SqlitePool> {
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect(database_url)
        .await?;
    run_migrations_with_legacy_repair(&pool).await?;
    recover_interrupted_crawler_work(&pool).await?;
    Ok(pool)
}

async fn recover_interrupted_crawler_work(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE crawler_run SET status = 'failed', \
         error_message = 'execution interrupted by service restart', finished_at = datetime('now') \
         WHERE status IN ('pending', 'running')",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE crawler_result SET download_status = 'failed', \
         error_message = 'qBittorrent submission interrupted by service restart' \
         WHERE download_status = 'downloading'",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn run_migrations_with_legacy_repair(pool: &SqlitePool) -> anyhow::Result<()> {
    match MIGRATOR.run(pool).await {
        Ok(()) => Ok(()),
        Err(MigrateError::VersionMismatch(version)) if matches!(version, 1..=3) => {
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
    } else if version == 2 {
        vec![("app_setting", &["key", "value", "updated_at"] as &[&str])]
    } else {
        vec![
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
                    "updated_at",
                    "finished_at",
                ] as &[&str],
            ),
            (
                "task_record",
                &[
                    "id",
                    "task_id",
                    "status",
                    "progress",
                    "error_message",
                    "created_at",
                    "finished_at",
                ],
            ),
        ]
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
    let row = sqlx::query(&format!("{TASK_SELECT} WHERE st.id = ?"))
        .bind(id)
        .fetch_optional(pool)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(task_from_row(&row))
}

pub async fn task_detail_by_id(pool: &SqlitePool, id: i64) -> AppResult<TaskDetail> {
    let task = task_by_id(pool, id).await?;
    let rows = sqlx::query(
        "SELECT * FROM task_record WHERE task_id = ? ORDER BY created_at DESC, id DESC",
    )
    .bind(id)
    .fetch_all(pool)
    .await?;
    Ok(TaskDetail {
        task,
        records: rows.iter().map(task_record_from_row).collect(),
    })
}

pub async fn media_by_id(pool: &SqlitePool, id: i64) -> AppResult<MediaItem> {
    let row = sqlx::query(&format!("{} WHERE mi.id = ?", MEDIA_SELECT))
        .bind(id)
        .fetch_optional(pool)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(media_from_row(&row))
}

pub async fn create_task_run(
    pool: &SqlitePool,
    media_id: Option<i64>,
    folder_id: Option<i64>,
    task_type: &str,
) -> AppResult<TaskRun> {
    let mut transaction = pool.begin().await?;
    sqlx::query(
        "INSERT OR IGNORE INTO scrape_task \
         (media_id, folder_id, task_type, status, progress, created_at, updated_at) \
         VALUES (?, ?, ?, 'pending', 0, datetime('now'), datetime('now'))",
    )
    .bind(media_id)
    .bind(folder_id)
    .bind(task_type)
    .execute(&mut *transaction)
    .await?;

    let task_id: i64 = match task_type {
        "scrape" => {
            let id = sqlx::query_scalar(
                "SELECT id FROM scrape_task WHERE task_type = 'scrape' AND media_id IS ? \
                 ORDER BY id DESC LIMIT 1",
            )
            .bind(media_id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(AppError::NotFound)?;
            sqlx::query("UPDATE scrape_task SET folder_id = ? WHERE id = ?")
                .bind(folder_id)
                .bind(id)
                .execute(&mut *transaction)
                .await?;
            id
        }
        "scan" => sqlx::query_scalar(
            "SELECT id FROM scrape_task WHERE task_type = 'scan' AND folder_id IS ? \
             ORDER BY id DESC LIMIT 1",
        )
        .bind(folder_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(AppError::NotFound)?,
        _ => sqlx::query_scalar(
            "SELECT id FROM scrape_task WHERE task_type = ? AND media_id IS ? AND folder_id IS ? \
             ORDER BY id DESC LIMIT 1",
        )
        .bind(task_type)
        .bind(media_id)
        .bind(folder_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(AppError::NotFound)?,
    };

    let active: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM task_record \
         WHERE task_id = ? AND status IN ('pending', 'running'))",
    )
    .bind(task_id)
    .fetch_one(&mut *transaction)
    .await?;
    if active != 0 {
        return Err(AppError::BadRequest(
            "this task already has an active execution".into(),
        ));
    }

    let record_id = sqlx::query("INSERT INTO task_record (task_id) VALUES (?)")
        .bind(task_id)
        .execute(&mut *transaction)
        .await?
        .last_insert_rowid();
    sqlx::query(
        "UPDATE scrape_task SET status = 'pending', progress = 0, error_message = NULL, \
         finished_at = NULL, updated_at = datetime('now') WHERE id = ?",
    )
    .bind(task_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;

    Ok(TaskRun {
        task: task_by_id(pool, task_id).await?,
        record_id,
    })
}

pub async fn start_task_run(
    pool: &SqlitePool,
    task_id: i64,
    record_id: i64,
    progress: i64,
) -> AppResult<bool> {
    let mut transaction = pool.begin().await?;
    let result = sqlx::query(
        "UPDATE task_record SET status = 'running', progress = ? \
         WHERE id = ? AND task_id = ? AND status = 'pending'",
    )
    .bind(progress)
    .bind(record_id)
    .bind(task_id)
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() == 1 {
        sqlx::query(
            "UPDATE scrape_task SET status = 'running', progress = ?, error_message = NULL, \
             finished_at = NULL, updated_at = datetime('now') WHERE id = ?",
        )
        .bind(progress)
        .bind(task_id)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    Ok(result.rows_affected() == 1)
}

pub async fn update_task_progress(
    pool: &SqlitePool,
    task_id: i64,
    record_id: i64,
    progress: i64,
) -> AppResult<()> {
    let mut transaction = pool.begin().await?;
    let result = sqlx::query(
        "UPDATE task_record SET progress = ? \
         WHERE id = ? AND task_id = ? AND status = 'running'",
    )
    .bind(progress)
    .bind(record_id)
    .bind(task_id)
    .execute(&mut *transaction)
    .await?;
    if result.rows_affected() == 1 {
        sqlx::query(
            "UPDATE scrape_task SET progress = ?, updated_at = datetime('now') WHERE id = ?",
        )
        .bind(progress)
        .bind(task_id)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    Ok(())
}

pub async fn task_run_is_running(pool: &SqlitePool, task_id: i64, record_id: i64) -> bool {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM task_record WHERE id = ? AND task_id = ?")
            .bind(record_id)
            .bind(task_id)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
    status.as_deref() == Some("running")
}

pub async fn finish_task_run(
    pool: &SqlitePool,
    task_id: i64,
    record_id: i64,
    status: &str,
    error_message: Option<&str>,
) -> AppResult<bool> {
    let mut transaction = pool.begin().await?;
    let result = if status == "success" {
        sqlx::query(
            "UPDATE task_record SET status = ?, progress = 100, error_message = ?, \
             finished_at = datetime('now') \
             WHERE id = ? AND task_id = ? AND status = 'running'",
        )
        .bind(status)
        .bind(error_message)
        .bind(record_id)
        .bind(task_id)
        .execute(&mut *transaction)
        .await?
    } else {
        sqlx::query(
            "UPDATE task_record SET status = ?, error_message = ?, finished_at = datetime('now') \
             WHERE id = ? AND task_id = ? AND status IN ('pending', 'running')",
        )
        .bind(status)
        .bind(error_message)
        .bind(record_id)
        .bind(task_id)
        .execute(&mut *transaction)
        .await?
    };
    if result.rows_affected() == 1 {
        sqlx::query(
            "UPDATE scrape_task SET status = ?, \
             progress = CASE WHEN ? = 'success' THEN 100 ELSE \
                (SELECT progress FROM task_record WHERE id = ?) END, \
             error_message = ?, finished_at = datetime('now'), updated_at = datetime('now') \
             WHERE id = ?",
        )
        .bind(status)
        .bind(status)
        .bind(record_id)
        .bind(error_message)
        .bind(task_id)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    Ok(result.rows_affected() == 1)
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
        metatube_url: "auto".into(),
        metatube_token: String::new(),
        output_format: "nfo".into(),
        scan_interval: 60,
        overwrite_policy: "missing".into(),
        log_level: "info".into(),
        qbittorrent_url: "http://127.0.0.1:8080".into(),
        qbittorrent_username: "admin".into(),
        qbittorrent_password: String::new(),
        qbittorrent_auto_update_trackers: false,
        qbittorrent_tracker_source_url:
            "https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_best.txt".into(),
        qbittorrent_tracker_update_interval: 1440,
    };
    let mut inferred_metatube_url = None;
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
            "deployment_metatube_url" => inferred_metatube_url = Some(value),
            "qbittorrent_url" => settings.qbittorrent_url = value,
            "qbittorrent_username" => settings.qbittorrent_username = value,
            "qbittorrent_password" => settings.qbittorrent_password = value,
            "qbittorrent_auto_update_trackers" => {
                settings.qbittorrent_auto_update_trackers = value == "true"
            }
            "qbittorrent_tracker_source_url" => settings.qbittorrent_tracker_source_url = value,
            "qbittorrent_tracker_update_interval" => {
                settings.qbittorrent_tracker_update_interval = value.parse().unwrap_or(1440)
            }
            _ => {}
        }
    }
    if settings.metatube_url == "auto" {
        settings.metatube_url = std::env::var("LUMA_METATUBE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or(inferred_metatube_url)
            .unwrap_or_else(local_metatube_url);
    }
    Ok(settings)
}

fn local_metatube_url() -> String {
    let ip = std::net::UdpSocket::bind("0.0.0.0:0")
        .and_then(|socket| {
            socket.connect("8.8.8.8:80")?;
            socket.local_addr()
        })
        .map(|address| address.ip())
        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
    match ip {
        std::net::IpAddr::V4(ip) => format!("http://{ip}:8080"),
        std::net::IpAddr::V6(ip) => format!("http://[{ip}]:8080"),
    }
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
    let media_id = row.get("media_id");
    let folder_id = row.get("folder_id");
    let media_title: Option<String> = row.get("media_title");
    let folder_name: Option<String> = row.get("folder_name");
    Task {
        id: row.get("id"),
        media_id,
        folder_id,
        task_type: row.get("task_type"),
        status: row.get("status"),
        progress: row.get("progress"),
        error_message: row.get("error_message"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        finished_at: row.get("finished_at"),
        record_count: row.get("record_count"),
        media: media_id.zip(media_title).map(|(id, title)| TaskMedia {
            id,
            title,
            filename: row.get("media_filename"),
            path: row.get("media_path"),
            status: row.get("media_status"),
        }),
        folder: folder_id.zip(folder_name).map(|(id, name)| TaskFolder {
            id,
            name,
            path: row.get("folder_path"),
        }),
    }
}

pub fn task_record_from_row(row: &sqlx::sqlite::SqliteRow) -> TaskRecord {
    TaskRecord {
        id: row.get("id"),
        task_id: row.get("task_id"),
        status: row.get("status"),
        progress: row.get("progress"),
        error_message: row.get("error_message"),
        created_at: row.get("created_at"),
        finished_at: row.get("finished_at"),
    }
}

pub fn media_from_row(row: &sqlx::sqlite::SqliteRow) -> MediaItem {
    let path: String = row.get("path");
    let (nfo_path, sidecar_poster_path) = local_media_resources(&path);
    let asset_status: Option<String> = row.try_get("poster_asset_status").unwrap_or(None);
    let asset_source: Option<String> = row.try_get("poster_asset_source").unwrap_or(None);
    let asset_path: Option<String> = row.try_get("poster_asset_path").unwrap_or(None);
    let asset_checked_at: Option<String> = row.try_get("poster_asset_checked_at").unwrap_or(None);
    let cached_poster_exists = asset_path
        .as_deref()
        .is_some_and(|path| Path::new(path).is_file());
    let poster_status = if sidecar_poster_path.is_some()
        || (asset_status.as_deref() == Some("ACTIVE") && cached_poster_exists)
    {
        "ready"
    } else if asset_status.is_some() {
        "failed"
    } else {
        "missing"
    };
    let poster_source = if asset_status.as_deref() == Some("ACTIVE") && cached_poster_exists {
        asset_source
    } else if sidecar_poster_path.is_some() {
        Some("local".into())
    } else {
        asset_source
    };
    let poster_path = sidecar_poster_path
        .map(|path| path.to_string_lossy().into_owned())
        .or(asset_path);

    MediaItem {
        id: row.get("id"),
        folder_id: row.get("folder_id"),
        path,
        filename: row.get("filename"),
        hash: row.get("hash"),
        title: row.get("title"),
        media_type: row.get("media_type"),
        provider_id: row.get("provider_id"),
        status: row.get("status"),
        scrape_task_id: row.try_get("scrape_task_id").unwrap_or(None),
        scrape_task_status: row.try_get("scrape_task_status").unwrap_or(None),
        scrape_record_count: row.try_get("scrape_record_count").unwrap_or(0),
        resources: MediaResources {
            nfo: MediaResourceState {
                status: if nfo_path.is_some() {
                    "ready"
                } else {
                    "missing"
                }
                .into(),
                source: nfo_path.as_ref().map(|_| "local".into()),
                path: nfo_path.map(|path| path.to_string_lossy().into_owned()),
                checked_at: None,
            },
            poster: MediaResourceState {
                status: poster_status.into(),
                source: poster_source,
                path: poster_path,
                checked_at: asset_checked_at,
            },
        },
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

pub fn local_media_resources(media_path: &str) -> (Option<PathBuf>, Option<PathBuf>) {
    let media_path = Path::new(media_path);
    let Some(parent) = media_path.parent() else {
        return (None, None);
    };
    let Some(stem) = media_path.file_stem().and_then(|stem| stem.to_str()) else {
        return (None, None);
    };
    let nfo_name = format!("{stem}.nfo").to_ascii_lowercase();
    let poster_names = ["jpg", "jpeg", "png", "webp", "avif"]
        .into_iter()
        .flat_map(|extension| {
            [
                format!("{stem}-poster.{extension}"),
                format!("{stem}-cover.{extension}"),
                format!("{stem}.{extension}"),
                format!("poster.{extension}"),
                format!("cover.{extension}"),
                format!("folder.{extension}"),
            ]
        })
        .map(|name| name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let mut nfo_path = None;
    let mut poster_path = None;
    let Ok(entries) = std::fs::read_dir(parent) else {
        return (None, None);
    };
    for entry in entries.filter_map(Result::ok) {
        if !entry.file_type().is_ok_and(|kind| kind.is_file()) {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_ascii_lowercase();
        if nfo_path.is_none() && name == nfo_name {
            nfo_path = Some(entry.path());
        }
        if poster_path.is_none() && poster_names.contains(&name) {
            poster_path = Some(entry.path());
        }
        if nfo_path.is_some() && poster_path.is_some() {
            break;
        }
    }
    (nfo_path, poster_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_nfo_and_cover_sidecars_case_insensitively() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory =
            std::env::temp_dir().join(format!("luma-resource-test-{}-{nonce}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let media = directory.join("Example.MKV");
        let nfo = directory.join("EXAMPLE.NFO");
        let cover = directory.join("Example-Cover.WEBP");
        std::fs::write(&media, []).unwrap();
        std::fs::write(&nfo, []).unwrap();
        std::fs::write(&cover, []).unwrap();

        let detected = local_media_resources(media.to_str().unwrap());

        assert_eq!(detected.0.as_deref(), Some(nfo.as_path()));
        assert_eq!(detected.1.as_deref(), Some(cover.as_path()));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn repairs_valid_legacy_migration_checksums() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        MIGRATOR.run(&pool).await.unwrap();
        for version in [1_i64, 3_i64] {
            sqlx::query("UPDATE _sqlx_migrations SET checksum = X'00' WHERE version = ?")
                .bind(version)
                .execute(&pool)
                .await
                .unwrap();

            run_migrations_with_legacy_repair(&pool).await.unwrap();

            let actual: Vec<u8> =
                sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version = ?")
                    .bind(version)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let expected = MIGRATOR
                .iter()
                .find(|migration| migration.version == version)
                .unwrap();
            assert_eq!(actual, expected.checksum.as_ref());
        }
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

    #[tokio::test]
    async fn repeated_execution_reuses_the_logical_task() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        MIGRATOR.run(&pool).await.unwrap();
        let folder_id = sqlx::query(
            "INSERT INTO media_config (name, path, media_type) VALUES ('Movies', '/movies', 'movie')",
        )
        .execute(&pool)
        .await
        .unwrap()
        .last_insert_rowid();
        let media_id = sqlx::query(
            "INSERT INTO media_item (folder_id, path, filename, hash, title, media_type) \
             VALUES (?, '/movies/example.mkv', 'example.mkv', 'hash', 'Example', 'movie')",
        )
        .bind(folder_id)
        .execute(&pool)
        .await
        .unwrap()
        .last_insert_rowid();

        let first = create_task_run(&pool, Some(media_id), Some(folder_id), "scrape")
            .await
            .unwrap();
        assert!(
            start_task_run(&pool, first.task.id, first.record_id, 10)
                .await
                .unwrap()
        );
        assert!(
            finish_task_run(
                &pool,
                first.task.id,
                first.record_id,
                "failed",
                Some("provider unavailable"),
            )
            .await
            .unwrap()
        );

        let retry = create_task_run(&pool, Some(media_id), Some(folder_id), "scrape")
            .await
            .unwrap();
        assert_eq!(retry.task.id, first.task.id);
        assert_ne!(retry.record_id, first.record_id);
        assert_eq!(retry.task.record_count, 2);
        assert_eq!(retry.task.media.as_ref().unwrap().title, "Example");

        let detail = task_detail_by_id(&pool, retry.task.id).await.unwrap();
        assert_eq!(detail.records.len(), 2);
        assert_eq!(detail.records[0].status, "pending");
        assert_eq!(detail.records[1].status, "failed");
    }

    #[tokio::test]
    async fn task_record_migration_collapses_legacy_retries() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::raw_sql(include_str!("../migrations/001_init.sql"))
            .execute(&pool)
            .await
            .unwrap();
        let folder_id = sqlx::query(
            "INSERT INTO media_config (name, path, media_type) VALUES ('Movies', '/movies', 'movie')",
        )
        .execute(&pool)
        .await
        .unwrap()
        .last_insert_rowid();
        let media_id = sqlx::query(
            "INSERT INTO media_item (folder_id, path, filename, hash, title, media_type) \
             VALUES (?, '/movies/example.mkv', 'example.mkv', 'hash', 'Example', 'movie')",
        )
        .bind(folder_id)
        .execute(&pool)
        .await
        .unwrap()
        .last_insert_rowid();
        for status in ["failed", "cancelled", "success"] {
            sqlx::query(
                "INSERT INTO scrape_task \
                 (media_id, folder_id, task_type, status, progress, finished_at) \
                 VALUES (?, ?, 'scrape', ?, 100, datetime('now'))",
            )
            .bind(media_id)
            .bind(folder_id)
            .bind(status)
            .execute(&pool)
            .await
            .unwrap();
        }

        sqlx::raw_sql(include_str!("../migrations/003_task_records.sql"))
            .execute(&pool)
            .await
            .unwrap();

        let task_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM scrape_task")
            .fetch_one(&pool)
            .await
            .unwrap();
        let record_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_record")
            .fetch_one(&pool)
            .await
            .unwrap();
        let status: String = sqlx::query_scalar("SELECT status FROM scrape_task")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(task_count, 1);
        assert_eq!(record_count, 3);
        assert_eq!(status, "success");
    }

    #[tokio::test]
    async fn startup_recovers_interrupted_crawler_work() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        MIGRATOR.run(&pool).await.unwrap();
        let script_id = sqlx::query(
            "INSERT INTO crawler_script (name, website_url, file_name, file_path) \
             VALUES ('Crawler', 'https://example.com', 'crawler.py', '/crawler.py')",
        )
        .execute(&pool)
        .await
        .unwrap()
        .last_insert_rowid();
        let run_id =
            sqlx::query("INSERT INTO crawler_run (script_id, status) VALUES (?, 'running')")
                .bind(script_id)
                .execute(&pool)
                .await
                .unwrap()
                .last_insert_rowid();
        sqlx::query(
            "INSERT INTO crawler_result \
             (run_id, script_id, title, download_url, raw_json, download_status) \
             VALUES (?, ?, 'Result', 'magnet:?xt=urn:btih:A', '{}', 'downloading')",
        )
        .bind(run_id)
        .bind(script_id)
        .execute(&pool)
        .await
        .unwrap();

        recover_interrupted_crawler_work(&pool).await.unwrap();

        let run_status: String = sqlx::query_scalar("SELECT status FROM crawler_run WHERE id = ?")
            .bind(run_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        let download_status: String =
            sqlx::query_scalar("SELECT download_status FROM crawler_result WHERE run_id = ?")
                .bind(run_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(run_status, "failed");
        assert_eq!(download_status, "failed");
    }
}
