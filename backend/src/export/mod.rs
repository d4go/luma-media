use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sqlx::{Row, SqlitePool};

pub mod artwork;
pub mod nfo;

#[derive(Debug, Clone, Copy)]
pub struct ExportOptions {
    pub overwrite_nfo: bool,
    pub overwrite_artwork: bool,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            overwrite_nfo: true,
            overwrite_artwork: false,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportReport {
    pub media_id: i64,
    pub library_item_id: Option<i64>,
    pub nfo_path: PathBuf,
    pub poster_path: Option<PathBuf>,
    pub metadata_updated_at: String,
}

pub async fn write_sidecars_for_media(
    pool: &SqlitePool,
    media_id: i64,
    video_path: &Path,
    options: ExportOptions,
) -> anyhow::Result<ExportReport> {
    let metadata_updated_at: String = sqlx::query_scalar("SELECT updated_at FROM media WHERE id=?")
        .bind(media_id)
        .fetch_one(pool)
        .await?;
    let nfo_path = nfo::write_for_video(pool, media_id, video_path, options.overwrite_nfo).await?;
    let poster_path =
        artwork::copy_cached_poster(pool, media_id, video_path, options.overwrite_artwork).await?;
    Ok(ExportReport {
        media_id,
        library_item_id: None,
        nfo_path,
        poster_path,
        metadata_updated_at,
    })
}

pub async fn regenerate_library_item(
    pool: &SqlitePool,
    library_item_id: i64,
    options: ExportOptions,
) -> anyhow::Result<ExportReport> {
    let row = sqlx::query("SELECT media_id,video_path FROM library_item WHERE id=?")
        .bind(library_item_id)
        .fetch_one(pool)
        .await?;
    let media_id: i64 = row.get("media_id");
    let video_path = PathBuf::from(row.get::<String, _>("video_path"));
    let outcome = write_sidecars_for_media(pool, media_id, &video_path, options).await;
    match outcome {
        Ok(mut report) => {
            report.library_item_id = Some(library_item_id);
            sqlx::query("UPDATE library_item SET nfo_path=?,poster_path=COALESCE(?,poster_path),status='ready',updated_at=datetime('now') WHERE id=?")
                .bind(report.nfo_path.to_string_lossy().to_string())
                .bind(report.poster_path.as_ref().map(|path| path.to_string_lossy().to_string()))
                .bind(library_item_id)
                .execute(pool)
                .await?;
            sqlx::query("INSERT INTO library_export_state(library_item_id,media_id,nfo_path,poster_path,metadata_updated_at,status,last_exported_at) VALUES (?,?,?,?,?,'success',datetime('now')) ON CONFLICT(library_item_id) DO UPDATE SET nfo_path=excluded.nfo_path,poster_path=COALESCE(excluded.poster_path,library_export_state.poster_path),metadata_updated_at=excluded.metadata_updated_at,status='success',last_error=NULL,last_exported_at=datetime('now'),updated_at=datetime('now')")
                .bind(library_item_id)
                .bind(media_id)
                .bind(report.nfo_path.to_string_lossy().to_string())
                .bind(report.poster_path.as_ref().map(|path| path.to_string_lossy().to_string()))
                .bind(&report.metadata_updated_at)
                .execute(pool)
                .await?;
            Ok(report)
        }
        Err(error) => {
            sqlx::query("INSERT INTO library_export_state(library_item_id,media_id,status,last_error,last_exported_at) VALUES (?,?,'failed',?,datetime('now')) ON CONFLICT(library_item_id) DO UPDATE SET status='failed',last_error=excluded.last_error,last_exported_at=datetime('now'),updated_at=datetime('now')")
                .bind(library_item_id)
                .bind(media_id)
                .bind(error.to_string())
                .execute(pool)
                .await?;
            Err(error)
        }
    }
}

pub(crate) async fn atomic_write(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("export path has no parent directory"))?;
    tokio::fs::create_dir_all(parent).await?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow::anyhow!("export filename is not valid UTF-8"))?;
    let temporary = parent.join(format!(".{filename}.luma-export-{nonce}.tmp"));
    tokio::fs::write(&temporary, bytes).await?;
    if let Err(error) = tokio::fs::rename(&temporary, path).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use quick_xml::{Reader, events::Event};
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;

    #[tokio::test]
    async fn regenerates_library_nfo_from_canonical_database_without_provider_records() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        let media_id: i64 = sqlx::query_scalar("INSERT INTO media(normalized_code,title,original_title,summary,release_date,duration_minutes,metadata_status) VALUES ('ABC-123','Database & title','鍘熼','Plot <local>','2026-08-10',123,'complete') RETURNING id")
            .fetch_one(&pool)
            .await
            .unwrap();
        let actor_id: i64 = sqlx::query_scalar(
            "INSERT INTO actor(normalized_name,name) VALUES ('actor-a','Actor A') RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO media_actor(media_id,actor_id,billing_order) VALUES (?,?,0)")
            .bind(media_id)
            .bind(actor_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO media_tag(media_id,normalized_tag,tag) VALUES (?,'local-tag','Local Tag')",
        )
        .bind(media_id)
        .execute(&pool)
        .await
        .unwrap();

        let directory = std::env::temp_dir().join(format!(
            "luma-export-test-{}",
            uuid::Uuid::new_v4().simple()
        ));
        tokio::fs::create_dir_all(&directory).await.unwrap();
        let video_path = directory.join("ABC-123.mkv");
        tokio::fs::write(&video_path, b"test video placeholder")
            .await
            .unwrap();
        let library_item_id: i64 = sqlx::query_scalar("INSERT INTO library_item(media_id,video_path,status) VALUES (?,?,'ready') RETURNING id")
            .bind(media_id)
            .bind(video_path.to_string_lossy().to_string())
            .fetch_one(&pool)
            .await
            .unwrap();

        let report = regenerate_library_item(
            &pool,
            library_item_id,
            ExportOptions {
                overwrite_nfo: true,
                overwrite_artwork: false,
            },
        )
        .await
        .unwrap();
        let first_xml = tokio::fs::read_to_string(&report.nfo_path).await.unwrap();
        assert!(first_xml.contains("Database &amp; title"));
        assert!(first_xml.contains("<name>Actor A</name>"));
        assert!(first_xml.contains("<genre>Local Tag</genre>"));
        assert!(first_xml.contains("type=\"luma\" default=\"true\""));
        assert_well_formed(&first_xml);

        let export_status: String =
            sqlx::query_scalar("SELECT status FROM library_export_state WHERE library_item_id=?")
                .bind(library_item_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(export_status, "success");
        let provider_record_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM metadata_source_record")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(provider_record_count, 0);

        sqlx::query("UPDATE media SET title='Offline regenerated title',updated_at=datetime('now','+1 second') WHERE id=?")
            .bind(media_id)
            .execute(&pool)
            .await
            .unwrap();
        regenerate_library_item(
            &pool,
            library_item_id,
            ExportOptions {
                overwrite_nfo: true,
                overwrite_artwork: false,
            },
        )
        .await
        .unwrap();
        let second_xml = tokio::fs::read_to_string(&report.nfo_path).await.unwrap();
        assert!(second_xml.contains("Offline regenerated title"));
        assert!(!second_xml.contains("Database &amp; title"));
        assert_well_formed(&second_xml);

        tokio::fs::remove_dir_all(&directory).await.unwrap();
    }

    fn assert_well_formed(xml: &str) {
        let mut reader = Reader::from_str(xml);
        loop {
            match reader.read_event().unwrap() {
                Event::Eof => break,
                _ => {}
            }
        }
    }
}
