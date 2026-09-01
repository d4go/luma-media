use std::path::{Path, PathBuf};

use sqlx::SqlitePool;

pub async fn copy_cached_poster(
    pool: &SqlitePool,
    media_id: i64,
    video_path: &Path,
    overwrite: bool,
) -> anyhow::Result<Option<PathBuf>> {
    let source: Option<String> = sqlx::query_scalar("SELECT local_path FROM canonical_media_asset WHERE media_id=? AND asset_type='poster' AND status='available' ORDER BY updated_at DESC,id DESC LIMIT 1")
        .bind(media_id)
        .fetch_optional(pool)
        .await?
        .flatten();
    let Some(source) = source else {
        return Ok(None);
    };
    let source = PathBuf::from(source);
    if !tokio::fs::try_exists(&source).await? {
        sqlx::query("UPDATE canonical_media_asset SET status='missing',last_verified_at=datetime('now'),updated_at=datetime('now') WHERE media_id=? AND asset_type='poster' AND local_path=?")
            .bind(media_id)
            .bind(source.to_string_lossy().to_string())
            .execute(pool)
            .await?;
        return Ok(None);
    }
    let extension = source
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("jpg");
    let stem = video_path
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow::anyhow!("video filename is not valid UTF-8"))?;
    let destination = video_path.with_file_name(format!("{stem}-poster.{extension}"));
    if source == destination {
        return Ok(Some(destination));
    }
    if !overwrite && tokio::fs::try_exists(&destination).await? {
        return Ok(Some(destination));
    }
    let bytes = tokio::fs::read(&source).await?;
    super::atomic_write(&destination, &bytes).await?;
    Ok(Some(destination))
}

pub async fn register_cached_asset(
    pool: &SqlitePool,
    media_id: i64,
    asset_type: &str,
    source_url: Option<&str>,
    local_path: &Path,
    mime_type: Option<&str>,
) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO canonical_media_asset(media_id,asset_type,source_url,local_path,mime_type,status) VALUES (?,?,?,?,?,'available') ON CONFLICT(media_id,asset_type,local_path) DO UPDATE SET source_url=COALESCE(excluded.source_url,canonical_media_asset.source_url),mime_type=COALESCE(excluded.mime_type,canonical_media_asset.mime_type),status='available',last_verified_at=datetime('now'),updated_at=datetime('now')")
        .bind(media_id)
        .bind(asset_type)
        .bind(source_url)
        .bind(local_path.to_string_lossy().to_string())
        .bind(mime_type)
        .execute(pool)
        .await?;
    Ok(())
}
