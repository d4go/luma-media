use sqlx::{Row, SqlitePool};

use super::SyncMode;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HydrationJobPayload {
    pub run_id: i64,
    pub discovery_item_id: i64,
    pub content_hash: String,
    pub mode: SyncMode,
    pub include_resources: bool,
}

#[derive(Debug, Clone)]
pub struct DiscoveryItem {
    pub id: i64,
    pub provider_key: String,
    pub provider_entity_id: String,
    pub normalized_code: String,
    pub title_hint: String,
    pub poster_hint: Option<String>,
    pub source_url: String,
    pub release_date: Option<String>,
    pub content_hash: String,
}

pub async fn start(pool: &SqlitePool, id: i64) -> anyhow::Result<DiscoveryItem> {
    let row = sqlx::query("UPDATE provider_discovery_item SET hydration_status='running',hydration_attempts=hydration_attempts+1,last_error=NULL WHERE id=? RETURNING *")
        .bind(id)
        .fetch_one(pool)
        .await?;
    Ok(DiscoveryItem {
        id: row.get("id"),
        provider_key: row.get("provider_key"),
        provider_entity_id: row.get("provider_entity_id"),
        normalized_code: row
            .get::<Option<String>, _>("normalized_code")
            .unwrap_or_default(),
        title_hint: row
            .get::<Option<String>, _>("title_hint")
            .unwrap_or_default(),
        poster_hint: row.get("poster_hint"),
        source_url: row.get("source_url"),
        release_date: row.get("release_date"),
        content_hash: row.get("content_hash"),
    })
}

pub async fn finish(
    pool: &SqlitePool,
    id: i64,
    media_id: i64,
    content_hash: &str,
) -> sqlx::Result<()> {
    sqlx::query("UPDATE provider_discovery_item SET hydration_status='hydrated',media_id=?,last_hydrated_hash=?,last_hydrated_at=datetime('now'),last_error=NULL WHERE id=?")
        .bind(media_id)
        .bind(content_hash)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn fail(pool: &SqlitePool, id: i64, error: &str) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE provider_discovery_item SET hydration_status='failed',last_error=? WHERE id=?",
    )
    .bind(error)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}
