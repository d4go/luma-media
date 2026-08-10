use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};

use crate::providers::SourceMedia;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    Bootstrap,
    Incremental,
    OnDemand,
}

impl SyncMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bootstrap => "bootstrap",
            Self::Incremental => "incremental",
            Self::OnDemand => "on_demand",
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryJobPayload {
    pub run_id: i64,
    pub mode: SyncMode,
    pub page_url: String,
    pub page: i64,
    pub window_from: String,
    pub window_to: String,
    pub include_resources: bool,
    pub pages_remaining: i64,
}

#[derive(Debug, Clone)]
pub struct DiscoveredCandidate {
    pub id: i64,
    pub content_hash: String,
    pub should_hydrate: bool,
    pub inserted: bool,
}

pub async fn upsert_candidates(
    pool: &SqlitePool,
    provider_key: &str,
    items: Vec<SourceMedia>,
) -> anyhow::Result<Vec<DiscoveredCandidate>> {
    let mut transaction = pool.begin().await?;
    let mut candidates = Vec::with_capacity(items.len());
    for item in items {
        let content_hash = candidate_hash(&item);
        let existing = sqlx::query("SELECT id,content_hash,last_hydrated_hash,hydration_status,media_id FROM provider_discovery_item WHERE provider_key=? AND provider_entity_id=?")
            .bind(provider_key)
            .bind(&item.provider_id)
            .fetch_optional(&mut *transaction)
            .await?;
        let (id, should_hydrate, inserted) = if let Some(existing) = existing {
            let old_hash: String = existing.get("content_hash");
            let hydrated_hash: Option<String> = existing.get("last_hydrated_hash");
            let status: String = existing.get("hydration_status");
            let media_id: Option<i64> = existing.get("media_id");
            let should_hydrate = old_hash != content_hash
                || hydrated_hash.as_deref() != Some(content_hash.as_str())
                || status != "hydrated"
                || media_id.is_none();
            sqlx::query("UPDATE provider_discovery_item SET normalized_code=?,source_url=?,release_date=COALESCE(?,release_date),title_hint=?,poster_hint=COALESCE(?,poster_hint),content_hash=?,last_seen_at=datetime('now'),hydration_status=CASE WHEN ? AND hydration_status!='running' THEN 'pending' ELSE hydration_status END,last_error=CASE WHEN ? THEN NULL ELSE last_error END WHERE id=?")
                .bind(&item.code)
                .bind(&item.source_url)
                .bind(&item.release_date)
                .bind(&item.title)
                .bind(&item.poster_url)
                .bind(&content_hash)
                .bind(should_hydrate)
                .bind(should_hydrate)
                .bind(existing.get::<i64, _>("id"))
                .execute(&mut *transaction)
                .await?;
            (existing.get("id"), should_hydrate, false)
        } else {
            let id = sqlx::query_scalar("INSERT INTO provider_discovery_item(provider_key,provider_entity_id,normalized_code,source_url,release_date,title_hint,poster_hint,content_hash) VALUES (?,?,?,?,?,?,?,?) RETURNING id")
                .bind(provider_key)
                .bind(&item.provider_id)
                .bind(&item.code)
                .bind(&item.source_url)
                .bind(&item.release_date)
                .bind(&item.title)
                .bind(&item.poster_url)
                .bind(&content_hash)
                .fetch_one(&mut *transaction)
                .await?;
            (id, true, true)
        };
        candidates.push(DiscoveredCandidate {
            id,
            content_hash,
            should_hydrate,
            inserted,
        });
    }
    transaction.commit().await?;
    Ok(candidates)
}

pub fn within_window(item: &SourceMedia, from: &str, to: &str) -> bool {
    item.release_date
        .as_deref()
        .is_none_or(|date| date >= from && date <= to)
}

pub fn reached_window_start(items: &[SourceMedia], from: &str) -> bool {
    let dates = items
        .iter()
        .filter_map(|item| item.release_date.as_deref())
        .collect::<Vec<_>>();
    !dates.is_empty() && dates.iter().all(|date| *date <= from)
}

fn candidate_hash(item: &SourceMedia) -> String {
    let mut digest = Sha256::new();
    for value in [
        item.provider_id.as_str(),
        item.code.as_str(),
        item.title.as_str(),
        item.poster_url.as_deref().unwrap_or_default(),
        item.source_url.as_str(),
        item.release_date.as_deref().unwrap_or_default(),
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(title: &str) -> SourceMedia {
        SourceMedia {
            provider_id: "ABC-123".into(),
            code: "abc-123".into(),
            title: title.into(),
            poster_url: Some("https://example.test/cover.jpg".into()),
            source_url: "https://example.test/ABC-123".into(),
            release_date: Some("2026-08-08".into()),
        }
    }

    #[tokio::test]
    async fn repeated_discovery_is_idempotent_until_content_changes() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();

        let first = upsert_candidates(&pool, "javdb", vec![item("First title")])
            .await
            .unwrap();
        assert!(first[0].should_hydrate);
        sqlx::query("UPDATE provider_discovery_item SET hydration_status='hydrated',last_hydrated_hash=content_hash,media_id=(SELECT id FROM media LIMIT 1) WHERE id=?")
            .bind(first[0].id)
            .execute(&pool)
            .await
            .unwrap();
        let media_id: i64 = sqlx::query_scalar(
            "INSERT INTO media(normalized_code,title) VALUES ('abc-123','First title') RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query("UPDATE provider_discovery_item SET media_id=? WHERE id=?")
            .bind(media_id)
            .bind(first[0].id)
            .execute(&pool)
            .await
            .unwrap();

        let repeated = upsert_candidates(&pool, "javdb", vec![item("First title")])
            .await
            .unwrap();
        assert_eq!(repeated[0].id, first[0].id);
        assert!(!repeated[0].should_hydrate);
        let changed = upsert_candidates(&pool, "javdb", vec![item("Updated title")])
            .await
            .unwrap();
        assert_eq!(changed[0].id, first[0].id);
        assert!(changed[0].should_hydrate);
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_discovery_item WHERE provider_key='javdb'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 1);
    }
}
