use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};

use crate::{
    providers::ResourceCandidate,
    qbittorrent::{magnet_hash, normalize_hash},
};

use super::rank_resource;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpsertedResource {
    pub id: i64,
    pub inserted: bool,
}

pub async fn upsert_candidate(
    pool: &SqlitePool,
    media_id: i64,
    provider_key: &str,
    candidate: &ResourceCandidate,
) -> anyhow::Result<UpsertedResource> {
    let info_hash = candidate
        .info_hash
        .as_deref()
        .and_then(normalize_hash)
        .or_else(|| magnet_hash(&candidate.download_url));
    let fingerprint = info_hash
        .as_deref()
        .map(|hash| format!("btih:{hash}"))
        .unwrap_or_else(|| {
            format!(
                "url:{}",
                sha256_hex(&canonical_url(&candidate.download_url))
            )
        });
    let source_identity = candidate
        .provider_resource_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("id:{}", value.trim()))
        .unwrap_or_else(|| fingerprint.clone());
    let size_label = candidate.size_bytes.map(|value| value.to_string());
    let (score, reasons) = rank_resource(
        &candidate.title,
        size_label.as_deref(),
        candidate.published_at.as_deref().unwrap_or_default(),
        provider_key,
    );
    let mut transaction = pool.begin().await?;
    let existing = if let Some(info_hash) = info_hash.as_deref() {
        sqlx::query("SELECT id,media_id,trackers_json FROM resource WHERE info_hash=?")
            .bind(info_hash)
            .fetch_optional(&mut *transaction)
            .await?
    } else if let Some(row) = sqlx::query("SELECT resource.id,resource.media_id,resource.trackers_json FROM resource_source_mapping mapping JOIN resource ON resource.id=mapping.resource_id WHERE mapping.provider_key=? AND mapping.source_identity=?")
        .bind(provider_key)
        .bind(&source_identity)
        .fetch_optional(&mut *transaction)
        .await?
    {
        Some(row)
    } else {
        sqlx::query("SELECT id,media_id,trackers_json FROM resource WHERE resource_fingerprint=?")
            .bind(&fingerprint)
            .fetch_optional(&mut *transaction)
            .await?
    };

    let (resource_id, inserted) = if let Some(row) = existing {
        let resource_id: i64 = row.get("id");
        let existing_media_id: i64 = row.get("media_id");
        if existing_media_id != media_id {
            tracing::warn!(
                resource_id,
                existing_media_id,
                candidate_media_id = media_id,
                ?info_hash,
                "canonical torrent already belongs to another media item"
            );
        }
        let trackers = merge_trackers(&row.get::<String, _>("trackers_json"), &candidate.trackers);
        sqlx::query("UPDATE resource SET title=?,download_url=?,info_hash=COALESCE(?,info_hash),size_bytes=COALESCE(?,size_bytes),resolution=COALESCE(?,resolution),subtitle_languages_json=?,trackers_json=?,published_at=COALESCE(?,published_at),score=MAX(score,?),score_reasons_json=?,available=1,availability_status='available',codec=COALESCE(?,codec),last_seen_at=datetime('now'),last_verified_at=datetime('now'),resource_fingerprint=COALESCE(resource_fingerprint,?),raw_json=?,updated_at=datetime('now') WHERE id=?")
            .bind(&candidate.title)
            .bind(&candidate.download_url)
            .bind(&info_hash)
            .bind(candidate.size_bytes)
            .bind(&candidate.resolution)
            .bind(serde_json::to_string(&candidate.subtitle_languages)?)
            .bind(serde_json::to_string(&trackers)?)
            .bind(&candidate.published_at)
            .bind(score)
            .bind(serde_json::to_string(&reasons)?)
            .bind(&candidate.codec)
            .bind(&fingerprint)
            .bind(candidate.raw_json.to_string())
            .bind(resource_id)
            .execute(&mut *transaction)
            .await?;
        (resource_id, false)
    } else {
        let resource_id: i64 = sqlx::query_scalar("INSERT INTO resource(media_id,provider_key,provider_resource_id,title,download_url,info_hash,size_bytes,resolution,subtitle_languages_json,trackers_json,published_at,score,score_reasons_json,available,raw_json,first_seen_at,last_seen_at,last_verified_at,availability_status,codec,source_count,resource_fingerprint) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,datetime('now'),datetime('now'),datetime('now'),'available',?,1,?) RETURNING id")
            .bind(media_id)
            .bind(provider_key)
            .bind(&candidate.provider_resource_id)
            .bind(&candidate.title)
            .bind(&candidate.download_url)
            .bind(&info_hash)
            .bind(candidate.size_bytes)
            .bind(&candidate.resolution)
            .bind(serde_json::to_string(&candidate.subtitle_languages)?)
            .bind(serde_json::to_string(&candidate.trackers)?)
            .bind(&candidate.published_at)
            .bind(score)
            .bind(serde_json::to_string(&reasons)?)
            .bind(candidate.raw_json.to_string())
            .bind(&candidate.codec)
            .bind(&fingerprint)
            .fetch_one(&mut *transaction)
            .await?;
        (resource_id, true)
    };

    sqlx::query("INSERT INTO resource_source_mapping(resource_id,provider_key,provider_resource_id,source_identity,source_url,raw_json) VALUES (?,?,?,?,?,?) ON CONFLICT(provider_key,source_identity) DO UPDATE SET resource_id=excluded.resource_id,provider_resource_id=excluded.provider_resource_id,source_url=excluded.source_url,last_seen_at=datetime('now'),raw_json=excluded.raw_json")
        .bind(resource_id)
        .bind(provider_key)
        .bind(&candidate.provider_resource_id)
        .bind(&source_identity)
        .bind(&candidate.source_url)
        .bind(candidate.raw_json.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("UPDATE resource SET source_count=(SELECT COUNT(*) FROM resource_source_mapping WHERE resource_id=?) WHERE id=?")
        .bind(resource_id)
        .bind(resource_id)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(UpsertedResource {
        id: resource_id,
        inserted,
    })
}

pub async fn cache_needs_refresh(
    pool: &SqlitePool,
    media_id: i64,
    provider_key: &str,
) -> sqlx::Result<bool> {
    let needs: i64 = sqlx::query_scalar("SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM media_resource_cache WHERE media_id=? AND provider_key=? AND expires_at>datetime('now')) OR NOT EXISTS(SELECT 1 FROM resource_source_mapping mapping JOIN resource ON resource.id=mapping.resource_id WHERE mapping.provider_key=? AND resource.media_id=? AND resource.available=1) THEN 1 ELSE 0 END")
        .bind(media_id)
        .bind(provider_key)
        .bind(provider_key)
        .bind(media_id)
        .fetch_one(pool)
        .await?;
    Ok(needs != 0)
}

pub async fn record_refresh_success(
    pool: &SqlitePool,
    media_id: i64,
    provider_key: &str,
    resource_count: usize,
    ttl_hours: i64,
) -> sqlx::Result<()> {
    let modifier = format!("+{} hours", ttl_hours.clamp(1, 24 * 30));
    sqlx::query("INSERT INTO media_resource_cache(media_id,provider_key,last_refreshed_at,last_success_at,expires_at,resource_count,last_error) VALUES (?,?,datetime('now'),datetime('now'),datetime('now',?),?,NULL) ON CONFLICT(media_id,provider_key) DO UPDATE SET last_refreshed_at=datetime('now'),last_success_at=datetime('now'),expires_at=excluded.expires_at,resource_count=excluded.resource_count,last_error=NULL,updated_at=datetime('now')")
        .bind(media_id)
        .bind(provider_key)
        .bind(&modifier)
        .bind(resource_count as i64)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn record_refresh_failure(
    pool: &SqlitePool,
    media_id: i64,
    provider_key: &str,
    error: &str,
) -> sqlx::Result<()> {
    sqlx::query("INSERT INTO media_resource_cache(media_id,provider_key,last_refreshed_at,expires_at,last_error) VALUES (?,?,datetime('now'),datetime('now','+1 hour'),?) ON CONFLICT(media_id,provider_key) DO UPDATE SET last_refreshed_at=datetime('now'),expires_at=datetime('now','+1 hour'),last_error=excluded.last_error,updated_at=datetime('now')")
        .bind(media_id)
        .bind(provider_key)
        .bind(error)
        .execute(pool)
        .await?;
    Ok(())
}

fn canonical_url(value: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(value.trim()) else {
        return value.trim().to_owned();
    };
    url.set_fragment(None);
    url.to_string()
}

fn sha256_hex(value: &str) -> String {
    Sha256::digest(value.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn merge_trackers(existing: &str, incoming: &[String]) -> Vec<String> {
    let mut trackers = serde_json::from_str::<Vec<String>>(existing).unwrap_or_default();
    trackers.extend(incoming.iter().cloned());
    trackers.sort();
    trackers.dedup();
    trackers
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn candidate(provider_resource_id: &str, url: &str) -> ResourceCandidate {
        ResourceCandidate {
            provider_resource_id: Some(provider_resource_id.into()),
            download_url: url.into(),
            title: "ABC-123 1080p".into(),
            info_hash: magnet_hash(url),
            size_bytes: None,
            resolution: Some("1080p".into()),
            subtitle_languages: Vec::new(),
            trackers: Vec::new(),
            published_at: None,
            codec: None,
            source_url: "https://source.test/item".into(),
            raw_json: json!({}),
        }
    }

    #[tokio::test]
    async fn same_torrent_from_two_providers_has_one_resource_and_two_sources() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let media_id: i64 = sqlx::query_scalar(
            "INSERT INTO media(normalized_code,title) VALUES ('abc-123','ABC-123') RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let magnet = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let first = upsert_candidate(&pool, media_id, "javbus", &candidate("bus-1", magnet))
            .await
            .unwrap();
        let second = upsert_candidate(&pool, media_id, "javdb", &candidate("db-1", magnet))
            .await
            .unwrap();
        assert_eq!(first.id, second.id);
        assert!(first.inserted);
        assert!(!second.inserted);
        let resource_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM resource")
            .fetch_one(&pool)
            .await
            .unwrap();
        let source_count: i64 = sqlx::query_scalar("SELECT source_count FROM resource WHERE id=?")
            .bind(first.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(resource_count, 1);
        assert_eq!(source_count, 2);

        let second_magnet = "magnet:?xt=urn:btih:89abcdef0123456789abcdef0123456789abcdef";
        let distinct = upsert_candidate(
            &pool,
            media_id,
            "javbus",
            &candidate("bus-2", second_magnet),
        )
        .await
        .unwrap();
        assert_ne!(distinct.id, first.id);
        let two_resources: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM resource")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(two_resources, 2);
    }

    #[tokio::test]
    async fn cache_refreshes_only_when_missing_expired_or_unavailable() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let media_id: i64 = sqlx::query_scalar(
            "INSERT INTO media(normalized_code,title) VALUES ('abc-124','ABC-124') RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            cache_needs_refresh(&pool, media_id, "javbus")
                .await
                .unwrap()
        );
        let magnet = "magnet:?xt=urn:btih:1123456789abcdef0123456789abcdef01234567";
        upsert_candidate(&pool, media_id, "javbus", &candidate("bus-2", magnet))
            .await
            .unwrap();
        record_refresh_success(&pool, media_id, "javbus", 1, 72)
            .await
            .unwrap();
        assert!(
            !cache_needs_refresh(&pool, media_id, "javbus")
                .await
                .unwrap()
        );

        sqlx::query(
            "UPDATE resource SET available=0,availability_status='unavailable' WHERE media_id=?",
        )
        .bind(media_id)
        .execute(&pool)
        .await
        .unwrap();
        assert!(
            cache_needs_refresh(&pool, media_id, "javbus")
                .await
                .unwrap()
        );
        sqlx::query("UPDATE resource SET available=1,availability_status='available'")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE media_resource_cache SET expires_at=datetime('now','-1 minute')")
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            cache_needs_refresh(&pool, media_id, "javbus")
                .await
                .unwrap()
        );
    }
}
