use std::{
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};

use chrono::Datelike;
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};
use tokio::io::AsyncWriteExt;

use crate::fetch::{FetchMode, FetchResponse};

#[derive(Debug)]
pub struct SnapshotInput<'a> {
    pub provider_key: &'a str,
    pub entity_type: &'a str,
    pub provider_entity_id: Option<&'a str>,
    pub media_id: Option<i64>,
    pub source_url: &'a str,
    pub response: &'a FetchResponse,
    pub raw_json: Value,
    pub parser_version: &'a str,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredSnapshot {
    pub id: i64,
    pub body_hash: String,
    pub body_path: String,
}

#[derive(Debug, Clone)]
pub struct LoadedSnapshot {
    pub id: i64,
    pub provider_key: String,
    pub entity_type: String,
    pub provider_entity_id: Option<String>,
    pub media_id: Option<i64>,
    pub source_url: String,
    pub final_url: Option<String>,
    pub fetch_mode: FetchMode,
    pub http_status: Option<u16>,
    pub content_type: Option<String>,
    pub body_hash: String,
    pub body: String,
    pub raw_json: Value,
    pub parser_version: String,
    pub fetched_at: String,
}

#[derive(Debug, Clone)]
pub struct SnapshotRepository {
    pool: SqlitePool,
    cache_root: PathBuf,
}

impl SnapshotRepository {
    pub fn new(pool: SqlitePool, cache_root: PathBuf) -> Self {
        Self { pool, cache_root }
    }

    pub async fn store(&self, input: SnapshotInput<'_>) -> anyhow::Result<StoredSnapshot> {
        validate_segment(input.provider_key, "provider key")?;
        validate_segment(input.entity_type, "entity type")?;
        let body_hash = sha256_hex(input.response.body.as_bytes());
        let relative_path = PathBuf::from(input.provider_key)
            .join(format!("{:04}", input.response.fetched_at.year()))
            .join(format!("{:02}", input.response.fetched_at.month()))
            .join(format!("{body_hash}.html.gz"));
        let body_path = relative_path.to_string_lossy().replace('\\', "/");
        let absolute_path = self.safe_cache_path(&body_path)?;
        if let Some(parent) = absolute_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        write_gzip_once(&absolute_path, input.response.body.as_bytes()).await?;
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO provider_raw_snapshot(provider_key,entity_type,provider_entity_id,media_id,source_url,final_url,fetch_mode,http_status,content_type,body_hash,body_path,raw_json,parser_version,fetched_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id",
        )
        .bind(input.provider_key)
        .bind(input.entity_type)
        .bind(input.provider_entity_id)
        .bind(input.media_id)
        .bind(input.source_url)
        .bind(input.response.final_url.as_str())
        .bind(fetch_mode_name(input.response.fetch_mode))
        .bind(input.response.status.map(i64::from))
        .bind(input.response.content_type.as_deref())
        .bind(&body_hash)
        .bind(&body_path)
        .bind(input.raw_json.to_string())
        .bind(input.parser_version)
        .bind(input.response.fetched_at.to_rfc3339())
        .fetch_one(&self.pool)
        .await?;
        Ok(StoredSnapshot {
            id,
            body_hash,
            body_path,
        })
    }

    pub async fn load(&self, id: i64) -> anyhow::Result<LoadedSnapshot> {
        let row = sqlx::query("SELECT * FROM provider_raw_snapshot WHERE id=?")
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        let body_path: String = row.get("body_path");
        let absolute_path = self.safe_cache_path(&body_path)?;
        let compressed = tokio::fs::read(absolute_path).await?;
        let body = tokio::task::spawn_blocking(move || -> anyhow::Result<String> {
            let mut decoder = GzDecoder::new(compressed.as_slice());
            let mut body = String::new();
            decoder.read_to_string(&mut body)?;
            Ok(body)
        })
        .await??;
        let body_hash: String = row.get("body_hash");
        anyhow::ensure!(
            sha256_hex(body.as_bytes()) == body_hash,
            "snapshot body hash does not match its database record"
        );
        Ok(LoadedSnapshot {
            id: row.get("id"),
            provider_key: row.get("provider_key"),
            entity_type: row.get("entity_type"),
            provider_entity_id: row.get("provider_entity_id"),
            media_id: row.get("media_id"),
            source_url: row.get("source_url"),
            final_url: row.get("final_url"),
            fetch_mode: FetchMode::parse(Some(&row.get::<String, _>("fetch_mode"))),
            http_status: row
                .get::<Option<i64>, _>("http_status")
                .and_then(|value| u16::try_from(value).ok()),
            content_type: row.get("content_type"),
            body_hash,
            body,
            raw_json: serde_json::from_str(&row.get::<String, _>("raw_json"))
                .unwrap_or_else(|_| serde_json::json!({})),
            parser_version: row.get("parser_version"),
            fetched_at: row.get("fetched_at"),
        })
    }

    pub async fn latest_ids(&self, provider_key: &str, limit: i64) -> sqlx::Result<Vec<i64>> {
        sqlx::query_scalar(
            "SELECT id FROM provider_raw_snapshot WHERE provider_key=? ORDER BY id DESC LIMIT ?",
        )
        .bind(provider_key)
        .bind(limit.clamp(1, 1000))
        .fetch_all(&self.pool)
        .await
    }

    pub async fn mark_reparsed(
        &self,
        id: i64,
        parser_version: &str,
        output: &Value,
    ) -> sqlx::Result<()> {
        sqlx::query("UPDATE provider_raw_snapshot SET parser_version=?,raw_json=json_set(raw_json,'$.reparse',json(?),'$.reparse.completedAt',datetime('now')) WHERE id=?")
            .bind(parser_version)
            .bind(output.to_string())
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    fn safe_cache_path(&self, relative: &str) -> anyhow::Result<PathBuf> {
        let relative = Path::new(relative);
        anyhow::ensure!(
            relative
                .components()
                .all(|component| matches!(component, Component::Normal(_))),
            "snapshot cache path is not safe"
        );
        Ok(self.cache_root.join(relative))
    }
}

async fn write_gzip_once(path: &Path, body: &[u8]) -> anyhow::Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let body = body.to_vec();
    let compressed = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<u8>> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&body)?;
        Ok(encoder.finish()?)
    })
    .await??;
    match tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .await
    {
        Ok(mut file) => {
            file.write_all(&compressed).await?;
            file.flush().await?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn validate_segment(value: &str, label: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "{label} is not safe for a cache path"
    );
    Ok(())
}

fn fetch_mode_name(mode: FetchMode) -> &'static str {
    match mode {
        FetchMode::Http => "http",
        FetchMode::Browser => "browser",
        FetchMode::Auto => "auto",
    }
}

fn sha256_hex(body: &[u8]) -> String {
    Sha256::digest(body)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cache_paths_reject_traversal() {
        let repository = SnapshotRepository::new(
            SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
            PathBuf::from("/data/source-cache"),
        );
        assert!(
            repository
                .safe_cache_path("javdb/2026/08/hash.html.gz")
                .is_ok()
        );
        assert!(repository.safe_cache_path("../luma.db").is_err());
        assert!(repository.safe_cache_path("javdb/../../luma.db").is_err());
    }

    #[test]
    fn snapshot_hash_is_stable() {
        assert_eq!(
            sha256_hex(b"luma"),
            "53009c20073f1d96f75d46db1f6f25bc9b461cda906accc86792e189986ecb1f"
        );
    }

    #[tokio::test]
    async fn snapshot_round_trip_uses_only_the_local_cache() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let cache_root = std::env::temp_dir().join(format!(
            "luma-snapshot-test-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let repository = SnapshotRepository::new(pool.clone(), cache_root.clone());
        let response = FetchResponse {
            final_url: reqwest::Url::parse("https://example.test/item/ABC-123").unwrap(),
            status: Some(200),
            content_type: Some("text/html; charset=utf-8".into()),
            headers: reqwest::header::HeaderMap::new(),
            body: "<html><title>ABC-123 テスト</title></html>".into(),
            fetched_at: chrono::Utc::now(),
            fetch_mode: FetchMode::Browser,
            elapsed_ms: 42,
        };

        let stored = repository
            .store(SnapshotInput {
                provider_key: "test-provider",
                entity_type: "detail",
                provider_entity_id: Some("ABC-123"),
                media_id: None,
                source_url: "https://example.test/item/ABC-123",
                response: &response,
                raw_json: serde_json::json!({ "pageKind": "valid_content" }),
                parser_version: "1",
            })
            .await
            .unwrap();
        assert!(
            tokio::fs::try_exists(cache_root.join(&stored.body_path))
                .await
                .unwrap()
        );

        let loaded = repository.load(stored.id).await.unwrap();
        assert_eq!(loaded.body, response.body);
        assert_eq!(loaded.body_hash, stored.body_hash);
        assert_eq!(loaded.provider_key, "test-provider");
        assert_eq!(loaded.provider_entity_id.as_deref(), Some("ABC-123"));
        assert_eq!(loaded.raw_json["pageKind"], "valid_content");
        assert_eq!(loaded.parser_version, "1");
        assert!(!loaded.fetched_at.is_empty());

        repository
            .mark_reparsed(
                stored.id,
                "2",
                &serde_json::json!({ "pageKind": "valid_content", "candidateCount": 1 }),
            )
            .await
            .unwrap();
        let reparsed = repository.load(stored.id).await.unwrap();
        assert_eq!(reparsed.parser_version, "2");
        assert_eq!(reparsed.raw_json["reparse"]["pageKind"], "valid_content");
        assert_eq!(reparsed.raw_json["reparse"]["candidateCount"], 1);

        pool.close().await;
        tokio::fs::remove_dir_all(cache_root).await.unwrap();
    }
}
