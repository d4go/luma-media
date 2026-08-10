use std::{sync::Arc, time::Duration};

use serde_json::Value;
use sqlx::{Row, SqlitePool, sqlite::SqliteRow};
use tokio::sync::Notify;

pub const PRIORITY_USER_ON_DEMAND: i64 = 1000;
pub const PRIORITY_DAILY_INCREMENTAL: i64 = 800;
pub const PRIORITY_RECENT_REPAIR: i64 = 500;
pub const PRIORITY_HISTORICAL_BOOTSTRAP: i64 = 100;

#[derive(Debug, Clone)]
pub struct IngestionJob {
    pub id: i64,
    pub provider_key: String,
    pub job_type: String,
    pub priority: i64,
    pub payload: Value,
    pub attempts: i64,
    pub max_attempts: i64,
}

#[derive(Debug, Clone)]
pub struct EnqueueJob<'a> {
    pub provider_key: &'a str,
    pub job_type: &'a str,
    pub priority: i64,
    pub payload: Value,
    pub max_attempts: i64,
    pub dedupe_key: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnqueuedJob {
    pub id: i64,
    pub inserted: bool,
}

#[derive(Debug, Clone)]
pub struct IngestionQueue {
    pool: SqlitePool,
    notify: Arc<Notify>,
}

impl IngestionQueue {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            notify: Arc::new(Notify::new()),
        }
    }

    pub async fn enqueue(&self, job: EnqueueJob<'_>) -> anyhow::Result<EnqueuedJob> {
        anyhow::ensure!(
            !job.provider_key.trim().is_empty(),
            "provider key is required"
        );
        anyhow::ensure!(!job.job_type.trim().is_empty(), "job type is required");
        anyhow::ensure!(
            matches!(
                job.priority,
                PRIORITY_USER_ON_DEMAND
                    | PRIORITY_DAILY_INCREMENTAL
                    | PRIORITY_RECENT_REPAIR
                    | PRIORITY_HISTORICAL_BOOTSTRAP
            ),
            "unsupported ingestion priority"
        );
        anyhow::ensure!(job.max_attempts > 0, "max attempts must be positive");
        anyhow::ensure!(
            job.dedupe_key.is_none_or(|value| !value.trim().is_empty()),
            "dedupe key cannot be empty"
        );
        let payload = job.payload.to_string();
        let inserted = sqlx::query_scalar::<_, i64>(
            "INSERT INTO ingestion_job(provider_key,job_type,priority,payload_json,dedupe_key,max_attempts) VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING RETURNING id",
        )
        .bind(job.provider_key)
        .bind(job.job_type)
        .bind(job.priority)
        .bind(&payload)
        .bind(job.dedupe_key)
        .bind(job.max_attempts)
        .fetch_optional(&self.pool)
        .await?;
        let result = if let Some(id) = inserted {
            EnqueuedJob { id, inserted: true }
        } else if let Some(dedupe_key) = job.dedupe_key {
            sqlx::query(
                "UPDATE ingestion_job SET payload_json=CASE WHEN status='pending' AND priority<? THEN ? ELSE payload_json END,priority=MAX(priority,?),available_at=CASE WHEN status='pending' THEN datetime('now') ELSE available_at END,updated_at=datetime('now') WHERE dedupe_key=? AND status IN ('pending','running')",
            )
            .bind(job.priority)
            .bind(&payload)
            .bind(job.priority)
            .bind(dedupe_key)
            .execute(&self.pool)
            .await?;
            let id = sqlx::query_scalar(
                "SELECT id FROM ingestion_job WHERE dedupe_key=? AND status IN ('pending','running') ORDER BY id DESC LIMIT 1",
            )
            .bind(dedupe_key)
            .fetch_one(&self.pool)
            .await?;
            EnqueuedJob {
                id,
                inserted: false,
            }
        } else {
            anyhow::bail!("job insert was ignored without a dedupe key");
        };
        self.notify.notify_one();
        Ok(result)
    }

    pub async fn claim(
        &self,
        lease_owner: &str,
        lease_duration: Duration,
    ) -> anyhow::Result<Option<IngestionJob>> {
        self.claim_at_or_above(lease_owner, lease_duration, PRIORITY_HISTORICAL_BOOTSTRAP)
            .await
    }

    pub async fn claim_at_or_above(
        &self,
        lease_owner: &str,
        lease_duration: Duration,
        minimum_priority: i64,
    ) -> anyhow::Result<Option<IngestionJob>> {
        anyhow::ensure!(!lease_owner.is_empty(), "lease owner is required");
        self.recover_expired().await?;
        let lease_modifier = format!("+{} seconds", lease_duration.as_secs().max(1));
        let row = sqlx::query(
            "UPDATE ingestion_job SET status='running',attempts=attempts+1,lease_owner=?,lease_expires_at=datetime('now',?),started_at=COALESCE(started_at,datetime('now')),updated_at=datetime('now'),finished_at=NULL WHERE id=(SELECT id FROM ingestion_job WHERE status='pending' AND attempts<max_attempts AND available_at<=datetime('now') AND priority>=? ORDER BY priority DESC,available_at,id LIMIT 1) RETURNING *",
        )
        .bind(lease_owner)
        .bind(lease_modifier)
        .bind(minimum_priority)
        .fetch_optional(&self.pool)
        .await?;
        row.map(job_from_row).transpose()
    }

    pub async fn renew(
        &self,
        id: i64,
        lease_owner: &str,
        lease_duration: Duration,
    ) -> sqlx::Result<bool> {
        let modifier = format!("+{} seconds", lease_duration.as_secs().max(1));
        let result = sqlx::query("UPDATE ingestion_job SET lease_expires_at=datetime('now',?),updated_at=datetime('now') WHERE id=? AND status='running' AND lease_owner=?")
            .bind(modifier)
            .bind(id)
            .bind(lease_owner)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn complete(&self, id: i64, lease_owner: &str) -> sqlx::Result<bool> {
        let result = sqlx::query("UPDATE ingestion_job SET status='succeeded',lease_owner=NULL,lease_expires_at=NULL,last_error=NULL,finished_at=datetime('now'),updated_at=datetime('now') WHERE id=? AND status='running' AND lease_owner=?")
            .bind(id)
            .bind(lease_owner)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn fail(
        &self,
        id: i64,
        lease_owner: &str,
        error: &str,
        retry_delay: Duration,
    ) -> sqlx::Result<bool> {
        let modifier = format!("+{} seconds", retry_delay.as_secs());
        let result = sqlx::query("UPDATE ingestion_job SET status=CASE WHEN attempts<max_attempts THEN 'pending' ELSE 'failed' END,available_at=CASE WHEN attempts<max_attempts THEN datetime('now',?) ELSE available_at END,lease_owner=NULL,lease_expires_at=NULL,last_error=?,finished_at=CASE WHEN attempts<max_attempts THEN NULL ELSE datetime('now') END,updated_at=datetime('now') WHERE id=? AND status='running' AND lease_owner=?")
            .bind(modifier)
            .bind(error)
            .bind(id)
            .bind(lease_owner)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 1 {
            self.notify.notify_one();
        }
        Ok(result.rows_affected() == 1)
    }

    pub async fn recover_startup(&self) -> sqlx::Result<u64> {
        let result = sqlx::query("UPDATE ingestion_job SET status=CASE WHEN attempts<max_attempts THEN 'pending' ELSE 'failed' END,available_at=datetime('now'),lease_owner=NULL,lease_expires_at=NULL,last_error='worker interrupted by service restart',finished_at=CASE WHEN attempts<max_attempts THEN NULL ELSE datetime('now') END,updated_at=datetime('now') WHERE status='running'")
            .execute(&self.pool)
            .await?;
        if result.rows_affected() > 0 {
            self.notify.notify_waiters();
        }
        Ok(result.rows_affected())
    }

    pub async fn recover_expired(&self) -> sqlx::Result<u64> {
        let result = sqlx::query("UPDATE ingestion_job SET status=CASE WHEN attempts<max_attempts THEN 'pending' ELSE 'failed' END,available_at=datetime('now'),lease_owner=NULL,lease_expires_at=NULL,last_error='worker lease expired',finished_at=CASE WHEN attempts<max_attempts THEN NULL ELSE datetime('now') END,updated_at=datetime('now') WHERE status='running' AND (lease_expires_at IS NULL OR lease_expires_at<=datetime('now'))")
            .execute(&self.pool)
            .await?;
        if result.rows_affected() > 0 {
            self.notify.notify_waiters();
        }
        Ok(result.rows_affected())
    }

    pub async fn wait_for_work(&self, timeout: Duration) {
        tokio::select! {
            _ = self.notify.notified() => {}
            _ = tokio::time::sleep(timeout) => {}
        }
    }
}

fn job_from_row(row: SqliteRow) -> anyhow::Result<IngestionJob> {
    let payload_json: String = row.get("payload_json");
    Ok(IngestionJob {
        id: row.get("id"),
        provider_key: row.get("provider_key"),
        job_type: row.get("job_type"),
        priority: row.get("priority"),
        payload: serde_json::from_str(&payload_json)?,
        attempts: row.get("attempts"),
        max_attempts: row.get("max_attempts"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn queue() -> IngestionQueue {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        IngestionQueue::new(pool)
    }

    #[tokio::test]
    async fn highest_priority_ready_job_is_claimed_first() {
        let queue = queue().await;
        let low = queue
            .enqueue(EnqueueJob {
                provider_key: "javdb",
                job_type: "bootstrap",
                priority: PRIORITY_HISTORICAL_BOOTSTRAP,
                payload: serde_json::json!({}),
                max_attempts: 3,
                dedupe_key: Some("test:bootstrap"),
            })
            .await
            .unwrap();
        let high = queue
            .enqueue(EnqueueJob {
                provider_key: "javbus",
                job_type: "on_demand",
                priority: PRIORITY_USER_ON_DEMAND,
                payload: serde_json::json!({}),
                max_attempts: 3,
                dedupe_key: Some("test:on-demand"),
            })
            .await
            .unwrap();
        assert_ne!(low.id, high.id);

        let claimed = queue
            .claim("worker-a", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.id, high.id);
        assert_eq!(claimed.priority, PRIORITY_USER_ON_DEMAND);
    }

    #[tokio::test]
    async fn startup_recovery_requeues_an_interrupted_lease() {
        let queue = queue().await;
        let enqueued = queue
            .enqueue(EnqueueJob {
                provider_key: "javdb",
                job_type: "hydrate",
                priority: PRIORITY_DAILY_INCREMENTAL,
                payload: serde_json::json!({ "providerEntityId": "ABC-123" }),
                max_attempts: 3,
                dedupe_key: Some("test:hydrate:ABC-123"),
            })
            .await
            .unwrap();
        let first = queue
            .claim("worker-a", Duration::from_secs(600))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.id, enqueued.id);
        assert_eq!(first.attempts, 1);

        assert_eq!(queue.recover_startup().await.unwrap(), 1);
        let resumed = queue
            .claim("worker-b", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resumed.id, enqueued.id);
        assert_eq!(resumed.attempts, 2);
    }

    #[tokio::test]
    async fn duplicate_active_job_is_promoted_instead_of_inserted() {
        let queue = queue().await;
        let first = queue
            .enqueue(EnqueueJob {
                provider_key: "javdb",
                job_type: "source_sync",
                priority: PRIORITY_DAILY_INCREMENTAL,
                payload: serde_json::json!({ "manual": false }),
                max_attempts: 3,
                dedupe_key: Some("source-sync:javdb"),
            })
            .await
            .unwrap();
        let second = queue
            .enqueue(EnqueueJob {
                provider_key: "javdb",
                job_type: "source_sync",
                priority: PRIORITY_USER_ON_DEMAND,
                payload: serde_json::json!({ "manual": true }),
                max_attempts: 3,
                dedupe_key: Some("source-sync:javdb"),
            })
            .await
            .unwrap();
        assert_eq!(first.id, second.id);
        assert!(!second.inserted);
        let claimed = queue
            .claim("worker-a", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claimed.priority, PRIORITY_USER_ON_DEMAND);
        assert_eq!(claimed.payload["manual"], true);
    }

    #[tokio::test]
    async fn reserved_worker_does_not_claim_historical_bootstrap() {
        let queue = queue().await;
        queue
            .enqueue(EnqueueJob {
                provider_key: "javdb",
                job_type: "discovery",
                priority: PRIORITY_HISTORICAL_BOOTSTRAP,
                payload: serde_json::json!({}),
                max_attempts: 3,
                dedupe_key: Some("test:historical"),
            })
            .await
            .unwrap();
        assert!(
            queue
                .claim_at_or_above(
                    "reserved-worker",
                    Duration::from_secs(60),
                    PRIORITY_RECENT_REPAIR,
                )
                .await
                .unwrap()
                .is_none()
        );
    }
}
