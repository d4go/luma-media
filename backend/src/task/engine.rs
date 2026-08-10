use std::{sync::Arc, time::Duration};

use serde_json::{Value, json};
use sqlx::{Row, SqlitePool};
use tokio::sync::Notify;

use super::model::{
    CreateJobRun, JobItem, JobItemStatus, JobRun, JobStatus, job_item_from_row, job_run_from_row,
};

#[derive(Debug, Clone)]
pub struct TaskEngine {
    pool: SqlitePool,
    notify: Arc<Notify>,
}

impl TaskEngine {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Create a JobRun. If an active run already exists for the idempotency
    /// key, return it instead of creating a duplicate execution.
    pub async fn create_run(&self, input: CreateJobRun<'_>) -> anyhow::Result<JobRun> {
        anyhow::ensure!(!input.job_type.trim().is_empty(), "job type is required");
        anyhow::ensure!(
            !input.idempotency_key.trim().is_empty(),
            "idempotency key is required"
        );
        if let Some(row) = sqlx::query(
            "SELECT * FROM job_run WHERE idempotency_key=? AND status IN ('pending','running','pausing','paused','cancelling') ORDER BY id DESC LIMIT 1",
        )
        .bind(input.idempotency_key)
        .fetch_optional(&self.pool)
        .await?
        {
            return Ok(job_run_from_row(&row));
        }
        let config = input.config.to_string();
        let id = sqlx::query(
            "INSERT INTO job_run(job_definition_id,job_type,provider_key,status,idempotency_key,priority,checkpoint_json) VALUES (?,?,?,'pending',?,?,?) RETURNING id",
        )
        .bind(input.job_definition_id)
        .bind(input.job_type)
        .bind(input.provider_key)
        .bind(input.idempotency_key)
        .bind(input.priority)
        .bind(&config)
        .fetch_one(&self.pool)
        .await?
        .get::<i64, _>("id");
        self.notify.notify_one();
        self.run_by_id(id).await
    }

    pub async fn run_by_id(&self, id: i64) -> anyhow::Result<JobRun> {
        let row = sqlx::query("SELECT * FROM job_run WHERE id=?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| anyhow::anyhow!("job run {id} not found"))?;
        Ok(job_run_from_row(&row))
    }

    pub async fn list_runs(
        &self,
        status: Option<JobStatus>,
        page: i64,
        page_size: i64,
    ) -> anyhow::Result<(Vec<JobRun>, i64)> {
        let page = page.max(1);
        let page_size = page_size.clamp(1, 200);
        let offset = (page - 1) * page_size;
        let total: i64 = if let Some(status) = status {
            sqlx::query_scalar("SELECT COUNT(*) FROM job_run WHERE status=?")
                .bind(status.as_str())
                .fetch_one(&self.pool)
                .await?
        } else {
            sqlx::query_scalar("SELECT COUNT(*) FROM job_run")
                .fetch_one(&self.pool)
                .await?
        };
        let rows = if let Some(status) = status {
            sqlx::query(
                "SELECT * FROM job_run WHERE status=? ORDER BY priority DESC,id DESC LIMIT ? OFFSET ?",
            )
            .bind(status.as_str())
            .bind(page_size)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query("SELECT * FROM job_run ORDER BY priority DESC,id DESC LIMIT ? OFFSET ?")
                .bind(page_size)
                .bind(offset)
                .fetch_all(&self.pool)
                .await?
        };
        Ok((rows.iter().map(job_run_from_row).collect(), total))
    }

    pub async fn create_item(
        &self,
        run_id: i64,
        item_key: &str,
        checkpoint: Value,
    ) -> anyhow::Result<JobItem> {
        anyhow::ensure!(!item_key.trim().is_empty(), "item key is required");
        let checkpoint = checkpoint.to_string();
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO job_item(run_id,item_key,checkpoint_json) VALUES (?,?,?) RETURNING id",
        )
        .bind(run_id)
        .bind(item_key)
        .bind(&checkpoint)
        .fetch_optional(&self.pool)
        .await?;
        let id: i64 = if let Some(row) = inserted {
            row.get("id")
        } else {
            sqlx::query_scalar("SELECT id FROM job_item WHERE run_id=? AND item_key=?")
                .bind(run_id)
                .bind(item_key)
                .fetch_one(&self.pool)
                .await?
        };
        self.notify.notify_one();
        self.item_by_id(id).await
    }

    pub async fn item_by_id(&self, id: i64) -> anyhow::Result<JobItem> {
        let row = sqlx::query("SELECT * FROM job_item WHERE id=?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| anyhow::anyhow!("job item {id} not found"))?;
        Ok(job_item_from_row(&row))
    }

    /// Claim the next pending item of a run. Runs that are paused, cancelling
    /// or cancelled never hand out new items.
    pub async fn claim_next_item(
        &self,
        run_id: i64,
        owner: &str,
        lease_duration: Duration,
    ) -> anyhow::Result<Option<JobItem>> {
        anyhow::ensure!(!owner.is_empty(), "lease owner is required");
        let lease_modifier = format!("+{} seconds", lease_duration.as_secs().max(1));
        let row = sqlx::query(
            "UPDATE job_item SET status='running',retry_count=retry_count+1,lease_owner=?,lease_expires_at=datetime('now',?),started_at=COALESCE(started_at,datetime('now')),updated_at=datetime('now') \
             WHERE id=(SELECT ji.id FROM job_item ji JOIN job_run jr ON jr.id=ji.run_id WHERE ji.run_id=? AND ji.status='pending' AND jr.status IN ('pending','running') ORDER BY ji.id LIMIT 1) RETURNING *",
        )
        .bind(owner)
        .bind(lease_modifier)
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| job_item_from_row(&row)))
    }

    /// Finish an item and refresh the run progress counters.
    pub async fn finish_item(
        &self,
        item_id: i64,
        owner: &str,
        status: JobItemStatus,
        error_message: Option<&str>,
        checkpoint: Value,
    ) -> anyhow::Result<bool> {
        let terminal = matches!(
            status,
            JobItemStatus::Success | JobItemStatus::Failed | JobItemStatus::Skipped | JobItemStatus::Cancelled
        );
        let result = sqlx::query(
            "UPDATE job_item SET status=?,error_message=?,checkpoint_json=?,lease_owner=NULL,lease_expires_at=NULL,finished_at=CASE WHEN ?=1 THEN datetime('now') ELSE finished_at END,updated_at=datetime('now') WHERE id=? AND lease_owner=?",
        )
        .bind(status.as_str())
        .bind(error_message)
        .bind(checkpoint.to_string())
        .bind(terminal as i64)
        .bind(item_id)
        .bind(owner)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            let run_id: i64 = sqlx::query_scalar("SELECT run_id FROM job_item WHERE id=?")
                .bind(item_id)
                .fetch_one(&self.pool)
                .await?;
            sqlx::query(
                "UPDATE job_run SET progress_current=(SELECT COUNT(*) FROM job_item WHERE run_id=? AND status IN ('success','failed','skipped','cancelled')),updated_at=datetime('now') WHERE id=?",
            )
            .bind(run_id)
            .bind(run_id)
            .execute(&self.pool)
            .await?;
        }
        Ok(result.rows_affected() == 1)
    }

    pub async fn report_progress(
        &self,
        run_id: i64,
        current: i64,
        total: Option<i64>,
        checkpoint: Value,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE job_run SET progress_current=?,progress_total=COALESCE(?,progress_total),checkpoint_json=?,updated_at=datetime('now') WHERE id=?",
        )
        .bind(current)
        .bind(total)
        .bind(checkpoint.to_string())
        .bind(run_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn transition(
        &self,
        run_id: i64,
        to: JobStatus,
        error_message: Option<&str>,
    ) -> anyhow::Result<JobRun> {
        let from: String =
            sqlx::query_scalar("SELECT status FROM job_run WHERE id=?")
                .bind(run_id)
                .fetch_one(&self.pool)
                .await?;
        let result = sqlx::query(
            "UPDATE job_run SET status=?,error_message=?,started_at=CASE WHEN ?='running' AND started_at IS NULL THEN datetime('now') ELSE started_at END,finished_at=CASE WHEN ? IN ('success','failed','cancelled') THEN datetime('now') ELSE NULL END,updated_at=datetime('now') WHERE id=?",
        )
        .bind(to.as_str())
        .bind(error_message)
        .bind(to.as_str())
        .bind(to.as_str())
        .bind(run_id)
        .execute(&self.pool)
        .await?;
        anyhow::ensure!(
            result.rows_affected() == 1,
            "job run {run_id} no longer exists"
        );
        if to == JobStatus::Cancelled {
            sqlx::query(
                "UPDATE job_item SET status='cancelled',error_message='run cancelled',lease_owner=NULL,lease_expires_at=NULL,finished_at=datetime('now'),updated_at=datetime('now') WHERE run_id=? AND status IN ('pending','running')",
            )
            .bind(run_id)
            .execute(&self.pool)
            .await?;
        }
        self.record_event(
            run_id,
            &format!("status-{}", to.as_str()),
            &format!("{from} → {}", to.as_str()),
            json!({}),
        )
        .await?;
        self.notify.notify_one();
        self.run_by_id(run_id).await
    }

    pub async fn record_event(
        &self,
        run_id: i64,
        event_key: &str,
        message: &str,
        payload: Value,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO job_event(run_id,event_key,message,payload_json) VALUES (?,?,?,?)",
        )
        .bind(run_id)
        .bind(event_key)
        .bind(message)
        .bind(payload.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Recover runs/items left in an in-progress state after a restart.
    pub async fn recover_startup(&self) -> anyhow::Result<usize> {
        let runs = sqlx::query(
            "UPDATE job_run SET status='pending',error_message='worker interrupted by service restart',updated_at=datetime('now') WHERE status IN ('running','pausing','cancelling')",
        )
        .execute(&self.pool)
        .await?
        .rows_affected();
        let items = sqlx::query(
            "UPDATE job_item SET status='pending',lease_owner=NULL,lease_expires_at=NULL,updated_at=datetime('now') WHERE status='running'",
        )
        .execute(&self.pool)
        .await?
        .rows_affected();
        if runs > 0 || items > 0 {
            self.notify.notify_waiters();
        }
        Ok((runs + items) as usize)
    }

    pub async fn wait(&self, timeout: Duration) {
        tokio::select! {
            _ = self.notify.notified() => {}
            _ = tokio::time::sleep(timeout) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};

    use super::*;

    async fn engine() -> TaskEngine {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        TaskEngine::new(pool)
    }

    fn input(key: &str) -> CreateJobRun<'_> {
        CreateJobRun {
            job_type: "bootstrap",
            provider_key: Some("javdb"),
            idempotency_key: key,
            priority: 100,
            config: json!({ "from": "2020-01-01" }),
            job_definition_id: None,
        }
    }

    #[tokio::test]
    async fn create_run_is_idempotent_by_key() {
        let engine = engine().await;
        let first = engine.create_run(input("bootstrap:javdb:2020")).await.unwrap();
        let second = engine.create_run(input("bootstrap:javdb:2020")).await.unwrap();
        assert_eq!(first.id, second.id);
        assert_eq!(first.status, JobStatus::Pending);
    }

    #[tokio::test]
    async fn claim_finish_item_advances_run_progress() {
        let engine = engine().await;
        let run = engine.create_run(input("bootstrap:javdb:2021")).await.unwrap();
        for key in ["page-1", "page-2"] {
            engine.create_item(run.id, key, json!({})).await.unwrap();
        }
        let item = engine
            .claim_next_item(run.id, "worker-a", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.item_key, "page-1");
        assert!(engine
            .finish_item(item.id, "worker-a", JobItemStatus::Success, None, json!({}))
            .await
            .unwrap());
        let refreshed = engine.run_by_id(run.id).await.unwrap();
        assert_eq!(refreshed.progress_current, 1);
    }

    #[tokio::test]
    async fn paused_run_stops_handing_out_items_and_resumes() {
        let engine = engine().await;
        let run = engine.create_run(input("bootstrap:javdb:2022")).await.unwrap();
        engine.create_item(run.id, "page-1", json!({})).await.unwrap();
        engine
            .transition(run.id, JobStatus::Paused, None)
            .await
            .unwrap();
        assert!(
            engine
                .claim_next_item(run.id, "worker-a", Duration::from_secs(60))
                .await
                .unwrap()
                .is_none()
        );
        engine
            .transition(run.id, JobStatus::Pending, None)
            .await
            .unwrap();
        assert!(
            engine
                .claim_next_item(run.id, "worker-a", Duration::from_secs(60))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn cancel_marks_pending_items_cancelled() {
        let engine = engine().await;
        let run = engine.create_run(input("bootstrap:javdb:2023")).await.unwrap();
        engine.create_item(run.id, "page-1", json!({})).await.unwrap();
        engine
            .transition(run.id, JobStatus::Cancelled, None)
            .await
            .unwrap();
        let item = engine.item_by_id(
            sqlx::query_scalar("SELECT id FROM job_item WHERE run_id=?")
                .bind(run.id)
                .fetch_one(engine.pool())
                .await
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(item.status, JobItemStatus::Cancelled);
    }

    #[tokio::test]
    async fn startup_recovery_requeues_interrupted_work() {
        let engine = engine().await;
        let run = engine.create_run(input("bootstrap:javdb:2024")).await.unwrap();
        engine.create_item(run.id, "page-1", json!({})).await.unwrap();
        engine
            .transition(run.id, JobStatus::Running, None)
            .await
            .unwrap();
        let _claimed = engine
            .claim_next_item(run.id, "worker-a", Duration::from_secs(600))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(engine.recover_startup().await.unwrap(), 2);
        let run = engine.run_by_id(run.id).await.unwrap();
        assert_eq!(run.status, JobStatus::Pending);
        assert!(
            engine
                .claim_next_item(run.id, "worker-b", Duration::from_secs(60))
                .await
                .unwrap()
                .is_some()
        );
    }
}
