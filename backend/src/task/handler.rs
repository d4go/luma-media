use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use serde_json::Value;

use crate::AppState;

use super::engine::TaskEngine;
use super::model::{JobItemStatus, JobStatus};

/// Runtime context handed to a JobHandler for one atomic item execution.
pub struct JobContext<'a> {
    pub state: &'a AppState,
    pub engine: &'a TaskEngine,
    pub owner: &'a str,
    pub run_id: i64,
    pub item_id: i64,
}

impl JobContext<'_> {
    pub async fn should_pause(&self) -> anyhow::Result<bool> {
        let run = self.engine.run_by_id(self.run_id).await?;
        Ok(matches!(run.status, JobStatus::Pausing | JobStatus::Paused))
    }

    pub async fn is_cancelled(&self) -> anyhow::Result<bool> {
        let run = self.engine.run_by_id(self.run_id).await?;
        Ok(matches!(
            run.status,
            JobStatus::Cancelling | JobStatus::Cancelled
        ))
    }

    pub async fn run_config(&self) -> anyhow::Result<Value> {
        Ok(self.engine.run_by_id(self.run_id).await?.config)
    }

    pub async fn item_checkpoint(&self) -> anyhow::Result<Value> {
        Ok(self.engine.item_by_id(self.item_id).await?.checkpoint)
    }

    pub async fn report_progress(
        &self,
        current: i64,
        total: Option<i64>,
        checkpoint: Value,
    ) -> anyhow::Result<()> {
        self.engine
            .report_progress(self.run_id, current, total, checkpoint)
            .await
    }

    pub async fn save_checkpoint(&self, checkpoint: Value) -> anyhow::Result<()> {
        let run = self.engine.run_by_id(self.run_id).await?;
        self.engine
            .report_progress(self.run_id, run.progress_current, run.progress_total, checkpoint)
            .await
    }

    pub async fn mark_item_success(&self, checkpoint: Value) -> anyhow::Result<()> {
        self.engine
            .finish_item(self.item_id, self.owner, JobItemStatus::Success, None, checkpoint)
            .await?;
        Ok(())
    }

    pub async fn mark_item_failed(&self, error: &str, checkpoint: Value) -> anyhow::Result<()> {
        self.engine
            .finish_item(
                self.item_id,
                self.owner,
                JobItemStatus::Failed,
                Some(error),
                checkpoint,
            )
            .await?;
        Ok(())
    }

    pub async fn mark_item_skipped(&self, reason: &str) -> anyhow::Result<()> {
        self.engine
            .finish_item(
                self.item_id,
                self.owner,
                JobItemStatus::Skipped,
                Some(reason),
                Value::Null,
            )
            .await?;
        Ok(())
    }

    pub async fn retry_item(&self, error: &str, delay: Duration) -> anyhow::Result<()> {
        self.engine
            .retry_item(self.item_id, self.owner, delay, Some(error))
            .await?;
        Ok(())
    }

    pub async fn log(&self, event_key: &str, message: &str, payload: Value) -> anyhow::Result<()> {
        self.engine
            .record_event(self.run_id, event_key, message, payload)
            .await
    }
}

#[async_trait]
pub trait JobHandler: Send + Sync {
    fn job_type(&self) -> &'static str;

    /// Execute one atomic item. Return Ok(()) when the item is done;
    /// Err(error) when it failed (the runner applies the retry policy).
    async fn run(&self, ctx: &mut JobContext<'_>) -> anyhow::Result<()>;
}

#[derive(Default)]
pub struct HandlerRegistry {
    handlers: Vec<Arc<dyn JobHandler>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, handler: Arc<dyn JobHandler>) {
        self.handlers.push(handler);
    }

    pub fn get(&self, job_type: &str) -> Option<Arc<dyn JobHandler>> {
        self.handlers
            .iter()
            .find(|handler| handler.job_type() == job_type)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StubHandler;

    #[async_trait]
    impl JobHandler for StubHandler {
        fn job_type(&self) -> &'static str {
            "stub"
        }

        async fn run(&self, _ctx: &mut JobContext<'_>) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn registry_dispatches_by_job_type() {
        let mut registry = HandlerRegistry::new();
        registry.register(Arc::new(StubHandler));
        assert!(registry.get("stub").is_some());
        assert!(registry.get("bootstrap").is_none());
    }
}
