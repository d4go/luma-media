use std::time::Duration;

use serde_json::Value;

use crate::AppState;

use super::IngestionJob;

const WORKER_COUNT: usize = 2;
const LEASE_DURATION: Duration = Duration::from_secs(120);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

pub fn start(state: AppState) {
    tokio::spawn(async move {
        if let Err(error) = crate::product::recover_interrupted_source_syncs(&state).await {
            tracing::error!(%error, "could not recover interrupted source sync state");
            return;
        }
        match state.ingestion_queue.recover_startup().await {
            Ok(count) if count > 0 => {
                tracing::warn!(
                    count,
                    "requeued ingestion jobs interrupted by service restart"
                )
            }
            Ok(_) => {}
            Err(error) => {
                tracing::error!(%error, "could not recover interrupted ingestion jobs");
                return;
            }
        }
        for index in 0..WORKER_COUNT {
            let worker_state = state.clone();
            tokio::spawn(async move {
                run_worker(worker_state, index).await;
            });
        }
    });
}

async fn run_worker(state: AppState, index: usize) {
    let owner = format!(
        "luma-{}-{index}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    );
    loop {
        let job = match state.ingestion_queue.claim(&owner, LEASE_DURATION).await {
            Ok(Some(job)) => job,
            Ok(None) => {
                state
                    .ingestion_queue
                    .wait_for_work(Duration::from_secs(1))
                    .await;
                continue;
            }
            Err(error) => {
                tracing::error!(%error, worker = owner, "ingestion worker could not claim a job");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        run_claimed_job(&state, &owner, job).await;
    }
}

async fn run_claimed_job(state: &AppState, owner: &str, job: IngestionJob) {
    tracing::info!(
        job_id = job.id,
        job_type = job.job_type,
        provider = job.provider_key,
        attempt = job.attempts,
        "ingestion job started"
    );
    let mut execution = Box::pin(execute(state, &job));
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    let outcome = loop {
        tokio::select! {
            outcome = &mut execution => break outcome,
            _ = heartbeat.tick() => {
                match state.ingestion_queue.renew(job.id, owner, LEASE_DURATION).await {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::error!(job_id = job.id, worker = owner, "ingestion worker lost its lease; cancelling local execution");
                        return;
                    }
                    Err(error) => tracing::warn!(%error, job_id = job.id, worker = owner, "ingestion lease heartbeat failed"),
                }
            }
        }
    };
    match outcome {
        Ok(()) => match state.ingestion_queue.complete(job.id, owner).await {
            Ok(true) => tracing::info!(job_id = job.id, "ingestion job completed"),
            Ok(false) => tracing::error!(
                job_id = job.id,
                "completed ingestion job no longer owned by worker"
            ),
            Err(error) => {
                tracing::error!(%error, job_id = job.id, "could not complete ingestion job")
            }
        },
        Err(error) => {
            let delay = retry_delay(job.attempts);
            tracing::warn!(%error, job_id = job.id, retry_seconds = delay.as_secs(), "ingestion job failed");
            if let Err(persist_error) = state
                .ingestion_queue
                .fail(job.id, owner, &error.to_string(), delay)
                .await
            {
                tracing::error!(%persist_error, job_id = job.id, "could not persist ingestion job failure");
            }
        }
    }
}

async fn execute(state: &AppState, job: &IngestionJob) -> anyhow::Result<()> {
    match job.job_type.as_str() {
        "source_sync" => {
            let manual = job
                .payload
                .get("manual")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            crate::product::execute_source_sync_job(state, &job.provider_key, manual, job.id).await
        }
        job_type => anyhow::bail!("unsupported ingestion job type: {job_type}"),
    }
}

fn retry_delay(attempts: i64) -> Duration {
    match attempts {
        0 | 1 => Duration::from_secs(60),
        2 => Duration::from_secs(5 * 60),
        _ => Duration::from_secs(30 * 60),
    }
}
