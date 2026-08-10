use std::time::Duration;

use crate::AppState;
use crate::providers::runtime::{GateBlocked, GateDecision};

use super::handler::JobHandler;
use super::model::{JobItem, JobStatus};

const RUNNER_COUNT: usize = 2;
const RUN_LEASE: Duration = Duration::from_secs(120);
const ITEM_LEASE: Duration = Duration::from_secs(300);
const ITEM_MAX_RETRIES: i64 = 3;
const ITEM_RETRY_DELAY: Duration = Duration::from_secs(60);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

pub async fn start(state: AppState) -> anyhow::Result<()> {
    let recovered = state.task_engine.recover_startup().await?;
    if recovered > 0 {
        tracing::warn!(
            recovered,
            "recovered task engine runs/items interrupted by restart"
        );
    }
    for index in 0..RUNNER_COUNT {
        let runner_state = state.clone();
        tokio::spawn(async move {
            run_loop(runner_state, index).await;
        });
    }
    Ok(())
}

async fn run_loop(state: AppState, index: usize) {
    let owner = format!(
        "task-runner-{}-{index}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    );
    loop {
        let run = match state
            .task_engine
            .claim_next_run(&owner, RUN_LEASE, 0)
            .await
        {
            Ok(Some(run)) => run,
            Ok(None) => {
                state.task_engine.wait(Duration::from_secs(1)).await;
                continue;
            }
            Err(error) => {
                tracing::error!(%error, runner = owner, "task runner could not claim a run");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        tracing::info!(
            run_id = run.id,
            job_type = run.job_type,
            runner = owner,
            "task runner claimed run"
        );
        let handler = match state.handler_registry.get(&run.job_type) {
            Some(handler) => handler,
            None => {
                let message = format!("no handler registered for job type {}", run.job_type);
                tracing::error!(run_id = run.id, "{message}");
                let _ = state
                    .task_engine
                    .release_run(run.id, &owner, JobStatus::Failed)
                    .await;
                let _ = state
                    .task_engine
                    .record_event(run.id, "handler-missing", &message, serde_json::json!({}))
                    .await;
                continue;
            }
        };
        process_run(&state, &owner, run.id, run.job_type.clone(), handler).await;
    }
}

async fn process_run(
    state: &AppState,
    owner: &str,
    run_id: i64,
    job_type: String,
    handler: std::sync::Arc<dyn JobHandler>,
) {
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                match state.task_engine.renew_run(run_id, owner, RUN_LEASE).await {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::error!(run_id, owner, "task runner lost its run lease");
                        return;
                    }
                    Err(error) => tracing::warn!(%error, run_id, "task run lease heartbeat failed"),
                }
            }
            outcome = run_one_step(state, owner, run_id, &job_type, &*handler) => {
                if outcome == StepOutcome::Stop {
                    return;
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StepOutcome {
    Continue,
    Stop,
}

async fn run_one_step(
    state: &AppState,
    owner: &str,
    run_id: i64,
    job_type: &str,
    handler: &dyn JobHandler,
) -> StepOutcome {
    let run = match state.task_engine.run_by_id(run_id).await {
        Ok(run) => run,
        Err(error) => {
            tracing::error!(%error, run_id, "could not reload run status");
            return StepOutcome::Stop;
        }
    };
    if matches!(run.status, JobStatus::Cancelling | JobStatus::Cancelled) {
        let _ = state
            .task_engine
            .transition(run_id, JobStatus::Cancelled, None)
            .await;
        let _ = state
            .task_engine
            .release_run(run_id, owner, JobStatus::Cancelled)
            .await;
        return StepOutcome::Stop;
    }
    if matches!(run.status, JobStatus::Pausing | JobStatus::Paused) {
        let _ = state
            .task_engine
            .release_run(run_id, owner, JobStatus::Paused)
            .await;
        return StepOutcome::Stop;
    }
    let item = match state
        .task_engine
        .claim_next_item(run_id, owner, ITEM_LEASE)
        .await
    {
        Ok(Some(item)) => item,
        Ok(None) => {
            let has_pending = state
                .task_engine
                .run_has_any_pending(run_id)
                .await
                .unwrap_or(false);
            let to = if has_pending {
                JobStatus::Pending
            } else {
                match state.task_engine.run_stats(run_id).await {
                    Ok(stats) if stats.failed > 0 => JobStatus::Failed,
                    _ => JobStatus::Success,
                }
            };
            let _ = state.task_engine.release_run(run_id, owner, to).await;
            return StepOutcome::Stop;
        }
        Err(error) => {
            tracing::error!(%error, run_id, "could not claim next task item");
            return StepOutcome::Stop;
        }
    };
    execute_item(state, owner, run_id, job_type, handler, item).await;
    StepOutcome::Continue
}

async fn execute_item(
    state: &AppState,
    owner: &str,
    run_id: i64,
    job_type: &str,
    handler: &dyn JobHandler,
    item: JobItem,
) {
    let mut ctx = crate::task::handler::JobContext {
        state,
        engine: &state.task_engine,
        owner,
        run_id,
        item_id: item.id,
    };
    tracing::info!(run_id, item_id = item.id, item_key = %item.item_key, job_type, "task item started");
    match handler.run(&mut ctx).await {
        Ok(()) => {
            if let Err(error) = ctx.mark_item_success(item.checkpoint.clone()).await {
                tracing::error!(%error, run_id, item_id = item.id, "could not persist item success");
            }
        }
        Err(error) => {
            let message = error.to_string();
            tracing::warn!(%error, run_id, item_id = item.id, "task item failed");
            if let Some(blocked) = error.downcast_ref::<GateBlocked>() {
                let delay = gate_defer_delay(blocked.decision);
                tracing::info!(
                    run_id,
                    item_id = item.id,
                    decision = ?blocked.decision,
                    defer_seconds = delay.as_secs(),
                    "task item deferred by provider runtime gate"
                );
                if let Err(persist_error) =
                    ctx.engine.defer_item(item.id, owner, delay, &message).await
                {
                    tracing::error!(%persist_error, run_id, item_id = item.id, "could not defer task item");
                }
                return;
            }
            if item.retry_count < ITEM_MAX_RETRIES {
                if let Err(persist_error) =
                    ctx.retry_item(&message, ITEM_RETRY_DELAY).await
                {
                    tracing::error!(%persist_error, run_id, item_id = item.id, "could not requeue task item");
                }
            } else if let Err(persist_error) =
                ctx.mark_item_failed(&message, item.checkpoint.clone()).await
            {
                tracing::error!(%persist_error, run_id, item_id = item.id, "could not persist item failure");
            }
        }
    }
}

fn gate_defer_delay(decision: GateDecision) -> Duration {
    match decision {
        GateDecision::Cooldown => Duration::from_secs(2 * 60),
        GateDecision::InteractionRequired => Duration::from_secs(15 * 60),
        GateDecision::Unavailable => Duration::from_secs(5 * 60),
        GateDecision::Allow | GateDecision::AllowDegraded => Duration::from_secs(60),
    }
}
