use sqlx::Row;

use crate::{AppState, api, storage};

pub fn start(state: AppState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if let Err(error) = schedule_due_folders(&state).await {
                tracing::warn!(%error, "folder scheduler tick failed");
            }
        }
    });
}

async fn schedule_due_folders(state: &AppState) -> anyhow::Result<()> {
    let settings = storage::load_settings(&state.pool).await?;
    let due_after = settings.scan_interval.max(1) as i64;
    let rows = sqlx::query(
        "SELECT mc.* FROM media_config mc \
         WHERE mc.enabled = 1 AND mc.scan_mode IN ('interval', 'watch') \
         AND NOT EXISTS (SELECT 1 FROM scrape_task active WHERE active.folder_id = mc.id AND active.status IN ('pending', 'running')) \
         AND (NOT EXISTS (SELECT 1 FROM scrape_task previous WHERE previous.folder_id = mc.id AND previous.task_type = 'scan') \
              OR COALESCE((SELECT (julianday('now') - julianday(MAX(previous.created_at))) * 1440 FROM scrape_task previous WHERE previous.folder_id = mc.id AND previous.task_type = 'scan'), ?) >= ?)",
    )
    .bind(due_after)
    .bind(due_after)
    .fetch_all(&state.pool)
    .await?;

    for row in rows {
        let id: i64 = row.get("id");
        let folder = storage::folder_by_id(&state.pool, id).await?;
        let task = storage::create_task(&state.pool, None, Some(id), "scan").await?;
        tokio::spawn(api::run_scan_with_auto_scrape(
            state.clone(),
            folder,
            task.id,
        ));
    }
    Ok(())
}
