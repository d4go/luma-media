use sqlx::Row;

use crate::{
    AppState, api, crawler,
    qbittorrent::{QBittorrentClient, parse_tracker_list},
    storage,
};

pub fn start(state: AppState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Leave time for the first settings request to infer the deployment host.
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(error) = schedule_due_folders(&state).await {
                tracing::warn!(%error, "folder scheduler tick failed");
            }
            if let Err(error) = crawler::schedule_due(&state).await {
                tracing::warn!(%error, "crawler scheduler tick failed");
            }
            if let Err(error) = schedule_tracker_update(&state).await {
                tracing::warn!(%error, "tracker scheduler tick failed");
            }
        }
    });
}

async fn schedule_tracker_update(state: &AppState) -> anyhow::Result<()> {
    let settings = storage::load_settings(&state.pool).await?;
    if !settings.qbittorrent_auto_update_trackers
        || settings.qbittorrent_tracker_source_url.trim().is_empty()
    {
        return Ok(());
    }
    let due: i64 = sqlx::query_scalar(
        "SELECT CASE WHEN value IS NULL OR value = '' THEN 1 \
         WHEN (julianday('now') - julianday(value)) * 1440 >= ? THEN 1 ELSE 0 END \
         FROM (SELECT (SELECT value FROM app_setting WHERE key = 'qbittorrent_tracker_last_run') AS value)",
    )
    .bind(settings.qbittorrent_tracker_update_interval.max(1) as i64)
    .fetch_one(&state.pool)
    .await?;
    if due == 0 {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO app_setting (key, value, updated_at) VALUES ('qbittorrent_tracker_last_run', datetime('now'), datetime('now')) \
         ON CONFLICT(key) DO UPDATE SET value = datetime('now'), updated_at = datetime('now')",
    )
    .execute(&state.pool)
    .await?;
    let state = state.clone();
    let retry_modifier = format!(
        "-{} minutes",
        settings
            .qbittorrent_tracker_update_interval
            .saturating_sub(5)
    );
    tokio::spawn(async move {
        if let Err(error) = update_trackers(&state).await {
            tracing::warn!(%error, "automatic tracker update failed");
            storage::log(
                &state.pool,
                "error",
                "qbittorrent",
                &format!("Automatic tracker update failed: {error}"),
            )
            .await;
            let _ = sqlx::query(
                "UPDATE app_setting SET value = datetime('now', ?), updated_at = datetime('now') \
                 WHERE key = 'qbittorrent_tracker_last_run'",
            )
            .bind(retry_modifier)
            .execute(&state.pool)
            .await;
        }
    });
    Ok(())
}

async fn update_trackers(state: &AppState) -> anyhow::Result<()> {
    let settings = storage::load_settings(&state.pool).await?;
    let response = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?
        .get(&settings.qbittorrent_tracker_source_url)
        .send()
        .await?
        .error_for_status()?;
    let trackers = parse_tracker_list(&response.text().await?);
    anyhow::ensure!(
        !trackers.is_empty(),
        "tracker source returned no valid trackers"
    );
    let updated = QBittorrentClient::new(&settings)?
        .update_all_trackers(&trackers)
        .await?;
    storage::log(
        &state.pool,
        "info",
        "qbittorrent",
        &format!("Added {} trackers to {updated} torrents", trackers.len()),
    )
    .await;
    Ok(())
}

async fn schedule_due_folders(state: &AppState) -> anyhow::Result<()> {
    let settings = storage::load_settings(&state.pool).await?;
    let due_after = settings.scan_interval.max(1) as i64;
    let rows = sqlx::query(
        "SELECT mc.* FROM media_config mc \
         WHERE mc.enabled = 1 AND mc.scan_mode IN ('interval', 'watch') \
         AND NOT EXISTS (SELECT 1 FROM scrape_task active WHERE active.folder_id = mc.id AND active.status IN ('pending', 'running')) \
         AND (NOT EXISTS (SELECT 1 FROM scrape_task previous WHERE previous.folder_id = mc.id AND previous.task_type = 'scan') \
              OR COALESCE((SELECT (julianday('now') - julianday(MAX(previous.updated_at))) * 1440 FROM scrape_task previous WHERE previous.folder_id = mc.id AND previous.task_type = 'scan'), ?) >= ?)",
    )
    .bind(due_after)
    .bind(due_after)
    .fetch_all(&state.pool)
    .await?;

    for row in rows {
        let id: i64 = row.get("id");
        let folder = storage::folder_by_id(&state.pool, id).await?;
        let run = storage::create_task_run(&state.pool, None, Some(id), "scan").await?;
        tokio::spawn(api::run_scan_with_auto_scrape(
            state.clone(),
            folder,
            run.task.id,
            run.record_id,
        ));
    }
    Ok(())
}
