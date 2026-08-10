use std::{collections::HashMap, path::PathBuf, time::Instant};

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use sqlx::Row;

use crate::{AppState, api, storage};

pub fn start(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(error) = watch_loop(state).await {
            tracing::error!(%error, "media directory watcher stopped");
        }
    })
}

async fn watch_loop(state: AppState) -> anyhow::Result<()> {
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut watcher: RecommendedWatcher = notify::recommended_watcher(move |event| {
        let _ = event_tx.send(event);
    })?;
    let mut watched: HashMap<PathBuf, i64> = HashMap::new();
    let mut pending: HashMap<i64, Instant> = HashMap::new();
    let mut refresh = tokio::time::interval(std::time::Duration::from_secs(10));
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut debounce = tokio::time::interval(std::time::Duration::from_millis(500));
    debounce.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = refresh.tick() => {
                reconcile(&state, &mut watcher, &mut watched).await;
            }
            Some(event) = event_rx.recv() => {
                let event = match event {
                    Ok(event) => event,
                    Err(error) => {
                        tracing::warn!(%error, "media watcher event error");
                        continue;
                    }
                };
                if !is_change(&event) {
                    continue;
                }
                let folder_id = event.paths.iter().filter_map(|event_path| {
                    watched.iter()
                        .filter(|(root, _)| event_path.starts_with(root))
                        .max_by_key(|(root, _)| root.components().count())
                        .map(|(_, id)| *id)
                }).next();
                let Some(folder_id) = folder_id else { continue; };
                pending.insert(folder_id, Instant::now() + std::time::Duration::from_secs(2));
            }
            _ = debounce.tick() => {
                let now = Instant::now();
                let due = pending.iter()
                    .filter(|(_, deadline)| **deadline <= now)
                    .map(|(folder_id, _)| *folder_id)
                    .collect::<Vec<_>>();
                for folder_id in due {
                    pending.remove(&folder_id);
                    if let Err(error) = queue_watch_scan(&state, folder_id).await {
                        tracing::warn!(%error, folder_id, "could not queue watcher scan");
                    }
                }
            }
        }
    }
}

async fn reconcile(
    state: &AppState,
    watcher: &mut RecommendedWatcher,
    watched: &mut HashMap<PathBuf, i64>,
) {
    let rows = match sqlx::query(
        "SELECT id, path FROM media_config WHERE enabled = 1 AND scan_mode = 'watch'",
    )
    .fetch_all(&state.pool)
    .await
    {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(%error, "could not refresh watched media directories");
            return;
        }
    };
    let desired = rows
        .into_iter()
        .map(|row| {
            (
                PathBuf::from(row.get::<String, _>("path")),
                row.get::<i64, _>("id"),
            )
        })
        .collect::<HashMap<_, _>>();

    let removed = watched
        .keys()
        .filter(|path| !desired.contains_key(*path))
        .cloned()
        .collect::<Vec<_>>();
    for path in removed {
        if let Err(error) = watcher.unwatch(&path) {
            tracing::warn!(%error, path = %path.display(), "could not remove media directory watch");
        }
        watched.remove(&path);
    }
    for (path, folder_id) in desired {
        if watched.contains_key(&path) {
            continue;
        }
        match watcher.watch(&path, RecursiveMode::Recursive) {
            Ok(()) => {
                tracing::info!(folder_id, path = %path.display(), "watching media directory");
                watched.insert(path, folder_id);
            }
            Err(error) => {
                tracing::warn!(%error, folder_id, path = %path.display(), "could not watch media directory")
            }
        }
    }
}

async fn queue_watch_scan(state: &AppState, folder_id: i64) -> anyhow::Result<()> {
    let active: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM scrape_task WHERE folder_id = ? AND task_type = 'scan' AND status IN ('pending', 'running'))",
    )
    .bind(folder_id)
    .fetch_one(&state.pool)
    .await?;
    if active != 0 {
        return Ok(());
    }
    let folder = storage::folder_by_id(&state.pool, folder_id).await?;
    let run = storage::create_task_run(&state.pool, None, Some(folder_id), "scan").await?;
    tokio::spawn(api::run_scan_with_auto_scrape(
        state.clone(),
        folder,
        run.task.id,
        run.record_id,
    ));
    Ok(())
}

fn is_change(event: &Event) -> bool {
    matches!(
        event.kind,
        EventKind::Any | EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    )
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use sqlx::sqlite::SqlitePoolOptions;
    use tokio::sync::Semaphore;

    use super::*;

    #[tokio::test]
    async fn watch_mode_indexes_new_media_file() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("luma-watcher-{}-{nonce}", std::process::id()));
        let media_root = root.join("media");
        tokio::fs::create_dir_all(&media_root).await.unwrap();
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO media_config (name, path, media_type, scan_mode, enabled) VALUES ('Watch', ?, 'movie', 'watch', 1)",
        )
        .bind(media_root.to_string_lossy().to_string())
        .execute(&pool)
        .await
        .unwrap();
        let state = AppState {
            pool: pool.clone(),
            scrape_limiter: Arc::new(Semaphore::new(1)),
            crawler_limiter: Arc::new(Semaphore::new(1)),
            asset_root: root.join("assets"),
            script_root: root.join("crawlers"),
            events: tokio::sync::broadcast::channel(32).0,
            fetch_manager: Arc::new(crate::fetch::FetchManager::default()),
            provider_registry: Arc::new(crate::providers::ProviderRegistry::default()),
            snapshot_repository: Arc::new(crate::ingestion::SnapshotRepository::new(
                pool.clone(),
                root.join("source-cache"),
            )),
        };
        let watcher = start(state);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        tokio::fs::write(media_root.join("new-movie.mp4"), b"video")
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(8);
        loop {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_item")
                .fetch_one(&pool)
                .await
                .unwrap();
            if count == 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "watcher did not index the new file"
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        tokio::fs::remove_file(media_root.join("new-movie.mp4"))
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(8);
        loop {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM media_item")
                .fetch_one(&pool)
                .await
                .unwrap();
            if count == 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "watcher did not remove the deleted file"
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        watcher.abort();
        pool.close().await;
        std::fs::remove_dir_all(root).unwrap();
    }
}
