use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    path::Path,
};

use tokio::task;
use walkdir::WalkDir;

use crate::{AppState, models::Folder, storage};

const MEDIA_EXTENSIONS: &[&str] = &[
    "mkv", "mp4", "avi", "mov", "m4v", "wmv", "flv", "webm", "ts", "iso",
];

pub async fn run_scan(state: AppState, folder: Folder, task_id: i64) {
    let pool = &state.pool;
    let _ = sqlx::query("UPDATE scrape_task SET status = 'running', progress = 5 WHERE id = ?")
        .bind(task_id)
        .execute(pool)
        .await;

    let scan_path = folder.path.clone();
    let files = task::spawn_blocking(move || collect_media_files(&scan_path)).await;
    let files = match files {
        Ok(Ok(files)) => files,
        Ok(Err(message)) => {
            fail_task(state, task_id, &message).await;
            return;
        }
        Err(error) => {
            fail_task(state, task_id, &format!("scanner worker failed: {error}")).await;
            return;
        }
    };

    let total = files.len().max(1);
    for (index, file) in files.iter().enumerate() {
        let filename = file
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown");
        let title = Path::new(filename)
            .file_stem()
            .and_then(|name| name.to_str())
            .unwrap_or(filename)
            .replace(['.', '_'], " ");
        let mut hasher = DefaultHasher::new();
        file.hash(&mut hasher);
        if let Ok(metadata) = std::fs::metadata(file) {
            metadata.len().hash(&mut hasher);
        }
        let hash = format!("{:016x}", hasher.finish());

        if let Err(error) = sqlx::query(
            "INSERT INTO media_item (folder_id, path, filename, hash, title, media_type) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT(path) DO UPDATE SET folder_id = excluded.folder_id, filename = excluded.filename, \
             hash = excluded.hash, media_type = excluded.media_type, updated_at = datetime('now')",
        )
        .bind(folder.id)
        .bind(file.to_string_lossy().to_string())
        .bind(filename)
        .bind(hash)
        .bind(title)
        .bind(&folder.media_type)
        .execute(pool)
        .await
        {
            tracing::warn!(%error, path = %file.display(), "failed to index media item");
        }

        let progress = 10 + (((index + 1) * 85 / total) as i64);
        let _ =
            sqlx::query("UPDATE scrape_task SET progress = ? WHERE id = ? AND status = 'running'")
                .bind(progress)
                .bind(task_id)
                .execute(pool)
                .await;
    }

    let _ = sqlx::query(
        "UPDATE scrape_task SET status = 'success', progress = 100, finished_at = datetime('now') \
         WHERE id = ? AND status = 'running'",
    )
    .bind(task_id)
    .execute(pool)
    .await;
    storage::log(
        pool,
        "info",
        "scanner",
        &format!(
            "Scanned {} and found {} media files",
            folder.name,
            files.len()
        ),
    )
    .await;
}

fn collect_media_files(path: &str) -> Result<Vec<std::path::PathBuf>, String> {
    if !Path::new(path).exists() {
        return Err(format!("folder path does not exist: {path}"));
    }
    let files = WalkDir::new(path)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| {
            if !entry.file_type().is_dir() {
                return true;
            }
            let name = entry.file_name().to_string_lossy();
            !matches!(
                name.as_ref(),
                ".git" | "node_modules" | "target" | "@eaDir" | "$RECYCLE.BIN"
            )
        })
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|extension| extension.to_str())
                .map(|extension| {
                    MEDIA_EXTENSIONS.contains(&extension.to_ascii_lowercase().as_str())
                })
                .unwrap_or(false)
        })
        .map(|entry| entry.into_path())
        .collect();
    Ok(files)
}

async fn fail_task(state: AppState, task_id: i64, message: &str) {
    let _ = sqlx::query(
        "UPDATE scrape_task SET status = 'failed', error_message = ?, finished_at = datetime('now') WHERE id = ?",
    )
    .bind(message)
    .bind(task_id)
    .execute(&state.pool)
    .await;
    storage::log(&state.pool, "error", "scanner", message).await;
}
