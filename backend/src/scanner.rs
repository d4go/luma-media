use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    path::Path,
};

use serde_json::json;
use tokio::task;
use walkdir::WalkDir;

use crate::{AppState, models::Folder, storage};

const MEDIA_EXTENSIONS: &[&str] = &[
    "mkv", "mp4", "avi", "mov", "m4v", "wmv", "flv", "webm", "ts", "iso",
];

pub async fn run_scan(state: AppState, folder: Folder, task_id: i64, record_id: i64) {
    let pool = &state.pool;
    if !matches!(
        storage::start_task_run(pool, task_id, record_id, 5).await,
        Ok(true)
    ) {
        return;
    }

    let scan_path = folder.path.clone();
    let files = task::spawn_blocking(move || collect_media_files(&scan_path)).await;
    let files = match files {
        Ok(Ok(files)) => files,
        Ok(Err(message)) => {
            fail_task(state, task_id, record_id, &message).await;
            return;
        }
        Err(error) => {
            fail_task(
                state,
                task_id,
                record_id,
                &format!("scanner worker failed: {error}"),
            )
            .await;
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
        let local_metadata = detect_local_metadata(file);

        let media_id = match sqlx::query_scalar::<_, i64>(
            "INSERT INTO media_item (folder_id, path, filename, hash, title, media_type) \
             VALUES (?, ?, ?, ?, ?, ?) \
             ON CONFLICT(path) DO UPDATE SET folder_id = excluded.folder_id, filename = excluded.filename, \
             hash = excluded.hash, media_type = excluded.media_type, updated_at = datetime('now') \
             RETURNING id",
        )
        .bind(folder.id)
        .bind(file.to_string_lossy().to_string())
        .bind(filename)
        .bind(hash)
        .bind(title)
        .bind(&folder.media_type)
        .fetch_one(pool)
        .await
        {
            Ok(media_id) => media_id,
            Err(error) => {
                tracing::warn!(%error, path = %file.display(), "failed to index media item");
                continue;
            }
        };

        if let Some(local) = local_metadata {
            let raw = json!({
                "source": "local-sidecars",
                "nfoPath": local.nfo_path.to_string_lossy(),
                "posterPath": local.poster_path.to_string_lossy(),
            });
            let mut transaction = match pool.begin().await {
                Ok(transaction) => transaction,
                Err(error) => {
                    tracing::warn!(%error, path = %file.display(), "failed to associate local metadata");
                    continue;
                }
            };
            let result = async {
                sqlx::query(
                    "UPDATE media_item SET provider_id = ?, title = COALESCE(?, title), \
                     status = 'ready', updated_at = datetime('now') WHERE id = ?",
                )
                .bind(&local.provider_id)
                .bind(local.title.as_deref())
                .bind(media_id)
                .execute(&mut *transaction)
                .await?;
                sqlx::query(
                    "DELETE FROM metadata_record WHERE media_id = ? AND provider = 'local'",
                )
                .bind(media_id)
                .execute(&mut *transaction)
                .await?;
                sqlx::query(
                    "INSERT INTO metadata_record (media_id, provider, raw_json) \
                     VALUES (?, 'local', ?)",
                )
                .bind(media_id)
                .bind(raw.to_string())
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%error, path = %file.display(), "failed to associate local metadata");
            }
        }

        let progress = 10 + (((index + 1) * 85 / total) as i64);
        let _ = storage::update_task_progress(pool, task_id, record_id, progress).await;
    }

    let _ = storage::finish_task_run(pool, task_id, record_id, "success", None).await;
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

struct LocalMetadata {
    nfo_path: std::path::PathBuf,
    poster_path: std::path::PathBuf,
    title: Option<String>,
    provider_id: String,
}

fn detect_local_metadata(media_path: &Path) -> Option<LocalMetadata> {
    let parent = media_path.parent()?;
    let stem = media_path.file_stem()?.to_str()?;
    let nfo_path = find_sidecar(parent, &[format!("{stem}.nfo")])?;
    let poster_names = ["jpg", "jpeg", "png", "webp", "avif"]
        .into_iter()
        .flat_map(|extension| {
            [
                format!("{stem}-poster.{extension}"),
                format!("{stem}-cover.{extension}"),
                format!("{stem}.{extension}"),
                format!("poster.{extension}"),
                format!("cover.{extension}"),
                format!("folder.{extension}"),
            ]
        })
        .collect::<Vec<_>>();
    let poster_path = find_sidecar(parent, &poster_names)?;
    let nfo = std::fs::read_to_string(&nfo_path).unwrap_or_default();
    let title = nfo_value(&nfo, "title").filter(|value| !value.is_empty());
    let provider_id = nfo_value(&nfo, "uniqueid")
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "local:nfo".into());
    Some(LocalMetadata {
        nfo_path,
        poster_path,
        title,
        provider_id,
    })
}

fn find_sidecar(parent: &Path, candidates: &[String]) -> Option<std::path::PathBuf> {
    std::fs::read_dir(parent)
        .ok()?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
        .find(|entry| {
            let filename = entry.file_name();
            let Some(filename) = filename.to_str() else {
                return false;
            };
            candidates
                .iter()
                .any(|candidate| filename.eq_ignore_ascii_case(candidate))
        })
        .map(|entry| entry.path())
}

fn nfo_value(document: &str, tag: &str) -> Option<String> {
    let opening = format!("<{tag}");
    let start = document.find(&opening)?;
    let content_start = document[start..].find('>')? + start + 1;
    let closing = format!("</{tag}>");
    let content_end = document[content_start..].find(&closing)? + content_start;
    Some(
        document[content_start..content_end]
            .trim()
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'"),
    )
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

async fn fail_task(state: AppState, task_id: i64, record_id: i64, message: &str) {
    let _ =
        storage::finish_task_run(&state.pool, task_id, record_id, "failed", Some(message)).await;
    storage::log(&state.pool, "error", "scanner", message).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_title_and_unique_id_from_nfo() {
        let nfo = r#"<movie><title>A &amp; B</title><uniqueid type="metatube">movie:42</uniqueid></movie>"#;
        assert_eq!(nfo_value(nfo, "title").as_deref(), Some("A & B"));
        assert_eq!(nfo_value(nfo, "uniqueid").as_deref(), Some("movie:42"));
    }

    #[test]
    fn detects_complete_local_sidecars_case_insensitively() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "luma-scanner-sidecars-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let media = directory.join("Example.mkv");
        std::fs::write(&media, []).unwrap();
        std::fs::write(
            directory.join("EXAMPLE.NFO"),
            "<movie><title>Local Movie</title></movie>",
        )
        .unwrap();
        std::fs::write(directory.join("POSTER.JPG"), []).unwrap();

        let local = detect_local_metadata(&media).unwrap();
        assert_eq!(local.title.as_deref(), Some("Local Movie"));
        assert_eq!(local.provider_id, "local:nfo");

        std::fs::remove_dir_all(directory).unwrap();
    }
}
