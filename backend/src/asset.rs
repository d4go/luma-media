use std::{
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use axum::{
    body::Body,
    extract::{Path as AxumPath, State},
    http::{Response, header},
    response::IntoResponse,
};
use serde_json::Value;
use sqlx::Row;

use crate::{
    AppState,
    error::{AppError, AppResult},
    models::MediaItem,
    provider::{DownloadedImage, MetaTubeClient, MovieSearchResult},
    storage,
};

const PROVIDER_PRIORITY: &[&str] = &["FC2PPVDB", "JavBus", "JavLibrary", "FC2", "fc2hub"];

#[derive(Debug, Clone)]
pub struct CachedAsset {
    pub path: PathBuf,
    pub extension: String,
    pub source: String,
}

pub async fn poster(
    State(state): State<AppState>,
    AxumPath(identifier): AxumPath<String>,
) -> AppResult<impl IntoResponse> {
    let media = media_by_asset_identifier(&state, &identifier).await?;
    let local_poster = storage::local_media_resources(&media.path)
        .1
        .map(|path| CachedAsset {
            extension: path
                .extension()
                .and_then(|extension| extension.to_str())
                .unwrap_or("jpg")
                .to_ascii_lowercase(),
            path,
            source: "local".into(),
        });
    let cached = match local_poster.or(cached_poster(&state, media.id).await?) {
        Some(cached) => cached,
        None => {
            let settings = storage::load_settings(&state.pool).await?;
            let client = MetaTubeClient::new(&settings)?;
            resolve_poster(&state, &client, &media, &media.title, None)
                .await?
                .ok_or(AppError::NotFound)?
        }
    };
    asset_response(&cached).await
}

pub async fn resolve_poster(
    state: &AppState,
    client: &MetaTubeClient,
    media: &MediaItem,
    query: &str,
    initial: Option<(&MovieSearchResult, &Value)>,
) -> anyhow::Result<Option<CachedAsset>> {
    if let Some(cached) = cached_poster(state, media.id).await? {
        return Ok(Some(cached));
    }

    let initial_key = initial.map(|(result, _)| (result.provider.as_str(), result.id.as_str()));
    let initial_source = initial.map(|(result, _)| result.provider.clone());
    if let Some((result, metadata)) = initial
        && let Some(cached) = try_candidate(state, client, media, result, metadata).await?
    {
        log_success(state, media, &cached).await;
        return Ok(Some(cached));
    }

    let mut candidates = match client.search_movies(query).await {
        Ok(candidates) => candidates,
        Err(error) => {
            log_failure(
                state,
                media,
                "",
                &format!("fallback search failed: {error}"),
            )
            .await;
            return Ok(None);
        }
    };
    prefer_exact_matches(query, &mut candidates);
    candidates.sort_by_key(|candidate| provider_rank(&candidate.provider));

    for candidate in candidates {
        if initial_key.is_some_and(|key| {
            candidate.provider.eq_ignore_ascii_case(key.0) && candidate.id == key.1
        }) {
            continue;
        }
        let metadata = match client.movie_info(&candidate.provider, &candidate.id).await {
            Ok(metadata) => metadata,
            Err(error) => {
                log_failure(
                    state,
                    media,
                    "",
                    &format!("{} metadata failed: {error}", candidate.provider),
                )
                .await;
                continue;
            }
        };
        if let Some(cached) = try_candidate(state, client, media, &candidate, &metadata).await? {
            if let Some(from) = &initial_source
                && !from.eq_ignore_ascii_case(&cached.source)
            {
                storage::log(
                    &state.pool,
                    "info",
                    "asset",
                    &format!(
                        "POSTER_FALLBACK mediaId={} from={} to={}",
                        media_identifier(media),
                        from,
                        cached.source
                    ),
                )
                .await;
            }
            log_success(state, media, &cached).await;
            return Ok(Some(cached));
        }
    }

    log_failure(state, media, "", "all poster providers failed").await;
    Ok(None)
}

pub fn start_health_job(state: AppState) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(24 * 60 * 60)).await;
            if let Err(error) = check_active_posters(&state).await {
                tracing::warn!(%error, "asset health check failed");
            }
        }
    });
}

async fn check_active_posters(state: &AppState) -> anyhow::Result<()> {
    let settings = storage::load_settings(&state.pool).await?;
    let client = MetaTubeClient::new(&settings)?;
    let rows = sqlx::query(
        "SELECT id, media_id, source, url FROM media_asset \
         WHERE asset_type = 'poster' AND status = 'ACTIVE' ORDER BY id ASC",
    )
    .fetch_all(&state.pool)
    .await?;
    for row in rows {
        let asset_id: i64 = row.get("id");
        let media_id: i64 = row.get("media_id");
        let url: String = row.get("url");
        match client.download_image(&url).await {
            Ok(_) => {
                sqlx::query(
                    "UPDATE media_asset SET checked_at = datetime('now'), updated_at = datetime('now') \
                     WHERE id = ?",
                )
                .bind(asset_id)
                .execute(&state.pool)
                .await?;
            }
            Err(error) => {
                sqlx::query(
                    "UPDATE media_asset SET status = 'BROKEN', checked_at = datetime('now'), \
                     updated_at = datetime('now') WHERE id = ?",
                )
                .bind(asset_id)
                .execute(&state.pool)
                .await?;
                if let Ok(media) = storage::media_by_id(&state.pool, media_id).await {
                    log_failure(state, &media, &url, &error.to_string()).await;
                    let _ = resolve_poster(state, &client, &media, &media.title, None).await;
                }
            }
        }
    }
    Ok(())
}

async fn try_candidate(
    state: &AppState,
    client: &MetaTubeClient,
    media: &MediaItem,
    candidate: &MovieSearchResult,
    metadata: &Value,
) -> anyhow::Result<Option<CachedAsset>> {
    let Some(url) = poster_url(metadata) else {
        return Ok(None);
    };
    match client.download_image(url).await {
        Ok(image) => cache_image(state, media, &candidate.provider, url, image)
            .await
            .map(Some),
        Err(error) => {
            mark_broken(state, media.id, &candidate.provider, url).await?;
            log_failure(state, media, url, &error.to_string()).await;
            Ok(None)
        }
    }
}

async fn cached_poster(state: &AppState, media_id: i64) -> anyhow::Result<Option<CachedAsset>> {
    let row = sqlx::query(
        "SELECT id, source, url, local_path FROM media_asset \
         WHERE media_id = ? AND asset_type = 'poster' AND status = 'ACTIVE' \
         ORDER BY updated_at DESC, id DESC LIMIT 1",
    )
    .bind(media_id)
    .fetch_optional(&state.pool)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let asset_id: i64 = row.get("id");
    let local_path: Option<String> = row.get("local_path");
    let Some(local_path) = local_path else {
        mark_asset_missing(state, asset_id).await?;
        return Ok(None);
    };
    let path = PathBuf::from(local_path);
    if !path.is_file() {
        mark_asset_missing(state, asset_id).await?;
        return Ok(None);
    }
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("jpg")
        .to_ascii_lowercase();
    Ok(Some(CachedAsset {
        path,
        extension,
        source: row.get("source"),
    }))
}

async fn cache_image(
    state: &AppState,
    media: &MediaItem,
    source: &str,
    url: &str,
    image: DownloadedImage,
) -> anyhow::Result<CachedAsset> {
    let directory = state.asset_root.join("poster");
    tokio::fs::create_dir_all(&directory).await?;
    let path = directory.join(format!("{}.{}", media.id, image.extension));
    atomic_write(&path, &image.bytes).await?;

    let mut transaction = state.pool.begin().await?;
    sqlx::query(
        "UPDATE media_asset SET status = 'BROKEN', updated_at = datetime('now') \
         WHERE media_id = ? AND asset_type = 'poster' AND status = 'ACTIVE'",
    )
    .bind(media.id)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "INSERT INTO media_asset \
         (media_id, asset_type, source, url, local_path, status, checked_at, updated_at) \
         VALUES (?, 'poster', ?, ?, ?, 'ACTIVE', datetime('now'), datetime('now')) \
         ON CONFLICT(media_id, asset_type, source, url) DO UPDATE SET \
         local_path = excluded.local_path, status = 'ACTIVE', checked_at = datetime('now'), \
         updated_at = datetime('now')",
    )
    .bind(media.id)
    .bind(source)
    .bind(url)
    .bind(path.to_string_lossy().to_string())
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;

    Ok(CachedAsset {
        path,
        extension: image.extension.into(),
        source: source.into(),
    })
}

async fn mark_broken(
    state: &AppState,
    media_id: i64,
    source: &str,
    url: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO media_asset \
         (media_id, asset_type, source, url, status, checked_at, updated_at) \
         VALUES (?, 'poster', ?, ?, 'BROKEN', datetime('now'), datetime('now')) \
         ON CONFLICT(media_id, asset_type, source, url) DO UPDATE SET \
         status = 'BROKEN', checked_at = datetime('now'), updated_at = datetime('now')",
    )
    .bind(media_id)
    .bind(source)
    .bind(url)
    .execute(&state.pool)
    .await?;
    Ok(())
}

async fn mark_asset_missing(state: &AppState, asset_id: i64) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE media_asset SET status = 'BROKEN', checked_at = datetime('now'), \
         updated_at = datetime('now') WHERE id = ?",
    )
    .bind(asset_id)
    .execute(&state.pool)
    .await?;
    Ok(())
}

async fn media_by_asset_identifier(state: &AppState, identifier: &str) -> AppResult<MediaItem> {
    if let Ok(id) = identifier.parse::<i64>() {
        return storage::media_by_id(&state.pool, id).await;
    }
    let pattern = format!("%{identifier}%");
    let id: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM media_item WHERE provider_id = ? OR title = ? OR filename LIKE ? \
         ORDER BY updated_at DESC, id DESC LIMIT 1",
    )
    .bind(identifier)
    .bind(identifier)
    .bind(pattern)
    .fetch_optional(&state.pool)
    .await?;
    storage::media_by_id(&state.pool, id.ok_or(AppError::NotFound)?).await
}

async fn asset_response(cached: &CachedAsset) -> AppResult<Response<Body>> {
    let bytes = tokio::fs::read(&cached.path)
        .await
        .context("could not read cached poster")?;
    let content_type = match cached.extension.as_str() {
        "png" => "image/png",
        "webp" => "image/webp",
        "avif" => "image/avif",
        _ => "image/jpeg",
    };
    Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "public, max-age=86400")
        .body(Body::from(bytes))
        .map_err(|error| AppError::Internal(error.into()))
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path.parent().context("asset cache path has no parent")?;
    let filename = path
        .file_name()
        .and_then(|filename| filename.to_str())
        .context("asset cache filename is not valid UTF-8")?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = parent.join(format!(
        ".{filename}.luma-{}-{nonce}.tmp",
        std::process::id()
    ));
    tokio::fs::write(&temporary, bytes).await?;
    if let Err(error) = tokio::fs::rename(&temporary, path).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error.into());
    }
    Ok(())
}

fn poster_url(metadata: &Value) -> Option<&str> {
    ["big_cover_url", "cover_url", "poster_url"]
        .into_iter()
        .find_map(|key| metadata.get(key).and_then(Value::as_str))
        .filter(|url| !url.trim().is_empty())
}

fn provider_rank(provider: &str) -> usize {
    PROVIDER_PRIORITY
        .iter()
        .position(|preferred| provider.eq_ignore_ascii_case(preferred))
        .unwrap_or(PROVIDER_PRIORITY.len())
}

fn prefer_exact_matches(query: &str, candidates: &mut Vec<MovieSearchResult>) {
    let query = normalize(query);
    let has_exact = candidates.iter().any(|candidate| {
        normalize(&candidate.number) == query || normalize(&candidate.title) == query
    });
    if has_exact {
        candidates.retain(|candidate| {
            normalize(&candidate.number) == query || normalize(&candidate.title) == query
        });
    }
}

fn normalize(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn media_identifier(media: &MediaItem) -> &str {
    media.provider_id.as_deref().unwrap_or(&media.title)
}

async fn log_success(state: &AppState, media: &MediaItem, cached: &CachedAsset) {
    storage::log(
        &state.pool,
        "info",
        "asset",
        &format!(
            "POSTER_RESOLVE_SUCCESS mediaId={} source={}",
            media_identifier(media),
            cached.source
        ),
    )
    .await;
}

async fn log_failure(state: &AppState, media: &MediaItem, url: &str, reason: &str) {
    storage::log(
        &state.pool,
        "error",
        "asset",
        &format!(
            "POSTER_RESOLVE_FAILED mediaId={} url={} reason={}",
            media_identifier(media),
            url,
            reason
        ),
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        http::{StatusCode, header},
        routing::get,
    };
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    #[test]
    fn fallback_provider_order_matches_design() {
        assert!(provider_rank("FC2PPVDB") < provider_rank("JavBus"));
        assert!(provider_rank("JavBus") < provider_rank("JavLibrary"));
        assert!(provider_rank("JavLibrary") < provider_rank("FC2"));
        assert!(provider_rank("FC2") < provider_rank("fc2hub"));
        assert!(provider_rank("fc2hub") < provider_rank("unknown"));
    }

    #[test]
    fn exact_results_exclude_unrelated_movies() {
        let mut candidates = vec![
            MovieSearchResult {
                id: "1".into(),
                provider: "FC2".into(),
                title: "Unrelated".into(),
                number: "OTHER-1".into(),
            },
            MovieSearchResult {
                id: "2".into(),
                provider: "JavBus".into(),
                title: "Matched".into(),
                number: "FC2PPV-4792609".into(),
            },
        ];
        prefer_exact_matches("fc2ppv_4792609", &mut candidates);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].provider, "JavBus");
    }

    #[tokio::test]
    async fn redirecting_cover_falls_back_and_then_uses_cache() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let asset_root = std::env::temp_dir().join(format!(
            "luma-asset-fallback-{}-{nonce}",
            std::process::id()
        ));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let folder_id = sqlx::query(
            "INSERT INTO media_config (name, path, media_type) VALUES ('Movies', '/movies', 'movie')",
        )
        .execute(&pool)
        .await
        .unwrap()
        .last_insert_rowid();
        let media_id = sqlx::query(
            "INSERT INTO media_item (folder_id, path, filename, hash, title, media_type) \
             VALUES (?, '/movies/fc2.mp4', 'fc2.mp4', 'hash', 'FC2PPV-4792609', 'movie')",
        )
        .bind(folder_id)
        .execute(&pool)
        .await
        .unwrap()
        .last_insert_rowid();
        let state = AppState {
            pool: pool.clone(),
            scrape_limiter: Arc::new(Semaphore::new(8)),
            crawler_limiter: Arc::new(Semaphore::new(2)),
            asset_root: asset_root.clone(),
            script_root: std::env::temp_dir().join(format!("luma-crawler-test-{nonce}")),
            events: tokio::sync::broadcast::channel(32).0,
            fetch_manager: Arc::new(crate::fetch::FetchManager::default()),
            provider_registry: Arc::new(crate::providers::ProviderRegistry::default()),
            snapshot_repository: Arc::new(crate::ingestion::SnapshotRepository::new(
                pool.clone(),
                asset_root.join("source-cache"),
            )),
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let good_url = format!("http://{address}/good.jpg");
        let search_result = MovieSearchResult {
            id: "fallback".into(),
            provider: "FC2PPVDB".into(),
            title: "FC2PPV-4792609".into(),
            number: "FC2PPV-4792609".into(),
        };
        let app = Router::new()
            .route(
                "/bad.jpg",
                get(|| async {
                    (
                        StatusCode::FOUND,
                        [(header::LOCATION, "/error.html")],
                        "redirect",
                    )
                }),
            )
            .route(
                "/good.jpg",
                get(|| async { ([(header::CONTENT_TYPE, "image/jpeg")], vec![4_u8, 2]) }),
            )
            .route(
                "/v1/movies/search",
                get(move || {
                    let result = search_result.clone();
                    async move { Json(serde_json::json!({ "data": [result] })) }
                }),
            )
            .route(
                "/v1/movies/FC2PPVDB/fallback",
                get(move || {
                    let url = good_url.clone();
                    async move { Json(serde_json::json!({ "data": { "cover_url": url } })) }
                }),
            );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let settings = crate::models::Settings {
            metatube_url: format!("http://{address}"),
            metatube_token: String::new(),
            output_format: "nfo".into(),
            scan_interval: 60,
            overwrite_policy: "missing".into(),
            log_level: "info".into(),
            ..crate::models::Settings::default()
        };
        let client = MetaTubeClient::new(&settings).unwrap();
        let media = storage::media_by_id(&pool, media_id).await.unwrap();
        let initial = MovieSearchResult {
            id: "initial".into(),
            provider: "fc2hub".into(),
            title: media.title.clone(),
            number: media.title.clone(),
        };
        let initial_metadata = serde_json::json!({
            "cover_url": format!("http://{address}/bad.jpg")
        });

        let resolved = resolve_poster(
            &state,
            &client,
            &media,
            &media.title,
            Some((&initial, &initial_metadata)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(resolved.source, "FC2PPVDB");
        assert_eq!(tokio::fs::read(&resolved.path).await.unwrap(), vec![4, 2]);
        let active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM media_asset WHERE media_id = ? AND status = 'ACTIVE'",
        )
        .bind(media_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        let broken: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM media_asset WHERE media_id = ? AND status = 'BROKEN'",
        )
        .bind(media_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(active, 1);
        assert_eq!(broken, 1);

        server.abort();
        let cached = resolve_poster(&state, &client, &media, &media.title, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cached.path, resolved.path);

        drop(state);
        drop(pool);
        std::fs::remove_dir_all(asset_root).unwrap();
    }
}
