mod api;
mod asset;
mod crawler;
mod error;
mod metadata;
mod models;
mod provider;
mod qbittorrent;
mod scanner;
mod scheduler;
mod storage;
mod watcher;

use std::{env, net::SocketAddr, path::PathBuf, sync::Arc};

use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};
use sqlx::SqlitePool;
use tokio::sync::Semaphore;
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
    pub scrape_limiter: Arc<Semaphore>,
    pub crawler_limiter: Arc<Semaphore>,
    pub asset_root: PathBuf,
    pub script_root: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "luma_server=info,tower_http=info".into()),
        )
        .init();

    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://luma.db?mode=rwc".into());
    let address: SocketAddr = env::var("LUMA_BIND")
        .unwrap_or_else(|_| "0.0.0.0:3000".into())
        .parse()?;
    let data_root = PathBuf::from(env::var("LUMA_DATA_DIR").unwrap_or_else(|_| "data".into()));
    let asset_root = data_root.join("assets");
    let script_root = data_root.join("crawlers");
    tokio::fs::create_dir_all(asset_root.join("poster")).await?;
    tokio::fs::create_dir_all(script_root.join("runs")).await?;
    tokio::fs::create_dir_all(script_root.join("scripts")).await?;
    let pool = storage::connect(&database_url).await?;
    let state = AppState {
        pool,
        scrape_limiter: Arc::new(Semaphore::new(8)),
        crawler_limiter: Arc::new(Semaphore::new(2)),
        asset_root,
        script_root,
    };
    scheduler::start(state.clone());
    let _watcher = watcher::start(state.clone());
    asset::start_health_job(state.clone());

    let api_router = api::router().route("/health", get(|| async { (StatusCode::OK, "ok") }));

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);
    let mut app = Router::new()
        .route("/asset/poster/{media_id}", get(asset::poster))
        .route("/asset/cover/{media_id}", get(asset::poster))
        .nest("/api/v1", api_router)
        .with_state(state)
        .layer(cors)
        .layer(TraceLayer::new_for_http());

    if let Ok(static_dir) = env::var("LUMA_STATIC_DIR") {
        let directory = PathBuf::from(static_dir);
        let index = directory.join("index.html");
        app =
            app.fallback_service(ServeDir::new(directory).not_found_service(ServeFile::new(index)));
    } else {
        app = app.fallback(|| async { (StatusCode::NOT_FOUND, "Luma API").into_response() });
    }

    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, "Luma server listening");
    axum::serve(listener, app).await?;
    Ok(())
}
