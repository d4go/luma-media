mod api;
mod error;
mod metadata;
mod models;
mod provider;
mod scanner;
mod scheduler;
mod storage;

use std::{env, net::SocketAddr, path::PathBuf};

use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};
use sqlx::SqlitePool;
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
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
        env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://luma-media.db?mode=rwc".into());
    let address: SocketAddr = env::var("LUMA_BIND")
        .unwrap_or_else(|_| "0.0.0.0:3000".into())
        .parse()?;
    let pool = storage::connect(&database_url).await?;
    let state = AppState { pool };
    scheduler::start(state.clone());

    let api_router = api::router()
        .route("/health", get(|| async { (StatusCode::OK, "ok") }))
        .with_state(state);

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);
    let mut app = Router::new()
        .nest("/api/v1", api_router)
        .layer(cors)
        .layer(TraceLayer::new_for_http());

    if let Ok(static_dir) = env::var("LUMA_STATIC_DIR") {
        let directory = PathBuf::from(static_dir);
        let index = directory.join("index.html");
        app =
            app.fallback_service(ServeDir::new(directory).not_found_service(ServeFile::new(index)));
    } else {
        app = app.fallback(|| async { (StatusCode::NOT_FOUND, "Luma Media API").into_response() });
    }

    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, "Luma Media server listening");
    axum::serve(listener, app).await?;
    Ok(())
}
