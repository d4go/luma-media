use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("{0}")]
    BadRequest(String),
    #[error("resource not found")]
    NotFound,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message.clone()),
            Self::NotFound => (StatusCode::NOT_FOUND, self.to_string()),
            Self::Database(error) => {
                tracing::error!(%error, "database error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "database operation failed".into(),
                )
            }
            Self::Internal(error) => {
                tracing::error!(%error, "internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".into(),
                )
            }
        };

        (status, Json(json!({ "message": message }))).into_response()
    }
}

pub type AppResult<T> = Result<T, AppError>;
