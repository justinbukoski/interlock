use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("authentication required")]
    Unauthorized,
    #[error("operation not permitted for this token")]
    Forbidden,
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("token budget too small; minimum is {minimum}")]
    BudgetTooSmall { minimum: usize },
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("retryable transaction conflict")]
    Retryable,
    #[error("not found")]
    NotFound,
    #[error("storage failure")]
    Storage(#[source] sqlx::Error),
    #[error("internal failure: {0}")]
    Internal(String),
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: ErrorDetail<'a>,
}

#[derive(Serialize)]
struct ErrorDetail<'a> {
    code: &'a str,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    minimum_token_budget: Option<usize>,
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, code, minimum) = match &self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized", None),
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden", None),
            Self::Invalid(_) => (StatusCode::BAD_REQUEST, "invalid_request", None),
            Self::BudgetTooSmall { minimum } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "budget_too_small",
                Some(*minimum),
            ),
            Self::Conflict(_) => (StatusCode::CONFLICT, "conflict", None),
            Self::Retryable => (StatusCode::SERVICE_UNAVAILABLE, "retryable", None),
            Self::NotFound => (StatusCode::NOT_FOUND, "not_found", None),
            Self::Storage(_) | Self::Internal(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", None)
            }
        };
        let message = if matches!(self, Self::Storage(_)) {
            "storage failure".to_owned()
        } else {
            self.to_string()
        };
        (
            status,
            Json(ErrorBody {
                error: ErrorDetail {
                    code,
                    message,
                    minimum_token_budget: minimum,
                },
            }),
        )
            .into_response()
    }
}

impl From<sqlx::Error> for AppError {
    fn from(value: sqlx::Error) -> Self {
        if value
            .as_database_error()
            .and_then(|error| error.code())
            .is_some_and(|code| matches!(code.as_ref(), "40001" | "40P01" | "55P03" | "57014"))
        {
            Self::Retryable
        } else {
            Self::Storage(value)
        }
    }
}
