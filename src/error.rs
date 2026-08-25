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
    #[error("database query exceeded its execution deadline")]
    QueryTimeout,
    #[error("{0} is not configured on this deployment")]
    Unavailable(&'static str),
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
            // 57014 is query_canceled. Reporting it as a transaction conflict told
            // callers to retry something a retry cannot fix; two agents burned a
            // session on that. 504 with no Retry-After says "this was too slow",
            // which is the actionable truth.
            Self::QueryTimeout => (StatusCode::GATEWAY_TIMEOUT, "query_timeout", None),
            Self::Unavailable(_) => (StatusCode::SERVICE_UNAVAILABLE, "unavailable", None),
            Self::NotFound => (StatusCode::NOT_FOUND, "not_found", None),
            Self::Storage(_) | Self::Internal(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", None)
            }
        };
        // Storage and Internal detail can carry SQL text, endpoint URLs, or
        // serde context — server-side tracing keeps the detail; the HTTP body
        // gets a constant.
        let message = match &self {
            Self::Storage(detail) => {
                tracing::error!(%detail, "storage error");
                "storage failure".to_owned()
            }
            Self::Internal(detail) => {
                tracing::error!(%detail, "internal error");
                "internal failure".to_owned()
            }
            _ => self.to_string(),
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
            .is_some_and(|code| matches!(code.as_ref(), "40001" | "40P01" | "55P03"))
        {
            Self::Retryable
        } else if value
            .as_database_error()
            .and_then(|error| error.code())
            .is_some_and(|code| code.as_ref() == "57014")
        {
            // query_canceled covers statement_timeout and explicit backend
            // cancellation. The client gets a constant; the operator needs the
            // database's own message to tell those apart.
            if let Some(detail) = value.as_database_error() {
                tracing::warn!(detail = %detail.message(), "database query canceled");
            }
            Self::QueryTimeout
        } else {
            Self::Storage(value)
        }
    }
}
