use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::json;

#[derive(Debug)]
pub struct AppError {
    pub status: StatusCode,
    pub message: String,
    pub details: Option<serde_json::Value>,
    pub retry_after: Option<HeaderValue>,
}

impl AppError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            details: None,
            retry_after: None,
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    pub fn with_retry_after(mut self, value: Option<HeaderValue>) -> Self {
        self.retry_after = value;
        self
    }

    pub fn error_type(&self) -> &'static str {
        match self.status.as_u16() {
            400 | 413 => "invalid_request_error",
            401 => "authentication_error",
            403 => "permission_error",
            404 => "not_found_error",
            429 => "rate_limit_error",
            _ => "api_error",
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let body = Json(json!({
            "type": "error",
            "error": {
                "type": self.error_type(),
                "message": self.message,
            }
        }));
        let mut response = (self.status, body).into_response();
        if let Some(value) = self.retry_after {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
        response
    }
}
