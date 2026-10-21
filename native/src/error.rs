use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::error::Error;
use tokio::time::Instant;

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

    pub fn with_context(mut self, context: Value) -> Self {
        let details = self.details.get_or_insert_with(|| json!({}));
        if let (Some(details), Some(context)) = (details.as_object_mut(), context.as_object()) {
            for (key, value) in context {
                details.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
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

pub(crate) fn error_chain(error: &dyn Error) -> Vec<String> {
    let mut chain = Vec::new();
    let mut current = Some(error);
    while let Some(error) = current {
        if chain.len() == 8 {
            break;
        }
        chain.push(safe_text(&error.to_string()));
        current = error.source();
    }
    chain
}

// Free-form quoted values and URLs are not needed to identify a validation or transport failure.
pub(crate) fn safe_text(text: &str) -> String {
    let mut output = String::new();
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if output.chars().count() >= 512 {
            break;
        }
        if matches!(ch, '\"' | '\'') {
            output.push_str("<redacted>");
            for next in chars.by_ref() {
                if next == ch {
                    break;
                }
            }
        } else if ch.is_control() {
            output.push(' ');
        } else {
            output.push(ch);
        }
    }
    output
        .split_whitespace()
        .map(|word| {
            if word.contains("://") || word.starts_with("sk-") {
                "<redacted>"
            } else {
                word
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(512)
        .collect()
}

pub(crate) fn response_metadata(response: &reqwest::Response) -> Value {
    let mut metadata = json!({
        "status": response.status().as_u16(),
        "httpVersion": format!("{:?}", response.version()),
        "contentLength": response.content_length(),
    });
    for (field, header) in [
        ("contentType", "content-type"),
        ("contentEncoding", "content-encoding"),
        ("transferEncoding", "transfer-encoding"),
        ("requestId", "request-id"),
        ("xRequestId", "x-request-id"),
        ("cfRay", "cf-ray"),
    ] {
        metadata[field] = response
            .headers()
            .get(header)
            .and_then(|v| v.to_str().ok())
            // Redact configured secrets before truncation at the storage boundary.
            .map(str::to_owned)
            .map(Value::String)
            .unwrap_or(Value::Null);
    }
    metadata
}

pub(crate) fn upstream_error_summary(error: &Value) -> Value {
    let mut summary = json!({"bodyOmitted":true});
    for field in ["type", "code", "param"] {
        if let Some(value) = error.get(field).and_then(Value::as_str) {
            let known = matches!(
                value,
                "invalid_request_error"
                    | "authentication_error"
                    | "permission_error"
                    | "rate_limit_error"
                    | "rate_limit_exceeded"
                    | "invalid_api_key"
                    | "model_not_found"
                    | "unsupported_field"
                    | "context_length_exceeded"
                    | "server_error"
                    | "overloaded_error"
                    | "stream"
                    | "stream_options"
                    | "reasoning_effort"
                    | "enable_thinking"
                    | "thinking"
                    | "max_tokens"
                    | "max_completion_tokens"
                    | "messages"
                    | "tools"
            );
            summary[field] = json!(if known { value } else { "unrecognized" });
        }
    }
    summary
}

pub(crate) struct BodyProgress {
    started: Instant,
    first_byte: Option<Instant>,
    last_byte: Option<Instant>,
    bytes: u64,
    chunks: u64,
}

impl BodyProgress {
    pub fn new(started: Instant) -> Self {
        Self {
            started,
            first_byte: None,
            last_byte: None,
            bytes: 0,
            chunks: 0,
        }
    }
    pub fn received(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let now = Instant::now();
        self.first_byte.get_or_insert(now);
        self.last_byte = Some(now);
        self.bytes = self.bytes.saturating_add(bytes as u64);
        self.chunks = self.chunks.saturating_add(1);
    }
    pub fn snapshot(&self) -> Value {
        json!({
            "bodyElapsedMs": self.started.elapsed().as_millis(),
            "receivedBytes": self.bytes, "receivedChunks": self.chunks,
            "firstByteMs": self.first_byte.map(|at| at.duration_since(self.started).as_millis()),
            "sinceLastByteMs": self.last_byte.map(|at| at.elapsed().as_millis()),
        })
    }
}
