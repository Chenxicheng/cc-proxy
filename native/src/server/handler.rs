use std::sync::atomic::{AtomicU64, Ordering};

use axum::{
    Json,
    body::Bytes,
    extract::{State, rejection::BytesRejection},
    http::{HeaderName, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use serde_json::{Value, json};

use crate::{
    error::{AppError, response_metadata, safe_text},
    protocol::{
        StreamRequest, convert_request_with_mode, convert_response, convert_usage,
        record_usage_metadata, transform_stream,
    },
};

use super::{
    AppState,
    upstream::{
        NONSTREAM_RESPONSE_TOTAL_TIMEOUT, STREAM_RESPONSE_HEADERS_TIMEOUT, await_upstream_headers,
        read_upstream_body, upstream_error, upstream_request_shape,
    },
};

static REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(super) async fn health(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "adapter": "cc-proxy",
        "dropped_jsonl_records": state.storage.dropped_count(),
        "logging": state.storage.health(),
    }))
}

pub(super) async fn messages(
    State(state): State<AppState>,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let request_id = request_id();
    let body = match body {
        Ok(body) => body,
        Err(_) => {
            return error_response(
                &state,
                &request_id,
                "unknown",
                false,
                AppError::new(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "Request body exceeds the 32 MiB limit",
                )
                .with_details(
                    json!({"phase":"request_body_limit", "elapsedMs":0, "upstreamAttempted":false}),
                ),
            );
        }
    };
    let anthropic: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return error_response(
                &state,
                &request_id,
                "unknown",
                false,
                AppError::bad_request(format!("body: Invalid JSON: {error}")).with_details(
                    json!({"phase":"request_json", "elapsedMs":0, "upstreamAttempted":false}),
                ),
            );
        }
    };
    let model = anthropic
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let streaming = anthropic.get("stream").and_then(Value::as_bool) == Some(true);
    let response = handle_messages(&state, &request_id, &anthropic).await;
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            return error_response(&state, &request_id, model, streaming, error);
        }
    };
    with_request_id(response, &request_id)
}

fn with_request_id(mut response: Response, request_id: &str) -> Response {
    if let Ok(value) = HeaderValue::from_str(request_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-request-id"), value.clone());
        response
            .headers_mut()
            .insert(HeaderName::from_static("request-id"), value);
    }
    response
}

async fn handle_messages(
    state: &AppState,
    request_id: &str,
    anthropic: &Value,
) -> Result<Response, AppError> {
    let started = tokio::time::Instant::now();
    let streaming = anthropic.get("stream").and_then(Value::as_bool) == Some(true);
    let openai = convert_request_with_mode(
        anthropic,
        state.config.upstream_capabilities.assistant_prefill,
        state.config.upstream_capabilities.max_tokens_field,
    )
    .map_err(|error| {
        error.with_details(json!({
            "phase": "request_conversion", "direction": "anthropic_to_chat",
            "elapsedMs":started.elapsed().as_millis(), "upstreamAttempted":false,
            "clientStream":streaming
        }))
    })?;
    let mut request_shape = upstream_request_shape(&openai);
    request_shape["clientStream"] = json!(streaming);
    let mut metadata = Value::Null;
    let mut headers_ms = None;
    let mut transfer = Value::Null;
    let result = async {
        let model = anthropic["model"].as_str().expect("validated model");
        let tool_names = openai
            .get("tools")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|tool| {
                tool.pointer("/function/name")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect::<Vec<_>>();
        eprintln!("-> {model} [sent] {request_id}");
        let request = state
            .client
            .post(state.config.chat_completions_url())
            .json(&openai);
        let upstream_deadline = tokio::time::Instant::now() + NONSTREAM_RESPONSE_TOTAL_TIMEOUT;
        let response =
            await_upstream_headers(request.send(), STREAM_RESPONSE_HEADERS_TIMEOUT).await?;
        let headers_received_at = tokio::time::Instant::now();
        headers_ms = Some(started.elapsed().as_millis());
        metadata = response_metadata(&response);
        if !response.status().is_success() {
            return Err(upstream_error(response, &openai, upstream_deadline).await);
        }

        if streaming {
            let body = transform_stream(
                response,
                StreamRequest {
                    model: model.to_owned(),
                    provider: state.config.base_url.clone(),
                    request_id: request_id.to_owned(),
                    tool_names,
                    summary: request_shape.clone(),
                    started,
                },
                state.storage.clone(),
                headers_received_at,
            );
            let response = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .header(header::CACHE_CONTROL, "no-cache")
                .header(header::CONNECTION, "keep-alive")
                .header("x-accel-buffering", "no")
                .body(body)
                .expect("valid streaming response");
            eprintln!("<- {model} [streaming] {request_id}");
            return Ok(response);
        }

        let (body, progress) = read_upstream_body(response, upstream_deadline).await?;
        transfer = progress;
        let openai_response: Value = serde_json::from_slice(&body).map_err(|error| {
            AppError::new(
                StatusCode::BAD_GATEWAY,
                format!("Invalid upstream JSON: {error}"),
            )
            .with_details(json!({
                "phase": "response_json", "direction": "chat_to_anthropic",
                "jsonLine": error.line(), "jsonColumn": error.column(),
                "bodyBytes": body.len(),
            }))
        })?;
        let converted =
            convert_response(&openai_response, model, &tool_names).map_err(|error| {
                error.with_details(json!({
                    "phase": "protocol_conversion", "direction": "chat_to_anthropic",
                    "diagnostic": crate::protocol::response_shape(&openai_response),
                }))
            })?;
        let usage = convert_usage(openai_response.get("usage"));
        let empty_end_turn = converted["stop_reason"] == "end_turn"
            && converted["content"].as_array().is_some_and(Vec::is_empty);
        let mut record = json!({
            "timestamp": now(),
            "requestId": request_id,
            "schemaVersion": 2,
            "provider": state.config.base_url,
            "modelName": model,
            "streaming": false,
            "outcome": if empty_end_turn { "empty_end_turn" } else { "usable" },
        });
        record_usage_metadata(&mut record, &usage);
        if let Some(raw_usage) = openai_response
            .get("usage")
            .filter(|usage| !usage.is_null())
        {
            record["usage"] = raw_usage.clone();
        }
        if let Some(actual_model) = openai_response.get("model") {
            record["model"] = actual_model.clone();
        }
        state.storage.record_usage(record);
        eprintln!("<- {model} [received] {request_id}");
        Ok(Json(converted).into_response())
    }
    .await;
    result.map_err(|error: AppError| {
        error.with_context(json!({
        "upstreamRequestShape":request_shape, "upstreamResponse":metadata,
        "requestElapsedMs":started.elapsed().as_millis(), "elapsedMs":started.elapsed().as_millis(),
        "headersMs":headers_ms, "upstreamAttempted":true, "transfer":transfer
    }))
    })
}

fn error_response(
    state: &AppState,
    request_id: &str,
    model: &str,
    streaming: bool,
    error: AppError,
) -> Response {
    state.storage.record_error(error_record_value(
        request_id,
        &state.config.base_url,
        model,
        streaming,
        &error,
    ));
    with_request_id(error.into_response(), request_id)
}

fn error_record_value(
    request_id: &str,
    provider: &str,
    model: &str,
    streaming: bool,
    error: &AppError,
) -> Value {
    let mut details = json!({
        "message": if error.details.as_ref().and_then(|d| d["phase"].as_str()) == Some("upstream_http") { format!("Upstream returned HTTP {} (body omitted)", error.status.as_u16()) } else { safe_text(&error.message) },
        "status": error.status.as_u16(),
        "type": error.error_type(),
    });
    let mut request_shape = None;
    if let Some(response) = error.details.as_ref().and_then(Value::as_object) {
        for (field, value) in response {
            if field == "upstreamRequestShape" {
                request_shape = Some(value.clone());
            } else {
                details[field] = value.clone();
            }
        }
    }
    let mut record = json!({
        "timestamp": now(),
        "requestId": request_id,
        "provider": provider,
        "modelName": model,
        "streaming": streaming,
        "error": details,
    });
    if let Some(shape) = request_shape {
        record["upstreamRequestShape"] = shape;
    }
    record
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn request_id() -> String {
    let counter = REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
    format!("req_{}_{}", Utc::now().timestamp_millis(), counter)
}

#[cfg(test)]
#[path = "../../tests/unit/server/handler.rs"]
mod tests;
