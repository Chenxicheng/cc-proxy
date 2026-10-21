use std::{
    collections::{BTreeMap, HashSet},
    convert::Infallible,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_stream::stream;
use axum::{
    body::{Body, Bytes},
    response::{IntoResponse, Sse, sse::Event},
};
use chrono::Utc;
use eventsource_stream::{EventStreamError, Eventsource};
use futures_util::{Stream, StreamExt};
use rand::Rng;
use serde_json::{Value, json};
use tokio::time::{Instant, timeout_at};

use crate::{
    error::{BodyProgress, error_chain, response_metadata, safe_text},
    storage::Storage,
};

use super::{
    response::{map_finish_reason, reasoning_content},
    usage::{UsageConversion, convert_usage, record_usage_metadata},
};

const FIRST_BODY_BYTES_TIMEOUT: Duration = Duration::from_secs(120);
const FIRST_ANTHROPIC_EVENT_TIMEOUT: Duration = Duration::from_secs(240);
const BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone, Copy)]
struct StreamTimeouts {
    first_body_bytes: Duration,
    first_anthropic_event: Duration,
    body_idle: Duration,
}

const STREAM_TIMEOUTS: StreamTimeouts = StreamTimeouts {
    first_body_bytes: FIRST_BODY_BYTES_TIMEOUT,
    first_anthropic_event: FIRST_ANTHROPIC_EVENT_TIMEOUT,
    body_idle: BODY_IDLE_TIMEOUT,
};

pub struct StreamRequest {
    pub model: String,
    pub provider: String,
    pub request_id: String,
    pub tool_names: Vec<String>,
    pub summary: Value,
    pub started: Instant,
}

pub fn transform_stream(
    response: reqwest::Response,
    request: StreamRequest,
    storage: Storage,
    headers_received_at: Instant,
) -> Body {
    let response_metadata = response_metadata(&response);
    transform_bytes_with_timeouts(
        response
            .bytes_stream()
            .map(|result| result.map_err(reqwest::Error::without_url)),
        StreamState::new(
            request.model,
            request.provider,
            request.request_id,
            request.tool_names,
        )
        .with_response_metadata(response_metadata)
        .with_request_context(request.summary, request.started, headers_received_at),
        storage,
        headers_received_at,
        STREAM_TIMEOUTS,
    )
}

fn transform_bytes_with_timeouts<S, E>(
    upstream: S,
    mut state: StreamState,
    storage: Storage,
    headers_received_at: Instant,
    timeouts: StreamTimeouts,
) -> Body
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    state.progress = Arc::new(Mutex::new(BodyProgress::new(headers_received_at)));
    let progress = state.progress.clone();
    let upstream = upstream.inspect(move |item| {
        if let Ok(bytes) = item {
            progress
                .lock()
                .expect("progress lock")
                .received(bytes.len());
        }
    });
    let output = stream! {
        let upstream = timeout_bytes(upstream, headers_received_at, timeouts);
        let events = upstream.eventsource();
        futures_util::pin_mut!(events);
        let mut failed = false;
        let mut done = false;
        let first_event_deadline = headers_received_at + timeouts.first_anthropic_event;
        let mut waiting_for_first_event = true;
        loop {
            let event = if waiting_for_first_event {
                match timeout_at(first_event_deadline, events.next()).await {
                    Ok(event) => event,
                    Err(_) => {
                        let message = "Upstream stream timed out waiting for first Anthropic event";
                        storage.record_error(state.error_record(message, "first_anthropic_event", headers_received_at.elapsed()));
                        yield Ok::<Event, Infallible>(encode_event(error_event(message)));
                        failed = true;
                        break;
                    }
                }
            } else {
                events.next().await
            };
            let Some(event) = event else { break };
            let result = match event {
                Ok(event) if event.data == "[DONE]" => {
                    done = true;
                    break;
                }
                Ok(event) => state.process_data(&event.data),
                Err(error) => {
                    state.diagnostic_details = Some(stream_error_details(&state, &error));
                    let phase = match &error {
                        EventStreamError::Transport(error) => error.phase,
                        _ => "sse_frame",
                    };
                    Err((phase, format!("Upstream stream failed: {error}")))
                },
            };
            match result {
                Ok(events) => {
                    if !events.is_empty() {
                        waiting_for_first_event = false;
                    }
                    for event in events { yield Ok::<Event, Infallible>(encode_event(event)); }
                }
                Err((phase, message)) => {
                    storage.record_error(state.error_record(&message, phase, headers_received_at.elapsed()));
                    yield Ok(encode_event(error_event(&message)));
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            match state.finish(done) {
                Ok(events) => {
                    storage.record_usage(state.usage_record());
                    eprintln!("<- {:?} [completed] {:?}", state.model, state.request_id);
                    for event in events { yield Ok(encode_event(event)); }
                }
                Err(message) => {
                    storage.record_error(state.error_record(&message, "stream_finish", headers_received_at.elapsed()));
                    yield Ok(encode_event(error_event(&message)));
                }
            }
        }
    };
    Sse::new(output).into_response().into_body()
}

#[derive(Debug)]
struct StreamReadError {
    message: String,
    error_chain: Vec<String>,
    phase: &'static str,
}

fn stream_error_details(state: &StreamState, error: &EventStreamError<StreamReadError>) -> Value {
    // EventStreamError does not expose its transport error through Error::source().
    let chain = match error {
        EventStreamError::Transport(error) => error.error_chain.clone(),
        _ => vec!["Upstream SSE framing or UTF-8 decoding failed".to_owned()],
    };
    let phase = match error {
        EventStreamError::Transport(error) => error.phase,
        _ => "sse_frame",
    };
    let mut details = json!({"errorChain": chain, "phase": phase});
    if let Some(response) = &state.response_metadata {
        details["upstreamResponse"] = response.clone();
    }
    details
}

impl fmt::Display for StreamReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StreamReadError {}

fn timeout_bytes<S, E>(
    upstream: S,
    headers_received_at: Instant,
    timeouts: StreamTimeouts,
) -> impl Stream<Item = Result<Bytes, StreamReadError>> + Send
where
    S: Stream<Item = Result<Bytes, E>> + Send,
    E: std::error::Error + Send + Sync,
{
    stream! {
        futures_util::pin_mut!(upstream);
        let mut first = true;
        let mut deadline = headers_received_at + timeouts.first_body_bytes;
        loop {
            match timeout_at(deadline, upstream.next()).await {
                Ok(Some(Ok(bytes))) if bytes.is_empty() => {}
                Ok(Some(Ok(bytes))) => {
                    first = false;
                    yield Ok(bytes);
                    deadline = Instant::now() + timeouts.body_idle;
                }
                Ok(Some(Err(error))) => {
                    let error_chain = error_chain(&error);
                    yield Err(StreamReadError { message: error.to_string(), error_chain, phase: "upstream_transport" });
                    break;
                }
                Ok(None) => break,
                Err(_) => {
                    let message = if first {
                        "Upstream stream timed out waiting for first body bytes"
                    } else {
                        "Upstream stream was idle for too long"
                    };
                    let phase = if first { "first_body_bytes" } else { "body_idle" };
                    yield Err(StreamReadError { message: message.into(), error_chain: vec![message.into()], phase });
                    break;
                }
            }
        }
    }
}

fn encode_event(event: Value) -> Event {
    Event::default()
        .event(event["type"].as_str().expect("internal event type"))
        .json_data(&event)
        .expect("JSON value serialization")
}

fn error_event(message: &str) -> Value {
    json!({"type": "error", "error": {"type": "api_error", "message": message}})
}

struct ToolCall {
    id: String,
    name: String,
    arguments: String,
    emitted_arguments: usize,
    block_index: Option<usize>,
    closed: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum ContentKind {
    Text,
    Thinking,
}

struct StreamState {
    message_id: String,
    request_id: String,
    model: String,
    response_model: Option<String>,
    provider: String,
    next_index: usize,
    tools: BTreeMap<usize, ToolCall>,
    tool_names: Vec<String>,
    used_tool_ids: HashSet<String>,
    active_content: Option<(usize, ContentKind)>,
    started: bool,
    content_seen: bool,
    refusal_text: String,
    finish_reason: Option<String>,
    converted_usage: UsageConversion,
    upstream_usage: Option<Value>,
    error_details: Option<Value>,
    response_metadata: Option<Value>,
    diagnostic_details: Option<Value>,
    chunk_index: u64,
    request_summary: Value,
    request_started: Instant,
    headers_ms: u128,
    progress: Arc<Mutex<BodyProgress>>,
}

impl StreamState {
    fn new(model: String, provider: String, request_id: String, tool_names: Vec<String>) -> Self {
        Self {
            message_id: format!("msg_{request_id}"),
            request_id,
            model,
            response_model: None,
            provider,
            next_index: 0,
            tools: BTreeMap::new(),
            tool_names,
            used_tool_ids: HashSet::new(),
            active_content: None,
            started: false,
            content_seen: false,
            refusal_text: String::new(),
            finish_reason: None,
            converted_usage: convert_usage(None),
            upstream_usage: None,
            error_details: None,
            response_metadata: None,
            diagnostic_details: None,
            chunk_index: 0,
            request_summary: Value::Null,
            request_started: Instant::now(),
            headers_ms: 0,
            progress: Arc::new(Mutex::new(BodyProgress::new(Instant::now()))),
        }
    }

    fn with_response_metadata(mut self, metadata: Value) -> Self {
        self.response_metadata = Some(metadata);
        self
    }

    fn with_request_context(mut self, summary: Value, started: Instant, headers: Instant) -> Self {
        self.request_summary = summary;
        self.request_started = started;
        self.headers_ms = headers.duration_since(started).as_millis();
        self
    }

    fn process_data(&mut self, data: &str) -> Result<Vec<Value>, (&'static str, String)> {
        self.chunk_index = self.chunk_index.saturating_add(1);
        let chunk: Value = serde_json::from_str(data).map_err(|error| {
            self.diagnostic_details = Some(json!({
                "jsonLine": error.line(), "jsonColumn": error.column(),
                "eventBytes": data.len(),
            }));
            ("sse_json", format!("Invalid upstream SSE JSON: {error}"))
        })?;
        self.process_chunk(&chunk).map_err(|message| {
            self.diagnostic_details = Some(super::response_shape(&chunk));
            let phase = if chunk.get("error").is_some() {
                "upstream_sse_error"
            } else {
                "protocol_conversion"
            };
            (phase, message)
        })
    }

    fn process_chunk(&mut self, chunk: &Value) -> Result<Vec<Value>, String> {
        let mut events = Vec::new();
        if let Some(error) = chunk.get("error") {
            self.error_details = Some(error.clone());
            return Err(error
                .get("message")
                .and_then(Value::as_str)
                .or_else(|| error.as_str())
                .map(str::to_owned)
                .unwrap_or_else(|| error.to_string()));
        }
        if let Some(usage) = chunk.get("usage").filter(|usage| !usage.is_null()) {
            self.converted_usage = convert_usage(Some(usage));
            self.upstream_usage = Some(usage.clone());
        }
        if self.response_model.is_none() {
            self.response_model = chunk
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
        let choices = chunk
            .get("choices")
            .and_then(Value::as_array)
            .ok_or("Upstream stream chunk is missing choices")?;
        if choices.is_empty() {
            return Ok(events);
        }
        if choices.len() != 1 {
            return Err("Upstream stream chunk must contain exactly one choice".into());
        }
        let choice = &choices[0];
        let delta = choice
            .get("delta")
            .and_then(Value::as_object)
            .ok_or("Upstream stream choice is missing delta")?;
        if delta
            .get("role")
            .is_some_and(|role| !role.is_null() && role.as_str() != Some("assistant"))
        {
            return Err("Upstream stream delta.role must be assistant or null".into());
        }
        for field in ["content", "refusal", "reasoning_content", "reasoning"] {
            if delta
                .get(field)
                .is_some_and(|value| !value.is_null() && !value.is_string())
            {
                return Err(format!(
                    "Upstream stream delta.{field} must be a string or null"
                ));
            }
        }
        for field in ["audio", "function_call"] {
            if delta.get(field).is_some_and(|value| !value.is_null()) {
                return Err(format!("Upstream stream delta.{field} is unsupported"));
            }
        }
        if delta.get("annotations").is_some_and(|value| {
            !value.is_null() && value.as_array().is_none_or(|items| !items.is_empty())
        }) {
            return Err("Upstream stream delta.annotations are unsupported".into());
        }
        if delta
            .get("tool_calls")
            .is_some_and(|calls| !calls.is_null() && !calls.is_array())
        {
            return Err("Upstream stream delta.tool_calls must be an array".into());
        }
        if let Some(finished) = self.finish_reason.as_deref() {
            if let Some(reason) = choice
                .get("finish_reason")
                .filter(|reason| !reason.is_null())
            {
                let reason = reason
                    .as_str()
                    .ok_or("Upstream stream finish_reason must be a string or null")?;
                if reason != finished {
                    return Err("Upstream stream sent a conflicting finish_reason".into());
                }
            }
            let has_content = delta.iter().any(|(field, value)| match field.as_str() {
                "role" => false,
                "content" | "refusal" | "reasoning_content" | "reasoning" => {
                    value.as_str().is_some_and(|text| !text.is_empty())
                }
                "annotations" => value.as_array().is_some_and(|items| !items.is_empty()),
                "tool_calls" => value.as_array().is_some_and(|calls| !calls.is_empty()),
                _ => !value.is_null(),
            });
            if has_content {
                return Err("Upstream stream sent content after finish_reason".into());
            }
            return Ok(events);
        }
        if !self.started {
            events.push(json!({"type": "message_start", "message": {
                "id": self.message_id, "type": "message", "role": "assistant", "content": [],
                "model": self.model, "stop_reason": Value::Null, "stop_sequence": Value::Null,
                "usage": {"input_tokens": 0, "output_tokens": 0}
            }}));
            self.started = true;
        }
        let reasoning = reasoning_content(&choice["delta"]);
        if !reasoning.is_empty() {
            self.push_content(reasoning, ContentKind::Thinking, &mut events);
        }
        for (field, kind) in [
            ("content", ContentKind::Text),
            ("refusal", ContentKind::Text),
        ] {
            if let Some(text) = delta
                .get(field)
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
            {
                if field == "refusal" {
                    self.refusal_text.push_str(text);
                }
                self.push_content(text, kind, &mut events);
            }
        }
        if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                self.process_tool_delta(call, &mut events)?;
            }
        }
        if let Some(reason) = choice
            .get("finish_reason")
            .filter(|reason| !reason.is_null())
        {
            self.finish_reason = Some(
                reason
                    .as_str()
                    .ok_or("Upstream stream finish_reason must be a string or null")?
                    .to_owned(),
            );
            self.close_blocks(&mut events)?;
        }
        Ok(events)
    }

    fn process_tool_delta(&mut self, delta: &Value, events: &mut Vec<Value>) -> Result<(), String> {
        let delta = delta
            .as_object()
            .ok_or("Upstream tool delta must be an object")?;
        let index = delta
            .get("index")
            .and_then(Value::as_u64)
            .and_then(|index| usize::try_from(index).ok())
            .ok_or("Upstream tool delta is missing index")?;
        if delta
            .get("type")
            .is_some_and(|kind| !kind.is_null() && kind.as_str() != Some("function"))
        {
            return Err("Upstream tool delta type must be function or null".into());
        }
        if delta
            .get("id")
            .is_some_and(|id| !id.is_null() && !id.is_string())
        {
            return Err("Upstream tool delta id must be a string or null".into());
        }
        if delta
            .get("function")
            .is_some_and(|function| !function.is_null() && !function.is_object())
        {
            return Err("Upstream tool delta function must be an object or null".into());
        }
        self.close_content(events);
        let tool = self.tools.entry(index).or_insert_with(|| ToolCall {
            id: String::new(),
            name: String::new(),
            arguments: String::new(),
            emitted_arguments: 0,
            block_index: None,
            closed: false,
        });
        if let Some(id) = delta
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        {
            // Identity is fixed when the block starts, including repaired IDs (main parity).
            if tool.block_index.is_none() {
                tool.id = id.to_owned();
            }
        }
        let function = delta.get("function").and_then(Value::as_object);
        for (field, target) in [("name", &mut tool.name), ("arguments", &mut tool.arguments)] {
            if let Some(fragment) = function
                .and_then(|function| function.get(field))
                .filter(|v| !v.is_null())
            {
                let fragment = fragment.as_str().ok_or_else(|| {
                    format!("Upstream tool delta function.{field} must be a string")
                })?;
                if field == "name" && !fragment.is_empty() && tool.block_index.is_some() {
                    if fragment != target.as_str() {
                        return Err(
                            "Upstream tool function.name changed after streaming started".into(),
                        );
                    }
                    continue;
                }
                target.push_str(fragment);
            }
        }
        let stable = self.tool_names.iter().any(|name| name == &tool.name)
            && !self
                .tool_names
                .iter()
                .any(|name| name != &tool.name && name.starts_with(&tool.name));
        if tool.block_index.is_some() || stable {
            self.emit_tool(index, events)?;
        }
        self.content_seen = true;
        Ok(())
    }

    fn emit_tool(&mut self, index: usize, events: &mut Vec<Value>) -> Result<(), String> {
        let tool = self.tools.get_mut(&index).expect("known tool");
        if tool.block_index.is_none() {
            if !self.tool_names.contains(&tool.name) {
                return Err("Upstream tool call has a missing or undeclared function.name".into());
            }
            while tool.id.is_empty() || self.used_tool_ids.contains(&tool.id) {
                tool.id = generated_tool_id();
            }
            self.used_tool_ids.insert(tool.id.clone());
            let index = self.next_index;
            self.next_index += 1;
            tool.block_index = Some(index);
            events.push(json!({"type": "content_block_start", "index": index,
                "content_block": {"type": "tool_use", "id": tool.id, "name": tool.name, "input": {}}}));
        }
        if tool.emitted_arguments < tool.arguments.len() {
            events.push(json!({"type": "content_block_delta", "index": tool.block_index,
                "delta": {"type": "input_json_delta", "partial_json": &tool.arguments[tool.emitted_arguments..]}}));
            tool.emitted_arguments = tool.arguments.len();
        }
        Ok(())
    }

    fn push_content(&mut self, text: &str, kind: ContentKind, events: &mut Vec<Value>) {
        if self
            .active_content
            .is_some_and(|(_, active)| active != kind)
        {
            self.close_content(events);
        }
        let index = if let Some((index, _)) = self.active_content {
            index
        } else {
            let index = self.next_index;
            self.next_index += 1;
            let block = match kind {
                ContentKind::Text => json!({"type": "text", "text": ""}),
                ContentKind::Thinking => json!({"type": "thinking", "thinking": ""}),
            };
            events.push(
                json!({"type": "content_block_start", "index": index, "content_block": block}),
            );
            self.active_content = Some((index, kind));
            index
        };
        let delta = match kind {
            ContentKind::Text => json!({"type": "text_delta", "text": text}),
            ContentKind::Thinking => json!({"type": "thinking_delta", "thinking": text}),
        };
        events.push(json!({"type": "content_block_delta", "index": index, "delta": delta}));
        self.content_seen = true;
    }

    fn close_content(&mut self, events: &mut Vec<Value>) {
        if let Some((index, _)) = self.active_content.take() {
            events.push(json!({"type": "content_block_stop", "index": index}));
        }
    }

    fn close_blocks(&mut self, events: &mut Vec<Value>) -> Result<(), String> {
        let reason = self.finish_reason.as_deref().expect("terminal reason");
        map_finish_reason(reason).map_err(|error| error.message)?;
        if (reason == "tool_calls") != !self.tools.is_empty() && reason != "length" {
            return Err("Upstream tool calls do not match finish_reason".into());
        }
        if reason == "content_filter" && !self.content_seen {
            self.push_content(
                "Response withheld by the upstream content filter.",
                ContentKind::Text,
                events,
            );
        }
        self.close_content(events);
        let indices = self
            .tools
            .iter()
            .filter(|(_, tool)| !tool.closed)
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        for index in indices {
            let tool = &self.tools[&index];
            if !tool.arguments.is_empty() {
                let input: Value = serde_json::from_str(&tool.arguments).map_err(|error| {
                    format!("Upstream tool arguments are invalid JSON: {error}")
                })?;
                if !input.is_object() {
                    return Err("Upstream tool arguments must decode to a JSON object".into());
                }
            }
            self.emit_tool(index, events)?;
            let tool = self.tools.get_mut(&index).expect("known tool");
            events.push(json!({"type": "content_block_stop", "index": tool.block_index}));
            tool.closed = true;
            tool.arguments = String::new();
        }
        Ok(())
    }

    fn finish(&mut self, done: bool) -> Result<Vec<Value>, String> {
        if !self.started {
            return Err("Upstream stream ended without a message".into());
        }
        let mut events = Vec::new();
        if self.finish_reason.is_none() {
            if !done || !self.content_seen {
                return Err("Upstream stream ended without a finish_reason".into());
            }
            self.finish_reason = Some(
                if self.tools.is_empty() {
                    "stop"
                } else {
                    "tool_calls"
                }
                .into(),
            );
            self.close_blocks(&mut events)?;
        }
        let reason = self.finish_reason.as_deref().expect("terminal reason");
        let stop_reason = if !self.refusal_text.is_empty() {
            "refusal"
        } else {
            map_finish_reason(reason).map_err(|error| error.message)?
        };
        let mut delta = json!({"stop_reason": stop_reason, "stop_sequence": Value::Null});
        if stop_reason == "refusal" {
            delta["stop_details"] = json!({"type": "refusal", "category": Value::Null,
                "explanation": if self.refusal_text.is_empty() { "Response withheld by the upstream content filter." } else { &self.refusal_text }});
        }
        events.push(
            json!({"type": "message_delta", "delta": delta, "usage": self.converted_usage.client}),
        );
        events.push(json!({"type": "message_stop"}));
        Ok(events)
    }

    fn usage_record(&self) -> Value {
        let mut record = json!({
            "timestamp": Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            "schemaVersion": 2,
            "requestId": self.request_id,
            "provider": self.provider,
            "modelName": self.model,
            "streaming": true,
            "outcome": if self.finish_reason.as_deref() == Some("stop") && !self.content_seen {
                "empty_end_turn"
            } else {
                "usable"
            }
        });
        record_usage_metadata(&mut record, &self.converted_usage);
        if let Some(model) = &self.response_model {
            record["model"] = Value::String(model.clone());
        }
        if let Some(usage) = &self.upstream_usage {
            record["usage"] = usage.clone();
        }
        record
    }

    fn error_record(&self, message: &str, phase: &str, elapsed: Duration) -> Value {
        let log_message = match phase {
            "upstream_sse_error" => "Upstream reported an SSE error (body omitted)".to_owned(),
            "sse_frame" => "Upstream SSE framing or UTF-8 decoding failed".to_owned(),
            _ => safe_text(message),
        };
        let mut error = json!({"message": log_message, "type": "stream_error", "phase": phase, "elapsedMs": elapsed.as_millis(),
            "requestElapsedMs":self.request_started.elapsed().as_millis(), "headersMs":self.headers_ms,
            "transfer":self.progress.lock().expect("progress lock").snapshot()});
        error["streamState"] = json!({
            "chunkIndex": self.chunk_index, "messageStarted": self.started,
            "contentSeen": self.content_seen, "finishReasonSeen": self.finish_reason.is_some(),
            "toolCount": self.tools.len(),
        });
        if matches!(phase, "protocol_conversion" | "sse_json" | "stream_finish") {
            error["direction"] = json!("chat_to_anthropic");
        }
        if let Some(details) = &self.error_details {
            error["response"] = json!({"error":crate::error::upstream_error_summary(details)});
        }
        if let Some(response) = &self.response_metadata {
            error["upstreamResponse"] = response.clone();
        }
        if let Some(details) = &self.diagnostic_details {
            error["diagnostic"] = details.clone();
        }
        json!({
            "timestamp": Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            "requestId": self.request_id,
            "provider": self.provider,
            "modelName": self.model,
            "streaming": true,
            "error": error,
            "upstreamRequestShape": self.request_summary,
        })
    }
}

fn generated_tool_id() -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let suffix: String = (0..24)
        .map(|_| CHARS[rand::rng().random_range(0..CHARS.len())] as char)
        .collect();
    format!("toolu_{suffix}")
}

#[cfg(test)]
#[path = "../../tests/unit/protocol/stream.rs"]
mod tests;
