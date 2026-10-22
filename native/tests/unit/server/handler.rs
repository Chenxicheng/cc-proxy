use super::*;

#[test]
fn conversion_error_log_exposes_phase_and_direction_without_changing_client_error() {
    let error = AppError::bad_request("thinking.type: invalid").with_details(json!({
        "phase":"request_conversion", "direction":"anthropic_to_chat"
    }));
    let record = error_record_value("req", "provider", "model", false, &error);
    assert_eq!(record["error"]["phase"], "request_conversion");
    assert_eq!(record["error"]["direction"], "anthropic_to_chat");
    assert_eq!(record["error"]["message"], "thinking.type: invalid");
}

#[test]
fn error_record_preserves_sanitized_upstream_details() {
    let shape = json!({"modelFamily": "glm-5"});
    let error = AppError::new(StatusCode::NOT_ACCEPTABLE, "Not Acceptable").with_details(json!({
        "response": "Not Acceptable",
        "upstreamRequestShape": shape.clone(),
    }));

    let record = error_record_value("req_1", "https://provider.test/v1", "glm-5.2", true, &error);

    assert_eq!(record["error"]["status"], 406);
    assert_eq!(record["error"]["response"], "Not Acceptable");
    assert_eq!(record["upstreamRequestShape"], shape);
}

#[tokio::test]
async fn nonstream_upstream_rejection_of_none_is_returned_without_retry() {
    use axum::{Router, routing::post};
    use std::sync::{Arc, Mutex};

    let observed = Arc::new(Mutex::new(Vec::<Value>::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_url = format!("http://{}/v1", listener.local_addr().unwrap());
    let requests = observed.clone();
    let upstream = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/v1/chat/completions",
                post(move |Json(request): Json<Value>| {
                    let requests = requests.clone();
                    async move {
                        requests.lock().unwrap().push(request);
                        (
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": {"message": "none unsupported"}})),
                        )
                    }
                }),
            ),
        )
        .await
        .unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let (storage, writer) = crate::storage::Storage::start(directory.path().to_path_buf());
    let state = AppState {
        config: crate::config::AdapterConfig {
            base_url: upstream_url,
            api_key: Some("local-test-key".into()),
            api_key_env: None,
            upstream_headers: None,
            upstream_capabilities: Default::default(),
        },
        client: reqwest::Client::new(),
        storage,
    };
    let body = json!({
        "model": "vendor/custom", "max_tokens": 10,
        "thinking": {"type": "disabled"},
        "messages": [{"role": "user", "content": "hello"}]
    });
    let response = messages(State(state.clone()), Ok(Bytes::from(body.to_string()))).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&bytes).contains("none unsupported"));
    {
        let requests = observed.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["reasoning_effort"], "none");
        assert_eq!(requests[0]["max_completion_tokens"], 10);
    }
    drop(state);
    writer.await.unwrap();
    upstream.abort();
}

#[tokio::test]
async fn invalid_upstream_json_returns_bad_gateway() {
    use axum::{Router, routing::post};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_url = format!("http://{}/v1", listener.local_addr().unwrap());
    let upstream = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/v1/chat/completions", post(|| async { "not json" })),
        )
        .await
        .unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let (storage, writer) = crate::storage::Storage::start(directory.path().to_path_buf());
    let state = AppState {
        config: crate::config::AdapterConfig {
            base_url: upstream_url,
            api_key: Some("local-test-key".into()),
            api_key_env: None,
            upstream_headers: None,
            upstream_capabilities: Default::default(),
        },
        client: reqwest::Client::new(),
        storage,
    };
    let body = json!({"model": "custom", "max_tokens": 10,
        "messages": [{"role": "user", "content": "hello"}]});
    let response = messages(State(state.clone()), Ok(Bytes::from(body.to_string()))).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&bytes).contains("Invalid upstream JSON"));
    drop(state);
    writer.await.unwrap();
    upstream.abort();
}

#[tokio::test]
async fn qwen_nonstreaming_thinking_request_uses_json_upstream() {
    use std::sync::{Arc, Mutex};

    use axum::{Router, routing::post};

    let observed = Arc::new(Mutex::new(Vec::<Value>::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_url = format!("http://{}/v1", listener.local_addr().unwrap());
    let observed_request = observed.clone();
    let upstream = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/v1/chat/completions",
                post(move |Json(request): Json<Value>| {
                    let observed = observed_request.clone();
                    async move {
                        let fail = request["model"] == "qwen3-error";
                        observed.lock().unwrap().push(request);
                        Json(if fail {
                            json!({"error":{"message":"provider failed"}})
                        } else {
                            json!({"choices":[{"message":{"role":"assistant", "content":null, "reasoning_content":"think", "tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"id\":1}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":5,"completion_tokens":3}})
                        })
                    }
                }),
            ),
        )
        .await
        .unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let (storage, writer) = crate::storage::Storage::start(directory.path().to_path_buf());
    let state = AppState {
        config: crate::config::AdapterConfig {
            base_url: upstream_url,
            api_key: Some("local-test-key".into()),
            api_key_env: None,
            upstream_headers: None,
            upstream_capabilities: Default::default(),
        },
        client: reqwest::Client::new(),
        storage,
    };
    let body = json!({
        "model": "qwen/qwen3.6-35b-a3b",
        "max_tokens": 64,
        "stream": false,
        "thinking": {"type": "enabled"},
        "messages": [{"role": "user", "content": "hello"}],
        "tools": [{"name": "lookup", "input_schema": {"type": "object"}}]
    });
    let response = messages(State(state.clone()), Ok(Bytes::from(body.to_string()))).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let message: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        message["content"][0],
        json!({"type":"thinking","thinking":"think"})
    );
    assert_eq!(message["content"][1]["type"], "tool_use");
    assert_eq!(message["content"][1]["input"], json!({"id":1}));
    assert_eq!(message["stop_reason"], "tool_use");
    assert_eq!(
        message["usage"],
        json!({"input_tokens":5,"output_tokens":3})
    );
    {
        let requests = observed.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["stream"], false);
        assert!(requests[0].get("stream_options").is_none());
        assert_eq!(requests[0]["enable_thinking"], true);
        assert!(requests[0].get("reasoning_effort").is_none());
        assert_eq!(requests[0]["max_tokens"], 64);
        assert!(requests[0].get("max_completion_tokens").is_none());
    }

    let mut failed_body = body;
    failed_body["model"] = json!("qwen3-error");
    let failed = messages(
        State(state.clone()),
        Ok(Bytes::from(failed_body.to_string())),
    )
    .await;
    assert_eq!(failed.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(failed.headers()[header::CONTENT_TYPE], "application/json");
    let bytes = axum::body::to_bytes(failed.into_body(), usize::MAX)
        .await
        .unwrap();
    let error: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(error["type"], "error");
    drop(state);
    writer.await.unwrap();
    let usage = std::fs::read_dir(directory.path().join("token_usage"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let records = std::fs::read_to_string(usage).unwrap();
    let records: Vec<Value> = records
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["streaming"], false);
    let errors = std::fs::read_dir(directory.path().join("error_logs"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let records = std::fs::read_to_string(errors).unwrap();
    let records: Vec<Value> = records
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["streaming"], false);
    assert_eq!(records[0]["error"]["status"], 502);
    upstream.abort();
}
