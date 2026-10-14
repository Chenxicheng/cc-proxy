use super::*;

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
async fn qwen_nonstreaming_request_uses_streamed_upstream_and_returns_json() {
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
                        (
                            [(header::CONTENT_TYPE, "text/event-stream")],
                            if fail {
                                "data: {\"error\":{\"message\":\"provider failed\"}}\n\n".to_owned()
                            } else {
                                concat!(
                                "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think\"},\"finish_reason\":null}]}\n\n",
                                "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"id\\\":1}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
                                "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3}}\n\n",
                                "data: [DONE]\n\n"
                                ).to_owned()
                            },
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
        assert_eq!(requests[0]["stream"], true);
        assert_eq!(
            requests[0]["stream_options"],
            json!({"include_usage": true})
        );
        assert_eq!(requests[0]["enable_thinking"], true);
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
