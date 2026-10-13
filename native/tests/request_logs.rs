use std::{
    io::{BufRead, BufReader, Read, Write},
    process::{Child, Command, Stdio},
    thread,
};

use axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::post};
use serde_json::{Value, json};

struct Proxy(Child);

impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn upstream(Json(request): Json<Value>) -> axum::response::Response {
    match request["model"].as_str().unwrap() {
        "http-error" => (StatusCode::TOO_MANY_REQUESTS, "private-upstream-body").into_response(),
        "invalid-response" => Json(json!({"choices": []})).into_response(),
        "stream-ok" => ([ ("content-type", "text/event-stream") ], concat!(
            "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        )).into_response(),
        "stream-error" => ([ ("content-type", "text/event-stream") ],
            "data: {\"error\":{\"message\":\"private-stream-message\"}}\n\n").into_response(),
        "stream-invalid-json" => ([ ("content-type", "text/event-stream") ], "data: {broken\n\n").into_response(),
        "stream-truncated" => ([ ("content-type", "text/event-stream") ],
            "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n").into_response(),
        _ => Json(json!({"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]})).into_response(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_request_rejections_upstream_errors_and_stream_terminal_outcomes_once() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_url = format!("http://{}/v1", listener.local_addr().unwrap());
    let server = tokio::spawn(async {
        axum::serve(
            listener,
            Router::new().route("/v1/chat/completions", post(upstream)),
        )
        .await
        .unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("config.json");
    std::fs::write(
        &config,
        json!({
            "baseUrl": upstream_url, "apiKey": "local-test-key",
            "models": {"opus": "stream-ok", "sonnet": "stream-ok", "haiku": "stream-ok"}
        })
        .to_string(),
    )
    .unwrap();
    let mut proxy = Proxy(
        Command::new(env!("CARGO_BIN_EXE_cc-proxy-native"))
            .args(["--config", config.to_str().unwrap(), "--port", "0"])
            .env("CC_PROXY_STDIN_SHUTDOWN", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut stderr = proxy.0.stderr.take().unwrap();
    let logs = thread::spawn(move || {
        let mut logs = String::new();
        stderr.read_to_string(&mut logs).unwrap();
        logs
    });
    let mut ready = String::new();
    BufReader::new(proxy.0.stdout.take().unwrap())
        .read_line(&mut ready)
        .unwrap();
    let ready: Value =
        serde_json::from_str(ready.trim().strip_prefix("CC_PROXY_READY=").unwrap()).unwrap();
    let url = format!("{}/v1/messages", ready["url"].as_str().unwrap());
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap();
    let mut failures = Vec::new();
    let mut completed_id = String::new();
    for (model, streaming, status, expect_error) in [
        ("stream-ok", true, 200, false),
        ("http-error", false, 429, true),
        ("http-error", true, 429, true),
        ("invalid-response", false, 502, true),
        ("stream-error", true, 200, true),
        ("stream-invalid-json", true, 200, true),
        ("stream-truncated", true, 200, true),
    ] {
        let response = client
            .post(&url)
            .json(&json!({
                "model": model, "stream": streaming, "max_tokens": 16,
                "messages": [{"role": "user", "content": "local fixture"}]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status, "{model}");
        let id = response.headers()["x-request-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let body = response.text().await.unwrap();
        if expect_error {
            assert!(!body.contains("event: message_stop"), "{model}");
            failures.push(id);
        } else {
            assert!(body.contains("event: message_stop"));
            completed_id = id;
        }
    }
    for (body, status) in [
        ("{broken".to_owned(), 400),
        (
            json!({"model": "invalid-request", "messages": []}).to_string(),
            400,
        ),
        ("x".repeat(32 * 1024 * 1024 + 1), 413),
    ] {
        let response = client.post(&url).body(body).send().await.unwrap();
        assert_eq!(response.status().as_u16(), status);
        failures.push(
            response.headers()["x-request-id"]
                .to_str()
                .unwrap()
                .to_owned(),
        );
        response.bytes().await.unwrap();
    }
    proxy
        .0
        .stdin
        .take()
        .unwrap()
        .write_all(b"shutdown\n")
        .unwrap();
    assert!(proxy.0.wait().unwrap().success());
    let logs = logs.join().unwrap();
    assert_eq!(
        logs.lines()
            .filter(|line| line.contains("[completed]"))
            .count(),
        1
    );
    assert!(
        logs.lines()
            .any(|line| line.contains("[completed]") && line.contains(&completed_id))
    );
    assert_eq!(
        logs.lines().filter(|line| line.contains("[error]")).count(),
        failures.len()
    );
    assert!(!logs.contains("private-upstream-body"));
    assert!(!logs.contains("private-stream-message"));
    let errors: Vec<Value> = std::fs::read_dir(directory.path().join("error_logs"))
        .unwrap()
        .flat_map(|file| {
            std::fs::read_to_string(file.unwrap().path())
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(errors.len(), failures.len());
    for id in failures {
        assert_eq!(
            logs.lines()
                .filter(|line| line.contains("[error]") && line.contains(&id))
                .count(),
            1
        );
        assert_eq!(
            errors
                .iter()
                .filter(|record| record["requestId"] == id)
                .count(),
            1
        );
        assert!(
            !logs
                .lines()
                .any(|line| line.contains("[completed]") && line.contains(&id))
        );
    }
    let usage_file = std::fs::read_dir(directory.path().join("token_usage"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let usage = std::fs::read_to_string(usage_file).unwrap();
    assert!(usage.contains(&completed_id));
    server.abort();
}
