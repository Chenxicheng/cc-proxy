use std::{
    io::{BufRead, BufReader, Read, Write},
    process::{Child, Command, Stdio},
    thread,
};

use serde_json::{Value, json};

struct Proxy(Child);

impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn serve_upstream(listener: tokio::net::TcpListener) {
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            break;
        };
        tokio::spawn(async move {
            use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

            let mut reader = BufReader::new(&mut socket);
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                    return;
                }
                if line == "\r\n" {
                    break;
                }
                headers.push_str(&line);
            }
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            let mut body = vec![0; content_length];
            if reader.read_exact(&mut body).await.is_err() {
                return;
            }
            drop(reader);
            let request: Value = serde_json::from_slice(&body).unwrap();
            let model = request["model"].as_str().unwrap_or("");
            if matches!(model, "nonstream-eof" | "rejection-eof") {
                let status = if model == "rejection-eof" {
                    "429 Too Many Requests"
                } else {
                    "200 OK"
                };
                socket.write_all(format!("HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: 999\r\nx-request-id: upstream-case-123\r\nset-cookie: private-cookie\r\nconnection: close\r\n\r\n{{\"private\":\"partial\"").as_bytes()).await.unwrap();
                return;
            }
            if model == "qwen3.8-27b" {
                assert_eq!(request["stream"], false);
                assert_eq!(request["enable_thinking"], true);
                assert!(request.get("reasoning_effort").is_none());
                assert_eq!(request["max_tokens"], 32);
            }
            let (payload, status) = match model {
                "field-shape" => (
                    "data: {\"choices\":[{\"delta\":{\"content\":{\"private\":true}},\"finish_reason\":null}]}\r\n\r\n",
                    "200 OK",
                ),
                "invalid-json" => ("data: {broken\r\n\r\n", "200 OK"),
                _ => (
                    "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"baseline-ok\"},\"finish_reason\":\"stop\"}]}",
                    "200 OK",
                ),
            };
            if model == "qwen3.8-stream" {
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n100\r\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":null}]}\n\n",
                    )
                    .await;
                return;
            }
            let body = payload.as_bytes();
            let streaming = request["stream"] == true;
            let response = if streaming {
                format!(
                    "HTTP/1.1 {status}\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n{:X}\r\n",
                    body.len()
                )
            } else {
                format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                )
            };
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.write_all(body).await;
            if streaming {
                let _ = socket.write_all(b"\r\n0\r\n\r\n").await;
            }
        });
    }
}

fn read_error_logs(directory: &std::path::Path) -> Vec<Value> {
    std::fs::read_dir(directory.join("error_logs"))
        .unwrap()
        .flat_map(|file| {
            std::fs::read_to_string(file.unwrap().path())
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect::<Vec<Value>>()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diagnostics_are_preserved_through_the_native_http_process() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_url = format!("http://{}/v1", listener.local_addr().unwrap());
    let upstream = tokio::spawn(serve_upstream(listener));
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("config.json");
    std::fs::write(
        &config,
        json!({
            "baseUrl": upstream_url, "apiKey": "local-test-key",
            "models": {
                "opus": "baseline", "sonnet": "field-shape", "haiku": "invalid-json",
                "qwen3.8-27b": "qwen-truncated"
            }
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

    let mut request_ids = Vec::new();
    for (model, streaming, status, thinking) in [
        ("baseline", false, 200, false),
        ("field-shape", true, 200, false),
        ("invalid-json", true, 200, false),
        ("qwen3.8-27b", false, 200, true),
        ("qwen3.8-stream", true, 200, true),
        ("nonstream-eof", false, 502, false),
        ("rejection-eof", false, 502, false),
    ] {
        let mut request = json!({
            "model": model, "stream": streaming, "max_tokens": 32,
            "messages": [{"role": "user", "content": "diagnostic E2E fixture"}]
        });
        if thinking {
            request["thinking"] = json!({"type": "enabled", "budget_tokens": 1024});
        }
        let response = client.post(&url).json(&request).send().await.unwrap();
        let actual_status = response.status().as_u16();
        let id = response.headers()["x-request-id"]
            .to_str()
            .unwrap()
            .to_owned();
        assert_eq!(response.headers()["request-id"], id);
        let body = response.text().await.unwrap();
        assert_eq!(actual_status, status, "{model}: {body}");
        if model == "baseline" || model == "qwen3.8-27b" {
            assert!(body.contains("baseline-ok"));
        } else if model.starts_with("qwen3.8") {
            assert!(!body.contains("errorChain"));
            assert!(!body.contains("upstreamResponse"));
            assert!(!body.contains("end of file before message length reached"));
            assert!(!body.contains("message_stop"));
        } else {
            assert!(!body.contains("message_stop"), "{model}: {body}");
            assert!(!body.contains("protocol_conversion"));
            assert!(!body.contains("jsonColumn"));
            assert!(!body.contains("private"));
        }
        request_ids.push((model, id));
    }

    upstream.abort();
    let _ = upstream.await;
    let response = client
        .post(&url)
        .json(&json!({
            "model":"connect-error", "max_tokens":32, "stream":false,
            "messages":[{"role":"user","content":"private-connect-prompt"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 502);
    request_ids.push((
        "connect-error",
        response.headers()["x-request-id"]
            .to_str()
            .unwrap()
            .to_owned(),
    ));
    response.bytes().await.unwrap();
    let health: Value = client
        .get(url.replace("/v1/messages", "/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["logging"]["ioFailureCount"], 0);
    assert!(
        health["logging"]["errorDirectory"]
            .as_str()
            .unwrap()
            .ends_with("error_logs")
    );
    proxy
        .0
        .stdin
        .take()
        .unwrap()
        .write_all(b"shutdown\n")
        .unwrap();
    assert!(proxy.0.wait().unwrap().success());
    let logs = logs.join().unwrap();
    let errors = read_error_logs(directory.path());
    assert_eq!(errors.len(), 6);
    let connection = errors
        .iter()
        .find(|r| r["modelName"] == "connect-error")
        .unwrap();
    assert_eq!(connection["error"]["phase"], "upstream_connect");
    assert!(connection["error"]["errorChain"].as_array().unwrap().len() > 1);
    assert!(connection["error"]["upstreamResponse"].is_null());
    assert!(!connection.to_string().contains("private-connect-prompt"));
    for record in &errors {
        assert_eq!(record["diagnosticsVersion"], 1);
        let package: Value = serde_json::from_str(include_str!("../../package.json")).unwrap();
        assert_eq!(record["build"]["packageVersion"], package["version"]);
        assert!(record["build"]["revision"].is_string());
        assert!(record["build"]["os"].is_string());
        assert!(record["upstreamRequestShape"]["clientStream"].is_boolean());
        assert_eq!(
            record["upstreamRequestShape"]["conversionPolicy"],
            if record["modelName"]
                .as_str()
                .is_some_and(|model| model.starts_with("qwen"))
            {
                "qwen"
            } else {
                "generic_chat"
            }
        );
        assert!(!record.to_string().contains("diagnostic E2E fixture"));
        assert!(!record.to_string().contains("local-test-key"));
        assert!(!record.to_string().contains("private-cookie"));
    }
    for (model, upstream_status) in [("nonstream-eof", 200), ("rejection-eof", 429)] {
        let record = errors.iter().find(|r| r["modelName"] == model).unwrap();
        let error = &record["error"];
        assert_eq!(error["phase"], "upstream_transport");
        assert_transport_chain_has_body_eof(error["errorChain"].as_array().unwrap());
        assert_eq!(error["upstreamResponse"]["status"], upstream_status);
        assert_eq!(error["upstreamResponse"]["xRequestId"], "upstream-case-123");
        assert_eq!(error["upstreamResponse"]["contentLength"], 999);
        assert!(error["transfer"]["receivedBytes"].as_u64().unwrap() > 0);
        assert!(error["transfer"]["firstByteMs"].is_number());
        assert!(error["transfer"]["sinceLastByteMs"].is_number());
        assert!(error["headersMs"].is_number());
        assert!(error["requestElapsedMs"].is_number());
    }

    for (model, id) in request_ids
        .iter()
        .filter(|(model, _)| *model != "baseline" && *model != "qwen3.8-27b")
    {
        let records: Vec<_> = errors
            .iter()
            .filter(|record| record["requestId"] == id.as_str())
            .collect();
        assert_eq!(records.len(), 1, "{model}");
        assert_eq!(
            logs.lines()
                .filter(|line| line.contains("[error]") && line.contains(id))
                .count(),
            1,
            "{model}"
        );
        assert!(
            !logs
                .lines()
                .any(|line| { line.contains("[completed]") && line.contains(id) })
        );
    }

    let malformed = errors
        .iter()
        .find(|record| record["requestId"] == request_ids[1].1)
        .unwrap();
    assert_eq!(
        malformed["error"]["phase"], "protocol_conversion",
        "{malformed}"
    );
    assert_eq!(malformed["error"]["direction"], "chat_to_anthropic");
    assert_eq!(
        malformed["error"]["message"],
        "Upstream stream delta.content must be a string or null"
    );
    assert_eq!(malformed["error"]["streamState"]["chunkIndex"], 1);
    assert_eq!(
        malformed["error"]["diagnostic"]["fieldTypes"]["/choices/0/delta/content"],
        "object"
    );
    assert!(!malformed.to_string().contains("private"));

    let invalid_json = errors
        .iter()
        .find(|record| record["requestId"] == request_ids[2].1)
        .unwrap();
    assert_eq!(invalid_json["error"]["phase"], "sse_json");
    assert_eq!(invalid_json["error"]["streamState"]["chunkIndex"], 1);
    assert!(invalid_json["error"]["diagnostic"]["jsonLine"].is_number());
    assert!(invalid_json["error"]["diagnostic"]["jsonColumn"].is_number());

    let qwen_streaming = errors
        .iter()
        .find(|record| record["requestId"] == request_ids[4].1)
        .unwrap();
    assert_eq!(qwen_streaming["streaming"], true);
    assert_eq!(qwen_streaming["error"]["phase"], "upstream_transport");
    let streaming_chain = qwen_streaming["error"]["diagnostic"]["errorChain"]
        .as_array()
        .unwrap();
    assert_transport_chain_has_body_eof(streaming_chain);
    assert_eq!(qwen_streaming["error"]["upstreamResponse"]["status"], 200);
    assert!(
        qwen_streaming["error"]["transfer"]["receivedBytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(
        qwen_streaming["upstreamRequestShape"]["reasoningEffort"],
        Value::Null
    );
    assert_eq!(
        qwen_streaming["upstreamRequestShape"]["enableThinking"],
        true
    );
}

fn assert_transport_chain_has_body_eof(chain: &[Value]) {
    assert!(chain.len() >= 2, "{chain:?}");
    assert!(
        chain.iter().any(|entry| {
            entry
                .as_str()
                .is_some_and(|message| message.contains("error decoding response body"))
        }),
        "{chain:?}"
    );
    assert!(
        chain.iter().any(|entry| {
            entry.as_str().is_some_and(|message| {
                message.contains("end of file before message length reached")
            })
        }),
        "missing underlying EOF cause: {chain:?}"
    );
}
