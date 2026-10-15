use std::sync::{Arc, atomic::AtomicU64};

use serde_json::json;
use tempfile::tempdir;
use tokio::sync::mpsc;

use super::*;

#[test]
fn terminal_error_summary_excludes_private_details_and_escapes_controls() {
    let record = json!({
        "modelName": "model\n\u{1b}[2J",
        "requestId": "req_1",
        "error": {"status": 429, "type": "rate_limit_error", "message": "private-message", "response": "private-body"}
    });
    let summary = error_summary(&record);
    assert!(summary.contains("[error]"));
    assert!(summary.contains("req_1"));
    assert!(summary.contains("status=429"));
    assert!(summary.contains("rate_limit_error"));
    assert!(!summary.contains("private"));
    assert!(!summary.contains('\n'));
    assert!(!summary.contains('\u{1b}'));
}

#[test]
fn terminal_stream_error_summary_shows_safe_phase_and_elapsed_time() {
    let summary = error_summary(&json!({
        "modelName": "qwen3.8-27b",
        "requestId": "req_1",
        "error": {
            "type": "stream_error",
            "phase": "first_body_bytes",
            "elapsedMs": 120_000,
            "message": "private upstream body"
        }
    }));
    assert!(summary.contains("phase=\"first_body_bytes\""));
    assert!(summary.contains("elapsed_ms=120000"));
    assert!(!summary.contains("private upstream body"));
}

#[tokio::test]
async fn flushes_queued_records_on_shutdown() {
    let directory = tempdir().unwrap();
    let (storage, handle) = Storage::start(directory.path().to_path_buf());
    storage.record_usage(json!({"value": 1}));
    storage.record_error(json!({"message": "failed"}));
    drop(storage);
    handle.await.unwrap();

    let date = today();
    let usage = tokio::fs::read_to_string(
        directory
            .path()
            .join("token_usage")
            .join(format!("{date}.jsonl")),
    )
    .await
    .unwrap();
    let errors = tokio::fs::read_to_string(
        directory
            .path()
            .join("error_logs")
            .join(format!("{date}.jsonl")),
    )
    .await
    .unwrap();
    assert_eq!(usage, "{\"value\":1}\n");
    assert_eq!(errors, "{\"message\":\"failed\"}\n");
}

#[tokio::test]
async fn writes_concurrent_records_as_complete_json_lines() {
    let directory = tempdir().unwrap();
    let (storage, handle) = Storage::start(directory.path().to_path_buf());
    let tasks = (0..100).map(|value| {
        let storage = storage.clone();
        tokio::spawn(async move { storage.record_usage(json!({"value": value})) })
    });
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(storage.dropped_count(), 0);
    drop(storage);
    handle.await.unwrap();

    let contents = tokio::fs::read_to_string(
        directory
            .path()
            .join("token_usage")
            .join(format!("{}.jsonl", today())),
    )
    .await
    .unwrap();
    let mut values = contents
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line).unwrap()["value"]
                .as_u64()
                .unwrap()
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, (0..100).collect::<Vec<_>>());
}

#[test]
fn counts_records_when_queue_is_closed() {
    let (sender, receiver) = mpsc::channel(1);
    drop(receiver);
    let storage = Storage {
        sender,
        dropped: Arc::new(AtomicU64::new(0)),
    };
    storage.record_usage(json!({"value": 1}));
    assert_eq!(storage.dropped_count(), 1);
}

#[test]
fn drops_without_blocking_when_queue_is_full() {
    let (sender, _receiver) = mpsc::channel(1);
    let storage = Storage {
        sender,
        dropped: Arc::new(AtomicU64::new(0)),
    };
    storage.record_usage(json!({"value": 1}));
    storage.record_usage(json!({"value": 2}));
    assert_eq!(storage.dropped_count(), 1);
}

#[tokio::test]
async fn rotates_daily_files_in_writer_loop() {
    let directory = tempdir().unwrap();
    let (sender, receiver) = mpsc::channel(2);
    let dates =
        std::collections::VecDeque::from(["2026-09-18".to_owned(), "2026-09-19".to_owned()]);
    let handle = tokio::spawn(writer_loop_with_date(
        directory.path().to_path_buf(),
        receiver,
        {
            let mut dates = dates;
            move || dates.pop_front().unwrap()
        },
    ));
    sender
        .send(WriteRecord {
            kind: RecordKind::Usage,
            line: b"{\"day\":1}\n".to_vec(),
        })
        .await
        .unwrap();
    sender
        .send(WriteRecord {
            kind: RecordKind::Usage,
            line: b"{\"day\":2}\n".to_vec(),
        })
        .await
        .unwrap();
    drop(sender);
    handle.await.unwrap();

    assert_eq!(
        tokio::fs::read_to_string(directory.path().join("token_usage/2026-09-18.jsonl"))
            .await
            .unwrap(),
        "{\"day\":1}\n"
    );
    assert_eq!(
        tokio::fs::read_to_string(directory.path().join("token_usage/2026-09-19.jsonl"))
            .await
            .unwrap(),
        "{\"day\":2}\n"
    );
}

#[tokio::test]
async fn survives_jsonl_open_failure() {
    let directory = tempdir().unwrap();
    let blocked_path = directory.path().join("not-a-directory");
    tokio::fs::write(&blocked_path, b"file").await.unwrap();
    let (storage, handle) = Storage::start(blocked_path);
    storage.record_usage(json!({"value": 1}));
    drop(storage);
    handle.await.unwrap();
}
