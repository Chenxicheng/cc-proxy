use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::error::safe_text;
use chrono::Utc;
use serde_json::{Value, json};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
    task::JoinHandle,
    time,
};

const QUEUE_CAPACITY: usize = 1024;
const FLUSH_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy)]
enum RecordKind {
    Usage,
    Error,
}

struct WriteRecord {
    kind: RecordKind,
    line: Vec<u8>,
}

#[derive(Default)]
struct LogHealth {
    io_failures: AtomicU64,
    last_failure: Mutex<Option<Value>>,
}

impl LogHealth {
    fn failure(&self, operation: &str, error: std::io::Error, base_dir: &Path) {
        self.io_failures.fetch_add(1, Ordering::Relaxed);
        *self.last_failure.lock().expect("log health lock") = Some(json!({
            "operation":operation, "kind":format!("{:?}", error.kind()),
            "timestamp":Utc::now().to_rfc3339(),
        }));
        eprintln!(
            "[adapter] Failed to persist logs ({operation}, {:?}); check permissions and free disk space at {:?}",
            error.kind(),
            base_dir
        );
    }
}

#[derive(Clone)]
pub struct Storage {
    sender: mpsc::Sender<WriteRecord>,
    dropped: Arc<AtomicU64>,
    health: Arc<LogHealth>,
    base_dir: Arc<PathBuf>,
    secrets: Arc<Vec<String>>,
}

impl Storage {
    pub fn start(base_dir: PathBuf) -> (Self, JoinHandle<()>) {
        let base_dir = if base_dir.is_relative() {
            std::env::current_dir()
                .map(|cwd| cwd.join(&base_dir))
                .unwrap_or(base_dir)
        } else {
            base_dir
        };
        let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        let health = Arc::new(LogHealth::default());
        let base_dir = Arc::new(base_dir);
        let handle = tokio::spawn(writer_loop(
            base_dir.as_ref().clone(),
            receiver,
            health.clone(),
        ));
        (
            Self {
                sender,
                dropped,
                health,
                base_dir,
                secrets: Arc::new(Vec::new()),
            },
            handle,
        )
    }

    pub fn with_redactions(mut self, secrets: Vec<String>) -> Self {
        let mut secrets: Vec<_> = secrets.into_iter().filter(|v| !v.is_empty()).collect();
        secrets.sort_by_key(|value| std::cmp::Reverse(value.len()));
        secrets.dedup();
        self.secrets = Arc::new(secrets);
        self
    }

    pub fn health(&self) -> Value {
        json!({"ioFailureCount":self.health.io_failures.load(Ordering::Relaxed),
            "lastIoFailure":*self.health.last_failure.lock().expect("log health lock"),
            "droppedRecords":self.dropped_count(),
            "errorDirectory":self.base_dir.join("error_logs"),
            "dateTimezone":"UTC"})
    }

    pub fn record_usage(&self, record: Value) {
        self.enqueue(RecordKind::Usage, record);
    }

    pub fn record_error(&self, mut record: Value) {
        record["diagnosticsVersion"] = json!(1);
        record["build"] = build_info().clone();
        record["logging"] = json!({"ioFailureCount":self.health.io_failures.load(Ordering::Relaxed), "droppedRecords":self.dropped_count()});
        if let Some(provider) = record.get_mut("provider") {
            *provider = provider
                .as_str()
                .and_then(|v| url::Url::parse(v).ok())
                .map(|url| json!(url.origin().ascii_serialization()))
                .filter(|origin| {
                    !self
                        .secrets
                        .iter()
                        .any(|secret| origin.as_str().is_some_and(|value| value.contains(secret)))
                })
                .unwrap_or(Value::Null);
        }
        let provider = record.get("provider").cloned();
        sanitize_record(&mut record, &self.secrets, 0);
        if let Some(provider) = provider {
            record["provider"] = provider;
        }
        eprintln!("{}", error_summary(&record));
        self.enqueue(RecordKind::Error, record);
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    fn enqueue(&self, kind: RecordKind, mut record: Value) {
        if let Some(provider) = record.get_mut("provider") {
            *provider = provider
                .as_str()
                .and_then(|v| url::Url::parse(v).ok())
                .map(|url| json!(url.origin().ascii_serialization()))
                .filter(|origin| {
                    !self
                        .secrets
                        .iter()
                        .any(|secret| origin.as_str().is_some_and(|value| value.contains(secret)))
                })
                .unwrap_or(Value::Null);
        }
        let mut line = match serde_json::to_vec(&record) {
            Ok(line) => line,
            Err(error) => {
                eprintln!("[adapter] Failed to serialize JSONL record: {error}");
                return;
            }
        };
        line.push(b'\n');

        if self.sender.try_send(WriteRecord { kind, line }).is_err() {
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped == 1 || dropped.is_multiple_of(100) {
                eprintln!("[adapter] JSONL writer unavailable; dropped {dropped} records");
            }
        }
    }
}

fn error_summary(record: &Value) -> String {
    // Summaries exclude upstream messages and bodies, which may contain private data.
    let model = record["modelName"].as_str().unwrap_or("unknown");
    let request_id = record["requestId"].as_str().unwrap_or("unknown");
    let error_type = record["error"]["type"].as_str().unwrap_or("api_error");
    let status = record["error"]["status"]
        .as_u64()
        .map_or_else(|| "n/a".to_owned(), |status| status.to_string());
    let phase = record["error"]["phase"]
        .as_str()
        .map_or_else(String::new, |phase| format!(" phase={phase:?}"));
    let elapsed = record["error"]["elapsedMs"]
        .as_u64()
        .map_or_else(String::new, |elapsed| format!(" elapsed_ms={elapsed}"));
    format!(
        "<- {model:?} [error] {request_id:?} status={status} type={error_type:?}{phase}{elapsed} (see error_logs)"
    )
}

struct DailyWriter {
    date: String,
    usage: BufWriter<File>,
    error: BufWriter<File>,
    buffered: usize,
}

impl DailyWriter {
    async fn open(base_dir: &Path, date: String) -> std::io::Result<Self> {
        let usage_dir = base_dir.join("token_usage");
        let error_dir = base_dir.join("error_logs");
        tokio::fs::create_dir_all(&usage_dir).await?;
        tokio::fs::create_dir_all(&error_dir).await?;

        Ok(Self {
            usage: BufWriter::new(open_append(usage_dir.join(format!("{date}.jsonl"))).await?),
            error: BufWriter::new(open_append(error_dir.join(format!("{date}.jsonl"))).await?),
            date,
            buffered: 0,
        })
    }

    async fn write(&mut self, record: WriteRecord) -> std::io::Result<()> {
        let length = record.line.len();
        match record.kind {
            RecordKind::Usage => self.usage.write_all(&record.line).await?,
            RecordKind::Error => self.error.write_all(&record.line).await?,
        }
        self.buffered += length;
        Ok(())
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        self.usage.flush().await?;
        self.error.flush().await?;
        self.buffered = 0;
        Ok(())
    }
}

async fn open_append(path: PathBuf) -> std::io::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
}

fn today() -> String {
    Utc::now().format("%Y-%m-%d").to_string()
}

async fn writer_loop(
    base_dir: PathBuf,
    receiver: mpsc::Receiver<WriteRecord>,
    health: Arc<LogHealth>,
) {
    writer_loop_with_date(base_dir, receiver, today, health).await;
}

async fn writer_loop_with_date<F>(
    base_dir: PathBuf,
    mut receiver: mpsc::Receiver<WriteRecord>,
    mut current_date: F,
    health: Arc<LogHealth>,
) where
    F: FnMut() -> String,
{
    let mut writer: Option<DailyWriter> = None;
    let mut interval = time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            record = receiver.recv() => {
                let Some(record) = record else { break };
                let date = current_date();
                if writer.as_ref().is_none_or(|current| current.date != date) {
                    if let Some(current) = writer.as_mut()
                        && let Err(error) = current.flush().await {
                        health.failure("rotate_flush", error, &base_dir);
                    }
                    writer = match DailyWriter::open(&base_dir, date).await {
                        Ok(next) => Some(next),
                        Err(error) => {
                            health.failure("open", error, &base_dir);
                            None
                        }
                    };
                }
                if let Some(current) = writer.as_mut() {
                    if let Err(error) = current.write(record).await {
                        health.failure("write", error, &base_dir);
                    } else if current.buffered >= FLUSH_BYTES
                        && let Err(error) = current.flush().await {
                        health.failure("flush", error, &base_dir);
                    }
                }
            }
            _ = interval.tick() => {
                if let Some(current) = writer.as_mut()
                    && current.buffered > 0
                    && let Err(error) = current.flush().await
                {
                    health.failure("flush", error, &base_dir);
                }
            }
        }
    }

    if let Some(current) = writer.as_mut()
        && let Err(error) = current.flush().await
    {
        health.failure("shutdown_flush", error, &base_dir);
    }
}

fn build_info() -> &'static Value {
    static INFO: OnceLock<Value> = OnceLock::new();
    INFO.get_or_init(|| {
        let package: Value =
            serde_json::from_str(include_str!("../../package.json")).expect("package metadata");
        json!({"packageVersion":package["version"], "nativeVersion":env!("CARGO_PKG_VERSION"),
            "revision":option_env!("CC_PROXY_BUILD_SHA").unwrap_or("unknown-local-build"),
            "os":std::env::consts::OS, "arch":std::env::consts::ARCH})
    })
}

fn sanitize_record(value: &mut Value, secrets: &[String], depth: usize) {
    if depth > 12 {
        *value = json!("<omitted>");
        return;
    }
    match value {
        Value::String(text) => {
            for secret in secrets {
                *text = text.replace(secret, "<redacted>");
            }
            *text = safe_text(text);
        }
        Value::Array(items) => {
            items.truncate(128);
            for item in items {
                sanitize_record(item, secrets, depth + 1);
            }
        }
        Value::Object(fields) => {
            let original = std::mem::take(fields);
            for (mut key, mut item) in original.into_iter().take(128) {
                for secret in secrets {
                    key = key.replace(secret, "<redacted>");
                }
                key = safe_text(&key);
                sanitize_record(&mut item, secrets, depth + 1);
                fields.insert(key, item);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
#[path = "../tests/unit/storage.rs"]
mod tests;
