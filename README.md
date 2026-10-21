# cc-proxy

`cc-proxy` adapts Anthropic Messages API requests to one configured OpenAI-compatible `/v1/chat/completions` upstream. The npm CLI owns configuration and process lifecycle; the embedded Rust service owns HTTP, protocol conversion, streaming, and bounded JSONL observability. Node.js does not proxy requests or provide a fallback service.

## Conversion contract

- `tools` and `stop_sequences` must use their documented array shapes. A named `tool_choice` must reference a declared tool, and tool results must complete the immediately preceding assistant tool-use round.
- The Chat token-limit field defaults to `max_completion_tokens`; configure `upstreamCapabilities.maxTokensField: "max_tokens"` for endpoints that require the older field. Explicit thinking disablement sends `reasoning_effort: "none"` on the generic path, so an unsupported upstream reports its own error.
- Strict JSON schema output is forwarded only when its root shape can pass Chat strict mode without rewriting the schema. Non-streaming upstream responses have bounded time and size.
- JSON and SSE responses reject undeclared tools, audio, non-empty annotations, malformed tool arguments, and unsupported finish reasons instead of silently dropping them.
- Streamed reasoning is preserved even when it follows a tool call; later argument fragments remain attached to the original tool block.
- Qwen uses the generic Chat conversion: preserve the client's streaming mode, map thinking/effort to `reasoning_effort`, and omit `enable_thinking` and historical reasoning. No Qwen-specific fallback is applied.
- GLM-5.2 maps effort to `high`/`max`, while GLM-5.3 maps it to `low`/`high`/`max`. Other GLM models reject unsupported explicit effort. GLM family matching is case-insensitive and uses the model-name prefix.
- Responses include both `request-id` and `x-request-id`. Upstream error bodies are capped at 64 KiB, and valid `Retry-After` values are forwarded without automatic request retries.
- Stream failures log a diagnostic phase and elapsed time in `error_logs/*.jsonl`; the terminal adds those safe fields to its request summary for correlation.
- Conversion failures log the direction, validation message and fixed-path JSON field types; SSE diagnostics also identify the chunk and conversion state without copying content or tool argument values.
- Customer diagnostics cover streaming and non-streaming transport failures, safe request controls, upstream request IDs, byte/timing counters and package/build/platform identity. Startup prints the actual error-log directory; `/health` reports queue drops and file I/O failures. Upstream error bodies are omitted from JSONL. See [diagnostic fields and collection guidance](docs/API.md#customer-diagnostics).
- Prompt usage is split into uncached input, cache reads, and cache writes when the upstream counters are complete. Missing or invalid usage never discards an otherwise valid answer; degraded values are explicitly classified in usage JSONL and are not suitable for exact billing.

See [docs/API.md](docs/API.md) for the complete request, response, streaming, and degradation rules.

## Installation

Each GitHub Release provides one offline npm tarball containing the CLI, production dependencies, and native binaries for macOS Intel (`darwin-x64`), macOS Apple Silicon (`darwin-arm64`), Linux glibc x64, and Windows x64. Install the release tarball with:

```bash
npm install -g ./cc-proxy-<version>.tgz
```

GitHub Actions creates this tarball only after the release tag matches `package.json`'s version; local development commands do not produce a release package.

## Development checks

Run commands from the repository root:

```bash
npm test -- --runInBand
npm run lint
npm run build
cargo test --manifest-path native/Cargo.toml
cargo fmt --manifest-path native/Cargo.toml --check
cargo clippy --manifest-path native/Cargo.toml --all-targets -- -D warnings
cargo build --manifest-path native/Cargo.toml --release
```

## Live REST acceptance

Run this manually with a JSON Config outside the repository and its configured API key environment variable available in the current shell:

```bash
cargo build --manifest-path native/Cargo.toml --release
node tests/live-e2e.mjs ~/.claude-adapter/
```

The test starts the Rust REST service directly, selects `models.sonnet` by default (or accepts a model name as the second argument), and checks non-streaming text, streaming text, and a tool-result/final-answer round trip against the configured real model. It prints only pass/fail summaries. Temporary logs are removed after the run; the external Config is not changed.
