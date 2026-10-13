# cc-proxy

`cc-proxy` adapts Anthropic Messages API requests to one configured OpenAI-compatible `/v1/chat/completions` upstream. The npm CLI owns configuration and process lifecycle; the embedded Rust service owns HTTP, protocol conversion, streaming, and bounded JSONL observability. Node.js does not proxy requests or provide a fallback service.

## Conversion contract

- `tools` and `stop_sequences` must use their documented array shapes. A named `tool_choice` must reference a declared tool, and each `tool_result` must match a preceding `tool_use` in the same request.
- JSON and SSE responses reject undeclared tools, audio, non-empty annotations, malformed tool arguments, and unsupported finish reasons instead of silently dropping them.
- Streamed reasoning is preserved even when it follows a tool call; later argument fragments remain attached to the original tool block.
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
