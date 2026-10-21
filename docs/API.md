# HTTP API Reference

Transport failure diagnostics retain the original HTTP body error chain before SSE wrapping. Chains are limited to eight entries of 512 characters each; request URLs are removed from reqwest errors. Streaming JSONL stores these under `error.diagnostic.errorChain` and includes `upstreamResponse` metadata (status and selected content/transfer headers); diagnostics are not added to client error messages. HTTP 200 indicates successful response headers, not a complete response body.

Conversion errors retain the specific validation message in `error.message`. `request_conversion` identifies Anthropic-to-Chat conversion; `protocol_conversion` identifies Chat-to-Anthropic conversion. Response JSON syntax errors use `response_json`, and SSE JSON syntax errors use `sse_json`, with line/column and byte counts. Failed response conversion includes a fixed allowlist of JSON pointer paths and their types (`missing` is distinct from `null`), with tool field types limited to the first eight calls in the first choice. No payload values or arbitrary property names are copied into this shape. For streaming, `error.diagnostic` contains the shape and `error.streamState` reports the 1-based JSON-event attempt count, message/content/finish flags and tool count. HTTP error logs expose `phase` and `direction` at `error` level. Stream-finish errors include state without a failing chunk. Validation messages are sanitized before logging. Upstream error bodies are omitted from JSONL; known error classifications are retained instead.

## Customer diagnostics

Error files are `error_logs/YYYY-MM-DD.jsonl` beside the active configuration file, using UTC dates. Startup prints the actual directory. `GET /health` exposes `logging.errorDirectory`, `ioFailureCount`, `lastIoFailure` (operation, I/O kind, UTC timestamp), and `droppedRecords`. Counters are cumulative since startup: queue drops count rejected records; I/O failures count failed operations, not an exact number of lost records. File failures do not stop the proxy; the terminal shows a red failure with permission/disk-space guidance. Files flush approximately every second and at graceful shutdown, not by fsync; abrupt termination or power loss can lose pending records. No automatic log deletion is performed.

Every error includes `diagnosticsVersion`, `build` (packageVersion, nativeVersion, revision, os, arch) and logging drop/I/O counters. Release CI embeds the checked-out commit; local builds without `CC_PROXY_BUILD_SHA` report `unknown-local-build`. After conversion, all upstream failures include `upstreamRequestShape`: client/upstream streaming modes, conversion policy, reasoning effort, thinking controls and message/tool counts. Input rejected before conversion instead has `upstreamAttempted: false`; no upstream request is claimed. Request text, schemas and tool arguments are excluded.

| Phase | Interpretation |
| --- | --- |
| `upstream_connect`, `response_headers_transport` | Failure before headers, with bounded HTTP-client cause chain and connect/timeout flags |
| `response_headers_timeout` | Local deadline waiting for headers |
| `upstream_transport` | Body read failure, including EOF in stream and non-stream modes |
| `first_body_bytes`, `first_anthropic_event`, `body_idle`, `response_total_timeout` | Distinct local waiting boundaries |
| `upstream_http`, `upstream_sse_error` | Upstream rejection/error; raw body omitted, known code/type/param retained and unknown classifications marked `unrecognized` |
| `request_conversion`, `response_json`, `sse_json`, `protocol_conversion`, `stream_finish` | Validation, syntax, conversion or incomplete application stream |

Non-stream cause chains use `error.errorChain`; stream chains retain `error.diagnostic.errorChain`. `error.upstreamResponse` holds HTTP status/version, selected content/transfer headers and allowlisted `request-id`, `x-request-id`, `cf-ray` values. Failure while reading an HTTP error body retains the original upstream status separately from the proxy status. Response metadata is null when headers were never received.

`error.transfer` reports received body bytes/chunks, `firstByteMs`, `sinceLastByteMs`, and body elapsed time. Body timing starts at receipt of headers for streams and at body-read entry for non-stream/error bodies. These are application-visible body bytes, not wire bytes or tokens. Null first/last timing means no nonempty chunk was observed. `headersMs` measures handling-to-headers; `requestElapsedMs` measures the whole request interval. Stream `elapsedMs` retains headers-to-failure semantics; non-stream `elapsedMs` is handling time, except header failures which measure the header wait. Scheduling and client backpressure can affect timings; they do not identify the disconnected network hop by themselves.

Error JSONL omits raw upstream error bodies, strips provider URLs to their origin, redacts configured credentials/custom-header values and quoted/URL free text, and caps strings at 512 characters, collections at 128 entries and nesting at 12. Chains have at most eight entries. Client-facing upstream errors are unchanged. Correlation IDs and hostnames remain operational metadata: review the selected record before sharing. Historical logs are not rewritten or sanitized retroactively. Collect the matching request ID, failure time/timezone, error record and logging-health status—not configuration, credentials or the whole usage directory. EOF proves incomplete transport, not whether the provider, gateway or customer network caused it.

cc-proxy is a CLI product. It starts a native Rust proxy and does not export Node.js server or converter APIs.

## `POST /v1/messages`

Accepts an Anthropic Messages request and forwards it to the configured OpenAI-compatible Chat Completions endpoint. The maximum request body is 32 MiB.

```typescript
{
  model: string;
  max_tokens: number;
  messages: Array<{
    role: 'user' | 'assistant' | 'system';
    content: string | ContentBlock[];
  }>;
  system?: string | ContentBlock[];
  temperature?: number;
  top_p?: number;
  stream?: boolean;
  stop_sequences?: string[];
  tools?: Tool[];
  tool_choice?: ToolChoice;
  thinking?: {
    type?: 'enabled' | 'disabled' | 'adaptive';
    budget_tokens?: number;
  };
  output_config?: {
    effort?: 'low' | 'medium' | 'high' | 'xhigh' | 'max';
    format?: {
      type: 'json_schema';
      schema: Record<string, unknown>;
    };
  };
  cache_control?: { type: 'ephemeral'; ttl?: '5m' | '1h' } | null;
  metadata?: { user_id?: string | null };
}
```

Every response includes matching `request-id` and `x-request-id` values generated by the proxy. An upstream Chat request ID is separate from this downstream ID. Rejected upstream error bodies are read only up to 64 KiB; truncated bodies produce a generic client message and a bounded prefix with `responseTruncated: true` in error JSONL. A syntactically valid upstream `Retry-After` is forwarded; the proxy does not retry POST requests.

Errors returned by this handler, including invalid JSON, validation failures and oversized bodies, are recorded in `error_logs/*.jsonl` with `requestId` and `error.type`. HTTP failures include `error.status`; errors emitted inside an already-started SSE response use `stream_error` without inventing a new HTTP status. Stream errors include `error.phase` (`first_body_bytes`, `first_anthropic_event`, `body_idle`, `upstream_transport`, `sse_frame`, `sse_json`, `upstream_sse_error`, `protocol_conversion`, or `stream_finish`) and `error.elapsedMs` measured from receipt of upstream response headers. The terminal prints an `[error]` summary with phase and elapsed time when available, without upstream error bodies. Validated stream completion prints `[completed]` before the final SSE events are emitted; this does not acknowledge client receipt. Usage records include `requestId` for correlation. See [terminal controls](terminal.md).

Request conversion retains the established text/tool Chat Completions behavior where it matches the documented contracts. Input `user`, `assistant`, and text-only `system` messages retain their roles and order; top-level `system` is prepended. Unsupported roles, system-message `clear_at` values other than `never`, per-message `output_config`, and unsupported result-bearing content blocks return `400` instead of changing semantics or disappearing. Historical `thinking` and `redacted_thinking` blocks keep the existing behavior described below. Messages with missing or null content are skipped; historical assistant text, including short replies and JSON-start text, is preserved. The adapter sends `max_completion_tokens` by default; `upstreamCapabilities.maxTokensField: "max_tokens"` selects the older field for an upstream that requires it. The Anthropic and Chat token budgets are upper bounds with no guaranteed visible-output equivalence.

Upstream requests explicitly send `Accept: application/json`, `Content-Type: application/json`, bearer authorization, `stream: true | false`, and an OpenAI JavaScript SDK-compatible user agent.

Qwen uses the generic Chat path: the upstream `stream` mode follows the client, with `stream_options.include_usage` only for streaming requests. No `enable_thinking` extension is injected and no SSE-to-JSON buffering is performed.

Client tools map only `name`, `description`, and `input_schema`; `strict: true` returns `400` because this path does not rewrite schemas to satisfy Chat's strict subset. `tools` and `stop_sequences` must be arrays, and every stop sequence must be a string; malformed values return `400` instead of being omitted. Tool choices map as `auto → auto`, `any → required`, `none → none`, and named `tool → function`; unsupported choices and named choices without a matching declared tool return `400`. An explicit `tool_choice.disable_parallel_tool_use` maps to the inverse Chat `parallel_tool_calls` value. Every assistant `tool_use` must have one matching `tool_result` in the immediately following user message; result blocks must precede user text or images. Tool-result text becomes `role=tool` messages in input order.

For the generic Chat path, explicit `output_config.effort` maps to `reasoning_effort`; `thinking.enabled/adaptive` maps approximately from its budget or mode. `thinking.disabled` sends `reasoning_effort: "none"`, and a conflicting explicit effort returns `400`. An upstream rejection is returned as an error without retry or effort fallback. A successful upstream response cannot prove a custom model actually disabled internal reasoning. GLM retains its provider-specific options and tool-turn `reasoning_content`. GLM model-family detection is case-insensitive and uses the final provider-path segment. Qwen follows the generic mapping, including conflict validation. Other historical thinking is not forwarded; a thinking-only historical turn becomes an assistant message with null content. `output_config.format` with `type: "json_schema"` maps to Chat `response_format.json_schema` with a fixed name and `strict: true`; the schema is not rewritten. The adapter checks the root object and required-property shape, while the upstream validates the remaining strict-schema subset. Unsupported formats, `top_k`, `service_tier`, `mcp_servers`, and `container` return `400`. `cache_control` and `metadata.user_id` are ignored as hints or operational metadata; neither is forwarded. Stop arrays, including empty arrays, are forwarded as `stop`.

For GLM-5.2, explicit effort enables `thinking` and maps `low/medium/high` to `reasoning_effort: "high"`, `xhigh/max` to `"max"`. For GLM-5.3, explicit effort enables `thinking` and maps `low` to `"low"`, `medium/high` to `"high"`, and `xhigh/max` to `"max"`; the model does not support disabled thinking. Explicit effort for other GLM models returns `400` rather than being silently dropped. These mappings describe documented Chat endpoints; an OpenAI-compatible local server may implement different model controls.

Anthropic defines a final assistant message as a response prefill, but standard OpenAI Chat Completions defines assistant input as prior model output and has no portable continuation flag. The adapter applies the configured prefill capability only to the final assistant message; earlier assistant messages remain history even when their text is short or begins JSON. An upstream that cannot continue a final assistant message may return a legal empty completion in a configured `native` mode.

### Images

User image blocks map to Chat `image_url` parts in their original text/image order. HTTP(S) URLs pass through without a download; base64 JPEG, PNG, GIF, and WebP data becomes `data:<media_type>;base64,<data>`. Invalid and unsupported sources, including `file_id`, return `400`. The upstream model must support image input.

Chat tool messages support text only. When a `tool_result` contains images, its text message retains markers at the original image positions; a following user multimodal message contains the images in order, each preceded by its resolved `tool_call_id`. This changes the image's message role and loses its tool-result identity, so it is an adapter policy rather than lossless protocol equivalence.

### Non-streaming response

```typescript
{
  id: string;
  type: 'message';
  role: 'assistant';
  content: ContentBlock[];
  model: string;
  stop_reason: 'end_turn' | 'max_tokens' | 'tool_use' | 'refusal';
  stop_details?: { type: 'refusal'; category: null; explanation: string };
  stop_sequence: null;
  usage: {
    input_tokens: number;
    output_tokens: number;
    cache_read_input_tokens?: number;
    cache_creation_input_tokens?: number;
  };
}
```

Finish reasons map as `stop → end_turn`, `length → max_tokens`, `tool_calls → tool_use`, and `content_filter → refusal`. A `length` response may retain complete tool calls as `tool_use` blocks. A refusal remains a normal text content block and includes `stop_details`. Unknown or legacy `function_call` finish reasons, undeclared or missing tool names, tool arguments that are not a complete JSON object, audio, and non-empty annotations are upstream protocol errors (`502`) in both JSON and SSE paths. The adapter cannot distinguish a natural OpenAI `stop` from a custom stop-sequence match, so `stop_sequence` remains `null`.

An upstream `stop` with no text, reasoning, refusal, or tool call is represented as the legal Anthropic shape `content: []` with `stop_reason: end_turn`. The adapter records this as `outcome: empty_end_turn` instead of changing it into an API error. Application acceptance tests may still fail such a response when the requested task required text or a tool call; protocol validity and task success are separate decisions.

### Streaming response

Streaming uses SSE and preserves Anthropic event order:

- `message_start`
- `content_block_start`
- `content_block_delta`
- `content_block_stop`
- `message_delta`
- `message_stop`

`message_start.message.usage` uses numeric zero placeholders. JSON and final `message_delta` usage share one conversion: `input_tokens = prompt_tokens - cached_tokens - cache_write_tokens`, `output_tokens = completion_tokens`, `cache_read_input_tokens = cached_tokens`, and `cache_creation_input_tokens = cache_write_tokens`. Token counts are unadjusted counts, and output does not subtract reasoning tokens.

When one cache detail is missing or null, the adapter subtracts and emits only the known cache count; `input_tokens` carries the remaining unclassified prompt balance, so it is not guaranteed to mean uncached input. When usage is absent, both required counters are zero placeholders and the usage record marks it `unreported`. Invalid totals, invalid cache details, or cache details exceeding `prompt_tokens` do not discard the answer: legal totals are retained, cache splitting is abandoned, and the record is marked `invalid` with the raw upstream usage. Complete and partially classified snapshots are recorded as `complete` and `incomplete`; incomplete records list the missing cache fields. These degraded counters are not suitable for exact billing. Streaming usage is a final cumulative snapshot and replaces earlier snapshots instead of being added.

Upstream `reasoning_content || reasoning` becomes `thinking` / `thinking_delta`, including reasoning-only completions and reasoning emitted after a tool call. A later tool-argument fragment remains attached to its original tool block. No cryptographic `signature` is fabricated. This preserves the established Claude Code display behavior; the unsigned block is not guaranteed to be replayable as native Anthropic signed thinking history. Reasoning text is not written to usage/error logs.

Every content block receives a consecutive, unique index; deltas for parallel tools route by their upstream index, regardless of arrival order. Text after tools is streamed immediately in a new block. Each block stops exactly once. Tool names are accumulated against the request's declared names: an exact match with no longer matching name starts immediately. Ambiguous names wait only for that tool until finish_reason or [DONE]. Repeating the same complete name after a tool block starts leaves its identity unchanged; a different name fails. Missing IDs are generated at block start; unknown final names fail. Arguments are streamed unchanged, then validated as a complete JSON object before block closure; absent arguments mean `{}`.

At finish_reason, content blocks close while the adapter continues reading final usage. `[DONE]`, or clean EOF following finish_reason, emits exactly one final message_delta/message_stop. An explicit `[DONE]` without finish_reason can infer end_turn/tool_use only for complete content. EOF without either terminal signal, malformed arguments, and transport errors emit `error` without successful final events. Empty choices, usage-only chunks, and content-free choice tails with no or the same finish_reason are accepted. New content or a conflicting finish_reason after finish_reason is an error.

A streamed `stop` with no content blocks likewise completes with the normal `message_delta(stop_reason=end_turn)` and `message_stop` sequence and is logged as `empty_end_turn`. Zero-byte bodies, malformed JSON/SSE, missing required structures, damaged tool arguments, and streams without a legal terminal signal remain protocol errors. Usage-only chunks update counters only and never create content or a second completion.

Streaming waits at four fixed boundaries: upstream response headers for 120 seconds, the first non-empty body bytes for 120 seconds after headers, the first emitted Anthropic event for 240 seconds after the same headers, and each later non-empty body chunk for 300 seconds while the downstream is polling. Header timeout returns an HTTP `504` error envelope. Once downstream SSE has started, timeout emits one `api_error` event and closes without `message_delta` or `message_stop`. Partial bytes, SSE comments, pings and empty frames do not satisfy the first Anthropic event deadline. The adapter does not pre-read or replay the first chunk, retry the POST, or synthesize heartbeats.

Non-streaming upstream responses use a 120-second header deadline, 300-second idle-body deadline, 600-second total deadline, and 32 MiB body limit. Timeouts return `504`; oversized or invalid JSON returns `502`. These are cc-proxy gateway policies, not Anthropic or OpenAI API limits. A failed POST is not retried. Streamed Anthropic message IDs are unique per proxy request and are not upstream message IDs.

Parity exceptions: main's reused tool/text indices, fragmented tool names, missing block stops, and streaming `length` mapping are corrected. Existing explicit refusal handling and validation of malformed upstream payloads remain. Unknown finish reasons are errors. SSE framing uses eventsource-stream 0.2.3; output uses Axum Sse/Event, with demand-driven streaming and cancellation rather than automatic POST replay.

## `GET /health`

```json
{
  "status": "ok",
  "adapter": "cc-proxy",
  "dropped_jsonl_records": 0
}
```

The dropped counter increases when the bounded observability queue cannot accept another JSONL record. Proxy traffic is not blocked by observability writes.

## Errors

```json
{
  "type": "error",
  "error": {
    "type": "invalid_request_error",
    "message": "Description of the error"
  }
}
```

| Status   | Error type              |
| -------- | ----------------------- |
| 400, 413 | `invalid_request_error` |
| 401      | `authentication_error`  |
| 403      | `permission_error`      |
| 404      | `not_found_error`       |
| 429      | `rate_limit_error`      |
| 500+     | `api_error`             |

## Configuration

For a new installation, configuration and update metadata are stored in `~/.cc-proxy/`.
If `~/.claude-adapter/` already exists, cc-proxy continues to use that directory without moving it,
so the existing configuration remains compatible:

```json
{
  "baseUrl": "https://api.openai.com/v1",
  "apiKey": "...",
  "upstreamCapabilities": {
    "assistantPrefill": "unsupported",
    "maxTokensField": "max_completion_tokens"
  },
  "models": {
    "opus": "gpt-4.1",
    "sonnet": "gpt-4.1",
    "haiku": "gpt-4.1-mini"
  },
  "upstreamHeaders": {
    "HTTP-Referer": "https://example.com",
    "User-Agent": "cc-proxy/2.0"
  }
}
```

To keep the secret out of `config.json`, configure only its environment variable name:

```json
{
  "baseUrl": "https://api.openai.com/v1",
  "apiKeyEnv": "OPENAI_API_KEY",
  "upstreamCapabilities": {
    "assistantPrefill": "unsupported"
  },
  "models": {
    "opus": "gpt-4.1",
    "sonnet": "gpt-4.1",
    "haiku": "gpt-4.1-mini"
  }
}
```

Exactly one of `apiKey` and `apiKeyEnv` is required. Environment variable names must match
`[A-Za-z_][A-Za-z0-9_]*`; the variable must exist and be non-empty when the wizard runs and whenever
the adapter starts. The resolved value is never written back to configuration or logs.

`upstreamCapabilities.assistantPrefill` accepts exactly three values:

| Value | Request behavior |
| ----- | ---------------- |
| `unsupported` | Default. Reject a terminal Anthropic assistant prefill with `400 invalid_request_error` before contacting the upstream. |
| `continue_final_message` | Preserve the terminal assistant message and add `continue_final_message: true`. |
| `native` | Preserve the terminal assistant message without adding an extension. |

This setting describes the upstream API contract, not a model family. The adapter never infers it
from `baseUrl`, a provider name, or a model identifier. Existing configurations that omit the field
remain valid and use `unsupported`.

`upstreamCapabilities.maxTokensField` accepts `max_completion_tokens` (default) or `max_tokens`.
Choose `max_tokens` only when the configured Chat endpoint requires it; model names do not change
this selection. There is no `supportsReasoningEffortNone` setting.

Configured upstream headers are forwarded unchanged. `Authorization`, `Accept`, `Content-Type`, `Content-Length`, and `Host` remain adapter-controlled and cannot be configured through the CLI.

The CLI selects a native binary, waits for its ready record, and then updates Claude settings. The proxy chooses the requested port or the next available port atomically.
