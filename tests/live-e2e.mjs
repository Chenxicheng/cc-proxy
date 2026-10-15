#!/usr/bin/env node
import { randomBytes } from 'node:crypto';
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdtemp, readFile, realpath, rm, stat, symlink } from 'node:fs/promises';
import { homedir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { readAnthropicStream } from '../bench/stream-validation.mjs';

const root = fileURLToPath(new URL('..', import.meta.url));
const binary = path.join(
  root,
  '..',
  '.cc-proxy-target',
  'release',
  process.platform === 'win32' ? 'cc-proxy-native.exe' : 'cc-proxy-native'
);
const requestTimeoutMs = 180_000;
const interrupted = new AbortController();
let stage = 'setup';
let child;
let directory;
class TestFailure extends Error {}
const interrupt = () => interrupted.abort();
process.once('SIGINT', interrupt);
process.once('SIGTERM', interrupt);

try {
  const [configArgument, modelArgument, extra] = process.argv.slice(2);
  if (!configArgument || extra) {
    throw new TestFailure('usage: node tests/live-e2e.mjs <external config.json or directory> [model]');
  }
  const supplied = path.resolve(
    configArgument.startsWith('~/')
      ? path.join(homedir(), configArgument.slice(2))
      : configArgument
  );
  const configPath = await realpath(
    (await stat(supplied)).isDirectory() ? path.join(supplied, 'config.json') : supplied
  );
  const config = JSON.parse(await readFile(configPath, 'utf8'));
  const model = modelArgument ?? config.models?.sonnet;
  if (!model || typeof model !== 'string') {
    throw new TestFailure('provide a model argument or configure models.sonnet');
  }
  if (config.apiKeyEnv && !process.env[config.apiKeyEnv]) {
    throw new TestFailure(`set the configured ${config.apiKeyEnv} environment variable before running`);
  }
  if (!existsSync(binary)) {
    throw new TestFailure('build the Rust release binary with npm run build:native first');
  }

  // The native server writes logs beside its config. Link the real config into
  // a disposable repository directory so the external config remains read-only.
  directory = await mkdtemp(path.join(root, 'tests', '.live-e2e-'));
  const linkedConfig = path.join(directory, 'config.json');
  await symlink(configPath, linkedConfig);
  child = spawn(binary, ['--config', linkedConfig, '--port', '0'], {
    stdio: ['pipe', 'pipe', 'ignore'],
    env: { ...process.env, CC_PROXY_STDIN_SHUTDOWN: '1' },
  });
  const url = await waitForReady(child, interrupted.signal);
  const endpoint = `${url}/v1/messages`;

  stage = 'non-streaming text';
  const textRequest = {
    model,
    max_tokens: 1024,
    messages: [{ role: 'user', content: 'Reply with one short sentence about the sky.' }],
  };
  const nonStream = await send(endpoint, { ...textRequest, stream: false });
  const nonStreamMessage = await nonStream.json();
  requireText(nonStreamMessage.content, 'non-streaming text');
  requireStop(nonStreamMessage.stop_reason, 'end_turn');

  stage = 'streaming text';
  const streamed = await readAnthropicStream(
    await send(endpoint, { ...textRequest, stream: true })
  );
  requireText(streamed.blocks, 'streaming text');
  requireStop(streamed.stopReason, 'end_turn');

  stage = 'tool call';
  const tool = {
    name: 'get_test_value',
    description: 'Return a value to repeat in the final answer.',
    input_schema: { type: 'object', properties: {} },
  };
  const toolPrompt = 'Call get_test_value once. After receiving its result, repeat that value in your final answer.';
  const called = await readAnthropicStream(
    await send(endpoint, {
      model,
      max_tokens: 1024,
      stream: true,
      messages: [{ role: 'user', content: toolPrompt }],
      tools: [tool],
      tool_choice: { type: 'any' },
    })
  );
  requireStop(called.stopReason, 'tool_use');
  const toolUse = called.blocks.find((block) => block.type === 'tool_use');
  if (!toolUse?.id || toolUse.name !== tool.name) {
    throw new TestFailure('model did not call get_test_value');
  }

  stage = 'tool result and final answer';
  const marker = `VALUE-${randomBytes(6).toString('hex')}`;
  const finalResponse = await send(endpoint, {
    model,
    max_tokens: 1024,
    stream: false,
    messages: [
      { role: 'user', content: toolPrompt },
      { role: 'assistant', content: [{ type: 'tool_use', id: toolUse.id, name: tool.name, input: toolUse.input }] },
      { role: 'user', content: [{ type: 'tool_result', tool_use_id: toolUse.id, content: marker }] },
    ],
    tools: [tool],
    tool_choice: { type: 'none' },
  });
  const finalMessage = await finalResponse.json();
  const finalText = requireText(finalMessage.content, 'tool result answer');
  requireStop(finalMessage.stop_reason, 'end_turn');
  if (!finalText.includes(marker)) {
    throw new TestFailure('final answer did not use the tool result');
  }

  stage = 'shutdown';
  const exitCode = await stop(child);
  child = undefined;
  if (exitCode !== 0) throw new TestFailure('server did not exit cleanly');
  console.log(JSON.stringify({ model, nonStreamingText: 'passed', streamingText: 'passed', toolLoop: 'passed' }));
} catch (error) {
  const message = error instanceof TestFailure ? error.message : error.name;
  console.error(`Live E2E failed at ${stage}: ${message}`);
  process.exitCode = 1;
} finally {
  if (child) await stop(child);
  if (directory) await rm(directory, { recursive: true, force: true });
  process.off('SIGINT', interrupt);
  process.off('SIGTERM', interrupt);
}

async function send(endpoint, request) {
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(request),
    signal: AbortSignal.any([AbortSignal.timeout(requestTimeoutMs), interrupted.signal]),
  });
  if (!response.ok) {
    throw new TestFailure(`HTTP ${response.status} from the REST service`);
  }
  return response;
}

function requireText(blocks, label) {
  const text = blocks?.filter((block) => block.type === 'text').map((block) => block.text).join('') ?? '';
  if (!text.trim()) throw new TestFailure(`${label} was empty`);
  return text;
}

function requireStop(actual, expected) {
  if (actual !== expected) throw new TestFailure(`unexpected stop reason: ${actual}`);
}

function waitForReady(process, signal) {
  return new Promise((resolve, reject) => {
    let buffer = '';
    const timer = setTimeout(() => finish(new TestFailure('startup timed out')), 10_000);
    const finish = (error, url) => {
      clearTimeout(timer);
      process.stdout.off('data', onData);
      process.off('error', onError);
      process.off('exit', onExit);
      signal.removeEventListener('abort', onAbort);
      process.stdout.resume();
      if (error) reject(error);
      else resolve(url);
    };
    const onError = (error) => finish(error);
    const onExit = () => finish(new TestFailure('server exited before ready'));
    const onAbort = () => finish(new TestFailure('interrupted'));
    const onData = (data) => {
      buffer += data.toString();
      if (buffer.length > 65_536) return finish(new TestFailure('ready output exceeded limit'));
      let newline;
      while ((newline = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        if (line.startsWith('CC_PROXY_READY=')) {
          try {
            const ready = JSON.parse(line.slice('CC_PROXY_READY='.length));
            if (typeof ready.url !== 'string') throw new TestFailure('ready URL missing');
            return finish(undefined, ready.url);
          } catch (error) {
            return finish(error);
          }
        }
      }
    };
    process.stdout.on('data', onData);
    process.once('error', onError);
    process.once('exit', onExit);
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

async function stop(process) {
  if (process.exitCode !== null || process.signalCode !== null) return process.exitCode;
  const exited = new Promise((resolve) => process.once('exit', resolve));
  process.stdin.end('shutdown\n');
  const timer = setTimeout(() => process.kill('SIGKILL'), 10_000);
  const code = await exited;
  clearTimeout(timer);
  return code;
}
