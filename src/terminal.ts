import { emitKeypressEvents, Key } from 'node:readline';
import { stripVTControlCharacters } from 'node:util';
import chalk from 'chalk';
import { ModelConfig } from './types/config';

const HISTORY_LIMIT = 1000;
const LINE_LIMIT = 4096;

// Match the reference monitor's semantic palette without changing setup prompts.
const colors = {
  header: '#121216',
  live: '#4EC9B0',
  model: '#BE8CF0',
  sent: '#78AAE6',
  success: '#78C878',
  warning: '#DCC864',
  error: '#DC7878',
  dim: '#B4BEC8',
  text: '#F0F4F8',
};

// Terminal output is presentation only; request processing remains in Rust.
export class TerminalMonitor {
  private active = false;
  private lines: string[] = [];
  private pending = { stdout: '', stderr: '' };
  private top: number | undefined;
  private url = '';
  private timer?: NodeJS.Timeout;
  private wasRaw = false;
  private wasPaused = true;

  constructor(
    private readonly models: ModelConfig,
    private readonly version: string,
    private readonly shutdown: () => void,
    private readonly input: NodeJS.ReadStream = process.stdin,
    private readonly output: NodeJS.WriteStream = process.stdout
  ) {}

  start(url: string): boolean {
    if (!this.input.isTTY || !this.output.isTTY || process.env.TERM === 'dumb') return false;
    this.url = url;
    this.wasRaw = this.input.isRaw;
    this.wasPaused = this.input.isPaused();
    this.active = true;
    emitKeypressEvents(this.input);
    this.input.setRawMode(true);
    this.input.on('keypress', this.onKey);
    this.input.resume();
    this.output.on('resize', this.scheduleRender);
    process.once('exit', this.stop);
    this.output.write('\x1b[?1049h\x1b[?25l\x1b[?7l');
    this.render();
    return true;
  }

  write = (chunk: string, stream: 'stdout' | 'stderr' = 'stderr'): void => {
    if (!this.active) {
      (stream === 'stdout' ? this.output : process.stderr).write(chunk);
    }
    const parts = chunk.split('\n');
    for (let index = 0; index < parts.length; index++) {
      this.pending[stream] = (this.pending[stream] + parts[index]).slice(0, LINE_LIMIT);
      if (index < parts.length - 1) {
        const line = clean(this.pending[stream]);
        if (line) this.lines.push(line);
        this.pending[stream] = '';
      }
    }
    const removed = Math.max(0, this.lines.length - HISTORY_LIMIT);
    this.lines.splice(0, removed);
    if (this.top !== undefined) this.top = Math.max(0, this.top - removed);
    this.scheduleRender();
  };

  stop = (): void => {
    if (!this.active) return;
    this.active = false;
    clearTimeout(this.timer);
    this.timer = undefined;
    this.output.removeListener('resize', this.scheduleRender);
    this.input.removeListener('keypress', this.onKey);
    process.removeListener('exit', this.stop);
    this.input.setRawMode(this.wasRaw);
    if (this.wasPaused) this.input.pause();
    this.output.write('\x1b[?7h\x1b[?25h\x1b[?1049l');
  };

  private onKey = (_text: string, key: Key): void => {
    if (key.ctrl && key.name === 'c') {
      this.shutdown();
      return;
    }
    const height = this.logHeight();
    const end = Math.max(0, this.lines.length - height);
    const current = this.top ?? end;
    switch (key.name) {
      case 'up':
        this.top = Math.max(0, current - 1);
        break;
      case 'down':
        this.top = Math.min(end, current + 1);
        break;
      case 'pageup':
        this.top = Math.max(0, current - height);
        break;
      case 'pagedown':
        this.top = Math.min(end, current + height);
        break;
      case 'home':
        this.top = 0;
        break;
      case 'end':
        this.top = undefined;
        break;
      default:
        return;
    }
    if (this.top === end) this.top = undefined;
    this.scheduleRender();
  };

  private scheduleRender = (): void => {
    if (!this.active || this.timer) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      this.render();
    }, 33);
  };

  private logHeight(): number {
    return Math.max(1, (this.output.rows || 24) - 7);
  }

  private render(): void {
    const width = Math.max(1, this.output.columns || 80);
    const height = Math.max(1, this.output.rows || 24);
    const count = this.logHeight();
    const end = Math.max(0, this.lines.length - count);
    const top = Math.min(this.top ?? end, end);
    const rows =
      height < 9
        ? ['Resize terminal to at least 9 rows. Ctrl+C stops the proxy.']
        : [
            `cc-proxy v${this.version}  |  Running  |  ${this.url}`,
            `Opus   ${this.models.opus}`,
            `Sonnet ${this.models.sonnet}`,
            `Haiku  ${this.models.haiku}`,
            `SEND / RESPONSE  |  ${this.top === undefined ? 'LIVE' : 'HISTORY'}  |  ${this.lines.length ? top + 1 : 0}-${Math.min(top + count, this.lines.length)} / ${this.lines.length}`,
            ...Array.from(
              { length: count },
              (_, index) =>
                this.lines[top + index] ??
                (index === 0 && !this.lines.length
                  ? 'Waiting for requests. Open another terminal and run claude.'
                  : '')
            ),
            'Up/Down scroll | PgUp/PgDn page | Home oldest | End live',
            'Ctrl+C stop',
          ];
    let frame = '\x1b[H';
    for (let index = 0; index < height; index++) {
      const row = rows[index] ?? '';
      let color = logColor(row);
      if (height < 9) color = colors.warning;
      else if (index === 0) color = colors.header;
      else if (index <= 3) color = colors.model;
      else if (index === 4) color = this.top === undefined ? colors.live : colors.warning;
      else if (index >= height - 2) color = colors.dim;
      const isHeader = index === 0 && height >= 9;
      const style = chalk.hex(color).bgHex(isHeader ? colors.live : '#121216');
      // Classify before clipping; apply our colors only after sanitizing the text.
      const text = `\x1b[2K${fit(row, width)}`;
      frame += `\x1b[${index + 1};1H${isHeader ? style.bold(text) : style(text)}`;
    }
    this.output.write(frame);
  }
}

function logColor(line: string): string {
  const status = line.match(/^(?:->|<-) .+ \[(sent|streaming|received|completed|error)\] /)?.[1];
  switch (status) {
    case 'sent':
      return colors.sent;
    case 'streaming':
      return colors.live;
    case 'received':
    case 'completed':
      return colors.success;
    case 'error':
      return colors.error;
  }
  if (line.startsWith('[adapter]')) {
    return /\b(Failed|unavailable)\b/.test(line) ? colors.error : colors.dim;
  }
  if (/^(Update |Manual setup:)/.test(line)) return colors.warning;
  return colors.text;
}

function clean(text: string): string {
  return stripVTControlCharacters(text).replace(/[\p{Cc}\p{Cf}]/gu, '');
}

function fit(text: string, width: number): string {
  let result = '';
  let used = 0;
  for (const char of clean(text)) {
    // Reserve two cells for non-ASCII to keep wide model names inside their row.
    const cells = char.codePointAt(0)! > 0x7e ? 2 : 1;
    if (used + cells > width) break;
    result += char;
    used += cells;
  }
  return result;
}
