import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { TerminalMonitor } from '../src/terminal';

function setup(rows = 12, columns = 100) {
  const input = Object.assign(new PassThrough(), {
    isTTY: true,
    isRaw: false,
    setRawMode: jest.fn(),
  });
  const output = Object.assign(new EventEmitter(), {
    isTTY: true,
    rows,
    columns,
    write: jest.fn<boolean, [string]>(() => true),
  });
  const shutdown = jest.fn();
  const monitor = new TerminalMonitor(
    { opus: 'model-a', sonnet: 'model-b', haiku: 'model-c' },
    '1.0.0',
    shutdown,
    input as unknown as NodeJS.ReadStream,
    output as unknown as NodeJS.WriteStream
  );
  const frame = () => String(output.write.mock.calls.at(-1)?.[0]);
  const key = (name: string, ctrl = false) => {
    input.emit('keypress', '', { name, ctrl });
    jest.advanceTimersByTime(33);
  };
  return { input, output, shutdown, monitor, frame, key };
}

describe('fullscreen terminal', () => {
  let envTerm: string | undefined;
  beforeEach(() => {
    jest.useFakeTimers();
    envTerm = process.env.TERM;
    process.env.TERM = 'xterm-256color';
  });
  afterEach(() => {
    if (envTerm === undefined) delete process.env.TERM;
    else process.env.TERM = envTerm;
    jest.useRealTimers();
  });

  it('shows completed and error records in the scrollable log pane', () => {
    const { monitor, frame } = setup(12, 160);
    try {
      monitor.start('http://localhost:3080');
      monitor.write('<- "model" [completed] "req_1"\n');
      monitor.write(
        '<- "model" [error] "req_2" status=429 type="rate_limit_error" (see error_logs)\n'
      );
      jest.advanceTimersByTime(33);
      expect(frame()).toContain('[completed] "req_1"');
      expect(frame()).toContain('[error] "req_2" status=429');
    } finally {
      monitor.stop();
    }
  });

  it('follows requests, freezes a scrolled viewport, and returns to live with End', () => {
    const { monitor, frame, key } = setup();
    try {
      expect(monitor.start('http://localhost:3080')).toBe(true);
      expect(frame()).toContain('Waiting for requests');
      monitor.write(Array.from({ length: 10 }, (_, i) => `-> model [sent] req-${i}\n`).join(''));
      jest.advanceTimersByTime(33);
      expect(frame()).toContain('req-9');
      key('pageup');
      expect(frame()).toContain('HISTORY');
      expect(frame()).toContain('req-0');
      expect(frame()).not.toContain('req-9');
      monitor.write('<- model [streaming] req-10\n');
      jest.advanceTimersByTime(33);
      expect(frame()).toContain('req-0');
      expect(frame()).not.toContain('req-10');
      key('end');
      expect(frame()).toContain('LIVE');
      expect(frame()).toContain('[streaming] req-10');
      expect(frame()).toContain('model-a');
    } finally {
      monitor.stop();
    }
  });

  it('handles fragmented lines, strips terminal commands, and bounds retained history', () => {
    const { monitor, frame, key, output } = setup();
    try {
      monitor.start('http://localhost:3080');
      monitor.write('-> mo');
      monitor.write('del [sent] req-1\x1b[2J\r\n');
      jest.advanceTimersByTime(33);
      expect(frame()).toContain('-> model [sent] req-1');
      expect(frame()).not.toContain('\x1b[2J');
      output.columns = 5000;
      monitor.write('x'.repeat(10000));
      monitor.write('\n');
      jest.advanceTimersByTime(33);
      expect(frame()).toContain('x'.repeat(4096));
      expect(frame()).not.toContain('x'.repeat(4097));
      output.columns = 100;
      monitor.write(Array.from({ length: 1002 }, (_, i) => `entry-${i}\n`).join(''));
      jest.advanceTimersByTime(33);
      key('home');
      expect(frame()).toContain('entry-2');
      expect(frame()).not.toContain('entry-1\x1b');
      expect(frame()).toContain('/ 1000');
    } finally {
      monitor.stop();
    }
  });

  it('redraws on resize and restores terminal state on Ctrl+C shutdown', () => {
    const { monitor, input, output, shutdown, frame, key } = setup();
    input.pause();
    monitor.start('http://localhost:3080');
    expect(input.setRawMode).toHaveBeenCalledWith(true);
    expect(output.write.mock.calls[0][0]).toContain('\x1b[?1049h');
    output.rows = 6;
    output.emit('resize');
    jest.advanceTimersByTime(33);
    expect(frame()).toContain('Resize terminal');
    key('c', true);
    expect(shutdown).toHaveBeenCalledTimes(1);
    monitor.stop();
    expect(input.setRawMode).toHaveBeenLastCalledWith(false);
    expect(input.isPaused()).toBe(true);
    expect(frame()).toContain('\x1b[?1049l');
    expect(output.listenerCount('resize')).toBe(0);
    expect(input.listenerCount('keypress')).toBe(0);
    expect(jest.getTimerCount()).toBe(0);
    monitor.stop();
  });

  it('does not enter fullscreen when input or output is redirected', () => {
    const { monitor, output, input } = setup();
    input.isTTY = false;
    expect(monitor.start('http://localhost:3080')).toBe(false);
    input.isTTY = true;
    output.isTTY = false;
    expect(monitor.start('http://localhost:3080')).toBe(false);
    monitor.write('plain log\n', 'stdout');
    expect(output.write).toHaveBeenCalledWith('plain log\n');
    expect(input.setRawMode).not.toHaveBeenCalled();
  });
});
