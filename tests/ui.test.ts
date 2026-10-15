// Tests for UI utility class
import { UI } from '../src/utils/ui';

// Mock console.log to capture output
const mockConsoleLog = jest.spyOn(console, 'log').mockImplementation();

describe('UI Utility Class', () => {
    beforeEach(() => {
        mockConsoleLog.mockClear();
    });

    afterAll(() => {
        mockConsoleLog.mockRestore();
    });

    describe('Basic logging', () => {
        it('should log messages with UI.log', () => {
            UI.log('test message');
            expect(mockConsoleLog).toHaveBeenCalledWith('test message');
        });

        it('should log info messages', () => {
            UI.info('info message');
            expect(mockConsoleLog).toHaveBeenCalled();
            const output = mockConsoleLog.mock.calls[0][0];
            expect(output).toContain('info message');
        });

        it('should log success messages', () => {
            UI.success('success message');
            expect(mockConsoleLog).toHaveBeenCalled();
            const output = mockConsoleLog.mock.calls[0][0];
            expect(output).toContain('success message');
        });

        it('should log warning messages', () => {
            UI.warning('warning message');
            expect(mockConsoleLog).toHaveBeenCalled();
            const output = mockConsoleLog.mock.calls[0][0];
            expect(output).toContain('warning message');
        });

        it('should log error messages without Error object', () => {
            UI.error('error message');
            expect(mockConsoleLog).toHaveBeenCalled();
            const output = mockConsoleLog.mock.calls[0][0];
            expect(output).toContain('error message');
        });

        it('should log error messages with Error object', () => {
            UI.error('error message', new Error('detailed error'));
            expect(mockConsoleLog).toHaveBeenCalledTimes(2);
            const secondOutput = mockConsoleLog.mock.calls[1][0];
            expect(secondOutput).toContain('detailed error');
        });
    });

    describe('Status methods', () => {
        it('should log status text', () => {
            UI.status('loading...');
            expect(mockConsoleLog).toHaveBeenCalled();
            const output = mockConsoleLog.mock.calls[0][0];
            expect(output).toContain('loading...');
        });

        it('should log statusDone with success', () => {
            UI.statusDone(true, 'completed');
            expect(mockConsoleLog).toHaveBeenCalled();
            const output = mockConsoleLog.mock.calls[0][0];
            expect(output).toContain('completed');
            expect(output).toContain('✔');
        });

        it('should log statusDone with failure', () => {
            UI.statusDone(false, 'failed');
            expect(mockConsoleLog).toHaveBeenCalled();
            const output = mockConsoleLog.mock.calls[0][0];
            expect(output).toContain('failed');
            expect(output).toContain('✖');
        });

        it('should handle statusDone with empty text', () => {
            UI.statusDone(true);
            expect(mockConsoleLog).toHaveBeenCalled();
        });
    });

    describe('Format methods', () => {
        it('should return styled URL with newUrl', () => {
            const result = UI.newUrl('http://localhost:3080');
            expect(result).toContain('http://localhost:3080');
        });

        it('should return dimmed text with dim', () => {
            const result = UI.dim('dimmed text');
            expect(result).toContain('dimmed text');
        });

        it('should return highlighted text with highlight', () => {
            const result = UI.highlight('highlighted');
            expect(result).toContain('highlighted');
        });
    });

    describe('Layout methods', () => {
        it('should display header with spacing', () => {
            UI.header('Test Header');
            expect(mockConsoleLog).toHaveBeenCalledTimes(3);
            expect(mockConsoleLog.mock.calls[0][0]).toBe('');
            expect(mockConsoleLog.mock.calls[1][0]).toContain('Test Header');
            expect(mockConsoleLog.mock.calls[2][0]).toBe('');
        });

        it('should display hint text', () => {
            UI.hint('Press Ctrl+C to exit');
            expect(mockConsoleLog).toHaveBeenCalled();
            const output = mockConsoleLog.mock.calls[0][0];
            expect(output).toContain('Press Ctrl+C to exit');
        });

        it('should display box with title and content', () => {
            UI.box('Box Title', ['line 1', 'line 2']);
            expect(mockConsoleLog).toHaveBeenCalled();
            // Box has: empty, border, title, border, content lines, border, empty
            expect(mockConsoleLog.mock.calls.length).toBeGreaterThanOrEqual(7);
        });

        it('should display table with aligned columns', () => {
            UI.table([
                { label: 'Key', value: 'Value' },
                { label: 'LongerKey', value: 'AnotherValue' }
            ]);
            // Table logs: empty, row1, row2, empty
            expect(mockConsoleLog).toHaveBeenCalledTimes(4);
        });

        it('should render the PNG banner through the iTerm2 image protocol', () => {
            const ttyDescriptor = Object.getOwnPropertyDescriptor(process.stdout, 'isTTY');
            const originalTermProgram = process.env.TERM_PROGRAM;
            const originalTerm = process.env.TERM;
            Object.defineProperty(process.stdout, 'isTTY', { configurable: true, value: true });
            process.env.TERM_PROGRAM = 'iTerm.app';
            process.env.TERM = 'xterm-256color';

            try {
                UI.banner();
                const output = mockConsoleLog.mock.calls[0][0] as string;
                expect(output).toContain('\x1b]1337;File=inline=1;width=80;height=18;preserveAspectRatio=1:');
                const terminator = output.lastIndexOf('\x07');
                const encodedImage = output.slice(output.lastIndexOf(':', terminator) + 1, terminator);
                expect(Buffer.from(encodedImage ?? '', 'base64').subarray(0, 8)).toEqual(
                    Buffer.from([137, 80, 78, 71, 13, 10, 26, 10])
                );
            } finally {
                if (ttyDescriptor) Object.defineProperty(process.stdout, 'isTTY', ttyDescriptor);
                else delete (process.stdout as unknown as { isTTY?: boolean }).isTTY;
                if (originalTermProgram === undefined) delete process.env.TERM_PROGRAM;
                else process.env.TERM_PROGRAM = originalTermProgram;
                if (originalTerm === undefined) delete process.env.TERM;
                else process.env.TERM = originalTerm;
            }
        });

        it('should show a text banner in terminals without inline image support', () => {
            const originalTermProgram = process.env.TERM_PROGRAM;
            process.env.TERM_PROGRAM = 'unsupported';

            try {
                UI.banner();
                expect(mockConsoleLog).toHaveBeenCalledWith(expect.stringContaining('cc-proxy'));
            } finally {
                if (originalTermProgram === undefined) delete process.env.TERM_PROGRAM;
                else process.env.TERM_PROGRAM = originalTermProgram;
            }
        });

        it('should display update notification', () => {
            UI.updateNotify('1.0.0', '1.1.0');
            expect(mockConsoleLog).toHaveBeenCalledTimes(3);
            expect(mockConsoleLog.mock.calls[0][0]).toBe('');
            const updateMessage = mockConsoleLog.mock.calls[1][0];
            expect(updateMessage).toContain('Update available:');
            expect(updateMessage).toContain('1.0.0');
            expect(updateMessage).toContain('1.1.0');
            const hintMessage = mockConsoleLog.mock.calls[2][0];
            expect(hintMessage).toContain('Run "npm i -g cc-proxy" to update');
        });
    });
});
