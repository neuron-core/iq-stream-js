/**
 * The conformance kit for the publisher and its adapters (Node only): a publisher that records in
 * memory, real servers to publish to, and a subscriber that collects what a stream delivered.
 */
import { execFile } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import http from 'node:http';
import https from 'node:https';
import net from 'node:net';
import { promisify } from 'node:util';
import { IQError } from '../shared/errors.js';
import { DEFAULT_MAX_FRAME_BYTES, TERMINAL_EVENTS } from '../shared/protocol.js';
import type { CloseOutcome } from '../shared/protocol.js';
import { FrameTooLarge, encodeFrame, frameSize } from '../publisher/frame.js';
import type { Publisher, PublishResult } from '../publisher/types.js';
import { writableOf } from '../publisher/writer.js';

const run = promisify(execFile);

async function docker(args: string[]): Promise<string> {
  try {
    return (await run('docker', args)).stdout.trim();
  } catch (error) {
    // Docker's own words, and not the command line, which carries the app secret.
    const stderr = String((error as { stderr?: unknown }).stderr ?? '').trim();
    throw new Error(`docker ${args[0]} failed: ${stderr || 'is Docker installed and running?'}`);
  }
}

export interface RecordedFrame {
  data: string;
  event?: string;
}

/**
 * The publisher's surface in memory, for an adapter's unit tests: it validates frames as the real one
 * does, records them in the order `send()` returned, and records the close outcome.
 */
export class RecordingPublisher implements Publisher {
  readonly streamId: string;
  readonly maxFrameBytes: number;
  readonly frames: RecordedFrame[] = [];
  /** The close line, once `close()` was called. The first outcome wins, as on the wire. */
  outcome: CloseOutcome | undefined;
  aborted = false;
  error: IQError | undefined;
  bytesSent = 0;

  constructor(options: { streamId?: string; maxFrameBytes?: number } = {}) {
    this.streamId = options.streamId ?? randomBytes(16).toString('base64url');
    this.maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
  }

  get framesSent(): number {
    return this.frames.length;
  }

  get alive(): boolean {
    return this.error === undefined && this.outcome === undefined && !this.aborted;
  }

  /** The data of every frame, in order. */
  get data(): string[] {
    return this.frames.map((frame) => frame.data);
  }

  send(data: string, event?: string): boolean {
    const line = encodeFrame(data, event);
    if (line.length - 1 > this.maxFrameBytes) throw new FrameTooLarge(line.length - 1, this.maxFrameBytes);
    if (!this.alive) return false;
    this.frames.push(event === undefined || event === null ? { data } : { data, event });
    this.bytesSent += line.length;
    return true;
  }

  close(outcome: CloseOutcome = 'end'): Promise<PublishResult> {
    if (this.alive) this.outcome = outcome;
    if (this.outcome === undefined) return Promise.resolve({ status: 0, closed: false });
    return Promise.resolve({
      status: 200,
      lastEventId: this.frames.length,
      closed: true,
      outcome: this.outcome,
      ...(this.outcome === 'fail' ? { reason: 'publisher' } : {}),
    });
  }

  abort(): void {
    if (this.alive) this.aborted = true;
  }

  /** Makes the publisher give up, as the real one does after a fatal answer: `send()` then returns false. */
  fail(error: IQError = new IQError(`IQ stream ${this.streamId}: the test made the publisher give up`, { streamId: this.streamId })): void {
    if (this.alive) this.error = error;
  }

  writable(): WritableStream<string | { data: string; event?: string }> {
    return writableOf(this);
  }

  frameSize(data: string, event?: string): number {
    return frameSize(data, event);
  }
}

/** The settings of the "short" server: the timeouts and limits of a stream, reachable in seconds. */
export const SHORT_SETTINGS: Readonly<Record<string, string>> = {
  PUBLISH_READ_TIMEOUT_MS: '2000',
  STREAM_IDLE_TIMEOUT_MS: '6000',
  PENDING_TIMEOUT_MS: '5000',
  RETRY_MS: '500',
  RETENTION_MS: '8000',
  MAX_FRAME_BYTES: '1024',
  MAX_STREAM_BYTES: '8192',
};

const DEFAULT_APP_KEY = 'demo-app';
const DEFAULT_APP_SECRET = 'test-app-secret-0123456789abcdef0123';

const trimmed = (url: string): string => url.replace(/\/+$/, '');

function healthy(url: string): Promise<boolean> {
  return new Promise((resolve) => {
    const request = http.get(`${url}/healthz`, { agent: false, timeout: 1000 }, (response) => {
      response.resume();
      resolve(response.statusCode === 200);
    });
    request.on('timeout', () => request.destroy());
    request.on('error', () => resolve(false));
  });
}

async function waitHealthy(url: string, seconds: number): Promise<boolean> {
  for (const deadline = Date.now() + seconds * 1000; Date.now() < deadline; ) {
    if (await healthy(url)) return true;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  return false;
}

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address() as net.AddressInfo;
      server.close(() => resolve(port));
    });
  });
}

export interface StartedServer {
  url: string;
  /** The container's id. */
  container: string;
  /** Kills the server without letting it drain, as a crash does. */
  kill(): Promise<void>;
  /** Starts it again, empty, on the same address. */
  start(): Promise<void>;
  /** Removes the container. */
  stop(): Promise<void>;
}

/** Starts a server of its own in Docker, on a free local port unless one is given, and waits until it answers. */
export async function startServer(
  options: { env?: Record<string, string>; port?: number; image?: string; appKey?: string; appSecret?: string } = {},
): Promise<StartedServer> {
  const image = options.image ?? process.env.IQ_STREAM_TEST_IMAGE ?? 'ghcr.io/neuron-core/iq-stream:1';
  const port = options.port ?? (await freePort());
  const env = { APP_KEY: options.appKey ?? DEFAULT_APP_KEY, APP_SECRET: options.appSecret ?? DEFAULT_APP_SECRET, ...options.env };
  const variables = Object.entries(env).flatMap(([name, value]) => ['-e', `${name}=${value}`]);
  const container = await docker(['run', '-d', '-p', `127.0.0.1:${port}:8080`, ...variables, image]);
  const url = `http://127.0.0.1:${port}`;
  const stop = async (): Promise<void> => {
    await docker(['rm', '-f', container]);
  };
  const up = async (): Promise<void> => {
    if (await waitHealthy(url, 20)) return;
    await stop();
    throw new Error(`The IQ-Stream server from ${image} did not answer on ${url}/healthz.`);
  };
  await up();
  return {
    url,
    container,
    stop,
    kill: async () => {
      await docker(['kill', container]);
    },
    start: async () => {
      await docker(['start', container]);
      await up();
    },
  };
}

export interface TestServers {
  /** A server with default settings. */
  default: { url: string };
  /** A server with `SHORT_SETTINGS`. */
  short: { url: string };
  appKey: string;
  appSecret: string;
  /** Removes the containers this call started. Servers that were already running are left alone. */
  stop(): Promise<void>;
}

/**
 * The two servers the integration tests publish to. Each is the one its environment variable names
 * (`IQ_STREAM_TEST_URL`, `IQ_STREAM_TEST_SHORT_URL`), else the one already answering on the default
 * address, else a container started from `IQ_STREAM_TEST_IMAGE`.
 */
export async function testServers(): Promise<TestServers> {
  const appKey = process.env.IQ_STREAM_TEST_APP_KEY || DEFAULT_APP_KEY;
  const appSecret = process.env.IQ_STREAM_TEST_APP_SECRET || DEFAULT_APP_SECRET;
  const started: StartedServer[] = [];
  const stop = async (): Promise<void> => {
    await Promise.all(started.splice(0).map((server) => server.stop()));
  };
  const find = async (variable: string, fallback: string, env?: Record<string, string>): Promise<{ url: string }> => {
    const named = process.env[variable];
    if (named) return { url: trimmed(named) };
    if (await healthy(fallback)) return { url: fallback };
    const server = await startServer({ appKey, appSecret, ...(env ? { env } : {}) });
    started.push(server);
    return { url: server.url };
  };
  try {
    return {
      default: await find('IQ_STREAM_TEST_URL', 'http://127.0.0.1:8080'),
      short: await find('IQ_STREAM_TEST_SHORT_URL', 'http://127.0.0.1:8081', { ...SHORT_SETTINGS }),
      appKey,
      appSecret,
      stop,
    };
  } catch (error) {
    await stop();
    throw error;
  }
}

export interface SubscribedFrame {
  id: string;
  /** `message` for an unnamed frame. */
  event: string;
  data: string;
  /** Milliseconds from the call to the frame's arrival. */
  at: number;
}

export interface Subscription {
  /** The data events, in the order they arrived. */
  frames: SubscribedFrame[];
  /** The server's terminal event, or undefined when the response ended without one. */
  terminal: { event: string; data: Record<string, unknown> } | undefined;
  /** The HTTP status; 0 when the server could not be reached. */
  status: number;
}

/**
 * Subscribes to a stream as a browser would and resolves with everything it delivered, when the
 * terminal event arrives, the response ends, or `timeout` seconds have passed (30 by default).
 */
export function subscribe(
  url: string,
  streamId: string,
  token: string,
  options: { lastEventId?: string | number; timeout?: number } = {},
): Promise<Subscription> {
  return new Promise((resolve) => {
    const started = performance.now();
    const result: Subscription = { frames: [], terminal: undefined, status: 0 };
    const target = `${trimmed(url)}/${streamId}?token=${encodeURIComponent(token)}`;
    const headers = { Accept: 'text/event-stream', ...(options.lastEventId === undefined ? {} : { 'Last-Event-ID': String(options.lastEventId) }) };
    const request = (target.startsWith('https:') ? https : http).get(target, { agent: false, headers });
    const timer = setTimeout(() => request.destroy(), (options.timeout ?? 30) * 1000);
    const finish = (): void => {
      clearTimeout(timer);
      request.destroy();
      resolve(result);
    };

    let id = '';
    let event = '';
    let data: string[] = [];
    const dispatch = (): void => {
      if (data.length > 0 || event !== '') {
        const text = data.join('\n');
        if ((TERMINAL_EVENTS as readonly string[]).includes(event)) {
          let parsed: unknown;
          try {
            parsed = JSON.parse(text);
          } catch {
            parsed = undefined;
          }
          result.terminal = { event, data: typeof parsed === 'object' && parsed !== null ? (parsed as Record<string, unknown>) : {} };
        } else if (!event.startsWith('iq:')) {
          result.frames.push({ id, event: event || 'message', data: text, at: Math.round(performance.now() - started) });
        }
      }
      event = '';
      data = [];
    };
    const line = (text: string): void => {
      if (text === '') return dispatch();
      if (text.startsWith(':')) return;
      const colon = text.indexOf(':');
      const field = colon < 0 ? text : text.slice(0, colon);
      const value = colon < 0 ? '' : text.slice(colon + 1).replace(/^ /, '');
      if (field === 'id') id = value;
      else if (field === 'event') event = value;
      else if (field === 'data') data.push(value);
    };

    request.on('error', finish);
    request.on('response', (response) => {
      result.status = response.statusCode ?? 0;
      let buffer = '';
      response.setEncoding('utf8');
      response.on('data', (chunk: string) => {
        buffer += chunk;
        for (let end = buffer.indexOf('\n'); end >= 0 && !result.terminal; end = buffer.indexOf('\n')) {
          line(buffer.slice(0, end).replace(/\r$/, ''));
          buffer = buffer.slice(end + 1);
        }
        if (result.terminal) finish();
      });
      response.on('end', finish);
      response.on('error', finish);
      response.on('close', finish);
    });
  });
}
