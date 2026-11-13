import type { IQError } from '../shared/errors.js';
import type { CloseOutcome } from '../shared/protocol.js';

/** Durations are in seconds unless a name ends in `Ms`. */
export interface IQClientOptions {
  /** How the backend reaches the server: `http(s)://host[:port]`, or `unix:///path/to/iq.sock` (Node build). Default: `IQ_STREAM_URL`. */
  url?: string;
  /** Default: `IQ_STREAM_APP_KEY`. */
  appKey?: string;
  /** At least 32 bytes. Default: `IQ_STREAM_APP_SECRET`. */
  appSecret?: string;
  /** The address browsers use, when it differs from `url`. Default: `IQ_STREAM_PUBLIC_URL`, else `url`. */
  publicUrl?: string;
  /** Seconds of silence after which an empty line is written. `null` turns keepalives off. Default 20. */
  keepalive?: number | null;
  /** Seconds a stream stays open before its close line goes out. Default 3. */
  minOpen?: number;
  /** The limit one NDJSON line must fit, in bytes. Must not exceed the app's limit on the server. Default 65536. */
  maxFrameBytes?: number;
  /** `report` (default): a publishing error is handed to `errorHandler` and the publisher goes inert. `raise`: `send()` and `close()` throw it. */
  onError?: 'report' | 'raise';
  /** Called once per publisher, with the error that made it give up. */
  errorHandler?: (error: IQError) => void;
  /** `stream` (default): one held chunked POST. `batch`: sequential short POSTs, for runtimes that cannot stream a request body. */
  mode?: 'stream' | 'batch';
  /** In batch mode, how long lines are collected before a request is sent. Default 100. */
  batchMs?: number;
  /** Frames kept for resending after a lost request. Default 512. */
  replayFrames?: number;
  /** Bytes kept for resending after a lost request. Default 4 MiB. */
  replayBytes?: number;
  /** Bytes that may wait for the server before the stream is given up. Default 8 MiB. */
  queueBytes?: number;
  /** Seconds spent retrying a 503 or a refused connection before giving up. Default 10. */
  retryBudget?: number;
  /**
   * Seconds spent waiting for the server to let go of a request it still counts as attached
   * (`409 publisher_active`). It does so after its own read timeout, 60 seconds by default, which is
   * what a connection lost without a trace costs. Default 65.
   */
  takeoverBudget?: number;
  /** The `fetch` to use (fetch build). Default: the global one. */
  fetch?: typeof fetch;
}

/** The server's answer to a publish request. */
export interface PublishResult {
  /** HTTP status; 0 when the publisher gave up without an answer. */
  status: number;
  /** How many data frames the stream holds. */
  lastEventId?: number;
  closed: boolean;
  outcome?: CloseOutcome;
  /** Why the stream failed, when `outcome` is `fail`. */
  reason?: string;
  /** The server's error code, when the request was refused. */
  error?: string;
}

export interface Publisher {
  readonly streamId: string;
  /**
   * Queue one frame. Returns false once the publisher has given up. Never waits for the network.
   * Throws FrameTooLarge, and TypeError for an invalid event name or data that is not a string.
   */
  send(data: string, event?: string): boolean;
  /** Queue the close line and wait for the server's answer. The first outcome wins; later calls return the same result. */
  close(outcome?: CloseOutcome, options?: { timeout?: number }): Promise<PublishResult>;
  /** Drop the connection without closing the stream, for a process that hands the stream to another one. */
  abort(): void;
  /** Closes the stream with `end` when closed and with `fail` when aborted. */
  writable(): WritableStream<string | { data: string; event?: string }>;
  /** The size a frame would have on the wire, envelope and escapes included, without sending it. */
  frameSize(data: string, event?: string): number;
  readonly maxFrameBytes: number;
  /** Data frames queued so far. */
  readonly framesSent: number;
  /** Bytes of the lines queued so far. */
  readonly bytesSent: number;
  /** False once the publisher has given up. */
  readonly alive: boolean;
  /** Why it gave up. */
  readonly error: IQError | undefined;
}
