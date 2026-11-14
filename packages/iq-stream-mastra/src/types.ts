import type { IQClient, PublishResult, Ticket } from '@neuron-core/iq-stream';

/** The major of the `ai` package in the front end, which decides the shape of the chunks. */
export type UIVersion = 'v5' | 'v6' | 'v7';

/** A web stream of either flavour (the global one, or the one of `node:stream/web`), or anything `for await` reads. */
export type ChunkSource<T = unknown> = ReadableStream<T> | AsyncIterable<T>;

/** What the browser needs: the core's ticket, with the Mastra run id for approval resumes and for abort. */
export interface IQTicket extends Ticket {
  runId?: string;
}

/** What a chunk says about how its leg ends. */
export type ChunkKind = 'error' | 'abort' | 'suspend' | undefined;

/**
 * A line over the frame limit: replace it with a small placeholder (`omit`, the default), leave it out
 * (`drop`), or return a smaller chunk to send in its place (`null` leaves it out).
 */
export type Oversize = 'omit' | 'drop' | ((chunk: unknown, bytes: number) => unknown);

export interface RunPublishResult {
  /** How the stream closed for its subscribers. `fail` also when publishing gave up, whatever the run did. */
  outcome: 'end' | 'fail';
  /** How the run ended when it did not finish. `abort` closes with `fail` unless `closeOnCancel` is `end`. */
  cause?: 'error' | 'abort' | 'stream-error';
  /** The leg ended on a tool approval or a tool suspension. */
  suspended: boolean;
  framesSent: number;
  /** Chunks left out because their line was over the frame limit, replaced by a placeholder or not. */
  framesOmitted: number;
  /** The server's answer to the close, when there was one. */
  publish?: PublishResult;
  /** The error of the run's stream, or the one that made the publisher give up. */
  error?: unknown;
}

export interface PublishedRun {
  ticket: IQTicket;
  /** Resolves when the stream is closed. Never rejects. */
  done: Promise<RunPublishResult>;
  /** Cancels the run when the adapter started it; otherwise does nothing. */
  abort(reason?: unknown): void;
}

export interface PublishOptions {
  iq: IQClient;
  /** Default: a new one. Never reuse an id for a second answer. */
  streamId?: string;
  /** Seconds the browser's token is valid. Default 3600. */
  subTokenTtl?: number;
  /** The close line of a run cancelled by the user or the application. Default `fail`. */
  closeOnCancel?: 'fail' | 'end';
  oversize?: Oversize;
  /**
   * A source that stays open after a tool approval or suspension (a durable agent, `agent.observe()`)
   * is left this many milliseconds after its last chunk, then the leg is closed. Default 1000.
   */
  suspendGraceMs?: number;
  /** Serverless: keeps the invocation alive until publishing is done. */
  waitUntil?: (promise: Promise<unknown>) => void;
  onResult?: (result: RunPublishResult, ticket: IQTicket) => void;
}
