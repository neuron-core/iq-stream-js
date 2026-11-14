/** What the adapter uses of the core's `Publisher`. `RecordingPublisher` and a test double fit as well. */
export interface IQPublisherLike {
  /** Queues one frame. May return `false` once the publisher has given up, and throw for a frame it refuses. */
  send(data: string, event?: string): unknown;
  close(outcome?: 'end' | 'fail'): Promise<unknown>;
  /** The limit one frame must fit, in bytes on the wire. Default 65,536. */
  readonly maxFrameBytes?: number;
  /** The size a frame would have on the wire. Default: the core's envelope, measured here. */
  frameSize?(data: string, event?: string): number;
  /** `false` once the publisher has given up: the adapter then stops sending and goes on reading. */
  readonly alive?: boolean;
}

export interface PublishResult {
  /** The close line that was sent. */
  outcome: 'end' | 'fail';
  /** What the run threw, when it failed or was aborted; else what `beforeClose` threw. */
  error?: unknown;
  /** Frames the publisher accepted. */
  frames: number;
}

export interface PublishStreamOptions<T> {
  /** One chunk to zero or more frames. Default: one frame, `JSON.stringify(chunk)`. */
  encode?: (chunk: T) => string | readonly string[] | undefined;
  /** Awaited after the last frame and before the close line. Persist the result here. If it throws, the stream closes with `fail`. */
  beforeClose?: (outcome: 'end' | 'fail', error?: unknown) => void | Promise<void>;
  /** The close line of a run that was aborted through its `AbortSignal`. Default `'fail'`. */
  abortOutcome?: 'end' | 'fail';
  /** A chunk that could not be encoded or that the publisher refused (too large) is dropped and reported here, as is a `close()` that threw. */
  onError?: (error: unknown) => void;
}

/** A `ReadableStream` is async-iterable on Node, but not on every runtime that has one. */
export function iterate<T>(source: AsyncIterable<T> | ReadableStream<T>): AsyncIterable<T> {
  if (Symbol.asyncIterator in source) return source;
  return (async function* () {
    const reader = (source as ReadableStream<T>).getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      yield value;
    }
  })();
}

export const isAbortError = (error: unknown): boolean => (error as { name?: unknown } | null | undefined)?.name === 'AbortError';

const frameList = (encoded: string | readonly string[] | undefined): readonly string[] =>
  encoded === undefined ? [] : typeof encoded === 'string' ? [encoded] : encoded;

/**
 * Publishes a stream as IQ frames and closes the IQ stream when it ends: with `end` when the source
 * finished, with `fail` when it threw. Never rejects: a failing run is in the result, not thrown.
 */
export async function publishStream<T>(
  source: AsyncIterable<T> | ReadableStream<T>,
  publisher: IQPublisherLike,
  options: PublishStreamOptions<T> = {},
): Promise<PublishResult> {
  const { encode = JSON.stringify as (chunk: T) => string, beforeClose, abortOutcome = 'fail' } = options;
  const report = (error: unknown): void => {
    try {
      options.onError?.(error);
    } catch {
      // A reporting hook must not break the publishing it reports on.
    }
  };
  let outcome: 'end' | 'fail' = 'end';
  let failed = false;
  let error: unknown;
  let frames = 0;

  try {
    for await (const chunk of iterate(source)) {
      // An inert publisher takes nothing more, but the source is read to its end: the run goes on, and its result is still the application's to save.
      if (publisher.alive === false) continue;
      try {
        for (const frame of frameList(encode(chunk))) {
          if (publisher.send(frame) !== false) frames++;
        }
      } catch (problem) {
        report(problem);
      }
    }
  } catch (thrown) {
    failed = true;
    error = thrown;
    outcome = isAbortError(thrown) ? abortOutcome : 'fail';
  }

  try {
    await beforeClose?.(outcome, error);
  } catch (thrown) {
    if (failed) report(thrown);
    else {
      failed = true;
      error = thrown;
    }
    outcome = 'fail';
  }

  try {
    await publisher.close(outcome);
  } catch (problem) {
    report(problem);
  }
  return failed ? { outcome, error, frames } : { outcome, frames };
}
