import { FrameTooLarge } from '@neuron-core/iq-stream';
import type { Publisher, PublishResult } from '@neuron-core/iq-stream';
import type { ChunkKind, ChunkSource, Oversize, RunPublishResult } from './types.js';

export interface PipeOptions<T> {
  /** Default `JSON.stringify`. */
  serialize?: (chunk: T) => string;
  /** What a chunk says about the ending. Without it every leg that ends closes with `end`. */
  classify?: (chunk: T) => ChunkKind;
  oversize?: Oversize;
  closeOnCancel?: 'fail' | 'end';
  suspendGraceMs?: number;
}

interface Reader<T> {
  read(): Promise<{ done?: boolean; value?: T }>;
  cancel(): Promise<unknown>;
}

function readerOf<T>(source: ChunkSource<T>): Reader<T> {
  if ('getReader' in source && typeof source.getReader === 'function') {
    const reader = source.getReader();
    return { read: () => reader.read(), cancel: () => reader.cancel() };
  }
  const iterator = (source as AsyncIterable<T>)[Symbol.asyncIterator]();
  return { read: () => iterator.next(), cancel: async () => iterator.return?.() };
}

const SILENT = Symbol('silent');

/** The next chunk, or SILENT when the source gives none within `ms`. */
function within<V>(read: Promise<V>, ms: number): Promise<V | typeof SILENT> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const silence = new Promise<typeof SILENT>((resolve) => {
    timer = setTimeout(resolve, ms, SILENT);
  });
  return Promise.race([read, silence]).finally(() => clearTimeout(timer));
}

/** What stands in for a chunk that is too large: a transient data part, which `useChat` hands to `onData` and does not store. */
function placeholder(chunk: unknown, bytes: number): unknown {
  const { type, toolCallId } = (chunk ?? {}) as { type?: unknown; toolCallId?: unknown };
  return { type: 'data-iq-omitted', transient: true, data: { type, ...(toolCallId === undefined ? {} : { toolCallId }), bytes } };
}

/**
 * The loop: one frame per chunk, in order, then one close line chosen from what the chunks said.
 * Publishing never reaches the run: when the publisher gives up, the source is still read to its end,
 * so the agent finishes and saves. Never rejects.
 */
export async function pipeToIQ<T>(source: ChunkSource<T>, publisher: Publisher, options: PipeOptions<T> = {}): Promise<RunPublishResult> {
  const { classify, oversize = 'omit', closeOnCancel = 'fail', suspendGraceMs = 1000 } = options;
  const serialize = options.serialize ?? (JSON.stringify as (chunk: T) => string);
  let cause: RunPublishResult['cause'];
  let suspended = false;
  let framesSent = 0;
  let framesOmitted = 0;
  let error: unknown;

  const publish = (chunk: T): void => {
    if (!publisher.alive) return;
    try {
      if (publisher.send(serialize(chunk))) framesSent++;
    } catch (thrown) {
      if (!(thrown instanceof FrameTooLarge)) {
        error ??= thrown;
        return;
      }
      const mapped = typeof oversize === 'function';
      let sent = false;
      try {
        const smaller = oversize === 'drop' ? null : mapped ? oversize(chunk, thrown.size) : placeholder(chunk, thrown.size);
        if (smaller !== null && smaller !== undefined) sent = publisher.send(serialize(smaller as T));
      } catch (again) {
        if (!(again instanceof FrameTooLarge)) error ??= again;
      }
      if (sent) framesSent++;
      if (!sent || !mapped) framesOmitted++;
    }
  };

  try {
    const reader = readerOf(source);
    for (;;) {
      const pending = reader.read();
      const next = suspended ? await within(pending, suspendGraceMs) : await pending;
      if (next === SILENT) {
        // The source waits for an answer that will come as a new request: this leg is over.
        pending.catch(() => undefined);
        reader.cancel().catch(() => undefined);
        break;
      }
      if (next.done) break;
      const chunk = next.value as T;
      let kind: ChunkKind;
      try {
        kind = classify?.(chunk);
      } catch (thrown) {
        error ??= thrown;
      }
      if (kind === 'error') cause = 'error';
      else if (kind === 'abort') cause ??= 'abort';
      else if (kind === 'suspend') suspended = true;
      publish(chunk);
    }
  } catch (thrown) {
    cause = 'stream-error';
    error = thrown;
  }

  const line = cause === undefined || (cause === 'abort' && closeOnCancel === 'end') ? 'end' : 'fail';
  let answer: PublishResult | undefined;
  try {
    answer = await publisher.close(line);
  } catch (thrown) {
    error ??= thrown;
  }
  error ??= publisher.error;
  const closedAsChosen = answer?.closed === true && answer.outcome === line;
  return {
    outcome: line === 'end' && closedAsChosen ? 'end' : 'fail',
    ...(cause === undefined ? {} : { cause }),
    suspended,
    framesSent,
    framesOmitted,
    ...(answer === undefined ? {} : { publish: answer }),
    ...(error === undefined ? {} : { error }),
  };
}
