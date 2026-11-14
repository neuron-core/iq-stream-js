/**
 * A LangChain or LangGraph stream as AI SDK UI message chunks, one JSON chunk per IQ frame: what
 * `useChat` reads through `chatTransport` of `@neuron-core/iq-stream/ai-sdk`.
 */
import { toUIMessageStream } from '@ai-sdk/langchain';
import type { UIMessageChunk } from 'ai';
import { MASKED_ERROR_TEXT, fitting, limitsOf, splitText, truncationMarker, utf8Length } from '../frames.js';
import type { FrameLimits } from '../frames.js';
import { isAbortError, iterate, publishStream } from '../publish-stream.js';
import type { IQPublisherLike, PublishResult } from '../publish-stream.js';
import { StreamOptionsError, firstChunkProblem } from './guard.js';

/**
 * The options `agent.stream()` and `graph.stream()` need for the conversion. Spread them into the call.
 * `values` is where a tool call's input and the final state come from, `messages` carries the tokens
 * and the tool results, `custom` what `config.writer()` sends. Other sets convert badly, without an
 * error: `'tools'` duplicates tool chunks, and a single mode as a string converts to nothing.
 * `subgraphs: true` may be added.
 */
export const UI_STREAM_OPTIONS: { streamMode: ('values' | 'messages' | 'custom')[] } = {
  streamMode: ['values', 'messages', 'custom'],
};

export interface PublishUIMessagesOptions<TState = unknown> {
  /** The final graph state (the last `values` chunk). Awaited before the close line; if it throws, the stream closes with `fail`. Not called when the run failed. */
  onFinish?: (finalState: TState | undefined) => void | Promise<void>;
  /** The run failed, the stream could not be converted, a chunk was too large for a frame, or `onFinish` threw. Not called for an abort. */
  onError?: (error: Error) => void | Promise<void>;
  /** The close line of a run that was aborted through its `AbortSignal`. Default `'fail'`. */
  abortOutcome?: 'end' | 'fail';
  /** Also publish the AI SDK's `[DONE]` frame after `finish`, for a client that parses the AI SDK's own SSE. Default false. */
  sendDone?: boolean;
  /** Publish error messages as they are. Default false: the `errorText` of an `error` chunk and of a failed tool call is "An error occurred."; `onError` gets the run's error. */
  exposeErrors?: boolean;
}

export interface UIMessageFramesOptions extends FrameLimits {
  /** Called when a chunk's payload was replaced by a truncation marker, and when a chunk was dropped because nothing could make it fit. */
  onError?: (error: Error) => void;
}

/** Chunks whose text concatenates on the client, so that one may be sent as several. */
const SPLITTABLE: Readonly<Record<string, string>> = { 'text-delta': 'delta', 'reasoning-delta': 'delta', 'tool-input-delta': 'inputTextDelta' };
/** The member that holds the payload of a chunk that cannot be split. */
const PAYLOADS = ['input', 'output', 'data'] as const;

const asError = (error: unknown): Error => (error instanceof Error ? error : new Error(String(error)));

/** One chunk as the frames it takes: one, several for an oversized delta, none for a chunk nothing can make fit. */
function framesOf(chunk: UIMessageChunk, fits: (data: string) => boolean, report: (error: Error) => void): string[] {
  const whole = JSON.stringify(chunk);
  if (fits(whole)) return [whole];
  const members = chunk as unknown as Record<string, unknown>;
  const size = `A ${chunk.type} chunk of ${utf8Length(whole)} bytes exceeds the frame limit`;

  const text = SPLITTABLE[chunk.type];
  if (text !== undefined && typeof members[text] === 'string') {
    const frame = (piece: string): string => JSON.stringify({ ...chunk, [text]: piece });
    const frames = splitText(members[text], (piece) => fits(frame(piece))).map(frame);
    if (frames.every(fits)) return frames;
  } else {
    const payload = PAYLOADS.find((name) => name in members);
    const cut = payload && JSON.stringify({ ...chunk, [payload]: truncationMarker(members[payload]) });
    if (cut && fits(cut)) {
      report(new Error(`${size}: its ${payload} was replaced by a truncation marker.`));
      return [cut];
    }
  }
  report(new Error(`${size} and was not published.`));
  return [];
}

/**
 * UI message chunks to frame strings that fit the frame limit: an oversized `text-delta`,
 * `reasoning-delta` or `tool-input-delta` becomes several, and an oversized `input`, `output` or
 * `data` is replaced by `{ truncated: true, bytes }`. Pass the publisher's `maxFrameBytes` and
 * `frameSize` when the app has a limit of its own.
 */
export function uiMessageFrames(options: UIMessageFramesOptions = {}): TransformStream<UIMessageChunk, string> {
  const fits = fitting(options);
  const report = (error: Error): void => options.onError?.(error);
  return new TransformStream<UIMessageChunk, string>({
    transform(chunk, controller) {
      for (const frame of framesOf(chunk, fits, report)) controller.enqueue(frame);
    },
  });
}

/**
 * Publishes what `agent.stream(input, UI_STREAM_OPTIONS)` or `graph.stream(...)` returned, converted by
 * `toUIMessageStream`, and closes the IQ stream: with `end` after `onFinish`, with `fail` when the run
 * threw. Never rejects, and reads the run to its end whatever happens to the publisher.
 */
export function publishUIMessages<TState = unknown>(
  stream: AsyncIterable<unknown> | ReadableStream<unknown>,
  publisher: IQPublisherLike,
  options: PublishUIMessagesOptions<TState> = {},
): Promise<PublishResult> {
  const { onFinish, sendDone = false, exposeErrors = false } = options;
  const fits = fitting(limitsOf(publisher));
  const report = async (error: unknown): Promise<void> => {
    try {
      await options.onError?.(asError(error));
    } catch {
      // A reporting hook must not break the publishing it reports on.
    }
  };
  const reportFrame = (error: Error): void => void report(error);
  let failed = false;
  let failure: unknown;
  let finalState: TState | undefined;
  const fail = (error: unknown): void => {
    if (failed) return;
    failed = true;
    failure = error;
  };

  // Keeps what the run threw as it was thrown (the converter's hooks get a copy, or nothing for an
  // abort), and checks the first chunk. A stream that fails the check is left alone, not cancelled:
  // cancelling it would stop the run.
  async function* source(): AsyncGenerator<unknown> {
    const chunks = iterate(stream)[Symbol.asyncIterator]();
    try {
      for (let first = true; ; first = false) {
        const { done, value } = await chunks.next();
        if (done) return;
        const problem = first ? firstChunkProblem(value) : undefined;
        if (problem !== undefined) throw new StreamOptionsError(problem);
        yield value;
      }
    } catch (error) {
      fail(error);
      throw error;
    }
  }

  // The converter never errors its output: after a failure it calls one of these hooks, enqueues an
  // `error` chunk and closes normally. The close line is chosen here, from what the hooks saw.
  const chunks = toUIMessageStream<TState>(source() as AsyncIterable<never>, {
    onFinish: (state) => {
      finalState = state;
    },
    onError: async (error) => {
      fail(error);
      await report(failure);
    },
    onAbort: () => fail(new DOMException('This operation was aborted', 'AbortError')),
  });

  async function* frames(): AsyncGenerator<string> {
    for await (const chunk of iterate(chunks)) {
      // The converter's last chunk after a cancelled run is an `error`: the data should tell a cancel from a failure.
      if (chunk.type === 'error' && failed && isAbortError(failure)) yield JSON.stringify({ type: 'abort' } satisfies UIMessageChunk);
      else yield* framesOf(exposeErrors || !('errorText' in chunk) ? chunk : { ...chunk, errorText: MASKED_ERROR_TEXT }, fits, reportFrame);
    }
    if (!failed) {
      try {
        await onFinish?.(finalState);
      } catch (error) {
        // `finish` is out already; the answer was not saved, and the page must not take it for one that was.
        fail(error);
        await report(error);
        yield* framesOf({ type: 'error', errorText: exposeErrors ? asError(error).message : MASKED_ERROR_TEXT }, fits, reportFrame);
      }
    }
    if (failed) throw failure;
    if (sendDone) yield '[DONE]';
  }

  return publishStream(frames(), publisher, {
    encode: (frame) => frame,
    onError: reportFrame as (error: unknown) => void,
    ...(options.abortOutcome === undefined ? {} : { abortOutcome: options.abortOutcome }),
  });
}
