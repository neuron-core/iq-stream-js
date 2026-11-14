/**
 * LangGraph's Agent Streaming Protocol events, from `streamEvents(input, { version: 'v3' })`, one
 * event per IQ frame: what `useStream` of `@langchain/react` reads through an `AgentServerAdapter`.
 */
import type { ProtocolEvent } from '@langchain/langgraph';
import { MASKED_ERROR_TEXT, fitting, limitsOf, utf8Length } from '../frames.js';
import { publishStream } from '../publish-stream.js';
import type { IQPublisherLike, PublishResult, PublishStreamOptions } from '../publish-stream.js';

export interface PublishProtocolEventsOptions {
  /** Protocol channels to publish: `messages`, `tools`, `lifecycle`, `values`, … (`custom` covers `custom:<name>`). Default: all. */
  channels?: readonly string[];
  /** Awaited after the last frame and before the close line. Persist the result here. */
  beforeClose?: PublishStreamOptions<ProtocolEvent>['beforeClose'];
  /** The close line of a run that was aborted through its `AbortSignal`. Default `'fail'`. */
  abortOutcome?: 'end' | 'fail';
  /** Publish the error messages the events carry. Default false: each is replaced by "An error occurred.". */
  exposeErrors?: boolean;
  /** An event too large for a frame is not published, and reported here. */
  onError?: (error: unknown) => void;
}

/** The channels whose events say that something failed, with the error's message. */
const ERROR_CHANNELS = new Set(['lifecycle', 'tasks', 'tools', 'messages']);

/** The event without the error messages in it: `error` (a string, or an object with a `message`), and the `message` of an error event. */
function masked(event: ProtocolEvent): ProtocolEvent {
  const data = event.params.data;
  if (!ERROR_CHANNELS.has(event.method) || typeof data !== 'object' || data === null) return event;
  const { error, event: kind, message } = data as { error?: unknown; event?: unknown; message?: unknown };
  let patch: Record<string, unknown> | undefined;
  if (typeof error === 'string') patch = { error: MASKED_ERROR_TEXT };
  else if (typeof error === 'object' && error !== null) patch = { error: { ...error, message: MASKED_ERROR_TEXT } };
  else if ((kind === 'error' || kind === 'tool-error') && typeof message === 'string') patch = { message: MASKED_ERROR_TEXT };
  return patch === undefined ? event : { ...event, params: { ...event.params, data: { ...data, ...patch } } };
}

/**
 * Publishes what `agent.streamEvents(input, { version: 'v3' })` resolved to and closes the IQ stream
 * when the run ends: with `end`, or with `fail` when the run threw. Never rejects.
 */
export function publishProtocolEvents(
  run: AsyncIterable<ProtocolEvent>,
  publisher: IQPublisherLike,
  options: PublishProtocolEventsOptions = {},
): Promise<PublishResult> {
  const { channels, beforeClose, abortOutcome, exposeErrors = false, onError } = options;
  const fits = fitting(limitsOf(publisher));
  const wanted = (method: string): boolean =>
    channels === undefined || channels.includes(method) || (method.startsWith('custom:') && channels.includes('custom'));

  return publishStream(run, publisher, {
    encode(event) {
      if (!wanted(event.method)) return undefined;
      const frame = JSON.stringify(exposeErrors ? event : masked(event));
      if (fits(frame)) return frame;
      // Only the application knows how a client would reassemble an event, so it is dropped, not split.
      onError?.(new Error(`The ${event.method} event with seq ${event.seq} is ${utf8Length(frame)} bytes, over the frame limit, and was not published.`));
      return undefined;
    },
    ...(beforeClose === undefined ? {} : { beforeClose }),
    ...(abortOutcome === undefined ? {} : { abortOutcome }),
    ...(onError === undefined ? {} : { onError }),
  });
}
