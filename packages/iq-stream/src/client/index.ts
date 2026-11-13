/**
 * The subscriber's side of the IQ-Stream transport protocol ("Client requirements" in the server's
 * docs/transport-protocol.md), once. It knows nothing about what a frame carries.
 */
import { IQError } from '../shared/errors.js';

/** Marks this build in a bundle, for the packaging tests. */
export const BUILD = 'iq-stream/client';

/** How a subscription ended. Members of a terminal event that this version does not know are kept too. */
export type Outcome =
  | { type: 'end'; lastEventId?: number }
  | { type: 'fail'; reason: string; lastEventId?: number }
  | { type: 'gone'; outcome: 'end' | 'fail' | 'unknown'; reason?: string; lastEventId?: number }
  /** `status` and `code` only when the source can know them: `EventSource` cannot. */
  | { type: 'refused'; status?: number; code?: string }
  /** `close()` was called here. */
  | { type: 'closed' };

export interface Frame {
  data: string;
  /** The publisher's event name, `message` for an unnamed frame. */
  event: string;
  /** Opaque: only ever sent back to the server. */
  id: string;
}

/** What a source passes to a listener: the members of the browser's events that the client reads. */
export interface SourceEvent {
  readonly type?: string;
  readonly data?: unknown;
  readonly lastEventId?: string;
  /** The HTTP status of a refusal, from sources that can read it. */
  readonly status?: unknown;
  /** The same, as the `eventsource` package for Node names it. */
  readonly code?: unknown;
  /** The server's error code for a refusal, from sources that can read it. */
  readonly error?: unknown;
}

/** The part of `EventSource` the client uses. */
export interface EventSourceLike {
  readonly readyState: number;
  addEventListener(type: string, listener: (event: SourceEvent) => void): void;
  close(): void;
  /**
   * True when the source also delivers every event, whatever its name, to listeners of `*`, with the
   * name in the event's `type`. `EventSource` cannot; the `fetch` source does.
   */
  readonly wildcard?: boolean;
}

/** What a source function is given when the client opens or reopens the source. */
export interface Resume {
  /** The id of the last frame received, to resume after it. */
  lastEventId?: string;
  /** What `retry.refresh` returned for this attempt. */
  url?: string;
}

export type Target = string | URL | ((resume: Resume) => EventSourceLike);

export interface SubscribeOptions {
  /** The event names the publisher uses, besides unnamed frames. `EventSource` delivers only names it listens to. */
  events?: string[];
  /**
   * Reopen after a refusal: `attempts` times (default 0), waiting `delayMs` (default 1000) and doubling.
   * `refresh` is asked for a new subscribe URL before each attempt. Without it a 401 or 403 is final.
   */
  retry?: { attempts?: number; delayMs?: number; refresh?: () => Promise<string | undefined> };
  /** For runtimes without a global `EventSource`. */
  EventSource?: new (url: string) => EventSourceLike;
}

export interface SubscribeHandlers {
  /** Every frame, in order. An exception thrown here closes the subscription and propagates. */
  onFrame(frame: Frame): void;
  /** Exactly once. */
  onClose?(outcome: Outcome): void;
}

export interface Subscription {
  close(): void;
  /**
   * Resolves at the source's first `open` event, for a page that subscribes before it starts the run.
   * Rejects with an `IQStreamError` when the subscription is refused or closed before that.
   */
  opened: Promise<void>;
  done: Promise<Outcome>;
  readonly lastEventId: string | undefined;
}

/** How a subscription ended, as an error for stream consumers. */
export class IQStreamError extends IQError {
  /** `closed` only on the rejection of `opened`. */
  kind: 'fail' | 'gone' | 'refused' | 'closed';
  /** The server's reason, for `fail`, and for `gone` when the stream had failed. */
  declare reason?: string;
  /** How the stream itself ended, for `gone`. */
  declare outcome?: 'end' | 'fail' | 'unknown';
  /** The number of frames the stream held, when the server said it. */
  declare lastEventId?: number;

  constructor(outcome: Exclude<Outcome, { type: 'end' }>) {
    super(
      outcome.type === 'fail' ? `stream failed: ${outcome.reason}`
        : outcome.type === 'gone' ? `stream gone (${outcome.outcome})`
          : outcome.type === 'refused' ? 'subscription refused' : 'subscription closed',
      outcome.type === 'refused' ? { status: outcome.status, code: outcome.code } : {},
    );
    this.name = 'IQStreamError';
    this.kind = outcome.type;
    if (outcome.type === 'fail' || outcome.type === 'gone') {
      if (outcome.reason !== undefined) this.reason = outcome.reason;
      if (outcome.lastEventId !== undefined) this.lastEventId = outcome.lastEventId;
    }
    if (outcome.type === 'gone') this.outcome = outcome.outcome;
  }
}

const CLOSED = 2;
const numeric = (value: unknown): number | undefined => (typeof value === 'number' ? value : undefined);

/** A terminal event's outcome: the server's members, with the ones this client types made sure of. */
function terminal(type: 'end' | 'fail' | 'gone', event: SourceEvent): Outcome {
  let members: Record<string, unknown> = {};
  try {
    const value: unknown = JSON.parse(String(event.data));
    if (value && typeof value === 'object' && !Array.isArray(value)) members = { ...value };
  } catch { /* not JSON: the subscription still ends */ }
  if (typeof members.lastEventId !== 'number') delete members.lastEventId;
  if (typeof members.reason !== 'string') delete members.reason;
  if (type === 'end') return { ...members, type };
  if (type === 'fail') return { reason: 'unknown', ...members, type };
  return { ...members, outcome: members.outcome === 'end' || members.outcome === 'fail' ? members.outcome : 'unknown', type };
}

/**
 * Subscribes to a stream. `target` is a subscribe URL, or a function that returns an `EventSource`-like
 * object and is called again to reopen.
 */
export function subscribe(target: Target, handlers: SubscribeHandlers, options: SubscribeOptions = {}): Subscription {
  const names = ['message', ...(options.events ?? [])];
  const { attempts: maxAttempts = 0, delayMs = 1000, refresh } = options.retry ?? {};
  let source: EventSourceLike | undefined;
  let url: string | undefined;
  let lastId: string | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let attempts = 0;
  let over = false;
  let resolveOpened!: () => void;
  let rejectOpened!: (error: IQStreamError) => void;
  let resolveDone!: (outcome: Outcome) => void;
  const opened = new Promise<void>((resolve, reject) => { resolveOpened = resolve; rejectOpened = reject; });
  const done = new Promise<Outcome>((resolve) => { resolveDone = resolve; });
  opened.catch(() => {});   // a page that never awaits `opened` must not see an unhandled rejection

  function finish(outcome: Outcome): void {
    if (over) return;
    over = true;
    clearTimeout(timer);
    source?.close();
    if (outcome.type === 'refused' || outcome.type === 'closed') rejectOpened(new IQStreamError(outcome));
    else resolveOpened();   // a terminal event came through the source, so it was open
    resolveDone(outcome);
    handlers.onClose?.(outcome);
  }

  function create(): EventSourceLike {
    if (typeof target === 'function') return target({ lastEventId: lastId, url });
    const EventSource = options.EventSource ?? (globalThis.EventSource as SubscribeOptions['EventSource']);
    if (!EventSource) throw new IQError('No EventSource in this runtime: pass options.EventSource, or a function that returns a source');
    const address = new URL(url ?? target, globalThis.location?.href);
    // A new EventSource sends no Last-Event-ID header; the server takes the id as a query parameter.
    if (lastId !== undefined) address.searchParams.set('lastEventId', lastId);
    return new EventSource(address.href);
  }

  function open(): void {
    const mine = (source = create());
    const live = (): boolean => !over && source === mine;
    const frame = (name?: string) => (event: SourceEvent) => {
      if (!live() || typeof event.data !== 'string') return;
      const eventName = name ?? event.type ?? 'message';
      if (eventName.startsWith('iq:')) return;
      attempts = 0;
      resolveOpened();
      if (event.lastEventId) lastId = event.lastEventId;
      try {
        handlers.onFrame({ data: event.data, event: eventName, id: event.lastEventId ?? '' });
      } catch (error) {
        finish({ type: 'closed' });
        throw error;
      }
    };
    if (mine.wildcard) mine.addEventListener('*', frame());
    else for (const name of names) mine.addEventListener(name, frame(name));
    mine.addEventListener('open', () => { if (live()) resolveOpened(); });
    mine.addEventListener('iq:end', (event) => { if (live()) finish(terminal('end', event)); });
    mine.addEventListener('iq:fail', (event) => { if (live()) finish(terminal('fail', event)); });
    mine.addEventListener('iq:gone', (event) => { if (live()) finish(terminal('gone', event)); });
    mine.addEventListener('error', (event) => {
      if (!live() || mine.readyState !== CLOSED) return;   // CONNECTING: the source resumes by itself
      const status = numeric(event.status) ?? numeric(event.code);
      const refused: Outcome = {
        type: 'refused',
        ...(status === undefined ? {} : { status }),
        ...(typeof event.error === 'string' ? { code: event.error } : {}),
      };
      // Only a new token heals a 401 or 403, and only `refresh` can bring one.
      if (attempts >= maxAttempts || ((status === 401 || status === 403) && !refresh)) return finish(refused);
      source = undefined;   // this source is over: whatever it still emits must not start a second retry
      timer = setTimeout(async () => {
        try {
          url = (await refresh?.()) ?? url;
          if (!over) open();
        } catch {
          finish(refused);
        }
      }, delayMs * 2 ** attempts++);
    });
  }

  open();
  return { close: () => finish({ type: 'closed' }), opened, done, get lastEventId() { return lastId; } };
}

export interface FramesOptions<T> extends SubscribeOptions {
  /**
   * Turns a frame's data into the value to enqueue, usually `JSON.parse`. An exception errors the stream.
   * It is given the data alone, so that `JSON.parse` never takes a second argument for its reviver.
   */
  parse?: (data: string) => T;
  /** Aborting it errors the stream with the signal's reason and closes the subscription. */
  signal?: AbortSignal;
}

/**
 * The same subscription as a `ReadableStream`. It closes on `iq:end` and errors with an `IQStreamError`
 * on `iq:fail`, `iq:gone` and a refusal. A source cannot be paused, so frames the reader has not taken
 * wait in the stream's queue, which the server's limit on a stream's size bounds.
 */
export function frames<T = Frame>(target: Target, options: FramesOptions<T> = {}): ReadableStream<T> {
  const { parse, signal } = options;
  let subscription: Subscription | undefined;
  let over = false;
  let failure: { error: unknown } | undefined;
  let onAbort: (() => void) | undefined;
  const settle = (): boolean => {
    if (over) return false;
    over = true;
    if (onAbort) signal?.removeEventListener('abort', onAbort);
    return true;
  };
  return new ReadableStream<T>({
    start(controller) {
      // Erroring a stream discards its queue, and the frames before a failure are owed to the reader:
      // unless the reader is told to stop, the error waits until the queue is empty.
      const fail = (error: unknown, now = false): void => {
        if (!settle()) return;
        subscription?.close();
        if (now || (controller.desiredSize ?? 0) > 0) controller.error(error);
        else failure = { error };
      };
      if (signal?.aborted) return fail(signal.reason, true);
      subscription = subscribe(target, {
        onFrame(frame) {
          if (over) return;
          try {
            controller.enqueue(parse ? parse(frame.data) : (frame as T));
          } catch (error) {
            fail(error);
          }
        },
        onClose(outcome) {
          if (outcome.type !== 'end') fail(new IQStreamError(outcome));
          else if (settle()) controller.close();
        },
      }, options);
      onAbort = () => fail(signal?.reason, true);
      signal?.addEventListener('abort', onAbort, { once: true });
    },
    pull(controller) {
      if (failure) controller.error(failure.error);
    },
    cancel() {
      settle();
      subscription?.close();
    },
  });
}
