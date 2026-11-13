/**
 * An `EventSource`-like source over `fetch()`, for what `EventSource` cannot do: send the token in a
 * header instead of the URL, read the status of a refusal, and deliver every event name.
 */
import type { EventSourceLike, SourceEvent } from '../client/index.js';

export interface FetchSourceOptions {
  /** The `sub` token, sent as `Authorization: Bearer`. Leave it out when the URL carries it. */
  token?: string;
  /** Resume after this id. */
  lastEventId?: string;
  fetch?: typeof fetch;
  /** A `503` is tried again `attempts` times (default 3), after `delayMs` (default 1000), doubling, before the source closes. */
  unavailable?: { attempts?: number; delayMs?: number };
  /** The longest line accepted, in characters (default 1 MiB). A longer one closes the source: it is not an IQ stream. */
  maxLineLength?: number;
}

const CONNECTING = 0, OPEN = 1, CLOSED = 2;

/**
 * Opens the stream at `url`. The source resumes by itself after a dropped connection, with the
 * `Last-Event-ID` header, and reports a refusal as an `error` event that carries `status` and `error`.
 */
export function fetchSource(url: string | URL, options: FetchSourceOptions = {}): EventSourceLike {
  const { token, unavailable = {}, maxLineLength = 1_048_576 } = options;
  const fetcher = options.fetch ?? globalThis.fetch;
  const listeners = new Map<string, Array<(event: SourceEvent) => void>>();
  let state = CONNECTING;
  let lastId = options.lastEventId;
  let retryMs = 2000;
  let refusals = 0;
  let abort: AbortController | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;

  function emit(type: string, event: SourceEvent, wildcard = false): void {
    for (const listener of [...(listeners.get(type) ?? []), ...(wildcard ? listeners.get('*') ?? [] : [])]) {
      // As with EventSource, a listener that throws does not stop the stream: the error surfaces as uncaught.
      try { listener(event); } catch (error) { queueMicrotask(() => { throw error; }); }
    }
  }
  function reconnect(delay: number): void {
    if (state === CLOSED) return;
    state = CONNECTING;
    emit('error', { type: 'error' });
    timer = setTimeout(connect, delay);
  }
  function fail(details: SourceEvent = {}): void {
    if (state === CLOSED) return;
    state = CLOSED;
    abort?.abort();
    emit('error', { type: 'error', ...details });
  }

  async function connect(): Promise<void> {
    abort = new AbortController();
    let response: Response;
    try {
      response = await fetcher(url, {
        signal: abort.signal,
        cache: 'no-store',
        headers: {
          Accept: 'text/event-stream',
          ...(token === undefined ? {} : { Authorization: `Bearer ${token}` }),
          ...(lastId === undefined ? {} : { 'Last-Event-ID': lastId }),
        },
      });
    } catch {
      return reconnect(retryMs);   // a network error: resume, as EventSource does
    }
    if (state === CLOSED) return;
    if (response.status !== 200 || !response.body) {
      let code: unknown;
      try { code = (await response.json() as { error?: unknown } | null)?.error; } catch { /* no JSON body */ }
      // The server is at capacity or restarting: worth waiting for, unlike a refusal of this subscriber.
      if (response.status === 503 && refusals < (unavailable.attempts ?? 3)) {
        return reconnect((unavailable.delayMs ?? 1000) * 2 ** refusals++ * (0.5 + Math.random() / 2));
      }
      return fail({ status: response.status, ...(typeof code === 'string' ? { error: code } : {}) });
    }
    refusals = 0;
    state = OPEN;
    emit('open', { type: 'open' });

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '', name = '', data: string[] = [], id = lastId, afterCR = false;
    const line = (text: string): void => {
      if (text === '') {
        lastId = id;
        if (data.length) emit(name || 'message', { type: name || 'message', data: data.join('\n'), lastEventId: lastId ?? '' }, true);
        name = '';
        data = [];
        return;
      }
      const colon = text.indexOf(':');
      if (colon === 0) return;   // a comment, such as the server's heartbeat
      const field = colon < 0 ? text : text.slice(0, colon);
      const value = colon < 0 ? '' : text.slice(colon + (text[colon + 1] === ' ' ? 2 : 1));
      if (field === 'data') data.push(value);
      else if (field === 'event') name = value;
      else if (field === 'id') { if (!value.includes('\0')) id = value; }
      else if (field === 'retry' && /^\d+$/.test(value)) retryMs = Number(value);
    };
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        let text = decoder.decode(value, { stream: true });
        if (text === '') continue;
        // A line ends with LF, CR LF or CR; the LF of a CR LF may come in the next chunk.
        if (afterCR && text.startsWith('\n')) text = text.slice(1);
        afterCR = text.endsWith('\r');
        buffer += text;
        let start = 0;
        for (const end of buffer.matchAll(/\r\n?|\n/g)) {
          line(buffer.slice(start, end.index));
          if (state === CLOSED) return;   // a listener closed the source
          start = end.index + end[0].length;
        }
        buffer = buffer.slice(start);
        if (buffer.length > maxLineLength) return fail();
      }
    } catch { /* aborted by close(), or the connection dropped */ }
    reconnect(retryMs);
  }

  void connect();
  return {
    wildcard: true,
    get readyState() { return state; },
    addEventListener(type, listener) {
      const list = listeners.get(type) ?? [];
      listeners.set(type, list);
      list.push(listener);
    },
    close() {
      state = CLOSED;
      clearTimeout(timer);
      abort?.abort();
    },
  };
}
