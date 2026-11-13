import { IQError } from '../shared/errors.js';
import { NO_ANSWER, parseBody } from './transport.js';
import type { Answer, OpenRequest, Transport } from './transport.js';

const NEWLINE = Uint8Array.of(10);

/**
 * `fetch` with a streamed body, for runtimes without `node:http`. With `duplex: 'half'` the response can
 * settle long before the body ends, and from then on a write into the body never resolves: so every
 * write is raced against the response, and nothing is written once it has settled.
 */
export function fetchTransport(url: string, fetcher: typeof fetch = (input, init) => fetch(input, init)): Transport {
  if (!/^https?:\/\//.test(url)) throw new IQError('@neuron-core/iq-stream: a unix:// url needs the Node build; this runtime reaches the server over http(s).');

  const send = (streamId: string, token: string, init: RequestInit): Promise<Response> =>
    // Inside a promise, so that a fetch that throws at once is a lost request like any other.
    Promise.resolve().then(() =>
      fetcher(`${url}/${streamId}`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/x-ndjson' },
        ...init,
      }),
    );
  const answerOf = (response: Promise<Response>): Promise<Answer> =>
    response.then(async (res) => ({ status: res.status, body: parseBody(await res.text()) })).catch(() => NO_ANSWER);

  return {
    open(streamId, token): OpenRequest {
      const abort = new AbortController();
      const body = new TransformStream<Uint8Array, Uint8Array>();
      const writer = body.writable.getWriter();
      writer.closed.catch(() => undefined);
      let settled = false;
      let pending: (() => void) | undefined;
      const settle = (): void => {
        settled = true;
        pending?.();
      };
      const response = send(streamId, token, { body: body.readable, duplex: 'half', signal: abort.signal } as RequestInit);
      response.then(settle, settle);
      // fetch may hold the head back until the body has its first chunk (Node's does). An empty line is a
      // keepalive to the server, and makes the request open now: a subscriber may be waiting for its publisher.
      writer.write(NEWLINE).catch(() => undefined);
      return {
        get settled() {
          return settled;
        },
        answer: answerOf(response),
        write: (bytes) =>
          new Promise<void>((done) => {
            if (settled) return done();
            pending = done;
            writer.write(bytes).then(done, done);
          }),
        end: () => {
          writer.close().catch(() => undefined);
        },
        destroy: () => {
          abort.abort();
          writer.abort().catch(() => undefined);
        },
      };
    },

    post(streamId, token, body, timeoutMs): Promise<Answer> {
      const abort = new AbortController();
      const timer = setTimeout(() => abort.abort(), timeoutMs);
      return answerOf(send(streamId, token, { body: body as BodyInit, signal: abort.signal })).finally(() => clearTimeout(timer));
    },
  };
}
