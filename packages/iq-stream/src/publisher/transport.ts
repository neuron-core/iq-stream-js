/**
 * What the writer needs from HTTP. The state machine is written once against this interface; the
 * `node:http` and `fetch` transports, and the tests' scripted one, are thin implementations of it.
 */

/** The server's answer to one request. `status` is 0 when the connection gave none. */
export interface Answer {
  status: number;
  body: Record<string, unknown>;
}

export const NO_ANSWER: Answer = { status: 0, body: {} };

/** A `POST /<streamId>` whose body is still open. */
export interface OpenRequest {
  /** Resolves once the bytes were handed to the connection, or the request is over. Never rejects. One write at a time. */
  write(bytes: Uint8Array): Promise<void>;
  /** Ends the body. */
  end(): void;
  /** True as soon as the server starts to answer or the connection is lost: nothing more may be written. */
  readonly settled: boolean;
  /** Never rejects. */
  readonly answer: Promise<Answer>;
  /** Drops the connection. `answer` then resolves, with status 0 if the server had not answered. */
  destroy(): void;
}

export interface Transport {
  /** Opens a chunked `POST` and sends its head at once. */
  open(streamId: string, token: string): OpenRequest;
  /** A short `POST` with its whole body: the probe, the out-of-band close, a batch. Never rejects. */
  post(streamId: string, token: string, body: Uint8Array, timeoutMs: number): Promise<Answer>;
}

export function parseBody(text: string): Record<string, unknown> {
  try {
    const value: unknown = JSON.parse(text);
    return typeof value === 'object' && value !== null ? (value as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}
