/** Errors shared by the publisher and the client. This module imports nothing. */

/** Base class of every error this package raises. Never carries a token, the secret or frame data. */
export class IQError extends Error {
  // `declare`: a class field would make each of them an own property, `undefined` when not given.
  /** The stream the error concerns, when there is one. */
  declare streamId?: string;
  /** The HTTP status of the server's answer, when there was one. */
  declare status?: number;
  /** The server's error code (`stream_closed`, `frame_too_large`, …), when it gave one. */
  declare code?: string;

  constructor(message: string, details: { streamId?: string; status?: number; code?: string; cause?: unknown } = {}) {
    super(message, details.cause === undefined ? undefined : { cause: details.cause });
    this.name = 'IQError';
    if (details.streamId !== undefined) this.streamId = details.streamId;
    if (details.status !== undefined) this.status = details.status;
    if (details.code !== undefined) this.code = details.code;
  }
}
