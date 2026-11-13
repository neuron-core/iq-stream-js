/**
 * The parts of the IQ-Stream wire protocol that both sides of this package rely on.
 * See docs/transport-protocol.md in the server's repository. This module imports nothing.
 */

/** Stream ids: 16 to 128 characters of the base64url alphabet. */
export const STREAM_ID = /^[A-Za-z0-9_-]{16,128}$/;

/** Event names of frames. A colon is impossible, which keeps the `iq:` names the server's own. */
export const EVENT_NAME = /^[A-Za-z0-9_.-]{1,64}$/;

/** The server's default limit on one NDJSON line, in bytes. */
export const DEFAULT_MAX_FRAME_BYTES = 65_536;

/** The furthest `exp` the server accepts by default, in seconds from now. */
export const DEFAULT_MAX_TOKEN_TTL = 3600;

export type Scope = 'pub' | 'sub';

/** How a publisher closes a stream. */
export type CloseOutcome = 'end' | 'fail';

/** The server's own SSE events. Each ends a subscription. */
export const TERMINAL_EVENTS = ['iq:end', 'iq:fail', 'iq:gone'] as const;

/** What a browser needs to subscribe. */
export interface Ticket {
  streamId: string;
  /** The `sub` token. */
  token: string;
  /** The subscribe URL with the token in its query: ready for `new EventSource(url)`. */
  url: string;
  /** Unix seconds at which the token expires. */
  expiresAt: number;
}
