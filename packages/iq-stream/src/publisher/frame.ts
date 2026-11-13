import { IQError } from '../shared/errors.js';
import { EVENT_NAME } from '../shared/protocol.js';
import type { CloseOutcome } from '../shared/protocol.js';

/** Thrown by `send()` for a line over the limit, so the caller can truncate or split the payload. */
export class FrameTooLarge extends IQError {
  /** Bytes of the line, envelope and escapes included. */
  readonly size: number;
  readonly limit: number;

  constructor(size: number, limit: number) {
    super(`The line is ${size} bytes; the limit is ${limit}.`);
    this.name = 'FrameTooLarge';
    this.size = size;
    this.limit = limit;
  }
}

const encoder = new TextEncoder();

/**
 * One frame as an NDJSON line, newline included. JSON.stringify leaves non-ASCII text and slashes
 * unescaped, which keeps lines small and the server on its fast path.
 */
export function encodeFrame(data: string, event?: string): Uint8Array {
  if (typeof data !== 'string') throw new TypeError('Frame data must be a string.');
  if (event === undefined || event === null) return encoder.encode(`${JSON.stringify({ data })}\n`);
  if (typeof event !== 'string' || !EVENT_NAME.test(event)) throw new TypeError('Invalid event name.');
  return encoder.encode(`${JSON.stringify({ event, data })}\n`);
}

/** The size the server measures against its limit: the line without its newline. */
export function frameSize(data: string, event?: string): number {
  return encodeFrame(data, event).length - 1;
}

export function closeLine(outcome: CloseOutcome): Uint8Array {
  return encoder.encode(`{"close":"${outcome}"}\n`);
}

export function concat(parts: Uint8Array[]): Uint8Array {
  if (parts.length === 1) return parts[0]!;
  const out = new Uint8Array(parts.reduce((total, part) => total + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}
