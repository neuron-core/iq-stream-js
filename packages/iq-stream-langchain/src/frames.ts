import type { IQPublisherLike } from './publish-stream.js';

/** The server's default `MAX_FRAME_BYTES`. */
export const DEFAULT_MAX_FRAME_BYTES = 65_536;

/** What a browser is told when the run failed, unless the application chose to expose the real text. */
export const MASKED_ERROR_TEXT = 'An error occurred.';

const encoder = new TextEncoder();

/** UTF-8 bytes of a string. */
export const utf8Length = (text: string): number => encoder.encode(text).length;

/** The size of an unnamed frame on the wire, as the core and the server measure it: the NDJSON line without its newline. */
export const lineSize = (data: string): number => utf8Length(JSON.stringify({ data }));

export interface FrameLimits {
  /** The limit one frame must fit, in bytes on the wire. Default 65,536. */
  maxFrameBytes?: number;
  /** The size a frame would have on the wire. Default: the core's envelope. */
  frameSize?: (data: string) => number;
}

/** A predicate for "this frame fits", from explicit limits or from a publisher's own. */
export function fitting(limits: FrameLimits = {}): (data: string) => boolean {
  const { maxFrameBytes = DEFAULT_MAX_FRAME_BYTES, frameSize = lineSize } = limits;
  return (data) => frameSize(data) <= maxFrameBytes;
}

export const limitsOf = (publisher: IQPublisherLike): FrameLimits => ({
  ...(publisher.maxFrameBytes === undefined ? {} : { maxFrameBytes: publisher.maxFrameBytes }),
  ...(publisher.frameSize === undefined ? {} : { frameSize: (data: string) => publisher.frameSize!(data) }),
});

/**
 * Cuts a text in halves, never inside a surrogate pair, until `fits` accepts every piece. A piece of
 * one code point that still does not fit is returned as it is: the caller sees that it does not fit.
 */
export function splitText(text: string, fits: (piece: string) => boolean): string[] {
  if (fits(text)) return [text];
  let middle = text.length >> 1;
  const unit = text.charCodeAt(middle);
  if (unit >= 0xdc00 && unit <= 0xdfff) middle++;
  if (middle <= 0 || middle >= text.length) return [text];
  return [...splitText(text.slice(0, middle), fits), ...splitText(text.slice(middle), fits)];
}

/** What stands in for a value too large for a frame. `bytes` is the size of the value as JSON. */
export const truncationMarker = (value: unknown): { truncated: true; bytes: number } => ({
  truncated: true,
  bytes: utf8Length(JSON.stringify(value) ?? ''),
});
