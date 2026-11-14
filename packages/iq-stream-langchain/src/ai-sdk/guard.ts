const MODES = new Set(['values', 'updates', 'debug', 'messages', 'checkpoints', 'tasks', 'custom', 'tools']);

const isMode = (value: unknown): boolean => typeof value === 'string' && MODES.has(value);

/**
 * `toUIMessageStream` decides what it was given from the first chunk and converts anything else to
 * nothing, without an error: a graph stream opened with `streamMode: 'messages'` or with none yields
 * `start` and `finish` only. This says what is wrong with a first chunk, or `undefined` when the
 * converter can read it: `[mode, payload]`, `[namespace, mode, payload]`, a message chunk from
 * `model.stream()`, or an event of `streamEvents` v2.
 */
export function firstChunkProblem(chunk: unknown): string | undefined {
  if (Array.isArray(chunk)) {
    if (chunk.length === 2 && isMode(chunk[0])) return undefined;
    if (chunk.length === 3 && Array.isArray(chunk[0]) && isMode(chunk[1])) return undefined;
    return 'the stream yields bare payloads of one stream mode';
  }
  if (typeof chunk === 'object' && chunk !== null) {
    if ('content' in chunk) return undefined;
    if (typeof (chunk as { event?: unknown }).event === 'string' && 'data' in chunk) return undefined;
  }
  return 'the stream yields state objects, not [mode, payload] chunks';
}

/** Thrown into the run's result (never at the caller) when the stream cannot be converted. */
export class StreamOptionsError extends Error {
  constructor(problem: string) {
    super(`publishUIMessages cannot convert this stream: ${problem}. Open it with UI_STREAM_OPTIONS: agent.stream(input, { ...UI_STREAM_OPTIONS }).`);
    this.name = 'StreamOptionsError';
  }
}
