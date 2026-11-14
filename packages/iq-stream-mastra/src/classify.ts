import type { ChunkKind } from './types.js';

type Chunk = { type?: unknown; data?: { resumed?: unknown }; code?: unknown } | null | undefined;

/** AI SDK UI message chunks, as `@mastra/ai-sdk` writes them. */
export function classifyUIChunk(chunk: unknown): ChunkKind {
  const { type, data } = (chunk as Chunk) ?? {};
  if (type === 'error' || type === 'abort') return type;
  if (type === 'tool-approval-request') return 'suspend';
  // The resumed leg repeats the part with `resumed: true`: that one announces the answer, not a wait.
  if ((type === 'data-tool-call-approval' || type === 'data-tool-call-suspended') && !data?.resumed) return 'suspend';
  return undefined;
}

/** AG-UI events. A suspended run needs nothing here: the bridge ends it with `RUN_FINISHED` itself. */
export function classifyAgUiEvent(event: unknown): ChunkKind {
  const { type, code } = (event as Chunk) ?? {};
  if (type !== 'RUN_ERROR') return undefined;
  return code === 'abort' ? 'abort' : 'error';
}

/** Mastra's own chunks, for an application that publishes `fullStream` unconverted. */
export function classifyMastraChunk(chunk: unknown): ChunkKind {
  const { type } = (chunk as Chunk) ?? {};
  if (type === 'error' || type === 'abort') return type;
  if (type === 'tool-call-approval' || type === 'tool-call-suspended') return 'suspend';
  return undefined;
}
