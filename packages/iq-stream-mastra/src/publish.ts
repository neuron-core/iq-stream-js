import { toAISdkStream } from '@mastra/ai-sdk';
import { MastraAgentNetworkStream, WorkflowRunOutput } from '@mastra/core/stream';
import type { Publisher } from '@neuron-core/iq-stream';
import { classifyUIChunk } from './classify.js';
import { pipeToIQ, readerOf } from './pipe.js';
import type { PipeOptions } from './pipe.js';
import type { ChunkSource, IQTicket, PublishOptions, PublishedRun, UIVersion } from './types.js';

/**
 * The error text a browser gets unless the application says otherwise. `@mastra/ai-sdk` would
 * serialize the error object, stack trace and file paths included.
 */
export const maskedError = (): string => 'An error occurred.';

/** Reads a source to its end and looks at nothing: reading is what makes a Mastra run proceed. */
async function drain(source: ChunkSource): Promise<void> {
  try {
    const reader = readerOf(source);
    while (!(await reader.read()).done);
  } catch {
    // The run's own failure is the application's to see, where it started the run.
  }
}

/** Mints the ticket, opens the publisher and starts the loop without waiting for it. */
export async function startPublishing<T>(
  source: ChunkSource<T>,
  options: PublishOptions,
  pipe: PipeOptions<T>,
  run: { runId?: string | undefined; abort?: ((reason?: unknown) => void) | undefined } = {},
): Promise<PublishedRun> {
  const { iq, closeOnCancel, oversize, suspendGraceMs, waitUntil, onResult } = options;
  let ticket: IQTicket;
  let publisher: Publisher;
  try {
    ticket = await iq.ticket(options.streamId, options.subTokenTtl);
    if (run.runId !== undefined) ticket = { ...ticket, runId: run.runId };
    publisher = iq.publisher(ticket.streamId);
  } catch (error) {
    // No stream for this answer, but the run has started: it is read to its end all the same, so that it saves.
    void drain(source);
    throw error;
  }
  const done = pipeToIQ(source, publisher, {
    ...pipe,
    ...(closeOnCancel === undefined ? {} : { closeOnCancel }),
    ...(oversize === undefined ? {} : { oversize }),
    ...(suspendGraceMs === undefined ? {} : { suspendGraceMs }),
  }).then((result) => {
    try {
      onResult?.(result, ticket);
    } catch {
      // A reporting callback must not turn a published run into a rejection.
    }
    return result;
  });
  waitUntil?.(done);
  return { ticket, done, abort: run.abort ?? (() => undefined) };
}

/**
 * Publishes a stream of AI SDK UI message chunks, such as the result of `handleChatStream`,
 * `handleWorkflowStream` or `handleNetworkStream`. Resolves as soon as the ticket exists; `done` is
 * the run. `runId` goes into the ticket, and `abort` becomes the run's `abort()`.
 */
export function publishUIMessageStream(
  stream: ChunkSource,
  options: PublishOptions & { runId?: string; abort?: (reason?: unknown) => void },
): Promise<PublishedRun> {
  return startPublishing(stream, options, { classify: classifyUIChunk }, { runId: options.runId, abort: options.abort });
}

/** A `MastraModelOutput` or a `WorkflowRunOutput`, by the one member the adapter reads. */
export interface MastraOutput {
  readonly fullStream: ChunkSource;
  readonly runId?: string;
}

/** What `agent.stream()`, `run.stream()`, `agent.network()`, a durable agent or `agent.observe()` return, or a bare stream of Mastra chunks. */
export type MastraSource = MastraOutput | { readonly output: MastraOutput } | ChunkSource;

export interface UIStreamOptions {
  sendStart?: boolean;
  sendFinish?: boolean;
  sendReasoning?: boolean;
  sendSources?: boolean;
  includeSubAgentMetadata?: boolean;
  lastMessageId?: string;
  messageMetadata?: (args: { part: unknown; traceId?: string }) => unknown;
  /** The text of an `error` chunk. Default: a fixed message that says nothing about the error. */
  onError?: (error: unknown) => string;
}

export interface PublishToIQOptions extends PublishOptions {
  /** Default: `workflow` for a workflow output, `network` for a network stream, else `agent`. */
  from?: 'agent' | 'workflow' | 'network';
  /** The major of `ai` in the front end. Default `v5`, as in `@mastra/ai-sdk`. */
  version?: UIVersion;
  /** Options of `toAISdkStream`. */
  ui?: UIStreamOptions;
}

/** Converts a Mastra result to UI message chunks with `toAISdkStream` and publishes them. */
export function publishToIQ(source: MastraSource, options: PublishToIQOptions): Promise<PublishedRun> {
  const { from, version = 'v5', ui, ...publish } = options;
  const result = 'output' in source && !('getReader' in source) ? source.output : source;
  const detected = result instanceof WorkflowRunOutput ? 'workflow' : result instanceof MastraAgentNetworkStream ? 'network' : 'agent';
  // The overloads of toAISdkStream pair each source class with its options; the pairing is made here, at run time.
  const convert = toAISdkStream as unknown as (stream: unknown, options: object) => ReadableStream<unknown>;
  const stream = convert(result, { onError: maskedError, ...ui, from: from ?? detected, version });
  const runId = (result as { runId?: unknown }).runId;
  return publishUIMessageStream(stream, { ...publish, ...(typeof runId === 'string' ? { runId } : {}) });
}
