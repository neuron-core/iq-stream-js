import { handleChatStream } from '@mastra/ai-sdk';
import type { ChatStreamHandlerOptions } from '@mastra/ai-sdk';
import { maskedError, publishUIMessageStream } from './publish.js';
import type { PublishOptions, PublishedRun } from './types.js';

export type PublishChatOptions = PublishOptions & ChatStreamHandlerOptions;

type Message = { role?: unknown; parts?: { state?: unknown; approval?: { id?: unknown } }[] };

/**
 * An AI SDK 6 or 7 front end answers a tool approval by sending the messages again, and
 * `handleChatStream` then resumes the run named in the approval id (`<runId>::<toolCallId>`), whatever
 * `runId` it was given. This finds that run, under the conditions `handleChatStream` applies.
 */
function resumedRunId({ params, version }: ChatStreamHandlerOptions): string | undefined {
  const messages = params.messages as Message[];
  if (version !== 'v6' && version !== 'v7') return undefined;
  if (params.resumeData || params.trigger === 'regenerate-message' || !Array.isArray(messages) || messages.at(-1)?.role !== 'assistant') return undefined;
  let runId: string | undefined;
  for (const message of messages) {
    if (message.role !== 'assistant') continue;
    for (const part of message.parts ?? []) {
      const id = part.state === 'approval-responded' ? part.approval?.id : undefined;
      if (typeof id === 'string' && id.lastIndexOf('::') > 0) runId = id.slice(0, id.lastIndexOf('::'));
    }
  }
  return runId;
}

/**
 * `handleChatStream`, published: starts the agent on a `useChat` request body and resolves with the
 * ticket once the run has started. Rejects only when the run could not be started (an unknown agent,
 * an invalid body); nothing is opened on the server then.
 *
 * The adapter chooses the run id unless `params.runId` names one, and gives the run an abort signal of
 * its own (`run.abort()`): the run does not end with the HTTP request that started it.
 */
export async function publishChat(options: PublishChatOptions): Promise<PublishedRun> {
  const { iq, streamId, subTokenTtl, closeOnCancel, oversize, suspendGraceMs, waitUntil, onResult, ...chat } = options;
  const controller = new AbortController();
  const given = chat.params.abortSignal;
  const runId = chat.params.runId ?? crypto.randomUUID();
  const start = handleChatStream as unknown as (options: ChatStreamHandlerOptions) => Promise<ReadableStream<unknown>>;
  const stream = await start({
    ...chat,
    onError: chat.onError ?? maskedError,
    params: { ...chat.params, runId, abortSignal: given ? AbortSignal.any([given, controller.signal]) : controller.signal },
  });
  return publishUIMessageStream(stream, {
    iq, streamId, subTokenTtl, closeOnCancel, oversize, suspendGraceMs, waitUntil, onResult,
    runId: resumedRunId(chat) ?? runId,
    abort: (reason) => controller.abort(reason),
  });
}
