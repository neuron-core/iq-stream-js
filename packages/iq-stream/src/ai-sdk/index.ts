/**
 * A `ChatTransport` for the AI SDK's `useChat` over any `ReadableStream` of UI message chunks.
 * `open` is the seam: IQ frames by default, or another transport's stream of the same objects.
 */
import type { ChatTransport, UIMessage, UIMessageChunk } from 'ai';
import { frames } from '../client/index.js';

/** What `useChat` asks for, as the application's backend needs it to start a run. */
export interface ChatStartRequest {
  chatId: string;
  messages: UIMessage[];
  trigger: 'submit-message' | 'regenerate-message';
  messageId?: string;
  body?: object;
  headers?: Record<string, string> | Headers;
  metadata?: unknown;
}

export type ChatResumeRequest = Pick<ChatStartRequest, 'chatId' | 'body' | 'headers' | 'metadata'>;

type Open<Ticket> = (ticket: Ticket, signal?: AbortSignal) => ReadableStream<UIMessageChunk>;

export type ChatTransportOptions<Ticket = { url: string }> = {
  /** Posts the request to the application and returns the ticket its backend answered with. */
  start(request: ChatStartRequest): Promise<Ticket>;
  /** The ticket of the run in flight for the chat, or `null`. */
  resume?(request: ChatResumeRequest): Promise<Ticket | null>;
  /** Called when the user aborts, so that the application can cancel the run: closing the subscription alone does not. */
  stop?(ticket: Ticket): void;
  /** The stream of chunks for a ticket. Default: `frames(ticket.url, { parse: JSON.parse, signal })`. */
  open?: Open<Ticket>;
} & (Ticket extends { url: string } ? unknown : { open: Open<Ticket> });

const fromIq = (ticket: { url: string }, signal?: AbortSignal): ReadableStream<UIMessageChunk> =>
  frames<UIMessageChunk>(ticket.url, { parse: JSON.parse, signal });

export function chatTransport<Ticket = { url: string }>(options: ChatTransportOptions<Ticket>): ChatTransport<UIMessage> {
  const { start, resume, stop } = options;
  const open = options.open ?? (fromIq as unknown as Open<Ticket>);

  function guard(ticket: Ticket, signal: AbortSignal | undefined): ReadableStream<UIMessageChunk> {
    const reader = open(ticket, signal).getReader();
    let told = false;   // the data layer already said how the run ended
    const onAbort = (): void => stop?.(ticket);
    const finished = (): void => signal?.removeEventListener('abort', onAbort);
    signal?.addEventListener('abort', onAbort, { once: true });
    return new ReadableStream<UIMessageChunk>({
      async pull(controller) {
        try {
          const { done, value } = await reader.read();
          if (done) {
            finished();
            return controller.close();
          }
          if (value.type === 'error' || value.type === 'abort') told = true;
          controller.enqueue(value);
        } catch (error) {
          finished();
          const { kind, reason } = (error ?? {}) as { kind?: unknown; reason?: unknown };
          // A failed or cancelled run arrives twice: as the protocol's own chunk, then as iq:fail. Once is enough.
          if (kind === 'fail' && reason === 'publisher' && told) controller.close();
          else controller.error(error);
        }
      },
      cancel(reason) {
        finished();
        return reader.cancel(reason);
      },
    });
  }

  return {
    // A new IQ subscriber gets the stream from its first frame, so the chat must reset the partial message.
    resumeStreamIsReplay: true,
    async sendMessages({ abortSignal, ...request }) {
      return guard(await start(request), abortSignal);
    },
    async reconnectToStream({ abortSignal, ...request }) {
      const ticket = await resume?.(request);
      return ticket ? guard(ticket, abortSignal) : null;
    },
  };
}
