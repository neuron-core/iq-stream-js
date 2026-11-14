// The application's side of the chat, as the README shows it: one function that starts a run and
// answers with the ticket, one that finds the run in flight for a chat, one that cancels a run.
import { toBaseMessages } from '@ai-sdk/langchain';
import { UI_STREAM_OPTIONS, publishUIMessages } from '../../../dist/ai-sdk/index.js';

export function chatBackend({ iq, agentFor, log = console.error }) {
  const running = new Map();   // chat id -> { streamId, controller, done } of the run in flight

  /** POST /chat: starts the run, publishes it in the background and returns what the page subscribes with. */
  async function start({ id: chatId, messages }, scenario) {
    const ticket = await iq.ticket();                    // { streamId, token, url, expiresAt }
    const publisher = iq.publisher(ticket.streamId);     // the request to the IQ server opens now
    const run = { streamId: ticket.streamId, controller: new AbortController() };
    running.set(chatId, run);

    const stream = await agentFor(scenario).stream(
      { messages: await toBaseMessages(messages) },
      { ...UI_STREAM_OPTIONS, signal: run.controller.signal },
    );
    // Not awaited: the page gets its ticket now, and the run is published while it reads.
    run.done = publishUIMessages(stream, publisher, {
      onFinish: async (state) => { /* save state.messages for chatId here: it is awaited before the close line */ },
      onError: (error) => log(`chat ${chatId}: ${error.message}`),
    });
    void run.done.then(() => {
      if (running.get(chatId) === run) running.delete(chatId);
    });
    return ticket;
  }

  /** GET /chat/:id/stream: a ticket for the run in flight, or null. A reloaded page resumes with it. */
  async function resume(chatId) {
    const run = running.get(chatId);
    return run ? iq.ticket(run.streamId) : null;
  }

  /** POST /chat/stop/:streamId: cancels the run. Its stream closes with an `abort` chunk and `fail`. */
  function stop(streamId) {
    for (const run of running.values()) if (run.streamId === streamId) run.controller.abort();
  }

  return { start, resume, stop, running };
}
