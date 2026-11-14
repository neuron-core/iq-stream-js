# @neuron-core/iq-stream-langchain

Publishes a [LangChain JS](https://github.com/langchain-ai/langchainjs) or [LangGraph.js](https://github.com/langchain-ai/langgraphjs) run to an [IQ-Stream server](https://github.com/neuron-core/iq-stream), so that browsers read the answer over Server-Sent Events and a reloaded page picks it up where it is. It is a thin layer over [`@neuron-core/iq-stream`](../iq-stream), which holds the connection to the server.

The adapter reads the stream your code already gets from `agent.stream()` or `graph.stream()`. It is not a callback handler and not middleware: nothing is patched, and a slow or broken publisher cannot delay or fail the run. Three entry points, one per wire format:

| Import | Publishes | The browser reads it with |
| --- | --- | --- |
| `@neuron-core/iq-stream-langchain/ai-sdk` | AI SDK UI message chunks, converted by `toUIMessageStream` of `@ai-sdk/langchain` | `useChat` and `chatTransport` of `@neuron-core/iq-stream/ai-sdk` |
| `@neuron-core/iq-stream-langchain/protocol` | LangGraph's protocol events, from `streamEvents(input, { version: 'v3' })` | an `AgentServerAdapter` of your own for `useStream` of `@langchain/react` |
| `@neuron-core/iq-stream-langchain` | any async iterable or `ReadableStream`, encoded by you | `subscribe()` or `frames()` of `@neuron-core/iq-stream/client` |

It ships ES modules only and has no dependency of its own.

```sh
npm install @neuron-core/iq-stream @neuron-core/iq-stream-langchain @ai-sdk/langchain ai
```

## Backend: publish an agent's answer

`agent` is a `createAgent` agent or any compiled graph. The function below is the body of the route that receives the user's message (`POST /chat`); answer the request with the ticket it returns, as JSON.

```ts
import { IQClient } from '@neuron-core/iq-stream';
import { publishUIMessages, UI_STREAM_OPTIONS } from '@neuron-core/iq-stream-langchain/ai-sdk';
import { toBaseMessages } from '@ai-sdk/langchain';
import type { UIMessage } from 'ai';

const iq = new IQClient();   // IQ_STREAM_URL, IQ_STREAM_APP_KEY, IQ_STREAM_APP_SECRET
const running = new Map<string, { streamId: string; controller: AbortController }>();

async function startChat({ id: chatId, messages }: { id: string; messages: UIMessage[] }) {
  const input = { messages: await toBaseMessages(messages) };
  const ticket = await iq.ticket();                    // { streamId, token, url, expiresAt }
  const publisher = iq.publisher(ticket.streamId);     // the request to the IQ server opens now
  const controller = new AbortController();
  running.set(chatId, { streamId: ticket.streamId, controller });

  // Neither call is awaited: the page gets its ticket now, and the run is published while it reads.
  const stream = agent.stream(input, { ...UI_STREAM_OPTIONS, signal: controller.signal });
  void publishUIMessages(stream, publisher, {
    onFinish: (state) => saveAnswer(chatId, state),    // awaited before the close line
    onError: (error) => console.error(`chat ${chatId}: ${error.message}`),
  }).then(() => running.delete(chatId));
  return ticket;                                       // answer the page's POST with it, as JSON
}
```

`running` is what two more routes need: one that gives a reloaded page a ticket for the run in flight (`iq.ticket(run.streamId)`, or `204` when there is none), and one that cancels a run (`run.controller.abort()`). The [browser side](#browser-usechat) calls both.

What `publishUIMessages` does:

- **One frame per chunk.** Each UI message chunk is one unnamed frame whose data is the chunk as JSON, in the order the run emitted it.
- **The close line follows the run.** When the run finishes, `onFinish` is awaited and the stream closes with `end`; so what a late browser loads from you is saved before any browser is told the answer is complete. When the run throws, the page gets an `error` chunk and the stream closes with `fail`. `toUIMessageStream` alone would end such a stream normally, which is why a plain `pipeTo(publisher.writable())` is not enough.
- **A run that waits for a human is not a failure.** An interrupt from `humanInTheLoopMiddleware` ends the stream with `tool-approval-request` and `end`. The decision starts a new run, on a new stream.
- **It never rejects and never stops reading.** The returned promise resolves with `{ outcome, error?, frames }`. If the server cannot be reached or closes the stream, the core reports it once and the adapter goes on reading the run, so that `onFinish` still saves the answer.
- **Frames fit the limit.** A text, reasoning or tool-input delta over the publisher's `maxFrameBytes` is sent as several deltas. A tool input, tool output or `data-*` payload over it is replaced by `{ "truncated": true, "bytes": n }` and reported to `onError`; the whole value is in the state `onFinish` receives.

### `publishUIMessages(stream, publisher, options?)`

`stream` is what `agent.stream()`, `graph.stream()` or `model.stream()` returns, or the promise of it: a promise that rejects is a run that failed before its first chunk, and closes the stream with `fail` like any other. `publisher` is the core's `Publisher`; `RecordingPublisher` of `@neuron-core/iq-stream/testing` fits too, for your own tests.

| Option | |
| --- | --- |
| `onFinish(finalState)` | Awaited after the last chunk and before the close line, with the final graph state (the last `values` chunk). Save the answer here. Not called when the run failed or was cancelled. If it throws, the page gets an `error` chunk and the stream closes with `fail`. |
| `onError(error)` | The run failed, the stream could not be converted, a chunk was too large for a frame, or `onFinish` threw. Not called for a cancel. |
| `abortOutcome` | The close line of a cancelled run: `'fail'` (default) or `'end'`, for an application that treats a stopped answer as final. |
| `exposeErrors` | `false` (default): every `errorText` sent to the browser, of the run's `error` chunk and of a failed tool call, is `An error occurred.`; the real error goes to `onError` and into the result. `true` publishes the messages as they are. |
| `sendDone` | Also publish the AI SDK's `[DONE]` frame after `finish`. Default `false`: `chatTransport` does not need it, and it is the one frame that is not JSON. |

### The stream options matter

Spread `UI_STREAM_OPTIONS` (`{ streamMode: ['values', 'messages', 'custom'] }`) into the call that opens the stream; `subgraphs: true` may be added, and chunks of a sub-agent then carry `providerMetadata.langchain.namespace`. The converter reads tokens and tool results from `messages`, a tool call's input and the final state from `values`, and `config.writer()` data from `custom`. Other sets convert badly without an error: adding `'tools'` duplicates tool chunks, and one mode given as a string, or no `streamMode`, converts to an empty answer. `publishUIMessages` checks the first chunk and turns that last case into a failure you can see: `onError` gets a `StreamOptionsError` and the stream closes with `fail`.

### Cancelling a run

Cancel through the `signal` given to `agent.stream()`. The page then gets the AI SDK's `abort` chunk, which is how it tells a cancel from a failure, and the stream closes with `fail` (or with `abortOutcome`). Do not call `stream.cancel()` on a stream the adapter is reading.

## Browser: `useChat`

The page uses `useChat` with the transport from the core package. Nothing of this adapter runs in the browser.

```tsx
import { useChat } from '@ai-sdk/react';
import { chatTransport } from '@neuron-core/iq-stream/ai-sdk';

const transport = chatTransport({
  start: async ({ chatId: id, messages }) =>
    (await fetch('/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id, messages }) })).json(),
  resume: async ({ chatId: id }) => {
    const response = await fetch(`/chat/${id}/stream`);
    return response.status === 204 ? null : response.json();
  },
  stop: (ticket) => { void fetch(`/chat/stop/${ticket.streamId}`, { method: 'POST' }); },
});

function Chat({ chatId }) {
  const { messages, sendMessage, stop, status, error } = useChat({ id: chatId, transport, resume: true });
  // A tool call is a part of type 'dynamic-tool', with toolName, input, output and state.
}
```

Use the same major version of `ai` in the backend and in the page: the chunks are passed to the chat as they were published.

## Other formats

### LangGraph protocol events

For a front end built on `useStream` of `@langchain/react`. The framework marks `streamEvents` v3 as experimental.

```ts
import { publishProtocolEvents } from '@neuron-core/iq-stream-langchain/protocol';

const run = agent.streamEvents(input, { version: 'v3' });
const result = await publishProtocolEvents(run, publisher, {
  channels: ['messages', 'tools', 'lifecycle', 'values'],
  onError: (error) => console.warn(error),                    // an event too large for a frame
});
```

Each event is one frame, as JSON, with the run's own `seq`. Options: `channels` (default: all; `custom` covers `custom:<name>`), `beforeClose(outcome, error)` to save the result before the close line, `abortOutcome`, `exposeErrors`, and `onError`. An event over the frame limit cannot be split and is not published; it is reported to `onError`. That is most likely a `values` event, which carries the whole state and grows with the conversation: leave `values` out of `channels` if the page does not need it. Error messages in `lifecycle`, `tasks`, `tools` and `messages` events are replaced by `An error occurred.` unless `exposeErrors` is set; what the run puts into its state is published as it is.

The browser side of this format is not part of the core package: `useStream` needs an `AgentServerAdapter` that opens the IQ stream and filters events by channel and namespace.

### Any stream

`publishStream` is the loop the two functions above are built on.

```ts
import { publishStream } from '@neuron-core/iq-stream-langchain';

const stream = graph.stream(input, { streamMode: 'updates' });
const result = await publishStream(stream, publisher, {
  encode: (update) => JSON.stringify(update),                 // or several frames, or none
  beforeClose: async (outcome, error) => { /* save the result */ },
});
// result: { outcome: 'end' | 'fail', error?, frames }
```

Options: `encode` (default: one frame, `JSON.stringify(chunk)`), `beforeClose`, `abortOutcome`, and `onError` for a chunk that could not be encoded or that the publisher refused as too large (it is dropped).

### UI message chunks you already have

`uiMessageFrames()` is the frame-limit step alone, as a `TransformStream` from UI message chunks to frame strings. This handler returns the answer as the HTTP response and publishes the same chunks, which adds resume to an ordinary AI SDK endpoint and keeps a serverless invocation alive for as long as the run:

```ts
import { publishStream } from '@neuron-core/iq-stream-langchain';
import { uiMessageFrames } from '@neuron-core/iq-stream-langchain/ai-sdk';
import { toUIMessageStream } from '@ai-sdk/langchain';
import { createUIMessageStreamResponse } from 'ai';

let failed = false;
const chunks = toUIMessageStream(stream, { onError: () => { failed = true; }, onAbort: () => { failed = true; } });
const [toResponse, toIq] = chunks.tee();
const frames = toIq.pipeThrough(uiMessageFrames({ maxFrameBytes: publisher.maxFrameBytes }));
const publishing = publishStream(frames, publisher, {
  encode: (frame) => frame,
  beforeClose: () => { if (failed) throw new Error('the run failed'); },   // closes with fail
});
const response = createUIMessageStreamResponse({ stream: toResponse });
```

Here the error text is the converter's, unmasked. Options of `uiMessageFrames`: `maxFrameBytes`, `frameSize` (pass `(data) => publisher.frameSize(data)` to measure as the publisher does) and `onError` for a truncated or dropped chunk.

## Where the run lives

The process that runs the graph must live as long as the run. On a long-lived Node server the `void publishUIMessages(...)` above is enough. On a serverless platform, hand the promise to the platform's `waitUntil`, or return the answer as the response as in the last sample. With a job queue, the route mints the ticket and enqueues the job, and the worker creates the publisher and runs the graph; call `iq.reserve(streamId)` in the route when the queue can be slower than the 30 seconds a subscriber waits for a publisher.

During a tool call the run emits nothing; the core's keepalive holds the request. The stream id is never put into the graph's config: LangGraph does not need to know it.

## Requirements

- `@neuron-core/iq-stream` 0.1 and what it requires: Node 20.19 or later, an IQ-Stream server 1.x.
- For `/ai-sdk`: `@ai-sdk/langchain` 3 or later and `ai` 7 or later. For `/protocol`: `@langchain/langgraph` 1.7 or later, for its types only.
- Tested on Node 22 with `langchain` 1.6.6, `@langchain/core` 1.4.1, `@langchain/langgraph` 1.7.4, `@ai-sdk/langchain` 3.0.172 and `ai` 7.0.172. Earlier versions and other runtimes were not run.

## Limits worth knowing

- A stream is one run. A frame is at most 65,536 bytes and a stream at most 10 MiB and 30 minutes, unless the app has limits of its own on the server. A run that exceeds them is closed by the server with `iq:fail`; the adapter keeps reading it and `onFinish` still runs.
- An application that only calls `agent.invoke()` has nothing to publish: switch the call to `agent.stream()`.
- Graphs served by LangGraph Server (`langgraph-api`) are not covered: that server runs the graph and streams it itself.
- With `subgraphs: true`, `@ai-sdk/langchain` 3.0.172 repeats a tool call's `tool-input-start` and `tool-input-available` after its output, with the same tool call id.
- On the protocol path, `@langchain/core` 1.4.1 merges a streamed tool call into the text block when the model sends the call with chunk index 0 next to text and implements only `_streamResponseChunks`; the agent then ends without running the tool. `agent.stream()`, and so `publishUIMessages`, is not affected.
