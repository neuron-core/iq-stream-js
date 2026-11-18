# @neuron-core/iq-stream-mastra

Publishes the answer of a [Mastra](https://mastra.ai) agent to browsers through an [IQ-Stream server](https://github.com/neuron-core/iq-stream). The backend holds one outbound request per answer; the browsers read the answer from the server, which also replays it to a tab that reloads or arrives late.

It is a thin layer over two packages that do the work: [`@neuron-core/iq-stream`](https://github.com/neuron-core/iq-stream-js/tree/main/packages/iq-stream), which talks to the server, and `@mastra/ai-sdk`, which turns a Mastra run into the Vercel AI SDK's UI message chunks. So the front end is the AI SDK's `useChat`, unchanged, with the transport from `@neuron-core/iq-stream/ai-sdk`.

```sh
npm install @neuron-core/iq-stream @neuron-core/iq-stream-mastra @mastra/ai-sdk
```

## On Mastra's server: `iqChatRoute`

```ts
import { Mastra } from '@mastra/core/mastra';
import { IQClient } from '@neuron-core/iq-stream';
import { iqChatRoute } from '@neuron-core/iq-stream-mastra';
import { weatherAgent } from './agents/weather';

const iq = new IQClient();   // IQ_STREAM_URL, IQ_STREAM_PUBLIC_URL, IQ_STREAM_APP_KEY, IQ_STREAM_APP_SECRET

export const mastra = new Mastra({
  agents: { weatherAgent },
  server: { apiRoutes: iqChatRoute({ path: '/iq/chat/:agentId', iq, version: 'v6' }) },
});
```

Three routes are registered:

| Route | |
| --- | --- |
| `POST <path>` | Takes the request body of `useChat`, starts the agent and answers the **ticket** at once, as JSON. The run goes on after the response and is published in the background. `404` for an unknown agent and `400` for a body without messages; nothing is opened on the server then. |
| `POST <path>/abort` | Body `{ "streamId": "…" }`. Cancels the run: `204`, or `404` when this process does not run that stream. |
| `GET <path>/:chatId/stream` | A new ticket for the run in flight for that chat, or `204`. It is what `useChat({ resume: true })` asks after a page reload. |

The ticket is the core's, with the Mastra run id:

```json
{"streamId":"dfHgzW8c9yIyuIat-k0EAw","token":"…","url":"https://stream.example.com/dfHgzW8c9yIyuIat-k0EAw?token=…","expiresAt":1794683449,"runId":"1edbe755-aabd-4b4b-b23a-a8efca9f482f"}
```

**Set `version` to the major of the `ai` package in your front end** (`'v5'`, `'v6'` or `'v7'`). The default is `'v5'`, as in `@mastra/ai-sdk`.

The routes go through Mastra's authentication like every `registerApiRoute` route. Keep it that way: a ticket is as good as the answer. When your authentication puts a resource id into the request context (`MASTRA_RESOURCE_ID_KEY`), the reconnect route answers a chat id only to the user who started the run.

As with Mastra's own `chatRoute`, the other fields of the request body reach the agent as execution options (`memory`, `maxSteps`, …) and win over `defaultOptions`. Two are not the browser's to choose: the run id, which the server picks unless the body resumes a run with `resumeData`, and the abort signal.

## The page

```tsx
import { useChat } from '@ai-sdk/react';
import { lastAssistantMessageIsCompleteWithApprovalResponses } from 'ai';
import { chatTransport } from '@neuron-core/iq-stream/ai-sdk';

const route = '/iq/chat/weatherAgent';
const json = { 'Content-Type': 'application/json' };

const transport = chatTransport({
  start: async ({ chatId: id, messages, trigger, messageId }) => {
    const response = await fetch(route, { method: 'POST', headers: json, body: JSON.stringify({ id, messages, trigger, messageId }) });
    if (!response.ok) throw new Error((await response.json()).error);
    return response.json();                                                   // the ticket
  },
  resume: async ({ chatId: id }) => {
    const response = await fetch(`${route}/${id}/stream`);
    return response.status === 204 ? null : response.json();
  },
  stop: (ticket) => { void fetch(`${route}/abort`, { method: 'POST', headers: json, body: JSON.stringify({ streamId: ticket.streamId }) }); },
});

const { messages, sendMessage, stop, status, error, addToolApprovalResponse } = useChat({
  id: chatId, transport, resume: true,
  sendAutomaticallyWhen: lastAssistantMessageIsCompleteWithApprovalResponses,   // an answered approval card starts the second leg
});
```

Send the body as `useChat` gives it: `handleChatStream` of `@mastra/ai-sdk` reads `id`, `messages`, `trigger` and `messageId` from it. The `id` is what the reconnect route finds the run by.

This package ships no browser code. The transport, and what it does with a failed, a cancelled or a lost stream, are described in the [core's README](https://github.com/neuron-core/iq-stream-js/tree/main/packages/iq-stream#bridges).

## Without Mastra's server: `publishChat`

The same in a route handler of Next.js, or of anything else:

```ts
import { publishChat } from '@neuron-core/iq-stream-mastra';

export async function POST(request: Request): Promise<Response> {
  const run = await publishChat({ iq, mastra, agentId: 'weatherAgent', params: await request.json(), version: 'v6', waitUntil });
  return Response.json(run.ticket);
}
```

`publishChat` is `handleChatStream` plus the publishing: it takes the same options and resolves as soon as the run has started and the ticket exists. It rejects only when the run could not be started. The run gets a run id and an abort signal from the adapter, not the request's: the request ends with the ticket, the run must not. `run.abort()` cancels it, and `run.done` resolves with the result.

On a serverless platform pass its `waitUntil` (for instance from `@vercel/functions`), which keeps the invocation alive until the answer is published.

## A run you started yourself: `publishToIQ`

```ts
import { publishToIQ } from '@neuron-core/iq-stream-mastra';

const output = await weatherAgent.stream(question, { memory: { thread: 'thread-1', resource: 'user-1' } });
const run = await publishToIQ(output, { iq, version: 'v6' });   // resolves once the ticket exists
void run.done.then((result) => {                                // { outcome: 'end' | 'fail', … }, never rejects
  if (result.outcome === 'fail') console.warn('not published', result.cause, result.error);
});
return run;                                                     // run.ticket goes to the browser
```

It converts the result with `toAISdkStream` and publishes the chunks. It takes what `agent.stream()` returns, what a workflow's `run.stream()` returns, the `{ output }` of a durable agent or of `agent.observe()`, an agent network's stream, or a bare stream of Mastra chunks. Workflow outputs and network streams are recognized; `from: 'agent' | 'workflow' | 'network'` says it explicitly. `ui` carries the options of `toAISdkStream` (`sendReasoning`, `sendSources`, `sendStart`, `sendFinish`, `includeSubAgentMetadata`, `lastMessageId`, `messageMetadata`, `onError`).

Your own code can keep reading `output.fullStream`, `output.text` and the rest: Mastra gives every reader the whole run.

For a stream that is already UI message chunks (the result of `handleChatStream`, `handleWorkflowStream` or `handleNetworkStream`), call `publishUIMessageStream(stream, { iq, runId, abort })`.

## AG-UI: `publishAgUi`

For a front end that speaks AG-UI, with the events of `@ag-ui/mastra` (an optional peer dependency):

```ts
import { MastraAgent } from '@ag-ui/mastra';
import { publishAgUi } from '@neuron-core/iq-stream-mastra';

const agent = new MastraAgent({ agentId: 'weatherAgent', agent: weatherAgent, resourceId: 'user-1' });
const run = await publishAgUi(agent.run(input), { iq });
```

Each event is one frame. `RUN_ERROR` closes the stream with `fail`. `run.abort()` unsubscribes, which is how that bridge cancels its run, and ends the stream with a `RUN_ERROR` whose code is `abort`, as an `HttpAgent` ends an aborted run. The page reads it with `StreamAgent` from `@neuron-core/iq-stream/ag-ui`.

## The loop itself: `pipeToIQ`

All of the above is one function over a publisher of the core. Use it for a stream of your own:

```ts
import { classifyMastraChunk, pipeToIQ } from '@neuron-core/iq-stream-mastra';

const output = await weatherAgent.stream(question);
const ticket = await iq.ticket();
const result = await pipeToIQ(output.fullStream, iq.publisher(ticket.streamId), { classify: classifyMastraChunk });
```

It reads a `ReadableStream` or an async iterable, sends each chunk as one unnamed frame (`serialize`, default `JSON.stringify`), and closes once. `classify` tells it what a chunk says about the ending: `'error'`, `'abort'`, `'suspend'` or nothing. `classifyUIChunk`, `classifyAgUiEvent` and `classifyMastraChunk` are the three the package uses. Mastra's own chunks are an escape hatch: they carry the provider request and the whole message list, so they are large and say more than a browser should read.

## How a stream closes

A failed or cancelled Mastra run does not throw: its stream ends normally after an `error` or `abort` chunk. So the adapter reads the chunks to choose the close line.

| The run | Frames | Close | `result` |
| --- | --- | --- | --- |
| finished | … `finish` | `end` | `{ outcome: 'end' }` |
| failed | … `error`, `finish` | `fail` | `{ outcome: 'fail', cause: 'error' }` |
| was cancelled | … `abort`, `finish` | `fail`, or `end` with `closeOnCancel: 'end'` | `{ cause: 'abort' }` |
| waits for a tool approval or a suspended tool | … `tool-approval-request`, `data-tool-call-approval` | `end` | `{ outcome: 'end', suspended: true }` |
| was stopped by a processor (tripwire) | … `data-tripwire`, `finish` | `end` | `{ outcome: 'end' }` |
| its stream itself broke | what came before | `fail` | `{ outcome: 'fail', cause: 'stream-error', error }` |

A leg that waits for an approval is complete: the frames tell the page to show the card, and the answer to it is a new request, a new ticket and a new stream id under the same `runId`. An AI SDK 6 or 7 page sends its messages again with the approval on the tool part; an AI SDK 5 page sends `resumeData` with the `runId` of the ticket. A source that stays open while it waits (a durable agent, `agent.observe()`) is left `suspendGraceMs` after its last chunk.

`result.outcome` is what the subscribers got. It is `fail` too when publishing gave up, whatever the run did; `result.error` then holds the core's error and `result.cause` is absent.

## Options

Every publish function and the route take these:

| Option | Default | |
| --- | --- | --- |
| `iq` | | The `IQClient`. Its own options apply: `minOpen` (a short answer stays open three seconds for a page that subscribes late), `keepalive`, `publicUrl`, `onError`, `errorHandler`, … |
| `streamId` | a new one | Not on the route. For a job whose ticket another process handed out. |
| `subTokenTtl` | `3600` | Seconds the browser's token is valid. |
| `closeOnCancel` | `'fail'` | The close line of a cancelled run. The `abort` chunk before it tells the page it was a cancel. |
| `oversize` | `'omit'` | A chunk over the frame limit: `'omit'` sends a placeholder, `'drop'` nothing, a function `(chunk, bytes) => smaller chunk or null` what it returns. |
| `suspendGraceMs` | `1000` | See above. |
| `waitUntil` | | Receives the publishing promise. |
| `onResult` | | `(result, ticket)`, once the stream is closed. |

`iqChatRoute` adds `path`, `agent` (when the path has no `:agentId`) and `reconnect` (default `true`), and takes the options of `handleChatStream`: `version`, `defaultOptions`, `agentVersion`, `sendStart`, `sendFinish`, `sendReasoning`, `sendSources`, `includeSubAgentMetadata`, `onError`, `messageMetadata`, `experimentalTransform`.

## What to know

- **The error text is masked.** `@mastra/ai-sdk` would put the serialized error into the `error` chunk, stack trace and file paths included. This package sends `An error occurred.` unless you pass `onError` (`ui.onError` for `publishToIQ`).
- **The answer must be saved before the stream closes**, because a tab that arrives after the close gets `iq:gone` and has only your backend to load it from. With Mastra memory this needs nothing: Mastra saves the turn before the `finish` chunk reaches a reader, and the adapter closes after the stream ended. **An agent without memory saves nothing**: save the result yourself in the run's `onFinish` option, which Mastra awaits at the same point.
- **Publishing never breaks the run.** When the server is unreachable or refuses the stream, the adapter keeps reading the run to its end, so the agent finishes and saves; `done` resolves with `outcome: 'fail'` and never rejects.
- **A chunk over the frame limit** (65,536 bytes; a tool result, large tool arguments, a file) is replaced by `{"type":"data-iq-omitted","transient":true,"data":{"type":"tool-output-available","toolCallId":"…","bytes":100123}}`. The AI SDK keeps a transient part out of the message and hands it to `onData`, where the page can show a placeholder and load the value from your backend. For AG-UI the placeholder is a `CUSTOM` event named `iq-omitted`.
- **Cancelling is explicit.** A page that stops listening does not stop the run; `POST <path>/abort` does. A tool that is running when the abort arrives finishes first. The route knows the runs of its own process only. With several instances, call Mastra's own `POST /api/agents/:agentId/runs/:runId/abort` with the `runId` of the ticket, which reaches the instance that runs it.
- **The reconnect route** keeps `chat id → stream` in `mastra.serverCache` while a run is in flight. Mastra's default cache is in memory; with a shared cache the route answers from every instance.
- **Silence is the core's.** While a tool runs, Mastra emits nothing and the core's keepalives hold the request. Do not send frames as heartbeats.
- **Limits of the server**: a stream takes 10 MiB and one hour. A run that outlasts them ends for its subscribers with `iq:fail`, and goes on in the backend.

## Requirements

- `@mastra/core` 1.83 or later and `@mastra/ai-sdk` 1.12 or later, of the 1.x line. The tests run on exactly these versions, with `@ag-ui/mastra` 2.0.2 and `ai` 7.
- Node 22.13 or later, which is what Mastra requires.
- `@neuron-core/iq-stream` 0.1 and an IQ-Stream server 1.x.
- ES modules only.

Not covered by tests: durable and evented agents, agent networks, sub-agent delegation, serverless and edge runtimes, and a shared `serverCache`.
