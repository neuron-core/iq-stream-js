# @neuron-core/iq-stream

The JavaScript client of the [IQ-Stream server](https://github.com/neuron-core/iq-stream), which relays an agent's answer from a backend to browsers over Server-Sent Events. One package holds both sides:

- a **publisher** for the backend, which sends an answer to the server over one held request and gives the browser a ticket to read it;
- a **client** for the browser, which reads the stream with `EventSource` and tells a finished answer from a failed or a lost one;
- two **bridges** that connect the client to the Vercel AI SDK's `useChat` and to AG-UI.

It has no runtime dependency and ships ES modules only.

```sh
npm install @neuron-core/iq-stream
```

## Backend: publish an answer

```ts
import { IQClient } from '@neuron-core/iq-stream';

const iq = new IQClient({
  url: 'http://iq.internal:8080',            // how this backend reaches the server
  publicUrl: 'https://stream.example.com',   // how browsers reach it
  appKey: process.env.IQ_STREAM_APP_KEY,
  appSecret: process.env.IQ_STREAM_APP_SECRET,
});

app.post('/chat', async (request, response) => {
  const ticket = await iq.ticket();                  // { streamId, token, url, expiresAt }
  const publisher = iq.publisher(ticket.streamId);   // the request to the server opens now
  response.json(ticket);                             // the browser can subscribe at once

  try {
    for await (const chunk of agent.stream(request.body.messages)) {
      publisher.send(JSON.stringify(chunk));         // one frame: written and flushed, never awaited
    }
    await saveAnswer(request.body.chatId);           // before the close: late browsers load it from you
    await publisher.close('end');
  } catch (error) {
    await publisher.close('fail');
  }
});
```

Without options the client reads `IQ_STREAM_URL`, `IQ_STREAM_PUBLIC_URL`, `IQ_STREAM_APP_KEY` and `IQ_STREAM_APP_SECRET` from the environment.

What the publisher does for you:

- **One request per answer.** It opens a single chunked `POST` when it is created and holds it to the close line, which is what the server is built for. A frame is one line of it.
- **Silence is covered.** While a tool runs it writes a keepalive line every 20 seconds, so the server's read timeout does not let the request go.
- **A lost request is continued.** After a `408` or a dropped connection it asks the server how many frames it holds and resends exactly the missing ones. If the gap cannot be bridged, it closes the stream with `fail` and stops, so that no browser gets an answer with a hole in it.
- **Publishing never breaks the run.** By default an error on the way to the server is reported once (to `errorHandler`, or with `console.warn`), the publisher goes inert, and `send()` returns `false`. Your agent finishes and saves its answer. `onError: 'raise'` makes `send()` and `close()` throw instead.
- **A quick answer is not lost.** The close line is held back until the stream is three seconds old (`minOpen`), so a browser that subscribes just after a short answer still receives it. Frames are never delayed.

### `IQClient`

| Method | |
| --- | --- |
| `ticket(streamId?, ttl?)` | What the browser needs: a new (or given) stream id, a `sub` token, the subscribe URL and the token's expiry. |
| `publisher(streamId)` | The publisher of one stream. |
| `newStreamId()` | 128 random bits as base64url. Never reuse an id for a second answer. |
| `token(scope, streamId, ttl?)`, `subscribeUrl(streamId, ttl?)` | The parts of a ticket. `tokenSync()` exists in the Node build. |
| `verify(scope, token, streamId)` | Whether a token is one this backend minted for that stream. For a page that subscribes first and then asks for the run to start, with its stream id. |
| `reserve(streamId)` | One empty request that takes the stream out of `pending`. A subscriber waits 30 seconds for a publisher; when the run is a queued job that may wait longer, call this where the ticket is handed out. |
| `closeStream(streamId, outcome?)` | Closes a stream from a process that never had its publisher, such as a job's failure handler. |

### `Publisher`

| Member | |
| --- | --- |
| `send(data, event?)` | Queues one frame and returns whether the publisher is still alive. `data` is a string, usually JSON. Throws `FrameTooLarge` when the line would exceed `maxFrameBytes`, so that you can shorten the payload. |
| `close(outcome?, { timeout? })` | Sends the close line, `'end'` or `'fail'`, and resolves with the server's answer. The first outcome wins. |
| `abort()` | Drops the connection and leaves the stream open on the server. |
| `writable()` | A `WritableStream` of strings or `{ data, event }`: closing it closes with `end`, aborting it with `fail`. A converter that turns a failed run into an error chunk and then ends normally would close with `end` through a pipe; call `send()` and `close()` yourself for those. |
| `frameSize(data, event?)`, `maxFrameBytes` | The size a frame would have on the wire, and the limit it must fit (65,536 bytes unless the app has its own). |
| `framesSent`, `bytesSent`, `alive`, `error` | What was queued, whether the publisher gave up, and why. |

Options beyond the ones above: `keepalive`, `minOpen`, `maxFrameBytes`, `retryBudget`, `takeoverBudget`, `replayFrames`, `replayBytes`, `queueBytes`, and `mode: 'batch'` with `batchMs` for a runtime that cannot stream a request body (sequential short requests; the server does more work per request that way). Durations are in seconds unless the name ends in `Ms`.

Which close line: `end` when the answer finished, `fail` when it did not, a cancelled run included. Send your protocol's own final event (`finish`, `RUN_FINISHED`, an `abort` chunk) as a frame before the close line; it is what tells the page why.

## Browser: read a stream

```ts
import { subscribe } from '@neuron-core/iq-stream/client';

const ticket = await (await fetch('/chat', { method: 'POST', body })).json();

const subscription = subscribe(ticket.url, {
  onFrame: (frame) => render(JSON.parse(frame.data)),
  onClose: (outcome) => {
    if (outcome.type === 'fail') showError(outcome.reason);
    if (outcome.type === 'gone') loadAnswerFromBackend();   // frames this tab missed no longer exist
    if (outcome.type === 'refused') retryLater();
  },
});
```

A subscription ends exactly once, with one outcome:

| Outcome | Meaning |
| --- | --- |
| `{ type: 'end' }` | The publisher finished, and this tab has every frame. |
| `{ type: 'fail', reason }` | The stream closed without finishing: `publisher`, `publisher_idle`, `max_duration`, `too_large`, … |
| `{ type: 'gone', outcome }` | The stream is over and frames this tab never received are gone. Load the answer from your backend. |
| `{ type: 'refused', status? }` | The server did not accept the subscription: an invalid or expired token, too many subscribers, an outage. |
| `{ type: 'closed' }` | `close()` was called here. |

`subscription.opened` resolves when the connection is established, for a page that subscribes before it starts the run. `subscription.done` resolves with the outcome.

`frames(url, { parse })` is the same subscription as a `ReadableStream`: it closes on `end` and errors with an `IQStreamError` otherwise, after the reader has taken the frames that came before.

```ts
import { frames } from '@neuron-core/iq-stream/client';

for await (const chunk of frames(ticket.url, { parse: JSON.parse })) render(chunk);
```

Three things to know:

- **Named frames must be announced.** `EventSource` delivers a named event only to a listener for that name. If your publisher sends `send(data, 'token')`, pass `{ events: ['token'] }`. Unnamed frames need nothing, which is why they are the better default.
- **A token can expire mid-stream.** `EventSource` reconnects with the URL it was given. Pass `retry: { attempts, refresh }`, where `refresh` returns a fresh subscribe URL from your backend, and the client reopens from the last frame it received.
- **Close what you open.** Call `subscription.close()` when the component unmounts or the page hides. A stream takes ten subscribers by default.

### Without a token in the URL

`EventSource` can only carry the token in the URL and cannot read why a subscription was refused. A source built on `fetch()` does both:

```ts
import { subscribe } from '@neuron-core/iq-stream/client';
import { fetchSource } from '@neuron-core/iq-stream/fetch';

const bare = `${iqUrl}/${ticket.streamId}`;
subscribe(({ lastEventId }) => fetchSource(bare, { token: ticket.token, lastEventId }), handlers);
```

It resumes with the `Last-Event-ID` header, reports a refusal's status and error code in the outcome, retries a `503` by itself, and delivers every event name without `events`.

## Bridges

A bridge connects a `ReadableStream` of protocol objects to a front-end SDK. By default it reads IQ frames; `open` lets it read any other stream of the same objects.

### Vercel AI SDK

For a backend that publishes UI message chunks (the output of `toUIMessageStream`, `@mastra/ai-sdk`, laravel/ai's Vercel protocol):

```tsx
import { useChat } from '@ai-sdk/react';
import { chatTransport } from '@neuron-core/iq-stream/ai-sdk';

const transport = chatTransport({
  start: async ({ chatId, messages, trigger, messageId }) =>
    (await fetch('/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: chatId, messages, trigger, messageId }) })).json(),          // → the ticket
  resume: async ({ chatId }) => {
    const response = await fetch(`/chat/${chatId}/stream`);                                    // the run in flight, if any
    return response.status === 204 ? null : response.json();
  },
  stop: (ticket) => void fetch(`/chat/stop/${ticket.streamId}`, { method: 'POST' }),
});

const { messages, sendMessage, status } = useChat({ id: chatId, transport, resume: true });
```

`resume` is what makes a page reload continue the answer: the server replays a running stream from its first frame. `stop` tells your backend to cancel; a page that merely stops listening does not stop the run.

### AG-UI

For a backend that publishes AG-UI events:

```ts
import { StreamAgent } from '@neuron-core/iq-stream/ag-ui';

const agent = new StreamAgent({
  threadId,
  start: async (input) =>
    (await fetch('/agent', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(input) })).json(),
});

await agent.runAgent();
```

`StreamAgent` extends `AbstractAgent` from `@ag-ui/client`, so CopilotKit and other AG-UI front ends take it where they take an `HttpAgent`. `@ag-ui/client` and `rxjs` are peer dependencies of this entry point only.

Both bridges treat a failed or cancelled run the same way: the protocol's own event (`error`, `abort`, `RUN_ERROR`) already told the SDK, so the `iq:fail` that follows is not reported a second time.

## One package, two sides

The publisher is given your app secret and must never run in a page. The package's export map decides what an import resolves to from the environment a build targets:

| Import | In a server build | In a browser build |
| --- | --- | --- |
| `@neuron-core/iq-stream` | The publisher (`node:http` on Node, Bun and Deno; `fetch` on Workers and edge runtimes) | A stub with the same exports, which throws when used and names the right import |
| `@neuron-core/iq-stream/client`, `/fetch`, `/ai-sdk`, `/ag-ui` | The client (for server-side rendering) | The client |

So a page that imports the publisher by mistake gets no publisher code and a clear error, and a file shared by server and client that imports it without using it in the page does no harm. The publisher also refuses to be constructed where there is a `document`, for a resolver that ignores the map. The repository's packaging tests build small applications with esbuild, Vite, webpack, Rollup and Next.js on every change and search the browser output for publisher code.

One consequence: backend code that uses the publisher cannot be tested under a DOM test environment such as jsdom. Run those tests in the Node environment.

## Testing your own code

```ts
import { RecordingPublisher } from '@neuron-core/iq-stream/testing';          // Node only
import { FakeEventSource } from '@neuron-core/iq-stream/client/testing';
```

`RecordingPublisher` has the publisher's surface in memory: it validates frames as the real one does and records them (`frames`, `data`) with the close `outcome`. `FakeEventSource` is a source a test drives by hand (`open()`, `frame()`, `terminal()`, `error()`); pass it as `options.EventSource`. `@neuron-core/iq-stream/testing` also exports `startServer()` and `testServers()`, which start the server in Docker, and `subscribe()`, which reads a stream from Node up to its terminal event.

## Requirements

- Node 20.19 or later for the publisher (the first releases that load ES modules through `require()`, for code that is CommonJS). The export map sends Bun and Deno to the same build, and Cloudflare Workers and edge runtimes to the `fetch` build. The tests run on Node 20, 22 and 24 and in Next.js's Node.js and Edge runtimes; Bun, Deno and Workers are not tested yet.
- An IQ-Stream server 1.x.
- For the bridges: `ai` 5 or later (tested with 7); `@ag-ui/client` 1 or later with `rxjs` 7.

## Limits worth knowing

- A frame is at most 65,536 bytes and a stream at most 10 MiB and 30 minutes, unless the app has limits of its own on the server.
- A connection that stalls without being reset is noticed only when the queue of unsent frames reaches `queueBytes`, or when `close()` times out.
- A stream belongs to the publisher that started it: another process cannot continue it after `abort()`.
