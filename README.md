# iq-stream-js

JavaScript packages for the [IQ-Stream server](https://github.com/neuron-core/iq-stream), which relays an agent's answer from a backend to browsers over Server-Sent Events.

| Package | |
| --- | --- |
| [`@neuron-core/iq-stream`](packages/iq-stream/README.md) | The publisher for backends, the client for browsers, and bridges to the Vercel AI SDK and AG-UI, in one package |

Adapters for agent frameworks (LangChain, Mastra) will be added as further packages of this workspace. Each depends on `@neuron-core/iq-stream` and brings its framework as a peer dependency.

## Development

Node 22 and Docker.

```sh
npm install
npm run build
npm run test:unit         # no network
npm run test:packaging    # packs the package and builds small applications against it with four bundlers
```

The integration and browser tests run against two real IQ-Stream servers, one with default settings on port 8080 and one with short timeouts and small limits on port 8081:

```sh
npm run servers:start     # in Docker, from IQ_STREAM_TEST_IMAGE (default ghcr.io/neuron-core/iq-stream:1)
npm run test:integration  # publisher and client against the servers
npm run test:browser      # the client in Chromium (npx playwright install chromium, once)
npm run servers:stop
```

To use servers that run elsewhere, set `IQ_STREAM_TEST_URL` and `IQ_STREAM_TEST_SHORT_URL`; the short one needs the settings of `SHORT_SETTINGS` in `packages/iq-stream/src/testing/index.ts`.

The Next.js packaging test installs Next.js and builds an application twice, so it runs only on request:

```sh
IQ_STREAM_TEST_NEXT=1 node --test packages/iq-stream/test/packaging/next.test.mjs
```

### Layout of `packages/iq-stream`

```
src/
  publisher/   the backend publisher: node.ts and fetch.ts are its two builds, writer.ts is the state machine both share
  client/      subscribe, frames: the transport rules of a subscriber
  fetch/       an EventSource-like source over fetch()
  ai-sdk/      a ChatTransport for useChat
  ag-ui/       an AbstractAgent for AG-UI
  testing/     RecordingPublisher and the server fixture (Node only)
  shared/      protocol constants and errors; imports nothing
  browser.ts   what the package root resolves to in a browser build
test/
  unit/        node --test, no network
  packaging/   the export map, checked with real bundlers against the packed tarball
  integration/ against the servers
  browser/     Playwright
```

Three rules keep the two sides of the package apart, and `test/unit/boundaries.test.mjs` enforces them: nothing outside `publisher/` and `testing/` imports the publisher; `node:` modules are imported only by the Node transport and the testing kit; `shared/` and `browser.ts` import nothing. The packaging tests check the same from the outside, on the tarball npm would publish.

### Releasing

A release publishes `@neuron-core/iq-stream` from `packages/iq-stream`. `prepublishOnly` builds and runs the unit and packaging tests, so a package whose export map would send a browser to the publisher cannot be published.

## License

MIT
