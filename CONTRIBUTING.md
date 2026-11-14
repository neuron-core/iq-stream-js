# Contributing

This repository is an npm workspace. Each package lives in `packages/<name>`, and the scripts in the root `package.json` run the same script in every package that has it.

`packages/iq-stream` is the core. The adapters, `packages/iq-stream-langchain` and `packages/iq-stream-mastra`, are thin layers over its publisher: each has `@neuron-core/iq-stream` as a peer dependency and brings its framework as a peer dependency too.

## Development

You need Node 20.19 or later and Docker. Mastra itself asks for Node 22.13, so use that to work on its adapter.

```sh
npm install
npm run build
npm run test:unit         # no network
npm run test:packaging    # packs the core package and builds small applications against it with four bundlers
```

The commands run in every package of the workspace. `npm run build` builds the core first, which the adapters need: they compile and run against its built output. To work on one package, add `-w packages/<name>`.

The integration and browser tests run against two real IQ-Stream servers, one with default settings on port 8080 and one with short timeouts and small limits on port 8081:

```sh
npm run servers:start     # in Docker, from IQ_STREAM_TEST_IMAGE (default ghcr.io/neuron-core/iq-stream:1)
npm run test:integration  # publisher and client against the servers
npm run test:browser      # the client in Chromium (npx playwright install chromium, once)
npm run servers:stop
```

CI pulls that image with the workflow's own token, so the package on GitHub must give this repository read access (its settings, "Manage Actions access"), or be public.

To use servers that run elsewhere, set `IQ_STREAM_TEST_URL` and `IQ_STREAM_TEST_SHORT_URL`; the short one needs the settings of `SHORT_SETTINGS` in `packages/iq-stream/src/testing/index.ts`.

The Next.js packaging test installs Next.js and builds an application twice, so it runs only on request:

```sh
IQ_STREAM_TEST_NEXT=1 node --test packages/iq-stream/test/packaging/next.test.mjs
```

## Layout of `packages/iq-stream`

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

## Releasing

Each package is released by itself. For `@neuron-core/iq-stream`, `prepublishOnly` builds and runs the unit and packaging tests, so a package whose export map would send a browser to the publisher cannot be published. An adapter's `prepublishOnly` builds the core first, since it compiles against the core's built output, then builds itself and runs its own unit tests. When an adapter needs a newer core, release the core first and raise the adapter's peer range.

Releases go out from GitHub Actions (`.github/workflows/release.yml`) when a tag `<package>@<version>` is pushed:

```sh
npm version patch -w packages/iq-stream   # in a workspace, npm neither commits nor tags
git commit -am "iq-stream 0.1.1"
git tag iq-stream@0.1.1
git push origin main iq-stream@0.1.1
```

The workflow checks the tag against the package's `package.json`, runs the whole of CI, and publishes with provenance. A version with a hyphen (`0.2.0-beta.1`) goes out under the `next` dist-tag. The workflow authenticates by npm's trusted publishing, which can be configured only for a package that already exists. So, once per package:

1. Publish the first version by hand, without provenance, which only CI can generate: `npm publish -w packages/iq-stream --provenance=false`.
2. On npmjs.com, in the package's settings under Trusted publishing, add GitHub Actions: organization `neuron-core`, repository `iq-stream-js`, workflow `release.yml`, environment `npm`.
