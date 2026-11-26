# IQ-Stream for JavaScript

JavaScript and TypeScript packages for the [IQ-Stream server](https://iq-stream.dev).

IQ-Stream relays an AI agent's answer from your backend to the browsers that are waiting for it, over Server-Sent Events. Your backend runs the agent and sends each chunk of the answer to the IQ-Stream server. Browsers subscribe to the stream with a short-lived token and receive the chunks as they arrive. If a page reloads or the connection drops, the browser picks up where it left off. The agent run never depends on the browser connection staying open.

This repository holds the core package, which covers both sides of that flow, and adapters that publish the runs of popular agent frameworks with a few lines of code.

## Packages

| Package | Description | Documentation |
| --- | --- | --- |
| `@neuron-core/iq-stream` | The core: a publisher for backends, a client for browsers, and bridges to the Vercel AI SDK (`useChat`) and AG-UI | [packages/iq-stream](packages/iq-stream/README.md) |
| `@neuron-core/iq-stream-langchain` | Publishes a LangChain JS or LangGraph.js run, as AI SDK UI message chunks or as LangGraph protocol events | [packages/iq-stream-langchain](packages/iq-stream-langchain/README.md) |
| `@neuron-core/iq-stream-mastra` | Publishes a Mastra agent's answer, from a ready-made chat route on Mastra's server or from your own route handlers, as AI SDK or AG-UI events | [packages/iq-stream-mastra](packages/iq-stream-mastra/README.md) |

Each package has its own README in its directory with installation steps, usage examples and the full API reference.

## Which package do I need?

**On the backend**, pick by how your agent is built:

- With **LangChain JS or LangGraph.js**: `@neuron-core/iq-stream-langchain`.
- With **Mastra**: `@neuron-core/iq-stream-mastra`.
- With **any other framework, or your own code**: the publisher of `@neuron-core/iq-stream`, used directly.

The adapters are thin layers over the core's publisher. They list `@neuron-core/iq-stream` as a peer dependency, so install it next to them.

**In the browser**, always use `@neuron-core/iq-stream`. Its client reads any stream, and its bridges plug the stream into `useChat` of the Vercel AI SDK or into AG-UI front ends such as CopilotKit. The adapters ship no browser code.

## Requirements

- Node 20.19 or later. The Mastra adapter needs Node 22.13 or later, as Mastra itself does.
- An IQ-Stream server 1.x.
- All packages ship ES modules only.

## Contributing

To build and test the packages locally or to release one, see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
