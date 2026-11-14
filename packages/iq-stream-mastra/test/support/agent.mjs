// A Mastra agent on Mastra's own mock model: no API key, no network. Step one streams reasoning and a
// `getWeather` tool call, step two streams the answer word by word.
import { Agent } from '@mastra/core/agent';
import { Mastra } from '@mastra/core/mastra';
import { InMemoryStore } from '@mastra/core/storage';
import { MastraLanguageModelV2Mock, simulateReadableStream } from '@mastra/core/test-utils/llm-mock';
import { createTool } from '@mastra/core/tools';
import { z } from 'zod';

export const ANSWER = 'It is 21°C and sunny in Paris.';
export const QUESTION = 'What is the weather in Paris?';
/** The request body `useChat` sends. */
export const chatBody = (extra = {}) => ({ messages: [{ id: 'u1', role: 'user', parts: [{ type: 'text', text: QUESTION }] }], ...extra });

const usage = { inputTokens: 10, outputTokens: 20, totalTokens: 30 };
const head = (id) => [{ type: 'stream-start', warnings: [] }, { type: 'response-metadata', id, modelId: 'mock-model-id', timestamp: new Date(0) }];
const result = (stream) => ({ rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream });

/**
 * `fail` breaks the answer after three words, as a provider outage does. `tool: false` answers at once.
 * `pace` is the delay between two chunks of the model, in milliseconds. The model keeps no state: it
 * calls the tool until the prompt holds a tool result, so one agent serves any number of runs.
 * `onCall` is told of every call.
 */
export function mockModel({ fail = false, tool = true, pace = 5, onCall } = {}) {
  return new MastraLanguageModelV2Mock({
    doStream: async ({ prompt }) => {
      onCall?.();
      if (tool && !prompt.some((message) => message.role === 'tool')) {
        return result(simulateReadableStream({
          chunkDelayInMs: pace,
          chunks: [
            ...head('resp-1'),
            { type: 'reasoning-start', id: 'r-1' },
            { type: 'reasoning-delta', id: 'r-1', delta: 'I need the weather. ' },
            { type: 'reasoning-end', id: 'r-1' },
            { type: 'tool-input-start', id: 'call-1', toolName: 'getWeather' },
            { type: 'tool-input-delta', id: 'call-1', delta: '{"city":' },
            { type: 'tool-input-delta', id: 'call-1', delta: '"Paris"}' },
            { type: 'tool-input-end', id: 'call-1' },
            { type: 'tool-call', toolCallId: 'call-1', toolName: 'getWeather', input: '{"city":"Paris"}' },
            { type: 'finish', finishReason: 'tool-calls', usage },
          ],
        }));
      }
      const words = ['It ', 'is ', '21°C ', 'and ', 'sunny ', 'in ', 'Paris.'];
      const chunks = [...head('resp-2'), { type: 'text-start', id: 't-1' },
        ...words.slice(0, fail ? 3 : words.length).map((delta) => ({ type: 'text-delta', id: 't-1', delta }))];
      if (!fail) {
        chunks.push({ type: 'text-end', id: 't-1' }, { type: 'finish', finishReason: 'stop', usage });
        return result(simulateReadableStream({ chunkDelayInMs: pace, chunks }));
      }
      const reader = simulateReadableStream({ chunkDelayInMs: pace, chunks }).getReader();
      return result(new ReadableStream({
        async pull(controller) {
          const { done, value } = await reader.read();
          if (done) controller.error(new Error('model exploded mid-stream'));
          else controller.enqueue(value);
        },
      }));
    },
  });
}

/** The tool. `toolDelay` is how long it runs, `output` what it returns. */
export function weatherTool({ toolDelay = 10, output } = {}) {
  return createTool({
    id: 'getWeather',
    description: 'Get the weather for a city',
    inputSchema: z.object({ city: z.string() }),
    execute: async (input) => {
      await new Promise((resolve) => setTimeout(resolve, toolDelay));
      return output ?? { city: input?.city ?? input?.context?.city, tempC: 21, sky: 'sunny' };
    },
  });
}

export function makeAgent({ id = 'weather', memory, ...options } = {}) {
  return new Agent({
    id, name: id,
    instructions: 'SECRET SYSTEM PROMPT: you answer weather questions.',
    model: mockModel(options),
    tools: options.tool === false ? {} : { getWeather: weatherTool(options) },
    ...(memory ? { memory } : {}),
  });
}

/**
 * The agent on a Mastra instance of its own, which gives it storage (a suspended run needs it) and keeps
 * Mastra's console logger quiet. `config` is merged into the instance's, for routes.
 */
export function weatherMastra(options = {}, config = {}) {
  const agent = makeAgent(options);
  return { agent, mastra: new Mastra({ agents: { [agent.id]: agent }, storage: new InMemoryStore(), logger: false, ...config }) };
}

export const weatherAgent = (options) => weatherMastra(options).agent;

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
/** The `type` of every recorded frame. */
export const types = (publisher) => publisher.data.map((data) => JSON.parse(data).type);
