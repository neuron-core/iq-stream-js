// A LangChain v1 agent (`createAgent`) driven by a scripted model: no API key, no network.
import { AIMessageChunk } from '@langchain/core/messages';
import { ChatGenerationChunk } from '@langchain/core/outputs';
import { FakeStreamingChatModel } from '@langchain/core/utils/testing';
import { createAgent, tool } from 'langchain';
import { z } from 'zod';

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * The framework's own FakeStreamingChatModel replays one chunk list on every call, which would loop an
 * agent forever. This one plays a chunk list per model turn and yields `tool_call_chunks`, so that tool
 * arguments stream as they do from a provider. A turn that is an Error is thrown. A turn of one chunk
 * is what a model that does not stream sends: the whole message at once.
 */
export class ScriptedChatModel extends FakeStreamingChatModel {
  constructor({ turns, delayMs = 2 }) {
    super({ sleep: 0 });
    this.turns = turns;
    this.delayMs = delayMs;
    this.turn = 0;
  }

  bindTools() {
    return this;
  }

  async _generate(messages, options, runManager) {
    let final;
    for await (const chunk of this._streamResponseChunks(messages, options, runManager)) final = final ? final.concat(chunk) : chunk;
    return { generations: [{ text: final.text, message: final.message }] };
  }

  async *_streamResponseChunks(_messages, options, runManager) {
    const script = this.turns[this.turn++];
    if (script instanceof Error) throw script;
    const id = `fake-msg-${this.turn}`;
    for (const fields of script) {
      await sleep(this.delayMs);
      if (options.signal?.aborted) return;
      const chunk = new ChatGenerationChunk({
        message: new AIMessageChunk({ id, ...fields }),
        text: typeof fields.content === 'string' ? fields.content : '',
      });
      yield chunk;
      await runManager?.handleLLMNewToken(chunk.text, undefined, undefined, undefined, undefined, { chunk });
    }
  }
}

export const words = (text) => text.split(/(?<= )/).map((content) => ({ content }));

/** One streamed call of a tool, as three chunks: the name, then the arguments in two halves. */
export function toolCall(name, args, { id = 'call_1', index = 0 } = {}) {
  const json = JSON.stringify(args);
  const half = json.length >> 1;
  return [
    { content: '', tool_call_chunks: [{ index, id, name, args: '' }] },
    { content: '', tool_call_chunks: [{ index, args: json.slice(0, half) }] },
    { content: '', tool_call_chunks: [{ index, args: json.slice(half) }] },
  ];
}

export const ANSWER = 'It is sunny in Paris, 21 degrees.';

/**
 * Turn 1: a little text, then a streamed tool call. Turn 2: the answer. `toolIndex` is the `index` of
 * the tool call chunk: 0 is what OpenAI-style providers send, and 1 is what the streamEvents v3 path
 * needs from this model (with 0, @langchain/core 1.4.1 merges the call into the text block).
 */
export const happyTurns = (toolIndex = 0) => [
  [...words('Let me check the weather. '), ...toolCall('get_weather', { city: 'Paris' }, { index: toolIndex })],
  words(ANSWER),
];

/** Turn 1 as above; on turn 2 the provider fails. */
export const failingTurns = (toolIndex = 0) => [happyTurns(toolIndex)[0], new Error('fake provider outage')];

/** The weather tool. `run` replaces what it does: it is given the arguments and the tool's config. */
export function weatherTool({ delayMs = 20, run } = {}) {
  return tool(
    async (args, config) => {
      if (run) return run(args, config);
      await sleep(delayMs); // the publisher is silent meanwhile
      return `Sunny, 21 degrees in ${args.city}`;
    },
    { name: 'get_weather', description: 'Get the weather for a city', schema: z.object({ city: z.string() }) },
  );
}

/** A `createAgent` agent on the scripted model with the weather tool. Other `createAgent` options pass through. */
export function buildAgent({ turns = happyTurns(), delayMs, toolDelayMs, tool: run, tools, ...rest } = {}) {
  return createAgent({
    model: new ScriptedChatModel({ turns, delayMs }),
    tools: tools ?? [weatherTool({ delayMs: toolDelayMs, run })],
    ...rest,
  });
}

export const input = { messages: [{ role: 'user', content: 'What is the weather in Paris?' }] };
