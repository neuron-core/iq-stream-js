// The bridges are written once: the same chatTransport reads protocol objects from Neuron envelopes through
// @neuron-core/streaming 0.3.0 as published, whether a Pusher channel or a real IQ-Stream server carried them.
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { test } from 'node:test';
import { createProtocolStream, subscribeToPusher } from '@neuron-core/streaming';
import { AbstractChat } from 'ai';
import { EventSource } from 'eventsource';
import { chatTransport } from '../../../dist/ai-sdk/index.js';
import { newStreamId, publish, subscribeUrl } from '../../browser/support/backend.mjs';
import { subscribeToIq } from './support/neuron-iq.mjs';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

class HeadlessChat extends AbstractChat {              // the class behind useChat, without React
  constructor(init) {
    super({
      ...init,
      state: {
        status: 'ready', error: undefined, messages: [],
        pushMessage(message) { this.messages = [...this.messages, message]; },
        popMessage() { this.messages = this.messages.slice(0, -1); },
        replaceMessage(index, message) { this.messages = [...this.messages.slice(0, index), message, ...this.messages.slice(index + 1)]; },
        snapshot: (value) => structuredClone(value),
      },
    });
  }
}
const describe = (chat) => ({
  status: chat.status,
  error: chat.error?.message ?? null,
  parts: (chat.messages.at(-1)?.parts ?? []).map((part) => (part.type === 'text' ? `text(${part.text})` : part.type)),
});
const COMPLETE = { status: 'ready', error: null, parts: ['step-start', 'text(Ciao da Napoli.)'] };

// Neuron envelopes for one segment, as the PHP channels publish them: {streamId, sequence, type, data}.
const segment = randomBytes(16).toString('hex');
const envelopes = [
  ['start', { messageId: 'm1' }], ['start-step', []], ['text-start', { id: 't1' }], ['text-delta', { id: 't1', delta: 'Ciao ' }],
  ['text-delta', { id: 't1', delta: 'da Napoli.' }], ['text-end', { id: 't1' }], ['finish-step', []], ['finish', { finishReason: 'stop' }],
  ['stream.completed', { workflowId: 'w1' }],
].map(([type, data], sequence) => ({ streamId: segment, sequence, type, data }));
const asFrames = (pace) => [...envelopes.map((envelope) => [pace, JSON.stringify(envelope)]), [0, { close: 'end' }]];

test('D1 envelopes over a Pusher-shaped channel, two swapped in transit -> createProtocolStream -> chatTransport', { timeout: 30_000 }, async () => {
  const channel = { handlers: new Set(), bind_global(handler) { this.handlers.add(handler); }, unbind_global(handler) { this.handlers.delete(handler); } };
  const transport = chatTransport({
    start: async () => {
      void (async () => {
        for (const index of [0, 1, 2, 4, 3, 5, 6, 7, 8]) {             // sequence 4 arrives before 3
          await sleep(20);
          for (const handler of channel.handlers) handler(envelopes[index].type, envelopes[index]);
        }
      })();
      return {};
    },
    open: () => createProtocolStream((callbacks) => subscribeToPusher(channel, callbacks, segment), (event) => event),
  });
  const chat = new HeadlessChat({ transport });
  await chat.sendMessage({ text: 'hi' });
  assert.deepEqual(describe(chat), COMPLETE);
  assert.equal(channel.handlers.size, 0);
});

test('D2 the same envelopes as IQ frames on the real server -> subscribeToIq -> createProtocolStream -> the same chatTransport', { timeout: 30_000 }, async () => {
  let run;
  const transport = chatTransport({
    start: async () => {
      run = publish(newStreamId(), asFrames(20));
      return { url: subscribeUrl(run.streamId) };
    },
    open: (ticket) => createProtocolStream((callbacks) => subscribeToIq(ticket.url, callbacks, segment, { EventSource }), (event) => event),
  });
  const chat = new HeadlessChat({ transport });
  await chat.sendMessage({ text: 'hi' });
  assert.deepEqual(describe(chat), COMPLETE);
  assert.equal((await run.done).body.lastEventId, envelopes.length);
});

test('D3 Neuron over IQ, the subscriber arrives after the close: iq:gone becomes onGap', { timeout: 30_000 }, async () => {
  const run = publish(newStreamId(), asFrames(0));
  await run.done;
  const result = await new Promise((resolve) => {
    subscribeToIq(subscribeUrl(run.streamId), { onEvent: (event) => resolve(`event ${event.type}`), onGap: (reason) => resolve(`onGap(${reason})`) }, undefined, { EventSource });
  });
  assert.equal(result, 'onGap(iq:gone)');
});
