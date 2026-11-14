// The adapter between a real createAgent run (on the scripted model) and the real servers, through the
// real IQClient. What a subscriber receives is read back with the testing kit's subscribe().
import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { IQClient } from '@neuron-core/iq-stream';
import { subscribe, testServers } from '@neuron-core/iq-stream/testing';
import { UI_STREAM_OPTIONS, publishUIMessages } from '../../dist/ai-sdk/index.js';
import { publishProtocolEvents } from '../../dist/protocol/index.js';
import { ANSWER, buildAgent, failingTurns, happyTurns, input, sleep, toolCall, words } from '../fakes/agent.mjs';

let servers;
const errors = [];
before(async () => { servers = await testServers(); });
after(() => servers?.stop());

/** A client of one of the servers. The short server takes lines of 1024 bytes, which the client must be told. */
const client = (server, options = {}) => new IQClient({
  url: server.url, appKey: servers.appKey, appSecret: servers.appSecret, minOpen: 0,
  errorHandler: (error) => errors.push(error),
  ...(server === servers.short ? { maxFrameBytes: 1024 } : {}),
  ...options,
});

/** A new stream with a subscriber already waiting for it, and its publisher. */
async function open(iq, server) {
  const streamId = iq.newStreamId();
  const subscription = subscribe(server.url, streamId, await iq.token('sub', streamId));
  await sleep(150);
  return { subscription, publisher: iq.publisher(streamId) };
}
const chunksOf = (received) => received.frames.map((frame) => JSON.parse(frame.data));
const idsOf = (received) => received.frames.map((frame) => Number(frame.id));
const inOrder = (count) => Array.from({ length: count }, (_, index) => index + 1);

test('happy path: ids 1..n in order, the tool chunks, iq:end, and one publish request', async () => {
  errors.splice(0);
  const iq = client(servers.default);
  const { subscription, publisher } = await open(iq, servers.default);
  const stream = await buildAgent().stream(input, UI_STREAM_OPTIONS);
  const result = await publishUIMessages(stream, publisher);

  const received = await subscription;
  const chunks = chunksOf(received);
  assert.equal(chunks.length, 29);
  assert.deepEqual(idsOf(received), inOrder(29));
  assert.ok(received.frames.every((frame) => frame.event === 'message'), 'unnamed frames: one onmessage handler reads them all');
  assert.deepEqual([chunks[0].type, chunks.at(-1).type], ['start', 'finish']);
  assert.deepEqual(chunks.find((chunk) => chunk.type === 'tool-input-available').input, { city: 'Paris' });
  assert.equal(chunks.find((chunk) => chunk.type === 'tool-output-available').output, 'Sunny, 21 degrees in Paris');
  assert.equal(chunks.filter((chunk) => chunk.type === 'text-delta' && chunk.id === 'fake-msg-2').map((chunk) => chunk.delta).join(''), ANSWER);
  assert.deepEqual(received.terminal, { event: 'iq:end', data: { lastEventId: 29 } });
  assert.deepEqual(result, { outcome: 'end', frames: 29 });
  assert.equal(publisher.requests, 1, 'one request for the whole answer');
  assert.deepEqual(errors, []);
});

test('failure path: an error chunk, then iq:fail with reason publisher', async () => {
  errors.splice(0);
  const iq = client(servers.default);
  const { subscription, publisher } = await open(iq, servers.default);
  const stream = await buildAgent({ turns: failingTurns() }).stream(input, UI_STREAM_OPTIONS);
  const result = await publishUIMessages(stream, publisher);

  const received = await subscription;
  const chunks = chunksOf(received);
  assert.deepEqual(chunks.at(-1), { type: 'error', errorText: 'An error occurred.' });
  assert.deepEqual(idsOf(received), inOrder(chunks.length));
  assert.deepEqual(received.terminal, { event: 'iq:fail', data: { reason: 'publisher', lastEventId: chunks.length } });
  assert.deepEqual([result.outcome, result.error.message], ['fail', 'fake provider outage']);
  assert.equal(publisher.requests, 1);
  assert.deepEqual(errors, [], 'a failed run is not a publishing error');
});

test('a tool that outlasts the server\'s read timeout: the core\'s keepalive holds the one request', async () => {
  errors.splice(0);
  const iq = client(servers.short, { keepalive: 0.5 });
  const { subscription, publisher } = await open(iq, servers.short);
  // The short server answers 408 after 2 s without a byte. The tool is silent for 3.2 s.
  const stream = await buildAgent({ toolDelayMs: 3200 }).stream(input, UI_STREAM_OPTIONS);
  const result = await publishUIMessages(stream, publisher);

  const received = await subscription;
  assert.deepEqual(idsOf(received), inOrder(29));
  assert.deepEqual(received.terminal, { event: 'iq:end', data: { lastEventId: 29 } });
  const at = (type) => received.frames.find((frame) => JSON.parse(frame.data).type === type).at;
  const silence = at('tool-output-available') - at('tool-input-available');
  assert.ok(silence >= 3000, `the tool was silent for ${silence} ms`);
  assert.equal(result.outcome, 'end');
  assert.equal(publisher.requests, 1, 'no 408, no second request');
  assert.deepEqual(errors, []);
});

test('the adapter does not disturb a resume: after a 408 the core reopens, and every frame arrives once, in order', async () => {
  errors.splice(0);
  const iq = client(servers.short, { keepalive: null });
  const { subscription, publisher } = await open(iq, servers.short);
  const stream = await buildAgent({ toolDelayMs: 3000 }).stream(input, UI_STREAM_OPTIONS);
  const result = await publishUIMessages(stream, publisher);

  const received = await subscription;
  assert.deepEqual(idsOf(received), inOrder(29));
  assert.deepEqual(chunksOf(received).map((chunk) => chunk.type).slice(-3), ['text-end', 'finish-step', 'finish']);
  assert.deepEqual(received.terminal, { event: 'iq:end', data: { lastEventId: 29 } });
  assert.deepEqual(result, { outcome: 'end', frames: 29 });
  assert.ok(publisher.requests > 1, `${publisher.requests} requests`);
  assert.deepEqual(errors, []);
});

test('persist before close: what onFinish saved is there when a subscriber sees iq:end', async () => {
  errors.splice(0);
  const iq = client(servers.default);
  const { subscription, publisher } = await open(iq, servers.default);
  const store = new Map();
  let savedAt;
  const started = performance.now();
  const stream = await buildAgent().stream(input, UI_STREAM_OPTIONS);
  const publishing = publishUIMessages(stream, publisher, {
    onFinish: async (state) => {
      await sleep(400);   // a slow database
      store.set(publisher.streamId, state.messages.at(-1).content);
      savedAt = performance.now() - started;
    },
  });

  // What a page does on iq:end: it takes the answer for final. A late tab loads it from the store.
  const received = await subscription;
  const seenAt = performance.now() - started;
  assert.equal(received.terminal.event, 'iq:end');
  assert.equal(store.get(publisher.streamId), ANSWER);
  assert.ok(savedAt <= seenAt, `saved at ${savedAt} ms, iq:end seen at ${seenAt} ms`);
  // The page had the finish chunk while the answer was being saved: only the close line waits.
  const finish = received.frames.at(-1);
  assert.equal(JSON.parse(finish.data).type, 'finish');
  assert.equal((await publishing).outcome, 'end');
  assert.deepEqual(errors, []);
});

test('the server is unreachable: the run completes, onFinish runs, and the promise resolves', async () => {
  errors.splice(0);
  const iq = client({ url: 'http://127.0.0.1:9' }, { retryBudget: 0.5 });
  const publisher = iq.publisher(iq.newStreamId());
  let saved;
  const stream = await buildAgent().stream(input, UI_STREAM_OPTIONS);
  const result = await publishUIMessages(stream, publisher, { onFinish: (state) => { saved = state.messages.at(-1).content; } });

  assert.equal(saved, ANSWER);
  assert.equal(result.outcome, 'end');
  assert.equal(result.error, undefined);
  assert.equal(publisher.alive, false);
  // The core reported it, once; the adapter added nothing.
  assert.equal(errors.length, 1);
  assert.equal(errors[0], publisher.error);
});

test('the server closes the stream for its size: the adapter keeps reading the run, and onFinish runs', async () => {
  errors.splice(0);
  const iq = client(servers.short);
  const { subscription, publisher } = await open(iq, servers.short);
  // About 170 frames of some 100 bytes: twice what the short server lets a stream hold (8192 bytes).
  const answer = 'word '.repeat(160).trim();
  let saved;
  const reported = [];
  const stream = await buildAgent({ turns: [words(answer)], delayMs: 5 }).stream(input, UI_STREAM_OPTIONS);
  const result = await publishUIMessages(stream, publisher, {
    onFinish: (state) => { saved = state.messages.at(-1).content; },
    onError: (error) => reported.push(error),
  });

  const received = await subscription;
  assert.equal(received.terminal.event, 'iq:fail');
  assert.equal(received.terminal.data.reason, 'too_large');
  assert.ok(received.frames.length > 20 && received.frames.length < 160, `${received.frames.length} frames`);
  assert.deepEqual(idsOf(received), inOrder(received.frames.length));
  // The run was read to its end and its answer saved, whole.
  assert.equal(saved, answer);
  assert.equal(result.error, undefined);
  assert.ok(result.frames < 165, `${result.frames} frames accepted`);
  assert.equal(publisher.alive, false);
  assert.equal(publisher.error.code, 'stream_too_large');
  assert.deepEqual(errors, [publisher.error]);
  assert.deepEqual(reported, [], 'a publishing problem is the core\'s to report');
});

test('a tool result over the app\'s frame limit is truncated before the core would refuse the line', async () => {
  errors.splice(0);
  const iq = client(servers.short);
  const { subscription, publisher } = await open(iq, servers.short);
  const reported = [];
  const turns = [toolCall('get_weather', { city: 'Paris' }), words('Done.')];
  const stream = await buildAgent({ turns, tool: () => 'z'.repeat(3000) }).stream(input, UI_STREAM_OPTIONS);
  const result = await publishUIMessages(stream, publisher, { onError: (error) => reported.push(error.message) });

  const received = await subscription;
  assert.deepEqual(chunksOf(received).find((chunk) => chunk.type === 'tool-output-available').output, { truncated: true, bytes: 3002 });
  assert.equal(received.terminal.event, 'iq:end');
  assert.equal(result.outcome, 'end');
  assert.equal(reported.length, 1);
  assert.deepEqual(errors, []);
});

test('protocol events end to end: every event in order, then iq:end', async () => {
  errors.splice(0);
  const iq = client(servers.default);
  const { subscription, publisher } = await open(iq, servers.default);
  const run = await buildAgent({ turns: happyTurns(1) }).streamEvents(input, { version: 'v3' });
  const result = await publishProtocolEvents(run, publisher);

  const received = await subscription;
  const events = chunksOf(received);
  assert.equal(events.length, result.frames);
  assert.deepEqual(events.map((event) => event.seq), events.map((_, index) => index));
  assert.deepEqual(idsOf(received), inOrder(events.length));
  assert.deepEqual([events.at(-1).method, events.at(-1).params.data.event], ['lifecycle', 'completed']);
  assert.deepEqual(received.terminal, { event: 'iq:end', data: { lastEventId: events.length } });
  assert.equal(publisher.requests, 1);
  assert.deepEqual(errors, []);
});
