// A Mastra agent published to the real servers with the real IQClient, and read back as a browser reads.
import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { MockMemory } from '@mastra/core/memory';
import { IQError } from '@neuron-core/iq-stream';
import { subscribe } from '@neuron-core/iq-stream/testing';
import { MastraAgent } from '@ag-ui/mastra';
import { publishAgUi, publishToIQ } from '../../dist/index.js';
import { ANSWER, QUESTION, weatherAgent } from '../support/agent.mjs';
import { countingProxy, delivered, oneTo, setup, sleep } from './support.mjs';

const TEXT = ['text-start', ...Array(7).fill('text-delta'), 'text-end'];
const TOOL = ['tool-input-start', 'tool-input-delta', 'tool-input-delta', 'tool-input-available', 'tool-output-available'];
const RUN = ['start', 'start-step', ...TOOL, 'finish-step', 'start-step', ...TEXT, 'finish-step', 'finish'];

let env;
before(async () => { env = await setup(); });
after(async () => { await env?.servers.stop(); });

/** Subscribes to a stream of its own before anything is published to it. */
async function listening(iq, server = 'default') {
  const streamId = iq.newStreamId();
  const subscription = subscribe(env.servers[server].url, streamId, await iq.token('sub', streamId));
  await sleep(150);
  return { streamId, subscription };
}

test('happy path, subscriber first: every chunk as one frame, in order, on one request, then iq:end', { timeout: 30_000 }, async () => {
  const proxy = await countingProxy(env.servers.default.url);
  try {
    const iq = env.client(proxy.url);
    const { streamId, subscription } = await listening(iq);
    const run = await publishToIQ(await weatherAgent().stream(QUESTION), { iq, streamId, version: 'v6' });
    const result = await run.done;
    const got = delivered(await subscription);

    assert.deepEqual(got.types, RUN);
    assert.deepEqual(got.ids, oneTo(20));
    assert.deepEqual(got.events, ['message'], 'frames are unnamed: EventSource.onmessage receives them');
    assert.deepEqual(got.chunks.find((chunk) => chunk.type === 'tool-output-available').output, { city: 'Paris', tempC: 21, sky: 'sunny' });
    assert.equal(got.chunks.filter((chunk) => chunk.type === 'text-delta').map((chunk) => chunk.delta).join(''), ANSWER);
    assert.deepEqual(got.terminal, { event: 'iq:end', data: { lastEventId: 20 } });
    assert.deepEqual(result, {
      outcome: 'end', suspended: false, framesSent: 20, framesOmitted: 0,
      publish: { status: 200, closed: true, lastEventId: 20, outcome: 'end' },
    });
    assert.equal(proxy.connections, 1);
    assert.deepEqual(env.errors, []);
  } finally {
    await proxy.close();
  }
});

test('failure path: the error frame, then iq:fail with reason publisher', { timeout: 30_000 }, async () => {
  const iq = env.client();
  const { streamId, subscription } = await listening(iq);
  const run = await publishToIQ(await weatherAgent({ fail: true }).stream(QUESTION), { iq, streamId, version: 'v6' });
  const result = await run.done;
  const got = delivered(await subscription);

  assert.deepEqual(got.types.slice(-6), ['text-delta', 'text-delta', 'text-delta', 'error', 'finish-step', 'finish']);
  assert.deepEqual(got.chunks.find((chunk) => chunk.type === 'error'), { type: 'error', errorText: 'An error occurred.' });
  assert.deepEqual(got.terminal, { event: 'iq:fail', data: { reason: 'publisher', lastEventId: got.ids.length } });
  assert.deepEqual([result.outcome, result.cause, result.publish.outcome, result.publish.reason], ['fail', 'error', 'fail', 'publisher']);
  assert.deepEqual(env.errors, []);
});

test('a subscriber that arrives mid-run gets the stream from its first frame', { timeout: 30_000 }, async () => {
  const iq = env.client();
  // About two and a half seconds of run; the subscriber comes one second in.
  const run = await publishToIQ(await weatherAgent({ pace: 100 }).stream(QUESTION), { iq, version: 'v6' });
  await sleep(1000);
  const subscription = subscribe(env.servers.default.url, run.ticket.streamId, run.ticket.token);
  const [got, result] = [delivered(await subscription), await run.done];
  assert.deepEqual(got.types, RUN);
  assert.deepEqual(got.ids, oneTo(20));
  assert.deepEqual(got.terminal, { event: 'iq:end', data: { lastEventId: 20 } });
  assert.equal(result.outcome, 'end');
  // It was late: the first frames came at once, as a replay, and the last ones live.
  const frames = (await subscription).frames;
  assert.ok(frames[4].at - frames[0].at < 100 && frames.at(-1).at - frames[0].at > 500, `${frames[4].at} ${frames.at(-1).at}`);
});

test('a tool that outlasts the server\'s read timeout: one request, no gap, iq:end', { timeout: 30_000 }, async () => {
  const proxy = await countingProxy(env.servers.short.url);
  try {
    // The short server lets a silent request go after 2 seconds; the tool takes 3. The core's keepalives carry it.
    const iq = env.client(proxy.url, { keepalive: 0.5 });
    const { streamId, subscription } = await listening(iq, 'short');
    const run = await publishToIQ(await weatherAgent({ toolDelay: 3000 }).stream(QUESTION), { iq, streamId, version: 'v6' });
    const result = await run.done;
    const got = delivered(await subscription);
    const frames = (await subscription).frames;
    const toolGap = frames[6].at - frames[5].at;

    assert.deepEqual(got.types, RUN);
    assert.deepEqual(got.ids, oneTo(20));
    assert.ok(toolGap >= 2900, `the tool kept the stream silent for ${toolGap} ms`);
    assert.deepEqual(got.terminal, { event: 'iq:end', data: { lastEventId: 20 } });
    assert.deepEqual([result.outcome, result.publish.status, result.framesSent], ['end', 200, 20]);
    assert.equal(proxy.connections, 1, 'no 408, no second request');
    assert.deepEqual(env.errors, []);
  } finally {
    await proxy.close();
  }
});

test('a tool result over the server\'s frame limit: the placeholder arrives, the stream ends with iq:end', { timeout: 30_000 }, async () => {
  // The short server takes lines of 1,024 bytes; the client is told so, as an application with its own limit would.
  const iq = env.client('short', { maxFrameBytes: 1024 });
  const { streamId, subscription } = await listening(iq, 'short');
  const output = await weatherAgent({ output: { forecast: 'sunny '.repeat(300) } }).stream(QUESTION);
  const run = await publishToIQ(output, { iq, streamId, version: 'v6' });
  const result = await run.done;
  const got = delivered(await subscription);

  assert.deepEqual(got.types, [...RUN.slice(0, 6), 'data-iq-omitted', ...RUN.slice(7)]);
  const placeholder = got.chunks[6];
  assert.deepEqual({ ...placeholder, data: { ...placeholder.data, bytes: 0 } },
    { type: 'data-iq-omitted', transient: true, data: { type: 'tool-output-available', toolCallId: 'call-1', bytes: 0 } });
  assert.ok(placeholder.data.bytes > 1024);
  assert.deepEqual(got.ids, oneTo(20));
  assert.deepEqual(got.terminal, { event: 'iq:end', data: { lastEventId: 20 } });
  assert.deepEqual([result.outcome, result.framesSent, result.framesOmitted], ['end', 20, 1]);
  assert.deepEqual(env.errors, []);
});

test('the server is unreachable: the run completes, memory is written, done resolves and says so', { timeout: 30_000 }, async () => {
  const errors = [];
  // Nothing listens on port 9. The core retries for retryBudget seconds, then gives up.
  const iq = env.client('http://127.0.0.1:9', { retryBudget: 0.5, errorHandler: (error) => errors.push(error) });
  const memory = new MockMemory();
  const output = await weatherAgent({ memory }).stream(QUESTION, { memory: { thread: 'thread-1', resource: 'user-1' } });
  const run = await publishToIQ(output, { iq, version: 'v6' });
  assert.equal(typeof run.ticket.token, 'string', 'the ticket needs no server');
  const result = await run.done;

  assert.equal(output.status, 'success');
  assert.equal(await output.text, ANSWER);
  const { messages } = await memory.recall({ threadId: 'thread-1', resourceId: 'user-1' });
  assert.deepEqual(messages.map((message) => message.role), ['user', 'assistant']);
  assert.equal(result.outcome, 'fail');
  assert.equal(result.cause, undefined, 'the run itself did not fail');
  assert.ok(result.error instanceof IQError);
  assert.deepEqual(errors, [result.error], 'reported once, to the client\'s errorHandler');
  assert.deepEqual(result.publish, { status: 0, closed: false, error: 'unreachable' });
});

test('AG-UI events of @ag-ui/mastra arrive as frames, then iq:end', { timeout: 30_000 }, async () => {
  const iq = env.client();
  const { streamId, subscription } = await listening(iq);
  const agent = new MastraAgent({ agentId: 'weather', agent: weatherAgent(), resourceId: 'user-1' });
  const input = { threadId: 'thread-1', runId: 'run-1', messages: [{ id: 'u1', role: 'user', content: QUESTION }], tools: [], context: [], state: {}, forwardedProps: {} };
  const run = await publishAgUi(agent.run(input), { iq, streamId });
  const result = await run.done;
  const got = delivered(await subscription);
  assert.deepEqual([got.types[0], got.types.at(-1)], ['RUN_STARTED', 'RUN_FINISHED']);
  assert.ok(got.types.includes('TOOL_CALL_RESULT'));
  assert.equal(got.chunks.filter((event) => event.type === 'TEXT_MESSAGE_CHUNK').map((event) => event.delta).join(''), ANSWER);
  assert.deepEqual(got.terminal, { event: 'iq:end', data: { lastEventId: got.ids.length } });
  assert.equal(result.outcome, 'end');
});
