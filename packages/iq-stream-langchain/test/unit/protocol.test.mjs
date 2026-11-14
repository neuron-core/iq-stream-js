// publishProtocolEvents over streamEvents(..., { version: 'v3' }) of a real createAgent run.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RecordingPublisher } from '@neuron-core/iq-stream/testing';
import { createMiddleware } from 'langchain';
import { publishProtocolEvents } from '../../dist/protocol/index.js';
import { ANSWER, buildAgent, failingTurns, happyTurns, input, sleep } from '../fakes/agent.mjs';

// The tool call chunk has index 1 here: with 0, @langchain/core 1.4.1 merges it into the text block on this path.
async function run(agentOptions = {}, { stream = {}, publisher = new RecordingPublisher(), ...options } = {}) {
  const events = await buildAgent({ turns: happyTurns(1), ...agentOptions }).streamEvents(input, { version: 'v3', ...stream });
  const result = await publishProtocolEvents(events, publisher, options);
  const published = publisher.data.map((data) => JSON.parse(data));
  return { publisher, result, events: published, methods: new Set(published.map((event) => event.method)) };
}
const OUTAGE = 'fake provider outage';

test('15 every event is one frame, in order: seq 0..n-1, with lifecycle, messages and tools among the channels', async () => {
  const { publisher, result, events, methods } = await run();
  assert.ok(events.length > 30, `${events.length} events`);
  assert.deepEqual(events.map((event) => event.seq), events.map((_, index) => index));
  assert.ok(events.every((event) => event.type === 'event'));
  for (const method of ['lifecycle', 'messages', 'tools', 'values']) assert.ok(methods.has(method), method);
  assert.equal(events.at(-1).method, 'lifecycle');
  assert.equal(events.at(-1).params.data.event, 'completed');
  const text = events.filter((event) => event.method === 'messages' && event.params.data.event === 'content-block-delta' && event.params.data.delta?.type === 'text-delta');
  assert.ok(text.map((event) => event.params.data.delta.text).join('').endsWith(ANSWER));
  assert.equal(publisher.outcome, 'end');
  assert.deepEqual(result, { outcome: 'end', frames: events.length });
  assert.ok(publisher.frames.every((frame) => frame.event === undefined));
});

test('15 a failing run ends with a lifecycle failed event and close(fail); its message is masked', async () => {
  const seen = [];
  const publisher = new RecordingPublisher();
  const { result, events } = await run({ turns: failingTurns(1) }, { publisher, beforeClose: (outcome, error) => void seen.push([outcome, error?.message, publisher.outcome]) });
  assert.equal(events.at(-1).method, 'lifecycle');
  assert.deepEqual([events.at(-1).params.data.event, events.at(-1).params.data.error], ['failed', 'An error occurred.']);
  assert.equal(publisher.outcome, 'fail');
  assert.deepEqual([result.outcome, result.error.message], ['fail', OUTAGE]);
  assert.deepEqual(seen, [['fail', OUTAGE, undefined]]);
  assert.ok(!publisher.data.some((data) => data.includes(OUTAGE)));

  const exposed = await run({ turns: failingTurns(1) }, { exposeErrors: true });
  assert.equal(exposed.events.at(-1).params.data.error, OUTAGE);
});

test('a tool call that fails the run: no event carries its message', async () => {
  const fatalTool = createMiddleware({ name: 'fatal', wrapToolCall: () => { throw new Error('tool exploded'); } });
  const { publisher, result } = await run({ middleware: [fatalTool] });
  assert.equal(publisher.outcome, 'fail');
  assert.equal(result.error.message, 'tool exploded');
  assert.ok(!publisher.data.some((data) => data.includes('tool exploded')));
});

test('15 channels: only the channels asked for are published', async () => {
  const { publisher, methods, events } = await run({}, { channels: ['messages', 'tools', 'lifecycle'] });
  assert.deepEqual([...methods].sort(), ['lifecycle', 'messages', 'tools']);
  // seq is the run's own numbering: a filtered stream keeps it, with gaps.
  const seqs = events.map((event) => event.seq);
  assert.deepEqual(seqs, [...seqs].sort((a, b) => a - b));
  assert.ok(seqs.at(-1) >= events.length);
  assert.equal(publisher.outcome, 'end');
});

test('an event over the frame limit is not published, and is reported', async () => {
  const errors = [];
  const { publisher, events, result } = await run({}, { publisher: new RecordingPublisher({ maxFrameBytes: 700 }), onError: (error) => errors.push(error.message) });
  assert.ok(errors.length > 0);
  assert.ok(errors.every((message) => /^The \w+ event with seq \d+ is \d+ bytes, over the frame limit, and was not published\.$/.test(message)), errors[0]);
  assert.equal(events.length + errors.length, events.at(-1).seq + 1);
  assert.ok(publisher.data.every((data) => publisher.frameSize(data) <= 700));
  // The small events still tell the run's story to its end.
  assert.equal(events.at(-1).params.data.event, 'completed');
  assert.deepEqual([publisher.outcome, result.outcome], ['end', 'end']);
});

test('abort through the signal: close(fail), or abortOutcome', async () => {
  for (const [abortOutcome, expected] of [[undefined, 'fail'], ['end', 'end']]) {
    const controller = new AbortController();
    const { publisher, result } = await run(
      { tool: async () => { setTimeout(() => controller.abort(), 10); await sleep(150); return 'too late'; } },
      { stream: { signal: controller.signal }, ...(abortOutcome ? { abortOutcome } : {}) },
    );
    assert.equal(publisher.outcome, expected);
    assert.deepEqual([result.outcome, result.error.name], [expected, 'AbortError']);
  }
});
