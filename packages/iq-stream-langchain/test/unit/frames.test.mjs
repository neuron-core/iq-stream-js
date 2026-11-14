// The frame limit: oversized deltas are split, oversized payloads are replaced by a marker.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RecordingPublisher } from '@neuron-core/iq-stream/testing';
import { UI_STREAM_OPTIONS, publishUIMessages, uiMessageFrames } from '../../dist/ai-sdk/index.js';
import { buildAgent, input, toolCall, words } from '../fakes/agent.mjs';

const LIMIT = 65_536;
const BIG = 200_000;

/** Pipes chunks through uiMessageFrames and returns the frames and what was reported. */
async function frames(chunks, options = {}) {
  const errors = [];
  const out = [];
  const stream = new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  }).pipeThrough(uiMessageFrames({ onError: (error) => errors.push(error.message), ...options }));
  for await (const frame of stream) out.push(frame);
  return { frames: out, chunks: out.map((frame) => JSON.parse(frame)), errors };
}

test('11 a model that does not stream: 200 kB of text become several text-delta frames, each within the limit', async () => {
  const text = 'The quick brown fox. '.repeat(BIG / 21 + 1);
  const publisher = new RecordingPublisher();
  const errors = [];
  const stream = await buildAgent({ turns: [[{ content: text }]] }).stream(input, UI_STREAM_OPTIONS);
  const result = await publishUIMessages(stream, publisher, { onError: (error) => errors.push(error) });

  const deltas = publisher.data.map((data) => JSON.parse(data)).filter((chunk) => chunk.type === 'text-delta');
  assert.ok(deltas.length >= 4, `${deltas.length} deltas`);
  assert.equal(deltas.map((chunk) => chunk.delta).join(''), text);
  assert.ok(deltas.every((chunk) => chunk.id === 'fake-msg-1'));
  assert.ok(publisher.data.every((data) => publisher.frameSize(data) <= LIMIT));
  assert.deepEqual(errors, []);
  assert.equal(result.outcome, 'end');
});

test('11 a tool that returns 200 kB: tool-output-available with the truncation marker, and one onError call', async () => {
  const output = 'x'.repeat(BIG);
  const publisher = new RecordingPublisher();
  const errors = [];
  let saved;
  const stream = await buildAgent({ tool: () => output }).stream(input, UI_STREAM_OPTIONS);
  const result = await publishUIMessages(stream, publisher, {
    onError: (error) => errors.push(error.message),
    onFinish: (state) => { saved = state.messages.find((message) => message.tool_call_id === 'call_1').content; },
  });

  const chunk = publisher.data.map((data) => JSON.parse(data)).find((candidate) => candidate.type === 'tool-output-available');
  assert.deepEqual(chunk, { type: 'tool-output-available', toolCallId: 'call_1', output: { truncated: true, bytes: BIG + 2 } });
  assert.equal(errors.length, 1);
  assert.match(errors[0], /^A tool-output-available chunk of \d+ bytes exceeds the frame limit: its output was replaced/);
  assert.ok(!errors[0].includes('xxxx'), 'the report carries no frame data');
  // The run was not disturbed, and the whole value is in the state the application saves.
  assert.equal(result.outcome, 'end');
  assert.equal(saved, output);
});

test('tool arguments of 200 kB: the deltas are split, the assembled input is replaced by the marker', async () => {
  const city = 'P'.repeat(BIG);
  const publisher = new RecordingPublisher();
  const errors = [];
  const turns = [toolCall('get_weather', { city }), words('Done.')];
  const result = await publishUIMessages(await buildAgent({ turns, tool: () => 'ok' }).stream(input, UI_STREAM_OPTIONS), publisher, { onError: (error) => errors.push(error.message) });

  const chunks = publisher.data.map((data) => JSON.parse(data));
  assert.equal(chunks.filter((chunk) => chunk.type === 'tool-input-delta').map((chunk) => chunk.inputTextDelta).join(''), JSON.stringify({ city }));
  assert.deepEqual(chunks.find((chunk) => chunk.type === 'tool-input-available').input, { truncated: true, bytes: BIG + 11 });
  assert.equal(errors.length, 1);
  assert.equal(result.outcome, 'end');
});

test('uiMessageFrames: a chunk that fits is one frame, as JSON', async () => {
  const chunks = [{ type: 'start' }, { type: 'text-delta', id: 't1', delta: 'Hello' }, { type: 'finish' }];
  const result = await frames(chunks);
  assert.deepEqual(result.frames, chunks.map((chunk) => JSON.stringify(chunk)));
  assert.deepEqual(result.errors, []);
});

test('uiMessageFrames: deltas are split on code points, whatever the text costs on the wire', async () => {
  const size = (data) => new TextEncoder().encode(JSON.stringify({ data })).length;
  for (const [type, member] of [['text-delta', 'delta'], ['reasoning-delta', 'delta'], ['tool-input-delta', 'inputTextDelta']]) {
    // Quotes and newlines are escaped twice on the wire, and an emoji is two UTF-16 units.
    const text = '😀"quoted"\n€ and more 🇮🇹 '.repeat(300);
    const result = await frames([{ type, id: 't1', toolCallId: 'c1', [member]: text }], { maxFrameBytes: 1024 });
    assert.ok(result.chunks.length > 8, `${result.chunks.length} pieces`);
    assert.equal(result.chunks.map((chunk) => chunk[member]).join(''), text);
    assert.ok(result.frames.every((frame) => size(frame) <= 1024));
    assert.ok(result.chunks.every((chunk) => chunk.type === type && chunk.id === 't1' && !/[\ud800-\udbff]$|^[\udc00-\udfff]/.test(chunk[member])));
    assert.deepEqual(result.errors, []);
  }
});

test('uiMessageFrames: input, output and data are replaced by the marker; what nothing can shrink is dropped', async () => {
  const big = 'y'.repeat(2000);
  const result = await frames([
    { type: 'tool-input-available', toolCallId: 'c1', toolName: 'search', input: { query: big } },
    { type: 'tool-output-available', toolCallId: 'c1', output: [big] },
    { type: 'data-progress', data: { log: big } },
    { type: 'file', url: `data:text/plain,${big}`, mediaType: 'text/plain' },
    { type: 'finish' },
  ], { maxFrameBytes: 1024 });
  assert.deepEqual(result.chunks, [
    { type: 'tool-input-available', toolCallId: 'c1', toolName: 'search', input: { truncated: true, bytes: 2012 } },
    { type: 'tool-output-available', toolCallId: 'c1', output: { truncated: true, bytes: 2004 } },
    { type: 'data-progress', data: { truncated: true, bytes: 2010 } },
    { type: 'finish' },
  ]);
  assert.equal(result.errors.length, 4);
  assert.match(result.errors[3], /^A file chunk of \d+ bytes exceeds the frame limit and was not published\.$/);
});

test('uiMessageFrames: the limit and the measure are the ones given, a publisher\'s for instance', async () => {
  const publisher = new RecordingPublisher({ maxFrameBytes: 300 });
  const text = 'word '.repeat(200);
  const result = await frames([{ type: 'text-delta', id: 't1', delta: text }], {
    maxFrameBytes: publisher.maxFrameBytes,
    frameSize: (data) => publisher.frameSize(data),
  });
  for (const frame of result.frames) publisher.send(frame);   // throws FrameTooLarge for a frame over the limit
  assert.equal(result.chunks.map((chunk) => chunk.delta).join(''), text);
  assert.ok(result.frames.length >= 5);
});

test('publishUIMessages splits against the publisher\'s own limit', async () => {
  const publisher = new RecordingPublisher({ maxFrameBytes: 256 });
  const text = 'A long answer in one piece. '.repeat(40);
  const errors = [];
  await publishUIMessages(await buildAgent({ turns: [[{ content: text }]] }).stream(input, UI_STREAM_OPTIONS), publisher, { onError: (error) => errors.push(error) });
  const deltas = publisher.data.map((data) => JSON.parse(data)).filter((chunk) => chunk.type === 'text-delta');
  assert.ok(deltas.length >= 5);
  assert.equal(deltas.map((chunk) => chunk.delta).join(''), text);
  assert.deepEqual(errors, []);
  assert.equal(publisher.outcome, 'end');
});
