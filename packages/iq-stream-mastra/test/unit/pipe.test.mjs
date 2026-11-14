// pipeToIQ on hand-made sources: the close decision, the oversize policies, and that nothing a
// publisher or a callback does reaches the source.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { IQError } from '@neuron-core/iq-stream';
import { RecordingPublisher } from '@neuron-core/iq-stream/testing';
import { classifyMastraChunk, classifyUIChunk, pipeToIQ } from '../../dist/index.js';

const streamOf = (chunks) => new ReadableStream({
  start(controller) {
    for (const chunk of chunks) controller.enqueue(chunk);
    controller.close();
  },
});
async function* generate(chunks) {
  for (const chunk of chunks) yield chunk;
}
const text = (delta) => ({ type: 'text-delta', id: 't', delta });
const sent = (publisher) => publisher.data.map((data) => JSON.parse(data));

test('a stream that ends closes with end: one frame per chunk, in order', async () => {
  const publisher = new RecordingPublisher();
  const chunks = [{ type: 'start' }, text('a'), text('b'), { type: 'finish' }];
  const result = await pipeToIQ(streamOf(chunks), publisher, { classify: classifyUIChunk });
  assert.deepEqual(sent(publisher), chunks);
  assert.ok(publisher.frames.every((frame) => frame.event === undefined), 'frames are unnamed');
  assert.equal(publisher.outcome, 'end');
  assert.deepEqual(result, {
    outcome: 'end', suspended: false, framesSent: 4, framesOmitted: 0,
    publish: { status: 200, lastEventId: 4, closed: true, outcome: 'end' },
  });
});

test('an async iterable is read like a stream, and serialize decides the frame', async () => {
  const publisher = new RecordingPublisher();
  const result = await pipeToIQ(generate(['a', 'b']), publisher, { serialize: (chunk) => `<${chunk}>` });
  assert.deepEqual(publisher.data, ['<a>', '<b>']);
  assert.equal(result.outcome, 'end');
});

test('an error chunk closes with fail, although the stream ends normally', async () => {
  const publisher = new RecordingPublisher();
  const result = await pipeToIQ(streamOf([text('a'), { type: 'error', errorText: 'x' }, { type: 'finish' }]), publisher, { classify: classifyUIChunk });
  assert.equal(publisher.framesSent, 3);
  assert.equal(publisher.outcome, 'fail');
  assert.deepEqual([result.outcome, result.cause, result.publish.reason], ['fail', 'error', 'publisher']);
});

test('an abort chunk closes with fail, or with end when closeOnCancel says so', async () => {
  const chunks = [text('a'), { type: 'abort' }, { type: 'finish' }];
  const publisher = new RecordingPublisher();
  const result = await pipeToIQ(streamOf(chunks), publisher, { classify: classifyUIChunk });
  assert.deepEqual([publisher.outcome, result.outcome, result.cause], ['fail', 'fail', 'abort']);

  const lenient = new RecordingPublisher();
  const other = await pipeToIQ(streamOf(chunks), lenient, { classify: classifyUIChunk, closeOnCancel: 'end' });
  assert.deepEqual(sent(lenient), chunks);
  assert.deepEqual([lenient.outcome, other.outcome, other.cause], ['end', 'end', 'abort']);

  // closeOnCancel is about a cancel: an error chunk in the same run still fails it.
  const failed = new RecordingPublisher();
  const third = await pipeToIQ(streamOf([{ type: 'abort' }, { type: 'error' }]), failed, { classify: classifyUIChunk, closeOnCancel: 'end' });
  assert.deepEqual([failed.outcome, third.cause], ['fail', 'error']);
});

test('a source that errors closes with fail and cause stream-error; the frames before it were sent', async () => {
  const publisher = new RecordingPublisher();
  const boom = new Error('the stream broke');
  let pulls = 0;
  const source = new ReadableStream({
    pull(controller) {
      if (++pulls === 3) controller.error(boom);
      else controller.enqueue(text(String(pulls)));
    },
  }, { highWaterMark: 0 });
  const result = await pipeToIQ(source, publisher, { classify: classifyUIChunk });
  assert.equal(publisher.framesSent, 2);
  assert.equal(publisher.outcome, 'fail');
  assert.deepEqual([result.outcome, result.cause, result.error], ['fail', 'stream-error', boom]);
});

test('a source that cannot be read at all still resolves', async () => {
  const publisher = new RecordingPublisher();
  const locked = streamOf([text('a')]);
  locked.getReader();
  const result = await pipeToIQ(locked, publisher);
  assert.deepEqual([result.outcome, result.cause, publisher.outcome], ['fail', 'stream-error', 'fail']);
  assert.ok(result.error instanceof TypeError);
});

test('a suspended leg that ends closes with end and says it was suspended', async () => {
  const publisher = new RecordingPublisher();
  const chunks = [{ type: 'tool-approval-request', approvalId: 'run::call', toolCallId: 'call' }, { type: 'data-tool-call-approval', data: { runId: 'run' } }];
  const result = await pipeToIQ(streamOf(chunks), publisher, { classify: classifyUIChunk });
  assert.deepEqual(sent(publisher), chunks);
  assert.deepEqual([publisher.outcome, result.outcome, result.suspended, result.cause], ['end', 'end', true, undefined]);
});

test('a source that stays open after a suspension is cancelled and the leg closes with end', async () => {
  const publisher = new RecordingPublisher();
  let cancelled = false;
  // A durable agent's stream: the approval request, its data part a moment later, then silence.
  const source = new ReadableStream({
    start(controller) {
      controller.enqueue({ type: 'start' });
      controller.enqueue({ type: 'tool-call-approval', payload: { toolCallId: 'call' } });
      setTimeout(() => controller.enqueue({ type: 'late' }), 20);
    },
    cancel() { cancelled = true; },
  });
  const started = performance.now();
  const result = await pipeToIQ(source, publisher, { classify: classifyMastraChunk, suspendGraceMs: 150 });
  const took = performance.now() - started;
  assert.deepEqual(sent(publisher).map((chunk) => chunk.type), ['start', 'tool-call-approval', 'late']);
  assert.equal(cancelled, true);
  assert.deepEqual([publisher.outcome, result.outcome, result.suspended], ['end', 'end', true]);
  assert.ok(took >= 150 && took < 1000, `the grace runs from the last chunk: ${took} ms`);
});

test('an async iterable that stays open after a suspension is left, too', async () => {
  const publisher = new RecordingPublisher();
  let release;
  const source = (async function* () {
    yield { type: 'data-tool-call-suspended', data: {} };
    await new Promise((resolve) => { release = resolve; });
  })();
  const result = await pipeToIQ(source, publisher, { classify: classifyUIChunk, suspendGraceMs: 50 });
  assert.deepEqual([publisher.outcome, result.suspended, publisher.framesSent], ['end', true, 1]);
  release();
});

test('oversize: the default replaces the chunk with a data-iq-omitted part; later frames are unaffected', async () => {
  const publisher = new RecordingPublisher();
  const big = { type: 'tool-output-available', toolCallId: 'call-1', output: 'x'.repeat(100_000) };
  const result = await pipeToIQ(streamOf([text('a'), big, text('b')]), publisher, { classify: classifyUIChunk });
  const [, omitted] = sent(publisher);
  assert.deepEqual(sent(publisher), [text('a'), omitted, text('b')]);
  assert.deepEqual(Object.keys(omitted), ['type', 'transient', 'data']);
  assert.deepEqual([omitted.type, omitted.transient, omitted.data.type, omitted.data.toolCallId], ['data-iq-omitted', true, 'tool-output-available', 'call-1']);
  assert.equal(omitted.data.bytes, publisher.frameSize(JSON.stringify(big)));
  assert.deepEqual([result.outcome, result.framesSent, result.framesOmitted, result.error], ['end', 3, 1, undefined]);
});

test('oversize: drop leaves the chunk out, a function sends what it returns', async () => {
  const big = { type: 'tool-output-available', toolCallId: 'call-1', output: 'x'.repeat(100_000) };
  const dropped = new RecordingPublisher();
  const first = await pipeToIQ(streamOf([text('a'), big, text('b')]), dropped, { oversize: 'drop' });
  assert.deepEqual(sent(dropped), [text('a'), text('b')]);
  assert.deepEqual([first.outcome, first.framesSent, first.framesOmitted], ['end', 2, 1]);

  const mapped = new RecordingPublisher();
  const calls = [];
  const second = await pipeToIQ(streamOf([text('a'), big, text('b')]), mapped, {
    oversize: (chunk, bytes) => {
      calls.push([chunk, bytes]);
      return { ...chunk, output: `${chunk.output.slice(0, 10)}…` };
    },
  });
  assert.deepEqual(calls, [[big, mapped.frameSize(JSON.stringify(big))]]);
  assert.deepEqual(sent(mapped), [text('a'), { ...big, output: 'xxxxxxxxxx…' }, text('b')]);
  assert.deepEqual([second.framesSent, second.framesOmitted], [3, 0]);

  // A function that gives up, returns something still too large, or throws: the chunk is left out, the run goes on.
  for (const oversize of [() => null, (chunk) => chunk, () => { throw new Error('mapper bug'); }]) {
    const publisher = new RecordingPublisher();
    const result = await pipeToIQ(streamOf([big, text('b')]), publisher, { oversize });
    assert.deepEqual(sent(publisher), [text('b')]);
    assert.deepEqual([result.outcome, result.framesSent, result.framesOmitted], ['end', 1, 1]);
  }
});

test('oversize follows the publisher\'s own limit', async () => {
  const publisher = new RecordingPublisher({ maxFrameBytes: 200 });
  const result = await pipeToIQ(streamOf([text('x'.repeat(300)), text('ok')]), publisher);
  assert.deepEqual(sent(publisher).map((chunk) => chunk.type), ['data-iq-omitted', 'text-delta']);
  assert.equal(result.framesOmitted, 1);
});

/** A RecordingPublisher whose send() throws when `when(call number)` says so. */
function throwing(when) {
  const publisher = new RecordingPublisher();
  const send = publisher.send.bind(publisher);
  let calls = 0;
  publisher.send = (data, event) => {
    if (when(++calls)) throw new IQError('refused');
    return send(data, event);
  };
  return publisher;
}

test('send throws on every call: the source is read to its end and the result says why nothing was sent', async () => {
  const publisher = throwing(() => true);
  let read = 0;
  const source = (async function* () {
    for (const chunk of [text('a'), text('b'), text('c')]) {
      read++;
      yield chunk;
    }
  })();
  const result = await pipeToIQ(source, publisher, { classify: classifyUIChunk });
  assert.equal(read, 3);
  assert.equal(result.framesSent, 0);
  assert.ok(result.error instanceof IQError);
  assert.equal(publisher.outcome, 'end');
});

test('send throws once: the remaining frames are sent', async () => {
  const publisher = throwing((call) => call === 2);
  const result = await pipeToIQ(streamOf([text('a'), text('b'), text('c')]), publisher);
  assert.deepEqual(sent(publisher), [text('a'), text('c')]);
  assert.deepEqual([result.outcome, result.framesSent], ['end', 2]);
  assert.ok(result.error instanceof IQError);
});

test('a publisher that gives up mid-run: the source is drained without encoding, and the outcome is fail', async () => {
  const publisher = new RecordingPublisher();
  let serialized = 0;
  let read = 0;
  const source = (async function* () {
    for (const chunk of [text('a'), text('b'), text('c'), { type: 'finish' }]) {
      read++;
      if (read === 3) publisher.fail();
      yield chunk;
    }
  })();
  const result = await pipeToIQ(source, publisher, { classify: classifyUIChunk, serialize: (chunk) => { serialized++; return JSON.stringify(chunk); } });
  assert.deepEqual([read, serialized, result.framesSent], [4, 2, 2]);
  assert.deepEqual([result.outcome, result.cause, result.publish], ['fail', undefined, { status: 0, closed: false }]);
  assert.equal(result.error, publisher.error);
});

test('close() that throws, a classifier that throws, a chunk that cannot be serialized: still a result', async () => {
  const publisher = new RecordingPublisher();
  publisher.close = async () => { throw new IQError('raised'); };
  const cyclic = { type: 'cyclic' };
  cyclic.self = cyclic;
  const result = await pipeToIQ(streamOf([text('a'), cyclic, text('b')]), publisher, {
    classify: (chunk) => { if (chunk.delta === 'b') throw new Error('classifier bug'); return undefined; },
  });
  assert.deepEqual(sent(publisher), [text('a'), text('b')]);
  assert.equal(result.outcome, 'fail');
  assert.ok(result.error instanceof TypeError, 'the first error is kept');
});
