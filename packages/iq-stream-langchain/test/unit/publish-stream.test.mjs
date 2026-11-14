// The generic loop: one frame per chunk, the close line at the end of the source, and nothing that rejects.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RecordingPublisher } from '@neuron-core/iq-stream/testing';
import { publishStream } from '../../dist/index.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function* generate(chunks, error) {
  for (const chunk of chunks) {
    await sleep(1);
    yield chunk;
  }
  if (error) throw error;
}

const readable = (chunks) => new ReadableStream({
  start(controller) {
    for (const chunk of chunks) controller.enqueue(chunk);
    controller.close();
  },
});

test('an async generator: one JSON frame per chunk, then close(end)', async () => {
  const publisher = new RecordingPublisher();
  const result = await publishStream(generate([{ n: 1 }, { n: 2 }, 'three']), publisher);
  assert.deepEqual(publisher.data, ['{"n":1}', '{"n":2}', '"three"']);
  assert.equal(publisher.outcome, 'end');
  assert.deepEqual(result, { outcome: 'end', frames: 3 });
});

test('a ReadableStream is read the same way, also where it is not async-iterable', async () => {
  const publisher = new RecordingPublisher();
  assert.deepEqual(await publishStream(readable([1, 2]), publisher), { outcome: 'end', frames: 2 });
  assert.deepEqual(publisher.data, ['1', '2']);

  const stream = readable([3, 4]);
  const other = new RecordingPublisher();
  assert.deepEqual(await publishStream({ getReader: () => stream.getReader() }, other), { outcome: 'end', frames: 2 });
  assert.deepEqual(other.data, ['3', '4']);
});

test('the promise of a stream is awaited; one that rejects is a run that failed before its first chunk', async () => {
  const publisher = new RecordingPublisher();
  assert.deepEqual(await publishStream(Promise.resolve(generate([1, 2])), publisher), { outcome: 'end', frames: 2 });
  assert.deepEqual(publisher.data, ['1', '2']);

  const refused = new Error('invalid input');
  const failing = new RecordingPublisher();
  assert.deepEqual(await publishStream(Promise.reject(refused), failing), { outcome: 'fail', error: refused, frames: 0 });
  assert.equal(failing.outcome, 'fail');
});

test('encode may return no frame, one, or several', async () => {
  const publisher = new RecordingPublisher();
  const encode = (chunk) => (chunk === 'skip' ? undefined : chunk === 'twice' ? ['a', 'b'] : chunk === 'none' ? [] : chunk);
  const result = await publishStream(generate(['skip', 'one', 'twice', 'none']), publisher, { encode });
  assert.deepEqual(publisher.data, ['one', 'a', 'b']);
  assert.equal(result.frames, 3);
});

test('a source that throws: the frames before it, then close(fail), and the error in the result', async () => {
  const publisher = new RecordingPublisher();
  const outage = new Error('outage');
  const result = await publishStream(generate([1, 2], outage), publisher);
  assert.deepEqual(publisher.data, ['1', '2']);
  assert.equal(publisher.outcome, 'fail');
  assert.deepEqual(result, { outcome: 'fail', error: outage, frames: 2 });
});

test('an aborted source closes with fail, or with abortOutcome', async () => {
  const aborted = () => generate([1], new DOMException('This operation was aborted', 'AbortError'));
  const byDefault = new RecordingPublisher();
  const first = await publishStream(aborted(), byDefault);
  assert.deepEqual([byDefault.outcome, first.outcome, first.error.name], ['fail', 'fail', 'AbortError']);

  const asEnd = new RecordingPublisher();
  const second = await publishStream(aborted(), asEnd, { abortOutcome: 'end' });
  assert.deepEqual([asEnd.outcome, second.outcome, second.error.name], ['end', 'end', 'AbortError']);

  // abortOutcome is for an abort only: any other error still fails the stream.
  const broken = new RecordingPublisher();
  await publishStream(generate([1], new Error('outage')), broken, { abortOutcome: 'end' });
  assert.equal(broken.outcome, 'fail');
});

test('beforeClose is awaited after the last frame and before the close line', async () => {
  const publisher = new RecordingPublisher();
  const seen = [];
  await publishStream(generate([1, 2]), publisher, {
    beforeClose: async (outcome, error) => {
      await sleep(50);
      seen.push({ outcome, error, frames: publisher.frames.length, closed: publisher.outcome });
    },
  });
  assert.deepEqual(seen, [{ outcome: 'end', error: undefined, frames: 2, closed: undefined }]);
  assert.equal(publisher.outcome, 'end');

  const failing = new RecordingPublisher();
  const outage = new Error('outage');
  await publishStream(generate([1], outage), failing, { beforeClose: (outcome, error) => void seen.push({ outcome, error }) });
  assert.deepEqual(seen.at(-1), { outcome: 'fail', error: outage });
});

test('beforeClose throws: the stream closes with fail and the promise still resolves', async () => {
  const publisher = new RecordingPublisher();
  const lost = new Error('database down');
  const result = await publishStream(generate([1]), publisher, { beforeClose: () => { throw lost; } });
  assert.equal(publisher.outcome, 'fail');
  assert.deepEqual(result, { outcome: 'fail', error: lost, frames: 1 });

  // After a failed run the run's error stays the result; the hook's goes to onError.
  const outage = new Error('outage');
  const reported = [];
  const after = await publishStream(generate([1], outage), new RecordingPublisher(), {
    beforeClose: () => { throw lost; },
    onError: (error) => reported.push(error),
  });
  assert.equal(after.error, outage);
  assert.deepEqual(reported, [lost]);
});

test('a frame the publisher refuses is dropped and reported, and the rest still goes out', async () => {
  const publisher = new RecordingPublisher({ maxFrameBytes: 64 });
  const reported = [];
  const result = await publishStream(generate(['small', 'x'.repeat(100), 'small again']), publisher, { onError: (error) => reported.push(error.name) });
  assert.deepEqual(publisher.data, ['"small"', '"small again"']);
  assert.deepEqual(reported, ['FrameTooLarge']);
  assert.deepEqual(result, { outcome: 'end', frames: 2 });
});

test('a chunk that cannot be encoded is dropped and reported', async () => {
  const publisher = new RecordingPublisher();
  const reported = [];
  const result = await publishStream(generate([1, 2n, 3]), publisher, { onError: (error) => reported.push(error.constructor.name) });
  assert.deepEqual(publisher.data, ['1', '3']);
  assert.deepEqual(reported, ['TypeError']);
  assert.equal(result.outcome, 'end');
});

test('an inert publisher is sent nothing more, and the source is still read to its end', async () => {
  const publisher = new RecordingPublisher();
  let read = 0;
  let closedWith;
  async function* source() {
    for (let n = 1; n <= 6; n++) {
      if (n === 3) publisher.fail();   // the publisher gives up, as the real one does after a fatal answer
      read++;
      yield n;
    }
  }
  const result = await publishStream(source(), publisher, { beforeClose: (outcome) => { closedWith = outcome; } });
  assert.equal(read, 6);
  assert.deepEqual(publisher.data, ['1', '2']);
  assert.equal(closedWith, 'end');
  assert.deepEqual(result, { outcome: 'end', frames: 2 });
});

test('a publisher that throws everywhere does not make the promise reject', async () => {
  const hostile = {
    send() { throw new Error('send failed'); },
    close() { return Promise.reject(new Error('close failed')); },
  };
  const reported = [];
  let saved = false;
  const result = await publishStream(generate([1, 2]), hostile, {
    beforeClose: () => { saved = true; },
    onError: (error) => { reported.push(error.message); throw new Error('the hook fails too'); },
  });
  assert.deepEqual(result, { outcome: 'end', frames: 0 });
  assert.equal(saved, true);
  assert.deepEqual(reported, ['send failed', 'send failed', 'close failed']);
});
