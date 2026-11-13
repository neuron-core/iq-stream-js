import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RecordingPublisher } from '../../../dist/testing/index.js';
import { IQError } from '../../../dist/shared/errors.js';

test('RecordingPublisher records frames in the order send() returned, and the close outcome', async () => {
  const pub = new RecordingPublisher();
  assert.match(pub.streamId, /^[A-Za-z0-9_-]{22}$/);
  assert.equal(pub.maxFrameBytes, 65536);
  assert.equal(pub.alive, true);
  assert.equal(pub.send('a'), true);
  assert.equal(pub.send('b', 'token'), true);
  assert.equal(pub.send('{"type":"finish"}'), true);
  assert.deepEqual(pub.frames, [{ data: 'a' }, { data: 'b', event: 'token' }, { data: '{"type":"finish"}' }]);
  assert.deepEqual(pub.data, ['a', 'b', '{"type":"finish"}']);
  assert.equal(pub.framesSent, 3);
  assert.equal(pub.bytesSent, '{"data":"a"}\n{"event":"token","data":"b"}\n{"data":"{\\"type\\":\\"finish\\"}"}\n'.length);
  assert.equal(pub.outcome, undefined);

  assert.deepEqual(await pub.close(), { status: 200, lastEventId: 3, closed: true, outcome: 'end' });
  assert.equal(pub.outcome, 'end');
  assert.equal(pub.alive, false);
  assert.equal(pub.send('late'), false);
  assert.equal(pub.frames.length, 3);
});

test('RecordingPublisher: the first close wins', async () => {
  const pub = new RecordingPublisher({ streamId: 'my-own-stream-id-0001' });
  assert.equal(pub.streamId, 'my-own-stream-id-0001');
  pub.send('a');
  assert.deepEqual(await pub.close('fail'), { status: 200, lastEventId: 1, closed: true, outcome: 'fail', reason: 'publisher' });
  assert.equal((await pub.close('end')).outcome, 'fail');
  assert.equal(pub.outcome, 'fail');
});

test('RecordingPublisher refuses what the real publisher refuses', () => {
  const pub = new RecordingPublisher({ maxFrameBytes: 100 });
  assert.equal(pub.frameSize('Hi'), 13);
  assert.throws(() => pub.send('x'.repeat(200)), (error) => error.name === 'FrameTooLarge' && error instanceof IQError && error.limit === 100);
  assert.throws(() => pub.send(42), TypeError);
  assert.throws(() => pub.send('a', 'iq:end'), TypeError);
  assert.equal(pub.send('x'.repeat(89)), true);
  assert.deepEqual(pub.frames.length, 1);
});

test('RecordingPublisher: abort() and fail() make it inert', async () => {
  const aborted = new RecordingPublisher();
  aborted.send('a');
  aborted.abort();
  assert.equal(aborted.aborted, true);
  assert.equal(aborted.send('b'), false);
  assert.deepEqual(await aborted.close(), { status: 0, closed: false });
  assert.equal(aborted.outcome, undefined);

  const failed = new RecordingPublisher();
  failed.send('a');
  failed.fail();
  assert.ok(failed.error instanceof IQError);
  assert.equal(failed.alive, false);
  assert.equal(failed.send('b'), false);
  assert.deepEqual(failed.data, ['a']);
  assert.deepEqual(await failed.close(), { status: 0, closed: false });
});

test('RecordingPublisher takes send() from concurrent producers', async () => {
  const pub = new RecordingPublisher();
  const producer = async (name) => {
    for (let i = 0; i < 100; i++) {
      pub.send(`${name}${i}`);
      if (i % 7 === 0) await new Promise((resolve) => setImmediate(resolve));
    }
  };
  await Promise.all([producer('a'), producer('b'), producer('c')]);
  assert.equal(pub.frames.length, 300);
  for (const name of ['a', 'b', 'c']) {
    assert.deepEqual(pub.data.filter((data) => data.startsWith(name)), Array.from({ length: 100 }, (_, i) => `${name}${i}`), 'each producer keeps its order');
  }
});

test('RecordingPublisher.writable() closes with end, and with fail when aborted', async () => {
  const ended = new RecordingPublisher();
  const source = new ReadableStream({
    start(controller) {
      controller.enqueue('a');
      controller.enqueue({ data: 'b', event: 'token' });
      controller.close();
    },
  });
  await source.pipeTo(ended.writable());
  assert.deepEqual(ended.frames, [{ data: 'a' }, { data: 'b', event: 'token' }]);
  assert.equal(ended.outcome, 'end');

  const failed = new RecordingPublisher();
  const writer = failed.writable().getWriter();
  await writer.write('a');
  await writer.abort();
  assert.equal(failed.outcome, 'fail');
});
