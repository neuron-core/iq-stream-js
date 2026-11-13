import assert from 'node:assert/strict';
import { test } from 'node:test';
import { setup, until } from './fake-transport.mjs';

const CLOSED = { lastEventId: 0, closed: true, outcome: 'end' };

test('writable() takes strings and {data, event} objects, and closes the stream with end', async () => {
  const { transport, publisher } = setup();
  const pub = publisher();
  const writer = pub.writable().getWriter();
  await writer.write('a');
  await writer.write({ data: 'b', event: 'token' });
  await writer.write({ data: 'c' });
  const request = await transport.request(0);
  const closing = writer.close();
  await until(() => request.ended);
  assert.deepEqual(request.lines, ['{"data":"a"}', '{"event":"token","data":"b"}', '{"data":"c"}', '{"close":"end"}']);
  request.respond(200, { lastEventId: 3, closed: true, outcome: 'end' });
  await closing;
  assert.equal((await pub.close()).outcome, 'end');
});

test('a pipe closes with end when its source ends', async () => {
  const { transport, publisher } = setup();
  const pub = publisher();
  transport.onOpen = (request) => until(() => request.ended).then(() => request.respond(200, { lastEventId: 3, closed: true, outcome: 'end' }));
  const source = new ReadableStream({
    start(controller) {
      for (const chunk of [{ type: 'start' }, { type: 'delta', text: 'Hi' }, { type: 'finish' }]) controller.enqueue(chunk);
      controller.close();
    },
  });
  const toJsonLines = new TransformStream({ transform: (chunk, controller) => controller.enqueue(JSON.stringify(chunk)) });
  await source.pipeThrough(toJsonLines).pipeTo(pub.writable());
  assert.deepEqual(transport.requests[0].data, ['{"type":"start"}', '{"type":"delta","text":"Hi"}', '{"type":"finish"}']);
  assert.deepEqual(transport.requests[0].closes, ['end']);
});

test('a pipe closes with fail when its source errors', async () => {
  const { transport, publisher } = setup();
  const pub = publisher();
  transport.onOpen = (request) => until(() => request.ended).then(() => request.respond(200, { lastEventId: 1, closed: true, outcome: 'fail', reason: 'publisher' }));
  let step = 0;
  const source = new ReadableStream({
    pull(controller) {
      if (step++ === 0) controller.enqueue('partial');
      else controller.error(new Error('the model failed'));
    },
  });
  await assert.rejects(source.pipeTo(pub.writable()), /the model failed/);
  assert.deepEqual(transport.requests[0].data, ['partial']);
  assert.deepEqual(transport.requests[0].closes, ['fail']);
  assert.equal((await pub.close('end')).outcome, 'fail', 'the first outcome wins');
});

test('aborting the writable closes with fail', async () => {
  const { transport, publisher } = setup();
  const pub = publisher();
  transport.onOpen = (request) => until(() => request.ended).then(() => request.respond(200, { lastEventId: 0, closed: true, outcome: 'fail', reason: 'publisher' }));
  const writer = pub.writable().getWriter();
  await writer.write('a');
  await writer.abort(new Error('cancelled'));
  assert.deepEqual(transport.requests[0].closes, ['fail']);
});

test('a chunk that send() refuses fails the stream instead of leaving it open', async () => {
  const { transport, publisher } = setup({ maxFrameBytes: 100 });
  const pub = publisher();
  transport.onOpen = (request) => until(() => request.ended).then(() => request.respond(200, { lastEventId: 1, closed: true, outcome: 'fail', reason: 'publisher' }));
  const writer = pub.writable().getWriter();
  await writer.write('fits');
  await assert.rejects(writer.write('x'.repeat(200)), (error) => error.name === 'FrameTooLarge');
  assert.deepEqual(transport.requests[0].data, ['fits']);
  assert.deepEqual(transport.requests[0].closes, ['fail']);
});

test('backpressure: a write waits until the writer has taken the line', async () => {
  const { transport, publisher } = setup();
  const pub = publisher();
  const request = await transport.request(0);
  request.stalled = true;
  const writer = pub.writable().getWriter();
  await writer.write('a'); // taken, and in flight on a connection that does not move
  let done = false;
  const second = writer.write('b').then(() => (done = true));
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(done, false, 'the second line waits behind the first');
  request.respond(408, { lastEventId: 0, closed: false, error: 'read_timeout' });
  await second;
  const next = await transport.request(1);
  await until(() => next.data.length === 2);
  assert.deepEqual(next.data, ['a', 'b']);
  next.respond(200, CLOSED);
  await writer.close();
});

test('a publisher that gave up drains a pipe without failing it', async () => {
  const { transport, publisher, errors } = setup();
  const pub = publisher();
  (await transport.request(0)).respond(401, { error: 'unauthorized' });
  await until(() => !pub.alive);
  const source = new ReadableStream({
    start(controller) {
      for (let i = 0; i < 50; i++) controller.enqueue(`chunk ${i}`);
      controller.close();
    },
  });
  await source.pipeTo(pub.writable());
  assert.equal(errors.length, 1);
  assert.equal(pub.framesSent, 0);
});
