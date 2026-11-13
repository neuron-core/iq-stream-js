// The scenarios of core-client.md ("Verified behaviour"), run against real servers with a real
// subscriber, for one build of the publisher. Each asserts on what the subscriber received.
import assert from 'node:assert/strict';
import { after, before, beforeEach, test } from 'node:test';
import { subscribe } from '../../../dist/testing/index.js';
import { proxy, setup, sleep, until } from './helpers.mjs';

const data = (subscription) => subscription.frames.map((frame) => frame.data);

export function scenarios(name, build) {
  let servers, errors, client, listen;
  before(async () => ({ servers, errors, client, listen } = await setup()));
  after(() => servers?.stop());
  beforeEach(() => errors.splice(0));

  test(`${name}: this is the build under test`, () => {
    assert.equal(build.BUILD, `iq-stream/publisher/${name}`);
  });

  test(`${name} T1: one request; each frame reaches the subscriber as it is written`, async () => {
    const iq = client(build, servers.default.url, { keepalive: 0.3 });
    const { id, subscription } = await listen(iq, servers.default);
    const pub = iq.publisher(id);
    assert.equal(pub.send('one'), true);
    await sleep(500);
    pub.send('two');
    await sleep(500);
    pub.send('three', 'token');
    await sleep(500);
    pub.send('four');
    const result = await pub.close('end');
    assert.deepEqual(result, { status: 200, lastEventId: 4, closed: true, outcome: 'end' });
    assert.equal(pub.requests, 1);

    const received = await subscription;
    assert.equal(received.status, 200);
    assert.deepEqual(data(received), ['one', 'two', 'three', 'four']);
    assert.deepEqual(received.frames.map((frame) => frame.event), ['message', 'message', 'token', 'message']);
    assert.deepEqual(received.frames.map((frame) => frame.id), ['1', '2', '3', '4']);
    assert.deepEqual(received.terminal, { event: 'iq:end', data: { lastEventId: 4 } });
    // Sent 500 ms apart: frames held back until a buffer filled or the body ended would arrive together.
    for (let i = 1; i < 4; i++) {
      const gap = received.frames[i].at - received.frames[i - 1].at;
      assert.ok(gap > 300 && gap < 700, `frame ${i + 1} arrived ${gap} ms after frame ${i}`);
    }
    assert.deepEqual(errors, []);
  });

  test(`${name}: the request is opened when the publisher is created, before any frame`, async () => {
    const iq = client(build, servers.short.url, { keepalive: null });
    const { id, subscription } = await listen(iq, servers.short);
    const pub = iq.publisher(id);
    // Nothing is ever written. Only a request whose head reached the server is answered 408 after its
    // read timeout; a stream without a publisher would fail with publisher_timeout instead.
    await until(() => pub.requests === 2, 'the 408 of a request that sent its head and nothing else', 4000);
    assert.equal((await pub.close()).lastEventId, 0);
    const received = await subscription;
    assert.deepEqual(received.frames, []);
    assert.deepEqual(received.terminal, { event: 'iq:end', data: { lastEventId: 0 } });
    assert.deepEqual(errors, []);
  });

  test(`${name} T2: silence past the read timeout is answered 408; the writer sees it without writing, reopens, and nothing is lost or repeated`, async () => {
    const iq = client(build, servers.short.url, { keepalive: null });
    const { id, subscription } = await listen(iq, servers.short);
    const pub = iq.publisher(id);
    pub.send('a');
    pub.send('b');
    await until(() => pub.requests === 2, 'the request to be reopened, with nothing written since the 408', 4000);
    await sleep(300);
    pub.send('c');
    pub.send('d');
    const result = await pub.close();
    assert.deepEqual(result, { status: 200, lastEventId: 4, closed: true, outcome: 'end' });
    assert.equal(pub.requests, 2);
    const received = await subscription;
    assert.deepEqual(data(received), ['a', 'b', 'c', 'd']);
    assert.equal(received.terminal.event, 'iq:end');
    assert.deepEqual(errors, []);
  });

  test(`${name} T3: silence with keepalives stays on one request`, async () => {
    const iq = client(build, servers.short.url, { keepalive: 0.5 });
    const { id, subscription } = await listen(iq, servers.short);
    const pub = iq.publisher(id);
    pub.send('a');
    await sleep(3200);
    pub.send('b');
    const result = await pub.close();
    assert.equal(result.lastEventId, 2);
    assert.equal(pub.requests, 1);
    const received = await subscription;
    assert.deepEqual(data(received), ['a', 'b']);
    assert.equal(received.terminal.event, 'iq:end');
    assert.deepEqual(errors, []);
  });

  test(`${name} T4: a line over the client's limit is refused before it is sent, and the stream goes on`, async () => {
    const iq = client(build, servers.short.url, { maxFrameBytes: 1024 });
    const { id, subscription } = await listen(iq, servers.short);
    const pub = iq.publisher(id);
    pub.send('x'.repeat(1024 - 11));
    assert.throws(() => pub.send('x'.repeat(2000)), build.FrameTooLarge);
    pub.send('after');
    assert.equal((await pub.close()).lastEventId, 2);
    const received = await subscription;
    assert.deepEqual(data(received).map((text) => text.length), [1013, 5], 'a line of exactly the limit is accepted by the server');
    assert.equal(received.terminal.event, 'iq:end');
    assert.deepEqual(errors, []);
  });

  test(`${name} T4: a line over the server's limit is answered 413, and the publisher closes the stream with fail at once`, async () => {
    const iq = client(build, servers.short.url);
    const { id, subscription } = await listen(iq, servers.short);
    const started = Date.now();
    const pub = iq.publisher(id);
    pub.send('ok');
    pub.send('x'.repeat(2000));
    await until(() => !pub.alive, 'the publisher to give up');
    assert.equal(pub.send('after'), false, 'fail open: no exception');
    assert.equal(pub.error.status, 413);
    assert.equal(pub.error.code, 'frame_too_large');
    assert.equal(errors.pop(), pub.error);
    const result = await pub.close('end');
    assert.deepEqual(result, { status: 413, lastEventId: 1, closed: true, outcome: 'fail', reason: 'publisher', error: 'frame_too_large' });

    const received = await subscription;
    assert.deepEqual(data(received), ['ok']);
    assert.deepEqual(received.terminal, { event: 'iq:fail', data: { reason: 'publisher', lastEventId: 1 } });
    assert.ok(Date.now() - started < 3000, 'not after the idle timeout, as publisher_idle');
    assert.deepEqual(errors, []);
  });

  test(`${name} T5: a stream past its byte limit is closed by the server; the publisher stops and has nothing to close`, async () => {
    const iq = client(build, servers.short.url);
    const { id, subscription } = await listen(iq, servers.short);
    const pub = iq.publisher(id);
    for (let i = 0; i < 20; i++) pub.send(`${i}`.padEnd(900, 'y'));
    const result = await pub.close();
    assert.equal(result.status, 413);
    assert.equal(result.error, 'stream_too_large');
    assert.deepEqual([result.closed, result.outcome, result.reason], [true, 'fail', 'too_large']);
    assert.equal(pub.error.code, 'stream_too_large');
    assert.equal(errors.pop(), pub.error);

    const received = await subscription;
    assert.ok(received.frames.length > 0 && received.frames.length < 20);
    assert.equal(received.frames.length, result.lastEventId);
    assert.deepEqual(data(received).map((text) => Number.parseInt(text, 10)), received.frames.map((_, i) => i), 'the frames that fit, in order');
    assert.equal(received.terminal.event, 'iq:fail');
    assert.equal(received.terminal.data.reason, 'too_large');
  });

  test(`${name} T6: a connection lost mid-stream is continued: probe, then exactly the missing frames`, async () => {
    const lossy = await proxy(servers.short.url);
    try {
      const iq = client(build, lossy.url, { keepalive: 1 });
      const { id, subscription } = await listen(iq, servers.short);
      const pub = iq.publisher(id);
      for (let i = 1; i <= 5; i++) pub.send(`f${i}`);
      await sleep(300);
      lossy.kill();
      for (let i = 6; i <= 10; i++) {
        pub.send(`f${i}`);
        await sleep(50);
      }
      const result = await pub.close();
      assert.deepEqual(result, { status: 200, lastEventId: 10, closed: true, outcome: 'end' });
      assert.equal(pub.requests, 2);
      const received = await subscription;
      assert.deepEqual(data(received), Array.from({ length: 10 }, (_, i) => `f${i + 1}`), 'all ten, once each, in order');
      assert.equal(received.terminal.event, 'iq:end');
      assert.deepEqual(errors, []);
    } finally {
      await lossy.close();
    }
  });

  test(`${name} T8: a run that closes at once is gone for a browser that arrives 400 ms later; minOpen keeps it`, async () => {
    const run = async (minOpen) => {
      const iq = client(build, servers.default.url, { minOpen });
      const id = iq.newStreamId();
      const ticket = await iq.ticket(id);
      const pub = iq.publisher(id);
      pub.send('quick ');
      pub.send('answer');
      const closing = pub.close();
      await sleep(400);
      const received = await subscribe(servers.default.url, id, ticket.token);
      assert.deepEqual(await closing, { status: 200, lastEventId: 2, closed: true, outcome: 'end' });
      return received;
    };
    const late = await run(0);
    assert.deepEqual(late.frames, []);
    assert.deepEqual(late.terminal, { event: 'iq:gone', data: { outcome: 'end', lastEventId: 2 } });

    const held = await run(1.5);
    assert.deepEqual(data(held), ['quick ', 'answer']);
    assert.deepEqual(held.terminal, { event: 'iq:end', data: { lastEventId: 2 } });
    assert.deepEqual(errors, []);
  });

  test(`${name} N2/N3: a closed stream refuses the request early; the publisher goes inert and close() does not hang`, async () => {
    const iq = client(build, servers.default.url);
    const id = iq.newStreamId();
    const first = iq.publisher(id);
    first.send('the answer');
    assert.equal((await first.close()).closed, true);

    const started = Date.now();
    const second = iq.publisher(id);
    // Written while the refusal is on its way: with fetch, a write after the answer would never settle.
    for (let i = 0; i < 10; i++) {
      second.send(`x${i}`);
      await sleep(20);
    }
    await until(() => !second.alive, 'the refusal to be seen', 2000);
    assert.equal(second.send('more'), false);
    const result = await second.close();
    assert.deepEqual(result, { status: 409, closed: true, outcome: 'end', error: 'stream_closed' });
    assert.ok(Date.now() - started < 2000);
    assert.equal(second.requests, 1);
    assert.equal(second.error.code, 'stream_closed');
    assert.equal(errors.pop(), second.error);
    assert.deepEqual(errors, []);
  });

  test(`${name}: abort() leaves the stream open, and closeStream() ends it from elsewhere`, async () => {
    const iq = client(build, servers.default.url);
    const { id, subscription } = await listen(iq, servers.default);
    const pub = iq.publisher(id);
    pub.send('a');
    pub.send('b');
    await sleep(200);
    pub.abort();
    assert.deepEqual(await pub.close(), { status: 0, closed: false });
    const result = await iq.closeStream(id, 'end');
    assert.deepEqual(result, { status: 200, lastEventId: 2, closed: true, outcome: 'end' });
    const again = await iq.closeStream(id);
    assert.deepEqual(again, { status: 409, closed: true, outcome: 'end', error: 'stream_closed' }, 'a closed stream is left as it is');
    const received = await subscription;
    assert.deepEqual(data(received), ['a', 'b']);
    assert.equal(received.terminal.event, 'iq:end');
    assert.deepEqual(errors, []);
  });

  test(`${name}: bad credentials are fatal and reported, and the run goes on`, async () => {
    const iq = client(build, servers.default.url, { appSecret: 'not-the-secret-of-this-app-0123456789' });
    const pub = iq.publisher(iq.newStreamId());
    pub.send('a');
    const result = await pub.close();
    assert.equal(result.status, 401);
    assert.equal(result.closed, false);
    assert.equal(pub.error.status, 401);
    assert.equal(errors.pop(), pub.error);
    await assert.rejects(iq.reserve(iq.newStreamId()), (error) => error instanceof build.IQError && error.status === 401);
  });

  test(`${name}: a server that cannot be reached is retried for retryBudget, then given up`, async () => {
    const closed = await proxy(servers.default.url);
    await closed.close();
    const iq = client(build, closed.url, { retryBudget: 0.5 });
    const started = Date.now();
    const pub = iq.publisher(iq.newStreamId());
    pub.send('a');
    const result = await pub.close();
    assert.deepEqual(result, { status: 0, closed: false, error: 'unreachable' });
    assert.ok(Date.now() - started < 3000);
    assert.equal(pub.error.code, 'unreachable');
    assert.equal(errors.pop(), pub.error);
  });

  test(`${name}: batch mode publishes in short requests`, async () => {
    const iq = client(build, servers.default.url, { mode: 'batch', batchMs: 50 });
    const { id, subscription } = await listen(iq, servers.default);
    const pub = iq.publisher(id);
    pub.send('a');
    pub.send('b');
    await sleep(250);
    pub.send('c', 'token');
    const result = await pub.close();
    assert.deepEqual(result, { status: 200, lastEventId: 3, closed: true, outcome: 'end' });
    assert.ok(pub.requests >= 3, `${pub.requests} requests`);
    const received = await subscription;
    assert.deepEqual(data(received), ['a', 'b', 'c']);
    assert.equal(received.frames[2].event, 'token');
    assert.ok(received.frames[2].at - received.frames[1].at > 100, 'the first batch did not wait for the second');
    assert.equal(received.terminal.event, 'iq:end');
    assert.deepEqual(errors, []);
  });

  test(`${name}: a pipe into writable() publishes and closes the stream`, async () => {
    const iq = client(build, servers.default.url);
    const { id, subscription } = await listen(iq, servers.default);
    const source = new ReadableStream({
      start(controller) {
        for (const chunk of ['a', { data: 'b', event: 'token' }, 'c']) controller.enqueue(chunk);
        controller.close();
      },
    });
    await source.pipeTo(iq.publisher(id).writable());
    const received = await subscription;
    assert.deepEqual(data(received), ['a', 'b', 'c']);
    assert.equal(received.terminal.event, 'iq:end');
  });
}
