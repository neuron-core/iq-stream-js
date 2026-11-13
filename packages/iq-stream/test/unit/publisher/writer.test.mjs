// The writer's state machine against a scripted transport: each row of core-client.md's table of
// answers, and what the writer does next.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { IQError } from '../../../dist/shared/errors.js';
import { SECRET, STREAM, setup, sleep, until } from './fake-transport.mjs';

const CLOSED = { lastEventId: 0, closed: true, outcome: 'end' };
const closedFail = (lastEventId) => ({ status: 200, body: { lastEventId, closed: true, outcome: 'fail', reason: 'publisher' } });
const open = (lastEventId) => ({ status: 200, body: { lastEventId, closed: false } });

test('the request opens at creation, lines go out in the order send() returned, and close() returns the answer', async () => {
  const { transport, publisher, client, errors } = setup();
  const pub = publisher();
  const request = await transport.request(0);
  assert.deepEqual(request.lines, [], 'opened before any frame');
  assert.equal(request.streamId, STREAM);
  assert.equal(await client.verify('pub', request.token, STREAM), true, 'a pub token for this stream');

  assert.equal(pub.send('a'), true);
  assert.equal(pub.send('b', 'token'), true);
  assert.equal(pub.send('{"type":"x"}'), true);
  await until(() => request.lines.length === 3);
  assert.deepEqual(request.lines, ['{"data":"a"}', '{"event":"token","data":"b"}', '{"data":"{\\"type\\":\\"x\\"}"}']);
  assert.equal(request.ended, false);
  assert.equal(pub.alive, true);

  const closing = pub.close();
  await until(() => request.ended);
  assert.deepEqual(request.closes, ['end']);
  assert.equal(pub.alive, false);
  assert.equal(pub.send('late'), false, 'nothing is queued after close()');
  request.respond(200, { stream: STREAM, lastEventId: 3, closed: true, outcome: 'end' });
  assert.deepEqual(await closing, { status: 200, lastEventId: 3, closed: true, outcome: 'end' });

  assert.equal(pub.requests, 1);
  assert.equal(pub.framesSent, 3);
  assert.equal(pub.bytesSent, request.lines.slice(0, 3).join('\n').length + 1, 'the bytes of the lines queued, newlines included');
  assert.equal(pub.error, undefined);
  assert.equal(request.destroyed, true, 'no connection is left open');
  assert.deepEqual(transport.posts, []);
  assert.deepEqual(errors, []);
});

test('408 read_timeout: the writer reopens and resumes from lastEventId, without a probe', async () => {
  const { transport, publisher, errors } = setup();
  const pub = publisher();
  for (const data of ['a', 'b', 'c', 'd']) pub.send(data);
  const first = await transport.request(0);
  await until(() => first.data.length === 4);
  first.respond(408, { lastEventId: 2, closed: false, error: 'read_timeout' });

  const second = await transport.request(1);
  await until(() => second.data.length === 2);
  assert.deepEqual(second.data, ['c', 'd'], 'exactly the frames the server does not hold');
  pub.send('e');
  await until(() => second.data.length === 3);
  second.respond(408, { lastEventId: 5, closed: false, error: 'read_timeout' });

  const third = await transport.request(2);
  const closing = pub.close();
  await until(() => third.ended);
  assert.deepEqual(third.lines, ['{"close":"end"}'], 'nothing to resend');
  third.respond(200, { lastEventId: 5, closed: true, outcome: 'end' });
  assert.equal((await closing).lastEventId, 5);
  assert.equal(pub.requests, 3);
  assert.equal(transport.probes, 0);
  assert.deepEqual(errors, []);
});

test('a lost connection: the writer probes with an empty POST and resends what the server is missing', async () => {
  const { transport, publisher, errors } = setup();
  const pub = publisher();
  for (let i = 1; i <= 5; i++) pub.send(`f${i}`);
  const first = await transport.request(0);
  await until(() => first.data.length === 5);
  transport.onPost = () => open(3);
  first.respond(0);

  const second = await transport.request(1);
  await until(() => second.data.length === 2);
  assert.deepEqual(transport.posts, [''], 'one probe, with an empty body');
  assert.deepEqual(second.data, ['f4', 'f5']);
  for (let i = 6; i <= 7; i++) pub.send(`f${i}`);
  const closing = pub.close();
  await until(() => second.ended);
  assert.deepEqual(second.data, ['f4', 'f5', 'f6', 'f7']);
  second.respond(200, { lastEventId: 7, closed: true, outcome: 'end' });
  assert.deepEqual(await closing, { status: 200, lastEventId: 7, closed: true, outcome: 'end' });
  assert.deepEqual(errors, []);
});

test('frames sent while the stream is being recovered wait in the queue and follow the resent ones', async () => {
  const { transport, publisher } = setup();
  const pub = publisher();
  pub.send('a');
  const first = await transport.request(0);
  await until(() => first.data.length === 1);
  let answerProbe;
  transport.onPost = () => new Promise((resolve) => (answerProbe = resolve));
  first.respond(0);
  await until(() => transport.probes === 1);
  pub.send('b');
  pub.send('c');
  answerProbe(open(0));
  const second = await transport.request(1);
  await until(() => second.data.length === 3);
  assert.deepEqual(second.data, ['a', 'b', 'c']);
  second.respond(200, CLOSED);
  await pub.close();
});

test('nothing is written into a request the server has answered', async () => {
  const { transport, publisher } = setup();
  const pub = publisher();
  pub.send('a');
  const first = await transport.request(0);
  await until(() => first.data.length === 1);
  first.settled = true; // the head of an answer has arrived; its body has not
  pub.send('b');
  pub.send('c');
  await sleep(30);
  assert.deepEqual(first.data, ['a']);
  first.settled = false;
  first.respond(408, { lastEventId: 1, closed: false, error: 'read_timeout' });
  const second = await transport.request(1);
  await until(() => second.data.length === 2);
  assert.deepEqual(second.data, ['b', 'c']);
  assert.equal(first.lateWrites, 0);
  second.respond(200, CLOSED);
  await pub.close();
});

test('a close line lost with its request is sent again', async () => {
  const { transport, publisher } = setup();
  const pub = publisher();
  pub.send('a');
  const closing = pub.close('fail');
  const first = await transport.request(0);
  await until(() => first.ended);
  first.respond(408, { lastEventId: 1, closed: false, error: 'read_timeout' });
  const second = await transport.request(1);
  await until(() => second.ended);
  assert.deepEqual(second.lines, ['{"close":"fail"}']);
  second.respond(200, { lastEventId: 1, closed: true, outcome: 'fail', reason: 'publisher' });
  assert.deepEqual(await closing, { status: 200, lastEventId: 1, closed: true, outcome: 'fail', reason: 'publisher' });
});

test('409 publisher_active on the probe is retried with backoff until the old request is let go', async () => {
  const { transport, publisher, errors } = setup();
  const pub = publisher();
  pub.send('a');
  const first = await transport.request(0);
  await until(() => first.data.length === 1);
  transport.onPost = (body, n) => (n < 2 ? { status: 409, body: { error: 'publisher_active' } } : open(1));
  const started = Date.now();
  first.respond(0);
  const second = await transport.request(1);
  assert.equal(transport.probes, 3);
  assert.ok(Date.now() - started >= 250, 'the retries are spaced out');
  assert.deepEqual(second.data, []);
  second.respond(200, CLOSED);
  await pub.close();
  assert.deepEqual(errors, []);
});

test('409 publisher_active may outlast retryBudget: the server lets a dead request go only after its read timeout', async () => {
  const { transport, publisher, errors } = setup({ retryBudget: 0.2, takeoverBudget: 5 });
  const pub = publisher();
  pub.send('a');
  const first = await transport.request(0);
  await until(() => first.data.length === 1);
  const started = Date.now();
  transport.onPost = () => (Date.now() - started < 600 ? { status: 409, body: { error: 'publisher_active' } } : open(1));
  first.respond(0);
  pub.send('b');
  const second = await transport.request(1);
  await until(() => second.data.length === 1);
  assert.ok(Date.now() - started >= 600, 'it waited well past retryBudget');
  assert.deepEqual(second.data, ['b']);
  second.respond(200, { lastEventId: 2, closed: true, outcome: 'end' });
  assert.equal((await pub.close()).status, 200);
  assert.deepEqual(errors, []);
});

test('409 publisher_active that never clears: retries stop at takeoverBudget, and the stream is closed with fail', async () => {
  const { transport, publisher, errors } = setup({ takeoverBudget: 0.5 });
  const pub = publisher();
  pub.send('a');
  transport.onPost = (body) => (body === '' ? { status: 409, body: { error: 'publisher_active' } } : closedFail(0));
  const started = Date.now();
  (await transport.request(0)).respond(409, { error: 'publisher_active' });
  const result = await pub.close();
  assert.ok(Date.now() - started < 1500, 'bounded');
  assert.ok(transport.probes >= 2, 'retried');
  assert.deepEqual(transport.closes, ['fail']);
  assert.deepEqual(result, { status: 409, closed: true, error: 'publisher_active', lastEventId: 0, outcome: 'fail', reason: 'publisher' });
  assert.equal(errors.length, 1);
  assert.equal(errors[0].status, 409);
  assert.equal(errors[0].code, 'publisher_active');
  assert.equal(pub.requests, 1, 'no second stream request while the server says "not now"');
});

test('503: retried with backoff, then the stream goes on with nothing lost', async () => {
  const { transport, publisher, errors } = setup();
  const pub = publisher();
  pub.send('a');
  pub.send('b');
  transport.onPost = (body, n) => (n === 0 ? { status: 503, body: { error: 'unavailable' } } : open(0));
  (await transport.request(0)).respond(503, { error: 'unavailable' });
  const second = await transport.request(1);
  await until(() => second.data.length === 2);
  assert.equal(transport.probes, 2);
  assert.deepEqual(second.data, ['a', 'b']);
  second.respond(200, { lastEventId: 2, closed: true, outcome: 'end' });
  assert.equal((await pub.close()).status, 200);
  assert.deepEqual(errors, []);
});

test('503 that lasts: fatal after retryBudget', async () => {
  const { transport, publisher, errors } = setup({ retryBudget: 0.4 });
  const pub = publisher();
  transport.onPost = () => ({ status: 503, body: { error: 'unavailable' } });
  (await transport.request(0)).respond(503, { error: 'unavailable' });
  await until(() => !pub.alive);
  assert.equal(pub.send('a'), false);
  const result = await pub.close();
  assert.equal(result.status, 503);
  assert.equal(result.closed, false);
  assert.equal(pub.error, errors[0]);
  assert.equal(pub.error.status, 503);
  assert.ok(transport.probes >= 1);
});

test('no answer at all: the probe is retried within retryBudget, then the publisher gives up', async () => {
  const { transport, publisher, errors } = setup({ retryBudget: 0.4 });
  const pub = publisher();
  transport.onPost = () => ({ status: 0, body: {} });
  transport.onOpen = (request) => request.respond(0);
  const result = await pub.close();
  assert.deepEqual(result, { status: 0, closed: false, error: 'unreachable' });
  assert.ok(transport.probes >= 2);
  assert.equal(errors.length, 1);
  assert.equal(errors[0].status, undefined);
  assert.equal(errors[0].code, 'unreachable');
});

test('a stream request that keeps failing while probes succeed does not loop for ever', async () => {
  const { transport, publisher, errors } = setup({ retryBudget: 0.4 });
  const pub = publisher();
  pub.send('a');
  transport.onOpen = (request) => request.respond(0);
  const result = await pub.close();
  assert.equal(result.error, 'unreachable');
  assert.equal(result.outcome, 'fail', 'the stream the probes created is closed');
  assert.ok(pub.requests >= 2 && pub.requests < 10, `${pub.requests} requests`);
  assert.equal(errors.length, 1);
});

test('409 stream_closed: fatal, and there is nothing to close', async () => {
  const { transport, publisher, errors } = setup();
  const pub = publisher();
  pub.send('a');
  (await transport.request(0)).respond(409, { error: 'stream_closed', outcome: 'fail', reason: 'max_duration' });
  await until(() => !pub.alive);
  assert.equal(pub.send('b'), false);
  assert.deepEqual(await pub.close(), { status: 409, closed: true, outcome: 'fail', reason: 'max_duration', error: 'stream_closed' });
  assert.deepEqual(transport.posts, []);
  assert.equal(errors.length, 1);
  assert.equal(errors[0].code, 'stream_closed');
  assert.equal(errors[0].streamId, STREAM);
  assert.equal(pub.requests, 1);
});

test('413 stream_too_large: the server closed the stream; fatal, nothing to close', async () => {
  const { transport, publisher, errors } = setup();
  const pub = publisher();
  pub.send('a');
  (await transport.request(0)).respond(413, { lastEventId: 9, closed: true, outcome: 'fail', reason: 'too_large', error: 'stream_too_large' });
  assert.deepEqual(await pub.close(), { status: 413, lastEventId: 9, closed: true, outcome: 'fail', reason: 'too_large', error: 'stream_too_large' });
  assert.deepEqual(transport.posts, []);
  assert.equal(errors[0].code, 'stream_too_large');
});

for (const [status, error] of [[413, 'frame_too_large'], [400, 'invalid_frame']]) {
  test(`${status} ${error}: a line was refused and the stream is still open, so it is closed with fail out of band`, async () => {
    const { transport, publisher, errors } = setup();
    const pub = publisher();
    pub.send('ok');
    pub.send('refused');
    transport.onPost = () => closedFail(1);
    const request = await transport.request(0);
    await until(() => request.data.length === 2);
    request.respond(status, { lastEventId: 1, closed: false, error, line: 2 });
    await until(() => !pub.alive);
    assert.equal(pub.send('after'), false);
    assert.deepEqual(await pub.close('end'), { status, lastEventId: 1, closed: true, outcome: 'fail', reason: 'publisher', error });
    assert.deepEqual(transport.posts, ['{"close":"fail"}\n']);
    assert.equal(request.destroyed, true);
    assert.equal(errors.length, 1);
    assert.equal(errors[0].code, error);
    assert.equal(pub.requests, 1);
  });
}

for (const [status, error] of [[401, 'unauthorized'], [403, 'forbidden'], [415, 'unsupported_media_type'], [429, 'app_limit'], [404, undefined], [500, undefined]]) {
  test(`${status}: fatal; the stream may not exist, so nothing is closed`, async () => {
    const { transport, publisher, errors } = setup();
    const pub = publisher();
    pub.send('a');
    (await transport.request(0)).respond(status, error ? { error } : {});
    assert.deepEqual(await pub.close(), error ? { status, closed: false, error } : { status, closed: false });
    assert.deepEqual(transport.posts, []);
    assert.equal(errors.length, 1);
    assert.equal(errors[0].status, status);
    assert.equal(errors[0].code, error);
    assert.equal(pub.alive, false);
    assert.equal(pub.requests, 1);
  });
}

test('an error says which stream and why, and never carries a token, the secret or frame data', async () => {
  const { transport, publisher, errors } = setup();
  const pub = publisher();
  pub.send('very-private-frame-data');
  const request = await transport.request(0);
  await until(() => request.data.length === 1);
  request.respond(413, { lastEventId: 0, closed: false, error: 'frame_too_large', line: 1 });
  await pub.close();
  const [error] = errors;
  assert.ok(error instanceof IQError);
  assert.match(error.message, new RegExp(STREAM));
  assert.match(error.message, /413/);
  assert.match(error.message, /frame_too_large/);
  const everything = `${error.message} ${error.stack} ${JSON.stringify(error)}`;
  for (const secret of ['very-private-frame-data', SECRET, request.token]) assert.ok(!everything.includes(secret));
});

test('the replay ring is bounded by frames: a gap it cannot bridge closes the stream with fail', async () => {
  const { transport, publisher, errors } = setup({ replayFrames: 2 });
  const pub = publisher();
  for (let i = 1; i <= 5; i++) pub.send(`f${i}`);
  transport.onPost = () => closedFail(2);
  const request = await transport.request(0);
  await until(() => request.data.length === 5);
  request.respond(408, { lastEventId: 2, closed: false, error: 'read_timeout' });
  const result = await pub.close('end');
  assert.deepEqual(result, { status: 408, lastEventId: 2, closed: true, outcome: 'fail', reason: 'publisher', error: 'gap' });
  assert.deepEqual(transport.posts, ['{"close":"fail"}\n']);
  assert.equal(pub.requests, 1, 'a stream with a hole in it is not continued');
  assert.equal(errors[0].code, 'gap');
});

test('the replay ring holds exactly replayFrames: one frame fewer missing is bridged', async () => {
  const { transport, publisher, errors } = setup({ replayFrames: 2 });
  const pub = publisher();
  for (let i = 1; i <= 5; i++) pub.send(`f${i}`);
  const first = await transport.request(0);
  await until(() => first.data.length === 5);
  first.respond(408, { lastEventId: 3, closed: false, error: 'read_timeout' });
  const second = await transport.request(1);
  await until(() => second.data.length === 2);
  assert.deepEqual(second.data, ['f4', 'f5']);
  second.respond(200, CLOSED);
  await pub.close();
  assert.deepEqual(errors, []);
});

test('the replay ring is bounded by bytes too', async () => {
  const { transport, publisher, errors } = setup({ replayBytes: 250 });
  const pub = publisher();
  for (let i = 1; i <= 4; i++) pub.send(`${i}`.repeat(89)); // each line is 101 bytes: the ring holds two
  const first = await transport.request(0);
  await until(() => first.data.length === 4);
  first.respond(408, { lastEventId: 2, closed: false, error: 'read_timeout' });
  const second = await transport.request(1);
  await until(() => second.data.length === 2);
  assert.deepEqual(second.data.map((data) => data[0]), ['3', '4']);
  second.respond(408, { lastEventId: 1, closed: false, error: 'read_timeout' });
  await until(() => !pub.alive);
  assert.equal(errors[0].code, 'gap');
  await pub.close();
});

test('a restarted server reports fewer frames than the ring can bridge: fail, and nothing is published into the new stream', async () => {
  const { transport, publisher, errors } = setup({ replayFrames: 3 });
  const pub = publisher();
  for (let i = 1; i <= 8; i++) pub.send(`f${i}`);
  const first = await transport.request(0);
  await until(() => first.data.length === 8);
  transport.onPost = (body) => (body === '' ? open(0) : closedFail(0));
  first.respond(0);
  const result = await pub.close();
  assert.equal(result.outcome, 'fail');
  assert.equal(result.error, 'gap');
  assert.deepEqual(transport.posts, ['', '{"close":"fail"}\n']);
  assert.equal(transport.requests.length, 1);
  assert.equal(errors.length, 1);
});

test('a server that holds more frames than were written is another stream: fail', async () => {
  const { transport, publisher, errors } = setup();
  const pub = publisher();
  pub.send('a');
  const first = await transport.request(0);
  await until(() => first.data.length === 1);
  first.respond(408, { lastEventId: 7, closed: false, error: 'read_timeout' });
  assert.equal((await pub.close()).error, 'gap');
  assert.deepEqual(transport.closes, ['fail']);
  assert.equal(errors.length, 1);
});

test('minOpen holds back the close line, and only the close line', async () => {
  const { transport, publisher } = setup({ minOpen: 0.8 });
  const started = Date.now();
  const pub = publisher();
  pub.send('quick');
  pub.send('answer');
  const closing = pub.close();
  const request = await transport.request(0);
  await until(() => request.data.length === 2);
  assert.ok(Date.now() - started < 300, 'frames are never delayed');
  await sleep(200);
  assert.deepEqual(request.closes, [], 'the close line waits');
  assert.equal(request.ended, false);
  await until(() => request.ended);
  assert.ok(Date.now() - started >= 750, `the close line went out after ${Date.now() - started} ms`);
  assert.deepEqual(request.lines, ['{"data":"quick"}', '{"data":"answer"}', '{"close":"end"}']);
  request.respond(200, { lastEventId: 2, closed: true, outcome: 'end' });
  await closing;
});

test('a publisher older than minOpen closes at once', async () => {
  const { transport, publisher } = setup({ minOpen: 0.2 });
  const pub = publisher();
  const request = await transport.request(0);
  await sleep(250);
  const closing = pub.close();
  await until(() => request.ended, 'the close line', 300);
  request.respond(200, CLOSED);
  await closing;
});

test('the first close wins, and every call returns the same result', async () => {
  const { transport, publisher } = setup();
  const pub = publisher();
  pub.send('a');
  const failing = pub.close('fail');
  const ending = pub.close('end');
  const request = await transport.request(0);
  await until(() => request.ended);
  assert.deepEqual(request.closes, ['fail']);
  request.respond(200, { lastEventId: 1, closed: true, outcome: 'fail', reason: 'publisher' });
  const result = await failing;
  assert.equal(result.outcome, 'fail');
  assert.equal(await ending, result);
  assert.equal(await pub.close('end'), result);
  assert.equal(await pub.close(), result);
  assert.equal(transport.requests.length, 1);
  assert.deepEqual(transport.posts, []);
  assert.throws(() => pub.close('done'), TypeError);
  assert.throws(() => pub.close('end', { timeout: Number.NaN }), TypeError);
});

test('fail open: after a fatal answer send() returns false, close() resolves, and the handler ran once', async () => {
  const { transport, publisher, errors } = setup();
  const pub = publisher();
  assert.equal(pub.send('a'), true);
  (await transport.request(0)).respond(401, { error: 'unauthorized' });
  await until(() => !pub.alive);
  assert.equal(pub.send('b'), false);
  assert.equal(pub.send('c', 'token'), false);
  assert.equal(pub.framesSent, 1, 'only what was queued is counted');
  assert.deepEqual(await pub.close('end'), { status: 401, closed: false, error: 'unauthorized' });
  assert.deepEqual(await pub.close('fail'), { status: 401, closed: false, error: 'unauthorized' });
  assert.equal(errors.length, 1);
  assert.equal(pub.error, errors[0]);
});

test('fail open without a handler: one warning, with nothing sensitive in it', async (t) => {
  const warn = t.mock.method(console, 'warn', () => {});
  const { transport, client } = setup({ errorHandler: undefined });
  const pub = client.publisher(STREAM);
  pub.send('private-data');
  const request = await transport.request(0);
  request.respond(403, { error: 'forbidden' });
  await pub.close();
  assert.equal(warn.mock.callCount(), 1);
  const [message] = warn.mock.calls[0].arguments;
  assert.match(message, /403/);
  for (const secret of ['private-data', SECRET, request.token]) assert.ok(!message.includes(secret));
});

test('a handler that throws does not break the run', async () => {
  const { transport, client } = setup({
    errorHandler: () => {
      throw new Error('handler failed');
    },
  });
  const pub = client.publisher(STREAM);
  (await transport.request(0)).respond(401, { error: 'unauthorized' });
  assert.equal((await pub.close()).status, 401);
  assert.equal(pub.send('a'), false);
});

test("onError: 'raise': send() and close() throw the error", async () => {
  const { transport, publisher, errors } = setup({ onError: 'raise' });
  const pub = publisher();
  assert.equal(pub.send('a'), true);
  (await transport.request(0)).respond(401, { error: 'unauthorized' });
  await until(() => !pub.alive);
  assert.throws(() => pub.send('b'), (error) => error instanceof IQError && error.status === 401 && error === pub.error);
  await assert.rejects(pub.close(), (error) => error === pub.error);
  await assert.rejects(pub.close('fail'), (error) => error === pub.error);
  assert.equal(errors.length, 1, 'the handler still runs once');
});

test("onError: 'raise' does not turn a clean run into an error", async () => {
  const { transport, publisher } = setup({ onError: 'raise' });
  const pub = publisher();
  pub.send('a');
  const closing = pub.close();
  const request = await transport.request(0);
  await until(() => request.ended);
  request.respond(200, { lastEventId: 1, closed: true, outcome: 'end' });
  assert.equal((await closing).closed, true);
  assert.equal(pub.send('late'), false);
});

test('the queue is bounded by bytes: a server that stops reading fails the stream', async () => {
  const { transport, publisher, errors } = setup({ queueBytes: 1000 });
  const pub = publisher();
  const request = await transport.request(0);
  request.stalled = true;
  transport.onPost = () => closedFail(0);
  const data = 'x'.repeat(289); // each line is 301 bytes
  assert.equal(pub.send(data), true);
  await until(() => request.data.length === 1); // in flight, and never acknowledged
  assert.equal(pub.send(data), true);
  assert.equal(pub.send(data), true);
  assert.equal(pub.alive, true);
  assert.equal(pub.send(data), false, 'a fourth line waiting for the server does not fit in 1000 bytes');
  assert.equal(pub.alive, false);
  assert.equal(pub.error.code, 'queue_full');
  assert.equal(request.destroyed, true, 'the stalled connection is dropped');
  assert.deepEqual(await pub.close(), { status: 0, closed: true, error: 'queue_full', lastEventId: 0, outcome: 'fail', reason: 'publisher' });
  assert.deepEqual(transport.closes, ['fail']);
  assert.equal(errors.length, 1);
  assert.equal(pub.framesSent, 3);
});

test("the queue bound with onError: 'raise' throws from send()", async () => {
  const { transport, publisher } = setup({ queueBytes: 100, onError: 'raise' });
  const pub = publisher();
  (await transport.request(0)).stalled = true;
  pub.send('x'.repeat(50));
  await sleep(10);
  assert.throws(() => pub.send('x'.repeat(50)), (error) => error instanceof IQError && error.code === 'queue_full');
  await assert.rejects(pub.close());
});

test('mistakes the caller can fix are thrown synchronously, even from a publisher that gave up', async () => {
  const { transport, publisher } = setup({ maxFrameBytes: 100 });
  const pub = publisher();
  assert.throws(() => pub.send(42), TypeError);
  assert.throws(() => pub.send('a', 'iq:end'), TypeError);
  assert.throws(() => pub.send('x'.repeat(200)), (error) => error.name === 'FrameTooLarge');
  (await transport.request(0)).respond(401, {});
  await pub.close();
  assert.throws(() => pub.send('x'.repeat(200)), (error) => error.name === 'FrameTooLarge');
  assert.throws(() => pub.send('a', 'bad name'), TypeError);
  assert.equal(pub.framesSent, 0);
});

test('silence is answered with keepalive lines, which are not frames', async () => {
  const { transport, publisher } = setup({ keepalive: 0.05 });
  const pub = publisher();
  pub.send('a');
  const request = await transport.request(0);
  await until(() => request.keepalives >= 3);
  assert.deepEqual(request.data, ['a']);
  assert.equal(pub.framesSent, 1);
  pub.send('b');
  await until(() => request.data.length === 2);
  // A reopened request resends by frame count: keepalives must not have been counted.
  request.respond(408, { lastEventId: 1, closed: false, error: 'read_timeout' });
  const second = await transport.request(1);
  await until(() => second.data.length === 1);
  assert.deepEqual(second.data, ['b']);
  second.respond(200, CLOSED);
  await pub.close();
});

test('keepalive: null writes no empty line', async () => {
  const { transport, publisher } = setup({ keepalive: null });
  const pub = publisher();
  pub.send('a');
  const request = await transport.request(0);
  await sleep(150);
  assert.deepEqual(request.lines, ['{"data":"a"}']);
  request.respond(200, CLOSED);
  await pub.close();
});

test('a write restarts the keepalive clock', async () => {
  const { transport, publisher } = setup({ keepalive: 0.4 });
  const pub = publisher();
  const request = await transport.request(0);
  for (let i = 0; i < 6; i++) {
    pub.send(`f${i}`);
    await sleep(100);
  }
  assert.equal(request.keepalives, 0);
  request.respond(200, CLOSED);
  await pub.close();
});

test('abort() drops the connection without closing the stream', async () => {
  const { transport, publisher, errors } = setup();
  const pub = publisher();
  pub.send('a');
  const request = await transport.request(0);
  await until(() => request.data.length === 1);
  pub.abort();
  assert.equal(request.destroyed, true);
  assert.equal(pub.alive, false);
  assert.equal(pub.send('b'), false);
  assert.deepEqual(await pub.close(), { status: 0, closed: false });
  pub.abort();
  await sleep(30);
  assert.deepEqual(request.closes, []);
  assert.deepEqual(transport.posts, [], 'no probe, no close: the stream is another process\'s now');
  assert.equal(transport.requests.length, 1);
  assert.equal(pub.error, undefined);
  assert.deepEqual(errors, []);
});

test('close() gives up after its timeout, and says the stream failed', async () => {
  const { transport, publisher, errors } = setup();
  const pub = publisher();
  pub.send('a');
  transport.onPost = () => closedFail(1);
  const started = Date.now();
  const result = await pub.close('end', { timeout: 0.3 });
  assert.ok(Date.now() - started >= 250 && Date.now() - started < 1500);
  assert.deepEqual(result, { status: 0, closed: true, error: 'close_timeout', lastEventId: 1, outcome: 'fail', reason: 'publisher' });
  assert.equal(transport.requests[0].destroyed, true);
  assert.deepEqual(transport.closes, ['fail']);
  assert.equal(errors[0].code, 'close_timeout');
});

test('the queue bound counts what waits during a recovery too, and lets go of what was written', async () => {
  const { transport, publisher, errors } = setup({ queueBytes: 1000 });
  const pub = publisher();
  const request = await transport.request(0);
  const data = 'x'.repeat(289);
  for (let i = 0; i < 10; i++) {
    assert.equal(pub.send(data), true, 'a healthy request empties the queue');
    await until(() => request.data.length === i + 1);
  }
  let answerProbe;
  transport.onPost = (body) => (body === '' ? new Promise((resolve) => (answerProbe = resolve)) : closedFail(10));
  request.respond(0);
  await until(() => transport.probes === 1);
  assert.deepEqual([pub.send(data), pub.send(data), pub.send(data), pub.send(data)], [true, true, true, false]);
  assert.equal(errors[0].code, 'queue_full');
  answerProbe(open(10));
  assert.equal((await pub.close()).error, 'queue_full');
  assert.equal(transport.requests.length, 1);
});

test('frameSize() measures without sending', () => {
  const { publisher } = setup();
  const pub = publisher();
  assert.equal(pub.frameSize('Hi'), 13);
  assert.equal(pub.frameSize('Hi', 'token'), 29);
  assert.equal(pub.framesSent, 0);
  assert.equal(pub.maxFrameBytes, 65536);
  pub.abort();
});

test('batch mode: the stream is reserved at once, lines are collected into short requests, and the last one carries the close line', async () => {
  const { transport, publisher, errors } = setup({ mode: 'batch', batchMs: 100 });
  let held = 0;
  transport.onPost = (body) => {
    const lines = body.split('\n').filter(Boolean).map((line) => JSON.parse(line));
    held += lines.filter((line) => 'data' in line).length;
    return lines.some((line) => 'close' in line) ? { status: 200, body: { lastEventId: held, closed: true, outcome: 'end' } } : open(held);
  };
  const pub = publisher();
  await until(() => transport.posts.length === 1);
  assert.deepEqual(transport.posts, [''], 'an empty request takes the stream out of pending');
  pub.send('a');
  pub.send('b');
  await sleep(30);
  pub.send('c');
  await until(() => transport.posts.length === 2);
  assert.equal(transport.posts[1], '{"data":"a"}\n{"data":"b"}\n{"data":"c"}\n', 'one request for the lines of a batch window');
  pub.send('d', 'token');
  const result = await pub.close();
  assert.equal(transport.posts[2], '{"event":"token","data":"d"}\n{"close":"end"}\n');
  assert.deepEqual(result, { status: 200, lastEventId: 4, closed: true, outcome: 'end' });
  assert.equal(pub.requests, 3);
  assert.equal(transport.requests.length, 0, 'no held request');
  assert.deepEqual(errors, []);
});

test('batch mode: a request whose answer was lost is probed, and what the server lacks is sent again', async () => {
  const { transport, publisher, errors } = setup({ mode: 'batch', batchMs: 10 });
  const script = [open(0), { status: 0, body: {} }, open(1), open(3), { status: 200, body: { lastEventId: 3, closed: true, outcome: 'end' } }];
  transport.onPost = () => script.shift();
  const pub = publisher();
  pub.send('a');
  pub.send('b');
  pub.send('c');
  await until(() => transport.posts.length === 4);
  assert.deepEqual(transport.posts, ['', '{"data":"a"}\n{"data":"b"}\n{"data":"c"}\n', '', '{"data":"b"}\n{"data":"c"}\n']);
  assert.deepEqual(await pub.close(), { status: 200, lastEventId: 3, closed: true, outcome: 'end' });
  assert.deepEqual(errors, []);
});

test('batch mode: silence is answered with an empty request', async () => {
  const { transport, publisher } = setup({ mode: 'batch', keepalive: 0.05 });
  const pub = publisher();
  await until(() => transport.posts.length >= 3);
  assert.deepEqual(transport.posts.slice(0, 3), ['', '', '']);
  transport.onPost = () => ({ status: 200, body: CLOSED });
  await pub.close();
});

test('batch mode: a refusal is fatal as in stream mode', async () => {
  const { transport, publisher, errors } = setup({ mode: 'batch', batchMs: 10 });
  transport.onPost = (body) => (body.includes('"data"') ? { status: 413, body: { lastEventId: 0, closed: false, error: 'frame_too_large', line: 1 } } : body === '' ? open(0) : closedFail(0));
  const pub = publisher();
  pub.send('a');
  assert.deepEqual(await pub.close(), { status: 413, lastEventId: 0, closed: true, outcome: 'fail', reason: 'publisher', error: 'frame_too_large' });
  assert.equal(errors[0].code, 'frame_too_large');
});

test('reserve() sends one empty POST; closeStream() sends a close line and ignores a stream that is already closed', async () => {
  const { transport, client } = setup();
  transport.onPost = () => open(0);
  assert.deepEqual(await client.reserve(STREAM), { status: 200, lastEventId: 0, closed: false });
  transport.onPost = () => ({ status: 409, body: { error: 'publisher_active' } });
  assert.deepEqual(await client.reserve(STREAM), { status: 409, closed: false, error: 'publisher_active' }, 'the run has started already');
  assert.deepEqual(transport.posts, ['', '']);

  transport.onPost = () => closedFail(2);
  assert.deepEqual(await client.closeStream(STREAM), { status: 200, lastEventId: 2, closed: true, outcome: 'fail', reason: 'publisher' });
  transport.onPost = () => ({ status: 200, body: { lastEventId: 2, closed: true, outcome: 'end' } });
  assert.equal((await client.closeStream(STREAM, 'end')).outcome, 'end');
  transport.onPost = () => ({ status: 409, body: { error: 'stream_closed', outcome: 'end' } });
  assert.deepEqual(await client.closeStream(STREAM), { status: 409, closed: true, outcome: 'end', error: 'stream_closed' });
  assert.deepEqual(transport.posts.slice(2), ['{"close":"fail"}\n', '{"close":"end"}\n', '{"close":"fail"}\n']);
  await assert.rejects(client.closeStream(STREAM, 'done'), TypeError);

  transport.onPost = () => ({ status: 401, body: { error: 'unauthorized' } });
  await assert.rejects(client.reserve(STREAM), (error) => error instanceof IQError && error.status === 401 && error.code === 'unauthorized' && error.streamId === STREAM);
  await assert.rejects(client.closeStream(STREAM), (error) => error instanceof IQError && error.status === 401);
  transport.onPost = () => ({ status: 409, body: { error: 'stream_closed', outcome: 'end' } });
  await assert.rejects(client.reserve(STREAM), (error) => error.code === 'stream_closed');
});

test('closeStream() waits for a publisher that is still attached, within retryBudget', async () => {
  const { transport, client } = setup({ retryBudget: 1 });
  transport.onPost = (body, n) => (n < 2 ? { status: 409, body: { error: 'publisher_active' } } : closedFail(4));
  assert.equal((await client.closeStream(STREAM)).closed, true);
  assert.equal(transport.posts.length, 3);

  const stuck = setup({ retryBudget: 0.3 });
  stuck.transport.onPost = () => ({ status: 409, body: { error: 'publisher_active' } });
  await assert.rejects(stuck.client.closeStream(STREAM), (error) => error.code === 'publisher_active');
});
