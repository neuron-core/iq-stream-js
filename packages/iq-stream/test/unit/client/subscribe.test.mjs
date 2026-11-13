// The transport client against a source driven by hand: no network, no browser.
import assert from 'node:assert/strict';
import { beforeEach, test } from 'node:test';
import { BUILD, IQStreamError, subscribe } from '../../../dist/client/index.js';
import { FakeEventSource } from '../../../dist/client/testing.js';
import { IQError } from '../../../dist/shared/errors.js';

const URL_ = 'http://iq.test/stream-0123456789abcdef?token=t';
const flush = () => new Promise((resolve) => setImmediate(resolve));
const last = () => FakeEventSource.instances.at(-1);

/** A subscription on FakeEventSource that records what its handlers were given. */
function open(options = {}, target = URL_) {
  const frames = [], closes = [];
  const sub = subscribe(target, { onFrame: (frame) => frames.push(frame), onClose: (outcome) => closes.push(outcome) },
    { EventSource: FakeEventSource, ...options });
  return { sub, frames, closes, source: last() };
}

beforeEach(() => FakeEventSource.reset());

test('the build marker is exported', () => {
  assert.equal(BUILD, 'iq-stream/client');
});

test('frames are delivered in order with their ids, and lastEventId follows the last frame', () => {
  const { sub, frames, source } = open();
  assert.equal(source.url, URL_);
  assert.equal(sub.lastEventId, undefined);
  source.open();
  source.frame('a', { id: '1' });
  source.frame('b', { id: '2' });
  assert.equal(sub.lastEventId, '2');
  source.frame('c', { id: 'opaque-3' });
  assert.deepEqual(frames, [
    { data: 'a', event: 'message', id: '1' },
    { data: 'b', event: 'message', id: '2' },
    { data: 'c', event: 'message', id: 'opaque-3' },
  ]);
  assert.equal(sub.lastEventId, 'opaque-3');
  sub.close();
});

for (const [name, data, expected] of [
  ['iq:end', { lastEventId: 2 }, { type: 'end', lastEventId: 2 }],
  ['iq:fail', { reason: 'max_duration', lastEventId: 2 }, { type: 'fail', reason: 'max_duration', lastEventId: 2 }],
  ['iq:gone', { outcome: 'fail', reason: 'publisher', lastEventId: 9 }, { type: 'gone', outcome: 'fail', reason: 'publisher', lastEventId: 9 }],
  ['iq:gone', { outcome: 'unknown' }, { type: 'gone', outcome: 'unknown' }],
]) {
  test(`${name} ${JSON.stringify(data)} closes the source once and ends the subscription with the matching outcome`, async () => {
    const { sub, frames, closes, source } = open();
    source.open();
    source.frame('a', { id: '1' });
    source.terminal(name, data);
    assert.equal(source.closeCalls, 1);
    assert.deepEqual(await sub.done, expected);
    // What a source might still deliver after the terminal event changes nothing.
    source.terminal('iq:fail', { reason: 'publisher' });
    source.terminal('iq:end');
    source.frame('late', { id: '2' });
    source.error('closed');
    sub.close();
    await flush();
    assert.deepEqual(closes, [expected]);
    assert.equal(frames.length, 1);
    assert.equal(source.closeCalls, 1);
    assert.equal(sub.lastEventId, '1');
  });
}

test('members of a terminal event that the client does not know are kept on the outcome', async () => {
  const { sub, source } = open();
  source.terminal('iq:end', { lastEventId: 4, region: 'eu', type: 'not-the-outcome-type' });
  assert.deepEqual(await sub.done, { type: 'end', lastEventId: 4, region: 'eu' });
});

test('a fail reason the client does not know is a failure with that reason', async () => {
  const { sub, source } = open();
  source.terminal('iq:fail', { reason: 'cosmic_rays', lastEventId: 1 });
  assert.deepEqual(await sub.done, { type: 'fail', reason: 'cosmic_rays', lastEventId: 1 });
});

for (const [name, data, expected] of [
  ['iq:end', 'not json', { type: 'end' }],
  ['iq:fail', 'not json', { type: 'fail', reason: 'unknown' }],
  ['iq:fail', '[1,2]', { type: 'fail', reason: 'unknown' }],
  ['iq:fail', '{"reason":7,"lastEventId":"x"}', { type: 'fail', reason: 'unknown' }],
  ['iq:gone', '', { type: 'gone', outcome: 'unknown' }],
  ['iq:gone', '{"outcome":"vanished"}', { type: 'gone', outcome: 'unknown' }],
]) {
  test(`${name} with data ${JSON.stringify(data)} still ends the subscription`, async () => {
    const { sub, source } = open();
    source.terminal(name, data);
    assert.deepEqual(await sub.done, expected);
    assert.equal(source.closeCalls, 1);
  });
}

test('iq: events the client does not know are not listened to', () => {
  const { sub, frames, closes, source } = open();
  assert.equal(source.listenerCount('iq:progress'), 0);
  source.emit('iq:progress', { data: '{}' });
  assert.deepEqual([frames, closes], [[], []]);
  sub.close();
});

test('an error while the source is CONNECTING changes nothing', async () => {
  const { sub, frames, closes, source } = open();
  source.open();
  source.frame('a', { id: '1' });
  source.error('connecting');
  source.error('connecting');
  await flush();
  assert.deepEqual(closes, []);
  assert.equal(source.closeCalls, 0);
  assert.equal(FakeEventSource.instances.length, 1);
  source.open();
  source.frame('b', { id: '2' });
  source.terminal('iq:end');
  assert.deepEqual(frames.map((frame) => frame.data), ['a', 'b']);
  assert.deepEqual(await sub.done, { type: 'end' });
});

test('an error when the source is CLOSED is a refusal', async () => {
  const { sub, closes, source } = open();
  source.error('closed');
  assert.deepEqual(await sub.done, { type: 'refused' });
  assert.deepEqual(closes, [{ type: 'refused' }]);
  assert.equal(FakeEventSource.instances.length, 1);
});

test('a refusal carries the status and the error code when the source knows them', async () => {
  const first = open();
  first.source.error('closed', { status: 429, error: 'too_many_subscribers' });
  assert.deepEqual(await first.sub.done, { type: 'refused', status: 429, code: 'too_many_subscribers' });
  // The `eventsource` package for Node reports the status as `code`.
  const second = open();
  second.source.readyState = FakeEventSource.CLOSED;
  second.source.emit('error', { code: 503 });
  assert.deepEqual(await second.sub.done, { type: 'refused', status: 503 });
});

test('retry: reopens with growing delays, asks refresh before each, resumes with lastEventId, then gives up', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let refreshed = 0;
  const { sub, frames, closes, source } = open({
    retry: { attempts: 2, delayMs: 100, refresh: async () => `http://iq.test/stream-0123456789abcdef?token=fresh${++refreshed}` },
  });
  source.open();
  source.frame('a', { id: '7' });
  source.error('closed');
  assert.equal(source.closeCalls, 0);

  t.mock.timers.tick(99);
  await flush();
  assert.deepEqual([refreshed, FakeEventSource.instances.length], [0, 1]);
  t.mock.timers.tick(1);
  await flush();
  assert.deepEqual([refreshed, FakeEventSource.instances.length], [1, 2]);
  assert.equal(last().url, 'http://iq.test/stream-0123456789abcdef?token=fresh1&lastEventId=7');

  // Events of the source that was given up are ignored.
  source.frame('stale', { id: '99' });
  source.terminal('iq:end');
  source.error('closed');
  assert.equal(frames.length, 1);

  last().error('closed');
  t.mock.timers.tick(199);
  await flush();
  assert.equal(FakeEventSource.instances.length, 2);
  t.mock.timers.tick(1);
  await flush();
  assert.deepEqual([refreshed, FakeEventSource.instances.length], [2, 3]);
  assert.equal(last().url, 'http://iq.test/stream-0123456789abcdef?token=fresh2&lastEventId=7');

  last().error('closed');
  assert.deepEqual(await sub.done, { type: 'refused' });
  assert.deepEqual(closes, [{ type: 'refused' }]);
  t.mock.timers.tick(10_000);
  await flush();
  assert.equal(FakeEventSource.instances.length, 3);
});

test('retry: a refused source that reports its error twice is retried once', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const { sub, source } = open({ retry: { attempts: 3, delayMs: 100 } });
  source.error('closed');
  source.error('closed');
  t.mock.timers.tick(100);
  await flush();
  t.mock.timers.tick(1000);
  await flush();
  assert.equal(FakeEventSource.instances.length, 2);
  sub.close();
});

test('retry: a frame resets the count', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const { sub, frames, source } = open({ retry: { attempts: 1, delayMs: 100 } });
  source.error('closed');
  t.mock.timers.tick(100);
  await flush();
  assert.equal(FakeEventSource.instances.length, 2);
  assert.equal(last().url, URL_);
  last().open();
  last().frame('a', { id: '1' });
  last().error('closed');
  // The first delay again, not the doubled one, and the attempt is allowed again.
  t.mock.timers.tick(100);
  await flush();
  assert.equal(FakeEventSource.instances.length, 3);
  assert.equal(last().url, `${URL_}&lastEventId=1`);
  last().frame('b', { id: '2' });
  last().terminal('iq:end', { lastEventId: 2 });
  assert.deepEqual(await sub.done, { type: 'end', lastEventId: 2 });
  assert.deepEqual(frames.map((frame) => frame.data), ['a', 'b']);
});

test('retry: without refresh a 401 or 403 is final, other statuses are tried again', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  for (const status of [401, 403]) {
    const { sub, source } = open({ retry: { attempts: 2, delayMs: 100 } });
    source.error('closed', { status, error: 'invalid_token' });
    assert.deepEqual(await sub.done, { type: 'refused', status, code: 'invalid_token' });
  }
  const before = FakeEventSource.instances.length;
  const { sub, source } = open({ retry: { attempts: 2, delayMs: 100 } });
  source.error('closed', { status: 503 });
  t.mock.timers.tick(100);
  await flush();
  assert.equal(FakeEventSource.instances.length, before + 2);
  sub.close();
});

test('retry: with refresh a 401 is tried again, since refresh can bring a new token', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const { sub, source } = open({ retry: { attempts: 1, delayMs: 100, refresh: async () => 'http://iq.test/s?token=new' } });
  source.error('closed', { status: 401 });
  t.mock.timers.tick(100);
  await flush();
  assert.equal(last().url, 'http://iq.test/s?token=new');
  last().error('closed', { status: 401 });
  assert.deepEqual(await sub.done, { type: 'refused', status: 401 });
});

test('retry: a refresh that fails ends the subscription as refused', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const { sub, source } = open({ retry: { attempts: 3, delayMs: 100, refresh: async () => { throw new Error('offline'); } } });
  source.error('closed', { status: 503 });
  t.mock.timers.tick(100);
  assert.deepEqual(await sub.done, { type: 'refused', status: 503 });
  assert.equal(FakeEventSource.instances.length, 1);
});

test('a source function is called again to reopen, with the last id and what refresh returned', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const calls = [];
  const { sub } = open({ retry: { attempts: 1, delayMs: 100, refresh: async () => 'fresh' } }, (resume) => {
    calls.push({ ...resume });
    return new FakeEventSource('made-by-the-application');
  });
  last().frame('a', { id: '5' });
  last().error('closed');
  t.mock.timers.tick(100);
  await flush();
  assert.deepEqual(calls, [{ lastEventId: undefined, url: undefined }, { lastEventId: '5', url: 'fresh' }]);
  sub.close();
});

test('close() ends the subscription as closed and closes the source', async () => {
  const { sub, frames, closes, source } = open();
  source.open();
  sub.close();
  sub.close();
  assert.deepEqual(await sub.done, { type: 'closed' });
  assert.deepEqual(closes, [{ type: 'closed' }]);
  assert.equal(source.closeCalls, 1);
  source.frame('late', { id: '1' });
  assert.deepEqual(frames, []);
});

test('close() stops a pending retry timer', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let refreshed = 0;
  const { sub, source } = open({ retry: { attempts: 2, delayMs: 100, refresh: async () => { refreshed++; return undefined; } } });
  source.error('closed');
  sub.close();
  assert.deepEqual(await sub.done, { type: 'closed' });
  t.mock.timers.tick(1000);
  await flush();
  assert.deepEqual([refreshed, FakeEventSource.instances.length], [0, 1]);
});

test('close() during refresh opens no source', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let release;
  const { sub, source } = open({ retry: { attempts: 2, delayMs: 100, refresh: () => new Promise((resolve) => { release = resolve; }) } });
  source.error('closed');
  t.mock.timers.tick(100);
  sub.close();
  release('http://iq.test/s?token=new');
  await flush();
  assert.equal(FakeEventSource.instances.length, 1);
});

test('events: a listener is registered for each name given, and for no other', () => {
  const plain = open();
  assert.equal(plain.source.listenerCount('message'), 1);
  assert.equal(plain.source.listenerCount('token'), 0);
  plain.source.frame('named', { id: '1', event: 'token' });
  assert.deepEqual(plain.frames, []);
  plain.sub.close();

  const named = open({ events: ['token', 'usage'] });
  assert.equal(named.source.listenerCount('token'), 1);
  assert.equal(named.source.listenerCount('usage'), 1);
  named.source.frame('plain', { id: '1' });
  named.source.frame('named', { id: '2', event: 'token' });
  assert.deepEqual(named.frames, [{ data: 'plain', event: 'message', id: '1' }, { data: 'named', event: 'token', id: '2' }]);
  named.sub.close();
});

test('a source that delivers every name needs no list of events', async () => {
  const wildcard = () => Object.assign(new FakeEventSource(), { wildcard: true });
  const { sub, frames } = open({}, wildcard);
  const source = last();
  assert.equal(source.listenerCount('*'), 1);
  assert.equal(source.listenerCount('message'), 0);
  source.frame('plain', { id: '1' });
  source.frame('named', { id: '2', event: 'token' });
  source.frame('named too', { id: '3', event: 'error' });
  source.terminal('iq:end', { lastEventId: 3 });
  assert.deepEqual(frames, [
    { data: 'plain', event: 'message', id: '1' },
    { data: 'named', event: 'token', id: '2' },
    { data: 'named too', event: 'error', id: '3' },
  ]);
  assert.deepEqual(await sub.done, { type: 'end', lastEventId: 3 });
});

test('an exception in onFrame closes the subscription and propagates', async () => {
  const closes = [];
  const sub = subscribe(URL_, { onFrame() { throw new Error('bad frame'); }, onClose: (outcome) => closes.push(outcome) }, { EventSource: FakeEventSource });
  assert.throws(() => last().frame('a', { id: '1' }), /bad frame/);
  assert.deepEqual(await sub.done, { type: 'closed' });
  assert.deepEqual(closes, [{ type: 'closed' }]);
  assert.equal(last().closeCalls, 1);
});

test('opened resolves at the first open event, before done', async () => {
  const { sub, source } = open();
  let opened = false;
  sub.opened.then(() => { opened = true; });
  await flush();
  assert.equal(opened, false);
  source.open();
  await sub.opened;
  source.error('connecting');
  source.open();
  source.terminal('iq:end');
  await sub.opened;
  await sub.done;
});

test('opened rejects when the subscription is refused or closed before the source opened', async () => {
  const refused = open();
  refused.source.error('closed', { status: 403 });
  await assert.rejects(refused.sub.opened, (error) => error instanceof IQStreamError && error instanceof IQError
    && error.kind === 'refused' && error.status === 403 && error.message === 'subscription refused');

  const closed = open();
  closed.sub.close();
  await assert.rejects(closed.sub.opened, (error) => error instanceof IQStreamError && error.kind === 'closed');
});

test('a subscription that nobody watches leaves no unhandled rejection', async () => {
  const { source } = open();
  source.error('closed');
  await flush();   // an unhandled rejection would fail the test run here
});

test('without an EventSource the error says what to pass', (t) => {
  if (globalThis.EventSource) t.mock.property(globalThis, 'EventSource', undefined);
  assert.throws(() => subscribe(URL_, { onFrame() {} }), (error) => error instanceof IQError && /options\.EventSource/.test(error.message));
});

test('the subscribe URL may be a URL object', () => {
  const { sub, source } = open({}, new URL(URL_));
  assert.equal(source.url, URL_);
  sub.close();
});
