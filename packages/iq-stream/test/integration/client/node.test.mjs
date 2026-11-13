// The client outside a browser, against the real servers: with the `eventsource` package in place of the
// browser's EventSource, and with the fetch source over Node's own fetch.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { EventSource } from 'eventsource';
import { frames, IQStreamError, subscribe } from '../../../dist/client/index.js';
import { fetchSource } from '../../../dist/fetch/index.js';
import { mint, newStreamId, publish, subscribeUrl } from '../../browser/support/backend.mjs';

const letters = (count, pace, close = 'end') => [...Array.from({ length: count }, (_, i) => [pace, `f${i + 1}`]), [pace, { close }]];
const bare = (streamId) => subscribeUrl(streamId).split('?')[0];

test('EventSource of the eventsource package: frames, then end', { timeout: 30_000 }, async () => {
  const run = publish(newStreamId(), letters(5, 30));
  const got = [];
  const sub = subscribe(subscribeUrl(run.streamId), { onFrame: (frame) => got.push(frame.data) }, { EventSource });
  await sub.opened;
  assert.deepEqual(await sub.done, { type: 'end', lastEventId: 5 });
  assert.deepEqual(got, ['f1', 'f2', 'f3', 'f4', 'f5']);
  assert.equal(sub.lastEventId, '5');
});

test('EventSource of the eventsource package: it knows the status of a refusal', { timeout: 30_000 }, async () => {
  const sub = subscribe(`${subscribeUrl(newStreamId()).slice(0, -4)}AAAA`, { onFrame() {} }, { EventSource });
  assert.deepEqual(await sub.done, { type: 'refused', status: 401 });
  await assert.rejects(sub.opened, (error) => error instanceof IQStreamError && error.status === 401);
});

test('fetch source: the token in a header, named frames without a list, fail with its reason', { timeout: 30_000 }, async () => {
  const run = publish(newStreamId(), [[20, 'plain'], [20, 'named', 'token'], [20, { close: 'fail' }]]);
  const got = [];
  const sub = subscribe(() => fetchSource(bare(run.streamId), { token: mint('sub', run.streamId) }), { onFrame: (frame) => got.push(`${frame.event}:${frame.data}`) });
  assert.deepEqual(await sub.done, { type: 'fail', reason: 'publisher', lastEventId: 2 });
  assert.deepEqual(got, ['message:plain', 'token:named']);
});

test('fetch source: a subscriber that comes back with the last id of a closed stream gets its terminal event', { timeout: 30_000 }, async () => {
  const run = publish(newStreamId(), letters(3, 10));
  const first = subscribe(() => fetchSource(subscribeUrl(run.streamId)), { onFrame() {} });
  await first.done;
  const stream = frames(() => fetchSource(subscribeUrl(run.streamId), { lastEventId: first.lastEventId }));
  assert.deepEqual(await stream.getReader().read(), { done: true, value: undefined });
});

test('ten subscribers on one stream, then an eleventh: refused with 429 too_many_subscribers', { timeout: 30_000 }, async () => {
  const streamId = newStreamId();
  const open = () => subscribe(() => fetchSource(bare(streamId), { token: mint('sub', streamId) }), { onFrame() {} });
  const subscribers = Array.from({ length: 10 }, open);
  try {
    await Promise.all(subscribers.map((sub) => sub.opened));
    const eleventh = open();
    assert.deepEqual(await eleventh.done, { type: 'refused', status: 429, code: 'too_many_subscribers' });
  } finally {
    for (const sub of subscribers) sub.close();
  }
  // The stream never had a publisher: leave none of it behind for the tests that follow.
  await publish(streamId, [[0, { close: 'end' }]]).done;
});
