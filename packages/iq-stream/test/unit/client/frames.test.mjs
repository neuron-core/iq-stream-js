// frames(): the subscription as a ReadableStream, which is the form the bridges consume.
import assert from 'node:assert/strict';
import { beforeEach, test } from 'node:test';
import { frames, IQStreamError } from '../../../dist/client/index.js';
import { FakeEventSource } from '../../../dist/client/testing.js';

const URL_ = 'http://iq.test/stream-0123456789abcdef?token=t';
const last = () => FakeEventSource.instances.at(-1);
const open = (options = {}) => frames(URL_, { EventSource: FakeEventSource, ...options });

/** Reads until the stream ends: what it delivered, and the error it ended with, if any. */
async function drain(stream) {
  const values = [];
  try {
    for await (const value of stream) values.push(value);
    return { values };
  } catch (error) {
    return { values, error };
  }
}

beforeEach(() => FakeEventSource.reset());

test('delivers frames and closes on iq:end', async () => {
  const stream = open();
  last().open();
  last().frame('a', { id: '1' });
  last().frame('b', { id: '2' });
  last().terminal('iq:end', { lastEventId: 2 });
  assert.deepEqual(await drain(stream), {
    values: [{ data: 'a', event: 'message', id: '1' }, { data: 'b', event: 'message', id: '2' }],
  });
  assert.equal(last().closeCalls, 1);
});

test('parse turns the data of each frame into the value enqueued', async () => {
  const stream = open({ parse: JSON.parse, events: ['token'] });
  last().frame('{"n":1}', { id: '1' });
  last().frame('{"n":2}', { id: '2', event: 'token' });
  last().terminal('iq:end');
  assert.deepEqual(await drain(stream), { values: [{ n: 1 }, { n: 2 }] });
});

for (const [name, data, expected] of [
  ['iq:fail', { reason: 'publisher_idle', lastEventId: 1 }, { kind: 'fail', reason: 'publisher_idle', lastEventId: 1, message: 'stream failed: publisher_idle' }],
  ['iq:gone', { outcome: 'end', lastEventId: 3 }, { kind: 'gone', outcome: 'end', lastEventId: 3, message: 'stream gone (end)' }],
  ['iq:gone', { outcome: 'fail', reason: 'too_large' }, { kind: 'gone', outcome: 'fail', reason: 'too_large', message: 'stream gone (fail)' }],
]) {
  test(`errors with an IQStreamError of kind ${expected.kind} on ${name} ${JSON.stringify(data)}, after the frames`, async () => {
    const stream = open();
    last().frame('a', { id: '1' });
    last().frame('b', { id: '2' });
    last().terminal(name, data);
    const { values, error } = await drain(stream);
    assert.deepEqual(values.map((frame) => frame.data), ['a', 'b']);   // queued when the stream failed, and still delivered
    assert.ok(error instanceof IQStreamError);
    assert.equal(error.name, 'IQStreamError');
    const { kind, reason, outcome, lastEventId, message } = error;
    assert.deepEqual(JSON.parse(JSON.stringify({ kind, reason, outcome, lastEventId, message })), expected);
  });
}

test('errors with kind refused, and the status when the source knows it', async () => {
  const stream = open();
  last().error('closed', { status: 429, error: 'too_many_subscribers' });
  const { error } = await drain(stream);
  assert.ok(error instanceof IQStreamError);
  assert.deepEqual([error.kind, error.status, error.code], ['refused', 429, 'too_many_subscribers']);
});

test('a failure with nothing queued reaches a reader that is already waiting', async () => {
  const reader = open().getReader();
  const read = reader.read();
  last().terminal('iq:fail', { reason: 'publisher' });
  await assert.rejects(read, (error) => error instanceof IQStreamError && error.reason === 'publisher');
});

test('cancel() closes the source', async () => {
  const stream = open();
  const reader = stream.getReader();
  last().frame('a', { id: '1' });
  assert.deepEqual(await reader.read(), { done: false, value: { data: 'a', event: 'message', id: '1' } });
  await reader.cancel();
  assert.equal(last().closeCalls, 1);
  last().frame('late', { id: '2' });   // must not throw into the source
});

test('an aborted signal errors the stream with its reason and closes the source', async () => {
  const controller = new AbortController();
  const stream = open({ signal: controller.signal });
  last().frame('a', { id: '1' });
  const reason = new Error('the user left');
  controller.abort(reason);
  const { values, error } = await drain(stream);
  assert.equal(values.length, 0);   // an errored stream drops what was not read
  assert.equal(error, reason);
  assert.equal(last().closeCalls, 1);
});

test('a signal that is already aborted opens no source', async () => {
  const stream = open({ signal: AbortSignal.abort(new Error('too late')) });
  const { error } = await drain(stream);
  assert.equal(error.message, 'too late');
  assert.equal(FakeEventSource.instances.length, 0);
});

test('a parse that throws errors the stream, after the frames before it, and closes the subscription', async () => {
  const stream = open({ parse: JSON.parse });
  last().frame('{"n":1}', { id: '1' });
  last().frame('not json', { id: '2' });
  last().frame('{"n":3}', { id: '3' });
  const { values, error } = await drain(stream);
  assert.deepEqual(values, [{ n: 1 }]);
  assert.ok(error instanceof SyntaxError);
  assert.equal(last().closeCalls, 1);
});

test('retries are those of subscribe', async () => {
  const stream = open({ retry: { attempts: 1, delayMs: 1 } });
  last().frame('a', { id: '1' });
  last().error('closed');
  const reader = stream.getReader();
  assert.equal((await reader.read()).value.data, 'a');
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(FakeEventSource.instances.length, 2);
  assert.match(last().url, /[?&]lastEventId=1$/);
  last().terminal('iq:end');
  assert.deepEqual(await reader.read(), { done: true, value: undefined });
});
