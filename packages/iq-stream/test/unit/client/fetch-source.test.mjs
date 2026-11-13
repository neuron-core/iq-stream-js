// The fetch() source against a fake fetch that answers with a Response over a ReadableStream.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { subscribe } from '../../../dist/client/index.js';
import { fetchSource } from '../../../dist/fetch/index.js';

const URL_ = 'http://iq.test/stream-0123456789abcdef';
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const encoder = new TextEncoder();

/** A 200 answer whose body is the chunks (strings or bytes), then stays open unless `end` is set. */
function stream(chunks, { end = false } = {}) {
  const body = new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(typeof chunk === 'string' ? encoder.encode(chunk) : chunk);
      if (end) controller.close();
    },
  });
  return new Response(body, { status: 200, headers: { 'Content-Type': 'text/event-stream' } });
}
const refusal = (status, error) => new Response(error ? JSON.stringify({ error }) : null, { status });

/** A fetch that gives the scripted answers in turn (a function is called, an Error is thrown) and records its calls. */
function fakeFetch(...answers) {
  const calls = [];
  const fetch = async (url, init) => {
    calls.push({ url: String(url), headers: { ...init.headers }, signal: init.signal, at: performance.now() });
    const answer = answers[Math.min(calls.length, answers.length) - 1];
    if (answer instanceof Error) throw answer;
    return typeof answer === 'function' ? answer() : answer;
  };
  return { fetch, calls };
}

/** Opens a source and records every event with the readyState at that moment. */
function listen(fetch, options = {}) {
  const events = [];
  const source = fetchSource(URL_, { fetch, ...options });
  let wake;
  const record = (kind) => (event) => { events.push({ kind, state: source.readyState, ...event }); wake?.(); };
  source.addEventListener('*', record('frame'));
  source.addEventListener('open', record('open'));
  source.addEventListener('error', record('error'));
  /** Waits until `count` events of a kind were recorded. */
  const until = async (kind, count = 1) => {
    while (events.filter((event) => event.kind === kind).length < count) await new Promise((resolve) => { wake = resolve; });
  };
  return { source, events, until };
}

const DONE = 'event: done\ndata: .\n\n';
const TABLE = [
  ['an unnamed event', ['data: hello\n\n'], [['message', 'hello', '']]],
  ['an id and a name', ['id: 7\nevent: token\ndata: x\n\n'], [['token', 'x', '7']]],
  ['comments, as the heartbeat', [': ping\n\n', 'data: a\n\n', ': ping\n\n'], [['message', 'a', '']]],
  ['a retry line', ['retry: 500\n\ndata: a\n\n'], [['message', 'a', '']]],
  ['multi-line data', ['data: a\ndata: b\ndata:\ndata: c\n\n'], [['message', 'a\nb\n\nc', '']]],
  ['CR LF line ends', ['id: 1\r\ndata: a\r\n\r\ndata: b\r\n\r\n'], [['message', 'a', '1'], ['message', 'b', '1']]],
  ['CR line ends', ['data: a\r\rdata: b\r\r'], [['message', 'a', ''], ['message', 'b', '']]],
  ['a chunk boundary inside a line', ['da', 'ta: he', 'llo\n', '\n'], [['message', 'hello', '']]],
  ['a chunk boundary between CR and LF', ['data: a\r', '\ndata: b\r\n\r', '\n'], [['message', 'a\nb', '']]],
  ['a chunk boundary inside a character', [encoder.encode('data: café\n\n').slice(0, 10), encoder.encode('data: café\n\n').slice(10)], [['message', 'café', '']]],
  ['an event without data', ['event: token\n\ndata: a\n\n'], [['message', 'a', '']]],
  ['an event without data that has an id', ['id: 5\n\ndata: a\n\n'], [['message', 'a', '5']]],
  ['an id that stays for the events after it', ['id: 1\ndata: a\n\ndata: b\n\nid: 2\ndata: c\n\n'], [['message', 'a', '1'], ['message', 'b', '1'], ['message', 'c', '2']]],
  ['an id with a NUL is ignored', ['id: 1\ndata: a\n\nid: 2\0\ndata: b\n\n'], [['message', 'a', '1'], ['message', 'b', '1']]],
  ['no space, and a second space', ['data:a\n\ndata:  b\n\n'], [['message', 'a', ''], ['message', ' b', '']]],
  ['empty data', ['data:\n\ndata\n\n'], [['message', '', ''], ['message', '', '']]],
  ['a colon in the data', ['data: {"a":"b: c"}\n\n'], [['message', '{"a":"b: c"}', '']]],
  ['an unknown field', ['foo: bar\ndata: a\n\n'], [['message', 'a', '']]],
  ['a terminal event', ['id: 3\ndata: a\n\nevent: iq:end\nid: e\ndata: {"lastEventId":3}\n\n'], [['message', 'a', '3'], ['iq:end', '{"lastEventId":3}', 'e']]],
  ['many events in one chunk', [Array.from({ length: 50 }, (_, i) => `id: ${i + 1}\ndata: f${i + 1}\n\n`).join('')],
    Array.from({ length: 50 }, (_, i) => ['message', `f${i + 1}`, String(i + 1)])],
];

for (const [name, chunks, expected] of TABLE) {
  test(`parser: ${name}`, async () => {
    const { fetch } = fakeFetch(stream([...chunks, DONE]));
    const { source, events, until } = listen(fetch);
    await until('frame', expected.length + 1);
    source.close();
    assert.deepEqual(events.filter((event) => event.kind === 'frame').slice(0, -1).map((event) => [event.type, event.data, event.lastEventId]), expected);
    assert.deepEqual(events.map((event) => event.kind).slice(0, 1), ['open']);
  });
}

test('an event the stream ends in the middle of is not delivered', async () => {
  const { fetch } = fakeFetch(stream(['data: a\n\ndata: half\n'], { end: true }), stream([DONE]));
  const { source, events, until } = listen(fetch, { lastEventId: undefined });
  await until('error');
  source.close();
  assert.deepEqual(events.filter((event) => event.kind === 'frame').map((event) => event.data), ['a']);
});

test('events reach the listeners of their name too, as with EventSource', async () => {
  const { fetch } = fakeFetch(stream(['data: a\n\nevent: token\ndata: b\n\n']));
  const source = fetchSource(URL_, { fetch });
  const got = [];
  await new Promise((resolve) => {
    source.addEventListener('message', (event) => got.push(`message:${event.data}`));
    source.addEventListener('token', (event) => { got.push(`token:${event.data}`); resolve(); });
  });
  source.close();
  assert.deepEqual(got, ['message:a', 'token:b']);
  assert.equal(source.wildcard, true);
});

test('the request: Accept, the token in the Authorization header, the id to resume from', async () => {
  const { fetch, calls } = fakeFetch(stream([DONE]));
  const first = listen(fetch, { token: 'sub-token', lastEventId: '41' });
  await first.until('frame');
  first.source.close();
  assert.equal(calls[0].url, URL_);
  assert.deepEqual(calls[0].headers, { Accept: 'text/event-stream', Authorization: 'Bearer sub-token', 'Last-Event-ID': '41' });

  const bare = fakeFetch(stream([DONE]));
  const second = listen(bare.fetch);
  await second.until('frame');
  second.source.close();
  assert.deepEqual(bare.calls[0].headers, { Accept: 'text/event-stream' });
});

for (const [status, code] of [[401, 'invalid_token'], [403, 'invalid_token'], [429, 'too_many_subscribers'], [204, undefined]]) {
  test(`a ${status} closes the source and the error event carries the status${code ? ' and the code' : ''}`, async () => {
    const { fetch, calls } = fakeFetch(refusal(status, code));
    const { source, events, until } = listen(fetch);
    await until('error');
    await sleep(20);
    assert.deepEqual(events, [{ kind: 'error', state: 2, type: 'error', status, ...(code ? { error: code } : {}) }]);
    assert.equal(source.readyState, 2);
    assert.equal(calls.length, 1);
  });
}

test('a 503 is tried again with a growing delay, and the source stays CONNECTING meanwhile', async () => {
  const { fetch, calls } = fakeFetch(refusal(503, 'server_unavailable'), refusal(503), stream(['data: a\n\n']));
  const { source, events, until } = listen(fetch, { unavailable: { attempts: 2, delayMs: 60 } });
  await until('frame');
  source.close();
  assert.deepEqual(events.map((event) => [event.kind, event.state]), [['error', 0], ['error', 0], ['open', 1], ['frame', 1]]);
  assert.equal(events[0].status, undefined);
  // 60 ms then 120 ms, each shortened by up to half: jitter keeps subscribers from coming back together.
  const [first, second] = [calls[1].at - calls[0].at, calls[2].at - calls[1].at];
  assert.ok(first >= 28 && first < 260, `first delay ${first}`);
  assert.ok(second >= 58 && second < 320, `second delay ${second}`);
});

test('a 503 that lasts closes the source with its status', async () => {
  const { fetch, calls } = fakeFetch(() => refusal(503, 'server_unavailable'));
  const { source, events, until } = listen(fetch, { unavailable: { attempts: 2, delayMs: 5 } });
  await until('error', 3);
  await sleep(40);
  assert.equal(calls.length, 3);
  assert.deepEqual(events.at(-1), { kind: 'error', state: 2, type: 'error', status: 503, error: 'server_unavailable' });
  assert.equal(source.readyState, 2);
});

test('after a stream that ends it resumes with the Last-Event-ID header, after the server\'s retry interval', async () => {
  const { fetch, calls } = fakeFetch(stream(['retry: 40\n\n', 'id: 1\ndata: a\n\nid: 2\ndata: b\n\n'], { end: true }), stream(['id: 3\ndata: c\n\n']));
  const { source, events, until } = listen(fetch, { token: 't' });
  await until('frame', 3);
  source.close();
  assert.deepEqual(events.map((event) => [event.kind, event.state, event.data]),
    [['open', 1, undefined], ['frame', 1, 'a'], ['frame', 1, 'b'], ['error', 0, undefined], ['open', 1, undefined], ['frame', 1, 'c']]);
  assert.equal(calls[0].headers['Last-Event-ID'], undefined);
  assert.equal(calls[1].headers['Last-Event-ID'], '2');
  assert.equal(calls[1].headers.Authorization, 'Bearer t');
  const waited = calls[1].at - calls[0].at;
  assert.ok(waited >= 38 && waited < 1000, `waited ${waited} ms (2000 without the retry line)`);
});

test('a network error is not a refusal: the source stays CONNECTING and tries again', async () => {
  const { fetch, calls } = fakeFetch(stream(['retry: 20\n\nid: 1\ndata: a\n\n'], { end: true }), new TypeError('fetch failed'), stream(['id: 2\ndata: b\n\n']));
  const { source, events, until } = listen(fetch);
  await until('frame', 2);
  source.close();
  assert.deepEqual(events.map((event) => [event.kind, event.state]), [['open', 1], ['frame', 1], ['error', 0], ['error', 0], ['open', 1], ['frame', 1]]);
  assert.equal(calls[2].headers['Last-Event-ID'], '1');
});

test('close() aborts the request and stops the source', async () => {
  const { fetch, calls } = fakeFetch(stream(['retry: 10\n\ndata: a\n\n']));
  const { source, events, until } = listen(fetch);
  await until('frame');
  source.close();
  assert.equal(source.readyState, 2);
  assert.equal(calls[0].signal.aborted, true);
  await sleep(50);
  assert.equal(calls.length, 1);
  assert.deepEqual(events.map((event) => event.kind), ['open', 'frame']);
});

test('a line longer than the cap closes the source', async () => {
  const { fetch, calls } = fakeFetch(stream(['data: ok\n\n', `data: ${'x'.repeat(40)}`, 'x'.repeat(40)]));
  const { source, events, until } = listen(fetch, { maxLineLength: 64 });
  await until('error');
  await sleep(20);
  assert.deepEqual(events.map((event) => [event.kind, event.state]), [['open', 1], ['frame', 1], ['error', 2]]);
  assert.equal(events.at(-1).status, undefined);
  assert.equal(source.readyState, 2);
  assert.equal(calls[0].signal.aborted, true);
  assert.equal(calls.length, 1);
});

test('lines up to the cap pass, whatever the size of the chunk they come in', async () => {
  const line = `data: ${'x'.repeat(58)}\n\n`;   // 64 characters before the line end
  const { fetch } = fakeFetch(stream([line.repeat(20), DONE]));
  const { source, events, until } = listen(fetch, { maxLineLength: 64 });
  await until('frame', 21);
  source.close();
  assert.equal(events.filter((event) => event.kind === 'error').length, 0);
});

test('with subscribe: every event name is delivered without a list, and the terminal event closes the request', async () => {
  const { fetch, calls } = fakeFetch(stream(['id: 1\ndata: a\n\nid: 2\nevent: token\ndata: b\n\nevent: iq:end\nid: x\ndata: {"lastEventId":2}\n\nid: 3\ndata: late\n\n']));
  const frames = [];
  const sub = subscribe((resume) => fetchSource(URL_, { fetch, lastEventId: resume.lastEventId }), { onFrame: (frame) => frames.push(frame) });
  await sub.opened;
  assert.deepEqual(await sub.done, { type: 'end', lastEventId: 2 });
  assert.deepEqual(frames, [{ data: 'a', event: 'message', id: '1' }, { data: 'b', event: 'token', id: '2' }]);
  assert.equal(sub.lastEventId, '2');
  assert.equal(calls[0].signal.aborted, true);
  await sleep(20);
  assert.equal(calls.length, 1);
});

test('with subscribe: a 401 is a refusal with its status, and is not retried', async () => {
  const { fetch, calls } = fakeFetch(refusal(401, 'invalid_token'));
  const sub = subscribe(() => fetchSource(URL_, { fetch }), { onFrame() {} }, { retry: { attempts: 2, delayMs: 5 } });
  assert.deepEqual(await sub.done, { type: 'refused', status: 401, code: 'invalid_token' });
  await sleep(30);
  assert.equal(calls.length, 1);
});

test('with subscribe: a refusal that may pass is retried, and the new source resumes after the last frame', async () => {
  const { fetch, calls } = fakeFetch(stream(['retry: 5\n\nid: 1\ndata: a\n\n'], { end: true }), refusal(429, 'too_many_subscribers'), stream(['id: 2\ndata: b\n\nevent: iq:end\ndata: {}\n\n']));
  const frames = [];
  const sub = subscribe((resume) => fetchSource(URL_, { fetch, lastEventId: resume.lastEventId }), { onFrame: (frame) => frames.push(frame.data) }, { retry: { attempts: 1, delayMs: 5 } });
  assert.deepEqual(await sub.done, { type: 'end' });
  assert.deepEqual(frames, ['a', 'b']);
  assert.deepEqual(calls.map((call) => call.headers['Last-Event-ID']), [undefined, '1', '1']);
});
