// The transport client in Chromium, with the browser's own EventSource and fetch, against the two real
// IQ-Stream servers. The page imports the built package files as they are, served under /dist/.
import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { startBrowser } from './support/browser.mjs';

let env;
before(async () => { env = await startBrowser(); });
after(async () => { await env?.close(); });

const letters = (count) => Array.from({ length: count }, (_, i) => `f${i + 1}`);

/** Runs one scenario in the page and returns what the page saw. */
const run = (job) => env.page.evaluate(async (job) => {
  const { subscribe, frames } = await import('/dist/client/index.js');
  const { fetchSource } = await import('/dist/fetch/index.js');
  const api = async (path) => (await fetch(path, { method: 'POST' })).json();
  const started = performance.now(), got = [], notes = [];
  const result = (more) => ({ frames: got, notes, ms: Math.round(performance.now() - started), ...more });

  const ticket = await api(job.publish);
  let target = job.badToken ? `${ticket.url.slice(0, -4)}AAAA` : ticket.url;
  if (job.ticketTtl) target = (await api(`/api/ticket?id=${ticket.streamId}&server=${job.server}&ttl=${job.ticketTtl}`)).url;

  if (job.mode === 'bare') {                       // an EventSource that the page never closes
    return new Promise((resolve) => {
      const source = new EventSource(target), states = [];
      source.addEventListener('iq:end', () => states.push('iq:end'));
      source.onmessage = (event) => got.push(event.data);
      source.onerror = () => {
        states.push(`error readyState=${source.readyState}`);
        if (source.readyState === 2) resolve(result({ states }));
      };
    });
  }
  if (job.mode === 'stream') {                     // the ReadableStream form
    const reader = frames(target).getReader();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) return result({ ended: 'closed' });
        got.push(value.data);
      }
    } catch (error) {
      return result({ ended: `${error.name} kind=${error.kind} reason=${error.reason}` });
    }
  }
  if (job.mode === 'opened') {                     // subscribe, wait for the source to open, then start the run
    const sub = subscribe(ticket.url, { onFrame: (frame) => got.push(frame.data) });
    await sub.opened;
    notes.push(`opened with ${got.length} frames`);
    await api(`/api/publish?id=${ticket.streamId}&n=3&interval=50`);
    return result({ outcome: await sub.done });
  }

  const viaFetch = job.fetch && ((resume) => {
    notes.push(`source opened${resume.lastEventId ? ` after ${resume.lastEventId}` : ''}`);
    return job.fetch === 'header' ? fetchSource(ticket.bare, { token: ticket.token, lastEventId: resume.lastEventId }) : fetchSource(target, { lastEventId: resume.lastEventId });
  });
  const sub = subscribe(viaFetch || target, {
    onFrame(frame) {
      got.push(job.withNames ? `${frame.event}:${frame.data}` : frame.data);
      if (got.length === job.dropAfter) void api('/api/drop').then((answer) => notes.push(`dropped ${answer.dropped > 0 ? 'the connections' : 'nothing'}`));
    },
  }, {
    events: job.events,
    retry: job.retry && {
      ...job.retry,
      refresh: async () => {
        notes.push('refresh');
        return (await api(`/api/ticket?id=${ticket.streamId}&server=${job.server}`)).url;
      },
    },
  });
  const opened = await sub.opened.then(() => 'opened', (error) => `${error.name} kind=${error.kind}`);
  return result({ outcome: await sub.done, opened, lastEventId: sub.lastEventId });
}, job);

test('B1 six frames, then the publisher closes with end', { timeout: 30_000 }, async () => {
  const { frames, outcome, opened, lastEventId } = await run({ publish: '/api/publish?n=6&interval=60' });
  assert.deepEqual(frames, letters(6));
  assert.deepEqual(outcome, { type: 'end', lastEventId: 6 });
  assert.equal(opened, 'opened');
  assert.equal(lastEventId, '6');
});

test('B2 the publisher closes with fail', { timeout: 30_000 }, async () => {
  const { frames, outcome } = await run({ publish: '/api/publish?n=3&interval=60&close=fail' });
  assert.deepEqual(frames, letters(3));
  assert.deepEqual(outcome, { type: 'fail', reason: 'publisher', lastEventId: 3 });
});

test('B3 a subscriber that arrives after the close gets iq:gone', { timeout: 30_000 }, async () => {
  const { frames, outcome, opened } = await run({ publish: '/api/publish?n=3&interval=20&wait=1' });
  assert.deepEqual(frames, []);
  assert.deepEqual(outcome, { type: 'gone', outcome: 'end', lastEventId: 3 });
  assert.equal(opened, 'opened');
});

test('B4 every connection cut after frame 4 of 12: EventSource resumes by itself, each frame once', { timeout: 30_000 }, async () => {
  const { frames, outcome, notes } = await run({ publish: '/api/publish?n=12&interval=200&server=short', dropAfter: 4 });
  assert.deepEqual(frames, letters(12));
  assert.deepEqual(outcome, { type: 'end', lastEventId: 12 });
  assert.deepEqual(notes, ['dropped the connections']);
});

test('B5 an invalid token is a refusal that EventSource cannot explain, and opened rejects', { timeout: 30_000 }, async () => {
  const { frames, outcome, opened } = await run({ publish: '/api/publish?n=3&interval=60', badToken: true });
  assert.deepEqual(frames, []);
  assert.deepEqual(outcome, { type: 'refused' });
  assert.equal(opened, 'IQStreamError kind=refused');
});

test('B6 a named frame is delivered only when its name is in events', { timeout: 30_000 }, async () => {
  const without = await run({ publish: '/api/publish?named=1', withNames: true });
  assert.deepEqual(without.frames, ['message:plain']);
  assert.deepEqual(without.outcome, { type: 'end', lastEventId: 2 });
  const withName = await run({ publish: '/api/publish?named=1', withNames: true, events: ['token'] });
  assert.deepEqual(withName.frames, ['message:plain', 'token:named']);
});

test('B7 a publisher that vanishes without a close line: iq:fail publisher_idle after the idle timeout', { timeout: 30_000 }, async () => {
  const { frames, outcome, ms } = await run({ publish: '/api/publish?n=2&interval=60&close=none&server=short' });
  assert.deepEqual(frames, letters(2));
  assert.deepEqual(outcome, { type: 'fail', reason: 'publisher_idle', lastEventId: 2 });
  assert.ok(ms > 4000, `the server waited ${ms} ms`);
});

test('B8 a bare EventSource that never closes reconnects after iq:end and is stopped by the 204', { timeout: 30_000 }, async () => {
  const { frames, states } = await run({ publish: '/api/publish?n=2&interval=60&server=short', mode: 'bare' });
  assert.deepEqual(frames, letters(2));
  assert.deepEqual(states, ['iq:end', 'error readyState=0', 'error readyState=2']);
});

test('B9 frames(): the stream closes on iq:end', { timeout: 30_000 }, async () => {
  const { frames, ended } = await run({ publish: '/api/publish?n=3&interval=50', mode: 'stream' });
  assert.deepEqual([frames, ended], [letters(3), 'closed']);
});

test('B10 frames(): the stream errors with an IQStreamError on iq:fail, after its frames', { timeout: 30_000 }, async () => {
  const { frames, ended } = await run({ publish: '/api/publish?n=3&interval=50&close=fail', mode: 'stream' });
  assert.deepEqual([frames, ended], [letters(3), 'IQStreamError kind=fail reason=publisher']);
});

test('B11 the token expires before a cut: the reconnect is refused, retry.refresh reopens after the last frame', { timeout: 30_000 }, async () => {
  const { frames, outcome, notes } = await run({
    publish: '/api/publish?n=14&interval=400&server=short', server: 'short', ticketTtl: 2, dropAfter: 6, retry: { attempts: 2, delayMs: 200 },
  });
  assert.deepEqual(frames, letters(14));
  assert.deepEqual(outcome, { type: 'end', lastEventId: 14 });
  assert.deepEqual(notes, ['dropped the connections', 'refresh']);
});

test('F1 fetch source: the status and the code of a refusal are readable', { timeout: 30_000 }, async () => {
  const { frames, outcome, opened } = await run({ publish: '/api/publish?n=3&interval=60', badToken: true, fetch: 'query' });
  assert.deepEqual(frames, []);
  assert.equal(outcome.type, 'refused');
  assert.equal(outcome.status, 401);
  assert.equal(typeof outcome.code, 'string');
  assert.equal(opened, 'IQStreamError kind=refused');
});

test('F2 fetch source: the token in the Authorization header, none in the URL', { timeout: 30_000 }, async () => {
  const requests = [];
  const record = (request) => { if (request.method() === 'GET' && !request.url().startsWith(env.backend.origin)) requests.push(request); };
  env.page.on('request', record);
  const { frames, outcome, opened } = await run({ publish: '/api/publish?n=4&interval=60', fetch: 'header' });
  env.page.off('request', record);
  assert.deepEqual(frames, letters(4));
  assert.deepEqual(outcome, { type: 'end', lastEventId: 4 });
  assert.equal(opened, 'opened');
  assert.equal(requests.length, 1);
  assert.equal(new URL(requests[0].url()).search, '');
  assert.match(requests[0].headers().authorization, /^Bearer /);
});

test('F3 fetch source: connections cut after frame 4 of 12, resumed with the Last-Event-ID header', { timeout: 30_000 }, async () => {
  const resumed = [];
  const record = (request) => { if (request.method() === 'GET' && request.headers()['last-event-id']) resumed.push(request.headers()['last-event-id']); };
  env.page.on('request', record);
  const { frames, outcome, notes } = await run({ publish: '/api/publish?n=12&interval=200&server=short', dropAfter: 4, fetch: 'header' });
  env.page.off('request', record);
  assert.deepEqual(frames, letters(12));
  assert.deepEqual(outcome, { type: 'end', lastEventId: 12 });
  assert.deepEqual(notes, ['source opened', 'dropped the connections']);   // one source: it resumed by itself
  assert.equal(resumed.length, 1);
  assert.ok(Number(resumed[0]) >= 4, `resumed after ${resumed[0]}`);
});

test('F4 fetch source: a named frame needs no list of events', { timeout: 30_000 }, async () => {
  const { frames, outcome } = await run({ publish: '/api/publish?named=1', withNames: true, fetch: 'query' });
  assert.deepEqual(frames, ['message:plain', 'token:named']);
  assert.deepEqual(outcome, { type: 'end', lastEventId: 2 });
});

test('the second order: subscribe, await opened, then start the run', { timeout: 30_000 }, async () => {
  const { frames, outcome, notes } = await run({ publish: '/api/ticket', mode: 'opened' });
  assert.deepEqual(notes, ['opened with 0 frames']);
  assert.deepEqual(frames, letters(3));
  assert.deepEqual(outcome, { type: 'end', lastEventId: 3 });
});

test('no scenario left an uncaught error in the page', () => {
  assert.deepEqual(env.pageErrors, []);
});
