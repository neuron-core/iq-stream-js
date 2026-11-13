// What surrounds a stream: reserving it, a Unix socket, and a server that restarts under a publisher.
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, before, beforeEach, test } from 'node:test';
import { promisify } from 'node:util';
import * as build from '../../../dist/publisher/node.js';
import { startServer, subscribe } from '../../../dist/testing/index.js';
import { containerPort, proxy, serverImage, setup, sleep, until } from './helpers.mjs';

const data = (subscription) => subscription.frames.map((frame) => frame.data);

let servers, errors, client, listen;
before(async () => ({ servers, errors, client, listen } = await setup()));
after(() => servers?.stop());
beforeEach(() => errors.splice(0));

test('reserve() takes a stream out of pending: it then waits the idle timeout for its publisher, not the pending timeout', async () => {
  const iq = client(build, servers.short.url);
  const id = iq.newStreamId();
  assert.deepEqual(await iq.reserve(id), { status: 200, lastEventId: 0, closed: false });
  const subscription = subscribe(servers.short.url, id, await iq.token('sub', id));
  // Wait well past PENDING_TIMEOUT_MS (5 s). The idle timeout (6 s) counts from the last request, so a
  // second reservation on the way keeps the stream for a queue that is slower still.
  await sleep(3200);
  assert.deepEqual(await iq.reserve(id), { status: 200, lastEventId: 0, closed: false });
  await sleep(3200);
  const pub = iq.publisher(id);
  pub.send('from the queue');
  assert.equal((await pub.close()).outcome, 'end');
  const received = await subscription;
  assert.deepEqual(data(received), ['from the queue']);
  assert.equal(received.terminal.event, 'iq:end');
  assert.deepEqual(errors, []);
});

test('a stream that is not reserved fails with publisher_timeout, and reserve() refuses a closed stream', async () => {
  const iq = client(build, servers.short.url);
  const id = iq.newStreamId();
  const received = await subscribe(servers.short.url, id, await iq.token('sub', id));
  assert.deepEqual(received.terminal, { event: 'iq:fail', data: { reason: 'publisher_timeout', lastEventId: 0 } });
  await assert.rejects(iq.reserve(id), (error) => error instanceof build.IQError && error.status === 409 && error.code === 'stream_closed');
});

test('closeStream() tells subscribers at once that a run failed, from a process that never had the publisher', async () => {
  const iq = client(build, servers.default.url);
  const { id, subscription } = await listen(iq, servers.default);
  await iq.reserve(id);
  const started = Date.now();
  assert.deepEqual(await iq.closeStream(id), { status: 200, lastEventId: 0, closed: true, outcome: 'fail', reason: 'publisher' });
  const received = await subscription;
  assert.deepEqual(received.terminal, { event: 'iq:fail', data: { reason: 'publisher', lastEventId: 0 } });
  assert.ok(Date.now() - started < 1000);
});

test('a unix:/// url publishes over a Unix socket', async () => {
  // The servers of this suite listen on TCP, so a proxy stands in for a server started with SOCKET=….
  const directory = await mkdtemp(join(tmpdir(), 'iq-'));
  const socket = await proxy(servers.default.url, join(directory, 'iq.sock'));
  try {
    const iq = client(build, socket.url, { publicUrl: servers.default.url });
    assert.match(iq.url, /^unix:\/\/\//);
    const ticket = await iq.ticket();
    assert.ok(ticket.url.startsWith(`${servers.default.url}/${ticket.streamId}?token=`));
    const subscription = subscribe(servers.default.url, ticket.streamId, ticket.token);
    await sleep(150);

    assert.equal((await iq.reserve(ticket.streamId)).status, 200);
    const pub = iq.publisher(ticket.streamId);
    pub.send('over');
    pub.send('a socket', 'token');
    assert.deepEqual(await pub.close(), { status: 200, lastEventId: 2, closed: true, outcome: 'end' });
    assert.equal(pub.requests, 1);
    assert.ok(socket.connections >= 2, 'the requests went through the socket');
    const received = await subscription;
    assert.deepEqual(data(received), ['over', 'a socket']);
    assert.equal(received.terminal.event, 'iq:end');
    assert.deepEqual(errors, []);
  } finally {
    await socket.close();
    await rm(directory, { recursive: true, force: true });
  }
});

/** Runs a script in a process of its own, with `iq` (a client for the short server) and `id` (a new stream) defined. */
async function runScript(body) {
  const script = `
    import { IQClient } from ${JSON.stringify(new URL('../../../dist/publisher/node.js', import.meta.url).href)};
    const iq = new IQClient({ url: ${JSON.stringify(servers.short.url)}, appKey: ${JSON.stringify(servers.appKey)}, appSecret: ${JSON.stringify(servers.appSecret)}, minOpen: 0.5 });
    const id = process.argv.at(-1);
    ${body}`;
  const id = new build.IQClient({ url: servers.short.url, appKey: servers.appKey, appSecret: servers.appSecret }).newStreamId();
  const started = Date.now();
  const { stdout } = await promisify(execFile)(process.execPath, ['--input-type=module', '-e', script, id], { timeout: 8000 });
  return { id, stdout: stdout.trim(), ms: Date.now() - started };
}

test('an open publisher does not keep its process alive', async () => {
  const iq = client(build, servers.short.url);
  const { ms } = await runScript("const pub = iq.publisher(id); pub.send('a'); setTimeout(() => console.log('done'), 300);");
  assert.ok(ms < 4000, `the process exited by itself, after ${ms} ms`);
  assert.equal(iq instanceof build.IQClient, true);
});

test('close() keeps the process alive until the server has answered, through minOpen', async () => {
  const iq = client(build, servers.short.url);
  const { id, stdout, ms } = await runScript("const pub = iq.publisher(id); pub.send('a'); console.log(JSON.stringify(await pub.close()));");
  assert.deepEqual(JSON.parse(stdout), { status: 200, lastEventId: 1, closed: true, outcome: 'end' });
  assert.ok(ms >= 450, 'the close line waited for minOpen');
  assert.equal((await iq.closeStream(id)).error, 'stream_closed');
});

test('a server that restarts mid-stream', async (t) => {
  let server;
  try {
    server = await startServer({ image: await serverImage(), port: await containerPort(), appKey: servers.appKey, appSecret: servers.appSecret });
  } catch (error) {
    t.skip(`no server container could be started: ${String(error.message).split('\n')[0]}`);
    return;
  }
  t.after(() => server.stop());

  await t.test('the publisher ends with fail and does not publish the tail of the answer into the new stream', async () => {
    const iq = client(build, server.url, { replayFrames: 2 });
    const { id, subscription } = await listen(iq, server);
    const pub = iq.publisher(id);
    for (let i = 1; i <= 5; i++) pub.send(`f${i}`);
    await sleep(300);
    await server.kill();
    pub.send('f6');
    await server.start();
    await until(() => !pub.alive, 'the publisher to give up', 10_000);
    assert.equal(pub.send('f7'), false);
    assert.equal(pub.error.code, 'gap');
    assert.equal(errors.pop(), pub.error);
    const result = await pub.close('end');
    assert.equal(result.outcome, 'fail');
    assert.equal(result.closed, true);

    const before = await subscription;
    assert.deepEqual(data(before), ['f1', 'f2', 'f3', 'f4', 'f5']);
    assert.equal(before.terminal, undefined, 'the old server died without a word');
    const fresh = await subscribe(server.url, id, await iq.token('sub', id));
    assert.deepEqual(fresh.frames, [], 'nothing was published into the new stream');
    assert.equal(fresh.terminal.event, 'iq:fail');
    assert.equal(fresh.terminal.data.reason, 'publisher');
    const resumed = await subscribe(server.url, id, await iq.token('sub', id), { lastEventId: 5 });
    assert.deepEqual(resumed.frames, []);
    assert.equal(resumed.terminal.event, 'iq:gone', 'a subscriber of the old stream is told to load the result');
  });

  await t.test('an answer the replay ring still holds whole is published again from its first frame', async () => {
    const iq = client(build, server.url);
    const id = iq.newStreamId();
    const pub = iq.publisher(id);
    for (let i = 1; i <= 5; i++) pub.send(`f${i}`);
    await sleep(300);
    await server.kill();
    pub.send('f6');
    await server.start();
    const subscription = subscribe(server.url, id, await iq.token('sub', id));
    pub.send('f7');
    await sleep(500);
    const result = await pub.close('end', { timeout: 15 });
    assert.deepEqual(result, { status: 200, lastEventId: 7, closed: true, outcome: 'end' });
    const received = await subscription;
    assert.deepEqual(data(received), ['f1', 'f2', 'f3', 'f4', 'f5', 'f6', 'f7'], 'the same frames under the same ids');
    assert.deepEqual(received.frames.map((frame) => frame.id), ['1', '2', '3', '4', '5', '6', '7']);
    assert.equal(received.terminal.event, 'iq:end');
    assert.deepEqual(errors, []);
  });
});
