import assert from 'node:assert/strict';
import { test } from 'node:test';
import * as fetchBuild from '../../../dist/publisher/fetch.js';
import * as nodeBuild from '../../../dist/publisher/node.js';
import { IQError } from '../../../dist/shared/errors.js';

// The vector of core-client.md, produced by the server's own IQ\Auth\Auth::mint.
const APP_KEY = 'demo-app';
const SECRET = 'test-app-secret-0123456789abcdef0123';
const STREAM = 'Q2hhdC1zdHJlYW0tMDAwMQ';
const EXP = 2_000_000_000;
const BODY = 'eyJhcHBfa2V5IjoiZGVtby1hcHAiLCJzdHJlYW0iOiJRMmhoZEMxemRISmxZVzB0TURBd01RIiwiZXhwIjoyMDAwMDAwMDAwfQ';
const VECTOR = { pub: `${BODY}.K9667Y9dJdNfJLyDD8CQ1Dtt5Kgjmc5vjreIFPX1ltQ`, sub: `${BODY}.axHyIkb73vpsdXhSCIuvasYjNTZEcVNnVqaCbckYL5U` };

const options = { url: 'http://iq.test:8080', appKey: APP_KEY, appSecret: SECRET };
const at = (t, seconds) => {
  t.mock.timers.reset();
  t.mock.timers.enable({ apis: ['Date'], now: seconds * 1000 });
};

for (const [name, build] of [['node', nodeBuild], ['fetch', fetchBuild]]) {
  test(`${name} build: token() reproduces the server's vector`, async (t) => {
    at(t, EXP - 3600);
    const client = new build.IQClient(options);
    assert.equal(await client.token('pub', STREAM), VECTOR.pub);
    assert.equal(await client.token('sub', STREAM), VECTOR.sub);
    assert.equal(await client.token('sub', STREAM, 3600), VECTOR.sub);
    at(t, EXP - 60);
    assert.equal(await client.token('sub', STREAM, 60), VECTOR.sub);
  });

  test(`${name} build: verify() accepts its own tokens and nothing else`, async (t) => {
    at(t, EXP - 3600);
    const client = new build.IQClient(options);
    assert.equal(await client.verify('sub', VECTOR.sub, STREAM), true);
    assert.equal(await client.verify('pub', VECTOR.pub, STREAM), true);
    assert.equal(await client.verify('pub', VECTOR.sub, STREAM), false, 'a sub token never passes as pub');
    assert.equal(await client.verify('sub', VECTOR.pub, STREAM), false);
    assert.equal(await client.verify('sub', VECTOR.sub, 'another-stream-000001'), false, 'another stream');
    assert.equal(await client.verify('sub', `${BODY}.b${VECTOR.sub.slice(BODY.length + 2)}`, STREAM), false, 'a changed signature');
    assert.equal(await client.verify('sub', `x${VECTOR.sub}`, STREAM), false, 'changed claims');
    for (const junk of ['', 'a', 'a.b', 'a.b.c', `${VECTOR.sub}.x`, '.', undefined, 42]) assert.equal(await client.verify('sub', junk, STREAM), false);

    const other = new build.IQClient({ ...options, appSecret: 'another-app-secret-0123456789abcdef' });
    assert.equal(await other.verify('sub', VECTOR.sub, STREAM), false, 'another secret');
    const otherKey = new build.IQClient({ ...options, appKey: 'other-app' });
    assert.equal(await otherKey.verify('sub', await client.token('sub', STREAM), STREAM), false, 'another app key');

    at(t, EXP);
    assert.equal(await client.verify('sub', VECTOR.sub, STREAM), false, 'expired');
  });

  test(`${name} build: subscribeUrl() and ticket() use the public address`, async (t) => {
    at(t, EXP - 3600);
    const client = new build.IQClient({ ...options, publicUrl: 'https://stream.example.com/' });
    assert.equal(await client.subscribeUrl(STREAM), `https://stream.example.com/${STREAM}?token=${VECTOR.sub}`);
    assert.deepEqual(await client.ticket(STREAM), { streamId: STREAM, token: VECTOR.sub, url: `https://stream.example.com/${STREAM}?token=${VECTOR.sub}`, expiresAt: EXP });
    assert.equal((await client.ticket(STREAM, 60)).expiresAt, EXP - 3540);

    const made = await client.ticket();
    assert.match(made.streamId, /^[A-Za-z0-9_-]{22}$/);
    assert.equal(await client.verify('sub', made.token, made.streamId), true);
    assert.equal(made.url, `https://stream.example.com/${made.streamId}?token=${made.token}`);

    assert.equal(await new build.IQClient(options).subscribeUrl(STREAM), `http://iq.test:8080/${STREAM}?token=${VECTOR.sub}`);
  });

  test(`${name} build: stream ids are 128 random bits, and are validated`, async () => {
    const client = new build.IQClient(options);
    const ids = new Set(Array.from({ length: 200 }, () => client.newStreamId()));
    assert.equal(ids.size, 200);
    for (const id of ids) assert.match(id, /^[A-Za-z0-9_-]{22}$/);

    for (const id of ['short', 'x'.repeat(129), 'has space in it 0000000', 'slash/in/it/0000000000', '', 42]) {
      await assert.rejects(client.token('sub', id), TypeError);
      await assert.rejects(client.ticket(id), TypeError);
      await assert.rejects(client.reserve(id), TypeError);
      await assert.rejects(client.closeStream(id), TypeError);
      assert.throws(() => client.publisher(id), TypeError);
    }
    await assert.rejects(client.token('admin', STREAM), TypeError);
    await assert.rejects(client.token('sub', STREAM, 0), TypeError);
    await assert.rejects(client.token('sub', STREAM, Number.NaN), TypeError);
  });

  test(`${name} build: the constructor validates its options`, () => {
    assert.throws(() => new build.IQClient({ ...options, appSecret: 'x'.repeat(31) }), IQError);
    assert.doesNotThrow(() => new build.IQClient({ ...options, appSecret: 'x'.repeat(32) }));
    assert.doesNotThrow(() => new build.IQClient({ ...options, appSecret: 'é'.repeat(16) }), 'bytes, not characters');
    assert.throws(() => new build.IQClient({ ...options, appKey: 'has space' }), IQError);
    assert.throws(() => new build.IQClient({ ...options, url: 'ftp://iq.test' }), IQError);
    assert.throws(() => new build.IQClient({ ...options, url: 'iq.test:8080' }), IQError);
    assert.throws(() => new build.IQClient({ ...options, url: 'http://iq test' }), IQError);
    assert.throws(() => new build.IQClient({ ...options, keepalive: 0 }), IQError);
    assert.throws(() => new build.IQClient({ ...options, minOpen: -1 }), IQError);
    assert.throws(() => new build.IQClient({ ...options, onError: 'ignore' }), IQError);
    assert.throws(() => new build.IQClient({ ...options, mode: 'burst' }), IQError);
    assert.doesNotThrow(() => new build.IQClient({ ...options, keepalive: null, minOpen: 0 }));
    const error = (() => {
      try {
        new build.IQClient({ ...options, appSecret: 'too-short-secret' });
      } catch (thrown) {
        return thrown;
      }
    })();
    assert.ok(!error.message.includes('too-short-secret'), 'the secret is never in a message');
  });

  test(`${name} build: options default to the environment`, async (t) => {
    const saved = { ...process.env };
    t.after(() => {
      for (const name of ['IQ_STREAM_URL', 'IQ_STREAM_APP_KEY', 'IQ_STREAM_APP_SECRET', 'IQ_STREAM_PUBLIC_URL']) {
        if (saved[name] === undefined) delete process.env[name];
        else process.env[name] = saved[name];
      }
    });
    for (const name of ['IQ_STREAM_URL', 'IQ_STREAM_APP_KEY', 'IQ_STREAM_APP_SECRET', 'IQ_STREAM_PUBLIC_URL']) delete process.env[name];
    assert.throws(() => new build.IQClient(), IQError);
    assert.throws(() => new build.IQClient({ url: 'http://iq.test' }), IQError);

    Object.assign(process.env, { IQ_STREAM_URL: 'http://iq.internal:8080/', IQ_STREAM_APP_KEY: APP_KEY, IQ_STREAM_APP_SECRET: SECRET });
    at(t, EXP - 3600);
    let client = new build.IQClient();
    assert.equal(client.url, 'http://iq.internal:8080');
    assert.equal(client.publicUrl, 'http://iq.internal:8080');
    assert.equal(await client.token('sub', STREAM), VECTOR.sub);

    process.env.IQ_STREAM_PUBLIC_URL = 'https://stream.example.com';
    client = new build.IQClient();
    assert.equal(client.publicUrl, 'https://stream.example.com');
    client = new build.IQClient({ url: 'http://other:1', publicUrl: 'https://public.example.com' });
    assert.equal(client.url, 'http://other:1');
    assert.equal(client.publicUrl, 'https://public.example.com');
  });

  test(`${name} build: the secret does not show when the client is printed`, async () => {
    const { inspect } = await import('node:util');
    const client = new build.IQClient(options);
    assert.ok(!inspect(client, { depth: 5 }).includes(SECRET));
    assert.ok(!JSON.stringify(client).includes(SECRET));
  });
}

test('node build: tokenSync() reproduces the vector', (t) => {
  at(t, EXP - 3600);
  const client = new nodeBuild.IQClient(options);
  assert.equal(client.tokenSync('pub', STREAM), VECTOR.pub);
  assert.equal(client.tokenSync('sub', STREAM), VECTOR.sub);
  assert.equal(client.tokenSync('sub', STREAM, 3600), VECTOR.sub);
  assert.throws(() => client.tokenSync('sub', 'short'), TypeError);
  assert.throws(() => client.tokenSync('admin', STREAM), TypeError);
});

test('fetch build: tokenSync() says where it exists', () => {
  assert.throws(() => new fetchBuild.IQClient(options).tokenSync('sub', STREAM), (error) => error instanceof IQError && /Node build/.test(error.message));
});

test('a unix socket address: the Node build takes it, the fetch build refuses it', async () => {
  const unix = { ...options, url: 'unix:///run/iq/iq.sock' };
  assert.throws(() => new fetchBuild.IQClient(unix), (error) => error instanceof IQError && /Node build/.test(error.message));
  const client = new nodeBuild.IQClient(unix);
  await assert.rejects(client.subscribeUrl(STREAM), (error) => error instanceof IQError && /publicUrl/.test(error.message));
  const withPublic = new nodeBuild.IQClient({ ...unix, publicUrl: 'https://stream.example.com' });
  assert.match(await withPublic.subscribeUrl(STREAM), /^https:\/\/stream\.example\.com\/Q2hhdC1zdHJlYW0tMDAwMQ\?token=/);
});
