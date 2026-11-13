import { execFile } from 'node:child_process';
import net from 'node:net';
import { promisify } from 'node:util';
import { subscribe, testServers } from '../../../dist/testing/index.js';

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export async function until(check, what = 'the condition', ms = 5000) {
  for (const deadline = Date.now() + ms; ; await sleep(10)) {
    const value = check();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
  }
}

/** The servers, and what a scenario needs around a client: its subscriber and the errors its publishers reported. */
export async function setup() {
  const servers = await testServers();
  const errors = [];
  const client = (build, url, options = {}) =>
    new build.IQClient({ url, appKey: servers.appKey, appSecret: servers.appSecret, minOpen: 0, errorHandler: (error) => errors.push(error), ...options });
  /** Subscribes to a new stream on `server`, straight to the server whatever the client's url is. */
  const listen = async (client, server, id = client.newStreamId()) => {
    const subscription = subscribe(server.url, id, await client.token('sub', id));
    await sleep(150);
    return { id, subscription };
  };
  return { servers, errors, client, listen };
}

/**
 * A TCP proxy in front of a server, listening on a local port or on a Unix socket path. `kill()` drops
 * every connection through it, which is how a test loses a publisher's connection without touching the server.
 */
export async function proxy(target, path) {
  const { hostname, port } = new URL(target);
  const pairs = new Set();
  let accepted = 0;
  const server = net.createServer((client) => {
    accepted++;
    const upstream = net.connect(Number(port), hostname);
    const pair = [client, upstream];
    pairs.add(pair);
    const drop = () => {
      pairs.delete(pair);
      client.destroy();
      upstream.destroy();
    };
    client.on('error', drop).on('close', drop);
    upstream.on('error', drop).on('close', drop);
    client.pipe(upstream).pipe(client);
  });
  await new Promise((resolve, reject) => {
    server.on('error', reject);
    if (path) server.listen(path, resolve);
    else server.listen(0, '127.0.0.1', resolve);
  });
  const kill = () => {
    for (const [client, upstream] of pairs) {
      client.destroy();
      upstream.destroy();
    }
  };
  return {
    url: path ? `unix://${path}` : `http://127.0.0.1:${server.address().port}`,
    /** Connections accepted so far. */
    get connections() {
      return accepted;
    },
    kill,
    close: () => {
      kill();
      return new Promise((resolve) => server.close(resolve));
    },
  };
}

const docker = promisify(execFile).bind(undefined, 'docker');
const hasImage = (image) => docker(['image', 'inspect', image]).then(() => true, () => false);

/** The image for a server of the test's own: the one named, else the released one, else a local build of the server's repository. */
export async function serverImage() {
  if (process.env.IQ_STREAM_TEST_IMAGE) return process.env.IQ_STREAM_TEST_IMAGE;
  const released = 'ghcr.io/neuron-core/iq-stream:1';
  return !(await hasImage(released)) && (await hasImage('iq-stream:local')) ? 'iq-stream:local' : released;
}

/** A free port among those this suite may publish a container on. */
export async function containerPort(from = 8100, to = 8119) {
  for (let port = from; port <= to; port++) {
    const free = await new Promise((resolve) => {
      const probe = net.createServer();
      probe.once('error', () => resolve(false));
      probe.listen(port, '127.0.0.1', () => probe.close(() => resolve(true)));
    });
    if (free) return port;
  }
  throw new Error(`no free port between ${from} and ${to}`);
}
