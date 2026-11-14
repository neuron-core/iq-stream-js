// What the integration tests share: the two real servers, real clients, and a TCP proxy that counts
// the publisher's requests (the core gives each request a connection of its own).
import net from 'node:net';
import { IQClient } from '@neuron-core/iq-stream';
import { subscribe, testServers } from '@neuron-core/iq-stream/testing';

export { sleep } from '../support/agent.mjs';

export async function setup() {
  const servers = await testServers();
  const errors = [];
  /** A client of `server` ('default' or 'short', or a URL). minOpen is 0 unless a test wants the default. */
  const client = (server = 'default', options = {}) => new IQClient({
    url: servers[server]?.url ?? server, appKey: servers.appKey, appSecret: servers.appSecret,
    minOpen: 0, errorHandler: (error) => errors.push(error), ...options,
  });
  /** Reads a stream to its terminal event, as a browser would. */
  const read = (ticket, server = 'default') => subscribe(servers[server].url, ticket.streamId, ticket.token);
  return { servers, errors, client, read };
}

/** A TCP proxy in front of a server. `connections` counts what it accepted. */
export async function countingProxy(target) {
  const { hostname, port } = new URL(target);
  const sockets = new Set();
  const state = { connections: 0 };
  const server = net.createServer((client) => {
    state.connections++;
    const upstream = net.connect(Number(port), hostname);
    for (const socket of [client, upstream]) {
      sockets.add(socket);
      socket.on('error', () => socket.destroy()).on('close', () => sockets.delete(socket));
    }
    client.on('close', () => upstream.destroy());
    upstream.on('close', () => client.destroy());
    client.pipe(upstream).pipe(client);
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    url: `http://127.0.0.1:${server.address().port}`,
    get connections() { return state.connections; },
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise((resolve) => server.close(resolve));
    },
  };
}

/** What a subscription delivered: the parsed chunks, their ids, and the terminal event. */
export const delivered = (subscription) => ({
  chunks: subscription.frames.map((frame) => JSON.parse(frame.data)),
  types: subscription.frames.map((frame) => JSON.parse(frame.data).type),
  ids: subscription.frames.map((frame) => Number(frame.id)),
  events: [...new Set(subscription.frames.map((frame) => frame.event))],
  terminal: subscription.terminal,
});

export const oneTo = (count) => Array.from({ length: count }, (_, index) => index + 1);
