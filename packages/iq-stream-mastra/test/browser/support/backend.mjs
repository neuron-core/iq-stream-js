// The application of the browser test: Mastra's own server on Hono, listening on a local port, with
// the routes of iqChatRoute and the real IQClient. It also serves the page and its bundle.
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { serve } from '@hono/node-server';
import { Mastra } from '@mastra/core/mastra';
import { InMemoryStore } from '@mastra/core/storage';
import { MastraServer } from '@mastra/hono';
import { IQClient } from '@neuron-core/iq-stream';
import { testServers } from '@neuron-core/iq-stream/testing';
import { build } from 'esbuild';
import { Hono } from 'hono';
import { iqChatRoute } from '../../../dist/index.js';
import { makeAgent } from '../../support/agent.mjs';
import { resultsByStream } from '../../support/server.mjs';

const PAGE = '<!doctype html><meta charset="utf-8"><title>useChat over IQ-Stream, from Mastra</title><body><div id="root"></div><script type="module" src="/bundle/chat.js"></script></body>';

export async function startBackend() {
  const servers = await testServers();
  const bundles = await mkdtemp(join(tmpdir(), 'iq-stream-mastra-browser-'));
  await build({
    entryPoints: [join(import.meta.dirname, 'chat.jsx')],
    outdir: bundles, bundle: true, format: 'esm', platform: 'browser', jsx: 'automatic', logLevel: 'error',
  });

  const errors = [];
  const iq = new IQClient({ url: servers.default.url, appKey: servers.appKey, appSecret: servers.appSecret, errorHandler: (error) => errors.push(error) });
  const { onResult, done } = resultsByStream();
  const agents = {
    weather: makeAgent({ id: 'weather', pace: 40 }),
    broken: makeAgent({ id: 'broken', pace: 40, fail: true }),
    slow: makeAgent({ id: 'slow', pace: 250 }),
    careful: makeAgent({ id: 'careful', pace: 40 }),
  };
  const routes = [
    ...iqChatRoute({ path: '/iq/chat/:agentId', iq, version: 'v6', onResult }),
    ...iqChatRoute({ path: '/iq/approve/:agentId', iq, version: 'v6', defaultOptions: { requireToolApproval: true }, onResult }),
  ];
  const mastra = new Mastra({ agents, storage: new InMemoryStore(), logger: false, server: { apiRoutes: routes } });

  const app = new Hono();
  app.get('/page', (c) => c.html(PAGE));
  app.get('/bundle/chat.js', async (c) => c.body(await readFile(join(bundles, 'chat.js')), 200, { 'Content-Type': 'text/javascript', 'Cache-Control': 'no-store' }));
  await new MastraServer({ app, mastra }).init();
  const server = await new Promise((resolve) => {
    const listening = serve({ fetch: app.fetch, port: 0, hostname: '127.0.0.1' }, () => resolve(listening));
  });

  return {
    origin: `http://127.0.0.1:${server.address().port}`,
    errors,
    /** The result of a stream's run, once it is over. */
    done,
    async close() {
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
      await rm(bundles, { recursive: true, force: true });
      await servers.stop();
    },
  };
}
