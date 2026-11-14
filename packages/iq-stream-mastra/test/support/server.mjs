// Mastra's own server on Hono, in process: requests go to `app.fetch`, no socket is opened.
import { MastraServer } from '@mastra/hono';
import { Hono } from 'hono';
import { weatherMastra } from './agent.mjs';

/** A Mastra instance with the weather agent and `apiRoutes`, behind Mastra's Hono server. */
export async function mastraApp(apiRoutes, agentOptions = {}, serverConfig = {}) {
  const { mastra, agent } = weatherMastra(agentOptions, { server: { apiRoutes, ...serverConfig } });
  const app = new Hono();
  await new MastraServer({ app, mastra }).init();
  const request = (method, path, body, init = {}) => app.fetch(new Request(`http://mastra.test${path}`, {
    method,
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    ...init,
    headers: { ...(body === undefined ? {} : { 'Content-Type': 'application/json' }), ...init.headers },
  }));
  return { app, mastra, agent, post: (path, body, init) => request('POST', path, body, init), get: (path, init) => request('GET', path, undefined, init) };
}

/** Collects what `onResult` reports: `done(streamId)` resolves with the result of that stream's run. */
export function resultsByStream() {
  const results = new Map();
  const waiting = new Map();
  return {
    onResult(result, ticket) {
      results.set(ticket.streamId, result);
      waiting.get(ticket.streamId)?.(result);
    },
    done: (streamId) => (results.has(streamId) ? Promise.resolve(results.get(streamId)) : new Promise((resolve) => waiting.set(streamId, resolve))),
  };
}
