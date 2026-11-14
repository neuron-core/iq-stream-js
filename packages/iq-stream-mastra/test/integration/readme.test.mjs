// The backend samples of the README, run as written against the real server. (Their TypeScript form is
// compiled by test/unit/types.test.mjs; the page sample is the page of the browser test.)
import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { MastraServer } from '@mastra/hono';
import { Mastra } from '@mastra/core/mastra';
import { MockMemory } from '@mastra/core/memory';
import { InMemoryStore } from '@mastra/core/storage';
import { IQClient } from '@neuron-core/iq-stream';
import { Hono } from 'hono';
import { classifyMastraChunk, iqChatRoute, pipeToIQ, publishChat, publishToIQ } from '../../dist/index.js';
import { ANSWER, QUESTION, chatBody, makeAgent } from '../support/agent.mjs';
import { delivered, setup } from './support.mjs';

let env, iq, mastra, weatherAgent;
before(async () => {
  env = await setup();
  // The README's client takes everything from the environment.
  Object.assign(process.env, { IQ_STREAM_URL: env.servers.default.url, IQ_STREAM_APP_KEY: env.servers.appKey, IQ_STREAM_APP_SECRET: env.servers.appSecret });
  iq = new IQClient();
  weatherAgent = makeAgent({ id: 'weatherAgent', memory: new MockMemory() });
  mastra = new Mastra({
    agents: { weatherAgent },
    server: { apiRoutes: iqChatRoute({ path: '/iq/chat/:agentId', iq, version: 'v6' }) },
    storage: new InMemoryStore(), logger: false,
  });
});
after(async () => { await env?.servers.stop(); });

const answerOf = (got) => got.chunks.filter((chunk) => chunk.type === 'text-delta').map((chunk) => chunk.delta).join('');

test('iqChatRoute in server.apiRoutes', { timeout: 30_000 }, async () => {
  const app = new Hono();
  await new MastraServer({ app, mastra }).init();
  const response = await app.fetch(new Request('http://mastra.test/iq/chat/weatherAgent', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(chatBody({ id: 'chat-1' })),
  }));
  const ticket = await response.json();
  assert.deepEqual(Object.keys(ticket), ['streamId', 'token', 'url', 'expiresAt', 'runId']);
  const got = delivered(await env.read(ticket));
  assert.equal(answerOf(got), ANSWER);
  assert.equal(got.terminal.event, 'iq:end');
});

test('publishChat in a route handler', { timeout: 30_000 }, async () => {
  const waiting = [];
  const waitUntil = (promise) => waiting.push(promise);

  async function POST(request) {
    const run = await publishChat({ iq, mastra, agentId: 'weatherAgent', params: await request.json(), version: 'v6', waitUntil });
    return Response.json(run.ticket);
  }

  const response = await POST(new Request('http://app.test/api/chat', { method: 'POST', body: JSON.stringify(chatBody()) }));
  const ticket = await response.json();
  const got = delivered(await env.read(ticket));
  assert.equal(answerOf(got), ANSWER);
  assert.equal(got.terminal.event, 'iq:end');
  assert.equal((await waiting[0]).outcome, 'end');
});

test('publishToIQ on a run the application started', { timeout: 30_000 }, async () => {
  const question = QUESTION;
  const warnings = [];
  const console = { warn: (...args) => warnings.push(args) };

  const output = await weatherAgent.stream(question, { memory: { thread: 'thread-1', resource: 'user-1' } });
  const run = await publishToIQ(output, { iq, version: 'v6' });   // resolves once the ticket exists
  void run.done.then((result) => {                                // { outcome: 'end' | 'fail', … }, never rejects
    if (result.outcome === 'fail') console.warn('not published', result.cause, result.error);
  });

  const got = delivered(await env.read(run.ticket));
  assert.equal(answerOf(got), ANSWER);
  assert.equal(got.terminal.event, 'iq:end');
  assert.equal((await run.done).outcome, 'end');
  assert.deepEqual(warnings, []);
  assert.equal(await output.text, ANSWER, 'the application reads the same run');
});

test('pipeToIQ on Mastra\'s own chunks', { timeout: 30_000 }, async () => {
  const question = QUESTION;
  const output = await weatherAgent.stream(question);
  const ticket = await iq.ticket();
  const reading = env.read(ticket);
  const result = await pipeToIQ(output.fullStream, iq.publisher(ticket.streamId), { classify: classifyMastraChunk });

  const got = delivered(await reading);
  assert.deepEqual([result.outcome, result.framesOmitted], ['end', 0]);
  assert.deepEqual([got.types[0], got.types.at(-1)], ['start', 'finish']);
  assert.ok(got.types.includes('tool-call') && got.types.includes('tool-result'));
  assert.equal(got.terminal.event, 'iq:end');
});
