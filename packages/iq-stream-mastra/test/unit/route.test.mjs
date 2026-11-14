// iqChatRoute on Mastra's Hono server, in process, with publishers that record in memory.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { MASTRA_RESOURCE_ID_KEY } from '@mastra/core/request-context';
import { iqChatRoute } from '../../dist/index.js';
import { chatBody, sleep, types } from '../support/agent.mjs';
import { recordingClient } from '../support/client.mjs';
import { mastraApp } from '../support/server.mjs';

/** The routes on a server, and what `onResult` reported, as promises by stream id. */
async function serve(routeOptions = {}, agentOptions = {}, serverConfig = {}) {
  const { iq, publishers } = recordingClient();
  const results = new Map();
  const waiting = new Map();
  const routes = iqChatRoute({
    path: '/iq/chat/:agentId', iq, version: 'v6',
    onResult: (result, ticket) => { results.set(ticket.streamId, result); waiting.get(ticket.streamId)?.(result); },
    ...routeOptions,
  });
  const server = await mastraApp(routes, agentOptions, serverConfig);
  const done = (streamId) => (results.has(streamId) ? Promise.resolve(results.get(streamId)) : new Promise((resolve) => waiting.set(streamId, resolve)));
  return { ...server, iq, publishers, routes, done };
}

test('three routes: start, abort, reconnect; two without reconnect', () => {
  const { iq } = recordingClient();
  const routes = iqChatRoute({ path: '/iq/chat/:agentId', iq });
  assert.deepEqual(routes.map((route) => [route.method, route.path]),
    [['POST', '/iq/chat/:agentId'], ['POST', '/iq/chat/:agentId/abort'], ['GET', '/iq/chat/:agentId/:chatId/stream']]);
  assert.deepEqual(iqChatRoute({ path: '/chat/', agent: 'weather', iq, reconnect: false }).map((route) => [route.method, route.path]),
    [['POST', '/chat/'], ['POST', '/chat/abort']]);
  // Like every registerApiRoute route they stay behind Mastra's authentication.
  assert.ok(routes.every((route) => route.requiresAuth !== false));
  assert.throws(() => iqChatRoute({ path: '/iq/chat', iq }), /:agentId/);
});

test('POST answers the ticket before the run ends, and the run goes on after the response', async () => {
  const { post, publishers, done, iq } = await serve({}, { pace: 20 });
  const response = await post('/iq/chat/weather', chatBody());
  assert.equal(response.status, 200);
  assert.match(response.headers.get('content-type'), /application\/json/);
  const ticket = await response.json();
  assert.deepEqual(Object.keys(ticket), ['streamId', 'token', 'url', 'expiresAt', 'runId']);
  assert.equal(await iq.verify('sub', ticket.token, ticket.streamId), true);
  assert.equal(publishers.length, 1);
  assert.equal(publishers[0].streamId, ticket.streamId);
  assert.equal(publishers[0].outcome, undefined, 'the run is still going when the ticket is answered');
  assert.ok(!types(publishers[0]).includes('finish'));

  const result = await done(ticket.streamId);
  assert.deepEqual([result.outcome, result.framesSent, publishers[0].outcome], ['end', 20, 'end']);
  assert.equal(types(publishers[0]).at(-1), 'finish');
});

test('the request\'s own signal does not reach the run: the browser may hang up once it has the ticket', async () => {
  const { post, publishers, done } = await serve({}, { pace: 20 });
  const controller = new AbortController();
  const ticket = await (await post('/iq/chat/weather', chatBody(), { signal: controller.signal })).json();
  controller.abort();
  const result = await done(ticket.streamId);
  assert.deepEqual([result.outcome, result.cause], ['end', undefined]);
  assert.ok(!types(publishers[0]).includes('abort'));
});

test('the server chooses the run id: one in the body counts only with resumeData', async () => {
  const { post, done } = await serve({}, { tool: false });
  const ticket = await (await post('/iq/chat/weather', chatBody({ runId: 'chosen-by-the-browser' }))).json();
  assert.match(ticket.runId, /^[0-9a-f-]{36}$/);
  await done(ticket.streamId);
});

test('an approval answered by an AI SDK 5 front end: resumeData with the run id of the ticket resumes the run', async () => {
  const { post, publishers, done } = await serve({ version: 'v5', defaultOptions: { requireToolApproval: true } });
  const first = await (await post('/iq/chat/weather', chatBody())).json();
  const suspended = await done(first.streamId);
  assert.deepEqual([suspended.outcome, suspended.suspended], ['end', true]);
  assert.equal(JSON.parse(publishers[0].data.at(-1)).data.runId, first.runId);

  const second = await (await post('/iq/chat/weather', chatBody({ runId: first.runId, resumeData: { approved: true } }))).json();
  const resumed = await done(second.streamId);
  assert.notEqual(second.streamId, first.streamId);
  assert.equal(second.runId, first.runId);
  assert.ok(types(publishers[1]).includes('tool-output-available'), types(publishers[1]).join(' '));
  assert.deepEqual([resumed.outcome, resumed.suspended, publishers[1].outcome], ['end', false, 'end']);
});

test('a fixed agent, and the options of handleChatStream', async () => {
  const { post, publishers, done } = await serve({ path: '/support-chat', agent: 'weather', sendReasoning: true }, {});
  const ticket = await (await post('/support-chat', chatBody())).json();
  await done(ticket.streamId);
  assert.ok(types(publishers[0]).includes('reasoning-delta'));
});

test('a request that cannot start is refused by name, and nothing is opened', async () => {
  const { post, publishers } = await serve();
  const unknown = await post('/iq/chat/nobody', chatBody());
  assert.deepEqual([unknown.status, await unknown.json()], [404, { error: 'Agent nobody not found.' }]);
  const invalid = await post('/iq/chat/weather', { messages: 'not messages' });
  assert.deepEqual([invalid.status, await invalid.json()], [400, { error: 'The body must carry the messages of useChat.' }]);
  const notJson = await post('/iq/chat/weather', undefined, { body: '{', headers: { 'Content-Type': 'application/json' } });
  assert.equal(notJson.status, 400);
  assert.equal(publishers.length, 0);
});

test('POST <path>/abort cancels the run: an abort frame, then fail', async () => {
  const { post, publishers, done } = await serve({}, { pace: 40 });
  const ticket = await (await post('/iq/chat/weather', chatBody())).json();
  await sleep(100);
  const aborted = await post('/iq/chat/weather/abort', { streamId: ticket.streamId });
  assert.equal(aborted.status, 204);
  assert.equal(await aborted.text(), '');
  const result = await done(ticket.streamId);
  assert.ok(types(publishers[0]).includes('abort'));
  assert.deepEqual([result.outcome, result.cause, publishers[0].outcome], ['fail', 'abort', 'fail']);

  // Once the run is over this process no longer knows the stream, like one it never knew.
  for (const body of [{ streamId: ticket.streamId }, { streamId: 'never-heard-of-this-stream' }, {}]) {
    assert.equal((await post('/iq/chat/weather/abort', body)).status, 404);
  }
});

test('GET <path>/:chatId/stream answers a new ticket for the run in flight, then 204', async () => {
  const { post, get, done, iq } = await serve({}, { pace: 20 });
  assert.equal((await get('/iq/chat/weather/chat-1/stream')).status, 204);

  const ticket = await (await post('/iq/chat/weather', chatBody({ id: 'chat-1' }))).json();
  const reconnect = await get('/iq/chat/weather/chat-1/stream');
  assert.equal(reconnect.status, 200);
  const again = await reconnect.json();
  assert.deepEqual([again.streamId, again.runId], [ticket.streamId, ticket.runId]);
  assert.equal(await iq.verify('sub', again.token, ticket.streamId), true);
  assert.equal(again.url, `https://stream.example.com/${ticket.streamId}?token=${again.token}`);
  // Another chat, and the same chat id under another agent, have no run.
  assert.equal((await get('/iq/chat/weather/chat-2/stream')).status, 204);
  assert.equal((await get('/iq/chat/other/chat-1/stream')).status, 204);

  await done(ticket.streamId);
  await sleep(20);
  assert.equal((await get('/iq/chat/weather/chat-1/stream')).status, 204);
});

test('a new run of the same chat takes the reconnect over, and the end of the old one leaves it', async () => {
  const { post, get, done } = await serve({}, { pace: 30 });
  const first = await (await post('/iq/chat/weather', chatBody({ id: 'chat-1' }))).json();
  await sleep(50);
  await post('/iq/chat/weather/abort', { streamId: first.streamId });
  const second = await (await post('/iq/chat/weather', chatBody({ id: 'chat-1' }))).json();
  await done(first.streamId);
  await sleep(20);
  assert.equal((await (await get('/iq/chat/weather/chat-1/stream')).json()).streamId, second.streamId);
  await done(second.streamId);
});

test('with a resource id in the request context, a chat id answers only its own user', async () => {
  // What an authentication middleware does: it names the user in the request context.
  const middleware = [async (c, next) => {
    c.get('requestContext').set(MASTRA_RESOURCE_ID_KEY, c.req.header('x-user'));
    await next();
  }];
  const { post, get, done } = await serve({}, { pace: 20 }, { middleware });
  const ticket = await (await post('/iq/chat/weather', chatBody({ id: 'chat-1' }), { headers: { 'x-user': 'ada' } })).json();
  assert.equal((await get('/iq/chat/weather/chat-1/stream', { headers: { 'x-user': 'bob' } })).status, 204);
  const mine = await get('/iq/chat/weather/chat-1/stream', { headers: { 'x-user': 'ada' } });
  assert.equal((await mine.json()).streamId, ticket.streamId);
  await done(ticket.streamId);
});

test('without reconnect there is no reconnect route and nothing is recorded', async () => {
  const { post, get, done } = await serve({ reconnect: false }, { tool: false });
  const ticket = await (await post('/iq/chat/weather', chatBody({ id: 'chat-1' }))).json();
  assert.equal((await get('/iq/chat/weather/chat-1/stream')).status, 404);
  await done(ticket.streamId);
});
