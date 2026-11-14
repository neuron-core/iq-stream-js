// iqChatRoute on Mastra's Hono server (in process), publishing to the real server with the real
// IQClient; the frames are read back as a browser reads them and handed to the AI SDK's own reader.
import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { readUIMessageStream } from 'ai';
import { iqChatRoute } from '../../dist/index.js';
import { ANSWER, chatBody } from '../support/agent.mjs';
import { mastraApp, resultsByStream } from '../support/server.mjs';
import { delivered, oneTo, setup, sleep } from './support.mjs';

let env;
before(async () => { env = await setup(); });
after(async () => { await env?.servers.stop(); });

/**
 * The routes on a server. The client keeps the core's default `minOpen`: the page subscribes after the
 * ticket, and a run of a few milliseconds must not be over and gone by then.
 */
async function serve(routeOptions = {}, agentOptions = {}) {
  const { onResult, done } = resultsByStream();
  const iq = env.client('default', { minOpen: 3 });
  const routes = iqChatRoute({ path: '/iq/chat/:agentId', iq, version: 'v6', onResult, ...routeOptions });
  return { ...(await mastraApp(routes, agentOptions)), done };
}

/** What useChat does with a transport's stream: the last snapshot of the message. */
async function messageOf(chunks, message) {
  const stream = new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
  let last;
  for await (const snapshot of readUIMessageStream({ stream, ...(message ? { message } : {}) })) last = snapshot;
  return last;
}
const summary = (message) => message.parts.map((part) =>
  part.type === 'text' || part.type === 'reasoning' ? [part.type, part.text, part.state]
    : part.type.startsWith('tool-') ? [part.type, part.state, part.input, part.output] : [part.type]);

test('round trip: the frames a page receives rebuild the message in the AI SDK, for v5, v6 and v7 chunks', { timeout: 60_000 }, async () => {
  await Promise.all(['v5', 'v6', 'v7'].map(async (version) => {
    const { post, done } = await serve({ version, sendReasoning: true });
    const started = performance.now();
    const response = await post('/iq/chat/weather', chatBody());
    const answeredAfter = performance.now() - started;
    const ticket = await response.json();
    // The page subscribes only now, to a run of some 150 ms that is likely over already.
    const got = delivered(await env.read(ticket));

    assert.equal(response.status, 200, version);
    assert.ok(answeredAfter < 1000, `the ticket came after ${answeredAfter} ms`);
    assert.deepEqual(got.ids, oneTo(got.ids.length), version);
    assert.deepEqual(got.terminal, { event: 'iq:end', data: { lastEventId: got.ids.length } }, version);
    assert.deepEqual(summary(await messageOf(got.chunks)), [
      ['step-start'],
      ['reasoning', 'I need the weather. ', 'done'],
      ['tool-getWeather', 'output-available', { city: 'Paris' }, { city: 'Paris', tempC: 21, sky: 'sunny' }],
      ['step-start'],
      ['text', ANSWER, 'done'],
    ], version);
    assert.ok(!JSON.stringify(got.chunks).includes('SECRET SYSTEM PROMPT'), 'the system prompt stays on the backend');
    const result = await done(ticket.streamId);
    assert.deepEqual([result.outcome, result.publish.status, result.framesSent], ['end', 200, got.ids.length], version);
  }));
  assert.deepEqual(env.errors, []);
});

test('approval in two legs: two stream ids, both end with iq:end, the second carries the tool result', { timeout: 60_000 }, async () => {
  const { post } = await serve({ defaultOptions: { requireToolApproval: true } });
  const first = await (await post('/iq/chat/weather', chatBody())).json();
  const leg1 = delivered(await env.read(first));
  assert.deepEqual(leg1.types.slice(-3), ['tool-input-available', 'tool-approval-request', 'data-tool-call-approval']);
  assert.deepEqual(leg1.terminal, { event: 'iq:end', data: { lastEventId: leg1.ids.length } });

  // The page shows the approval card from these frames; the user says yes, as addToolApprovalResponse() records it.
  const assistant = await messageOf(leg1.chunks);
  const card = assistant.parts.find((part) => part.type === 'tool-getWeather');
  assert.deepEqual([card.state, card.approval.id], ['approval-requested', `${first.runId}::call-1`]);
  card.state = 'approval-responded';
  card.approval = { id: card.approval.id, approved: true };

  // useChat sends the messages again; the route answers a new ticket for a new stream.
  const second = await (await post('/iq/chat/weather', { messages: [...chatBody().messages, assistant] })).json();
  const leg2 = delivered(await env.read(second));
  assert.notEqual(second.streamId, first.streamId);
  assert.equal(second.runId, first.runId);
  assert.ok(leg2.types.includes('tool-output-available'), leg2.types.join(' '));
  assert.equal(leg2.types.at(-1), 'finish');
  assert.deepEqual(leg2.terminal, { event: 'iq:end', data: { lastEventId: leg2.ids.length } });

  // The second leg continues the first one's message, as useChat continues it.
  assert.deepEqual(summary(await messageOf(leg2.chunks, assistant)).filter(([type]) => type !== 'step-start' && !type.startsWith('data-')), [
    ['tool-getWeather', 'output-available', { city: 'Paris' }, { city: 'Paris', tempC: 21, sky: 'sunny' }],
    ['text', ANSWER, 'done'],
  ]);
  assert.deepEqual(env.errors, []);
});

test('abort through the route: the abort frame, then iq:fail', { timeout: 60_000 }, async () => {
  const { post, done } = await serve({}, { pace: 100 });
  const ticket = await (await post('/iq/chat/weather', chatBody())).json();
  const subscription = env.read(ticket);
  await sleep(1200);
  assert.equal((await post('/iq/chat/weather/abort', { streamId: ticket.streamId })).status, 204);
  const got = delivered(await subscription);
  assert.ok(got.types.includes('abort'), got.types.join(' '));
  assert.ok(!got.types.includes('text-end'), 'the answer was cut');
  assert.deepEqual(got.terminal, { event: 'iq:fail', data: { reason: 'publisher', lastEventId: got.ids.length } });
  const result = await done(ticket.streamId);
  assert.deepEqual([result.outcome, result.cause, result.publish.reason], ['fail', 'abort', 'publisher']);
  assert.deepEqual(env.errors, []);
});

test('reconnect through the route: the ticket of GET reads the running stream from its first frame', { timeout: 60_000 }, async () => {
  const { post, get, done } = await serve({}, { pace: 100 });
  const ticket = await (await post('/iq/chat/weather', chatBody({ id: 'chat-1' }))).json();
  await sleep(1000);
  const response = await get('/iq/chat/weather/chat-1/stream');
  assert.equal(response.status, 200);
  const again = await response.json();
  assert.equal(again.streamId, ticket.streamId);
  assert.notEqual(again.token, undefined);
  const got = delivered(await env.read(again));
  assert.deepEqual(got.ids, oneTo(20));
  assert.equal(got.types[0], 'start');
  assert.deepEqual(got.terminal, { event: 'iq:end', data: { lastEventId: 20 } });
  await done(ticket.streamId);
  await sleep(50);
  assert.equal((await get('/iq/chat/weather/chat-1/stream')).status, 204);
  assert.deepEqual(env.errors, []);
});
