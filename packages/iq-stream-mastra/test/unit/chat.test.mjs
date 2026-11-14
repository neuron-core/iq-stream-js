// publishChat: handleChatStream of @mastra/ai-sdk on a useChat request body, with a run id and an
// abort signal the adapter chose.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { publishChat } from '../../dist/index.js';
import { ANSWER, chatBody, types, weatherMastra } from '../support/agent.mjs';
import { recordingClient } from '../support/client.mjs';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const chunks = (publisher) => publisher.data.map((data) => JSON.parse(data));

test('a chat request is published: the ticket carries a run id the adapter chose', async () => {
  const { iq, publishers } = recordingClient();
  const { mastra } = weatherMastra();
  const run = await publishChat({ iq, mastra, agentId: 'weather', params: chatBody(), version: 'v6' });
  assert.match(run.ticket.runId, UUID);
  assert.equal(run.ticket.streamId, publishers[0].streamId);
  const result = await run.done;
  assert.deepEqual([result.outcome, result.framesSent, publishers[0].outcome], ['end', 20, 'end']);
  assert.equal(chunks(publishers[0]).filter((chunk) => chunk.type === 'text-delta').map((chunk) => chunk.delta).join(''), ANSWER);
});

test('the run id reaches the run, and a run id in the params is kept', async () => {
  const { iq, publishers } = recordingClient();
  const options = { iq, agentId: 'weather', version: 'v6', defaultOptions: { requireToolApproval: true } };
  // The approval request is the one chunk that shows the run's id.
  const chosen = await publishChat({ ...options, mastra: weatherMastra().mastra, params: chatBody() });
  await chosen.done;
  assert.equal(chunks(publishers[0]).find((chunk) => chunk.type === 'tool-approval-request').approvalId, `${chosen.ticket.runId}::call-1`);

  const given = await publishChat({ ...options, mastra: weatherMastra().mastra, params: chatBody({ runId: 'run-of-my-own' }) });
  await given.done;
  assert.equal(given.ticket.runId, 'run-of-my-own');
  assert.equal(chunks(publishers[1]).find((chunk) => chunk.type === 'tool-approval-request').approvalId, 'run-of-my-own::call-1');
});

test('run.abort() cancels the run: an abort frame, then fail', async () => {
  const { iq, publishers } = recordingClient();
  const run = await publishChat({ iq, mastra: weatherMastra({ pace: 40 }).mastra, agentId: 'weather', params: chatBody(), version: 'v6' });
  setTimeout(() => run.abort(), 100);
  const result = await run.done;
  assert.ok(types(publishers[0]).includes('abort'), types(publishers[0]).join(' '));
  assert.ok(!types(publishers[0]).includes('text-end'));
  assert.equal(publishers[0].outcome, 'fail');
  assert.deepEqual([result.outcome, result.cause], ['fail', 'abort']);
});

test('closeOnCancel: end closes a cancelled run with end, after the same abort frame', async () => {
  const { iq, publishers } = recordingClient();
  const run = await publishChat({ iq, mastra: weatherMastra({ pace: 40 }).mastra, agentId: 'weather', params: chatBody(), version: 'v6', closeOnCancel: 'end' });
  setTimeout(() => run.abort(), 100);
  const result = await run.done;
  assert.ok(types(publishers[0]).includes('abort'));
  assert.equal(publishers[0].outcome, 'end');
  assert.deepEqual([result.outcome, result.cause], ['end', 'abort']);
});

test('an abort signal of the application still cancels the run', async () => {
  const { iq, publishers } = recordingClient();
  const controller = new AbortController();
  const run = await publishChat({
    iq, mastra: weatherMastra({ pace: 40 }).mastra, agentId: 'weather', version: 'v6',
    params: { ...chatBody(), abortSignal: controller.signal },
  });
  setTimeout(() => controller.abort(), 100);
  const result = await run.done;
  assert.deepEqual([result.outcome, result.cause, publishers[0].outcome], ['fail', 'abort', 'fail']);
});

test('a request that cannot start rejects and opens no publisher', async () => {
  const { iq, publishers } = recordingClient();
  const { mastra } = weatherMastra();
  await assert.rejects(publishChat({ iq, mastra, agentId: 'nobody', params: chatBody(), version: 'v6' }), /nobody/);
  await assert.rejects(publishChat({ iq, mastra, agentId: 'weather', params: { messages: 'not messages' }, version: 'v6' }), /Messages must be an array/);
  assert.equal(publishers.length, 0);
});

test('the error text is masked unless onError says otherwise', async () => {
  const { iq, publishers } = recordingClient();
  const masked = await publishChat({ iq, mastra: weatherMastra({ fail: true }).mastra, agentId: 'weather', params: chatBody(), version: 'v6' });
  const result = await masked.done;
  assert.deepEqual([result.outcome, result.cause], ['fail', 'error']);
  assert.deepEqual(chunks(publishers[0]).find((chunk) => chunk.type === 'error'), { type: 'error', errorText: 'An error occurred.' });
  assert.ok(!publishers[0].data.join('').includes('exploded'));

  const told = await publishChat({ iq, mastra: weatherMastra({ fail: true }).mastra, agentId: 'weather', params: chatBody(), version: 'v6', onError: (error) => `Failed: ${error.message}` });
  await told.done;
  assert.equal(chunks(publishers[1]).find((chunk) => chunk.type === 'error').errorText, 'Failed: model exploded mid-stream');
});

test('waitUntil receives the publishing promise, and onResult the result with the ticket', async () => {
  const { iq } = recordingClient();
  const waited = [];
  const reported = [];
  const run = await publishChat({
    iq, mastra: weatherMastra({ tool: false }).mastra, agentId: 'weather', params: chatBody(), version: 'v6',
    waitUntil: (promise) => waited.push(promise),
    onResult: (result, ticket) => { reported.push([result, ticket]); throw new Error('a reporting bug'); },
  });
  assert.deepEqual(waited, [run.done]);
  const result = await run.done;
  assert.equal(result.outcome, 'end');
  assert.deepEqual(reported, [[result, run.ticket]]);
});

test('an approval answered by an AI SDK 6 front end: the second leg resumes the run and its ticket names it', async () => {
  const { iq, publishers } = recordingClient();
  const { mastra } = weatherMastra();
  const options = { iq, mastra, agentId: 'weather', version: 'v6', defaultOptions: { requireToolApproval: true } };
  const first = await publishChat({ ...options, params: chatBody() });
  const result1 = await first.done;
  assert.deepEqual([result1.outcome, result1.suspended], ['end', true]);
  const approvalId = chunks(publishers[0]).find((chunk) => chunk.type === 'tool-approval-request').approvalId;

  // What useChat sends after addToolApprovalResponse(): the messages again, with the answer on the tool part.
  const messages = [...chatBody().messages, {
    id: 'a1', role: 'assistant',
    parts: [{ type: 'tool-getWeather', toolCallId: 'call-1', state: 'approval-responded', input: { city: 'Paris' }, approval: { id: approvalId, approved: true } }],
  }];
  const second = await publishChat({ ...options, params: { messages } });
  const result2 = await second.done;
  assert.notEqual(second.ticket.streamId, first.ticket.streamId);
  assert.equal(second.ticket.runId, first.ticket.runId);
  assert.ok(types(publishers[1]).includes('tool-output-available'), types(publishers[1]).join(' '));
  assert.equal(types(publishers[1]).at(-1), 'finish');
  assert.deepEqual([result2.outcome, result2.suspended, publishers[1].outcome], ['end', false, 'end']);
});
