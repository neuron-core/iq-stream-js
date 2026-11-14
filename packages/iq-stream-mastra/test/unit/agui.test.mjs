// publishAgUi: the Observable of @ag-ui/mastra, and hand-made ones for what a real run cannot show.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { MastraAgent } from '@ag-ui/mastra';
import { publishAgUi } from '../../dist/index.js';
import { ANSWER, QUESTION, sleep, types, weatherAgent } from '../support/agent.mjs';
import { recordingClient } from '../support/client.mjs';

const input = { threadId: 'thread-1', runId: 'run-1', messages: [{ id: 'u1', role: 'user', content: QUESTION }], tools: [], context: [], state: {}, forwardedProps: {} };
const events = (publisher) => publisher.data.map((data) => JSON.parse(data));

/** An Observable driven by hand, by the one method the adapter uses. */
function subject() {
  const state = { observer: undefined, unsubscribed: 0 };
  return {
    state,
    subscribe(observer) {
      state.observer = observer;
      return { unsubscribe() { state.unsubscribed++; } };
    },
  };
}

test('a run of @ag-ui/mastra: RUN_STARTED … RUN_FINISHED, then end', async () => {
  const { iq, publishers } = recordingClient();
  const agent = new MastraAgent({ agentId: 'weather', agent: weatherAgent(), resourceId: 'user-1' });
  const run = await publishAgUi(agent.run(input), { iq });
  assert.equal(run.ticket.streamId, publishers[0].streamId);
  const result = await run.done;
  const seen = types(publishers[0]);
  assert.deepEqual([seen[0], seen.at(-1)], ['RUN_STARTED', 'RUN_FINISHED']);
  for (const type of ['TOOL_CALL_START', 'TOOL_CALL_ARGS', 'TOOL_CALL_END', 'TOOL_CALL_RESULT', 'TEXT_MESSAGE_CHUNK']) assert.ok(seen.includes(type), type);
  assert.equal(events(publishers[0]).filter((event) => event.type === 'TEXT_MESSAGE_CHUNK').map((event) => event.delta).join(''), ANSWER);
  assert.equal(publishers[0].outcome, 'end');
  assert.deepEqual([result.outcome, result.cause, result.framesSent], ['end', undefined, seen.length]);
});

test('a failing run: RUN_ERROR, then fail', async () => {
  const { iq, publishers } = recordingClient();
  const agent = new MastraAgent({ agentId: 'weather', agent: weatherAgent({ fail: true }), resourceId: 'user-1' });
  const result = await (await publishAgUi(agent.run(input), { iq })).done;
  assert.equal(types(publishers[0]).at(-1), 'RUN_ERROR');
  assert.ok(!publishers[0].data.at(-1).includes('agent.mjs'), 'the bridge sends the message, not the stack');
  assert.equal(publishers[0].outcome, 'fail');
  assert.deepEqual([result.outcome, result.cause], ['fail', 'error']);
});

test('an Observable that errors: fail, with cause stream-error', async () => {
  const { iq, publishers } = recordingClient();
  const source = subject();
  const run = await publishAgUi(source, { iq });
  source.state.observer.next({ type: 'RUN_STARTED' });
  const boom = new Error('the bridge broke');
  source.state.observer.error(boom);
  const result = await run.done;
  assert.deepEqual(types(publishers[0]), ['RUN_STARTED']);
  assert.deepEqual([result.outcome, result.cause, result.error, publishers[0].outcome], ['fail', 'stream-error', boom, 'fail']);
});

test('abort() unsubscribes, which cancels the run, and ends the stream with RUN_ERROR code abort', async () => {
  for (const closeOnCancel of [undefined, 'end']) {
    const { iq, publishers } = recordingClient();
    const source = subject();
    const run = await publishAgUi(source, { iq, ...(closeOnCancel ? { closeOnCancel } : {}) });
    source.state.observer.next({ type: 'RUN_STARTED' });
    source.state.observer.next({ type: 'TEXT_MESSAGE_CHUNK', delta: 'It is' });
    run.abort();
    run.abort();
    // What the bridge still emits after the unsubscribe is not published.
    source.state.observer.next({ type: 'TEXT_MESSAGE_CHUNK', delta: ' late' });
    source.state.observer.complete();
    const result = await run.done;
    assert.equal(source.state.unsubscribed, 1);
    assert.deepEqual(events(publishers[0]), [
      { type: 'RUN_STARTED' }, { type: 'TEXT_MESSAGE_CHUNK', delta: 'It is' }, { type: 'RUN_ERROR', message: 'The run was aborted.', code: 'abort' },
    ]);
    assert.deepEqual([result.outcome, result.cause, publishers[0].outcome], [closeOnCancel ?? 'fail', 'abort', closeOnCancel ?? 'fail']);
  }
});

test('abort() cancels a real run of @ag-ui/mastra: the model is not called for the second step', async () => {
  const { iq, publishers } = recordingClient();
  let calls = 0;
  const mastraAgent = weatherAgent({ pace: 40, onCall: () => calls++ });
  const run = await publishAgUi(new MastraAgent({ agentId: 'weather', agent: mastraAgent, resourceId: 'user-1' }).run(input), { iq });
  setTimeout(() => run.abort(), 100);
  const result = await run.done;
  assert.deepEqual(events(publishers[0]).at(-1), { type: 'RUN_ERROR', message: 'The run was aborted.', code: 'abort' });
  assert.ok(!types(publishers[0]).includes('RUN_FINISHED'));
  assert.deepEqual([result.outcome, result.cause], ['fail', 'abort']);
  // Left alone, the first step would end after 400 ms and the second begin.
  await sleep(800);
  assert.equal(calls, 1);
});

test('an Observable that throws while subscribing rejects, and opens nothing', async () => {
  const { iq, publishers } = recordingClient();
  await assert.rejects(publishAgUi({ subscribe() { throw new Error('no bridge'); } }, { iq }), /no bridge/);
  assert.equal(publishers.length, 0);
});

test('an event over the frame limit becomes a CUSTOM iq-omitted event', async () => {
  const { iq, publishers } = recordingClient();
  const source = subject();
  const run = await publishAgUi(source, { iq });
  source.state.observer.next({ type: 'TOOL_CALL_RESULT', toolCallId: 'call-1', content: 'x'.repeat(100_000) });
  source.state.observer.next({ type: 'RUN_FINISHED' });
  source.state.observer.complete();
  const result = await run.done;
  const [omitted, finished] = events(publishers[0]);
  assert.deepEqual({ ...omitted, value: { ...omitted.value, bytes: 0 } }, { type: 'CUSTOM', name: 'iq-omitted', value: { type: 'TOOL_CALL_RESULT', toolCallId: 'call-1', bytes: 0 } });
  assert.ok(omitted.value.bytes > 100_000);
  assert.deepEqual(finished, { type: 'RUN_FINISHED' });
  assert.deepEqual([result.outcome, result.framesOmitted], ['end', 1]);
});

test('an Observable that completes while subscribing is published whole', async () => {
  const { iq, publishers } = recordingClient();
  const run = await publishAgUi({
    subscribe(observer) {
      observer.next({ type: 'RUN_STARTED' });
      observer.next({ type: 'RUN_FINISHED' });
      observer.complete();
      return { unsubscribe() {} };
    },
  }, { iq });
  const result = await run.done;
  assert.deepEqual(types(publishers[0]), ['RUN_STARTED', 'RUN_FINISHED']);
  assert.equal(result.outcome, 'end');
});
