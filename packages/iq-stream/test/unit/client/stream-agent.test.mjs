// StreamAgent through runAgent() of @ag-ui/client, with hand-made streams and no network.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { AbstractAgent } from '@ag-ui/client';
import { StreamAgent } from '../../../dist/ag-ui/index.js';
import { IQStreamError } from '../../../dist/client/index.js';
import { FakeEventSource } from '../../../dist/client/testing.js';

const tick = () => new Promise((resolve) => setImmediate(resolve));
const failed = (reason) => new IQStreamError({ type: 'fail', reason });
const INPUT = { threadId: 'thread-1', runId: 'run-1', messages: [], tools: [], context: [], state: {}, forwardedProps: {} };
const STARTED = [
  { type: 'RUN_STARTED', threadId: 'thread-1', runId: 'run-1' },
  { type: 'TEXT_MESSAGE_START', messageId: 'a1', role: 'assistant' },
  { type: 'TEXT_MESSAGE_CONTENT', messageId: 'a1', delta: 'It is sunny ' },
];
const FINISHED = [
  { type: 'TEXT_MESSAGE_CONTENT', messageId: 'a1', delta: 'in Naples.' },
  { type: 'TEXT_MESSAGE_END', messageId: 'a1' },
  { type: 'RUN_FINISHED', threadId: 'thread-1', runId: 'run-1' },
];
const RUN_ERROR = [{ type: 'TEXT_MESSAGE_END', messageId: 'a1' }, { type: 'RUN_ERROR', message: 'provider outage' }];

/** A stream of the events that then closes, errors with `ending`, or stays open when `ending` is 'never'. */
function scripted(events, ending, onCancel) {
  let next = 0;
  return new ReadableStream({
    pull(controller) {
      if (next < events.length) controller.enqueue(events[next++]);
      else if (ending === 'never') return new Promise(() => {});
      else if (ending) controller.error(ending);
      else controller.close();
    },
    cancel: onCancel,
  });
}
const agentOver = (events, ending) => new StreamAgent({ threadId: 'thread-1', start: async () => ({ url: 'unused' }), open: () => scripted(events, ending) });

/** What the agent's observable delivers and how it ends. */
function observe(agent) {
  return new Promise((resolve) => {
    const types = [];
    agent.run(INPUT).subscribe({
      next: (event) => types.push(event.type),
      complete: () => resolve({ types, ended: 'complete' }),
      error: (error) => resolve({ types, ended: 'error', error }),
    });
  });
}
const lastAssistant = (agent) => agent.messages.findLast((message) => message.role === 'assistant')?.content;

test('it is an AbstractAgent', () => {
  assert.ok(agentOver([]) instanceof AbstractAgent);
});

test('runAgent: a run that finishes builds the message', async () => {
  const agent = agentOver([...STARTED, ...FINISHED]);
  agent.messages = [{ id: 'u1', role: 'user', content: 'Weather in Naples?' }];
  const seen = [];
  await agent.runAgent({}, { onEvent: ({ event }) => { seen.push(event.type); } });
  assert.deepEqual(seen, [...STARTED, ...FINISHED].map((event) => event.type));
  assert.equal(lastAssistant(agent), 'It is sunny in Naples.');
});

test('runAgent: iq:fail publisher after RUN_ERROR is not a second error', async () => {
  const agent = agentOver([...STARTED, ...RUN_ERROR], failed('publisher'));
  const seen = [], failures = [];
  await agent.runAgent({}, { onEvent: ({ event }) => { seen.push(event.type); }, onRunFailed: ({ error }) => { failures.push(error.message); } });
  assert.equal(seen.at(-1), 'RUN_ERROR');
  assert.equal(lastAssistant(agent), 'It is sunny ');
  assert.ok(failures.every((message) => !/stream failed/.test(message)), `failures: ${failures}`);
});

test('iq:fail publisher after RUN_ERROR or RUN_FINISHED completes the observable', async () => {
  const afterError = await observe(agentOver([...STARTED, ...RUN_ERROR], failed('publisher')));
  assert.deepEqual([afterError.ended, afterError.types.at(-1), afterError.types.length], ['complete', 'RUN_ERROR', 5]);
  const afterFinish = await observe(agentOver([...STARTED, ...FINISHED], failed('publisher')));
  assert.deepEqual([afterFinish.ended, afterFinish.types.at(-1)], ['complete', 'RUN_FINISHED']);
});

test('iq:fail publisher without either errors the observable', async () => {
  const { ended, error, types } = await observe(agentOver(STARTED, failed('publisher')));
  assert.equal(ended, 'error');
  assert.equal(types.length, 3);
  assert.ok(error instanceof IQStreamError);
  assert.deepEqual([error.kind, error.reason], ['fail', 'publisher']);
});

for (const ending of [failed('publisher_idle'), new IQStreamError({ type: 'gone', outcome: 'end' }), new IQStreamError({ type: 'refused', status: 401 })]) {
  test(`${ending.message} errors the observable, even after RUN_ERROR`, async () => {
    const { ended, error } = await observe(agentOver([...STARTED, ...RUN_ERROR], ending));
    assert.deepEqual([ended, error], ['error', ending]);
  });
}

test('runAgent: a stream that fails with no word from the run rejects', async (t) => {
  t.mock.method(console, 'error', () => {});   // the client logs the failure too
  const agent = agentOver(STARTED, failed('publisher_idle'));
  await assert.rejects(agent.runAgent({}), (error) => error instanceof IQStreamError && error.reason === 'publisher_idle');
});

test('start is given the input of the run, and its ticket goes to open', async () => {
  const seen = {};
  const agent = new StreamAgent({
    threadId: 'thread-9',
    start: async (input) => { seen.input = input; return { streamId: 'stream-1' }; },
    open: (ticket) => { seen.ticket = ticket; return scripted([{ type: 'RUN_STARTED', threadId: 'thread-9', runId: 'r' }, { type: 'RUN_FINISHED', threadId: 'thread-9', runId: 'r' }]); },
  });
  agent.messages = [{ id: 'u1', role: 'user', content: 'hi' }];
  await agent.runAgent({ runId: 'r' });
  assert.deepEqual([seen.input.threadId, seen.input.runId, seen.input.messages.length], ['thread-9', 'r', 1]);
  assert.deepEqual(seen.ticket, { streamId: 'stream-1' });
});

test('a start that fails errors the observable', async () => {
  const agent = new StreamAgent({ start: async () => { throw new Error('backend down'); }, open: () => scripted([]) });
  const { ended, error } = await observe(agent);
  assert.deepEqual([ended, error.message], ['error', 'backend down']);
});

test('unsubscribing cancels the reader', async () => {
  let cancelled = 0;
  const agent = new StreamAgent({ start: async () => ({ url: 'unused' }), open: () => scripted(STARTED, 'never', () => { cancelled++; }) });
  const types = [];
  const subscription = agent.run(INPUT).subscribe({ next: (event) => types.push(event.type) });
  while (types.length < 3) await tick();
  subscription.unsubscribe();
  await tick();
  assert.equal(cancelled, 1);
});

test('unsubscribing before the ticket arrives opens no stream', async () => {
  let release, opened = 0;
  const agent = new StreamAgent({ start: () => new Promise((resolve) => { release = resolve; }), open: () => { opened++; return scripted([]); } });
  const subscription = agent.run(INPUT).subscribe({});
  subscription.unsubscribe();
  release({ url: 'unused' });
  await tick();
  assert.equal(opened, 0);
});

test('a clone runs like the original', async () => {
  const agent = agentOver([...STARTED, ...FINISHED]);
  const clone = agent.clone();
  assert.ok(clone instanceof StreamAgent);
  assert.notEqual(clone, agent);
  await clone.runAgent({});
  assert.equal(lastAssistant(clone), 'It is sunny in Naples.');
});

test('the default open reads IQ frames from the ticket\'s url and parses them as JSON', async (t) => {
  const before = globalThis.EventSource;
  globalThis.EventSource = FakeEventSource;
  FakeEventSource.reset();
  t.after(() => { globalThis.EventSource = before; });
  const url = 'http://iq.test/stream-0123456789abcdef?token=t';
  const agent = new StreamAgent({ threadId: 'thread-1', start: async () => ({ url }) });
  const run = agent.runAgent({ runId: 'run-1' });
  while (FakeEventSource.instances.length === 0) await tick();
  const source = FakeEventSource.instances[0];
  assert.equal(source.url, url);
  source.open();
  [...STARTED, ...RUN_ERROR].forEach((event, index) => source.frame(JSON.stringify(event), { id: String(index + 1) }));
  source.terminal('iq:fail', { reason: 'publisher', lastEventId: 5 });
  await run;
  assert.equal(lastAssistant(agent), 'It is sunny ');
  assert.equal(source.closeCalls, 1);
});
