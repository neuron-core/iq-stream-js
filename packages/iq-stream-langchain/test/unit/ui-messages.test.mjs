// publishUIMessages over a real createAgent run on the scripted model, recorded in memory.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { MemorySaver } from '@langchain/langgraph';
import { RecordingPublisher } from '@neuron-core/iq-stream/testing';
import { createAgent, createMiddleware, humanInTheLoopMiddleware, tool } from 'langchain';
import { z } from 'zod';
import { UI_STREAM_OPTIONS, publishUIMessages } from '../../dist/ai-sdk/index.js';
import { ANSWER, ScriptedChatModel, buildAgent, failingTurns, input, sleep, words } from '../fakes/agent.mjs';

/** Runs an agent through the adapter and returns what was recorded. */
async function run(agentOptions = {}, { stream = UI_STREAM_OPTIONS, publisher = new RecordingPublisher(), ...options } = {}) {
  const errors = [];
  const source = await buildAgent(agentOptions).stream(input, stream);
  const result = await publishUIMessages(source, publisher, { onError: (error) => void errors.push(error), ...options });
  const chunks = publisher.data.filter((data) => data !== '[DONE]').map((data) => JSON.parse(data));
  return { publisher, result, errors, chunks, types: chunks.map((chunk) => chunk.type) };
}

const repeat = (type, count) => Array.from({ length: count }, () => type);
const FIRST_TURN = [
  'start', 'start-step', 'text-start', ...repeat('text-delta', 5), 'tool-input-start', ...repeat('tool-input-delta', 2),
  'text-end', 'tool-input-available', 'finish-step', 'start-step',
];
const HAPPY = [
  ...FIRST_TURN, 'tool-output-available', 'finish-step', 'start-step', 'text-start', ...repeat('text-delta', 7),
  'text-end', 'finish-step', 'finish',
];
const OUTAGE = 'fake provider outage';
/** A tool call that fails the run: middleware errors are not fed back to the model. */
const fatalTool = createMiddleware({ name: 'fatal', wrapToolCall: () => { throw new Error('tool exploded'); } });

test('1 happy path: the chunk sequence of a run with one tool call, then close(end)', async () => {
  const { publisher, result, errors, chunks, types } = await run();
  assert.deepEqual(types, HAPPY);
  assert.equal(publisher.outcome, 'end');
  assert.deepEqual(result, { outcome: 'end', frames: HAPPY.length });
  assert.deepEqual(errors, []);
  assert.ok(publisher.frames.every((frame) => frame.event === undefined), 'every frame is unnamed');

  const byType = (type) => chunks.filter((chunk) => chunk.type === type);
  assert.deepEqual(byType('tool-input-available'), [{ type: 'tool-input-available', toolCallId: 'call_1', toolName: 'get_weather', input: { city: 'Paris' }, dynamic: true }]);
  assert.deepEqual(byType('tool-output-available'), [{ type: 'tool-output-available', toolCallId: 'call_1', output: 'Sunny, 21 degrees in Paris' }]);
  assert.equal(byType('tool-input-delta').map((chunk) => chunk.inputTextDelta).join(''), '{"city":"Paris"}');
  assert.equal(byType('text-delta').filter((chunk) => chunk.id === 'fake-msg-2').map((chunk) => chunk.delta).join(''), ANSWER);
});

test('2 the model throws on its second turn: an error chunk, close(fail), onError and no onFinish', async () => {
  let finished = false;
  const { publisher, result, errors, chunks, types } = await run({ turns: failingTurns() }, { onFinish: () => { finished = true; } });
  assert.deepEqual(types, [...FIRST_TURN, 'tool-output-available', 'error']);
  assert.equal(publisher.outcome, 'fail');
  assert.equal(result.outcome, 'fail');
  assert.equal(result.error.message, OUTAGE);
  assert.deepEqual(errors, [result.error]);
  assert.equal(finished, false);
  // The browser is not told what broke unless the application says so.
  assert.deepEqual(chunks.at(-1), { type: 'error', errorText: 'An error occurred.' });
  assert.ok(!publisher.data.some((data) => data.includes(OUTAGE)));
});

test('2b exposeErrors: the error chunk carries the run\'s message', async () => {
  const { chunks } = await run({ turns: failingTurns() }, { exposeErrors: true });
  assert.deepEqual(chunks.at(-1), { type: 'error', errorText: OUTAGE });
});

test('3 a tool call that fails the run: the same as a failing model', async () => {
  let finished = false;
  const { publisher, result, errors, types } = await run({ middleware: [fatalTool] }, { onFinish: () => { finished = true; } });
  assert.deepEqual(types, [...FIRST_TURN.slice(0, -2), 'error']);
  assert.equal(publisher.outcome, 'fail');
  assert.deepEqual([result.outcome, result.error.message], ['fail', 'tool exploded']);
  assert.deepEqual(errors, [result.error]);
  assert.equal(finished, false);
});

test('3b a tool that throws is the agent\'s to handle: the run goes on, and the page gets a masked tool-output-error', async () => {
  const failing = { tool: () => { throw new Error('tool exploded'); } };
  const { publisher, chunks, types, errors } = await run(failing);
  assert.deepEqual(types, HAPPY.map((type) => (type === 'tool-output-available' ? 'tool-output-error' : type)));
  assert.equal(publisher.outcome, 'end');
  assert.deepEqual(errors, []);
  assert.deepEqual(chunks.find((chunk) => chunk.type === 'tool-output-error'), { type: 'tool-output-error', toolCallId: 'call_1', errorText: 'An error occurred.' });

  const exposed = await run(failing, { exposeErrors: true });
  assert.match(exposed.chunks.find((chunk) => chunk.type === 'tool-output-error').errorText, /tool exploded/);
});

test('4 onFinish gets the final state and is awaited before the close line', async () => {
  const seen = [];
  const publisher = new RecordingPublisher();
  await run({}, {
    publisher,
    onFinish: async (state) => {
      await sleep(50);
      seen.push({ last: state.messages.at(-1).content, messages: state.messages.length, lastFrame: publisher.data.at(-1), closed: publisher.outcome });
    },
  });
  assert.deepEqual(seen, [{ last: ANSWER, messages: 4, lastFrame: '{"type":"finish"}', closed: undefined }]);
  assert.equal(publisher.outcome, 'end');
});

test('5 onFinish throws: the stream closes with fail, after an error chunk, and the promise resolves', async () => {
  const lost = new Error('database down');
  const { publisher, result, errors, types, chunks } = await run({}, { onFinish: () => { throw lost; } });
  assert.deepEqual(types, [...HAPPY, 'error']);
  assert.deepEqual(chunks.at(-1), { type: 'error', errorText: 'An error occurred.' });
  assert.equal(publisher.outcome, 'fail');
  assert.deepEqual(result, { outcome: 'fail', error: lost, frames: HAPPY.length + 1 });
  assert.deepEqual(errors, [lost]);
});

test('6 abort through the signal during the tool call: an abort chunk, then close(fail), or abortOutcome', async () => {
  for (const [abortOutcome, expected] of [[undefined, 'fail'], ['end', 'end']]) {
    const controller = new AbortController();
    let finished = false;
    const { publisher, result, errors, types } = await run(
      { tool: async () => { setTimeout(() => controller.abort(), 10); await sleep(150); return 'too late'; } },
      { stream: { ...UI_STREAM_OPTIONS, signal: controller.signal }, onFinish: () => { finished = true; }, ...(abortOutcome ? { abortOutcome } : {}) },
    );
    assert.deepEqual(types, [...FIRST_TURN.slice(0, -2), 'abort']);
    assert.equal(publisher.outcome, expected);
    assert.equal(result.outcome, expected);
    assert.equal(result.error.name, 'AbortError');
    // A cancel is the application's own doing: it is not reported as an error, and nothing is saved.
    assert.deepEqual(errors, []);
    assert.equal(finished, false);
  }
});

test('7 wrong stream options: a configuration error to onError, close(fail), and no finish frame', async () => {
  for (const stream of [{ streamMode: 'messages' }, {}, { streamMode: 'values' }]) {
    const { publisher, result, errors, types } = await run({}, { stream });
    assert.deepEqual(types, ['start', 'error'], JSON.stringify(stream));
    assert.equal(publisher.outcome, 'fail');
    assert.equal(result.outcome, 'fail');
    assert.equal(result.error.name, 'StreamOptionsError');
    assert.match(result.error.message, /UI_STREAM_OPTIONS/);
    assert.deepEqual(errors, [result.error]);
  }
});

test('7c a stream that fails the check is not cancelled: the run goes on by itself', async () => {
  let toolRan = false;
  const { result } = await run({ tool: async () => { await sleep(300); toolRan = true; return 'ok'; } }, { stream: { streamMode: 'messages' } });
  assert.equal(result.error.name, 'StreamOptionsError');
  assert.equal(toolRan, false, 'the adapter answered at the first chunk');
  for (const deadline = Date.now() + 2000; !toolRan && Date.now() < deadline;) await sleep(10);
  assert.equal(toolRan, true);
});

test('7b the option sets the converter reads pass the check', async () => {
  for (const stream of [{ streamMode: ['values', 'messages'] }, { ...UI_STREAM_OPTIONS, subgraphs: true }]) {
    const { publisher, types, errors } = await run({}, { stream });
    assert.deepEqual(types, HAPPY, JSON.stringify(stream));
    assert.equal(publisher.outcome, 'end');
    assert.deepEqual(errors, []);
  }
});

test('8 subgraphs: frames of a sub-agent called from a tool carry its namespace', async () => {
  const researcher = createAgent({ model: new ScriptedChatModel({ turns: [words('Paris is sunny.')] }), tools: [] });
  const ask = tool(
    async ({ city }) => (await researcher.invoke({ messages: [{ role: 'user', content: city }] })).messages.at(-1).content,
    { name: 'get_weather', description: 'Asks the researcher', schema: z.object({ city: z.string() }) },
  );
  const { publisher, chunks } = await run({ tools: [ask] }, { stream: { ...UI_STREAM_OPTIONS, subgraphs: true } });
  assert.equal(publisher.outcome, 'end');
  const namespace = (chunk) => chunk.providerMetadata?.langchain?.namespace;
  const nested = chunks.filter((chunk) => namespace(chunk)?.length > 0);
  assert.equal(nested.filter((chunk) => chunk.type === 'text-delta').map((chunk) => chunk.delta).join(''), 'Paris is sunny.');
  assert.ok(nested.every((chunk) => /^tools:/.test(namespace(chunk)[0])));
  // The parent's own text is in the root namespace.
  const root = chunks.filter((chunk) => chunk.type === 'text-delta' && namespace(chunk)?.length === 0);
  assert.ok(root.map((chunk) => chunk.delta).join('').endsWith(ANSWER));
});

test('9 custom data: config.writer() in a tool yields a data-<type> frame', async () => {
  const { chunks, types } = await run({ tool: ({ city }, config) => { config.writer?.({ type: 'progress', value: 50 }); return `Sunny in ${city}`; } });
  assert.deepEqual(chunks.find((chunk) => chunk.type === 'data-progress'), { type: 'data-progress', transient: true, data: { type: 'progress', value: 50 } });
  assert.ok(types.indexOf('data-progress') < types.indexOf('tool-output-available'));
});

test('10 human in the loop: tool-approval-request, then close(end)', async () => {
  let state;
  const { publisher, result, types, chunks } = await run(
    { middleware: [humanInTheLoopMiddleware({ interruptOn: { get_weather: true } })], checkpointer: new MemorySaver() },
    { stream: { ...UI_STREAM_OPTIONS, configurable: { thread_id: 'thread-1' } }, onFinish: (final) => { state = final; } },
  );
  assert.deepEqual(types, [...FIRST_TURN.slice(0, -2), 'tool-approval-request', 'finish-step', 'finish']);
  assert.deepEqual(chunks.find((chunk) => chunk.type === 'tool-approval-request'), { type: 'tool-approval-request', approvalId: 'call_1', toolCallId: 'call_1' });
  // An interrupt is not a failure: the answer resumes as a new run, on a new stream.
  assert.equal(publisher.outcome, 'end');
  assert.equal(result.outcome, 'end');
  assert.ok(state.__interrupt__?.length > 0);
});

test('12 a publisher that fails does not make the promise reject, and onFinish still runs', async () => {
  const hostile = {
    send() { throw new Error('send failed'); },
    close() { return Promise.reject(new Error('close failed')); },
  };
  let saved;
  const { result } = await run({}, { publisher: Object.assign(hostile, { data: [] }), onFinish: (state) => { saved = state.messages.at(-1).content; } });
  assert.deepEqual(result, { outcome: 'end', frames: 0 });
  assert.equal(saved, ANSWER);
});

test('12b a publisher that goes inert mid-run: the run is read to its end and onFinish still runs', async () => {
  const publisher = new RecordingPublisher();
  let saved;
  const { result, types } = await run(
    { tool: async ({ city }) => { publisher.fail(); return `Sunny in ${city}`; } },
    { publisher, onFinish: (state) => { saved = state.messages.at(-1).content; } },
  );
  assert.equal(saved, ANSWER);
  // What was sent before the publisher gave up, and nothing after it.
  assert.ok(types.length > 0 && types.length < FIRST_TURN.length, `${types.length} frames`);
  assert.deepEqual(types, FIRST_TURN.slice(0, types.length));
  assert.equal(result.outcome, 'end');
  assert.equal(result.frames, types.length);
});

test('13 two runs at once, each with its own publisher: each gets a complete run of its own', async () => {
  const [first, second] = await Promise.all([
    run({ delayMs: 3 }),
    run({ delayMs: 2, tool: async () => 'Rain, 12 degrees in Paris' }),
  ]);
  assert.deepEqual(first.types, HAPPY);
  assert.deepEqual(second.types, HAPPY);
  assert.equal(first.chunks.find((chunk) => chunk.type === 'tool-output-available').output, 'Sunny, 21 degrees in Paris');
  assert.equal(second.chunks.find((chunk) => chunk.type === 'tool-output-available').output, 'Rain, 12 degrees in Paris');
  assert.deepEqual([first.publisher.outcome, second.publisher.outcome], ['end', 'end']);
});

test('14 sendDone appends [DONE] after finish, on success only', async () => {
  const done = await run({}, { sendDone: true });
  assert.deepEqual(done.publisher.data.slice(-2), ['{"type":"finish"}', '[DONE]']);
  assert.equal(done.result.frames, HAPPY.length + 1);

  const failed = await run({ turns: failingTurns() }, { sendDone: true });
  assert.ok(!failed.publisher.data.includes('[DONE]'));
  const byDefault = await run();
  assert.ok(!byDefault.publisher.data.includes('[DONE]'));
});

test('a model stream, without a graph, is published as well', async () => {
  const publisher = new RecordingPublisher();
  const model = new ScriptedChatModel({ turns: [words('Hello there.')] });
  let state = 'not called';
  const result = await publishUIMessages(await model.stream('Hi'), publisher, { onFinish: (final) => { state = final; } });
  assert.deepEqual(publisher.data.map((data) => JSON.parse(data).type), ['start', 'text-start', 'text-delta', 'text-delta', 'text-end', 'finish']);
  assert.deepEqual(result, { outcome: 'end', frames: 6 });
  assert.equal(state, undefined);
});

test('agent.stream() may be passed without awaiting it; if it rejects, the stream closes with fail', async () => {
  const publisher = new RecordingPublisher();
  const result = await publishUIMessages(buildAgent().stream(input, UI_STREAM_OPTIONS), publisher);
  assert.deepEqual(publisher.data.map((data) => JSON.parse(data).type), HAPPY);
  assert.deepEqual(result, { outcome: 'end', frames: HAPPY.length });

  const refused = new Error('invalid input');
  const failing = new RecordingPublisher();
  const errors = [];
  const failed = await publishUIMessages(Promise.reject(refused), failing, { onError: (error) => void errors.push(error) });
  assert.deepEqual(failing.data, ['{"type":"start"}', '{"type":"error","errorText":"An error occurred."}']);
  assert.equal(failing.outcome, 'fail');
  assert.deepEqual(failed, { outcome: 'fail', error: refused, frames: 2 });
  assert.deepEqual(errors, [refused]);
});

test('an onError that throws changes nothing', async () => {
  const { publisher, result } = await run({ turns: failingTurns() }, { onError: () => { throw new Error('the hook fails too'); } });
  assert.equal(publisher.outcome, 'fail');
  assert.equal(result.error.message, OUTAGE);
});
