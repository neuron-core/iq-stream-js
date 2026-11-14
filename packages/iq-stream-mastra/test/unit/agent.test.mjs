// publishToIQ on real Mastra runs: the agent, its tool and its memory are Mastra's, the model is
// Mastra's mock, and the publisher records in memory.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Agent } from '@mastra/core/agent';
import { Mastra } from '@mastra/core/mastra';
import { MockMemory } from '@mastra/core/memory';
import { createStep, createWorkflow } from '@mastra/core/workflows';
import { IQError } from '@neuron-core/iq-stream';
import { RecordingPublisher } from '@neuron-core/iq-stream/testing';
import { readUIMessageStream } from 'ai';
import { z } from 'zod';
import { publishToIQ } from '../../dist/index.js';
import { ANSWER, QUESTION, mockModel, types, weatherAgent } from '../support/agent.mjs';
import { recordingClient } from '../support/client.mjs';

const TEXT = ['text-start', ...Array(7).fill('text-delta'), 'text-end'];
const TOOL = ['tool-input-start', 'tool-input-delta', 'tool-input-delta', 'tool-input-available', 'tool-output-available'];
const chunks = (publisher) => publisher.data.map((data) => JSON.parse(data));
const stored = async (memory, threadId) => (await memory.recall({ threadId, resourceId: 'user-1' })).messages.map((message) => message.role);

test('a text-only run: the frames of one step, then end', async () => {
  const { iq, publishers } = recordingClient();
  const output = await weatherAgent({ tool: false }).stream(QUESTION);
  const run = await publishToIQ(output, { iq, version: 'v6' });
  const result = await run.done;
  const [publisher] = publishers;
  assert.deepEqual(types(publisher), ['start', 'start-step', ...TEXT, 'finish-step', 'finish']);
  assert.equal(chunks(publisher).filter((chunk) => chunk.type === 'text-delta').map((chunk) => chunk.delta).join(''), ANSWER);
  assert.equal(publisher.outcome, 'end');
  assert.deepEqual(result, {
    outcome: 'end', suspended: false, framesSent: 13, framesOmitted: 0,
    publish: { status: 200, lastEventId: 13, closed: true, outcome: 'end' },
  });
});

test('the ticket is the core\'s, for the stream that is published, with the run id', async () => {
  const { iq, publishers } = recordingClient();
  const output = await weatherAgent({ tool: false }).stream(QUESTION);
  const before = Math.floor(Date.now() / 1000);
  const run = await publishToIQ(output, { iq, subTokenTtl: 120 });
  const { ticket } = run;
  assert.deepEqual(Object.keys(ticket), ['streamId', 'token', 'url', 'expiresAt', 'runId']);
  assert.equal(ticket.streamId, publishers[0].streamId);
  assert.equal(ticket.url, `https://stream.example.com/${ticket.streamId}?token=${ticket.token}`);
  assert.equal(await iq.verify('sub', ticket.token, ticket.streamId), true);
  assert.ok(ticket.expiresAt >= before + 120 && ticket.expiresAt <= before + 122);
  assert.equal(ticket.runId, output.runId);
  await run.done;

  // A stream id the application minted itself, for a job that publishes what another process ticketed.
  const second = await publishToIQ(await weatherAgent({ tool: false }).stream(QUESTION), { iq, streamId: 'a-stream-id-of-my-own' });
  assert.equal(second.ticket.streamId, 'a-stream-id-of-my-own');
  assert.equal(publishers[1].streamId, 'a-stream-id-of-my-own');
  await second.done;
});

test('a streamed tool call sits between the two steps, in order', async () => {
  const { iq, publishers } = recordingClient();
  const run = await publishToIQ(await weatherAgent().stream(QUESTION), { iq, version: 'v6' });
  await run.done;
  const [publisher] = publishers;
  assert.deepEqual(types(publisher), ['start', 'start-step', ...TOOL, 'finish-step', 'start-step', ...TEXT, 'finish-step', 'finish']);
  const byType = Object.fromEntries(chunks(publisher).map((chunk) => [chunk.type, chunk]));
  assert.deepEqual(byType['tool-input-available'], { type: 'tool-input-available', toolCallId: 'call-1', toolName: 'getWeather', input: { city: 'Paris' } });
  assert.deepEqual(byType['tool-output-available'], { type: 'tool-output-available', toolCallId: 'call-1', output: { city: 'Paris', tempC: 21, sky: 'sunny' } });
  assert.ok(!publisher.data.join('').includes('SECRET SYSTEM PROMPT'), 'the provider request stays on the backend');
});

test('reasoning is left out by default and sent with ui.sendReasoning', async () => {
  const { iq, publishers } = recordingClient();
  await (await publishToIQ(await weatherAgent().stream(QUESTION), { iq, version: 'v6' })).done;
  await (await publishToIQ(await weatherAgent().stream(QUESTION), { iq, version: 'v6', ui: { sendReasoning: true } })).done;
  assert.ok(!types(publishers[0]).some((type) => type.startsWith('reasoning')));
  assert.deepEqual(types(publishers[1]).slice(0, 5), ['start', 'start-step', 'reasoning-start', 'reasoning-delta', 'reasoning-end']);
});

test('the model breaks mid-answer: an error frame, then fail', async () => {
  const { iq, publishers } = recordingClient();
  const output = await weatherAgent({ fail: true }).stream(QUESTION);
  const run = await publishToIQ(output, { iq, version: 'v6' });
  const result = await run.done;
  const [publisher] = publishers;
  assert.deepEqual(types(publisher).slice(-6), ['text-delta', 'text-delta', 'text-delta', 'error', 'finish-step', 'finish']);
  assert.equal(publisher.outcome, 'fail');
  assert.deepEqual([result.outcome, result.cause, result.suspended], ['fail', 'error', false]);
  assert.equal(output.status, 'failed');
});

test('the error text says nothing about the error unless ui.onError does', async () => {
  const { iq, publishers } = recordingClient();
  await (await publishToIQ(await weatherAgent({ fail: true }).stream(QUESTION), { iq, version: 'v6' })).done;
  const all = publishers[0].data.join('\n');
  assert.deepEqual(chunks(publishers[0]).find((chunk) => chunk.type === 'error'), { type: 'error', errorText: 'An error occurred.' });
  assert.ok(!all.includes('model exploded') && !all.includes('agent.mjs') && !/\bat \S+ \(/.test(all), 'no message, no path, no stack');

  const seen = [];
  await (await publishToIQ(await weatherAgent({ fail: true }).stream(QUESTION), {
    iq, version: 'v6', ui: { onError: (error) => { seen.push(error); return 'The weather service is down.'; } },
  })).done;
  assert.equal(chunks(publishers[1]).find((chunk) => chunk.type === 'error').errorText, 'The weather service is down.');
  assert.equal(seen.length, 1);
});

test('a run cancelled by the application: an abort frame, then fail, or end with closeOnCancel', async () => {
  for (const closeOnCancel of [undefined, 'end']) {
    const { iq, publishers } = recordingClient();
    const controller = new AbortController();
    const output = await weatherAgent({ pace: 40 }).stream(QUESTION, { abortSignal: controller.signal });
    const run = await publishToIQ(output, { iq, version: 'v6', ...(closeOnCancel ? { closeOnCancel } : {}) });
    setTimeout(() => controller.abort(), 100);
    const result = await run.done;
    const [publisher] = publishers;
    assert.ok(types(publisher).includes('abort'), types(publisher).join(' '));
    assert.equal(publisher.outcome, closeOnCancel ?? 'fail');
    assert.deepEqual([result.outcome, result.cause], [closeOnCancel ?? 'fail', 'abort']);
    assert.equal(output.status, 'canceled');
    // The application did not start this run through the adapter: abort() has nothing to cancel.
    assert.doesNotThrow(() => run.abort());
  }
});

test('a tool that needs approval: the leg ends suspended with end; the approval is a second leg on a new stream', async () => {
  const { iq, publishers } = recordingClient();
  const agent = weatherAgent();

  const first = await agent.stream(QUESTION, { requireToolApproval: true });
  const leg1 = await publishToIQ(first, { iq, version: 'v6' });
  const result1 = await leg1.done;
  assert.deepEqual(types(publishers[0]).slice(-3), ['tool-input-available', 'tool-approval-request', 'data-tool-call-approval']);
  assert.ok(!types(publishers[0]).includes('finish'));
  assert.deepEqual(chunks(publishers[0]).at(-2), { type: 'tool-approval-request', approvalId: `${first.runId}::call-1`, toolCallId: 'call-1' });
  assert.equal(publishers[0].outcome, 'end');
  assert.deepEqual([result1.outcome, result1.suspended, result1.cause], ['end', true, undefined]);
  assert.equal(first.status, 'suspended');

  const second = await agent.approveToolCall({ runId: first.runId, toolCallId: 'call-1' });
  const leg2 = await publishToIQ(second, { iq, version: 'v6' });
  const result2 = await leg2.done;
  assert.notEqual(leg2.ticket.streamId, leg1.ticket.streamId);
  assert.deepEqual([leg1.ticket.runId, leg2.ticket.runId], [first.runId, first.runId]);
  assert.ok(types(publishers[1]).includes('tool-output-available'));
  assert.equal(types(publishers[1]).at(-1), 'finish');
  assert.equal(publishers[1].outcome, 'end');
  assert.deepEqual([result2.outcome, result2.suspended], ['end', false]);
});

test('a tripwire from an output processor: a data-tripwire frame, then end', async () => {
  const { iq, publishers } = recordingClient();
  const agent = new Agent({
    id: 'guarded', name: 'guarded', instructions: 'x', model: mockModel({ tool: false }),
    outputProcessors: [{
      id: 'guard',
      processOutputStream: async ({ part, abort }) => {
        if (part.type === 'text-delta' && part.payload.text.includes('sunny')) abort('weather is classified');
        return part;
      },
    }],
  });
  const result = await (await publishToIQ(await agent.stream(QUESTION), { iq, version: 'v6' })).done;
  const [publisher] = publishers;
  const tripwire = chunks(publisher).find((chunk) => chunk.type === 'data-tripwire');
  assert.equal(tripwire.data.reason, 'weather is classified');
  assert.equal(types(publisher).at(-1), 'finish');
  assert.equal(publisher.outcome, 'end');
  assert.deepEqual([result.outcome, result.cause], ['end', undefined]);
});

test('a 100 KB tool result: the placeholder by default, nothing with drop, the mapped chunk with a function', async () => {
  const output = { blob: 'x'.repeat(100_000) };
  const run = async (oversize) => {
    const { iq, publishers } = recordingClient();
    const stream = await weatherAgent({ output }).stream(QUESTION);
    const result = await (await publishToIQ(stream, { iq, version: 'v6', ...(oversize ? { oversize } : {}) })).done;
    return { publisher: publishers[0], result };
  };
  const tail = ['finish-step', 'start-step', ...TEXT, 'finish-step', 'finish'];

  const omitted = await run();
  assert.deepEqual(types(omitted.publisher), ['start', 'start-step', ...TOOL.slice(0, 4), 'data-iq-omitted', ...tail]);
  const placeholder = chunks(omitted.publisher).find((chunk) => chunk.type === 'data-iq-omitted');
  assert.deepEqual({ ...placeholder, data: { ...placeholder.data, bytes: 0 } },
    { type: 'data-iq-omitted', transient: true, data: { type: 'tool-output-available', toolCallId: 'call-1', bytes: 0 } });
  assert.ok(placeholder.data.bytes > 100_000);
  assert.deepEqual([omitted.result.outcome, omitted.result.framesOmitted, omitted.result.framesSent], ['end', 1, 20]);
  // The AI SDK keeps a transient part out of the message: the tool part stays without an output.
  let message;
  const stream = new ReadableStream({ start(controller) { for (const chunk of chunks(omitted.publisher)) controller.enqueue(chunk); controller.close(); } });
  for await (const snapshot of readUIMessageStream({ stream })) message = snapshot;
  assert.deepEqual(message.parts.map((part) => [part.type, part.state]).filter(([type]) => type !== 'step-start'),
    [['tool-getWeather', 'input-available'], ['text', 'done']]);

  const dropped = await run('drop');
  assert.deepEqual(types(dropped.publisher), ['start', 'start-step', ...TOOL.slice(0, 4), ...tail]);
  assert.deepEqual([dropped.result.outcome, dropped.result.framesOmitted], ['end', 1]);

  const mapped = await run((chunk) => ({ ...chunk, output: { truncated: true } }));
  assert.deepEqual(types(mapped.publisher), ['start', 'start-step', ...TOOL, ...tail]);
  assert.deepEqual(chunks(mapped.publisher).find((chunk) => chunk.type === 'tool-output-available').output, { truncated: true });
  assert.deepEqual([mapped.result.outcome, mapped.result.framesOmitted], ['end', 0]);
});

test('send throws on every call: the run completes, memory holds the answer, done resolves', async () => {
  const { iq, publishers } = recordingClient({
    make: (streamId) => {
      const publisher = new RecordingPublisher({ streamId });
      publisher.send = () => { throw new IQError('the wire is cut'); };
      return publisher;
    },
  });
  const memory = new MockMemory();
  const output = await weatherAgent({ memory }).stream(QUESTION, { memory: { thread: 'thread-1', resource: 'user-1' } });
  const result = await (await publishToIQ(output, { iq, version: 'v6' })).done;
  assert.equal(result.framesSent, 0);
  assert.ok(result.error instanceof IQError);
  assert.equal(output.status, 'success');
  assert.equal(await output.text, ANSWER);
  assert.deepEqual(await stored(memory, 'thread-1'), ['user', 'assistant']);
  assert.equal(publishers[0].framesSent, 0);
});

test('persist before close: memory holds the assistant message when the publisher is closed', async () => {
  const memory = new MockMemory();
  const atClose = [];
  const pending = [];
  const { iq } = recordingClient({
    make: (streamId) => {
      const publisher = new RecordingPublisher({ streamId });
      const close = publisher.close.bind(publisher);
      publisher.close = (outcome) => {
        pending.push(stored(memory, 'thread-2').then((roles) => atClose.push(roles)));
        return close(outcome);
      };
      return publisher;
    },
  });
  const output = await weatherAgent({ memory }).stream(QUESTION, { memory: { thread: 'thread-2', resource: 'user-1' } });
  await (await publishToIQ(output, { iq, version: 'v6' })).done;
  await Promise.all(pending);
  assert.deepEqual(atClose, [['user', 'assistant']]);
});

test('two readers: the application reads fullStream whole while the adapter publishes', async () => {
  const { iq, publishers } = recordingClient();
  const output = await weatherAgent().stream(QUESTION);
  const mine = [];
  const reading = (async () => { for await (const chunk of output.fullStream) mine.push(chunk.type); })();
  const run = await publishToIQ(output, { iq, version: 'v6' });
  await Promise.all([reading, run.done]);
  assert.deepEqual([mine[0], mine.at(-1)], ['start', 'finish']);
  assert.equal(mine.filter((type) => type === 'text-delta').length, 7);
  assert.ok(mine.includes('tool-call') && mine.includes('tool-result'));
  assert.equal(types(publishers[0]).filter((type) => type === 'text-delta').length, 7);
  assert.deepEqual([types(publishers[0])[0], types(publishers[0]).at(-1)], ['start', 'finish']);
});

test('five concurrent runs: each publisher gets its own frames, in order', async () => {
  const { iq, publishers } = recordingClient();
  const runs = await Promise.all(Array.from({ length: 5 }, async (_, index) => {
    const output = await weatherAgent({ output: { run: index } }).stream(QUESTION);
    return publishToIQ(output, { iq, version: 'v6' });
  }));
  const results = await Promise.all(runs.map((run) => run.done));
  assert.equal(publishers.length, 5);
  for (const [index, run] of runs.entries()) {
    const publisher = publishers.find((candidate) => candidate.streamId === run.ticket.streamId);
    assert.deepEqual(types(publisher), ['start', 'start-step', ...TOOL, 'finish-step', 'start-step', ...TEXT, 'finish-step', 'finish']);
    assert.deepEqual(chunks(publisher).find((chunk) => chunk.type === 'tool-output-available').output, { run: index });
    assert.deepEqual([results[index].outcome, results[index].framesSent], ['end', 20]);
  }
});

test('the source is detected: an output, { output }, a workflow output, a bare stream of chunks', async () => {
  const { iq, publishers } = recordingClient();
  const expected = ['start', 'start-step', ...TEXT, 'finish-step', 'finish'];

  const wrapped = { output: await weatherAgent({ tool: false }).stream(QUESTION) };   // what a durable agent and agent.observe() return
  const first = await publishToIQ(wrapped, { iq, version: 'v6' });
  await first.done;
  assert.deepEqual(types(publishers[0]), expected);
  assert.equal(first.ticket.runId, wrapped.output.runId);

  const bare = (await weatherAgent({ tool: false }).stream(QUESTION)).fullStream;
  const second = await publishToIQ(bare, { iq, version: 'v6' });
  await second.done;
  assert.deepEqual(types(publishers[1]), expected);
  assert.equal('runId' in second.ticket, false);

  const upper = createStep({
    id: 'upper', inputSchema: z.object({ text: z.string() }), outputSchema: z.object({ text: z.string() }),
    execute: async ({ inputData }) => ({ text: inputData.text.toUpperCase() }),
  });
  const workflow = createWorkflow({ id: 'shout', inputSchema: z.object({ text: z.string() }), outputSchema: z.object({ text: z.string() }) }).then(upper).commit();
  new Mastra({ workflows: { shout: workflow }, logger: false });
  for (const from of [undefined, 'workflow']) {
    const run = await workflow.createRun();
    const published = await publishToIQ(run.stream({ inputData: { text: 'hello' } }), { iq, version: 'v6', ...(from ? { from } : {}) });
    const result = await published.done;
    const publisher = publishers.at(-1);
    assert.deepEqual([types(publisher)[0], types(publisher).at(-1)], ['start', 'finish']);
    assert.ok(types(publisher).includes('data-workflow') && types(publisher).includes('data-workflow-step'), types(publisher).join(' '));
    assert.ok(publisher.data.join('').includes('HELLO'));
    assert.equal(publisher.outcome, 'end');
    assert.equal(result.outcome, 'end');
    assert.equal(published.ticket.runId, run.runId);
  }
});

test('the version decides the chunks: v5 has no approval request of its own', async () => {
  const { iq, publishers } = recordingClient();
  for (const version of [undefined, 'v5', 'v7']) {
    const agent = weatherAgent();
    const result = await (await publishToIQ(await agent.stream(QUESTION, { requireToolApproval: true }), { iq, ...(version ? { version } : {}) })).done;
    assert.deepEqual([result.outcome, result.suspended], ['end', true], version);
  }
  assert.deepEqual(types(publishers[0]).slice(-2), ['tool-input-available', 'data-tool-call-approval']);
  assert.deepEqual(types(publishers[1]), types(publishers[0]));
  assert.deepEqual(types(publishers[2]).slice(-2), ['tool-approval-request', 'data-tool-call-approval']);
});

test('no ticket, no stream: the run is still read to its end, so that it saves', async () => {
  const { iq } = recordingClient();
  iq.ticket = async () => { throw new Error('no ticket today'); };
  const memory = new MockMemory();
  const output = await weatherAgent({ memory }).stream(QUESTION, { memory: { thread: 'thread-3', resource: 'user-1' } });
  await assert.rejects(publishToIQ(output, { iq, version: 'v6' }), /no ticket today/);
  assert.equal(await output.text, ANSWER);
  assert.deepEqual(await stored(memory, 'thread-3'), ['user', 'assistant']);
});
