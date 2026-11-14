import assert from 'node:assert/strict';
import { test } from 'node:test';
import { classifyAgUiEvent, classifyMastraChunk, classifyUIChunk } from '../../dist/index.js';

test('AI SDK UI chunks', () => {
  assert.equal(classifyUIChunk({ type: 'error', errorText: 'x' }), 'error');
  assert.equal(classifyUIChunk({ type: 'abort' }), 'abort');
  assert.equal(classifyUIChunk({ type: 'tool-approval-request', approvalId: 'r::c', toolCallId: 'c' }), 'suspend');
  assert.equal(classifyUIChunk({ type: 'data-tool-call-approval', data: { runId: 'r' } }), 'suspend');
  assert.equal(classifyUIChunk({ type: 'data-tool-call-suspended', data: { runId: 'r' } }), 'suspend');
  // The resumed leg repeats the part: it is the answer, not a new wait.
  assert.equal(classifyUIChunk({ type: 'data-tool-call-approval', data: { runId: 'r', resumed: true } }), undefined);
  assert.equal(classifyUIChunk({ type: 'data-tool-call-suspended', data: { resumed: true } }), undefined);
  // A tripwire is a finished run with a verdict in its data.
  for (const type of ['start', 'text-delta', 'finish', 'data-tripwire', 'tool-output-error', 'data-iq-omitted']) {
    assert.equal(classifyUIChunk({ type }), undefined, type);
  }
});

test('AG-UI events', () => {
  assert.equal(classifyAgUiEvent({ type: 'RUN_ERROR', message: 'x' }), 'error');
  assert.equal(classifyAgUiEvent({ type: 'RUN_ERROR', message: 'x', code: 'abort' }), 'abort');
  for (const type of ['RUN_STARTED', 'RUN_FINISHED', 'TEXT_MESSAGE_CHUNK', 'TOOL_CALL_RESULT']) assert.equal(classifyAgUiEvent({ type }), undefined, type);
});

test('Mastra chunks', () => {
  assert.equal(classifyMastraChunk({ type: 'error', payload: {} }), 'error');
  assert.equal(classifyMastraChunk({ type: 'abort', payload: {} }), 'abort');
  assert.equal(classifyMastraChunk({ type: 'tool-call-approval', payload: {} }), 'suspend');
  assert.equal(classifyMastraChunk({ type: 'tool-call-suspended', payload: {} }), 'suspend');
  for (const type of ['start', 'finish', 'tripwire', 'tool-error', 'tool-call']) assert.equal(classifyMastraChunk({ type }), undefined, type);
});

test('anything that is not a chunk is nothing', () => {
  for (const classify of [classifyUIChunk, classifyAgUiEvent, classifyMastraChunk]) {
    for (const value of [null, undefined, 'error', 7, {}, []]) assert.equal(classify(value), undefined);
  }
});
