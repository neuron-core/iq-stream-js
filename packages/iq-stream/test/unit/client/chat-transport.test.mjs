// chatTransport driven by AbstractChat, the class behind useChat, with hand-made streams and no network.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { AbstractChat } from 'ai';
import { chatTransport } from '../../../dist/ai-sdk/index.js';
import { IQStreamError } from '../../../dist/client/index.js';
import { FakeEventSource } from '../../../dist/client/testing.js';

class HeadlessChat extends AbstractChat {
  constructor(init) {
    super({
      ...init,
      state: {
        status: 'ready', error: undefined, messages: init.messages ?? [],
        pushMessage(message) { this.messages = [...this.messages, message]; },
        popMessage() { this.messages = this.messages.slice(0, -1); },
        replaceMessage(index, message) { this.messages = [...this.messages.slice(0, index), message, ...this.messages.slice(index + 1)]; },
        snapshot: (value) => structuredClone(value),
      },
    });
  }
}
const describe = (chat) => ({
  status: chat.status,
  error: chat.error?.message ?? null,
  text: (chat.messages.at(-1)?.parts ?? []).filter((part) => part.type === 'text').map((part) => part.text).join(''),
});

const TEXT = [
  { type: 'start', messageId: 'm1' }, { type: 'start-step' }, { type: 'text-start', id: 't1' },
  { type: 'text-delta', id: 't1', delta: 'Ciao ' }, { type: 'text-delta', id: 't1', delta: 'da Napoli.' },
];
const FINISH = [{ type: 'text-end', id: 't1' }, { type: 'finish-step' }, { type: 'finish', finishReason: 'stop' }];
const failed = (reason) => new IQStreamError({ type: 'fail', reason });

/** A stream of the chunks that then closes, errors with `ending`, or stays open when `ending` is 'never'. */
function scripted(chunks, ending) {
  let next = 0;
  return new ReadableStream({
    pull(controller) {
      if (next < chunks.length) controller.enqueue(chunks[next++]);
      else if (ending === 'never') return new Promise(() => {});
      else if (ending) controller.error(ending);
      else controller.close();
    },
  });
}
/** What the transport's stream delivers, and the error it ends with, if any. */
async function drain(stream) {
  const values = [];
  try {
    for await (const value of stream) values.push(value);
    return { values };
  } catch (error) {
    return { values, error };
  }
}
/** The default open uses the global EventSource, which Node does not have. */
function useFakeEventSource(t) {
  const before = globalThis.EventSource;
  globalThis.EventSource = FakeEventSource;
  FakeEventSource.reset();
  t.after(() => { globalThis.EventSource = before; });
}
const request = { chatId: 'chat-1', messages: [], trigger: 'submit-message', messageId: undefined, abortSignal: undefined };
const transportOver = (chunks, ending, more = {}) => chatTransport({ start: async () => ({ url: 'unused' }), open: () => scripted(chunks, ending), ...more });

test('iq:fail publisher after an error chunk closes the stream cleanly', async () => {
  const chunks = [...TEXT, { type: 'error', errorText: 'provider outage' }];
  const { values, error } = await drain(await transportOver(chunks, failed('publisher')).sendMessages(request));
  assert.deepEqual(values, chunks);
  assert.equal(error, undefined);
});

test('iq:fail publisher after an abort chunk closes the stream cleanly', async () => {
  const chunks = [...TEXT, { type: 'abort' }];
  const { values, error } = await drain(await transportOver(chunks, failed('publisher')).sendMessages(request));
  assert.deepEqual(values, chunks);
  assert.equal(error, undefined);
});

test('iq:fail publisher without such a chunk errors the stream', async () => {
  const { values, error } = await drain(await transportOver(TEXT, failed('publisher')).sendMessages(request));
  assert.deepEqual(values, TEXT);
  assert.ok(error instanceof IQStreamError);
  assert.deepEqual([error.kind, error.reason], ['fail', 'publisher']);
});

for (const ending of [failed('publisher_idle'), failed('max_duration'), new IQStreamError({ type: 'gone', outcome: 'fail', reason: 'publisher' }), new IQStreamError({ type: 'refused' })]) {
  test(`${ending.message} errors the stream, even after an error chunk`, async () => {
    const { error } = await drain(await transportOver([...TEXT, { type: 'error', errorText: 'x' }], ending).sendMessages(request));
    assert.equal(error, ending);
  });
}

test('in a chat: a run that finishes', async () => {
  const chat = new HeadlessChat({ transport: transportOver([...TEXT, ...FINISH]) });
  await chat.sendMessage({ text: 'hi' });
  assert.deepEqual(describe(chat), { status: 'ready', error: null, text: 'Ciao da Napoli.' });
});

test('in a chat: a failed run shows the error of its own chunk, not a second one', async () => {
  const errors = [];
  const chat = new HeadlessChat({ transport: transportOver([...TEXT, { type: 'error', errorText: 'provider outage' }], failed('publisher')), onError: (error) => errors.push(error.message) });
  await chat.sendMessage({ text: 'hi' });
  assert.deepEqual(describe(chat), { status: 'error', error: 'provider outage', text: 'Ciao da Napoli.' });
  assert.deepEqual(errors, ['provider outage']);
});

test('in a chat: a stream whose frames are gone is an error of kind gone', async () => {
  const chat = new HeadlessChat({ transport: transportOver(TEXT.slice(0, 4), new IQStreamError({ type: 'gone', outcome: 'end', lastEventId: 8 })) });
  await chat.sendMessage({ text: 'hi' });
  assert.equal(chat.status, 'error');
  assert.ok(chat.error instanceof IQStreamError);
  assert.deepEqual([chat.error.kind, chat.error.outcome, chat.error.lastEventId], ['gone', 'end', 8]);
});

test('start is given the request of useChat, without the signal, and its ticket goes to open with the signal', async () => {
  const seen = {};
  const transport = chatTransport({
    start: async (startRequest) => { seen.request = startRequest; return { streamId: 'stream-1' }; },
    open: (ticket, signal) => { seen.ticket = ticket; seen.signal = signal; return scripted([...TEXT, ...FINISH]); },
  });
  const chat = new HeadlessChat({ id: 'chat-7', transport });
  await chat.sendMessage({ text: 'hi' }, { body: { model: 'small' } });
  assert.equal(chat.status, 'ready');
  assert.deepEqual(Object.keys(seen.request).sort(), ['body', 'chatId', 'headers', 'messageId', 'messages', 'metadata', 'trigger']);
  assert.deepEqual([seen.request.chatId, seen.request.trigger, seen.request.body], ['chat-7', 'submit-message', { model: 'small' }]);
  assert.deepEqual(seen.request.messages.map((message) => message.role), ['user']);
  assert.deepEqual(seen.ticket, { streamId: 'stream-1' });
  assert.ok(seen.signal instanceof AbortSignal);
});

test('resume: without the option, or when it returns null, there is no stream to reconnect to', async () => {
  assert.equal(await transportOver([]).reconnectToStream({ chatId: 'chat-1' }), null);
  const asked = [];
  const transport = transportOver([], undefined, { resume: async (resumeRequest) => { asked.push(resumeRequest); return null; } });
  assert.equal(await transport.reconnectToStream({ chatId: 'chat-1', abortSignal: undefined }), null);
  assert.deepEqual(asked, [{ chatId: 'chat-1' }]);

  const chat = new HeadlessChat({ transport });
  await chat.resumeStream();
  assert.deepEqual(describe(chat), { status: 'ready', error: null, text: '' });
});

test('resume: the ticket of the run in flight is opened, and the replay rebuilds the message', async () => {
  const opened = [];
  const transport = chatTransport({
    start: async () => { throw new Error('not used'); },
    resume: async ({ chatId }) => ({ url: `ticket-of-${chatId}` }),
    open: (ticket) => { opened.push(ticket.url); return scripted([...TEXT, ...FINISH]); },
  });
  assert.equal(transport.resumeStreamIsReplay, true);
  const chat = new HeadlessChat({
    id: 'chat-9', transport,
    messages: [{ id: 'u1', role: 'user', parts: [{ type: 'text', text: 'hi' }] }, { id: 'm1', role: 'assistant', parts: [{ type: 'text', text: 'Ciao ' }] }],
  });
  await chat.resumeStream();
  assert.deepEqual(opened, ['ticket-of-chat-9']);
  assert.deepEqual(describe(chat), { status: 'ready', error: null, text: 'Ciao da Napoli.' });
  assert.deepEqual(chat.messages.map((message) => message.role), ['user', 'assistant']);
});

test('stop is called with the ticket when the user aborts', async () => {
  const stopped = [];
  let signalSeen;
  const transport = chatTransport({
    start: async () => ({ streamId: 'stream-1' }),
    stop: (ticket) => stopped.push(ticket),
    open: (ticket, signal) => {
      signalSeen = signal;
      // As frames() does: the stream errors with the signal's reason.
      return new ReadableStream({
        start(controller) {
          for (const chunk of TEXT) controller.enqueue(chunk);
          signal.addEventListener('abort', () => controller.error(signal.reason));
        },
      });
    },
  });
  const chat = new HeadlessChat({ transport });
  const sent = chat.sendMessage({ text: 'hi' });
  while (describe(chat).text !== 'Ciao da Napoli.') await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(stopped, []);
  await chat.stop();
  await sent;
  assert.deepEqual(stopped, [{ streamId: 'stream-1' }]);
  assert.equal(signalSeen.aborted, true);
  assert.deepEqual(describe(chat), { status: 'ready', error: null, text: 'Ciao da Napoli.' });
});

test('stop is not called for a run that finished, nor when the reader cancels', async () => {
  const stopped = [];
  const controller = new AbortController();
  const transport = transportOver([...TEXT, ...FINISH], undefined, { stop: (ticket) => stopped.push(ticket) });
  await drain(await transport.sendMessages({ ...request, abortSignal: controller.signal }));
  const cancelled = (await transport.sendMessages({ ...request, abortSignal: controller.signal })).getReader();
  await cancelled.read();
  await cancelled.cancel();
  const failedStream = await transportOver(TEXT, failed('publisher_idle'), { stop: (ticket) => stopped.push(ticket) }).sendMessages({ ...request, abortSignal: controller.signal });
  await drain(failedStream);
  controller.abort();
  assert.deepEqual(stopped, []);
});

test('cancelling the transport\'s stream cancels the stream it reads', async () => {
  let cancelledWith;
  const transport = chatTransport({
    start: async () => ({ url: 'unused' }),
    open: () => new ReadableStream({ start(controller) { controller.enqueue(TEXT[0]); }, cancel(reason) { cancelledWith = reason; } }),
  });
  const reader = (await transport.sendMessages(request)).getReader();
  await reader.read();
  await reader.cancel('left the page');
  assert.equal(cancelledWith, 'left the page');
});

test('the default open reads IQ frames from the ticket\'s url and parses them as JSON', async (t) => {
  useFakeEventSource(t);
  const url = 'http://iq.test/stream-0123456789abcdef?token=t';
  const chat = new HeadlessChat({ transport: chatTransport({ start: async () => ({ url }) }) });
  const sent = chat.sendMessage({ text: 'hi' });
  while (FakeEventSource.instances.length === 0) await new Promise((resolve) => setImmediate(resolve));
  const source = FakeEventSource.instances[0];
  assert.equal(source.url, url);
  source.open();
  [...TEXT, { type: 'error', errorText: 'provider outage' }].forEach((chunk, index) => source.frame(JSON.stringify(chunk), { id: String(index + 1) }));
  source.terminal('iq:fail', { reason: 'publisher', lastEventId: 6 });
  await sent;
  assert.deepEqual(describe(chat), { status: 'error', error: 'provider outage', text: 'Ciao da Napoli.' });
  assert.equal(source.closeCalls, 1);
});

test('the default open: a frame that is not JSON errors the chat', async (t) => {
  useFakeEventSource(t);
  const chat = new HeadlessChat({ transport: chatTransport({ start: async () => ({ url: 'http://iq.test/s' }) }) });
  const sent = chat.sendMessage({ text: 'hi' });
  while (FakeEventSource.instances.length === 0) await new Promise((resolve) => setImmediate(resolve));
  FakeEventSource.instances[0].frame('not json', { id: '1' });
  await sent;
  assert.equal(chat.status, 'error');
  assert.ok(chat.error instanceof SyntaxError);
  assert.equal(FakeEventSource.instances[0].closeCalls, 1);
});
