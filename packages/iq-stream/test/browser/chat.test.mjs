// React's useChat over chatTransport, and a plain @ag-ui/client page over StreamAgent, in Chromium against
// the real servers. esbuild bundles each page from the built package, resolved by name through its export map.
import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { startBrowser } from './support/browser.mjs';

let env, page, backend;
before(async () => {
  env = await startBrowser({ entries: ['chat.jsx', 'agui.js'] });
  ({ page, backend } = env);
});
after(async () => { await env?.close(); });

const ANSWER = 'It is sunny in Naples, 24 degrees.';
let chats = 0;
/** Opens the chat page for a new chat (or the one given) and returns the chat's id. */
async function openChat(query = '', chatId = `chat-${++chats}`) {
  await page.goto(`${backend.origin}/pages/chat.html?chat=${chatId}${query}`);
  await page.waitForSelector('#send');
  return chatId;
}
const state = () => page.evaluate(() => ({
  status: document.querySelector('#status').textContent,
  error: document.querySelector('#error').textContent,
  kind: document.querySelector('#error').dataset.kind,
  roles: [...document.querySelectorAll('#messages > div')].map((message) => message.dataset.role),
  tools: [...document.querySelectorAll('.tool')].map((tool) => `${tool.textContent}:${tool.dataset.state}`),
  texts: [...document.querySelectorAll('.text')].map((text) => text.textContent),
}));
const status = (expected) => page.waitForFunction((value) => document.querySelector('#status').textContent === value, expected, { timeout: 20_000 });
/** Waits until the answer has begun, and no more than begun. */
const answerBegun = () => page.waitForFunction(() => [...document.querySelectorAll('.text')].some((text) => text.textContent.startsWith('It is')), null, { timeout: 20_000 });

test('C1 send, stream, finish', { timeout: 60_000 }, async () => {
  await openChat();
  await page.click('#send');
  await page.waitForSelector('.tool');
  await status('ready');
  assert.deepEqual(await state(), {
    status: 'ready', error: '', kind: '', roles: ['user', 'assistant'],
    tools: ['tool-getWeather:output-available'], texts: ['Weather in Naples?', ANSWER],
  });
});

test('C2 the run fails mid-answer: the error is the one of its own chunk', { timeout: 60_000 }, async () => {
  await openChat('&scenario=fail');
  await page.click('#send');
  await status('error');
  assert.deepEqual(await state(), {
    status: 'error', error: 'An error occurred.', kind: '', roles: ['user', 'assistant'],
    tools: ['tool-getWeather:output-available'], texts: ['Weather in Naples?', 'It is sunny'],
  });
});

test('C3 the page is reloaded mid-answer: useChat({ resume: true }) rebuilds the message from the replay', { timeout: 60_000 }, async () => {
  const chatId = await openChat('&pace=300');
  await page.click('#send');
  await answerBegun();
  const before = (await state()).texts.at(-1);
  assert.notEqual(before, ANSWER);
  await openChat('&pace=300&resume', chatId);
  await answerBegun();
  await status('ready');
  assert.deepEqual(await state(), {
    status: 'ready', error: '', kind: '', roles: ['assistant'], tools: ['tool-getWeather:output-available'], texts: [ANSWER],
  });
});

test('C4 every connection is cut mid-answer: the answer still arrives whole, once', { timeout: 60_000 }, async () => {
  await openChat('&pace=300');
  await page.click('#send');
  await answerBegun();
  assert.ok(backend.drop() > 0);
  await status('ready');
  assert.deepEqual(await state(), {
    status: 'ready', error: '', kind: '', roles: ['user', 'assistant'],
    tools: ['tool-getWeather:output-available'], texts: ['Weather in Naples?', ANSWER],
  });
});

test('C5 stop: the page tells the application, which cancels the run, and the chat is ready without an error', { timeout: 60_000 }, async () => {
  const chatId = await openChat('&pace=300');
  await page.click('#send');
  await answerBegun();
  const run = backend.runs.get(chatId);
  assert.equal(run.finished, false);
  await page.click('#stop');
  await status('ready');
  // The backend was asked to cancel this run, and closed its stream with the cancel chunk and fail.
  const { status: answered, body } = await run.done;
  assert.ok(backend.stops.includes(run.streamId));
  assert.deepEqual([answered, body.closed, body.outcome, body.reason], [200, true, 'fail', 'publisher']);
  const { error, texts } = await state();
  assert.equal(error, '');
  assert.ok(texts.at(-1).startsWith('It is') && texts.at(-1) !== ANSWER, texts.at(-1));
});

test('C6 a reconnect that arrives after the close gets iq:gone: the chat reports frames it cannot have', { timeout: 60_000 }, async () => {
  const chatId = await openChat('&pace=150&server=short');
  await page.click('#send');
  await answerBegun();
  // Offline for the IQ server until the run is over: the reconnect then finds a closed stream.
  backend.block(true);
  backend.drop();
  await backend.runs.get(chatId).done;
  backend.block(false);
  await status('error');
  const { error, kind, texts } = await state();
  assert.deepEqual([error, kind], ['stream gone (end)', 'gone']);
  assert.notEqual(texts.at(-1), ANSWER);
});

test('C7 another tab that watches a stopped run ends without an error', { timeout: 60_000 }, async () => {
  const chatId = await openChat('&pace=300');
  await page.click('#send');
  await answerBegun();
  const watcher = await env.browser.newPage();
  try {
    await watcher.goto(`${backend.origin}/pages/chat.html?chat=${chatId}&resume`);
    await watcher.waitForFunction(() => [...document.querySelectorAll('.text')].some((text) => text.textContent.startsWith('It is')), null, { timeout: 20_000 });
    await page.click('#stop');
    // The watcher receives the abort chunk and then iq:fail publisher: one ending, not an error.
    await watcher.waitForFunction(() => document.querySelector('#status').textContent === 'ready', null, { timeout: 20_000 });
    assert.equal(await watcher.evaluate(() => document.querySelector('#error').textContent), '');
  } finally {
    await watcher.close();
  }
});

test('A1 StreamAgent in a page: a run through runAgent()', { timeout: 60_000 }, async () => {
  await page.goto(`${backend.origin}/pages/agui.html`);
  await page.waitForFunction(() => typeof window.runAgent === 'function');
  const { outcome, events, messages } = await page.evaluate(() => window.runAgent('ok'));
  assert.equal(outcome, 'resolved');
  assert.equal(events.length, 10);
  assert.deepEqual([events[0], events.at(-1)], ['RUN_STARTED', 'RUN_FINISHED']);
  assert.deepEqual(messages, [
    { user: 'Weather in Naples?' }, { assistant: null, toolCalls: ['getWeather'] }, { tool: 'sunny, 24' }, { assistant: 'It is sunny in Naples.' },
  ]);
});

test('A2 StreamAgent in a page: a failing run ends with RUN_ERROR, not with a second error', { timeout: 60_000 }, async () => {
  await page.goto(`${backend.origin}/pages/agui.html`);
  await page.waitForFunction(() => typeof window.runAgent === 'function');
  const { outcome, events, messages } = await page.evaluate(() => window.runAgent('fail'));
  assert.equal(outcome, 'resolved');
  assert.equal(events.at(-1), 'RUN_ERROR');
  assert.deepEqual(messages.at(-1), { assistant: 'It is sunny ' });
});

test('no scenario left an uncaught error in the page', () => {
  assert.deepEqual(env.pageErrors, []);
});
