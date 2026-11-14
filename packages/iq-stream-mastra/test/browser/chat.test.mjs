// One end-to-end proof, in Chromium: React's useChat over the core's chatTransport, against
// iqChatRoute on Mastra's own server, which runs a Mastra agent (on Mastra's mock model) and publishes
// to the real IQ-Stream server with the real IQClient. The assertions are on what the page renders.
import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { chromium } from 'playwright';
import { startBackend } from './support/backend.mjs';

const QUESTION = 'What is the weather in Paris?';
const ANSWER = 'It is 21°C and sunny in Paris.';

let backend, browser, page;
const pageErrors = [];
const requests = [];
before(async () => {
  backend = await startBackend();
  browser = await chromium.launch(process.env.IQ_STREAM_TEST_CHROMIUM ? { executablePath: process.env.IQ_STREAM_TEST_CHROMIUM } : {});
  page = await browser.newPage();
  page.on('pageerror', (error) => pageErrors.push(error.message));
  page.on('request', (request) => {
    const { pathname } = new URL(request.url());
    if (pathname.startsWith('/iq/')) requests.push(`${request.method()} ${pathname}`);
  });
});
after(async () => {
  await browser?.close();
  await backend?.close();
});

let chats = 0;
/** Opens the page for a new chat (or the one given) and returns the chat's id. */
async function openChat(agent, { resume = false, chatId = `chat-${++chats}`, route = 'chat' } = {}) {
  requests.length = 0;
  await page.goto(`${backend.origin}/page?${new URLSearchParams({ agent, route, chat: chatId, ...(resume ? { resume: '' } : {}) })}`);
  await page.waitForSelector('#send');
  return chatId;
}
const isStart = (response) => response.request().method() === 'POST' && /^\/iq\/[a-z]+\/[a-z]+$/.test(new URL(response.url()).pathname);
/** Clicks send and returns the ticket the route answered. */
async function send() {
  const response = page.waitForResponse(isStart);
  await page.click('#send');
  return (await response).json();
}
const state = () => page.evaluate(() => ({
  status: document.querySelector('#status').textContent,
  error: document.querySelector('#error').textContent,
  roles: [...document.querySelectorAll('#messages > div')].map((message) => message.dataset.role),
  tools: [...document.querySelectorAll('.tool')].map((tool) => `${tool.firstChild.textContent}:${tool.dataset.state}`),
  texts: [...document.querySelectorAll('.text')].map((text) => text.textContent),
}));
const status = (expected) => page.waitForFunction((value) => document.querySelector('#status').textContent === value, expected, { timeout: 20_000 });
/** Waits until the answer has begun. */
const answerBegun = () => page.waitForFunction(() => [...document.querySelectorAll('.text')].some((text) => text.textContent.startsWith('It is')), null, { timeout: 20_000 });

test('an answer streams into the page with its tool call', { timeout: 60_000 }, async () => {
  await openChat('weather');
  const ticket = await send();
  await page.waitForSelector('.tool');
  await status('ready');
  assert.deepEqual(await state(), {
    status: 'ready', error: '', roles: ['user', 'assistant'],
    tools: ['tool-getWeather:output-available'], texts: [QUESTION, ANSWER],
  });
  assert.deepEqual(requests, ['POST /iq/chat/weather']);
  const result = await backend.done(ticket.streamId);
  assert.deepEqual([result.outcome, result.framesSent, result.publish.status], ['end', 20, 200]);
});

test('a run that fails mid-answer: the page shows the masked error and what came before it', { timeout: 60_000 }, async () => {
  await openChat('broken');
  const ticket = await send();
  await status('error');
  assert.deepEqual(await state(), {
    status: 'error', error: 'An error occurred.', roles: ['user', 'assistant'],
    tools: ['tool-getWeather:output-available'], texts: [QUESTION, 'It is 21°C '],
  });
  const result = await backend.done(ticket.streamId);
  assert.deepEqual([result.outcome, result.cause, result.publish.reason], ['fail', 'error', 'publisher']);
});

test('the page is reloaded mid-answer: resume asks the route\'s GET and the answer is rebuilt from the replay', { timeout: 60_000 }, async () => {
  const chatId = await openChat('slow');
  const ticket = await send();
  await answerBegun();
  assert.notEqual((await state()).texts.at(-1), ANSWER);

  await openChat('slow', { resume: true, chatId });
  await answerBegun();
  await status('ready');
  // The reloaded page holds no messages of its own: all it shows came through the reconnect.
  assert.deepEqual(await state(), {
    status: 'ready', error: '', roles: ['assistant'], tools: ['tool-getWeather:output-available'], texts: [ANSWER],
  });
  assert.deepEqual(requests, [`GET /iq/chat/slow/${chatId}/stream`]);

  // Once the run is over the route has nothing to reconnect to, and the page stays empty.
  await backend.done(ticket.streamId);
  const answered = page.waitForResponse((response) => response.url().includes(`/iq/chat/slow/${chatId}/stream`));
  await openChat('slow', { resume: true, chatId });
  assert.equal((await answered).status(), 204);
  await status('ready');
  assert.deepEqual((await state()).roles, []);
});

test('stop: the page asks the abort route, the run is cancelled, and the chat is ready without an error', { timeout: 60_000 }, async () => {
  await openChat('slow');
  const ticket = await send();
  await answerBegun();
  await page.click('#stop');
  await status('ready');
  const { error, texts } = await state();
  assert.equal(error, '');
  assert.ok(texts.at(-1).startsWith('It is') && texts.at(-1) !== ANSWER, texts.at(-1));
  assert.deepEqual(requests, ['POST /iq/chat/slow', 'POST /iq/chat/slow/abort']);
  // The run ended early, cancelled, and its stream closed with fail.
  const result = await backend.done(ticket.streamId);
  assert.deepEqual([result.outcome, result.cause], ['fail', 'abort']);
});

test('a tool that needs approval: the card, then the answer on a second stream once the user approves', { timeout: 60_000 }, async () => {
  await openChat('careful', { route: 'approve' });
  const first = await send();
  await page.waitForSelector('#approve');
  await status('ready');
  assert.deepEqual(await state(), {
    status: 'ready', error: '', roles: ['user', 'assistant'], tools: ['tool-getWeather:approval-requested'], texts: [QUESTION],
  });
  const suspended = await backend.done(first.streamId);
  assert.deepEqual([suspended.outcome, suspended.suspended], ['end', true]);

  // Approving sends the messages again: the route answers a second ticket, for a second stream.
  const response = page.waitForResponse(isStart);
  await page.click('#approve');
  const second = await (await response).json();
  await page.waitForFunction((answer) => [...document.querySelectorAll('.text')].some((text) => text.textContent === answer), ANSWER, { timeout: 20_000 });
  await status('ready');
  assert.deepEqual(await state(), {
    status: 'ready', error: '', roles: ['user', 'assistant'], tools: ['tool-getWeather:output-available'], texts: [QUESTION, ANSWER],
  });
  assert.notEqual(second.streamId, first.streamId);
  assert.equal(second.runId, first.runId);
  assert.deepEqual(requests, ['POST /iq/approve/careful', 'POST /iq/approve/careful']);
  const resumed = await backend.done(second.streamId);
  assert.deepEqual([resumed.outcome, resumed.suspended], ['end', false]);
});

test('a tool result over the frame limit: the page gets the placeholder in onData, and the rest of the answer', { timeout: 60_000 }, async () => {
  await openChat('verbose');
  const ticket = await send();
  await status('ready');
  assert.deepEqual(await state(), {
    status: 'ready', error: '', roles: ['user', 'assistant'], tools: ['tool-getWeather:input-available'], texts: [QUESTION, ANSWER],
  });
  assert.equal(await page.evaluate(() => document.querySelector('#data').textContent), 'data-iq-omitted:tool-output-available');
  const result = await backend.done(ticket.streamId);
  assert.deepEqual([result.outcome, result.framesOmitted], ['end', 1]);
});

test('no uncaught error in the page, and none reported by the publisher', () => {
  assert.deepEqual(pageErrors, []);
  assert.deepEqual(backend.errors, []);
});
