// End to end in Chromium: React's useChat over the core's chatTransport, against a backend that runs a
// createAgent agent on the scripted model and publishes it through the adapter with the real IQClient.
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import http from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, before, test } from 'node:test';
import { build } from 'esbuild';
import { chromium } from 'playwright';
import { IQClient } from '@neuron-core/iq-stream';
import { testServers } from '@neuron-core/iq-stream/testing';
import { ANSWER, buildAgent, failingTurns, happyTurns } from '../fakes/agent.mjs';
import { chatBackend } from './support/chat-backend.mjs';

const SUPPORT = join(import.meta.dirname, 'support');
const QUESTION = 'What is the weather in Paris?';
const PREAMBLE = 'Let me check the weather. ';

// `slow` paces the model so that a test can act while the answer is being written.
const agents = {
  ok: () => buildAgent(),
  fail: () => buildAgent({ turns: failingTurns() }),
  slow: () => buildAgent({ turns: happyTurns(), delayMs: 250 }),
};

let servers, bundles, app, origin, browser, page, backend;
const pageErrors = [];
const logged = [];   // what the backend's onError was told

before(async () => {
  servers = await testServers();
  // minOpen keeps its default: a run that ends before the page has subscribed is still delivered.
  const iq = new IQClient({ url: servers.default.url, appKey: servers.appKey, appSecret: servers.appSecret });
  backend = chatBackend({ iq, agentFor: (scenario) => agents[scenario](), log: (line) => logged.push(line) });

  bundles = await mkdtemp(join(tmpdir(), 'iq-stream-langchain-'));
  await build({ entryPoints: [join(SUPPORT, 'chat.jsx')], outdir: bundles, bundle: true, format: 'esm', platform: 'browser', jsx: 'automatic', logLevel: 'error' });

  app = http.createServer(async (request, response) => {
    const url = new URL(request.url, 'http://app');
    const json = (value) => {
      response.writeHead(value === null ? 204 : 200, { 'Content-Type': 'application/json' });
      response.end(value === null ? undefined : JSON.stringify(value));
    };
    try {
      const resume = /^\/chat\/([^/]+)\/stream$/.exec(url.pathname);
      const stop = /^\/chat\/stop\/([^/]+)$/.exec(url.pathname);
      if (url.pathname === '/chat' && request.method === 'POST') {
        let body = '';
        for await (const chunk of request) body += chunk;
        return json(await backend.start(JSON.parse(body), url.searchParams.get('scenario') ?? 'ok'));
      }
      if (resume) return json(await backend.resume(resume[1]));
      if (stop) return json(backend.stop(stop[1]) ?? {});
      const file = url.pathname === '/chat.js' ? join(bundles, 'chat.js') : join(SUPPORT, 'chat.html');
      response.writeHead(200, { 'Content-Type': file.endsWith('.js') ? 'text/javascript' : 'text/html', 'Cache-Control': 'no-store' });
      response.end(await readFile(file));
    } catch (error) {
      response.writeHead(500);
      response.end(String(error));
    }
  });
  await new Promise((resolve) => app.listen(0, '127.0.0.1', resolve));
  origin = `http://127.0.0.1:${app.address().port}`;

  // IQ_STREAM_TEST_CHROMIUM: a Chromium binary to use in place of the one `playwright install` downloads.
  browser = await chromium.launch(process.env.IQ_STREAM_TEST_CHROMIUM ? { executablePath: process.env.IQ_STREAM_TEST_CHROMIUM } : {});
  page = await browser.newPage();
  page.on('pageerror', (error) => pageErrors.push(error.message));
});

after(async () => {
  await browser?.close();
  for (const run of backend?.running.values() ?? []) run.controller.abort();
  app?.closeAllConnections();
  await new Promise((resolve) => (app ? app.close(resolve) : resolve()));
  if (bundles) await rm(bundles, { recursive: true, force: true });
  await servers?.stop();
});

let chats = 0;
async function openChat(query = '', chatId = `chat-${++chats}`) {
  await page.goto(`${origin}/?chat=${chatId}${query}`);
  await page.waitForSelector('#send');
  return chatId;
}
const state = () => page.evaluate(() => ({
  status: document.querySelector('#status').textContent,
  error: document.querySelector('#error').textContent,
  roles: [...document.querySelectorAll('#messages > div')].map((message) => message.dataset.role),
  tools: [...document.querySelectorAll('.tool')].map((tool) => `${tool.textContent} [${tool.dataset.state}]`),
  texts: [...document.querySelectorAll('.text')].map((text) => text.textContent),
}));
const status = (expected) => page.waitForFunction((value) => document.querySelector('#status').textContent === value, expected, { timeout: 20_000 });
/** Waits until the answer after the tool call has begun, and no more than begun. */
const answerBegun = () => page.waitForFunction(() => [...document.querySelectorAll('.text')].some((text) => text.textContent.startsWith('It is')), null, { timeout: 20_000 });
const TOOL = 'get_weather({"city":"Paris"}) = "Sunny, 21 degrees in Paris" [output-available]';

test('a streamed answer with its tool call', { timeout: 60_000 }, async () => {
  await openChat();
  await page.click('#send');
  await page.waitForSelector('.tool');
  await status('ready');
  assert.deepEqual(await state(), {
    status: 'ready', error: '', roles: ['user', 'assistant'], tools: [TOOL], texts: [QUESTION, PREAMBLE, ANSWER],
  });
});

test('a failing run: the page shows the masked error, and what was streamed before it', { timeout: 60_000 }, async () => {
  await openChat('&scenario=fail');
  await page.click('#send');
  await status('error');
  assert.deepEqual(await state(), {
    status: 'error', error: 'An error occurred.', roles: ['user', 'assistant'], tools: [TOOL], texts: [QUESTION, PREAMBLE],
  });
  // The real message went to the application, not to the page.
  assert.deepEqual(logged, [`chat chat-${chats}: fake provider outage`]);
});

test('the page is reloaded mid-answer: useChat({ resume: true }) rebuilds the message and follows it to its end', { timeout: 60_000 }, async () => {
  const chatId = await openChat('&scenario=slow');
  await page.click('#send');
  await answerBegun();
  assert.notEqual((await state()).texts.at(-1), ANSWER);

  await openChat('&scenario=slow&resume', chatId);
  await answerBegun();
  await status('ready');
  assert.deepEqual(await state(), {
    status: 'ready', error: '', roles: ['assistant'], tools: [TOOL], texts: [PREAMBLE, ANSWER],
  });
  // The run is over: a later reload finds nothing to resume.
  assert.equal(backend.running.has(chatId), false);
});

test('stop: the application aborts the run, and the page ends without an error', { timeout: 60_000 }, async () => {
  const chatId = await openChat('&scenario=slow');
  await page.click('#send');
  await answerBegun();
  const run = backend.running.get(chatId);
  await page.click('#stop');
  await status('ready');
  const { error, texts } = await state();
  assert.equal(error, '');
  assert.ok(texts.at(-1).startsWith('It is') && texts.at(-1) !== ANSWER, texts.at(-1));
  // The run was cancelled in the backend, not merely unsubscribed from, and its stream closed with fail.
  const result = await run.done;
  assert.deepEqual([result.outcome, result.error.name], ['fail', 'AbortError']);
  assert.equal(backend.running.has(chatId), false);
});

test('no scenario left an uncaught error in the page', () => {
  assert.deepEqual(pageErrors, []);
});
