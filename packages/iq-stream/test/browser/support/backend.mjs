// The application backend of the browser tests: it mints tickets, publishes scripted streams to the two
// IQ-Stream servers with a few lines of node:http, serves the pages and the built package, and runs a TCP
// proxy in front of each server so that a test can cut every connection, or keep new ones out.
import { createHmac, randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import http from 'node:http';
import net from 'node:net';
import { extname, join, normalize } from 'node:path';

const KEY = process.env.IQ_STREAM_TEST_APP_KEY ?? 'demo-app';
const SECRET = process.env.IQ_STREAM_TEST_APP_SECRET ?? 'test-app-secret-0123456789abcdef0123';
const UPSTREAMS = {
  default: new URL(process.env.IQ_STREAM_TEST_URL ?? 'http://127.0.0.1:8080'),
  short: new URL(process.env.IQ_STREAM_TEST_SHORT_URL ?? 'http://127.0.0.1:8081'),
};
const PACKAGE = join(import.meta.dirname, '../../..');
const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.mjs': 'text/javascript' };

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const json = (value) => JSON.stringify(value);
export const newStreamId = () => randomBytes(16).toString('base64url');

export function mint(scope, stream, ttl = 600) {
  const body = Buffer.from(json({ app_key: KEY, stream, exp: Math.floor(Date.now() / 1000) + ttl })).toString('base64url');
  return `${body}.${createHmac('sha256', SECRET).update(`${scope}.${body}`).digest('base64url')}`;
}

/** A subscribe URL straight to a server, for tests that run in Node. */
export const subscribeUrl = (streamId, server = 'default', ttl = 600) => `${UPSTREAMS[server].origin}/${streamId}?token=${mint('sub', streamId, ttl)}`;

/**
 * One chunked POST held open. A step is [delayMs, data, event?] or [delayMs, { close }], where close is
 * 'end', 'fail' or null (the publisher leaves without a close line). `cancel()` makes the run stop at its
 * next step and send `onCancel` instead. `done` resolves with the server's answer.
 */
export function publish(streamId, steps, { server = 'default', onCancel = [[0, { close: 'fail' }]] } = {}) {
  let cancelled = false;
  const done = new Promise((resolve) => {
    const { hostname, port } = UPSTREAMS[server];
    const request = http.request({
      host: hostname, port, path: `/${streamId}`, method: 'POST', agent: false,
      headers: { Authorization: `Bearer ${mint('pub', streamId)}`, 'Content-Type': 'application/x-ndjson' },
    }, (response) => {
      let body = '';
      response.on('data', (chunk) => { body += chunk; });
      response.on('end', () => resolve({ status: response.statusCode, body: body ? JSON.parse(body) : null }));
    });
    request.on('error', () => resolve({ status: 0, body: null }));
    request.setNoDelay(true);
    request.flushHeaders();
    (async () => {
      const send = async (list) => {
        for (const [delay, data, event] of list) {
          await sleep(delay);
          if (cancelled && list === steps) return false;
          if (data && typeof data === 'object') {
            if (data.close) request.write(`${json({ close: data.close })}\n`);
            return true;
          }
          request.write(`${json(event ? { event, data } : { data })}\n`);
        }
        return true;
      };
      if (!(await send(steps))) await send(onCancel);
      request.end();
    })();
  });
  return { streamId, done, cancel() { cancelled = true; } };
}

const letters = (count, interval, close) => [...Array.from({ length: count }, (_, i) => [interval, `f${i + 1}`]), [interval, { close }]];

/** A run as UI message chunks of the AI SDK: a tool call, then a sentence word by word. */
function uiChunks(scenario, pace) {
  const words = 'It is sunny in Naples, 24 degrees.'.split(' ');
  const steps = [
    [0, json({ type: 'start', messageId: 'm1' })], [pace, json({ type: 'start-step' })],
    [pace, json({ type: 'tool-input-available', toolCallId: 'c1', toolName: 'getWeather', input: { city: 'Naples' } })],
    [pace * 3, json({ type: 'tool-output-available', toolCallId: 'c1', output: { sky: 'sunny', celsius: 24 } })],
    [pace, json({ type: 'finish-step' })], [pace, json({ type: 'start-step' })], [pace, json({ type: 'text-start', id: 't1' })],
  ];
  for (const [index, word] of words.entries()) {
    if (scenario === 'fail' && index === 3) return [...steps, [pace, json({ type: 'error', errorText: 'An error occurred.' })], [0, { close: 'fail' }]];
    steps.push([pace, json({ type: 'text-delta', id: 't1', delta: (index ? ' ' : '') + word })]);
  }
  return [...steps, [pace, json({ type: 'text-end', id: 't1' })], [pace, json({ type: 'finish-step' })],
    [pace, json({ type: 'finish', finishReason: 'stop' })], [0, { close: 'end' }]];
}

/** A run as AG-UI events: a tool call and a text message. */
function agUiEvents(input, scenario) {
  const run = { threadId: input.threadId, runId: input.runId };
  const event = (value) => [40, json(value)];
  const steps = [
    event({ type: 'RUN_STARTED', ...run }),
    event({ type: 'TOOL_CALL_START', toolCallId: 'c1', toolCallName: 'getWeather', parentMessageId: 'a0' }),
    event({ type: 'TOOL_CALL_ARGS', toolCallId: 'c1', delta: '{"city":"Naples"}' }),
    event({ type: 'TOOL_CALL_END', toolCallId: 'c1' }),
    event({ type: 'TOOL_CALL_RESULT', messageId: 'r1', toolCallId: 'c1', content: 'sunny, 24', role: 'tool' }),
    event({ type: 'TEXT_MESSAGE_START', messageId: 'a1', role: 'assistant' }),
    event({ type: 'TEXT_MESSAGE_CONTENT', messageId: 'a1', delta: 'It is sunny ' }),
  ];
  if (scenario === 'fail') {
    return [...steps, event({ type: 'TEXT_MESSAGE_END', messageId: 'a1' }), event({ type: 'RUN_ERROR', message: 'provider outage' }), [0, { close: 'fail' }]];
  }
  return [...steps, event({ type: 'TEXT_MESSAGE_CONTENT', messageId: 'a1', delta: 'in Naples.' }), event({ type: 'TEXT_MESSAGE_END', messageId: 'a1' }),
    event({ type: 'RUN_FINISHED', ...run }), [0, { close: 'end' }]];
}

const listen = (server) => new Promise((resolve) => { server.listen(0, '127.0.0.1', () => resolve(server.address().port)); });

/**
 * Starts the backend. `bundles` is a directory served under /bundle/ (what esbuild wrote for a test).
 * Returns its origin and, for the tests themselves: `runs` (chat id -> the run started for it),
 * `stops` (stream ids the page asked to cancel), `drop()`, `block(on)` and `close()`.
 */
export async function startBackend({ bundles } = {}) {
  const runs = new Map();        // chat id -> { streamId, done, cancel, finished }
  const byStream = new Map();    // stream id -> the same run
  const stops = [];
  const sockets = new Set();
  const proxies = {};
  let blocked = false;

  for (const [name, upstream] of Object.entries(UPSTREAMS)) {
    const proxy = net.createServer((client) => {
      if (blocked) return client.destroy();
      const server = net.connect(Number(upstream.port), upstream.hostname);
      for (const socket of [client, server]) {
        sockets.add(socket);
        socket.on('close', () => sockets.delete(socket));
        socket.on('error', () => {});
      }
      client.pipe(server);
      server.pipe(client);
      client.on('close', () => server.destroy());
      server.on('close', () => client.destroy());
    });
    proxies[name] = { server: proxy, port: await listen(proxy) };
  }

  /** What the page needs to subscribe: through the proxy, so that a test can cut the connection. */
  const ticket = (streamId, server = 'default', ttl = 600) => {
    const token = mint('sub', streamId, ttl);
    const bare = `http://127.0.0.1:${proxies[server].port}/${streamId}`;
    return { streamId, token, url: `${bare}?token=${token}`, bare };
  };
  const drop = () => {
    const count = sockets.size;
    for (const socket of sockets) socket.destroy();
    return count;
  };
  const start = (chatId, streamId, steps, options) => {
    const run = publish(streamId, steps, options);
    run.finished = false;
    if (chatId) runs.set(chatId, run);
    byStream.set(streamId, run);
    run.done.then(() => { run.finished = true; });
    return run;
  };
  const body = async (request) => {
    let text = '';
    for await (const chunk of request) text += chunk;
    return text ? JSON.parse(text) : {};
  };

  const app = http.createServer(async (request, response) => {
    const url = new URL(request.url, 'http://backend');
    const query = url.searchParams;
    const answer = (value, status = 200) => {
      response.writeHead(status, { 'Content-Type': 'application/json' });
      response.end(json(value));
    };
    try {
      const server = query.get('server') ?? 'default';
      if (url.pathname === '/api/publish') {
        const streamId = query.get('id') ?? newStreamId();
        const close = query.get('close') === 'none' ? null : (query.get('close') ?? 'end');
        const steps = query.get('named') ? [[50, 'plain'], [50, 'named', 'token'], [50, { close: 'end' }]]
          : letters(Number(query.get('n') ?? 5), Number(query.get('interval') ?? 100), close);
        const run = start(null, streamId, steps, { server });
        if (query.get('wait')) await run.done;
        return answer(ticket(streamId, server));
      }
      if (url.pathname === '/api/ticket') return answer(ticket(query.get('id') ?? newStreamId(), server, Number(query.get('ttl') ?? 600)));
      if (url.pathname === '/api/drop') return answer({ dropped: drop() });
      if (url.pathname === '/api/chat' && request.method === 'POST') {
        const { id } = await body(request);
        const run = start(id, newStreamId(), uiChunks(query.get('scenario') ?? 'ok', Number(query.get('pace') ?? 60)),
          { server, onCancel: [[0, json({ type: 'abort' })], [0, { close: 'fail' }]] });
        return answer(ticket(run.streamId, server));
      }
      if (url.pathname === '/api/chat/stop' && request.method === 'POST') {
        const streamId = query.get('stream');
        stops.push(streamId);
        byStream.get(streamId)?.cancel();
        return answer({});
      }
      const resume = /^\/api\/chat\/([^/]+)\/stream$/.exec(url.pathname);
      if (resume) {
        const run = runs.get(resume[1]);
        if (!run || run.finished) {
          response.writeHead(204);
          return response.end();
        }
        return answer(ticket(run.streamId, server));
      }
      if (url.pathname === '/api/agui' && request.method === 'POST') {
        const run = start(null, newStreamId(), agUiEvents(await body(request), query.get('scenario') ?? 'ok'));
        return answer(ticket(run.streamId));
      }
      // Static files: the pages, the built package as it will be published, and the bundles of this run.
      const [, root, ...rest] = normalize(url.pathname).split('/');
      const base = { dist: join(PACKAGE, 'dist'), pages: join(import.meta.dirname, 'pages'), bundle: bundles }[root];
      const file = url.pathname === '/' ? join(import.meta.dirname, 'pages/blank.html') : base && join(base, ...rest);
      if (!file) throw new Error('not found');
      const data = await readFile(file);
      response.writeHead(200, { 'Content-Type': TYPES[extname(file)] ?? 'application/octet-stream', 'Cache-Control': 'no-store' });
      response.end(data);
    } catch {
      response.writeHead(404);
      response.end();
    }
  });
  const port = await listen(app);

  return {
    origin: `http://127.0.0.1:${port}`,
    runs, stops, ticket, drop,
    /** While on, the proxies refuse new connections: the page is offline for the IQ servers. */
    block(on) { blocked = on; },
    /** Starts a run from the test itself. */
    publish: (streamId, steps, options) => start(null, streamId, steps, options),
    async close() {
      await Promise.all([...byStream.values()].map((run) => { run.cancel(); return run.done; }));
      drop();
      app.closeAllConnections();
      await Promise.all([app, ...Object.values(proxies).map((proxy) => proxy.server)].map((server) => new Promise((resolve) => server.close(resolve))));
    },
  };
}
