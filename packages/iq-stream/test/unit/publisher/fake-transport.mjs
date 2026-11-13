// A scripted transport: the test plays the server, one answer at a time, and reads what the writer wrote.
import { IQClientCore } from '../../../dist/publisher/client.js';

export const SECRET = 'test-app-secret-0123456789abcdef0123';
export const STREAM = 'Q2hhdC1zdHJlYW0tMDAwMQ';

const decoder = new TextDecoder();
export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export async function until(check, what = 'the condition', ms = 3000) {
  for (const deadline = Date.now() + ms; ; await sleep(5)) {
    const value = check();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
  }
}

export class FakeRequest {
  /** Every line written, without its newline; '' is a keepalive. */
  lines = [];
  ended = false;
  destroyed = false;
  settled = false;
  /** When true, writes do not complete: a server that stopped reading. */
  stalled = false;
  /** Writes made after the answer: the writer must never make one. */
  lateWrites = 0;
  #resolve;
  #pending = [];
  answer = new Promise((resolve) => (this.#resolve = resolve));

  constructor(streamId, token) {
    this.streamId = streamId;
    this.token = token;
  }

  write(bytes) {
    if (this.settled) {
      this.lateWrites++;
      return Promise.resolve();
    }
    this.lines.push(...decoder.decode(bytes).split('\n').slice(0, -1));
    return this.stalled ? new Promise((resolve) => this.#pending.push(resolve)) : Promise.resolve();
  }

  end() {
    this.ended = true;
  }

  destroy() {
    this.destroyed = true;
    this.respond(0);
  }

  /** The server answers; status 0 is a connection lost without an answer. */
  respond(status, body = {}) {
    if (this.settled) return;
    this.settled = true;
    this.#resolve({ status, body });
    for (const resolve of this.#pending.splice(0)) resolve();
  }

  get objects() {
    return this.lines.filter((line) => line !== '').map((line) => JSON.parse(line));
  }

  /** The data of the frames written, in order. */
  get data() {
    return this.objects.filter((line) => 'data' in line).map((line) => line.data);
  }

  /** The close lines written. */
  get closes() {
    return this.objects.filter((line) => 'close' in line).map((line) => line.close);
  }

  get keepalives() {
    return this.lines.filter((line) => line === '').length;
  }
}

export class FakeTransport {
  requests = [];
  /** The bodies of the short requests: '' is a probe. */
  posts = [];
  /** Called with each request as it opens, to answer it at once. */
  onOpen = undefined;
  /** Answers each short request. By default the stream is empty and open, and a close line is accepted. */
  onPost = (body) =>
    body.includes('"close"')
      ? { status: 200, body: { lastEventId: 0, closed: true, outcome: 'fail', reason: 'publisher' } }
      : { status: 200, body: { lastEventId: 0, closed: false } };

  open(streamId, token) {
    const request = new FakeRequest(streamId, token);
    this.requests.push(request);
    this.onOpen?.(request, this.requests.length - 1);
    return request;
  }

  async post(streamId, token, body) {
    const text = decoder.decode(body);
    this.posts.push(text);
    return this.onPost(text, this.posts.length - 1);
  }

  /** The nth request, once the writer has opened it. */
  request(n) {
    return until(() => this.requests[n], `request ${n}`);
  }

  get probes() {
    return this.posts.filter((body) => body === '').length;
  }

  /** The close lines sent out of band. */
  get closes() {
    return this.posts.filter((body) => body !== '').map((body) => JSON.parse(body).close);
  }
}

/** A client over a fake transport, with the errors its publishers reported. */
export function setup(options = {}) {
  const transport = new FakeTransport();
  const errors = [];
  const client = new IQClientCore(
    { url: 'http://iq.test', appKey: 'demo-app', appSecret: SECRET, minOpen: 0, errorHandler: (error) => errors.push(error), ...options },
    { transport: () => transport },
  );
  return { transport, errors, client, publisher: () => client.publisher(STREAM) };
}
