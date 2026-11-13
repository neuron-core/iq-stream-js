import { IQError } from '../shared/errors.js';
import { DEFAULT_MAX_FRAME_BYTES, DEFAULT_MAX_TOKEN_TTL, STREAM_ID } from '../shared/protocol.js';
import type { CloseOutcome, Scope, Ticket } from '../shared/protocol.js';
import { refuseBrowser } from './guard.js';
import { base64url, claims, importSecret, sign, verify } from './token.js';
import type { Answer, Transport } from './transport.js';
import type { IQClientOptions, Publisher, PublishResult } from './types.js';
import { POST_TIMEOUT_MS, StreamPublisher, closeStreamWithin, refusal, toResult } from './writer.js';
import type { Settings, Wire } from './writer.js';

/** What differs between the two builds. */
export interface Platform {
  transport(url: string, options: IQClientOptions): Transport;
  /** HMAC-SHA256 as base64url without padding, for `tokenSync()`, where the platform has a synchronous one. */
  hmacSync?: (secret: string, message: string) => string;
}

/** The server checks a `pub` token once, when the request starts. */
const PUB_TOKEN_TTL = 300;
const EMPTY = new Uint8Array(0);

const env = (name: string): string | undefined =>
  (typeof process !== 'undefined' && process.env ? process.env[name] : undefined) || undefined;

function number(name: string, value: number | undefined, fallback: number, min = 0): number {
  if (value === undefined) return fallback;
  if (typeof value !== 'number' || !Number.isFinite(value) || value < min) throw new IQError(`@neuron-core/iq-stream: invalid option ${name}.`);
  return value;
}

function checkScope(scope: Scope): Scope {
  if (scope !== 'pub' && scope !== 'sub') throw new TypeError('The scope is "pub" or "sub".');
  return scope;
}

function checkStreamId(streamId: string): string {
  if (typeof streamId !== 'string' || !STREAM_ID.test(streamId)) throw new TypeError('Invalid stream id: 16 to 128 characters of A-Z a-z 0-9 _ -.');
  return streamId;
}

/** Everything of `IQClient` but the transport, shared by the two builds. */
export class IQClientCore {
  /** How this backend reaches the server. */
  readonly url: string;
  /** The address handed to browsers. */
  readonly publicUrl: string;
  readonly appKey: string;
  readonly maxFrameBytes: number;

  readonly #secret: string;
  readonly #hmacSync: Platform['hmacSync'];
  readonly #settings: Settings;
  readonly #wire: Wire;
  #key: Promise<CryptoKey> | undefined;

  constructor(options: IQClientOptions, platform: Platform) {
    refuseBrowser();
    const url = (options.url ?? env('IQ_STREAM_URL') ?? '').replace(/\/+$/, '');
    if (!/^(https?:\/\/[^/]|unix:\/\/\/.)/.test(url) || (url.startsWith('http') && !URL.canParse(url))) {
      throw new IQError('@neuron-core/iq-stream: set url (or IQ_STREAM_URL) to http(s)://host[:port] or unix:///path/to/iq.sock.');
    }
    const appKey = options.appKey ?? env('IQ_STREAM_APP_KEY') ?? '';
    if (!/^[!-~]{1,256}$/.test(appKey)) {
      throw new IQError('@neuron-core/iq-stream: set appKey (or IQ_STREAM_APP_KEY): 1 to 256 printable ASCII characters without spaces.');
    }
    const secret = options.appSecret ?? env('IQ_STREAM_APP_SECRET') ?? '';
    if (new TextEncoder().encode(secret).length < 32) {
      throw new IQError('@neuron-core/iq-stream: set appSecret (or IQ_STREAM_APP_SECRET): at least 32 bytes.');
    }
    if (options.onError !== undefined && options.onError !== 'report' && options.onError !== 'raise') {
      throw new IQError('@neuron-core/iq-stream: invalid option onError.');
    }
    if (options.mode !== undefined && options.mode !== 'stream' && options.mode !== 'batch') {
      throw new IQError('@neuron-core/iq-stream: invalid option mode.');
    }

    this.url = url;
    this.publicUrl = (options.publicUrl ?? env('IQ_STREAM_PUBLIC_URL') ?? url).replace(/\/+$/, '');
    this.appKey = appKey;
    this.#secret = secret;
    this.#hmacSync = platform.hmacSync;
    this.#settings = {
      keepaliveMs: options.keepalive === null ? null : number('keepalive', options.keepalive, 20, 0.001) * 1000,
      minOpenMs: number('minOpen', options.minOpen, 3) * 1000,
      maxFrameBytes: number('maxFrameBytes', options.maxFrameBytes, DEFAULT_MAX_FRAME_BYTES, 1),
      raise: options.onError === 'raise',
      errorHandler: options.errorHandler,
      batch: options.mode === 'batch',
      batchMs: number('batchMs', options.batchMs, 100),
      replayFrames: number('replayFrames', options.replayFrames, 512),
      replayBytes: number('replayBytes', options.replayBytes, 4 << 20),
      queueBytes: number('queueBytes', options.queueBytes, 8 << 20, 1),
      retryBudgetMs: number('retryBudget', options.retryBudget, 10) * 1000,
      takeoverBudgetMs: number('takeoverBudget', options.takeoverBudget, 65) * 1000,
    };
    this.maxFrameBytes = this.#settings.maxFrameBytes;

    const transport = platform.transport(url, options);
    // A fresh `pub` token for every request, never handed to application code.
    this.#wire = {
      open: async (streamId) => transport.open(streamId, await this.#mint('pub', streamId, PUB_TOKEN_TTL)),
      post: async (streamId, body, timeoutMs) => transport.post(streamId, await this.#mint('pub', streamId, PUB_TOKEN_TTL), body, timeoutMs),
    };
  }

  /** 128 random bits as base64url: 22 characters. Never reuse an id for a second answer. */
  newStreamId(): string {
    return base64url(crypto.getRandomValues(new Uint8Array(16)));
  }

  /** Mints a token. The `sub` token is what a browser subscribes with; `ttl` is in seconds and the server refuses more than its MAX_TOKEN_TTL_S. */
  async token(scope: Scope, streamId: string, ttl: number = DEFAULT_MAX_TOKEN_TTL): Promise<string> {
    return this.#mint(checkScope(scope), checkStreamId(streamId), ttl);
  }

  /** `token()` without a promise, on the Node build. */
  tokenSync(scope: Scope, streamId: string, ttl: number = DEFAULT_MAX_TOKEN_TTL): string {
    if (!this.#hmacSync) throw new IQError('@neuron-core/iq-stream: tokenSync() exists in the Node build only. Use token().');
    const body = claims(this.appKey, checkStreamId(streamId), this.#expiry(ttl));
    return `${body}.${this.#hmacSync(this.#secret, `${checkScope(scope)}.${body}`)}`;
  }

  /** The subscribe URL with a `sub` token in its query: ready for `new EventSource(url)`. */
  async subscribeUrl(streamId: string, ttl: number = DEFAULT_MAX_TOKEN_TTL): Promise<string> {
    return (await this.ticket(streamId, ttl)).url;
  }

  /** What a browser needs to subscribe. Without a stream id, a new one is made. */
  async ticket(streamId: string = this.newStreamId(), ttl: number = DEFAULT_MAX_TOKEN_TTL): Promise<Ticket> {
    if (!/^https?:\/\//.test(this.publicUrl)) throw new IQError('@neuron-core/iq-stream: set publicUrl (or IQ_STREAM_PUBLIC_URL) to the address browsers use.');
    const expiresAt = this.#expiry(ttl);
    const token = await sign(await this.#secretKey(), 'sub', claims(this.appKey, checkStreamId(streamId), expiresAt));
    return { streamId, token, url: `${this.publicUrl}/${streamId}?token=${token}`, expiresAt };
  }

  /**
   * Checks a token the way the server does. A backend that starts a run for a stream id sent by a page
   * asks for the page's `sub` token too, and verifies it: the id then provably came from this backend.
   */
  async verify(scope: Scope, token: string, streamId: string): Promise<boolean> {
    if (typeof token !== 'string' || typeof streamId !== 'string') return false;
    return verify(await this.#secretKey(), checkScope(scope), this.appKey, streamId, token, Date.now() / 1000);
  }

  /**
   * Takes a stream out of `pending` with one empty POST, for a run that waits in a queue: from then on
   * the stream waits the idle timeout for its publisher, not the pending timeout.
   */
  async reserve(streamId: string): Promise<PublishResult> {
    const answer = await this.#wire.post(checkStreamId(streamId), EMPTY, POST_TIMEOUT_MS);
    // publisher_active: the run has already started, which is as good as reserved.
    return this.#accept(streamId, answer, 'publisher_active');
  }

  /**
   * Closes a stream from a process that never had its publisher, such as a job's failure handler, so
   * that subscribers hear of it at once. A stream that is already closed is left as it is.
   */
  async closeStream(streamId: string, outcome: CloseOutcome = 'fail'): Promise<PublishResult> {
    if (outcome !== 'end' && outcome !== 'fail') throw new TypeError('The close outcome is "end" or "fail".');
    const answer = await closeStreamWithin(this.#wire, checkStreamId(streamId), outcome, this.#settings.retryBudgetMs);
    return this.#accept(streamId, answer, 'stream_closed');
  }

  /** Opens the stream's request at once and returns its publisher. */
  publisher(streamId: string): Publisher {
    return new StreamPublisher(this.#wire, this.#settings, checkStreamId(streamId));
  }

  #accept(streamId: string, answer: Answer, harmless: string): PublishResult {
    if (answer.status === 200 || (answer.status === 409 && answer.body.error === harmless)) return toResult(answer);
    throw refusal(streamId, 'the server refused the request', answer);
  }

  #expiry(ttl: number): number {
    if (typeof ttl !== 'number' || !Number.isFinite(ttl) || ttl <= 0) throw new TypeError('The ttl is a positive number of seconds.');
    return Math.floor(Date.now() / 1000 + ttl);
  }

  #secretKey(): Promise<CryptoKey> {
    return (this.#key ??= importSecret(this.#secret));
  }

  async #mint(scope: Scope, streamId: string, ttl: number): Promise<string> {
    return sign(await this.#secretKey(), scope, claims(this.appKey, streamId, this.#expiry(ttl)));
  }
}
