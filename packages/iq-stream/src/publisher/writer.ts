import { IQError } from '../shared/errors.js';
import type { CloseOutcome } from '../shared/protocol.js';
import { FrameTooLarge, closeLine, concat, encodeFrame, frameSize } from './frame.js';
import { NO_ANSWER } from './transport.js';
import type { Answer, OpenRequest } from './transport.js';
import type { Publisher, PublishResult } from './types.js';

/** The client's options, resolved: durations in milliseconds. */
export interface Settings {
  keepaliveMs: number | null;
  minOpenMs: number;
  maxFrameBytes: number;
  raise: boolean;
  errorHandler: ((error: IQError) => void) | undefined;
  batch: boolean;
  batchMs: number;
  replayFrames: number;
  replayBytes: number;
  queueBytes: number;
  retryBudgetMs: number;
  takeoverBudgetMs: number;
}

/** The transport with the stream's `pub` token already minted for each request. */
export interface Wire {
  open(streamId: string): Promise<OpenRequest>;
  post(streamId: string, body: Uint8Array, timeoutMs: number): Promise<Answer>;
}

export const POST_TIMEOUT_MS = 10_000;
/** How long giving up may take: the time allowed to tell subscribers at once that the stream failed. */
const OUT_OF_BAND_MS = 3_000;
/** A request that lived this long was a working one, and its loss starts a new series of retries. */
const HEALTHY_MS = 1_000;

const EMPTY = new Uint8Array(0);
const NEWLINE = Uint8Array.of(10);
const now = (): number => performance.now();
const backoff = (tries: number): number => Math.min(100 * 2 ** tries, 2_000);

const isClosed = (body: Answer['body']): boolean => body.closed === true || body.error === 'stream_closed';

/** No answer, or one that says "not now": the stream may still be there, so ask again. */
const isRetryable = ({ status, body }: Answer): boolean =>
  status === 0 || status === 502 || status === 503 || status === 504 || (status === 409 && body.error === 'publisher_active');

/** A 200 or 408 that does not say how many frames the stream holds cannot be acted on: treat it as no answer. */
const usable = (answer: Answer): Answer =>
  (answer.status === 200 || answer.status === 408) && !isClosed(answer.body) && typeof answer.body.lastEventId !== 'number' ? NO_ANSWER : answer;

export function toResult({ status, body }: Answer): PublishResult {
  const result: PublishResult = { status, closed: isClosed(body) };
  if (typeof body.lastEventId === 'number') result.lastEventId = body.lastEventId;
  if (body.outcome === 'end' || body.outcome === 'fail') result.outcome = body.outcome;
  if (typeof body.reason === 'string') result.reason = body.reason;
  if (typeof body.error === 'string') result.error = body.error;
  return result;
}

/** Errors carry the stream id, the status and the server's code, and never a token or frame data. */
export function refusal(streamId: string, what: string, answer: Answer = NO_ANSWER, code?: string): IQError {
  const error = code ?? (typeof answer.body.error === 'string' ? answer.body.error : undefined);
  const detail = answer.status === 0 ? (error ?? 'no answer') : `HTTP ${answer.status}${error === undefined ? '' : `, ${error}`}`;
  return new IQError(`IQ stream ${streamId}: ${what} (${detail})`, {
    streamId,
    ...(answer.status === 0 ? {} : { status: answer.status }),
    ...(error === undefined ? {} : { code: error }),
  });
}

/** A short request holding only a close line, retried while the server says "not now". */
export async function closeStreamWithin(wire: Wire, streamId: string, outcome: CloseOutcome, budgetMs: number): Promise<Answer> {
  const deadline = now() + budgetMs;
  for (let tries = 0; ; tries++) {
    const answer = await wire.post(streamId, closeLine(outcome), Math.max(1, Math.min(POST_TIMEOUT_MS, deadline - now())));
    if (!isRetryable(answer) || now() + backoff(tries) >= deadline) return answer;
    await new Promise((resolve) => setTimeout(resolve, backoff(tries)));
  }
}

/** A WritableStream over a publisher: closes with `end` when closed, with `fail` when aborted or when a write throws. */
export function writableOf(
  publisher: Publisher,
  drained: () => Promise<void> = () => Promise.resolve(),
): WritableStream<string | { data: string; event?: string }> {
  return new WritableStream({
    async write(chunk) {
      try {
        if (typeof chunk === 'string') publisher.send(chunk);
        else publisher.send(chunk.data, chunk.event);
      } catch (error) {
        // A sink that throws is not aborted by the stream, and the stream must not be left open.
        await publisher.close('fail').catch(() => undefined);
        throw error;
      }
      await drained();
    },
    async close() {
      await publisher.close('end');
    },
    async abort() {
      await publisher.close('fail').catch(() => undefined);
    },
  });
}

/**
 * One stream's publisher. `send()` puts an encoded line on a queue; a single writer, `#run`, drains
 * the queue into the open request and acts on every answer the server can give.
 */
export class StreamPublisher implements Publisher {
  readonly streamId: string;
  readonly maxFrameBytes: number;

  readonly #wire: Wire;
  readonly #settings: Settings;
  readonly #created = now();

  /** Lines not yet handed to a request. */
  #queue: Uint8Array[] = [];
  #queuedBytes = 0;
  /** Bytes handed to the request whose write has not completed: they wait for the server too. */
  #inFlight = 0;
  /** The most recent data frames handed to a request: the frames numbered `#written - #ring.length + 1` to `#written`. */
  #ring: Uint8Array[] = [];
  #ringBytes = 0;
  #written = 0;
  /** Frames the server does not hold, to send first on the next request. */
  #resend: Uint8Array[] = [];

  #framesSent = 0;
  #bytesSent = 0;
  #requests = 0;
  #closing: CloseOutcome | undefined;
  #closeAt = 0;
  #stopped = false;
  #error: IQError | undefined;
  #request: OpenRequest | undefined;
  #waiter: (() => void) | undefined;
  #drains: (() => void)[] = [];
  #closed: Promise<PublishResult> | undefined;
  #settle!: (result: PublishResult) => void;
  readonly #done = new Promise<PublishResult>((resolve) => {
    this.#settle = resolve;
  });

  constructor(wire: Wire, settings: Settings, streamId: string) {
    this.#wire = wire;
    this.#settings = settings;
    this.streamId = streamId;
    this.maxFrameBytes = settings.maxFrameBytes;
    // Opened now, not on the first frame: a subscriber waits only PENDING_TIMEOUT_MS for its publisher.
    this.#run().catch((cause: unknown) => {
      this.#giveUp(new IQError(`IQ stream ${streamId}: the publisher stopped on an internal error`, { streamId, cause }), undefined, true);
    });
  }

  get framesSent(): number {
    return this.#framesSent;
  }

  get bytesSent(): number {
    return this.#bytesSent;
  }

  /** Requests opened for this stream: 1 unless something was resumed (or the mode is `batch`). */
  get requests(): number {
    return this.#requests;
  }

  get alive(): boolean {
    return !this.#stopped && this.#closing === undefined;
  }

  get error(): IQError | undefined {
    return this.#error;
  }

  frameSize(data: string, event?: string): number {
    return frameSize(data, event);
  }

  send(data: string, event?: string): boolean {
    const line = encodeFrame(data, event);
    if (line.length - 1 > this.maxFrameBytes) throw new FrameTooLarge(line.length - 1, this.maxFrameBytes);
    if (this.#error && this.#settings.raise) throw this.#error;
    if (!this.alive) return false;
    if (this.#queuedBytes + this.#inFlight + line.length > this.#settings.queueBytes) {
      // The server stopped reading: a stalled network must not hold the agent's memory.
      const error = refusal(this.streamId, 'the queue is full, the server is not reading', NO_ANSWER, 'queue_full');
      this.#giveUp(error, undefined, true);
      if (this.#settings.raise) throw error;
      return false;
    }
    this.#queue.push(line);
    this.#queuedBytes += line.length;
    this.#framesSent++;
    this.#bytesSent += line.length;
    this.#wake();
    return true;
  }

  close(outcome: CloseOutcome = 'end', options: { timeout?: number } = {}): Promise<PublishResult> {
    if (outcome !== 'end' && outcome !== 'fail') throw new TypeError('The close outcome is "end" or "fail".');
    const timeout = options.timeout ?? 10;
    if (typeof timeout !== 'number' || !(timeout >= 0)) throw new TypeError('The timeout is a number of seconds.');
    if (!this.#closed) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      if (!this.#stopped) {
        this.#closing = outcome;
        this.#closeAt = Math.max(now(), this.#created + this.#settings.minOpenMs);
        // The timeout counts from the moment the close line may go out. This timer is the one that is not
        // unref'd: it keeps the process alive until the server has answered.
        timer = setTimeout(
          () => this.#giveUp(refusal(this.streamId, 'the close was not answered in time', NO_ANSWER, 'close_timeout'), undefined, true),
          this.#closeAt - now() + timeout * 1000,
        );
        this.#wake();
      }
      this.#closed = this.#done.then((result) => {
        clearTimeout(timer);
        if (this.#error && this.#settings.raise) throw this.#error;
        return result;
      });
    }
    return this.#closed;
  }

  abort(): void {
    if (this.#stopped) return;
    this.#stop();
    this.#settle({ status: 0, closed: false });
  }

  writable(): WritableStream<string | { data: string; event?: string }> {
    // Backpressure: a write is done once the writer has taken the line from the queue.
    return writableOf(this, () =>
      this.#queue.length > 0 && !this.#stopped ? new Promise<void>((resolve) => this.#drains.push(resolve)) : Promise.resolve(),
    );
  }

  async #run(): Promise<void> {
    let failingSince: number | undefined;
    let tries = 0;
    while (!this.#stopped) {
      const started = now();
      let answer = usable(await (this.#settings.batch ? this.#batch() : this.#stream()));
      if (!isRetryable(answer) || now() - started > HEALTHY_MS) {
        failingSince = undefined;
        tries = 0;
      }
      // Until an answer says what the stream holds, ask with an empty POST: the server answers it with
      // the stream's state. After a lost connection the first one goes out at once.
      while (!this.#stopped && isRetryable(answer)) {
        failingSince ??= now();
        if (answer.status !== 0 || tries > 0) {
          // The server still counts a dead request of ours as attached until its read timeout passes,
          // which is longer than an outage is worth waiting for.
          const budget = answer.status === 409 ? this.#settings.takeoverBudgetMs : this.#settings.retryBudgetMs;
          if (now() + backoff(tries) > failingSince + budget) {
            return this.#giveUp(refusal(this.streamId, 'gave up retrying', answer, answer.status === 0 ? 'unreachable' : undefined), answer, true);
          }
          await this.#idle(backoff(tries));
          if (this.#stopped) return;
        }
        tries++;
        answer = usable(await this.#wire.post(this.streamId, EMPTY, POST_TIMEOUT_MS));
      }
      if (this.#stopped) return;

      if (isClosed(answer.body)) {
        if (answer.status === 200) {
          this.#stop();
          return this.#settle(toResult(answer));
        }
        return this.#giveUp(refusal(this.streamId, 'the stream is closed', answer), answer, false);
      }
      if (answer.status === 200 || answer.status === 408) {
        if (this.#resume(answer.body.lastEventId as number)) continue;
        // A stream with a hole in it is worse for a client than one that says it failed. This is also
        // how a restarted server shows: the tail of an answer is not published into a fresh stream.
        const what = `the server holds ${String(answer.body.lastEventId)} frames, ${this.#written} were written, and the replay buffer cannot bridge them`;
        return this.#giveUp(refusal(this.streamId, what, answer, 'gap'), answer, true);
      }
      // A refused line leaves the stream open on the server. Bad credentials or an app at its limit: there is nothing to close.
      return this.#giveUp(refusal(this.streamId, 'the server refused the request', answer), answer, answer.status === 400 || answer.status === 413);
    }
  }

  /** One held request: pending resends, then the queue as it fills, keepalives, the close line. Returns its answer. */
  async #stream(): Promise<Answer> {
    const request = await this.#wire.open(this.streamId);
    this.#requests++;
    if (this.#stopped) {
      request.destroy();
      return NO_ANSWER;
    }
    this.#request = request;
    void request.answer.then(() => this.#wake());
    const keepalive = this.#settings.keepaliveMs ?? Infinity;
    let lastWrite = now();
    const write = async (bytes: Uint8Array): Promise<void> => {
      this.#inFlight = bytes.length;
      await request.write(bytes);
      this.#inFlight = 0;
      lastWrite = now();
    };

    if (this.#resend.length > 0) {
      const frames = this.#resend;
      this.#resend = [];
      await write(concat(frames));
    }
    // `settled` is looked at before every write: once the server has answered, it reads nothing more.
    while (!request.settled && !this.#stopped) {
      if (this.#queue.length > 0) {
        await write(this.#take());
      } else if (this.#closing !== undefined && now() >= this.#closeAt) {
        await write(closeLine(this.#closing));
        request.end();
        break;
      } else if (now() - lastWrite >= keepalive) {
        await write(NEWLINE);
      } else {
        await this.#idle(Math.min(lastWrite + keepalive, this.#closing === undefined ? Infinity : this.#closeAt) - now());
      }
    }
    const answer = await request.answer;
    request.destroy();
    this.#request = undefined;
    return answer;
  }

  /** Batch mode: one short request with the lines collected for `batchMs`. The first goes out at once, to take the stream out of `pending`. */
  async #batch(): Promise<Answer> {
    const closeReady = (): boolean => this.#closing !== undefined && now() >= this.#closeAt;
    const closeAt = (): number => (this.#closing === undefined ? Infinity : this.#closeAt);
    if (this.#requests > 0 && this.#resend.length === 0) {
      // An empty request after `keepalive` seconds of silence keeps the stream from idling out.
      const keepalive = now() + (this.#settings.keepaliveMs ?? Infinity);
      while (!this.#stopped && this.#queue.length === 0 && !closeReady() && now() < keepalive) {
        await this.#idle(Math.min(keepalive, closeAt()) - now());
      }
      const flush = now() + this.#settings.batchMs;
      while (!this.#stopped && this.#queue.length > 0 && !closeReady() && now() < flush) {
        await this.#idle(Math.min(flush, closeAt()) - now());
      }
    }
    if (this.#stopped) return NO_ANSWER;
    const lines = this.#resend;
    this.#resend = [];
    if (this.#queue.length > 0) lines.push(this.#take());
    if (closeReady()) lines.push(closeLine(this.#closing!));
    this.#requests++;
    return this.#wire.post(this.streamId, lines.length > 0 ? concat(lines) : EMPTY, POST_TIMEOUT_MS);
  }

  /** Moves the queue into the replay ring and returns it as one chunk: lines queued together leave together. */
  #take(): Uint8Array {
    const lines = this.#queue;
    this.#queue = [];
    this.#queuedBytes = 0;
    this.#written += lines.length;
    for (const line of lines) {
      this.#ring.push(line);
      this.#ringBytes += line.length;
    }
    let drop = 0;
    while (this.#ring.length - drop > this.#settings.replayFrames || this.#ringBytes > this.#settings.replayBytes) {
      this.#ringBytes -= this.#ring[drop++]!.length;
    }
    if (drop > 0) this.#ring.splice(0, drop);
    this.#drained();
    return concat(lines);
  }

  /** Puts the frames the server does not hold at the head of the next request. False when the ring cannot bridge the gap. */
  #resume(held: number): boolean {
    const oldest = this.#written - this.#ring.length + 1;
    if (held > this.#written || held + 1 < oldest) return false;
    this.#resend = this.#ring.slice(held + 1 - oldest);
    return true;
  }

  /** Waits for `ms`, or until `#wake()`. Unref'd, so an open publisher never keeps a process alive by itself. */
  #idle(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const timer = Number.isFinite(ms) ? setTimeout(() => this.#wake(), Math.max(0, ms)) : undefined;
      (timer as { unref?: () => void } | undefined)?.unref?.();
      this.#waiter = () => {
        clearTimeout(timer);
        this.#waiter = undefined;
        resolve();
      };
    });
  }

  #wake(): void {
    this.#waiter?.();
  }

  #drained(): void {
    const drains = this.#drains;
    this.#drains = [];
    for (const resolve of drains) resolve();
  }

  #stop(): void {
    this.#stopped = true;
    this.#request?.destroy();
    this.#wake();
    this.#drained();
  }

  /**
   * Fail open: the error is recorded and reported once, the publisher goes inert, and the caller's run
   * goes on. With `closeIt`, the server may still hold the stream open, and is told that it failed,
   * so that subscribers get `iq:fail` now and not after the idle timeout.
   */
  #giveUp(error: IQError, answer: Answer | undefined, closeIt: boolean): void {
    if (this.#stopped) return;
    this.#stop();
    this.#error = error;
    try {
      if (this.#settings.errorHandler) this.#settings.errorHandler(error);
      else if (!this.#settings.raise) console.warn(`[iq-stream] ${error.message}`);
    } catch {
      // A failing handler must not break the run either.
    }
    const result: PublishResult = answer && answer.status !== 0 ? toResult(answer) : { status: 0, closed: false };
    if (error.code !== undefined) result.error = error.code;
    if (!closeIt) return this.#settle(result);
    closeStreamWithin(this.#wire, this.streamId, 'fail', Math.min(OUT_OF_BAND_MS, this.#settings.retryBudgetMs))
      .then((closed) => {
        // The result keeps the status and the code of what went wrong, and says how the stream ended.
        const { lastEventId, outcome, reason } = toResult(closed);
        if (isClosed(closed.body)) Object.assign(result, { closed: true }, lastEventId === undefined ? {} : { lastEventId }, outcome && { outcome }, reason && { reason });
      })
      .catch(() => undefined)
      .then(() => this.#settle(result));
  }
}
