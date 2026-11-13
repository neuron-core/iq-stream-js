/** A source that a unit test drives by hand, in place of `EventSource`. No network, no timers. */
import type { EventSourceLike, SourceEvent } from './index.js';

export class FakeEventSource implements EventSourceLike {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSED = 2;

  /** Every source created since the last `reset()`, in order: what `subscribe` opened, and with which URL. */
  static instances: FakeEventSource[] = [];
  static reset(): void { FakeEventSource.instances = []; }

  /** The URL the source was opened with. */
  readonly url: string;
  readyState: number = FakeEventSource.CONNECTING;
  /** How many times `close()` was called. */
  closeCalls = 0;
  /** Set it to make the source deliver every event to listeners of `*` too, as the `fetch` source does. */
  wildcard = false;

  #listeners = new Map<string, Array<(event: SourceEvent) => void>>();
  #lastEventId = '';

  constructor(url: string | URL = '') {
    this.url = String(url);
    FakeEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: (event: SourceEvent) => void): void {
    const list = this.#listeners.get(type) ?? [];
    this.#listeners.set(type, list);
    list.push(listener);
  }

  listenerCount(type: string): number {
    return this.#listeners.get(type)?.length ?? 0;
  }

  close(): void {
    this.closeCalls++;
    this.readyState = FakeEventSource.CLOSED;
  }

  /** Dispatches any event. Unlike `EventSource` it does so after `close()` too, to test that a client ignores it. */
  emit(type: string, event: Omit<SourceEvent, 'type'> = {}): void {
    const full = { ...event, type };
    for (const listener of [...(this.#listeners.get(type) ?? [])]) listener(full);
    if (this.wildcard && 'data' in event) for (const listener of [...(this.#listeners.get('*') ?? [])]) listener(full);
  }

  /** The connection is established. */
  open(): void {
    this.readyState = FakeEventSource.OPEN;
    this.emit('open');
  }

  /** A frame. As in `EventSource`, a frame without an id carries the last id seen. */
  frame(data: string, options: { id?: string; event?: string } = {}): void {
    if (options.id !== undefined) this.#lastEventId = options.id;
    this.emit(options.event ?? 'message', { data, lastEventId: this.#lastEventId });
  }

  /** A terminal event. `data` is sent as JSON unless it is a string, which is sent as it is. */
  terminal(name: 'iq:end' | 'iq:fail' | 'iq:gone', data: object | string = {}): void {
    this.emit(name, { data: typeof data === 'string' ? data : JSON.stringify(data), lastEventId: this.#lastEventId });
  }

  /**
   * An `error` event. `connecting`: the connection dropped and the source resumes by itself.
   * `closed`: the server refused; `details` is what a source that can read the answer adds.
   */
  error(state: 'connecting' | 'closed', details: { status?: number; error?: string } = {}): void {
    this.readyState = state === 'closed' ? FakeEventSource.CLOSED : FakeEventSource.CONNECTING;
    this.emit('error', details);
  }
}
