/**
 * An AG-UI agent whose events come from any `ReadableStream` of AG-UI events: IQ frames by default,
 * or another transport's stream of the same objects.
 */
import { AbstractAgent, EventType } from '@ag-ui/client';
import type { AgentConfig, BaseEvent, RunAgentInput, RunErrorEvent } from '@ag-ui/client';
import { Observable } from 'rxjs';
import { frames } from '../client/index.js';

/**
 * The base class's own `Observable` type. `@ag-ui/client` may resolve a copy of rxjs other than the
 * application's, and two copies of the class do not match as types, though they work together.
 */
type EventStream = ReturnType<AbstractAgent['run']>;

type Open<Ticket> = (ticket: Ticket) => ReadableStream<BaseEvent>;

export type StreamAgentConfig<Ticket = { url: string }> = AgentConfig & {
  /** Posts the run's input to the application and returns the ticket its backend answered with. */
  start(input: RunAgentInput): Promise<Ticket>;
  /** Called on `abortRun()`, so that the application can cancel the run: closing the subscription alone does not. */
  stop?(ticket: Ticket): void;
  /** The stream of events for a ticket. Default: `frames(ticket.url, { parse: JSON.parse })`. */
  open?: Open<Ticket>;
} & (Ticket extends { url: string } ? unknown : { open: Open<Ticket> });

const fromIq = (ticket: { url: string }): ReadableStream<BaseEvent> =>
  frames<BaseEvent>(ticket.url, { parse: JSON.parse });

export class StreamAgent<Ticket = { url: string }> extends AbstractAgent {
  private start: (input: RunAgentInput) => Promise<Ticket>;
  private stop: ((ticket: Ticket) => void) | undefined;
  private open: Open<Ticket>;
  /** One for each run in flight: what `abortRun()` calls. */
  private aborts = new Set<() => void>();

  constructor(config: StreamAgentConfig<Ticket>) {
    const { start, stop, open, ...agent } = config;
    super(agent);
    this.start = start;
    this.stop = stop;
    this.open = open ?? (fromIq as unknown as Open<Ticket>);
  }

  run(input: RunAgentInput): EventStream {
    const events = new Observable<BaseEvent>((observer) => {
      let reader: ReadableStreamDefaultReader<BaseEvent> | undefined;
      let started: { ticket: Ticket } | undefined;
      let told = false;   // the data layer already said how the run ended
      let unsubscribed = false;
      let aborted = false;
      // As HttpAgent does: an aborted run ends with a RUN_ERROR whose code is `abort`. A run that has not
      // said how it ended is stopped too, now or as soon as `start()` returns its ticket.
      const abort = (): void => {
        aborted = true;
        this.aborts.delete(abort);
        if (started && !told) this.stop?.(started.ticket);
        void reader?.cancel().catch(() => {});
        const event: RunErrorEvent = { type: EventType.RUN_ERROR, message: 'Run aborted', code: 'abort' };
        if (!told) observer.next(event);
        observer.complete();
      };
      this.aborts.add(abort);
      void (async () => {
        try {
          const ticket = await this.start(input);
          started = { ticket };
          if (aborted) return this.stop?.(ticket);
          if (unsubscribed) return;
          reader = this.open(ticket).getReader();
          for (;;) {
            const { done, value } = await reader.read();
            if (done) return observer.complete();
            if (value.type === 'RUN_ERROR' || value.type === 'RUN_FINISHED') told = true;
            observer.next(value);
          }
        } catch (error) {
          if (aborted) return;
          const { kind, reason } = (error ?? {}) as { kind?: unknown; reason?: unknown };
          // A failed run arrives twice: as RUN_ERROR, then as iq:fail. Once is enough.
          if (kind === 'fail' && reason === 'publisher' && told) observer.complete();
          else observer.error(error);
        } finally {
          this.aborts.delete(abort);
        }
      })();
      return () => {
        unsubscribed = true;
        this.aborts.delete(abort);
        void reader?.cancel().catch(() => {});
      };
    });
    return events as unknown as EventStream;
  }

  /** The base class's `abortRun()` does nothing: each agent ends its own runs. */
  override abortRun(): void {
    for (const abort of [...this.aborts]) abort();
    super.abortRun();
  }

  /** The base class copies only its own fields, and CopilotKit runs clones. */
  override clone(): StreamAgent<Ticket> {
    const copy = super.clone() as StreamAgent<Ticket>;
    copy.start = this.start;
    copy.stop = this.stop;
    copy.open = this.open;
    copy.aborts = new Set();
    return copy;
  }
}
