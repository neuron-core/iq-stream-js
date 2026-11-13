/**
 * An AG-UI agent whose events come from any `ReadableStream` of AG-UI events: IQ frames by default,
 * or another transport's stream of the same objects.
 */
import { AbstractAgent } from '@ag-ui/client';
import type { AgentConfig, BaseEvent, RunAgentInput } from '@ag-ui/client';
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
  /** The stream of events for a ticket. Default: `frames(ticket.url, { parse: JSON.parse })`. */
  open?: Open<Ticket>;
} & (Ticket extends { url: string } ? unknown : { open: Open<Ticket> });

const fromIq = (ticket: { url: string }): ReadableStream<BaseEvent> =>
  frames<BaseEvent>(ticket.url, { parse: JSON.parse });

export class StreamAgent<Ticket = { url: string }> extends AbstractAgent {
  private start: (input: RunAgentInput) => Promise<Ticket>;
  private open: Open<Ticket>;

  constructor(config: StreamAgentConfig<Ticket>) {
    const { start, open, ...agent } = config;
    super(agent);
    this.start = start;
    this.open = open ?? (fromIq as unknown as Open<Ticket>);
  }

  run(input: RunAgentInput): EventStream {
    const events = new Observable<BaseEvent>((observer) => {
      let reader: ReadableStreamDefaultReader<BaseEvent> | undefined;
      let told = false;   // the data layer already said how the run ended
      let unsubscribed = false;
      void (async () => {
        try {
          const ticket = await this.start(input);
          if (unsubscribed) return;
          reader = this.open(ticket).getReader();
          for (;;) {
            const { done, value } = await reader.read();
            if (done) return observer.complete();
            if (value.type === 'RUN_ERROR' || value.type === 'RUN_FINISHED') told = true;
            observer.next(value);
          }
        } catch (error) {
          const { kind, reason } = (error ?? {}) as { kind?: unknown; reason?: unknown };
          // A failed run arrives twice: as RUN_ERROR, then as iq:fail. Once is enough.
          if (kind === 'fail' && reason === 'publisher' && told) observer.complete();
          else observer.error(error);
        }
      })();
      return () => {
        unsubscribed = true;
        void reader?.cancel().catch(() => {});
      };
    });
    return events as unknown as EventStream;
  }

  /** The base class copies only its own fields, and CopilotKit runs clones. */
  override clone(): StreamAgent<Ticket> {
    const copy = super.clone() as StreamAgent<Ticket>;
    copy.start = this.start;
    copy.open = this.open;
    return copy;
  }
}
