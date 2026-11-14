import { classifyAgUiEvent } from './classify.js';
import { startPublishing } from './publish.js';
import type { Oversize, PublishOptions, PublishedRun } from './types.js';

/** An rxjs `Observable`, by the one method the adapter calls: no dependency on rxjs. */
export interface ObservableLike<T = unknown> {
  subscribe(observer: { next(value: T): void; error(error: unknown): void; complete(): void }): { unsubscribe(): void };
}

/** The AG-UI counterpart of the `data-iq-omitted` part: a custom event with the same content. */
const omitted: Oversize = (event, bytes) => {
  const { type, toolCallId } = (event ?? {}) as { type?: unknown; toolCallId?: unknown };
  return { type: 'CUSTOM', name: 'iq-omitted', value: { type, ...(toolCallId === undefined ? {} : { toolCallId }), bytes } };
};

/**
 * Publishes AG-UI events, such as `new MastraAgent({ … }).run(input)` of `@ag-ui/mastra`. `abort()`
 * unsubscribes, which is how that bridge cancels its run, and ends the stream as an `HttpAgent` ends an
 * aborted one: with a `RUN_ERROR` whose code is `abort`.
 */
export function publishAgUi(events: ObservableLike, options: PublishOptions): Promise<PublishedRun> {
  let subscription: { unsubscribe(): void } | undefined;
  let controller!: ReadableStreamDefaultController<unknown>;
  let over = false;
  const finish = (end: () => void): void => {
    if (over) return;
    over = true;
    end();
  };
  const stream = new ReadableStream<unknown>({
    start(streamController) {
      controller = streamController;
      subscription = events.subscribe({
        next: (event) => {
          if (!over) controller.enqueue(event);
        },
        error: (error) => finish(() => controller.error(error)),
        complete: () => finish(() => controller.close()),
      });
      // An Observable that ended while subscribing has left nothing to unsubscribe from.
      if (over) subscription = undefined;
    },
    cancel() {
      over = true;
      subscription?.unsubscribe();
    },
  });
  const abort = (): void =>
    finish(() => {
      subscription?.unsubscribe();
      controller.enqueue({ type: 'RUN_ERROR', message: 'The run was aborted.', code: 'abort' });
      controller.close();
    });
  const oversize = options.oversize === undefined || options.oversize === 'omit' ? omitted : options.oversize;
  return startPublishing(stream, { ...options, oversize }, { classify: classifyAgUiEvent }, { abort });
}
