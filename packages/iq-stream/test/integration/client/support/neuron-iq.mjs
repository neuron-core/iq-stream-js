// `subscribeToIq` as @neuron-core/streaming is to have it (frontend-module.md, "What changes in
// @neuron-core/streaming"): Neuron envelopes carried as IQ frames, handed to that package's own consumer.
// It belongs to that package; it is kept here to test that this package's client is all it needs.
import { createChannelConsumer } from '@neuron-core/streaming';
import { subscribe } from '../../../../dist/client/index.js';

export function subscribeToIq(target, callbacks, streamId, options) {
  let closed = false;
  const consumer = createChannelConsumer({ onEvent: callbacks.onEvent, onGap: fail }, streamId);
  const sub = subscribe(target, {
    onFrame({ data }) {
      let envelope;
      try { envelope = JSON.parse(data); } catch { return fail('Invalid channel envelope'); }
      consumer.accept(envelope);
    },
    // iq:end after Neuron's own terminal envelope is a clean end; anything else means frames are missing.
    onClose(outcome) { if (outcome.type !== 'end' && outcome.type !== 'closed') fail(outcome.type === 'fail' ? `iq:fail ${outcome.reason}` : `iq:${outcome.type}`); },
  }, options);
  function close() { if (closed) return; closed = true; sub.close(); consumer.close(); }
  function fail(reason) { if (closed) return; close(); callbacks.onGap(reason); }
  return { close };
}
