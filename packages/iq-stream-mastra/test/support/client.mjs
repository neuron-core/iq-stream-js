// A real IQClient, so that tickets are real, whose publishers record in memory: no server is contacted.
import { IQClient } from '@neuron-core/iq-stream';
import { RecordingPublisher } from '@neuron-core/iq-stream/testing';

export const APP_KEY = 'demo-app';
export const APP_SECRET = 'test-app-secret-0123456789abcdef0123';

/** `make` builds the publisher of a stream; by default a RecordingPublisher. `publishers` lists them in order. */
export function recordingClient({ make = (streamId) => new RecordingPublisher({ streamId }), ...options } = {}) {
  const iq = new IQClient({ url: 'http://127.0.0.1:9', publicUrl: 'https://stream.example.com', appKey: APP_KEY, appSecret: APP_SECRET, ...options });
  const publishers = [];
  iq.publisher = (streamId) => {
    const publisher = make(streamId);
    publishers.push(publisher);
    return publisher;
  };
  return { iq, publishers };
}
