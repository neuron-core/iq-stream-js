// Every entry point, as an application would import it. `tsc` must accept this under each resolution mode.
import { IQClient, FrameTooLarge, type Publisher, type PublishResult } from '@neuron-core/iq-stream';
import { subscribe, frames, IQStreamError, type Outcome } from '@neuron-core/iq-stream/client';
import { fetchSource } from '@neuron-core/iq-stream/fetch';
import { chatTransport } from '@neuron-core/iq-stream/ai-sdk';
import { StreamAgent } from '@neuron-core/iq-stream/ag-ui';
import { FakeEventSource } from '@neuron-core/iq-stream/client/testing';

const client = new IQClient({ url: 'http://127.0.0.1:8080', appKey: 'app', appSecret: 'x'.repeat(32) });
const publisher: Publisher = client.publisher(client.newStreamId());
const sent: boolean = publisher.send('{"type":"x"}');
const result: Promise<PublishResult> = publisher.close('end');

const subscription = subscribe('http://127.0.0.1:8080/x', { onFrame(frame) { frame.data.toUpperCase(); }, onClose(outcome: Outcome) { outcome.type; } });
const stream: ReadableStream<{ a: number }> = frames('http://127.0.0.1:8080/x', { parse: (data) => JSON.parse(data) as { a: number } });
const transport = chatTransport({ start: async () => ({ url: 'http://127.0.0.1:8080/x' }) });

export { FrameTooLarge, IQStreamError, FakeEventSource, StreamAgent, fetchSource, sent, result, subscription, stream, transport };
