// A file shared by server and client: it imports the publisher, and the page never uses it.
import { IQClient } from '@neuron-core/iq-stream';
import { BUILD } from '@neuron-core/iq-stream/client';
globalThis.__iq = { BUILD };
