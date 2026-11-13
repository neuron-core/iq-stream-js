// A page that imports the publisher and uses it: the mistake the package must make harmless.
import { IQClient, BUILD } from '@neuron-core/iq-stream';
globalThis.__iq = { BUILD };
globalThis.__use = () => new IQClient({ url: 'http://127.0.0.1:1', appKey: 'app', appSecret: 'x'.repeat(32) });
