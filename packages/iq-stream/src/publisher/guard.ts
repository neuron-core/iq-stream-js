import { IQError } from '../shared/errors.js';

/**
 * Last line of defence, for a resolver that ignored the `browser` export condition: a page has a
 * window and a document. There is no option to turn this off, because no page should hold the app secret.
 */
export function refuseBrowser(): void {
  if (typeof document !== 'undefined' && typeof window !== 'undefined') {
    throw new IQError(
      '@neuron-core/iq-stream: the publisher is given your app secret and must not run in a browser. Use "@neuron-core/iq-stream/client" in the page.',
    );
  }
}
