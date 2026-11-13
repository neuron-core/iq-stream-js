import { IQClientCore } from './client.js';
import { fetchTransport } from './transport.fetch.js';
import type { IQClientOptions } from './types.js';

export { IQError } from '../shared/errors.js';
export { FrameTooLarge } from './frame.js';
export type * from './exports.js';

/** Tells the packaging tests which build a bundle contains. */
export const BUILD = 'iq-stream/publisher/fetch';

/** The publisher's client for Workers, edge runtimes and any runtime that names none: it holds each stream's request with `fetch`. */
export class IQClient extends IQClientCore {
  constructor(options: IQClientOptions = {}) {
    super(options, { transport: (url) => fetchTransport(url, options.fetch) });
  }
}
