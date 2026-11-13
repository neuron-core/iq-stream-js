import { createHmac } from 'node:crypto';
import { IQClientCore } from './client.js';
import { nodeTransport } from './transport.node.js';
import type { IQClientOptions } from './types.js';

export { IQError } from '../shared/errors.js';
export { FrameTooLarge } from './frame.js';
export type * from './exports.js';

/** Tells the packaging tests which build a bundle contains. */
export const BUILD = 'iq-stream/publisher/node';

/** The publisher's client for Node, Bun and Deno: it holds each stream's request with `node:http` or `node:https`. */
export class IQClient extends IQClientCore {
  constructor(options: IQClientOptions = {}) {
    super(options, {
      transport: nodeTransport,
      hmacSync: (secret, message) => createHmac('sha256', secret).update(message).digest('base64url'),
    });
  }
}
