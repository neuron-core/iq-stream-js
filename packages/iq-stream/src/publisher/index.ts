/**
 * The declarations of the package root. The root resolves to `node.js` or to `fetch.js` by export
 * condition; this file states what both export, and is never loaded at run time.
 */
import type { IQClientCore } from './client.js';
import type * as FetchBuild from './fetch.js';
import type * as Declared from './index.js';
import type * as NodeBuild from './node.js';
import type { IQClientOptions } from './types.js';

export { IQError } from '../shared/errors.js';
export { FrameTooLarge } from './frame.js';
export type * from './exports.js';

/** Which build the root resolved to: `iq-stream/publisher/node` or `iq-stream/publisher/fetch`. */
export declare const BUILD: string;

/**
 * The backend's client. It is given the app secret: it mints tokens and publishes streams, and never
 * runs in a browser. `tokenSync()` works in the Node build only.
 */
export declare class IQClient extends IQClientCore {
  constructor(options?: IQClientOptions);
}

// Fails the build when the two builds and these declarations stop exporting the same run-time names.
type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
type Check<T extends true> = T;
type _Parity = [
  Check<Same<keyof typeof NodeBuild, keyof typeof Declared>>,
  Check<Same<keyof typeof FetchBuild, keyof typeof Declared>>,
  Check<Same<InstanceType<typeof NodeBuild.IQClient>, InstanceType<typeof Declared.IQClient>>>,
  Check<Same<InstanceType<typeof FetchBuild.IQClient>, InstanceType<typeof Declared.IQClient>>>,
];
