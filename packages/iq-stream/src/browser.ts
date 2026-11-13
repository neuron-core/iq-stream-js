/**
 * What the package root resolves to in a browser build. It has the publisher's export names and does
 * nothing until one of them is used, so an import that a page never uses costs nothing and breaks
 * nothing. This module imports nothing.
 */
const refuse = (): never => {
  throw new Error(
    '@neuron-core/iq-stream: the publisher is given your app secret and does not run in a browser. Import "@neuron-core/iq-stream/client" in the page.',
  );
};

/** Tells the packaging tests which build a bundle contains. */
export const BUILD = 'iq-stream/browser-stub';

export class IQClient {
  constructor() {
    refuse();
  }
}

export class IQError extends Error {
  constructor() {
    super();
    refuse();
  }
}

export class FrameTooLarge extends IQError {}
