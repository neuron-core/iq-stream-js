import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';
import * as stub from '../../../dist/browser.js';
import * as fetchBuild from '../../../dist/publisher/fetch.js';
import { refuseBrowser } from '../../../dist/publisher/guard.js';
import * as nodeBuild from '../../../dist/publisher/node.js';
import { IQError } from '../../../dist/shared/errors.js';

const options = { url: 'http://iq.test', appKey: 'demo-app', appSecret: 'test-app-secret-0123456789abcdef0123' };
const dist = new URL('../../../dist/', import.meta.url);
const names = (module) => Object.keys(module).sort();

test('the two builds and the stub export the same names', () => {
  assert.deepEqual(names(nodeBuild), ['BUILD', 'FrameTooLarge', 'IQClient', 'IQError']);
  assert.deepEqual(names(fetchBuild), names(nodeBuild));
  assert.deepEqual(names(stub), names(nodeBuild), 'an export added to the publisher must be added to the stub');
});

test('the declarations of the root declare exactly those names', async () => {
  const declarations = await readFile(new URL('publisher/index.d.ts', dist), 'utf8');
  const declared = [...declarations.matchAll(/^export (?:declare (?:const|class|function) (\w+)|\{ (\w+) \})/gm)].map((match) => match[1] ?? match[2]).sort();
  assert.deepEqual(declared, names(nodeBuild));
});

test('each build names itself', () => {
  assert.equal(nodeBuild.BUILD, 'iq-stream/publisher/node');
  assert.equal(fetchBuild.BUILD, 'iq-stream/publisher/fetch');
  assert.equal(stub.BUILD, 'iq-stream/browser-stub');
});

test('the two builds share their error classes and the client core', () => {
  assert.equal(nodeBuild.IQError, IQError);
  assert.equal(fetchBuild.IQError, IQError);
  assert.equal(nodeBuild.FrameTooLarge, fetchBuild.FrameTooLarge);
  const methods = (build) => Object.getOwnPropertyNames(Object.getPrototypeOf(build.IQClient.prototype)).sort();
  assert.deepEqual(methods(nodeBuild), methods(fetchBuild));
  for (const method of ['newStreamId', 'token', 'tokenSync', 'subscribeUrl', 'ticket', 'verify', 'reserve', 'closeStream', 'publisher']) {
    assert.ok(methods(nodeBuild).includes(method), method);
  }
});

test('every export of the stub refuses when used, and says what to import', () => {
  const message = /does not run in a browser\. Import "@neuron-core\/iq-stream\/client" in the page\./;
  assert.throws(() => new stub.IQClient(options), message);
  assert.throws(() => new stub.IQError('x'), message);
  assert.throws(() => new stub.FrameTooLarge(1, 2), message);
});

test('only the Node build imports node: modules, and the stub and shared/ import nothing', async () => {
  const imports = async (file) => [...(await readFile(new URL(file, dist), 'utf8')).matchAll(/^\s*(?:import|export)\b[^'"\n]*from\s+['"]([^'"]+)['"]/gm)].map((match) => match[1]);
  assert.deepEqual(await imports('browser.js'), []);
  assert.deepEqual(await imports('shared/protocol.js'), []);
  assert.deepEqual(await imports('shared/errors.js'), []);

  const reach = async (entry, seen = new Set()) => {
    if (seen.has(entry)) return seen;
    seen.add(entry);
    for (const specifier of await imports(entry)) {
      if (specifier.startsWith('.')) await reach(new URL(specifier, new URL(entry, dist)).pathname.slice(dist.pathname.length), seen);
      else seen.add(specifier);
    }
    return seen;
  };
  const fetchGraph = [...(await reach('publisher/fetch.js'))];
  assert.deepEqual(fetchGraph.filter((file) => file.startsWith('node:') || file.includes('.node.')), [], 'the fetch build runs where node: does not exist');
  const nodeGraph = [...(await reach('publisher/node.js'))];
  assert.deepEqual(nodeGraph.filter((file) => file.startsWith('node:')).sort(), ['node:crypto', 'node:http', 'node:https']);
  assert.deepEqual(nodeGraph.filter((file) => !file.startsWith('node:') && !/^(publisher|shared)\//.test(file)), [], 'the publisher imports from publisher/ and shared/ only');
});

function inBrowser(run) {
  globalThis.window = globalThis;
  globalThis.document = {};
  try {
    return run();
  } finally {
    delete globalThis.window;
    delete globalThis.document;
  }
}

test('the guard: the publisher refuses to be constructed where there is a window and a document', () => {
  const message = /must not run in a browser\. Use "@neuron-core\/iq-stream\/client" in the page\./;
  for (const build of [nodeBuild, fetchBuild]) {
    assert.doesNotThrow(() => new build.IQClient(options));
    inBrowser(() => {
      assert.throws(() => new build.IQClient(options), (error) => error instanceof IQError && message.test(error.message));
      assert.throws(() => new build.IQClient(), message, 'before anything else is looked at');
    });
    assert.doesNotThrow(() => new build.IQClient(options));
  }
  inBrowser(() => assert.throws(refuseBrowser, message));
  assert.doesNotThrow(refuseBrowser);
});

test('the guard needs both: a window alone, as in some server runtimes, or a document alone, does not trip it', () => {
  globalThis.window = globalThis;
  try {
    assert.doesNotThrow(() => new fetchBuild.IQClient(options));
  } finally {
    delete globalThis.window;
  }
  globalThis.document = {};
  try {
    assert.doesNotThrow(() => new nodeBuild.IQClient(options));
  } finally {
    delete globalThis.document;
  }
});
