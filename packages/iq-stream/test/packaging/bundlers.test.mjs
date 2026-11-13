// The promise of the package's export map (see the repository's README, "One package, two sides"):
// whatever is built for a browser contains the client and never the publisher.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { before, describe, test } from 'node:test';
import { MARKS, browserBundlers, execute, fixture, output, publisherTraces } from './support.mjs';

let dir;
before(async () => { dir = await fixture('bundlers'); });
const out = (name) => path.join(dir, 'out', name);

for (const [bundler, build] of Object.entries(browserBundlers)) {
  describe(`${bundler}, building for a browser`, () => {
    for (const entry of ['client', 'fetch', 'ai-sdk', 'ag-ui']) {
      test(`./${entry} brings no publisher code`, async () => {
        await build(dir, `apps/${entry}.js`, out(`${bundler}-${entry}`));
        const text = await output(out(`${bundler}-${entry}`));
        assert.deepEqual(publisherTraces(text), []);
        if (entry === 'client') assert.ok(text.includes(MARKS.client), 'the client is in the bundle');
      });
    }

    test('the root resolves to the stub, which refuses when used', async () => {
      await build(dir, 'apps/root.js', out(`${bundler}-root`));
      const text = await output(out(`${bundler}-root`));
      assert.deepEqual(publisherTraces(text), []);
      assert.ok(text.includes(MARKS.stub), 'the stub is in the bundle');
      const result = execute(path.join(out(`${bundler}-root`), 'app.mjs'));
      assert.match(result, /^use failed: .*@neuron-core\/iq-stream\/client/, 'the message names the right import');
    });

    test('a root import the page never uses leaves the page working', async () => {
      await build(dir, 'apps/root-unused.js', out(`${bundler}-root-unused`));
      assert.deepEqual(publisherTraces(await output(out(`${bundler}-root-unused`))), []);
      assert.equal(execute(path.join(out(`${bundler}-root-unused`), 'app.mjs')), 'loaded');
    });
  });
}

describe('builds for a server', () => {
  test('esbuild, platform node: the node:http build', async () => {
    const { build } = await import('esbuild');
    await build({ absWorkingDir: dir, entryPoints: ['apps/root.js'], bundle: true, format: 'esm', platform: 'node', outfile: path.join(out('esbuild-node'), 'app.mjs'), logLevel: 'silent' });
    assert.ok((await output(out('esbuild-node'))).includes(MARKS.publisherNode));
  });

  test('Vite, SSR build: the node:http build', async () => {
    const { build } = await import('vite');
    await build({ root: dir, logLevel: 'silent', configFile: false, build: { ssr: path.join(dir, 'apps/root.js'), outDir: out('vite-ssr'), emptyOutDir: true, minify: false }, ssr: { noExternal: true } });
    assert.ok((await output(out('vite-ssr'))).includes(MARKS.publisherNode));
  });

  test('webpack, target node: the node:http build', async () => {
    const { default: webpack } = await import('webpack');
    await new Promise((resolve, reject) => webpack({ mode: 'production', target: 'node', context: dir, entry: path.join(dir, 'apps/root.js'), optimization: { minimize: false }, output: { path: out('webpack-node'), filename: 'app.js' } },
      (error, stats) => (error || stats.hasErrors() ? reject(error ?? new Error(stats.toJson({ all: false, errors: true }).errors[0].message)) : resolve())));
    assert.ok((await output(out('webpack-node'))).includes(MARKS.publisherNode));
  });
});

describe('export conditions of the root', () => {
  // esbuild lets a test name the conditions exactly, as each platform's own build does.
  const withConditions = async (name, platform, conditions) => {
    const { build } = await import('esbuild');
    await build({ absWorkingDir: dir, entryPoints: ['apps/root.js'], bundle: true, format: 'esm', platform, conditions, outfile: path.join(out(name), 'app.mjs'), logLevel: 'silent' });
    return output(out(name));
  };

  test('a Workers build (workerd, worker, browser) gets the fetch build, without node: imports', async () => {
    const text = await withConditions('workerd', 'browser', ['workerd', 'worker', 'browser']);
    assert.ok(text.includes(MARKS.publisherFetch));
    assert.doesNotMatch(text, /node:(http|https|crypto|net|tls)/);
  });

  test('an Edge build (edge-light, worker, browser) gets the fetch build', async () => {
    const text = await withConditions('edge-light', 'browser', ['edge-light', 'worker', 'browser']);
    assert.ok(text.includes(MARKS.publisherFetch));
    assert.doesNotMatch(text, /node:(http|https|crypto|net|tls)/);
  });

  test('a browser Web Worker (worker, browser) gets the stub', async () => {
    const text = await withConditions('web-worker', 'browser', ['worker', 'browser']);
    assert.ok(text.includes(MARKS.stub));
    assert.deepEqual(publisherTraces(text), []);
  });

  test('a resolver that names no runtime gets the fetch build', async () => {
    const text = await withConditions('neutral', 'neutral', []);
    assert.ok(text.includes(MARKS.publisherFetch));
  });
});

describe('Node resolving the installed package', () => {
  const node = (script, type = 'module') => execFileSync(process.execPath, [`--input-type=${type}`, '-e', script], { cwd: dir, encoding: 'utf8' }).trim();

  test('import and require of the root give the node:http build', () => {
    assert.equal(node("console.log((await import('@neuron-core/iq-stream')).BUILD)"), MARKS.publisherNode);
    assert.equal(node("console.log(require('@neuron-core/iq-stream').BUILD)", 'commonjs'), MARKS.publisherNode);
  });

  test('the browser entry points load in Node too, for server-side rendering', () => {
    assert.equal(node("console.log((await import('@neuron-core/iq-stream/client')).BUILD)"), MARKS.client);
    assert.equal(node("console.log(typeof (await import('@neuron-core/iq-stream/ai-sdk')).chatTransport)"), 'function');
  });

  test('files the map does not list cannot be imported', () => {
    for (const file of ['dist/publisher/fetch.js', 'dist/browser.js', 'dist/shared/protocol.js']) {
      const code = node(`try { await import('@neuron-core/iq-stream/${file}'); console.log('resolved'); } catch (error) { console.log(error.code); }`);
      assert.equal(code, 'ERR_PACKAGE_PATH_NOT_EXPORTED', file);
    }
  });

  test('the testing kit is for Node only', () => {
    assert.equal(node("console.log(typeof (await import('@neuron-core/iq-stream/testing')).RecordingPublisher)"), 'function');
  });
});
