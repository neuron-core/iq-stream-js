// Next.js puts a server, an edge runtime and a browser in one project, which is where the export map
// is most likely to be wrong. This builds a small application with each of its bundlers. It installs
// Next.js and takes a few minutes, so it runs only when IQ_STREAM_TEST_NEXT=1.
import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { cp, rm } from 'node:fs/promises';
import path from 'node:path';
import { after, before, describe, test } from 'node:test';
import { MARKS, fixture, output, pack, publisherTraces } from './support.mjs';

const enabled = process.env.IQ_STREAM_TEST_NEXT === '1';
const PORT = 3190;
const env = { ...process.env, NEXT_TELEMETRY_DISABLED: '1' };

describe('Next.js', { skip: enabled ? false : 'set IQ_STREAM_TEST_NEXT=1 to run' }, () => {
  let dir;
  before(async () => {
    dir = path.join(await fixture('next'), 'app');
    await cp(path.join(import.meta.dirname, 'next-app'), dir, { recursive: true });
    execFileSync('npm', ['install', '--silent', '--no-audit', '--no-fund', 'next', 'react', 'react-dom', pack()], { cwd: dir, env, stdio: 'inherit' });
  });

  for (const bundler of ['turbopack', 'webpack']) {
    describe(bundler, () => {
      let server;
      before(async () => {
        await rm(path.join(dir, '.next'), { recursive: true, force: true });
        execFileSync('npx', ['next', 'build', ...(bundler === 'webpack' ? ['--webpack'] : [])], { cwd: dir, env, stdio: ['ignore', 'ignore', 'inherit'] });
        server = spawn('npx', ['next', 'start', '-p', String(PORT)], { cwd: dir, env, stdio: 'ignore', detached: true });
        for (let i = 0; i < 60; i++) {
          try { if ((await fetch(`http://127.0.0.1:${PORT}/`)).ok) return; } catch { /* not up yet */ }
          await new Promise((resolve) => setTimeout(resolve, 250));
        }
        throw new Error('next start did not answer');
      });
      after(() => { try { process.kill(-server.pid); } catch { /* already gone */ } });

      test('no publisher code in what browsers download', async () => {
        const text = await output(path.join(dir, '.next/static'));
        assert.deepEqual(publisherTraces(text), []);
        assert.ok(text.includes(MARKS.client), 'the client is there');
      });

      test('a route handler on the Node.js runtime gets the node:http build', async () => {
        const body = await (await fetch(`http://127.0.0.1:${PORT}/api/node`)).json();
        assert.deepEqual(body, { runtime: 'nodejs', BUILD: MARKS.publisherNode, verified: true });
      });

      test('a route handler on the Edge runtime gets the fetch build', async () => {
        const body = await (await fetch(`http://127.0.0.1:${PORT}/api/edge`)).json();
        assert.deepEqual(body, { runtime: 'edge', BUILD: MARKS.publisherFetch, verified: true });
      });

      test('a client component that imports the publisher is told so, in a real browser', async () => {
        const { chromium } = await import('playwright');
        const browser = await chromium.launch();
        try {
          const page = await browser.newPage();
          await page.goto(`http://127.0.0.1:${PORT}/misuse`);
          await page.waitForFunction(() => document.querySelector('#misuse')?.textContent !== 'waiting');
          assert.match(await page.textContent('#misuse'), /@neuron-core\/iq-stream\/client/);
          await page.goto(`http://127.0.0.1:${PORT}/`);
          assert.equal(await page.textContent('#client'), `${MARKS.client} function`);
        } finally {
          await browser.close();
        }
      });
    });
  }
});
