// What every browser test file starts: the backend, Chromium, and one page that fails the test on an
// uncaught error. `entries` are page sources that esbuild bundles into a temporary directory first.
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { build } from 'esbuild';
import { chromium } from 'playwright';
import { startBackend } from './backend.mjs';

export async function startBrowser({ entries = [] } = {}) {
  const bundles = await mkdtemp(join(tmpdir(), 'iq-stream-browser-'));
  if (entries.length) {
    await build({
      entryPoints: entries.map((entry) => join(import.meta.dirname, 'pages', entry)),
      outdir: bundles, bundle: true, format: 'esm', platform: 'browser', jsx: 'automatic', logLevel: 'error',
    });
  }
  const backend = await startBackend({ bundles });
  const browser = await chromium.launch();
  const page = await browser.newPage();
  const pageErrors = [];
  page.on('pageerror', (error) => pageErrors.push(error.message));
  await page.goto(`${backend.origin}/`);
  return {
    backend, browser, page, pageErrors,
    async close() {
      await browser.close();
      await backend.close();
      await rm(bundles, { recursive: true, force: true });
    },
  };
}
