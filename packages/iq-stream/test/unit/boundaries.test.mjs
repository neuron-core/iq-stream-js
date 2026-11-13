// The export map separates the entry points; these rules keep the source tree from joining them
// again. They read the TypeScript sources, so they hold before anything is built.
import assert from 'node:assert/strict';
import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { test } from 'node:test';

const src = path.resolve(import.meta.dirname, '../../src');
const files = (await readdir(src, { recursive: true })).filter((file) => file.endsWith('.ts')).map((file) => file.split(path.sep).join('/'));

async function imports(file) {
  const text = await readFile(path.join(src, file), 'utf8');
  // `import type` and `export type` are erased by the compiler: they join nothing at run time.
  const statements = [...text.matchAll(/(?:^|\n)\s*(import|export)(\s+type)?\s[^;]*?from\s+['"]([^'"]+)['"]/g)].filter((match) => !match[2]).map((match) => match[3]);
  const dynamic = [...text.matchAll(/\bimport\(\s*['"]([^'"]+)['"]\s*\)/g)].map((match) => match[1]);
  return [...statements, ...dynamic];
}
const area = (file) => (file.includes('/') ? file.split('/')[0] : file);
const target = (file, specifier) => path.posix.normalize(path.posix.join(path.posix.dirname(file), specifier));

test('only the publisher and the testing kit import the publisher', async () => {
  for (const file of files) {
    if (['publisher', 'testing'].includes(area(file))) continue;
    for (const specifier of await imports(file)) {
      if (specifier.startsWith('.')) assert.ok(!target(file, specifier).startsWith('publisher/'), `${file} imports ${specifier}`);
    }
  }
});

test('node: modules are imported only by the Node transport and the testing kit', async () => {
  for (const file of files) {
    const allowed = file === 'publisher/node.ts' || file.endsWith('.node.ts') || file.startsWith('testing/');
    for (const specifier of await imports(file)) {
      if (specifier.startsWith('node:')) assert.ok(allowed, `${file} imports ${specifier}`);
    }
  }
});

test('files with node: imports are reachable only from the Node build', async () => {
  for (const file of files) {
    if (file === 'publisher/node.ts' || file.endsWith('.node.ts') || file.startsWith('testing/')) continue;
    for (const specifier of await imports(file)) {
      assert.ok(!/\.node\.js$/.test(specifier) && !/(^|\/)node\.js$/.test(specifier), `${file} imports ${specifier}`);
    }
  }
});

test('the shared modules and the browser stub import nothing', async () => {
  for (const file of files) {
    if (area(file) === 'shared') {
      for (const specifier of await imports(file)) assert.ok(target(file, specifier).startsWith('shared/'), `${file} imports ${specifier}`);
    }
    if (file === 'browser.ts') assert.deepEqual(await imports(file), [], 'browser.ts has imports');
  }
});

test('the browser entry points use no bare import beyond their optional peers', async () => {
  const peers = { 'ai-sdk': ['ai'], 'ag-ui': ['@ag-ui/client', 'rxjs'] };
  for (const file of files) {
    if (!['client', 'fetch', 'ai-sdk', 'ag-ui', 'shared'].includes(area(file))) continue;
    for (const specifier of await imports(file)) {
      if (!specifier.startsWith('.')) assert.ok((peers[area(file)] ?? []).includes(specifier), `${file} imports ${specifier}`);
    }
  }
});
