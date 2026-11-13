// What TypeScript and the two packaging linters make of the published tarball.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { cp } from 'node:fs/promises';
import path from 'node:path';
import { before, describe, test } from 'node:test';
import { fixture, pack, packageDir } from './support.mjs';

const bin = (name) => path.join(packageDir, '../../node_modules/.bin', name);
const run = (command, args, cwd) => {
  try { return { ok: true, text: execFileSync(command, args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }) }; }
  catch (error) { return { ok: false, text: `${error.stdout ?? ''}${error.stderr ?? ''}` }; }
};

let dir;
before(async () => {
  dir = await fixture('types');
  await cp(path.join(import.meta.dirname, 'types'), path.join(dir, 'types'), { recursive: true });
});

describe('TypeScript resolves every entry point', () => {
  for (const mode of ['bundler', 'nodenext', 'browser']) {
    test(`moduleResolution ${mode === 'browser' ? 'bundler with the browser condition' : mode}`, () => {
      const result = run(bin('tsc'), ['-p', `types/tsconfig.${mode}.json`], dir);
      assert.ok(result.ok, result.text);
    });
  }
});

describe('packaging linters', () => {
  test('publint finds nothing', () => {
    const result = run(bin('publint'), [pack()], dir);
    assert.ok(result.ok, result.text);
  });

  test('arethetypeswrong passes for an ES-module-only package', () => {
    const result = run(bin('attw'), [pack(), '--profile', 'esm-only'], dir);
    assert.ok(result.ok, result.text);
  });
});
