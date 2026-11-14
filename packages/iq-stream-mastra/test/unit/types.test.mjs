// The README's samples, as TypeScript, compiled against the built package resolved by name: what an
// application's own `tsc` makes of the public types, with Mastra's real ones as arguments.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { test } from 'node:test';

const packageDir = path.resolve(import.meta.dirname, '../..');

test('the samples of the README compile, strictly', { timeout: 120_000 }, () => {
  let output = '';
  try {
    execFileSync(path.join(packageDir, '../../node_modules/.bin/tsc'), ['-p', 'test/types/tsconfig.json'], { cwd: packageDir, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
  } catch (error) {
    output = `${error.stdout ?? ''}${error.stderr ?? ''}` || String(error);
  }
  assert.equal(output, '');
});
