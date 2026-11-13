#!/usr/bin/env node
// Runs the test files under a directory with `node --test`. The runner takes glob patterns only from
// Node 21 on and directories only before it; a list of files is what every supported version accepts.
//
//   node test/run.mjs <directory> [flags for node --test]
import { spawnSync } from 'node:child_process';
import { readdirSync } from 'node:fs';
import path from 'node:path';

const [directory, ...flags] = process.argv.slice(2);
if (!directory) {
  console.error('usage: node test/run.mjs <directory> [flags for node --test]');
  process.exit(2);
}
const files = readdirSync(directory, { recursive: true })
  .filter((file) => file.endsWith('.test.mjs') && !/(^|[\\/])(\.out|node_modules)[\\/]/.test(file))
  .map((file) => path.join(directory, file))
  .sort();
if (files.length === 0) {
  console.error(`no test files under ${directory}`);
  process.exit(1);
}
process.exit(spawnSync(process.execPath, ['--test', ...flags, ...files], { stdio: 'inherit' }).status ?? 1);
