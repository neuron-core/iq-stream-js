#!/usr/bin/env node
// Starts and stops the two IQ-Stream servers the integration and browser tests run against, in Docker:
// one with default settings on port 8080, one with short timeouts and small limits on port 8081.
//
//   node test/servers.mjs start     (image: IQ_STREAM_TEST_IMAGE, default ghcr.io/neuron-core/iq-stream:1)
//   node test/servers.mjs stop
//
// Tests look for servers on those two addresses, or on IQ_STREAM_TEST_URL and IQ_STREAM_TEST_SHORT_URL.
import { execFileSync } from 'node:child_process';
import { readFile, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { SHORT_SETTINGS, startServer } from '../dist/testing/index.js';

const record = path.join(import.meta.dirname, '.servers.json');
const command = process.argv[2];

if (command === 'start') {
  const servers = [await startServer({ port: 8080 }), await startServer({ port: 8081, env: { ...SHORT_SETTINGS } })];
  await writeFile(record, JSON.stringify(servers.map((server) => server.container)));
  for (const server of servers) console.log(`IQ-Stream server on ${server.url}`);
} else if (command === 'stop') {
  const containers = JSON.parse(await readFile(record, 'utf8').catch(() => '[]'));
  if (containers.length) execFileSync('docker', ['rm', '-f', ...containers], { stdio: 'ignore' });
  await rm(record, { force: true });
} else {
  console.error('usage: node test/servers.mjs start|stop');
  process.exit(2);
}
