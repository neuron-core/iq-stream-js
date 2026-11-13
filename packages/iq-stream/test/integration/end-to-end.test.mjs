// The two sides of the package together: the publisher feeds a real server, and the client reads it
// through the ticket the publisher handed out.
import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { EventSource } from 'eventsource';
import { IQClient } from '../../dist/publisher/node.js';
import { frames, subscribe, IQStreamError } from '../../dist/client/index.js';
import { testServers } from '../../dist/testing/index.js';

let servers, client;
before(async () => {
  servers = await testServers();
  client = new IQClient({ url: servers.default.url, appKey: servers.appKey, appSecret: servers.appSecret, minOpen: 0 });
});
after(() => servers.stop());

test('an answer goes from publisher.send() to the client, in order, and ends with `end`', async () => {
  const ticket = await client.ticket();
  const received = [];
  const subscription = subscribe(ticket.url, { onFrame: (frame) => received.push(JSON.parse(frame.data)) }, { EventSource });
  await subscription.opened;

  const publisher = client.publisher(ticket.streamId);
  for (const delta of ['It ', 'is ', 'sunny.']) publisher.send(JSON.stringify({ type: 'text-delta', delta }));
  const result = await publisher.close('end');

  assert.deepEqual(await subscription.done, { type: 'end', lastEventId: 3 });
  assert.equal(received.map((chunk) => chunk.delta).join(''), 'It is sunny.');
  assert.equal(result.lastEventId, 3);
  assert.equal(publisher.requests, 1, 'one request for the whole answer');
});

test('a failed run reaches the client as a failure, after the frames that were sent', async () => {
  const ticket = await client.ticket();
  const publisher = client.publisher(ticket.streamId);
  publisher.send('{"type":"text-delta","delta":"partial"}');
  const stream = frames(ticket.url, { parse: JSON.parse, EventSource });
  const received = [];
  const reading = (async () => { for await (const chunk of stream) received.push(chunk.delta); })();
  const failed = assert.rejects(reading, (error) => error instanceof IQStreamError && error.kind === 'fail' && error.reason === 'publisher');
  await publisher.close('fail');

  await failed;
  assert.deepEqual(received, ['partial']);
});

test('a ticket is for one stream: its token opens no other', async () => {
  const ticket = await client.ticket();
  const other = client.newStreamId();
  const outcome = await subscribe(ticket.url.replace(ticket.streamId, other), { onFrame() {} }, { EventSource }).done;
  assert.equal(outcome.type, 'refused');
  assert.equal(await client.verify('sub', ticket.token, ticket.streamId), true);
  assert.equal(await client.verify('sub', ticket.token, other), false);
});

test('a quick answer is not lost to a subscriber that arrives after it: the close line waits for minOpen', async () => {
  const patient = new IQClient({ url: servers.default.url, appKey: servers.appKey, appSecret: servers.appSecret, minOpen: 1.5 });
  const ticket = await patient.ticket();
  const publisher = patient.publisher(ticket.streamId);
  publisher.send('quick');
  const closing = publisher.close('end');
  await new Promise((resolve) => setTimeout(resolve, 400));
  const received = [];
  const outcome = await subscribe(ticket.url, { onFrame: (frame) => received.push(frame.data) }, { EventSource }).done;
  await closing;
  assert.deepEqual(received, ['quick']);
  assert.equal(outcome.type, 'end');
});
