import assert from 'node:assert/strict';
import { test } from 'node:test';
import { FrameTooLarge, closeLine, encodeFrame, frameSize } from '../../../dist/publisher/frame.js';
import { IQError } from '../../../dist/shared/errors.js';
import { setup } from './fake-transport.mjs';

const text = (bytes) => new TextDecoder().decode(bytes);

test('a frame is one compact JSON line, with non-ASCII text and slashes unescaped', () => {
  const table = [
    ['Hi', undefined, '{"data":"Hi"}\n'],
    ['Hi', 'token', '{"event":"token","data":"Hi"}\n'],
    ['', undefined, '{"data":""}\n'],
    ['héllo wörld', undefined, '{"data":"héllo wörld"}\n'],
    ['日本語 🎉', undefined, '{"data":"日本語 🎉"}\n'],
    ['a\nb\r\nc\td', undefined, '{"data":"a\\nb\\r\\nc\\td"}\n'],
    ['say "hi" \\ now', undefined, '{"data":"say \\"hi\\" \\\\ now"}\n'],
    ['https://example.com/a/b', undefined, '{"data":"https://example.com/a/b"}\n'],
    ['{"type":"TEXT","delta":"é"}', undefined, '{"data":"{\\"type\\":\\"TEXT\\",\\"delta\\":\\"é\\"}"}\n'],
    ['\u0000\u001f', undefined, '{"data":"\\u0000\\u001f"}\n'],
  ];
  for (const [data, event, line] of table) {
    assert.equal(text(encodeFrame(data, event)), line);
    assert.deepEqual(JSON.parse(line), event === undefined ? { data } : { event, data });
  }
});

test('the size of a frame is its line in UTF-8 bytes, without the newline', () => {
  assert.equal(frameSize('Hi'), 13);
  assert.equal(frameSize('é'), 11 + 2);
  assert.equal(frameSize('🎉'), 11 + 4);
  assert.equal(frameSize('a\nb'), 11 + 4);
  assert.equal(frameSize('"'), 11 + 2);
  assert.equal(frameSize('x', 'token'), '{"event":"token","data":"x"}'.length);
});

test('event names are checked at each boundary of the pattern', () => {
  for (const event of ['a', 'A', '0', '_', '.', '-', 'run.step-1_done', 'x'.repeat(64), 'end', 'fail', 'gone']) {
    assert.deepEqual(JSON.parse(text(encodeFrame('d', event))), { event, data: 'd' });
  }
  for (const event of ['', 'x'.repeat(65), 'iq:end', 'a:b', 'a b', 'a/b', 'é', 'a\n', 42, {}]) {
    assert.throws(() => encodeFrame('d', event), TypeError, `event ${JSON.stringify(event)}`);
  }
});

test('data must be a string', () => {
  for (const data of [undefined, null, 42, { a: 1 }, ['a']]) assert.throws(() => encodeFrame(data), TypeError);
});

test('a close line', () => {
  assert.equal(text(closeLine('end')), '{"close":"end"}\n');
  assert.equal(text(closeLine('fail')), '{"close":"fail"}\n');
});

test('send() accepts a line of exactly the limit and refuses one byte more, synchronously', async () => {
  const { publisher, transport, errors } = setup({ maxFrameBytes: 1024 });
  const pub = publisher();
  const fits = 'x'.repeat(1024 - 11);
  assert.equal(pub.maxFrameBytes, 1024);
  assert.equal(pub.frameSize(fits), 1024);
  assert.equal(pub.send(fits), true);
  assert.equal(pub.send('é'.repeat((1024 - 11) / 2) + 'x'), true, 'bytes are counted, not characters');

  const error = (() => {
    try {
      pub.send(`${fits}x`);
    } catch (thrown) {
      return thrown;
    }
  })();
  assert.ok(error instanceof FrameTooLarge);
  assert.ok(error instanceof IQError);
  assert.equal(error.size, 1025);
  assert.equal(error.limit, 1024);
  assert.ok(!error.message.includes('xxx'), 'no frame data in the message');
  assert.throws(() => pub.send('é'.repeat(507)), FrameTooLarge);
  assert.throws(() => pub.send('x'.repeat(1024 - 26), 'token'), FrameTooLarge);

  assert.equal(pub.alive, true, 'a refused frame is the caller\'s to fix: the publisher goes on');
  assert.equal(pub.send('after'), true);
  assert.equal(pub.framesSent, 3);
  const request = await transport.request(0);
  request.respond(200, { lastEventId: 3, closed: true, outcome: 'end' });
  await pub.close();
  assert.deepEqual(request.data.map((data) => data.length), [1013, 507, 5]);
  assert.deepEqual(errors, []);
});
