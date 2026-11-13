import http from 'node:http';
import https from 'node:https';
import { NO_ANSWER, parseBody } from './transport.js';
import type { Answer, OpenRequest, Transport } from './transport.js';

const CONNECT_TIMEOUT_MS = 5_000;

/** Resolves with the server's answer, or with status 0 when the request ends without a complete one. */
function answerOf(request: http.ClientRequest, onResponse?: () => void): Promise<Answer> {
  return new Promise((resolve) => {
    const none = (): void => resolve(NO_ANSWER);
    request.on('response', (response) => {
      onResponse?.();
      let text = '';
      response.setEncoding('utf8');
      response.on('data', (chunk: string) => {
        text += chunk;
      });
      response.on('end', () => resolve({ status: response.statusCode ?? 0, body: parseBody(text) }));
      response.on('error', none);
      response.on('close', none);
    });
    request.on('error', none);
    request.on('close', none);
  });
}

/**
 * `node:http` reports an early answer as an event, without any write, so a 408 is seen when it happens.
 * Each request has a connection of its own (`agent: false`): a stream's request is held for as long as the answer takes.
 */
export function nodeTransport(url: string): Transport {
  const socketPath = url.startsWith('unix://') ? url.slice('unix://'.length) : undefined;
  const tls = url.startsWith('https:');

  const start = (streamId: string, token: string, length?: number): http.ClientRequest => {
    const options: http.RequestOptions = {
      method: 'POST',
      agent: false,
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/x-ndjson',
        ...(length === undefined ? {} : { 'Content-Length': length }),
      },
    };
    if (socketPath !== undefined) return http.request({ ...options, socketPath, path: `/${streamId}` });
    return (tls ? https : http).request(`${url}/${streamId}`, options);
  };

  return {
    open(streamId, token): OpenRequest {
      const request = start(streamId, token);
      let settled = false;
      let pending: (() => void) | undefined;
      const connecting = setTimeout(() => request.destroy(), CONNECT_TIMEOUT_MS);
      connecting.unref();
      const settle = (): void => {
        settled = true;
        clearTimeout(connecting);
        pending?.();
      };
      request.on('socket', (socket) => {
        // An open publisher must not keep a process alive by itself; close() holds it until the answer.
        socket.unref();
        socket.once(tls ? 'secureConnect' : 'connect', () => clearTimeout(connecting));
      });
      // A small line must not wait for the previous one to be acknowledged.
      request.setNoDelay(true);
      const answer = answerOf(request, settle).then((result) => {
        settle();
        return result;
      });
      // The head leaves now, not with the first line: a subscriber may already be waiting for its publisher.
      request.flushHeaders();
      return {
        get settled() {
          return settled;
        },
        answer,
        write: (bytes) =>
          new Promise<void>((done) => {
            if (settled) return done();
            pending = done;
            request.write(bytes, () => done());
          }),
        end: () => {
          request.end();
        },
        destroy: () => {
          request.destroy();
        },
      };
    },

    post(streamId, token, body, timeoutMs): Promise<Answer> {
      const request = start(streamId, token, body.length);
      const timer = setTimeout(() => request.destroy(), timeoutMs);
      const answer = answerOf(request).then((result) => {
        clearTimeout(timer);
        request.destroy();
        return result;
      });
      request.end(body);
      return answer;
    },
  };
}
