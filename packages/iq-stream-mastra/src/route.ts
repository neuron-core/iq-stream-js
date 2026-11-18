import type { ChatStreamHandlerOptions } from '@mastra/ai-sdk';
import type { Mastra } from '@mastra/core/mastra';
import { MASTRA_RESOURCE_ID_KEY } from '@mastra/core/request-context';
import type { RequestContext } from '@mastra/core/request-context';
import { registerApiRoute } from '@mastra/core/server';
import type { ApiRoute } from '@mastra/core/server';
import { publishChat } from './chat.js';
import type { IQTicket, PublishOptions } from './types.js';

export type IQChatRouteOptions = Omit<PublishOptions, 'streamId'> &
  Omit<ChatStreamHandlerOptions, 'mastra' | 'agentId' | 'params'> & {
    /** Where the routes are registered. May contain `:agentId`. */
    path: string;
    /** The agent, when the path does not name it. */
    agent?: string;
    /** Registers `GET <path>/:chatId/stream`, which `useChat({ resume: true })` asks after a page reload. Default true. */
    reconnect?: boolean;
  };

/** What the reconnect route needs to mint a new ticket for a run in flight. */
interface ActiveRun {
  streamId: string;
  runId?: string;
}

function knows(mastra: Mastra, agentId: string): boolean {
  try {
    return Boolean(mastra.getAgentById(agentId));
  } catch {
    return false;
  }
}

/** A stream lives one hour at most on the server; an entry that is never deleted goes with it. */
const ACTIVE_RUN_TTL_MS = 65 * 60 * 1000;

/**
 * The chat routes for Mastra's server, to put in `server.apiRoutes`:
 *
 * - `POST <path>` starts the agent on a `useChat` request body and answers the ticket at once, as JSON.
 *   The run goes on after the response and is published in the background.
 * - `POST <path>/abort` with `{ "streamId": "…" }` cancels that run: `204`, or `404` when this process does not run it.
 * - `GET <path>/:chatId/stream` answers a new ticket for the run in flight for that chat, or `204`.
 *
 * The routes go through Mastra's authentication like any `registerApiRoute` route. A ticket is as good
 * as the answer: do not take them out of it.
 */
export function iqChatRoute(options: IQChatRouteOptions): ApiRoute[] {
  const { path, agent, reconnect = true, ...chat } = options;
  const { iq, subTokenTtl, defaultOptions } = chat;
  if (!agent && !path.includes('/:agentId')) {
    throw new Error('iqChatRoute: the path must contain :agentId, or `agent` must name the agent.');
  }
  const base = path.replace(/\/$/, '');
  /** The runs this process can cancel, by stream id. */
  const aborts = new Map<string, (reason?: unknown) => void>();

  // Only a request context the server made counts, never one from a request body: the resource id in
  // it is what keeps one user's chat id from answering another user's reconnect.
  const chatKey = (agentId: string, chatId: string, context: RequestContext | undefined): string =>
    `iq-stream:chat:${JSON.stringify([agentId, context?.get(MASTRA_RESOURCE_ID_KEY) ?? null, chatId])}`;

  const start = registerApiRoute(path, {
    method: 'POST',
    handler: async (c) => {
      let body: ChatStreamHandlerOptions['params'] & { id?: unknown };
      try {
        body = await c.req.json();
      } catch {
        return c.json({ error: 'The request body must be JSON.' }, 400);
      }
      const mastra = c.get('mastra');
      const agentId = agent ?? c.req.param('agentId');
      // Refused here, by name: a throw would reach the browser as the server's anonymous 500.
      if (!agentId || !knows(mastra, agentId)) return c.json({ error: `Agent ${agentId ?? ''} not found.` }, 404);
      if (!Array.isArray(body?.messages)) return c.json({ error: 'The body must carry the messages of useChat.' }, 400);
      const trusted: RequestContext | undefined = c.get('requestContext') ?? defaultOptions?.requestContext;

      // A throw here is the server's to answer, as for any route; no stream was opened.
      const run = await publishChat({
        ...chat,
        mastra,
        agentId,
        params: {
          ...body,
          // The server chooses the run id; a body names one only to resume that run.
          runId: body.resumeData ? body.runId : undefined,
          requestContext: trusted ?? body.requestContext,
          // Not the request's signal: the request ends with the ticket, the run must not.
          abortSignal: undefined,
        },
      });

      const { streamId, runId } = run.ticket;
      aborts.set(streamId, run.abort);
      const key = reconnect && typeof body.id === 'string' ? chatKey(agentId, body.id, trusted) : undefined;
      const cache = mastra.serverCache;
      if (key) {
        const active: ActiveRun = { streamId, ...(runId === undefined ? {} : { runId }) };
        await cache.set(key, active, ACTIVE_RUN_TTL_MS).catch(() => undefined);
      }
      void run.done.then(async () => {
        aborts.delete(streamId);
        if (!key) return;
        try {
          // A newer run of the same chat may have taken the entry over.
          if (((await cache.get(key)) as ActiveRun | undefined)?.streamId === streamId) await cache.delete(key);
        } catch {
          // The entry expires by itself.
        }
      });
      return c.json(run.ticket);
    },
  });

  const abort = registerApiRoute(`${base}/abort`, {
    method: 'POST',
    handler: async (c) => {
      const body = (await c.req.json().catch(() => null)) as { streamId?: unknown } | null;
      const cancel = typeof body?.streamId === 'string' ? aborts.get(body.streamId) : undefined;
      if (!cancel) return c.json({ error: 'No run in this process publishes that stream.' }, 404);
      cancel();
      return c.body(null, 204);
    },
  });

  if (!reconnect) return [start, abort];

  const resume = registerApiRoute(`${base}/:chatId/stream`, {
    method: 'GET',
    handler: async (c) => {
      const agentId = agent ?? c.req.param('agentId');
      const chatId = c.req.param('chatId');
      const trusted: RequestContext | undefined = c.get('requestContext') ?? defaultOptions?.requestContext;
      let active: ActiveRun | undefined;
      if (agentId && chatId) {
        active = (await c.get('mastra').serverCache.get(chatKey(agentId, chatId, trusted)).catch(() => undefined)) as ActiveRun | undefined;
      }
      if (!active) return c.body(null, 204);
      const ticket: IQTicket = { ...(await iq.ticket(active.streamId, subTokenTtl)), ...(active.runId === undefined ? {} : { runId: active.runId }) };
      return c.json(ticket);
    },
  });

  return [start, abort, resume];
}
