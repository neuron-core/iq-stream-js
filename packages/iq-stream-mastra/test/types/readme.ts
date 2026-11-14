// The samples of the README, as an application would write them: this file must compile against the
// built package, resolved by name. test/unit/types.test.mjs compiles it; the other tests run the same calls.
import { MastraAgent } from '@ag-ui/mastra';
import { Agent } from '@mastra/core/agent';
import { Mastra } from '@mastra/core/mastra';
import { IQClient } from '@neuron-core/iq-stream';
import { classifyMastraChunk, iqChatRoute, pipeToIQ, publishAgUi, publishChat, publishToIQ } from '@neuron-core/iq-stream-mastra';
import type { PublishedRun, RunPublishResult } from '@neuron-core/iq-stream-mastra';

declare const weatherAgent: Agent;
declare const waitUntil: (promise: Promise<unknown>) => void;

const iq = new IQClient();   // IQ_STREAM_URL, IQ_STREAM_PUBLIC_URL, IQ_STREAM_APP_KEY, IQ_STREAM_APP_SECRET

// --- the routes on Mastra's server
export const mastra = new Mastra({
  agents: { weatherAgent },
  server: { apiRoutes: iqChatRoute({ path: '/iq/chat/:agentId', iq, version: 'v6' }) },
});

// --- without Mastra's server: a route handler of Next.js, or any other
export async function POST(request: Request): Promise<Response> {
  const run = await publishChat({ iq, mastra, agentId: 'weatherAgent', params: await request.json(), version: 'v6', waitUntil });
  return Response.json(run.ticket);
}

// --- code that calls agent.stream() itself
export async function answer(question: string): Promise<PublishedRun> {
  const output = await weatherAgent.stream(question, { memory: { thread: 'thread-1', resource: 'user-1' } });
  const run = await publishToIQ(output, { iq, version: 'v6' });   // resolves once the ticket exists
  void run.done.then((result: RunPublishResult) => {              // { outcome: 'end' | 'fail', … }, never rejects
    if (result.outcome === 'fail') console.warn('not published', result.cause, result.error);
  });
  return run;                                                     // run.ticket goes to the browser
}

// --- a workflow
export async function shout(text: string): Promise<PublishedRun> {
  const run = await mastra.getWorkflow('shout').createRun();
  return publishToIQ(run.stream({ inputData: { text } }), { iq, version: 'v6' });
}

// --- AG-UI
export async function agUi(input: Parameters<MastraAgent['run']>[0]): Promise<PublishedRun> {
  const agent = new MastraAgent({ agentId: 'weatherAgent', agent: weatherAgent, resourceId: 'user-1' });
  return publishAgUi(agent.run(input), { iq });
}

// --- Mastra's own chunks, unconverted
export async function native(question: string): Promise<RunPublishResult> {
  const output = await weatherAgent.stream(question);
  const ticket = await iq.ticket();
  return pipeToIQ(output.fullStream, iq.publisher(ticket.streamId), { classify: classifyMastraChunk });
}

// --- the options
export const routes = iqChatRoute({
  path: '/support-chat',
  agent: 'weatherAgent',
  iq,
  version: 'v7',
  sendReasoning: true,
  defaultOptions: { maxSteps: 5 },
  onError: (error) => (error instanceof Error ? error.message : 'An error occurred.'),
  subTokenTtl: 600,
  closeOnCancel: 'end',
  oversize: (chunk, bytes) => ({ type: 'data-too-large', data: { bytes } }),
  onResult: (result, ticket) => console.log(ticket.streamId, ticket.runId, result.outcome, result.framesSent),
  reconnect: false,
});
