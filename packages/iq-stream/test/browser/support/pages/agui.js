// A plain @ag-ui/client page: StreamAgent, imported from the package by name, run with runAgent().
import { StreamAgent } from '@neuron-core/iq-stream/ag-ui';

window.runAgent = async (scenario) => {
  const agent = new StreamAgent({
    threadId: 'thread-1',
    start: async (input) => (await fetch(`/api/agui?scenario=${scenario}`, { method: 'POST', body: JSON.stringify(input) })).json(),
  });
  agent.messages = [{ id: 'u1', role: 'user', content: 'Weather in Naples?' }];
  const events = [];
  let outcome = 'resolved';
  try {
    await agent.runAgent({}, { onEvent: ({ event }) => { events.push(event.type); } });
  } catch (error) {
    outcome = `rejected: ${error.message}`;
  }
  const messages = agent.messages.map((message) => (message.role === 'assistant'
    ? { assistant: message.content ?? null, ...(message.toolCalls ? { toolCalls: message.toolCalls.map((call) => call.function.name) } : {}) }
    : { [message.role]: message.content }));
  return { outcome, events, messages };
};
