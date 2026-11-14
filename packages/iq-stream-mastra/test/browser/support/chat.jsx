// The page an application writes: useChat of @ai-sdk/react over the core's chatTransport, against the
// three routes of iqChatRoute. Nothing here is specific to the test but the query string.
import React from 'react';
import { createRoot } from 'react-dom/client';
import { useChat } from '@ai-sdk/react';
import { lastAssistantMessageIsCompleteWithApprovalResponses } from 'ai';
import { chatTransport } from '@neuron-core/iq-stream/ai-sdk';

const params = new URLSearchParams(location.search);
const chatId = params.get('chat') ?? 'chat-1';
const route = `/iq/${params.get('route') ?? 'chat'}/${params.get('agent') ?? 'weather'}`;
const json = { 'Content-Type': 'application/json' };

const transport = chatTransport({
  start: async ({ chatId: id, messages, trigger, messageId }) => {
    const response = await fetch(route, { method: 'POST', headers: json, body: JSON.stringify({ id, messages, trigger, messageId }) });
    if (!response.ok) throw new Error((await response.json()).error);
    return response.json();
  },
  resume: async ({ chatId: id }) => {
    const response = await fetch(`${route}/${id}/stream`);
    return response.status === 204 ? null : response.json();
  },
  stop: (ticket) => { void fetch(`${route}/abort`, { method: 'POST', headers: json, body: JSON.stringify({ streamId: ticket.streamId }) }); },
});

function App() {
  const [data, setData] = React.useState([]);
  const { messages, sendMessage, stop, status, error, addToolApprovalResponse } = useChat({
    id: chatId, transport, resume: params.has('resume'),
    // Transient parts come here and nowhere else: the placeholder of a chunk that was too large is one.
    onData: (part) => setData((seen) => [...seen, `${part.type}:${part.data?.type}`]),
    // An answered approval card sends the messages again: the second leg.
    sendAutomaticallyWhen: lastAssistantMessageIsCompleteWithApprovalResponses,
  });
  return (
    <div>
      <button id="send" onClick={() => sendMessage({ text: 'What is the weather in Paris?' })}>send</button>
      <button id="stop" onClick={() => stop()}>stop</button>
      <p id="status">{status}</p>
      <p id="error">{error ? error.message : ''}</p>
      <p id="data">{data.join(' ')}</p>
      <div id="messages">{messages.map((message) => (
        <div key={message.id} data-role={message.role}>{message.parts.map((part, index) => {
          if (part.type === 'text') return <span key={index} className="text">{part.text}</span>;
          if (!part.type.startsWith('tool-')) return null;
          return (
            <span key={index} className="tool" data-state={part.state}>
              {part.type}
              {part.state === 'approval-requested' && <button id="approve" onClick={() => addToolApprovalResponse({ id: part.approval.id, approved: true })}>approve</button>}
            </span>
          );
        })}</div>
      ))}</div>
    </div>
  );
}
createRoot(document.getElementById('root')).render(<App />);
