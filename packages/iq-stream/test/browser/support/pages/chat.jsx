// The page an application writes: useChat of @ai-sdk/react over chatTransport, imported from the package
// by name, so that the bundle is made from the built files through the export map.
import React from 'react';
import { createRoot } from 'react-dom/client';
import { useChat } from '@ai-sdk/react';
import { chatTransport } from '@neuron-core/iq-stream/ai-sdk';

const params = new URLSearchParams(location.search);
const chatId = params.get('chat') ?? 'chat-1';
const run = new URLSearchParams({ scenario: params.get('scenario') ?? 'ok', pace: params.get('pace') ?? '60', server: params.get('server') ?? 'default' });

const transport = chatTransport({
  start: async ({ chatId: id, messages, trigger, messageId }) =>
    (await fetch(`/api/chat?${run}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id, messages, trigger, messageId }) })).json(),
  resume: async ({ chatId: id }) => {
    const response = await fetch(`/api/chat/${id}/stream?${run}`);
    return response.status === 204 ? null : response.json();
  },
  stop: (ticket) => { void fetch(`/api/chat/stop?stream=${ticket.streamId}`, { method: 'POST' }); },
});

function App() {
  const { messages, sendMessage, stop, status, error } = useChat({ id: chatId, transport, resume: params.has('resume') });
  return (
    <div>
      <button id="send" onClick={() => sendMessage({ text: 'Weather in Naples?' })}>send</button>
      <button id="stop" onClick={() => stop()}>stop</button>
      <p id="status">{status}</p>
      <p id="error" data-kind={error?.kind ?? ''}>{error ? error.message : ''}</p>
      <div id="messages">{messages.map((message) => (
        <div key={message.id} data-role={message.role}>{message.parts.map((part, index) =>
          part.type === 'text' ? <span key={index} className="text">{part.text}</span>
            : part.type.startsWith('tool-') || part.type === 'dynamic-tool' ? <span key={index} className="tool" data-state={part.state}>{part.type}</span> : null)}</div>
      ))}</div>
    </div>
  );
}
createRoot(document.getElementById('root')).render(<App />);
