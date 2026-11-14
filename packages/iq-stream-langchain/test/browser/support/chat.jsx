// The page an application writes: useChat of @ai-sdk/react over the core's chatTransport.
import React from 'react';
import { createRoot } from 'react-dom/client';
import { useChat } from '@ai-sdk/react';
import { chatTransport } from '@neuron-core/iq-stream/ai-sdk';

const params = new URLSearchParams(location.search);
const chatId = params.get('chat');
const scenario = params.get('scenario') ?? 'ok';

const transport = chatTransport({
  start: async ({ chatId: id, messages }) =>
    (await fetch(`/chat?scenario=${scenario}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id, messages }) })).json(),
  resume: async ({ chatId: id }) => {
    const response = await fetch(`/chat/${id}/stream`);
    return response.status === 204 ? null : response.json();
  },
  stop: (ticket) => { void fetch(`/chat/stop/${ticket.streamId}`, { method: 'POST' }); },
});

function App() {
  const { messages, sendMessage, stop, status, error } = useChat({ id: chatId, transport, resume: params.has('resume') });
  return (
    <div>
      <button id="send" onClick={() => sendMessage({ text: 'What is the weather in Paris?' })}>send</button>
      <button id="stop" onClick={() => stop()}>stop</button>
      <p id="status">{status}</p>
      <p id="error">{error ? error.message : ''}</p>
      <div id="messages">{messages.map((message) => (
        <div key={message.id} data-role={message.role}>{message.parts.map((part, index) =>
          part.type === 'text' ? <span key={index} className="text">{part.text}</span>
            : part.type === 'dynamic-tool' ? <span key={index} className="tool" data-state={part.state}>{part.toolName}({JSON.stringify(part.input)}) = {JSON.stringify(part.output ?? null)}</span> : null)}</div>
      ))}</div>
    </div>
  );
}
createRoot(document.getElementById('root')).render(<App />);
