'use client';

import { useState } from 'react';

const Chat = () => {
  const [open, setOpen] = useState(false);
  const [messages, setMessages] = useState<string[]>([]);
  const [input, setInput] = useState('');

  const sendMessage = () => {
    if (input.trim()) {
      setMessages([...messages, input]);
      setInput('');
    }
  };

  return (
    <div className="fixed bottom-6 right-6 z-50">
      {open ? (
        <div className="w-80 h-96 bg-[#1a1d24] rounded-xl shadow-lg border border-[#2a2e39] flex flex-col overflow-hidden">
          <div className="bg-[#101218] p-3 text-white font-bold text-sm flex justify-between items-center">
            <span>🧠 Ask about this news</span>
            <button onClick={() => setOpen(false)}>✖</button>
          </div>
          <div className="flex-1 p-3 overflow-y-auto text-sm text-gray-300 space-y-2">
            {messages.map((msg, i) => (
              <div key={i} className="bg-[#2a2e39] p-2 rounded-lg">{msg}</div>
            ))}
          </div>
          <div className="p-2 border-t border-[#2a2e39] bg-[#101218] flex">
            <input
              className="flex-1 px-3 py-2 text-sm bg-[#1a1d24] text-white rounded-l-md outline-none"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && sendMessage()}
              placeholder="Ask a question..."
            />
            <button
              onClick={sendMessage}
              className="px-4 bg-[var(--primary-color)] text-white rounded-r-md"
            >
              Send
            </button>
          </div>
        </div>
      ) : (
        <button
          className="bg-[var(--primary-color)] text-white px-4 py-2 rounded-full shadow-lg"
          onClick={() => setOpen(true)}
        >
          💬 Chat
        </button>
      )}
    </div>
  );
};

export default Chat;
