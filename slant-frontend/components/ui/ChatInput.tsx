import React, { useState } from 'react';

interface ChatInputProps {
  onSendMessage: (content: string) => void;
}

const ChatInput: React.FC<ChatInputProps> = ({ onSendMessage }) => {
  const [message, setMessage] = useState('');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (message.trim()) {
      onSendMessage(message);
      setMessage('');
    }
  };

  return (
    <form onSubmit={handleSubmit} className="flex gap-2 p-4 border-t">
      <input
        type="text"
        value={message}
        onChange={(e) => setMessage(e.target.value)}
        className="flex-1 px-4 py-2 border rounded-lg focus:outline-none focus:border-[#1373eb]"
        placeholder="Type a message..."
      />
      <button
        type="submit"
        className="px-6 py-2 text-white bg-[#1373eb] rounded-lg hover:bg-[#1060c9] transition-colors"
      >
        Send
      </button>
    </form>
  );
};

export default ChatInput;