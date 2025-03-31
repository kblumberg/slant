import React from 'react';
import Message from '../../types/Message';

const ChatMessage: React.FC<{ message: Message }> = ({ message }) => {
  const isUser = message.sender === 'user';
  
  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'} mb-4`}>
      <div className={`max-w-[70%] bg-gray-50 rounded-lg px-4 py-2 ${
        isUser ? 'text-white' : 'bg-gray-100'
      }`}>
        <p className="text-sm">{message.content}</p>
        <span className="text-xs opacity-70">
          {new Date(message.timestamp).toLocaleTimeString()}
        </span>
      </div>
    </div>
  );
};

export default ChatMessage;