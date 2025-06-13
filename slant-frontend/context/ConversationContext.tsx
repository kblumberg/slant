// context/ConversationContext.tsx
'use client';

import { createContext, useContext, useEffect, useState, ReactNode } from 'react';

interface ConversationContextType {
  conversation_id: string;
  setConversationId: (conversation_id: string) => void;
}

const ConversationContext = createContext<ConversationContextType | undefined>(undefined);

export const ConversationProvider = ({ children }: { children: ReactNode }) => {
  const [conversation_id, setConversationId] = useState<string>('');

  useEffect(() => {
    const convo_id = crypto.randomUUID();
    setConversationId(convo_id);
  }, []);

  useEffect(() => {
    if (conversation_id) {
      console.log('Conversation ID changed:', conversation_id);
    }
  }, [conversation_id]);

  return (
    <ConversationContext.Provider value={{ conversation_id, setConversationId }}>
      {children}
    </ConversationContext.Provider>
  );
};

export const useConversation = () => {
  const context = useContext(ConversationContext);
  if (!context) throw new Error('useConversation must be used within a ConversationProvider');
  return context;
};
