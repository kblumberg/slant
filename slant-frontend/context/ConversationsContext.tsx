// context/ConversationContext.tsx
'use client';

import { createContext, useContext, useEffect, useState, ReactNode } from 'react';
import { Conversation } from '@/types/Conversation';
import { generateUrl } from '@/utils/utils';
import { useWallet } from '@solana/wallet-adapter-react';


interface ConversationsContextType {
  conversations: Conversation[];
  setConversations: (conversations: Conversation[]) => void;
}

const ConversationsContext = createContext<ConversationsContextType | undefined>(undefined);

export const ConversationsProvider = ({ children }: { children: ReactNode }) => {
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const walletAddress = useWallet().publicKey?.toBase58() || "unknown";

  const fetchConversations = async () => {
    const url = generateUrl('/api/load-conversations');
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        user_id: walletAddress
      })
    });
    // const response = await fetch(`${url}?${params.toString()}`);
    const data = await response.json();
    console.log(`fetchConversations`)
    console.log(data)
    setConversations(data.conversations);
  };

  useEffect(() => {
    fetchConversations();
  }, [walletAddress]);

  return (
    <ConversationsContext.Provider value={{ conversations, setConversations }}>
      {children}
    </ConversationsContext.Provider>
  );
};

export const useConversations = () => {
  const context = useContext(ConversationsContext);
  if (!context) throw new Error('useConversations must be used within a ConversationsProvider');
  return context;
};
