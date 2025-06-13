// components/ConversationsPanel.tsx
'use client';
import { useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useConversation } from '@/context/ConversationContext';
import { useConversations } from '@/context/ConversationsContext';

const ConversationsPanel = () => {
  const { conversations } = useConversations();

  const router = useRouter();
  const { setConversationId } = useConversation();

  useEffect(() => {
  }, []);

  const handleConversationClick = (id: string) => {
    setConversationId(id);
    router.push('/chat');
  };

  return (
    <div className="w-full overflow-x-auto py-4 px-6 bg-[#0e0f12] z-10">
      <div className="flex space-x-4 min-w-max">
        {conversations.map((conversation) => (
          <div
            key={conversation.id}
            className="bg-[#1E1E1E] rounded-lg p-4 cursor-pointer hover:bg-[#2D2D2D] transition-colors w-[300px]"
            onClick={() => handleConversationClick(conversation.id)}
          >
            <h3 className="p-0 m-0 text-white font-medium">{conversation.title}</h3>
            {/* <div> */}
            <div className="flex items-center justify-between">
              <span className="text-gray-400 text-sm">
                {(() => {
                  const minutes = Math.floor((Date.now() - new Date(conversation.created_at).getTime()) / (1000 * 60));
                  const hours = Math.floor(minutes / 60);
                  const days = Math.floor(hours / 24);
                  if (minutes < 60) return `${minutes} m`;
                  if (hours < 24) return `${hours} h`;
                  if (hours === 24) return '1 d';
                  return `${days} d`;
                })()}{' ago'}
                {/* {new Date(conversation.created_at).toLocaleDateString()} */}
              </span>
              </div>
              <div className="flex items-center justify-between">
              {conversation.id && (
                <div className="relative w-60 h-40">
                    <img
                    src={`https://slant-graphs.s3.us-west-2.amazonaws.com/chart-${conversation.id}.png`}
                    alt="Conversation chart"
                    className="object-cover rounded"
                    onError={(e) => {
                        const target = e.target as HTMLImageElement;
                        target.onerror = null;
                        target.src = 'data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="40" height="40"><text x="0" y="20" font-size="20">📊</text></svg>';
                    }}
                    />

                </div>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default ConversationsPanel;
