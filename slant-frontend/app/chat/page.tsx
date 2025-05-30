'use client';
import ChatInterface from "@/components/ChatInterface";
import { useConversation } from "@/context/ConversationContext";

export default function ChatPage() {
    console.log(`Conversation ID: ${useConversation().conversation_id}`);
    return (
      <div className="p-12 text-white">
        <ChatInterface userId="demo_user" conversationId={useConversation().conversation_id} />
      </div>
    );
  }
  