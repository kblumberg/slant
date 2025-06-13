'use client';
import ChatInterface from "@/components/ChatInterface";
import { useConversation } from "@/context/ConversationContext";
import { useWallet } from "@solana/wallet-adapter-react";

export default function ChatPage() {
    console.log(`Conversation ID: ${useConversation().conversation_id}`);
    const walletAddress = useWallet().publicKey?.toBase58() || "unknown";
    return (
      <div className="p-12 text-white">
        <ChatInterface userId={walletAddress} conversationId={useConversation().conversation_id} />
      </div>
    );
  }
  