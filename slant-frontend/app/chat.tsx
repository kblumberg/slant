"use client";

import { useState } from "react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { MessageSquare } from "lucide-react";

export default function ChatApp() {
  const [messages, setMessages] = useState<string[]>([]);
  const [input, setInput] = useState("");

  const sendMessage = () => {
    if (!input.trim()) return;
    setMessages([...messages, input]);
    setInput("");
  };

  return (
    <div className="flex items-center justify-center min-h-screen bg-gray-100 p-4">
      <Card className="w-full max-w-md shadow-lg rounded-2xl">
        <CardContent className="p-6 space-y-4">
          {messages.length === 0 ? (
            <div className="text-center">
              <h1 className="text-2xl font-bold text-[#0E4BA3]">Ask for alpha</h1>
              <Input
                value={input}
                onChange={(e) => setInput(e.target.value)}
                placeholder="Type your question..."
                className="mt-4 w-full"
              />
              <Button onClick={sendMessage} className="mt-2 w-full text-white">
                Ask
              </Button>
            </div>
          ) : (
            <div className="space-y-4">
              <div className="max-h-96 overflow-y-auto space-y-2 p-2 border rounded-md bg-white">
                {messages.map((msg, index) => (
                  <div key={index} className="p-2 bg-gray-200 rounded-md">
                    {msg}
                  </div>
                ))}
              </div>
              <div className="flex items-center space-x-2">
                <Input
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  placeholder="Type a message..."
                  className="flex-1"
                />
                <Button onClick={sendMessage} className="text-white">
                  <MessageSquare size={20} />
                </Button>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
