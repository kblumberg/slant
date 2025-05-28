from typing import List, Dict, Any
from datetime import datetime

class AgentMessage:
    def __init__(self, id: str, timestamp: datetime, message: str, conversation_id: str, user_message_id: str):
        self.id = id
        self.timestamp = timestamp
        self.message = message
        self.conversation_id = conversation_id
        self.user_message_id = user_message_id

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'timestamp': self.timestamp,
            'message': self.message,
            'conversation_id': self.conversation_id,
            'user_message_id': self.user_message_id
        }

    @classmethod
    def from_conversation(cls, agent_message: Dict[str, Any]) -> "AgentMessage":
        return cls(
            id=str(agent_message['id']),
            timestamp=agent_message['timestamp'],
            message=agent_message['message'],
            conversation_id=agent_message['conversation_id'],
            user_message_id=agent_message['user_message_id']
        )

    def to_string(self) -> str:
        return f"""
        ID: {self.id}
        Timestamp: {self.timestamp}
        Message: {self.message}
        Conversation ID: {self.conversation_id}
        User Message ID: {self.user_message_id}
        """

    def __str__(self):
        return self.to_string()
