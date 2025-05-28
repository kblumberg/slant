from typing import List, Dict, Any
from datetime import datetime

class UserMessage:
    def __init__(self, id: str, timestamp: datetime, message: str, conversation_id: str):
        self.id = id
        self.timestamp = timestamp
        self.message = message
        self.conversation_id = conversation_id

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'timestamp': self.timestamp,
            'message': self.message,
            'conversation_id': self.conversation_id
        }

    @classmethod
    def from_conversation(cls, user_message: Dict[str, Any]) -> "UserMessage":
        return cls(
            id=str(user_message['id']),
            timestamp=user_message['timestamp'],
            message=user_message['message'],
            conversation_id=user_message['conversation_id']
        )

    def to_string(self) -> str:
        return f"""
        ID: {self.id}
        Timestamp: {self.timestamp}
        Message: {self.message}
        Conversation ID: {self.conversation_id}
        """

    def __str__(self):
        return self.to_string()
