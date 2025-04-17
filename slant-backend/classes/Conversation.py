from typing import List, Dict, Any
from datetime import datetime

class Conversation:
    def __init__(self, id: str, user_id: str, created_at: datetime, updated_at: datetime, title: str):
        self.id = id
        self.user_id = user_id
        self.created_at = created_at
        self.updated_at = updated_at
        self.title = title

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'user_id': self.user_id,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'title': self.title
        }

    @classmethod
    def from_conversation(cls, conversation: Dict[str, Any]) -> "Conversation":
        return cls(
            id=str(conversation['id']),
            user_id=str(conversation['user_id']),
            created_at=conversation['created_at'],
            updated_at=conversation['updated_at'],
            title=conversation['title']
        )

    def to_string(self) -> str:
        return f"""
        Title: {self.title}
        User ID: {self.user_id}
        Created At: {self.created_at}
        Updated At: {self.updated_at}
        """

    def __str__(self):
        return self.to_string()
