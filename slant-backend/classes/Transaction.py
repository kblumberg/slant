import json
from typing import Dict, Any

class Transaction:
    def __init__(self, id: str, context: str, data: dict):
        self.id = id
        self.context = context
        self.data = data

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'context': self.context,
            'data': self.data
        }

    @classmethod
    def from_transaction(cls, transaction: Dict[str, Any]) -> "Transaction":
        return cls(
            id=str(transaction['id']),
            context=transaction['context'],
            data=transaction['data']
        )

    def to_string(self) -> str:
        return f"""
        Transaction ID: {self.id}
        Context: {self.context}
        Data: {json.dumps(self.data, indent=4)}
        """

    def __str__(self):
        return self.to_string()
