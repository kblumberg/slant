from typing import Dict, Any

class TwitterKol:
    def __init__(self, id: str, name: str, description: str, associated_project_id: int, username: str, score: int):
        self.id = id
        self.name = name
        self.description = description
        self.associated_project_id = associated_project_id
        self.username = username
        self.score = score

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'associated_project_id': self.associated_project_id,
            'username': self.username,
            'score': self.score
        }

    @classmethod
    def from_twitter_kol(cls, twitter_kol: Dict[str, Any]) -> "TwitterKol":
        return cls(
            id=str(twitter_kol['id']),
            name=twitter_kol['name'],
            description=twitter_kol['description'],
            associated_project_id=twitter_kol['associated_project_id'],
            username=twitter_kol['username'],
            score=twitter_kol['score']
        )

    def to_string(self) -> str:
        return f"""
        Name: {self.name}
        Description: {self.description}
        Username: {self.username}
        Score: {self.score}
        """

    def __str__(self):
        return self.to_string()
