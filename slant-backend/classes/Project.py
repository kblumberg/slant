from typing import List, Dict, Any

class Project:
    def __init__(self, id: str, name: str, description: str, ecosystem: str, tags: List[str], score: int):
        self.id = id
        self.name = name
        self.description = description
        self.ecosystem = ecosystem
        self.tags = tags
        self.score = score

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'ecosystem': self.ecosystem,
            'tags': self.tags,
            'score': self.score
        }

    @classmethod
    def from_project(cls, project: Dict[str, Any]) -> "Project":
        return cls(
            id=str(project['id']),
            name=project['name'],
            description=project['description'],
            ecosystem=project['ecosystem'],
            tags=project['tags'],
            score=project['score']
        )

    def to_string(self) -> str:
        return f"""
        Name: {self.name}
        Description: {self.description}
        Ecosystem: {self.ecosystem}
        Tags: {', '.join(self.tags)}
        Score: {self.score}
        """

    def __str__(self):
        return self.to_string()
