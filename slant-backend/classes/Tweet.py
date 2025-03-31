from typing import Dict, Any
from datetime import datetime

class Tweet:
    def __init__(self, id: str, text: str, author_id: int, created_at: int, retweet_count: int, impression_count: int, tweet_url: str):
        self.id = id
        self.text = text
        self.author_id = author_id
        self.created_at = created_at
        self.retweet_count = retweet_count
        self.impression_count = impression_count
        self.tweet_url = tweet_url

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'text': self.text,
            'author_id': self.author_id,
            'created_at': self.created_at,
            'retweet_count': self.retweet_count,
            'impression_count': self.impression_count,
            'tweet_url': self.tweet_url
        }

    @classmethod
    def from_tweet(cls, tweet: Dict[str, Any]) -> "Tweet":
        return cls(
            id=str(tweet['id']),
            text=tweet['text'],
            author_id=tweet['author_id'],
            created_at=tweet['created_at'],
            retweet_count=tweet['retweet_count'],
            impression_count=tweet['impression_count'],
            tweet_url=tweet['tweet_url'],
        )

    def to_string(self) -> str:
        return f"""
        Tweet URL: {self.tweet_url}
        Date: {datetime.fromtimestamp(self.created_at).strftime('%Y-%m-%d %H:%M')}
        Text: {self.text}
        """

    def __str__(self):
        return self.to_string()