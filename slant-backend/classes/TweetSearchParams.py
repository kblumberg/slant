from typing import TypedDict, List

class TweetSearchParams(TypedDict):
    query: str
    top_n_tweets: int
    author_ids: List[int]
    start_time: int
    end_time: int