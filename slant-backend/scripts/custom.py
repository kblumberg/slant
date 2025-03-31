import json
import time
from typing import List
from classes.Tweet import Tweet
from pinecone import Pinecone
from langchain_openai import OpenAIEmbeddings
from classes.TweetSearchParams import TweetSearchParams
from utils.db import PINECONE_API_KEY, pg_load_data
from classes.GraphState import GraphState
from ai.tools.utils.prompt_refiner import prompt_refiner
from utils.utils import log

# Initialize Pinecone
pc = Pinecone(api_key=PINECONE_API_KEY)
index = pc.Index("slant", namespace="tweets")

# Get embeddings for query
embeddings = OpenAIEmbeddings()
query_embedding = embeddings.embed_query(
    # "Find tweets where users are requesting or searching for people to analyze crypto data. Look for phrases like 'analysts', 'buildooors', 'data', 'api' 'flipsidecrypto', 'dune' in combination with words like 'Anyone have', 'Where can I find', 'I wish there was', or 'Looking for...'"
    "Find tweets where there is a possible application for a data scientist to analyze crypto data"
)
query_text = """
Find tweets where users are asking for data, analytics, or insights in crypto. 
Focus on phrases like:
- "Looking for data on..."
- "Does anyone have stats on..."
- "Need an API for..."
- "Where can I find data for..."
- "Wish I had numbers on..."
- "Anyone tracking..."
- "I need a dashboard that shows..."
- "Trying to analyze..."

Prioritize tweets mentioning:
- Staking data
- Wallet activity
- Trading volume
- DeFi metrics
- NFTs
- Airdrops
- On-chain trends
"""

query_embedding = embeddings.embed_query(query_text)

filter_conditions = {}


# Search Pinecone
results = index.query(
    vector=query_embedding
    , top_k=200
    , include_metadata=True
    , filter=filter_conditions
    , namespace="tweets"
)

# log('results')
# log(results)

# Format results
tweets = []
for match in results['matches']:
    tweet = match.metadata
    tweet['id'] = match['id']
    tweets.append(Tweet.from_tweet(tweet))

tweets_df = pd.DataFrame([str(x).strip() for x in tweets], columns=['tweet'])
tweets_df.to_csv('~/Downloads/tweets-1.csv', index=False)
