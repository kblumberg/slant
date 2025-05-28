import json
import time
from utils.utils import log
from pinecone import Pinecone
from classes.Tweet import Tweet
from classes.GraphState import GraphState
from langchain_openai import OpenAIEmbeddings
from utils.db import PINECONE_API_KEY, pg_load_data
from classes.TweetSearchParams import TweetSearchParams
from classes.JobState import JobState

def rag_search_tweets(state: JobState) -> JobState:
    promp
    refined_query = state['user_prompt']
    start_time = time.time()
    params = {
        "query": refined_query
        , "top_n_tweets": 20
        , "author_ids": []
        , "start_time": 0
        , "end_time": 0
    }
    # log('\n')
    # log('='*20)
    # log('\n')
    # log('starting rag_search_tweets...')
    # log(f'params: {params}')
    # Ensure params is a dictionary
    if isinstance(params, str):
        try:
            params = json.loads(params)  # Convert JSON string to dict
        except json.JSONDecodeError:
            return "Invalid JSON input"
    author_ids = params["author_ids"] if 'author_ids' in params else []
    gte = params["start_time"] if 'start_time' in params else 0
    end_time = params["end_time"] if 'end_time' in params and params["end_time"] > 0 else int(time.time())
    # refined_query = prompt_refiner(state, 'Search a RAG database of tweets.')
    # refined_query = params["query"]

    # log(f'params["query"]: {params["query"]}')
    # log(f'refined_query: {refined_query}')
    # log(f'params["top_n_tweets"]: {params["top_n_tweets"]}')
    # log(f'params["author_ids"]: {author_ids}')
    # log(f'params["start_time"]: {start_time}')
    # log(f'params["end_time"]: {end_time}')

    # Initialize Pinecone
    pc = Pinecone(api_key=PINECONE_API_KEY)
    index = pc.Index("slant", namespace="tweets")
    
    # Get embeddings for query
    embeddings = OpenAIEmbeddings()
    query_embedding = embeddings.embed_query(refined_query)
    filter_conditions = {
        "created_at": {
            "$gte": gte,
            "$lte": end_time
        }
    }

    # Conditionally include the "author_id" filter
    if len(author_ids) > 0:
        filter_conditions["author_id"] = {"$in": author_ids}

    # Search Pinecone
    results = index.query(
        vector=query_embedding
        , top_k=params["top_n_tweets"]
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
    # log('tweets')
    # log(tweets)
    new_tweets = list(state['tweets']) + tweets
    unique_tweets = {tweet.id: tweet for tweet in new_tweets}.values()  
    time_taken = round(time.time() - start_time, 1)
    # log(f'rag_search_tweets finished in {time_taken} seconds')
    return {'tweets': unique_tweets, 'completed_tools': ["RagSearchTweets"], 'upcoming_tools': ["RespondWithContext"]}


def get_tweets_by_project_ids_and_start_time(params: TweetSearchParams) -> str:
    """
    Gets tweets from projects.
    Input: a dictionary with the following keys:
        - project_ids: a list of project ids (List[int])
        - start_time: a unix timestamp in seconds (int)
        - top_n_tweets: the number of tweets to return for each project (int, default to 50 unless there is a specific reason to change this)
    """
    # log('\n')
    # log('='*20)
    # log('\n')
    # log('get_tweets_by_project_ids_and_start_time')
    # log(f'params: {params}')
    # Ensure params is a dictionary
    if isinstance(params, str):
        try:
            params = json.loads(params)  # Convert JSON string to dict
        except json.JSONDecodeError:
            return "Invalid JSON input"

    # log(f'params["project_ids"]: {params["project_ids"]}')
    # log(f'params["start_time"]: {params["start_time"]}')
    # log(f'params["top_n_tweets"]: {params["top_n_tweets"]}')
    """Performs a search on a postgres database of projects."""
    
    # Parse the query and construct SQL
    query = f"""
        with t0 as (
            SELECT t.id
            , p.id as project_id
            , t.text
            , t.impressions
            , to_char(to_timestamp(t.created_at), 'YYYY-MM-DD HH24:MI') as created_at_formatted
            , tk.username
            , row_number() over (partition by t.id order by t.impressions desc, t.created_at desc) as rn
            FROM tweets t
            JOIN twitter_kols tk
                ON t.author_id = tk.id
            JOIN projects p
                ON tk.associated_project_id = p.id
            WHERE p.id IN ({', '.join(map(str, params['project_ids']))})
            AND t.created_at >= {params['start_time']}
        )
        , t1 as (
            select id
            , text
            , created_at_formatted
            , username
            , row_number() over (partition by project_id order by impressions desc) as rn
            from t0
        )
        select *
        from t1
        where rn <= {params['top_n_tweets']}
    """
    
    # Execute query and get results
    try:
        results_df = pg_load_data(query)
        # log('results_df')
        # log(results_df)
        if len(results_df) == 0:
            return "No matching projects found"
            
        # Format results as string
        results = []
        for _, row in results_df.iterrows():
            tweet = f"Author: {row['username']}\n"
            tweet += f"Created at: {row['created_at_formatted']}\n"
            tweet += f"Tweet: {row['text']}\n"
            results.append(tweet)
            
        return "\n\n".join(results)
        
    except Exception as e:
        return f"Error querying projects: {str(e)}"