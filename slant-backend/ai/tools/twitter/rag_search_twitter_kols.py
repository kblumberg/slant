import json
import time
from utils.utils import log
from pinecone import Pinecone
from utils.db import PINECONE_API_KEY
from classes.TwitterKol import TwitterKol
from classes.GraphState import GraphState
from langchain_openai import OpenAIEmbeddings


def rag_search_twitter_kols(state: GraphState) -> GraphState:
    """
    Performs a search on a RAG database of tweets.
    Input: a dictionary with the following keys:
        - query: a project or topic you want to find twitter kols about (str)
        - top_n_twitter_kols: the number of twitter kols to return (int, default to 25, with discretion to change this based on the question)
        - project_ids: a list of project ids to filter by (List[int], default to empty list)
    """
    # refined_query = prompt_refiner(state, 'Search a RAG database of twitter kols.')
    refined_query = state['refined_query']
    start_time = time.time()
    params = {
        "query": refined_query
        , "top_n_twitter_kols": 15
        , "project_ids": []
    }
    print('\n')
    print('='*20)
    print('\n')
    print('rag_search_twitter_kols starting...')
    # print('rag_search_twitter_kols')
    # print(f'params: {params}')
    # Ensure params is a dictionary
    if isinstance(params, str):
        try:
            params = json.loads(params)  # Convert JSON string to dict
        except json.JSONDecodeError:
            return "Invalid JSON input"

    project_ids = params["project_ids"] if 'project_ids' in params else []
    # print(f'params["query"]: {params["query"]}')
    # print(f'params["top_n_twitter_kols"]: {params["top_n_twitter_kols"]}')
    # print(f'params["project_ids"]: {project_ids}')

    # Initialize Pinecone
    pc = Pinecone(api_key=PINECONE_API_KEY)
    index = pc.Index("slant")
    
    # Get embeddings for query
    embeddings = OpenAIEmbeddings()
    query_embedding = embeddings.embed_query(refined_query)
    filter_conditions = {
        'score': {'$gt': 0}
    }

    # Conditionally include the "author_id" filter
    if len(project_ids) > 0:
        filter_conditions["project_id"] = {"$in": project_ids}

    # Search Pinecone
    results = index.query(
        vector=query_embedding
        , top_k=params["top_n_twitter_kols"]
        , include_metadata=True
        , filter=filter_conditions
        , namespace='twitter_kols'
    )
    
    # Format results
    kols = []
    for match in results['matches']:
        kol = match.metadata
        kol['id'] = match['id']
        kols.append(TwitterKol.from_twitter_kol(kol))
    new_kols = list(state['kols']) + kols
    unique_kols = {kol.id: kol for kol in new_kols}.values()
    time_taken = round(time.time() - start_time, 1)
    log(f'rag_search_twitter_kols finished in {time_taken} seconds')
    return {'kols': unique_kols, 'completed_tools': ["RagSearchTwitterKols"], 'upcoming_tools': ["RespondWithContext"]}