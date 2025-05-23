import time
import json
from utils.utils import log
from pinecone import Pinecone
from classes.Project import Project
from utils.db import PINECONE_API_KEY
from classes.GraphState import GraphState
from langchain_openai import OpenAIEmbeddings

def rag_search_projects(state: GraphState) -> GraphState:
    """Performs a search on a RAG database of projects.
    Input: a dictionary with the following keys:
        - query: a question or topic you want to find projects about (str)
        - top_n_projects: the number of projects to return (int, default to 20, with discretion to change this based on the question)
    """
    # refined_query = prompt_refiner(state, 'Search a RAG database of projects.')
    refined_query = state['refined_query']
    start_time = time.time()
    params = {
        "query": refined_query
        , "top_n_projects": 5
    }
    print('\n')
    print('='*20)
    print('\n')
    print('rag_search_projects starting...')
    # print(f'params: {params}')
    # Ensure params is a dictionary
    if isinstance(params, str):
        try:
            params = json.loads(params)  # Convert JSON string to dict
        except json.JSONDecodeError:
            return "Invalid JSON input"

    # print(f'params["query"]: {params["query"]}')
    # print(f'params["top_n_projects"]: {params["top_n_projects"]}')

    # Initialize Pinecone
    pc = Pinecone(api_key=PINECONE_API_KEY)
    index = pc.Index("slant")
    
    # Get embeddings for query
    embeddings = OpenAIEmbeddings()
    query_embedding = embeddings.embed_query(refined_query)

    filter_conditions = {'score': {'$gt': 0}}
    
    # Search Pinecone
    results = index.query(
        vector=query_embedding,
        top_k=params["top_n_projects"],
        include_metadata=True,
        filter=filter_conditions,
        namespace='projects'
    )
    
    # Format results
    projects = []
    for match in results['matches']:
        project = match.metadata
        project['id'] = match['id']
        projects.append(Project.from_project(project))
    new_projects = list(state['projects']) + projects
    unique_projects = {project.id: project for project in new_projects}.values()  
    time_taken = round(time.time() - start_time, 1)
    log(f'rag_search_projects finished in {time_taken} seconds')
    return {'projects': unique_projects, 'completed_tools': ["RagSearchProjects"]}
