import time
import json
from utils.utils import log
from pinecone import Pinecone
from classes.Project import Project
from utils.db import PINECONE_API_KEY
from classes.JobState import JobState
from langchain_openai import OpenAIEmbeddings
from ai.tools.utils.utils import get_refined_prompt

def rag_search_projects(state: JobState) -> JobState:
    """Performs a search on a RAG database of projects.
    Input: a dictionary with the following keys:
        - query: a question or topic you want to find projects about (str)
        - top_n_projects: the number of projects to return (int, default to 20, with discretion to change this based on the question)
    """
    refined_prompt = get_refined_prompt(state)
    start_time = time.time()
    params = {
        "query": refined_prompt
        , "top_n_projects": 5
    }
    # log('\n')
    # log('='*20)
    # log('\n')
    log('rag_search_projects starting...')
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
    query_embedding = embeddings.embed_query(refined_prompt)

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
    # log(f'rag_search_projects finished in {time_taken} seconds')
    return {'projects': unique_projects, 'completed_tools': ['RagSearchProjects']}
