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
from ai.tools.utils.utils import get_refined_prompt, get_web_search

def web_search(state: JobState) -> JobState:
    refined_prompt = get_refined_prompt(state)
    web_search_results = get_web_search(refined_prompt, state['tavily_client'])
    return {'web_search_results': web_search_results, 'completed_tools': ['WebSearch']}