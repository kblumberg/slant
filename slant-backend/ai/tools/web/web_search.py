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
from ai.tools.utils.utils import get_refined_prompt

def web_search(state: JobState) -> JobState:
    refined_prompt = get_refined_prompt(state)
    log('\n')
    log('='*20)
    log('\n')
    log('starting web_search...')
    web_search_results = state['tavily_client'].search(refined_prompt, search_depth="advanced", include_answer=True, include_images=False, max_results=5)
    log('web_search_results')
    log(web_search_results)
    if 'answer' in web_search_results.keys():
        return {'web_search_results': web_search_results['answer'], 'completed_tools': ['WebSearch']}
    return {'completed_tools': ['WebSearch']}