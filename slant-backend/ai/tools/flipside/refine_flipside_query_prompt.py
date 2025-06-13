import re
import time
import json
import pandas as pd
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from ai.tools.utils.utils import read_schemas, log_llm_call
from db.flipside.rag_search_queries import rag_search_queries

def refine_flipside_query_prompt(state: JobState) -> JobState:
    if state['question_type'] == 'other':
        return {'completed_tools': ["RefineFlipsideQueryPrompt"]}
    tokens = list(set([token for x in state['analyses'] for token in x.tokens]))
    projects = list(set([ x.project for x in state['analyses']]))
    example_queries='\n\n'.join(state['flipside_example_queries'].text.tolist())
    analysis_description=state['analysis_description']
    tweets_summary=state['tweets_summary']
    web_search_summary=state['web_search_summary']
    prompt = f"""
        You are a highly skilled data assistant helping to generate a refined search query for use in a Retrieval-Augmented Generation (RAG) system.

        Your task is to synthesize the most relevant information based on the user's analysis goal, a list of tokens and projects, tweets, web search results, and example SQL queries. Your output will be a *single refined search query string* that will guide the RAG system toward the most useful knowledge for this task.

        ---

        **USER ANALYSIS GOAL**:
        {analysis_description}

        **POTENTIALLY RELEVANT TOKENS**:
        {tokens}

        **POTENTIALLY RELEVANT PROJECTS**:
        {projects}

        **TWEET SUMMARY**:
        {tweets_summary}

        **WEB SEARCH SUMMARY**:
        {web_search_summary}

        **EXAMPLE SQL QUERIES**:
        {example_queries}

        ---

        **Instructions**:
        - Identify and extract only the most relevant details related to the user's analysis goal.
        - Pay special attention to:
            - Program names (use full, exact names)
            - Token symbols and full names
            - Program IDs
            - Token or wallet addresses
            - Project names
        - Not all example SQL queries will be relevant—use the *query description*, *summary*, and *query text* to judge relevance.
        - You should combine relevant terms and insights from all sources above into a concise, intelligent search string for a RAG database.
        - ONLY include information that is directly relevant to the user's analysis goal
        - Do NOT include any explanatory text or formatting—just return the search query as a plain string.
        - Do not include any time periods or dates
        - Try to keep the query as short as possible, targeting 10 or fewer words

        Respond with the refined search query only.
    """
    refine_flipside_query_prompt = log_llm_call(prompt, state['llm'], state['user_message_id'], 'RefineFlipsideQueryPrompt')
    log(f"refine_flipside_query_prompt: {refine_flipside_query_prompt}")
    queries = rag_search_queries(refine_flipside_query_prompt, tokens + projects, top_k=40, n_queries=10)
    return {'flipside_example_queries': queries, 'completed_tools': ['RefineFlipsideQueryPrompt']}
