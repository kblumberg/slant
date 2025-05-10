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
from ai.tools.utils.prompt_refiner import twitter_prompt_refiner
from ai.tools.utils.utils import get_refined_prompt

def summarize_web_search(state: JobState) -> JobState:
    prompt = """
    You are an expert crypto analyst and web search summarizer.

    TASK:
    Given a user prompt describing a crypto-related analysis goal and a series of web search results, extract and summarize only the most relevant and insightful information from the web search results. Focus specifically on:
    - Dates, timeframes, or events
    - Mentioned protocols, projects, chains, and tokens
    - Any specific program ids or addresses
    - How a protocol operates, what it does, and its goals
    - User sentiment or reactions (if meaningful)
    - Any emerging patterns, warnings, or alpha

    Be concise but precise. Avoid generalities. Only include web search content that clearly relates to the user's analysis goal.

    CONTEXT: This summary will be used by a data analyst AI agent to guide further on-chain or off-chain analysis.

    USER PROMPT:
    {analysis_description}

    WEB SEARCH RESULTS:
    {web_search_results}

    OUTPUT FORMAT:
    Return only a plain-text summary (no bullets, no markdown), written in 2–5 sentences, focused strictly on web search content that supports the user's analysis objective.
    """

    formatted_prompt = prompt.format(
        analysis_description=state['analysis_description'],
        web_search_results='\n'.join([str(x)[:15000] for x in state['web_search_results']])[:35000]
    )
    web_search_summary = state['llm'].invoke(formatted_prompt).content
    log(f'web_search_summary:\n{web_search_summary}')
    return {'web_search_summary': web_search_summary, 'completed_tools': ['SummarizeWebSearch']}