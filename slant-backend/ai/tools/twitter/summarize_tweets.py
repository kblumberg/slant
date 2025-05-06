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

def summarize_tweets(state: JobState) -> JobState:
    prompt = """
    You are an expert crypto analyst and Twitter summarizer.

    TASK:
    Given a user prompt describing a crypto-related analysis goal and a series of tweets, extract and summarize only the most relevant and insightful information from the tweets. Focus specifically on:
    - Dates, timeframes, or events
    - Mentioned protocols, projects, chains, and tokens
    - Any specific program ids or addresses
    - How a protocol operates, what it does, and its goals
    - User sentiment or reactions (if meaningful)
    - Any emerging patterns, warnings, or alpha

    Be concise but precise. Avoid generalities. ONLY include tweet content that clearly relates to the user's specific analysis goal.

    CONTEXT: This summary will be used by a data analyst AI agent to guide further on-chain or off-chain analysis.

    USER PROMPT:
    {analysis_description}

    TWEETS:
    {tweets}

    OUTPUT FORMAT:
    Return only a plain-text summary (no bullets, no markdown), written in 2–5 sentences, focused strictly on tweet content that supports the user's analysis objective.
    """

    formatted_prompt = prompt.format(
        analysis_description=state['analysis_description'],
        tweets='\n'.join([str(x) for x in state['tweets']])
    )
    tweets_summary = state['llm'].invoke(formatted_prompt).content
    log(f'tweets_summary:\n{tweets_summary}')
    return {'tweets_summary': tweets_summary, 'completed_tools': ['SummarizeTweets']}