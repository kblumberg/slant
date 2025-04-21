import json
import time
from utils.utils import log
from classes.JobState import JobState
from langchain.schema import SystemMessage, HumanMessage
from ai.tools.utils.utils import get_web_search, rag_search_tweets

def tool_executor(state: JobState):
    additional_contexts = []
    for i in range(len(state['follow_up_questions'])):
        question = state['follow_up_questions'][i]
        tools = state['run_tools'][i]
        additional_context = ''
        if "WebSearch" in tools:
            web_search_results = get_web_search(question, state['tavily_client'])
            additional_context += f"WebSearch: {web_search_results}\n"
        if "RagSearchTweets" in tools:
            tweets = rag_search_tweets(question)
            additional_context += f"Tweets: {tweets}\n"
        if "RagSearchQueries" in tools:
            tweets = rag_search_tweets(question)
            additional_context += f"Tweets: {tweets}\n"
        if "ExecuteFlipsideQuery" in tools:
            pass
        additional_contexts.append(additional_context)
    tried_tools = state['tried_tools'] + 1
    return {'additional_contexts': additional_contexts, 'completed_tools': ['ToolExecutor'], 'tried_tools': tried_tools}