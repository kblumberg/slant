import json
import time
from utils.utils import log
from classes.JobState import JobState
from langchain.schema import SystemMessage, HumanMessage
from ai.tools.utils.utils import get_web_search, rag_search_tweets, log_llm_call

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
    additional_contexts_text = '\n'.join(additional_contexts)
    additional_context_summary = ''
    if len(additional_contexts):
        prompt = """
        You are an expert crypto analyst and summarizer.

        TASK:
        Given a user prompt describing a crypto-related analysis goal, some potential clarifying questions, and a series of additional context, extract and summarize only the most relevant and insightful information from the additional context. Focus specifically on:
        - Dates, timeframes, or events
        - Mentioned protocols, projects, chains, and tokens
        - Any specific program ids or addresses
        - How a protocol operates, what it does, and its goals
        - User sentiment or reactions (if meaningful)
        - Any emerging patterns, warnings, or alpha

        Be concise but precise. Avoid generalities. ONLY include additional context that clearly relates to the user's specific analysis goal.

        CONTEXT: This summary will be used by a data analyst AI agent to guide further on-chain or off-chain analysis.

        USER PROMPT:
        {analysis_description}

        POTENTIAL CLARIFYING QUESTIONS:
        {follow_up_questions}

        ADDITIONAL CONTEXT:
        {additional_contexts_text}

        OUTPUT FORMAT:
        Return only a summary, written in 2–5 sentences, focused strictly on additional context that supports the user's analysis objective.
        """

        formatted_prompt = prompt.format(
            analysis_description=state['analysis_description'],
            follow_up_questions='\n'.join(state['follow_up_questions']),
            additional_contexts_text=additional_contexts_text
        )
        additional_context_summary = log_llm_call(formatted_prompt, state['llm'], state['user_message_id'], 'ToolExecutor')
        log(f'additional_context_summary:\n{additional_context_summary}')
    return {'additional_context_summary': additional_context_summary, 'completed_tools': ['ToolExecutor'], 'tried_tools': tried_tools}