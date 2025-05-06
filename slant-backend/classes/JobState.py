import json
import pandas as pd
from classes.Tweet import Tweet
from classes.Project import Project
from classes.TwitterKol import TwitterKol
from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI
from typing import Annotated, TypedDict, List, Literal
# from utils.memory import PostgresConversationMemory
from classes.Analysis import Analysis
from utils.db import pg_upload_data
from tavily import TavilyClient
from classes.Transaction import Transaction

def append(a, b):
    return a + b

class JobState(TypedDict):
    user_message_id: str
    user_prompt: str
    response: str
    schema: str
    pre_query_clarifications: str
    follow_up_questions: list[str]
    tweets: list[Tweet]
    projects: list[Project]
    user_id: str
    conversation_id: str
    analysis_description: str
    write_flipside_query_or_investigate_data: str
    flipside_sql_query: str
    improved_flipside_sql_query: str
    verified_flipside_sql_query: str
    flipside_sql_errors: Annotated[List[str], append]
    investigation_flipside_sql_errors: Annotated[List[str], append]
    flipside_sql_queries: Annotated[List[str], append]
    investigation_flipside_sql_queries: Annotated[List[str], append]
    flipside_sql_query_results: Annotated[List[pd.DataFrame], append]
    investigation_flipside_sql_query_results: Annotated[List[pd.DataFrame], append]
    flipside_sql_error: str
    highcharts_configs: list[dict]
    messages: list[str]
    analyses: list[Analysis]
    transactions: Annotated[List[Transaction], append]
    llm: ChatAnthropic | ChatOpenAI
    complex_llm: ChatAnthropic | ChatOpenAI
    reasoning_llm: ChatAnthropic | ChatOpenAI
    memory: any
    tried_tools: int
    additional_contexts: Annotated[List[str], append]
    run_tools: Annotated[List[str], append]
    completed_tools: Annotated[List[str], append]
    upcoming_tools: Annotated[List[str], append]
    flipside_tables: list[str]
    flipside_example_queries: pd.DataFrame
    flipside_sql_query_result: pd.DataFrame
    flipside_sql_attempts: int
    web_search_results: str
    tavily_client: TavilyClient
    context_summary: str
    tweets_summary: str
    web_search_summary: str
    curated_tables: list[str]
    raw_tables: list[str]
    approach: str

    def to_dict(self):
        return {
            'user_prompt': self.user_prompt,
            'response': self.response,
            'follow_up_questions': self.follow_up_questions,
            'pre_query_clarifications': self.pre_query_clarifications,
            'tweets': self.tweets,
            'user_id': self.user_id,
            'analysis_description': self.analysis_description,
            'write_flipside_query_or_investigate_data': self.write_flipside_query_or_investigate_data,
            'flipside_sql_query': self.flipside_sql_query,
            'improved_flipside_sql_query': self.improved_flipside_sql_query,
            'verified_flipside_sql_query': self.verified_flipside_sql_query,
            'flipside_sql_error': self.flipside_sql_error,
            'conversation_id': self.conversation_id,
            'highcharts_configs': self.highcharts_configs,
            'messages': self.messages,
            'analyses': self.analyses,
            'llm': self.llm,
            'complex_llm': self.complex_llm,
            'reasoning_llm': self.reasoning_llm,
            'memory': self.memory,
            'tried_tools': self.tried_tools,
            'additional_contexts': self.additional_contexts,
            'run_tools': self.run_tools,
            'completed_tools': self.completed_tools,
            'upcoming_tools': self.upcoming_tools,
            'transactions': self.transactions,
            'flipside_tables': self.flipside_tables,
            'flipside_example_queries': self.flipside_example_queries,
            'flipside_sql_query_result': self.flipside_sql_query_result,
            'flipside_sql_attempts': self.flipside_sql_attempts,
            'web_search_results': self.web_search_results,
            'context_summary': self.context_summary,
            'tweets_summary': self.tweets_summary,
            'web_search_summary': self.web_search_summary,
            'curated_tables': self.curated_tables,
            'raw_tables': self.raw_tables,
            'approach': self.approach
        }

    def save_context(self, inputs: dict, outputs: dict):
        df = pd.DataFrame(inputs)
        pg_upload_data(df, 'job_state')
        # df = pd.DataFrame(outputs)
        # pg_upload_df(df, 'job_state_outputs')



