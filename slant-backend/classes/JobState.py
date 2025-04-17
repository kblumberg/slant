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
import json


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
    flipside_sql_error: str
    highcharts_config: dict
    messages: list[str]
    analyses: list[Analysis]
    llm: ChatAnthropic | ChatOpenAI
    complex_llm: ChatAnthropic | ChatOpenAI
    resoning_llm: ChatAnthropic | ChatOpenAI
    memory: any
    completed_tools: Annotated[List[str], append]
    upcoming_tools: Annotated[List[str], append]
    flipside_tables: list[str]
    flipside_example_queries: pd.DataFrame
    flipside_sql_query_result: pd.DataFrame
    flipside_sql_attempts: int
    web_search_results: str
    tavily_client: TavilyClient

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
            'flipside_sql_error': self.flipside_sql_error,
            'conversation_id': self.conversation_id,
            'highcharts_config': self.highcharts_config,
            'messages': self.messages,
            'analyses': self.analyses,
            'llm': self.llm,
            'complex_llm': self.complex_llm,
            'resoning_llm': self.resoning_llm,
            'memory': self.memory,
            'completed_tools': self.completed_tools,
            'upcoming_tools': self.upcoming_tools,
            'flipside_tables': self.flipside_tables,
            'flipside_example_queries': self.flipside_example_queries,
            'flipside_sql_query_result': self.flipside_sql_query_result,
            'flipside_sql_attempts': self.flipside_sql_attempts,
            'web_search_results': self.web_search_results,
        }

    def save_context(self, inputs: dict, outputs: dict):
        df = pd.DataFrame(inputs)
        pg_upload_data(df, 'job_state')
        # df = pd.DataFrame(outputs)
        # pg_upload_df(df, 'job_state_outputs')



def state_to_json(state: JobState):
    j = {
        'user_prompt': state['user_prompt'],
        'response': state['response'],
        'analysis_description': state['analysis_description'],
        'web_search_results': state['web_search_results'],
        'flipside_example_queries': state['flipside_example_queries'].query_id.tolist(),
        # 'follow_up_questions': state['follow_up_questions'],
        # 'tweets': state['tweets'],
        # 'user_id': state['user_id'],
        # 'conversation_id': state['conversation_id'],
        # 'highcharts_config': state['highcharts_config'],
        # 'analyses': state['analyses'],
        # 'completed_tools': state['completed_tools'],
        # 'flipside_tables': state['flipside_tables'],
        # 'flipside_example_queries': state['flipside_example_queries'],
        # 'web_search_results': state['web_search_results']
    }
    return json.dumps(j)