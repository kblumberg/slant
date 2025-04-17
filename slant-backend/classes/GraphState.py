import pandas as pd
from classes.Tweet import Tweet
from classes.Project import Project
from classes.TwitterKol import TwitterKol
from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI
from typing import Annotated, TypedDict, List, Literal
from utils.memory import PostgresConversationMemory

def append(a, b):
    return a + b

class GraphState(TypedDict):
    query: str
    clarified_query: str
    refined_query: str
    refined_prompt_for_flipside_sql: str
    tweets: list[Tweet]
    projects: list[Project]
    kols: list[TwitterKol]
    run_tools: list[str]
    end_workflow: bool
    iteration: int
    sql_query: str
    sql_query_result: str
    flipside_sql_query: str
    flipside_sql_query_result: pd.DataFrame
    flipside_sql_error: str
    flipside_sql_attempts: int
    flipside_example_queries: str
    response: str
    sharky_agent_answer: str
    llm: ChatAnthropic | ChatOpenAI
    sql_llm: ChatAnthropic | ChatOpenAI
    highcharts_config: dict
    error: Exception
    current_message: str
    upcoming_tools: Annotated[List[str], append]
    completed_tools: Annotated[List[str], append]
    memory: PostgresConversationMemory
    conversation_id: str
    start_timestamp: int
    news_df: pd.DataFrame