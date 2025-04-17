from typing import TypedDict
from classes.Tweet import Tweet
from classes.Project import Project
from classes.TwitterKol import TwitterKol
from langchain_anthropic import ChatAnthropic
import pandas as pd
from langchain_openai import ChatOpenAI

class SharkyState(TypedDict):
    question: str
    response: str
    sql_query: str
    sql_query_result: pd.DataFrame
    highcharts_config: dict
    llm: ChatAnthropic | ChatOpenAI
    sql_llm: ChatAnthropic | ChatOpenAI
    error: Exception
    is_advisory: bool | None