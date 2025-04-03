import pandas as pd
from classes.Tweet import Tweet
from classes.Project import Project
from classes.TwitterKol import TwitterKol
from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI
from typing import Annotated, TypedDict, List, Literal
from utils.memory import PostgresConversationMemory
from classes.Analysis import Analysis
from utils.db import pg_upload_data

def append(a, b):
    return a + b

class JobState(TypedDict):
    user_prompt: str
    answer: str
    user_id: str
    session_id: str
    messages: list[str]
    analyses: list[Analysis]
    llm: ChatAnthropic | ChatOpenAI
    memory: PostgresConversationMemory
    completed_tools: Annotated[List[str], append]
    flipside_tables: list[str]
    flipside_example_queries: pd.DataFrame

    def to_dict(self):
        return {
            'user_prompt': self.user_prompt,
            'answer': self.answer,
            'user_id': self.user_id,
            'session_id': self.session_id,
            'messages': self.messages,
            'analyses': self.analyses,
            'llm': self.llm,
            'memory': self.memory,
            'completed_tools': self.completed_tools,
            'flipside_tables': self.flipside_tables,
            'flipside_example_queries': self.flipside_example_queries,
        }

    def save_context(self, inputs: dict, outputs: dict):
        df = pd.DataFrame(inputs)
        pg_upload_data(df, 'job_state')
        # df = pd.DataFrame(outputs)
        # pg_upload_df(df, 'job_state_outputs')
