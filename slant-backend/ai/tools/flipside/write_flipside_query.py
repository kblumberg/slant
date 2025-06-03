import os
import time
import pandas as pd
from utils.utils import log
from datetime import datetime, timedelta
from utils.db import pg_upload_data
from utils.db import pc_execute_query
from classes.JobState import JobState
from utils.flipside import extract_project_tags_from_user_prompt
from ai.tools.utils.prompt_refiner_for_flipside_sql import prompt_refiner_for_flipside_sql
from constants.keys import OPENAI_API_KEY
from langchain_openai import ChatOpenAI
from ai.tools.utils.utils import state_to_reference_materials, log_llm_call
from ai.tools.flipside.optimize_flipside_query import flipside_optimize_query_fn

def write_flipside_query(state: JobState) -> JobState:

    reference_materials = state_to_reference_materials(state, include_performance_notes=True)

    prompt = f"""
        You are an expert in writing accurate, efficient, and idiomatic Snowflake SQL queries for blockchain analytics using the Flipside database.

        ---

        ## Task

        Write a **valid and optimized Snowflake SQL query** that answers the following user question:

        ### ❓ Question:
        {state['analysis_description']}

        ---

        {reference_materials}

        ---

        ## ✍️ Output

        Any time-based column should be aliased as `date_time` in the final SELECT statement.

        If there is 1 or more categorical columns (must be a string column), the first categorical column should be aliased as `category` in the final SELECT statement.

        Write a **correct, performant, and idiomatic** Snowflake SQL query that answers the user’s question.

        Think carefully about the logic required to answer the user's question.

        As a reminder, the user's question is:
        {state['analysis_description']}

        Return ONLY the raw SQL (no extra text):
    """

    sql_query = log_llm_call(prompt, state['reasoning_llm_anthropic'], state['user_message_id'], 'WriteFlipsideQuery')

    # Remove SQL code block markers if present
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    log(f"write_flipside_query query:")
    log(sql_query)
    # sql_query = flipside_optimize_query_fn(state, sql_query)
    # log(f"write_flipside_query optimized query:")
    # log(sql_query)
    return {'flipside_sql_query': sql_query, 'completed_tools': ["WriteFlipsideQuery"], 'upcoming_tools': ["ExecuteFlipsideQuery"]}