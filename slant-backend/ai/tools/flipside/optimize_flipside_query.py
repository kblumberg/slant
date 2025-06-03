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
from ai.tools.utils.utils import state_to_reference_materials, get_optimization_sql_notes_for_flipside, log_llm_call, get_flipside_schema_data

def flipside_optimize_query_fn(state: JobState, flipside_sql_query: str) -> str:

    # flipside_sql_query = state['optimized_flipside_sql_query'] if state['optimized_flipside_sql_query'] else state['verified_flipside_sql_query'] if state['verified_flipside_sql_query'] else state['improved_flipside_sql_query'] if state['improved_flipside_sql_query'] else state['flipside_sql_query']
    optimization_sql_notes = get_optimization_sql_notes_for_flipside()

    schema = get_flipside_schema_data(state['flipside_tables'], include_performance_notes=True)

    prompt = f"""
        You are an expert in writing accurate, efficient, and idiomatic Snowflake SQL queries for blockchain analytics using the Flipside database.

        ---

        ## Task

        Optimize the following Snowflake SQL query, keeping the same logic and output, but making it more efficient by adapting the JOIN and WHERE clauses where possible to make the query more performant.

        ### SQL Query:
        {flipside_sql_query}

        ---

        {optimization_sql_notes}

        ---

        ## Flipside Data Schema
        {schema}

        ---

        ## ✍️ Output

        Write a **correct, performant, and idiomatic** Snowflake SQL query that keeps the same logic and output, but is more efficient. If there are no optimizations to make, just return the original query.

        Keep everything else the same, just optimize the JOIN and WHERE clauses.

        Return ONLY the raw SQL (no extra text):
    """
    # log('flipside_optimize_query_fn')
    # log(prompt)

    sql_query = log_llm_call(prompt, state['complex_llm'], state['user_message_id'], 'OptimizeFlipsideQuery')

    # Remove SQL code block markers if present
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    log(f"flipside_optimize_query_fn query:")
    log(sql_query)
    return sql_query

def optimize_flipside_query(state: JobState) -> JobState:

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

        Write a **correct, performant, and idiomatic** Snowflake SQL query that answers the user’s question.

        Think carefully about the logic required to answer the user's question.

        As a reminder, the user's question is:
        {state['analysis_description']}

        Return ONLY the raw SQL (no extra text):
    """

    sql_query = log_llm_call(prompt, state['complex_llm'], state['user_message_id'], 'OptimizeFlipsideQuery')

    # Remove SQL code block markers if present
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    log(f"optimize_flipside_query query:")
    log(sql_query)
    return {'flipside_sql_query': sql_query, 'completed_tools': ["OptimizeFlipsideQuery"], 'upcoming_tools': ["ExecuteFlipsideQuery"]}