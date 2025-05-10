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
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from ai.tools.utils.utils import state_to_reference_materials
from utils.db import fs_load_data

def flipside_check_query_subset(state: JobState) -> JobState:

    flipside_sql_query = state['verified_flipside_sql_query'] if state['verified_flipside_sql_query'] else state['improved_flipside_sql_query'] if state['improved_flipside_sql_query'] else state['flipside_sql_query']

    prompt = f"""
        You are an expert in writing accurate, efficient, and idiomatic Snowflake SQL queries for blockchain analytics using the Flipside database.

        ---

        ## Task

        Update the following SQL query to to filter data just in the past 50 hours (e.g. `WHERE block_timestamp >= DATEADD(hour, -50, CURRENT_TIMESTAMP())`).

        ### SQL Query:
        {flipside_sql_query}

        ---

        ## ✍️ Output

        Write a **correct, performant, and idiomatic** Snowflake SQL query that is a replica of the original query, but with the data filtered to the past 25 hours.
        Return ONLY the raw SQL (no extra text):
    """

    sql_query = state['llm'].invoke(prompt).content

    # Remove SQL code block markers if present
    sql_query = parse_json_from_llm(sql_query, state['llm'], to_json=False)
    log(f"flipside_check_query_subset:")
    log(sql_query)
    df, error, load_time = fs_load_data(sql_query, 3)
    start_date = datetime.strptime(state['start_timestamp'], '%Y-%m-%d')
    total_days = (datetime.now() - start_date).days
    eta = int(round(load_time * total_days / 2))
    log(f"flipside_check_query_subset")
    log(f"eta: {eta}")
    log(f"load_time: {load_time}s")
    log(f"rows: {df.shape[0]}")
    return {'eta': eta, 'completed_tools': ["FlipsideCheckQuerySubset"], 'upcoming_tools': ["ExecuteFlipsideQuery"]}