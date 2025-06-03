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
from ai.tools.utils.utils import state_to_reference_materials, get_optimization_sql_notes_for_flipside, log_llm_call
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from db.add_flipside_table_names import parse_tables_from_query

def flipside_write_investigation_queries(state: JobState) -> JobState:

    reference_materials = state_to_reference_materials(state, exclude_keys=['projects'])

    sql_query = state['optimized_flipside_sql_query'] if state['optimized_flipside_sql_query'] else state['verified_flipside_sql_query'] if state['verified_flipside_sql_query'] else state['improved_flipside_sql_query'] if state['improved_flipside_sql_query'] else state['flipside_sql_query']

    prompt = f"""
    You are a senior crypto data analyst with deep expertise in Flipside Crypto's SQL schema.

    ---

    ## 🎯 Objective

    You are provided with:
    - A user question to be answered via SQL
    - A proposed SQL query that attempts to answer it
    - Reference materials including database schema, sample queries, and other helpful context
    - Any previously run investigation queries and their results

    Your task is to write a **list of lightweight SQL queries** to quickly investigate the relevant data. These queries will help verify that key Common Table Expressions (CTEs), filters, or joins in the proposed query are returning the expected structure and data.

    These queries are not the final queries — they are a **sanity check**.

    ---

    ### 🔍 Guidelines for the Investigation Queries:
    - Always use a `WHERE block_timestamp >= ...` filter first in your `WHERE` clause (where possible, preferably `WHERE block_timestamp >= CURRENT_DATE() - 1`) to keep the query fast.
    - Use `LIMIT` (default to limit 3 rows unless more are needed) to keep the query fast -> this query has a timeout of 1 minute
    - Prefer selecting from just one or two relevant tables, focusing on verifying key filters or joins.
    - The goal is to confirm whether the filtered data exists and looks as expected.
    - If the proposed query uses well-understood or curated tables (and you're confident the data will return as expected), return an **empty list**.
    - Only include queries that are necessary to verify the proposed query (e.g. either a join or a filter).
    - Do NOT include queries that already exist in the investigation queries.

    ---

    ### ❓ User Question:
    {state['analysis_description']}

    ---

    ### 🧠 Proposed SQL Query:
    {sql_query}

    ---

    {reference_materials}

    ---

    ## ✅ Output Format

    Return **only** a list of SQL queries as raw strings — no explanations, comments, or formatting.

    If no investigation is necessary, return an **empty list**.
    """
    response = log_llm_call(prompt, state['complex_llm'], state['user_message_id'], 'FlipsideWriteInvestigationQueries')
    response = parse_json_from_llm(response, state['complex_llm'], to_json=True)
    log(f"flipside_write_investigation_queries queries:")
    log(response)
    existing_queries = [q['query'].lower() for q in state['flipside_investigations']]
    flipside_investigations = state['flipside_investigations'] + [ {'query': query, 'result': None, 'error': None, 'load_time': 0} for query in response if query.lower() not in existing_queries ]
    return {'flipside_investigations': flipside_investigations, 'completed_tools': ["FlipsideWriteInvestigationQueries"]}

