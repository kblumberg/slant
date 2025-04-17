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
from ai.tools.utils.utils import print_tool_starting

def write_flipside_query(state: JobState) -> JobState:
    start_time = time.time()
    print_tool_starting('write_flipside_query')

    if state['write_flipside_query_or_investigate_data'] != 'YES':
        return {}
    
    if state['flipside_sql_query'] and state['flipside_sql_error']:
        log('Trying to fix the previous query')
        log("state['flipside_sql_query']")
        log(state['flipside_sql_query'])
        log("state['flipside_sql_error']")
        log(state['flipside_sql_error'])
        prompt = f"""
            You are an expert in writing accurate, efficient, and idiomatic Snowflake SQL queries for blockchain analytics using the Flipside database.

            You are given a previous attempt at a query and the error it returned.

            Your task is to correct the query to fix the error.
            
            ## Reference

            Use the following example queries for inspiration and to understand available schema and patterns:
            {state['flipside_example_queries']}

            Schema:
            {state['schema']}


            ### ❌ Previous SQL Query:
            {state['flipside_sql_query']}

            ### 🛠️ Error Message:
            {state['flipside_sql_error']}

            🧠 Think carefully about the cause of the error. Was it a syntax issue, incorrect table/column, logic problem, or missing filter?

            Then, write a corrected version of the SQL query below.


            ## ✍️ Output

            Write a **correct, performant, and idiomatic** Snowflake SQL query that fixes the error from above.

            Return ONLY the raw SQL (no extra text):
        """

        sql_query = state['resoning_llm'].invoke(prompt).content

        # Remove SQL code block markers if present
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        return {'flipside_sql_query': sql_query}
    else:
        example_queries = '\n\n'.join(state['flipside_example_queries'].text.apply(lambda x: x[:10000]).values)
        # Create prompt template
        prompt = f"""
            You are an expert in writing accurate, efficient, and idiomatic Snowflake SQL queries for blockchain analytics using the Flipside database.

            ---

            ## Task

            Write a **valid and optimized Snowflake SQL query** that answers the following user question:

            ### ❓ Question:
            {state['analysis_description']}

            ---

            ## Reference

            Use the following example queries for inspiration and to understand available schema and patterns:

            {example_queries}

            Schema:
            {state['schema']}

            Make sure to remember any IMPORTANT notes from the schema. Override anything in the example queries based on the IMPORTANT notes.

            The `solana.price.ez_token_prices_hourly` table is deprecated. Use `solana.price.ez_prices_hourly` instead.

            ---

            ## Important Notes
            - Do NOT use `solana.price.ez_token_prices_hourly` (deprecated). Use `solana.price.ez_prices_hourly` instead.
            - Use example queries for structure and patterns, but tailor them to the user’s question.
            - Use `block_timestamp` filtering where applicable.
            - Avoid `SELECT *` and limit columns to what is needed.
            - Use `GROUP BY` when using aggregate functions.
            - Make sure token addresses and program IDs are correct (cross-reference with examples).
            - Ensure the query performs well by limiting time range and data volume where possible.
            - Return ONLY the raw SQL. No explanation, markdown, or formatting.
            - Any time-based column should be aliased as `date_time`.
            - Any categorical column should be aliased as `category`.

            ---

            ## ✍️ Output

            Write a **correct, performant, and idiomatic** Snowflake SQL query that answers the user’s question.

            Return ONLY the raw SQL (no extra text):
        """

        sql_query = state['resoning_llm'].invoke(prompt).content

        # Remove SQL code block markers if present
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        time_taken = round(time.time() - start_time, 1)
        log(f'write_flipside_query finished in {time_taken} seconds')
        log(f"Generated SQL Query:")
        log(sql_query)
        return {'flipside_sql_query': sql_query, 'completed_tools': ["DataAnalyst"], 'upcoming_tools': ["ExecuteFlipsideQuery"]}