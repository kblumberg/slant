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
from ai.tools.utils.utils import state_to_reference_materials

def fix_flipside_query(state: JobState) -> JobState:

    reference_materials = state_to_reference_materials(state)

    prompt = f"""
        You are an expert in writing accurate, efficient, and idiomatic Snowflake SQL queries for blockchain analytics using the Flipside database.

        You are given a previous attempt at a query and the error it returned.

        Your task is to correct the query to fix the error.
        
        {reference_materials}

        ### ❌ Previous SQL Query:
        {state['flipside_sql_query']}

        ### 🛠️ Error Message:
        {state['flipside_sql_error']}

        🧠 Think carefully about the cause of the error. Was it a syntax issue, incorrect table/column, logic problem, or missing filter?

        Then, write a corrected version of the SQL query below.

        ## Tips
        - If you are doing some kind of running total, make sure there are no gaps in the data by using the `crosschain.core.dim_dates` table.

        ## ✍️ Output

        Write a **correct, performant, and idiomatic** Snowflake SQL query that fixes the error from above.

        Format the query so that the final output has at most 3 columns: timestamp, category, and value.


        Return ONLY the raw SQL (no extra text):
    """

    sql_query = state['reasoning_llm'].invoke(prompt).content
    # Remove SQL code block markers if present
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    log(f"fix_flipside_query query:")
    log(sql_query)
    return {'improved_flipside_sql_query': sql_query, 'completed_tools': ["FixFlipsideQuery"], 'upcoming_tools': ["VerifyFlipsideQuery"]}