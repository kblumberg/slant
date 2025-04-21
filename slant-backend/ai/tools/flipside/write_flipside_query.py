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

def write_flipside_query(state: JobState) -> JobState:

    reference_materials = state_to_reference_materials(state)

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

        Return ONLY the raw SQL (no extra text):
    """

    sql_query = state['reasoning_llm'].invoke(prompt).content

    # Remove SQL code block markers if present
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    log(f"write_flipside_query query:")
    log(sql_query)
    return {'flipside_sql_query': sql_query, 'completed_tools': ["WriteFlipsideQuery"], 'upcoming_tools': ["ExecuteFlipsideQuery"]}