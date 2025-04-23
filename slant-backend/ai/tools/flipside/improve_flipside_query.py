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

def improve_flipside_query(state: JobState) -> JobState:
    reference_materials = state_to_reference_materials(state, use_summary=True)

    prompt = f"""
    You are an expert in writing **accurate, efficient, and idiomatic** Snowflake SQL queries for **blockchain analytics** using the **Flipside Crypto** database.

    You are given:
    - An **analysis objective**
    - A **SQL query** intended to fulfill that objective
    - A **schema reference** and **example queries** to guide you

    ---

    ## 🧠 Your Task

    Review the SQL query and determine:

    1. Does it accurately fulfill the **analysis objective**?
    2. Is it written in a **performant and idiomatic** way using Snowflake best practices?
    3. Does it align with Flipside's **data conventions** and **schema structure**?

    ---

    {reference_materials}

    **Analysis Objective:**  
    {state['analysis_description']}

    ---

    ## 🔎 SQL Query to Review

    ```sql
    {state['flipside_sql_query']}
    ```

    ✍️ Output Instructions
    If the query is already correct and optimal, return an empty string.

    If it is incomplete, incorrect, or unoptimized, return a single improved SQL query.

    Your response should be:

    Accurate – answers the analysis objective completely

    Performant – avoids inefficiencies

    Idiomatic – follows standard SQL practices in Flipside

    Return ONLY the corrected SQL query. No comments, no explanation, no formatting outside the SQL. """

    # ---

    # ### Optional Upgrades:
    # - You can add a line like:
    # > “Common pitfalls include: missing WHERE clauses, redundant CTEs, improper time filtering, or selecting too many columns.”

    # - If the queries are usually complex, consider giving a hint like:
    # > "Prefer using `ez_*` tables where appropriate to simplify logic."

    # Would you like me to help you add follow-up tools like structured diff checking, error classification, or self-scoring next?


    sql_query = state['reasoning_llm'].invoke(prompt).content

    # Remove SQL code block markers if present
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    log(f"improve_flipside_query query:")
    log(sql_query)
    return {'improved_flipside_sql_query': sql_query, 'completed_tools': ["ImproveFlipsideQuery"], 'upcoming_tools': ["VerifyFlipsideQuery"]}