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
from ai.tools.utils.utils import print_tool_starting, get_sql_notes, state_to_reference_materials
from constants.constant import MAX_FLIPSIDE_SQL_ATTEMPTS

def verify_flipside_query(state: JobState) -> JobState:
    return {'verified_flipside_sql_query': '', 'completed_tools': ["VerifyFlipsideQuery"], 'upcoming_tools': ["WriteFlipsideQuery"]}
    if state["flipside_sql_error"] or state["flipside_sql_attempts"] >= MAX_FLIPSIDE_SQL_ATTEMPTS:
        return {}

    results = state['flipside_sql_query_result'].to_markdown() if len(state['flipside_sql_query_result']) <= 20 else pd.concat([state['flipside_sql_query_result'].head(10), state['flipside_sql_query_result'].tail(10)]).to_markdown()

    sql_query = state['improved_flipside_sql_query'] if state['improved_flipside_sql_query'] else state['flipside_sql_query']

    previous_sql_queries = ''
    if len(state['flipside_sql_queries']) > 1:
        previous_sql_queries = '**Previous SQL Queries:**\n' + 'Here are the previous SQL queries that have been tried and did not produce successful results:\n'
        for a, b, c in zip(state['flipside_sql_queries'][:-1], state['flipside_sql_query_results'][:-1], state['flipside_sql_errors'][:-1]):
            prev_results = b.to_markdown() if len(b) <= 10 else pd.concat([b.head(5), b.tail(5)]).to_markdown()
            results_text = '**Results** (first and last 5 rows):\n' + prev_results if len(b) > 0 else '**Results**: No rows returned'
            error_text = '**Error**:\n' + c if c else ''
            previous_sql_queries += f'**Query:**\n{a}\n{results_text}\n{error_text}\n\n'

    if len(state['flipside_sql_query_result']) == 0:
        log(f"No results from flipside query")
        prompt = f"""
        You are an expert in writing **correct, efficient, and idiomatic** Snowflake SQL queries for **blockchain analytics** using the **Flipside Crypto** database.

        You will be given:
        - A **description of the analysis objective**
        - One or more **previous SQL queries** that failed (e.g., returned no results or incorrect results)
        - A reference **schema**
        - A set of **example queries** to show good patterns and table usage
        - Other **reference materials** to enrich your understanding of the analysis objective
        - The **latest SQL query**, which has returned no results

        ---

        ## 🔍 Your Task

        Analyze the failed SQL attempts and write a **new SQL query** that successfully fulfills the analysis objective.

        ---

        {state_to_reference_materials(state, use_summary=True)}

        ---

        ## 🧠 Inputs

        **Analysis Objective:**  
        {state['analysis_description']}

        **Previous SQL Attempts:**  
        ```sql
        {previous_sql_queries}
        ```

        **Latest SQL Query:** (returned no results)
        ```sql
        {sql_query}
        ```

        ✍️ Output Instructions
        Do NOT repeat any of the previous failed queries, including the latest one.

        You must try a new approach, such as:

        Using different tables

        Revising filters

        Modifying the query logic

        Re-thinking how the metric or event is represented in Flipside’s schema

        Your output must be:

        A single, correct SQL query

        Efficient and idiomatic Snowflake SQL

        Aligned with the analysis objective

        Think about why the previous query returned no results and how to modify the query to return the correct results. You MUST change the filters or the tables used (or both).

        Return only the raw SQL. No explanations, no comments, no markdown. """
        # -- Notes:
        # -- - Likely problem: wrong `program_id` used
        # -- - Consider joining with `dim_labels` to find wallet tags

    else:
        prompt = f"""
        You are an expert in writing **correct, efficient, and idiomatic** Snowflake SQL queries for **blockchain analytics** using the **Flipside Crypto** database.

        You will be given:
        - A **description of the analysis**
        - A **SQL query**
        - A **sample of the query results**
        - Reference **schema** and **example queries**
        - Other **reference materials** to enrich your understanding of the analysis objective

        ---

        ## 🔍 Your Task

        Carefully review the SQL query and evaluate:

        1. **Correctness** – Does the query correctly fulfill the intended analysis?
        2. **Results** – Do the results answer the analysis goal?

        ---

        ## 📘 Reference Materials
        {state_to_reference_materials(state, exclude_keys=['tweets','web_search_results','projects','additional_contexts'])}

        ---

        ## 🧠 Inputs

        **Analysis Goal:**  
        {state['analysis_description']}

        {previous_sql_queries}

        **SQL Query:**  
        ```sql
        {sql_query}
        ```

        **Results:** (first and last 20 rows)
        ```
        {results}
        ```

        ✍️ Output
        Think carefully. If the results provide data that answers the analysis goal, return an empty string.

        Otherwise, return a single block of improved SQL that:
        - Produces correct and complete results
        - Is efficient and idiomatic
        - Aligns with the analysis objective

        Return only the raw SQL. No explanation or extra text. """

        return {'verified_flipside_sql_query': '', 'completed_tools': ["VerifyFlipsideQuery"], 'upcoming_tools': ["WriteFlipsideQuery"]}

    sql_query = state['reasoning_llm'].invoke(prompt).content

    # Remove SQL code block markers if present
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    log(f"verify_flipside_query query:")
    log(sql_query)
    return {'verified_flipside_sql_query': sql_query, 'completed_tools': ["VerifyFlipsideQuery"], 'upcoming_tools': ["WriteFlipsideQuery"]}