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
from constants.constant import MAX_FLIPSIDE_SQL_ATTEMPTS
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm

def verify_flipside_query(state: JobState) -> JobState:
    if state["flipside_sql_error"] or state["flipside_sql_attempts"] >= MAX_FLIPSIDE_SQL_ATTEMPTS:
        return {}

    results = state['flipside_sql_query_result'].to_markdown() if len(state['flipside_sql_query_result']) <= 20 else pd.concat([state['flipside_sql_query_result'].head(10), state['flipside_sql_query_result'].tail(10)]).to_markdown()

    sql_query = state['optimized_flipside_sql_query'] if state['optimized_flipside_sql_query'] else state['verified_flipside_sql_query'] if state['verified_flipside_sql_query'] else state['improved_flipside_sql_query'] if state['improved_flipside_sql_query'] else state['flipside_sql_query']

    previous_sql_queries = ''
    if len(state['flipside_sql_queries']) > 1:
        previous_sql_queries = '**Previous SQL Queries:**\n' + 'Here are the previous SQL queries that have been tried and did not produce successful results:\n'
        for a, b, c in zip(state['flipside_sql_queries'][:-1], state['flipside_sql_query_results'][:-1], state['flipside_sql_errors'][:-1]):
            prev_results = b.to_markdown() if len(b) <= 10 else pd.concat([b.head(5), b.tail(5)]).to_markdown()
            results_text = '**Results** (first and last 5 rows):\n' + prev_results if len(b) > 0 else '**Results**: No rows returned'
            error_text = '**Error**:\n' + c if c else ''
            previous_sql_queries += f'**Query:**\n{a}\n{results_text}\n{error_text}\n\n'

    numerical_columns = state['flipside_sql_query_result'].select_dtypes(include=['number']).columns.tolist()
    # cur = pd.DataFrame([{'category': 'a', 'num': 1}, {'category': 'b', 'num': 2}])
    # numerical_columns = cur.select_dtypes(include=['number']).columns.tolist()
    tot = 0
    for col in numerical_columns:
        if not col in ['timestamp']:
            tot += state['flipside_sql_query_result'][col].sum()
    log(f'tot: {tot}')
    if state['flipside_sql_attempts'] == 0:
        log(f"First attempt")
        
        prompt = f"""
        You are an expert in writing **correct, efficient, and idiomatic** Snowflake SQL queries for **blockchain analytics** using the **Flipside Crypto** database.

        You will be given:
        - A **description of the analysis**
        - A **proposed SQL query**
        - Reference **schema** and **example queries**
        - Other **reference materials** to enrich your understanding of the analysis objective

        ---

        ## 🔍 Your Task

        Carefully review the SQL query and evaluate:

        1. **Correctness** – Does the query correctly fulfill the intended analysis?
        2. **Results** – Will the results of this query pull the correct data to answer the analysis goal?

        ---

        ## 📘 Reference Materials
        {state_to_reference_materials(state, exclude_keys=['tweets','web_search_results','projects','additional_contexts'], include_performance_notes=True)}

        ---

        ## 🧠 Inputs

        **Analysis Goal:**  
        {state['analysis_description']}


        **Proposed SQL Query:**  
        ```sql
        {sql_query}
        ```

        ✍️ Output
        Think carefully. If the proposed SQL query will produce the correct results, return an empty string.

        Otherwise, return a single block of improved SQL that:
        - Produces correct and complete results
        - Is efficient and idiomatic
        - Aligns with the analysis objective

        Return only the raw SQL. No explanation or extra text. """
        # log(f"verify_flipside_query prompt:")
        # log(prompt)
        sql_query = log_llm_call(prompt, state['reasoning_llm_anthropic'], state['user_message_id'], 'VerifyFlipsideQuery')
        verified_flipside_sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        # log(f"verified_flipside_sql_query query:")
        # log(verified_flipside_sql_query)

        return {'verified_flipside_sql_query': verified_flipside_sql_query, 'completed_tools': ["VerifyFlipsideQuery"], 'upcoming_tools': ["WriteFlipsideQuery"]}

    elif state['flipside_sql_attempts'] > 0 and (len(state['flipside_sql_query_result']) == 0 or tot == 0):
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

        {state_to_reference_materials(state, include_performance_notes=True)}

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
        # sql query returned results - check if the results are correct
        log(f"sql query returned results")

        include_keys = ['flipside_example_queries','schema','transactions','program_ids','flipside_determine_approach']
        reference_materials = state_to_reference_materials(state, include_keys=include_keys, include_performance_notes=True, include_column_types=False, include_column_descriptions=False)
        prompt = f"""
        You are an expert in analyzing blockchain analytics and providing feedback on SQL queries and results.

        You will be given:
        - A **description of the analysis objective**
        - Possibly **previous SQL queries** that failed (e.g., returned no results or incorrect results)
        - A reference **schema**
        - A set of **example queries** to show good patterns and table usage
        - Other **reference materials** to enrich your understanding of the analysis objective
        - The **latest SQL query**, which has returned results
        - The **latest SQL query results** (first and last 10 rows)

        ---

        ## 🔍 Your Task

        Analyze the latest SQL query results and determine if the query successfully fulfills the analysis objective and return a valid JSON object with the following fields:
        - `change_type`: int: 0 = no change, 1 = almost there, just a minor change to query, 2 = more substantial change to query, possibly a new table or new filters
        - `change_summary`: a single block of text that explains why the query / results are wrong and some potential ways to fix it. If there are no issues with the query and the results are correct, just an empty string.

        Only suggest a change if you are confident that the query and results are materially incorrect. If the labeling or ordering of the results is incorrect, that is not a problem.

        ---

        {reference_materials}

        **Today's date:** {datetime.now().strftime('%Y-%m-%d')}
        
        ---

        ## 🧠 Inputs

        **Analysis Objective:** 
        {state['analysis_description']}

        **Previous SQL Attempts:** 
        ```sql
        {previous_sql_queries}
        ```

        **Latest SQL Query:**
        ```sql
        {sql_query}
        ```

        **Latest SQL Query Results:**
        We are just showing the first and last 10 rows of the results. There may be more rows that are not shown.
        {results}

        ⚠️ Red Flags to Watch For
        When analyzing whether the results seem correct, look for these issues:
        - Mostly zero values or NULL values in key columns where non-zero values are expected (counts, volumes, fees)
        - Extremely low row count when the objective suggests an active dataset
        - Duplicate rows where unique rows are expected
        - Values that seems very erratic or not consistent with other values within the same column
        - Data is cut off in a way that prevents a complete analysis
        - Values that do not match the intended metric

        🤔 Reasoning Process
        1️⃣ Analyze the query results and check whether they are correct and aligned with the analysis objective, using the red flags above. Web an twitter data is often noisy and may not be correct, so if the results are inconsistent with the web or twitter data, it still may be correct. Rely primarily on the red flags to determine if the results are correct.
        2️⃣ If the results are correct and sufficient, return a JSON object with the following fields:
        - `change_type`: 0
        - `change_summary`: an empty string
        3️⃣ If the results are incorrect or insufficient:
        - Identify what is wrong with the current query and results.
        - Decide how to rewrite it: what tables, filters, or logic to change. If the change is minor, set `change_type` to 1. If the change is more substantial, set `change_type` to 3.
        4️⃣ Output a JSON object with the following fields:
        - `change_type`: 1 or 2
        - `change_summary`: a single block of text that explains why the query / results are wrong and some potential ways to fix it.

        **Reminders:**
        - Have a bias towards accepting the latest SQL query and results. Only suggest a change if you are confident that the query and results are materially incorrect. If the issue is minor, just accept the latest SQL query and results.

        📝 Output
        Return only the JSON object.

        """
        # feedback = log_llm_call(prompt, state['reasoning_llm_anthropic'], state['user_message_id'], 'VerifyFlipsideQueryFeedback')
        feedback = log_llm_call(prompt, state['reasoning_llm_openai'], state['user_message_id'], 'VerifyFlipsideQueryFeedback')
        feedback = parse_json_from_llm(feedback, state['llm'], True)
        log(f"verify_flipside_query feedback:")
        log(feedback)
        prompt = ''

        if feedback['change_type'] > 0:

            include_keys = [] if feedback['change_type'] == 1 else ['flipside_example_queries','schema','transactions','program_ids','flipside_determine_approach','flipside_investigations']
            reference_materials = state_to_reference_materials(state, include_keys=include_keys, include_performance_notes=True)

            prompt = f"""
            You are an expert in writing **correct, efficient, and idiomatic** Snowflake SQL queries for **blockchain analytics** using the **Flipside Crypto** database.

            You will be given:
            - A **description of the analysis objective**
            - Possibly **previous SQL queries** that failed (e.g., returned no results or incorrect results)
            - A reference **schema**
            - A set of **example queries** to show good patterns and table usage
            - Other **reference materials** to enrich your understanding of the analysis objective
            - The **latest SQL query**, which has returned results
            - The **latest SQL query results** (first and last 10 rows)
            - The **feedback** on the latest SQL query (why the results are incorrect and some potential ways to fix it)

            ---

            ## 🔍 Your Task

            Analyze the failed SQL attempts and write a **new SQL query** that successfully fulfills the analysis objective.

            ---

            {reference_materials}

            ---

            ## 🧠 Inputs

            **Analysis Objective:** 
            {state['analysis_description']}

            **Previous SQL Attempts:** 
            ```sql
            {previous_sql_queries}
            ```

            **Latest SQL Query:**
            ```sql
            {sql_query}
            ```

            **Latest SQL Query Results:** First and last 10 rows
            {results}

            **Feedback on Latest SQL Query:**
            {feedback['change_summary']}

            ✍️ Output Instructions
            Do NOT repeat any of the previous failed queries, including the latest one.

            You must try a new approach, such as:
            - Using different tables
            - Revising filters
            - Modifying the query logic
            - Re-thinking how the metric or event is represented in Flipside’s schema

            Your output must be:
            - A single, correct SQL query
            - Efficient and idiomatic Snowflake SQL
            - Aligned with the analysis objective

            Return only the raw SQL. No explanations, no comments, no markdown. 

            """
        # return {'verified_flipside_sql_query': '', 'completed_tools': ["VerifyFlipsideQuery"], 'upcoming_tools': ["WriteFlipsideQuery"]}
    sql_query = log_llm_call(prompt, state['reasoning_llm_anthropic'], state['user_message_id'], 'VerifyFlipsideQuery') if prompt else ''

    # Remove SQL code block markers if present
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    log(f"verify_flipside_query query:")
    log(sql_query)
    return {'verified_flipside_sql_query': sql_query, 'completed_tools': ["VerifyFlipsideQuery"], 'upcoming_tools': ["WriteFlipsideQuery"]}