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

def flipside_subset_example_queries(state: JobState) -> JobState:

    reference_materials = state_to_reference_materials(state, exclude_keys=['projects','flipside_example_queries'])
    example_queries = ''
    log(state['flipside_example_queries'])
    for i in range(len(state['flipside_example_queries'])):
        example_queries = example_queries + f"### Example Query #{i}:\n{state['flipside_example_queries'].text.values[i]}\n\n"
    
    flipside_tables = '\n- '.join(state['flipside_tables'])
    tables = f"""
    ### 📚 Priority Tables:
    Prioritize queries that utilize the following tables:
    - {flipside_tables}
    """ if len(flipside_tables) > 0 else ''

    prompt = f"""
    You are a senior crypto analyst specializing in Flipside's SQL query library.

    ---

    ## 🎯 Objective

    You are given:
    - A user question that needs to be answered with a SQL query
    - Reference materials to provide additional context
    - A list of example SQL queries (with titles, summaries, and full statements)

    Your task is to **select the most relevant example queries** that will help another LLM write a new SQL query to answer the user’s question.

    ---

    ### ❓ User Question:
    {state['analysis_description']}

    ---

    {reference_materials}

    ---

    {tables}

    ---

    ### 🧠 Example SQL Queries:
    {example_queries}

    Most examples include:
    - **Query Title**
    - **Dashboard Title**
    - **Query Summary**
    - **SQL Statement**

    Evaluate all available information and select the subset of example queries that are most useful to guide the generation of a new SQL query.

    Prioritize example queries that include specific tables, filters, program ids, addresses, etc. that will be helpful to guide the generation of a new SQL query.
    Also prioritize example queries that tackle similar questions to the user's question or use similar methodologies that we might need.

    Return at most 5 examples, but you may select fewer depending on relevance.

    Select as few examples as possible, prioritizing relevance, usefulness, and distinctness. If 2 queries are very similar, select just one of them.

    ---

    ## ✅ Output Format

    Return ONLY a list of query indices (e.g., [2, 3, 5, 7, 8]). Do not include any explanations, text, or formatting.

    Valid examples:
    [7]
    [0, 2, 3]
    [2, 4, 6, 8, 12]
    """

    response = log_llm_call(prompt, state['reasoning_llm'], state['user_message_id'], 'FlipsideSubsetExampleQueries')


    # Remove SQL code block markers if present
    response = parse_json_from_llm(response, state['llm'])
    response = [int(x) for x in response]
    log(f"flipside_subset_example_queries query:")
    log(response)
    queries = []
    for i in response:
        queries.append(state['flipside_example_queries'].text.values[i])
    tables = parse_tables_from_query('\n'.join(queries))
    tables = list(set(tables + state['flipside_tables']))
    return {'flipside_subset_example_queries': response, 'completed_tools': ["FlipsideSubsetExampleQueries"], 'flipside_tables': tables}

