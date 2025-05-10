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
from ai.tools.utils.utils import state_to_reference_materials, get_optimization_sql_notes_for_flipside
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm

def flipside_subset_example_queries(state: JobState) -> JobState:

    reference_materials = state_to_reference_materials(state, exclude_keys=['projects','flipside_example_queries'])
    example_queries = ''
    for i in range(len(state['flipside_example_queries'])):
        example_queries = example_queries + f"### Example Query {i}:\n{state['flipside_example_queries'][i]}\n\n"

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

    ### 📚 Reference Materials:
    {reference_materials}

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

    You may select more or fewer than 3 depending on relevance.

    ---

    ## ✅ Output Format

    Return ONLY a list of query indices (e.g., [2, 5, 7]). Do not include any explanations, text, or formatting.

    Valid examples:
    [0, 2, 3]
    [5, 9]  
    [2, 4, 6, 8, 11]  
    """

    response = state['reasoning_llm'].invoke(prompt).content

    # Remove SQL code block markers if present
    response = parse_json_from_llm(response, state['llm'])
    log(f"flipside_subset_example_queries query:")
    log(response)
    return {'flipside_example_queries': response, 'completed_tools': ["FlipsideSubsetExampleQueries"]}