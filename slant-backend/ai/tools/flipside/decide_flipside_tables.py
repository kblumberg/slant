import re
import time
import json
import pandas as pd
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from ai.tools.utils.utils import read_schemas
from db.flipside.rag_search_queries import rag_search_queries
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from ai.tools.utils.utils import state_to_reference_materials

def decide_flipside_tables(state: JobState) -> JobState:
    reference_materials = state_to_reference_materials(state)



    prompt = f"""
    You are an expert blockchain data analyst specializing in Flipside Crypto's SQL data architecture.

    ---

    ## 🎯 Task

    Given:
    - A user question describing the desired analysis
    - Reference materials (including relevant SQL example queries and database schema metadata)

    Your task is to **identify the specific SQL tables** that should be used to generate a query to answer the user’s question.

    If you think a table might be useful but are unsure, include it in the output. Better to include a table that is not needed than to exclude a table that is needed.

    ---

    ### 🧾 User Question:
    {state['analysis_description']}

    ---

    ### 📚 Reference Materials:
    {reference_materials}

    Reference materials may include:
    - Example queries with titles, summaries, and SQL code
    - Schema information (table names, column descriptions, datatypes, example values)

    Use this context to infer which tables contain the most relevant data.

    ---

    ## ✅ Output Format

    Return ONLY a JSON array of **relevant table names**, e.g.:

    ```json
    ["solana.core.fact_transfers", "solana.core.fact_transactions"]
    ```

    Do not include explanations, reasoning, or any extra formatting.
    """

    formatted_prompt = prompt.format(
        user_prompt=state['analysis_description'],
        reference_materials=reference_materials
    )
    response = state['reasoning_llm'].invoke(formatted_prompt).content
    j = parse_json_from_llm(response, state['llm'])
    log(f'decide_flipside_tables_from_queries response:')
    log(j)
    return {'flipside_tables': j, 'completed_tools': ["DecideFlipsideTables"]}
