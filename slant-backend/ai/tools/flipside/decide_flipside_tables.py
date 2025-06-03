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
from ai.tools.utils.utils import state_to_reference_materials, log_llm_call

def decide_flipside_tables(state: JobState) -> JobState:
    include_keys = ['flipside_example_queries','schema','transactions','program_ids','flipside_investigations']
    reference_materials = state_to_reference_materials(state, include_keys=include_keys, include_column_types=False, include_column_descriptions=False, include_example_values=False, include_usage_tips=False)

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
    response = log_llm_call(prompt, state['reasoning_llm_anthropic'], state['user_message_id'], 'DecideFlipsideTables')
    j = parse_json_from_llm(response, state['llm'])
    log(f'decide_flipside_tables response:')
    log(j)
    j = [table.strip() for table in j]
    j = [table.strip() for table in j if not table in ['solana.core.ez_events_decoded']]
    must_include = [
        ('solana.defi.fact_swaps', 'solana.defi.ez_dex_swaps')
        , ('solana.defi.fact_swaps_jupiter_summary', 'solana.defi.fact_swaps_jupiter_inner')
        , ('solana.nft.fact_nft_sales', 'solana.nft.ez_nft_sales')
        # , ('solana.defi.fact_swaps_jupiter_inner', 'solana.defi.fact_swaps_jupiter_summary')
    ]
    for a, b in must_include:
        if a in j and not b in j:
            j.append(b)
    return {'flipside_tables': j, 'completed_tools': ["DecideFlipsideTables"]}
