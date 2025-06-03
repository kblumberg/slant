import re
import time
import json
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from ai.tools.utils.utils import read_schemas
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from ai.tools.utils.utils import state_to_reference_materials
from ai.tools.utils.utils import log_llm_call

def flipside_basic_table_selection(state: JobState) -> JobState:
    reference_materials = state_to_reference_materials(state, include_keys=['schema'])
    prompt = f"""
    You are an expert crypto data scientist trained in the Flipside SQL database. Your task is to read the user's analysis goal and Flipside database schema to determine the basic tables that are needed to analyze the user's analysis goal.

    Use the context below to help determine the basic tables that are needed to analyze the user's analysis goal.

    ---

    ## User Analysis Goal
    {state['analysis_description']}

    {reference_materials}

    ---

    **Instructions:**
    - Return a valid JSON list of the tables that are needed to analyze the user's analysis goal.
    - Use the schema to guide your selection.
    - Synergize all the context above to determine the basic tables.

    Return only the valid JSON list of the tables. No other text or comments.
    """
    flipside_basic_table_selection = log_llm_call(prompt, state['llm'], state['user_message_id'], 'FlipsideBasicTableSelection')
    flipside_basic_table_selection = parse_json_from_llm(flipside_basic_table_selection, state['llm'])
    log(f"flipside_basic_table_selection (1): {flipside_basic_table_selection}")
    must_include = [
        ('solana.defi.fact_swaps', 'solana.defi.ez_dex_swaps')
        , ('solana.defi.fact_swaps_jupiter_summary', 'solana.defi.fact_swaps_jupiter_inner')
        , ('solana.defi.fact_swaps_jupiter_inner', 'solana.defi.fact_swaps_jupiter_summary')
        , ('solana.nft.fact_nft_sales', 'solana.nft.ez_nft_sales')
    ]
    for a, b in must_include:
        if a in flipside_basic_table_selection and not b in flipside_basic_table_selection:
            flipside_basic_table_selection.append(b)
    log(f"flipside_basic_table_selection (2): {flipside_basic_table_selection}")
    return {'flipside_basic_table_selection': flipside_basic_table_selection, 'completed_tools': ['FlipsideBasicTableSelection']}
