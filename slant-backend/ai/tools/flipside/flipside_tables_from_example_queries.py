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
from db.add_flipside_table_names import parse_tables_from_query

def flipside_tables_from_example_queries(state: JobState) -> JobState:
    if state['question_type'] == 'other':
        return {'completed_tools': ["FlipsideTablesFromExampleQueries"]}
    queries = '\n'.join(state['flipside_example_queries']['text'].values)
    j = parse_tables_from_query(queries)

    log(f"flipside_tables_from_example_queries (1): {j}")
    must_include = [
        ('solana.defi.fact_swaps', 'solana.defi.ez_dex_swaps')
        , ('solana.defi.fact_swaps_jupiter_summary', 'solana.defi.fact_swaps_jupiter_inner')
        , ('solana.defi.fact_swaps_jupiter_inner', 'solana.defi.fact_swaps_jupiter_summary')
        , ('solana.nft.fact_nft_sales', 'solana.nft.ez_nft_sales')
    ]
    for a, b in must_include:
        if a in j and not b in j:
            j.append(b)
    log(f"flipside_tables_from_example_queries (2): {j}")
    return {'flipside_tables_from_example_queries': j, 'completed_tools': ['FlipsideTablesFromExampleQueries']}
