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
from utils.db import fs_load_data

def check_decoded_flipside_tables(state: JobState) -> JobState:
    if len(state['program_ids']) == 0:
        return {'check_decoded_tables': '', 'completed_tools': ["CheckDecodedTables"]}
    
    query = f"""
    with t0 as (
        select program_id
        , min(block_timestamp) as min_block_timestamp
        from solana.core.ez_events_decoded
        where program_id in ({', '.join(state['program_ids'])})
        group by 1
    )
    , t1 as (
        select t0.program_id
        , count(distinct e.tx_id) as tx_count
        , max(datediff(day, e.block_timestamp, t0.min_block_timestamp)) as max_days_diff
        from t0
        left join solana.core.fact_events e on t0.program_id = e.program_id
            and e.block_timestamp < t0.min_block_timestamp
        group by 1
    )
    select * from t1
    """
    df = fs_load_data(query)
    log(f'check_decoded_flipside_tables program_ids: {state["program_ids"]}')
    log(df)
    use_decoded_flipside_tables = len(df) == len(state['program_ids']) and df.tx_count.max() <= 25
    log(f'use_decoded_flipside_tables: {use_decoded_flipside_tables}')
    return {'use_decoded_flipside_tables': use_decoded_flipside_tables, 'completed_tools': ["CheckDecodedFlipsideTables"]}