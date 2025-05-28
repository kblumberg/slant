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
from constants.constant import MAX_FLIPSIDE_SQL_ATTEMPTS
from utils.db import fs_load_data

def check_decoded_flipside_tables(state: JobState) -> JobState:
    if len(state['program_ids']) == 0:
        tables = [x for x in state['flipside_tables'] if not 'ez_events_decoded' in x]
        return {'use_decoded_flipside_tables': False, 'flipside_tables': tables, 'completed_tools': ["CheckDecodedFlipsideTables"]}
    
    query = f"""
    with t0 as (
        select program_id
        , min(block_timestamp) as min_block_timestamp
        , min(block_timestamp::date) as min_date
        from solana.core.ez_events_decoded
        where block_timestamp >= '{state['start_timestamp']}'
            and program_id in ('{"', '".join(state['program_ids'])}')
        group by 1
    )
    , t1 as (
        select t0.program_id
        , min_date
        , count(distinct e.tx_id) as tx_count
        , max(datediff(day, e.block_timestamp, t0.min_block_timestamp)) as max_days_diff
        from t0
        left join solana.core.fact_events e
            on e.block_timestamp >= '{state['start_timestamp']}'
            and e.block_timestamp < t0.min_block_timestamp
            and t0.program_id = e.program_id
        group by 1, 2
    )
    select * from t1
    """
    df, error, load_time = fs_load_data(query, 20)
    log(f'check_decoded_flipside_tables program_ids: {state["program_ids"]}')
    log(df)
    use_decoded_flipside_tables = len(df) == len(state['program_ids']) and df.tx_count.max() <= 25
    log(f'use_decoded_flipside_tables: {use_decoded_flipside_tables}')
    if len(df):
        start_timestamp = max(str(df.min_date.min())[:10], state['start_timestamp'])
        tables = list(set(state['flipside_tables'] + ['solana.core.ez_events_decoded'])) if use_decoded_flipside_tables else state['flipside_tables']
        log(f'setting start_timestamp from {state["start_timestamp"]} -> {start_timestamp}')
        return {'use_decoded_flipside_tables': use_decoded_flipside_tables, 'start_timestamp': start_timestamp, 'flipside_tables': tables, 'completed_tools': ["CheckDecodedFlipsideTables"]}
    else:
        tables = [x for x in state['flipside_tables'] if not 'ez_events_decoded' in x]
        return {'use_decoded_flipside_tables': False, 'flipside_tables': tables, 'completed_tools': ["CheckDecodedFlipsideTables"]}
