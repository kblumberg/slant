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
from utils.db import fs_load_data

def flipside_execute_investigation_queries(state: JobState) -> JobState:

    investigations = []
    for investigation in state['flipside_investigations']:
        if investigation['load_time'] > 0:
            log(f"investigation already loaded: {investigation['query']}")
            investigations.append(investigation)
            continue
        df, error, load_time = fs_load_data(investigation['query'], timeout_minutes=1)
        df = df.dropna()
        log(f"investigation loaded: {investigation['query']}")
        log(f"length of df: {len(df)}")
        log(f"load_time: {load_time}")
        log(df.head(3))
        investigations.append({
            'query': investigation['query'],
            'result': df,
            'error': error,
            'load_time': load_time
        })
    return {'flipside_investigations': investigations, 'completed_tools': ["FlipsideExecuteInvestigationQueries"]}

