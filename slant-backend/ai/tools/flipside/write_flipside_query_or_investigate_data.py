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
from ai.tools.utils.utils import state_to_reference_materials

def write_flipside_query_or_investigate_data(state: JobState) -> JobState:
    start_time = time.time()
    # log('\n')
    # log('='*20)
    # log('\n')
    # log('write_flipside_query_or_investigate_data starting...')
    reference_materials = state_to_reference_materials(state)
    # Create prompt template
    prompt = f"""
        You are an expert in writing accurate, efficient, and idiomatic Snowflake SQL queries for blockchain analytics using the Flipside database.

        ---

        ## Task

        Determine whether you have enough information to write a valid SQL query to answer the following user question. Do you know what SQL query to write, what tables to use, and what filters to apply? Do you need to investigate the data further to confirm your understanding or confirm that the data is available?

        If you do not have enough information, describe what tables you would need to query and examine to get the information you need.

        ### ❓ Question:
        Here is the user question that you are trying to answer:
        {state['analysis_description']}

        ---

        {reference_materials}

        ---

        ## ✍️ Output

        If you have enough information and are 90%+ confident that you know what SQL query to write, what tables to use, and what filters to apply, respond with a "YES". Do not include any other text.

        If you do not have enough information, respond with "NO". Do not include any other text.

    """

    response = state['reasoning_llm'].invoke(prompt).content

    # Remove SQL code block markers if present
    time_taken = round(time.time() - start_time, 1)
    # log(f'write_flipside_query_or_investigate_data finished in {time_taken} seconds')
    log(f"write_flipside_query_or_investigate_data response:")
    log(response)
    return {'write_flipside_query_or_investigate_data': response, 'completed_tools': ["WriteFlipsideQueryOrInvestigateData"]}