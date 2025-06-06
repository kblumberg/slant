import time
import pandas as pd
from utils.utils import log
from utils.db import fs_load_data
from classes.JobState import JobState
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from ai.tools.utils.utils import log_llm_call

def execute_flipside_query(state: JobState) -> JobState:
    log(f'execute_flipside_query attempt #{state["flipside_sql_attempts"] + 1}...')
    """
    Executes a query on the flipside database.
    Input:
        - sql_query (str).
    Returns:
        - a dictionary with the following keys:
            - sql_query_result: a data frame of the results
            - error: an error if there is one
    """
    start_time = time.time()
    # log('\n')
    # log('='*20)
    # log('\n')
    # log('execute_flipside_query starting...')
    # log('state:')
    # log(print_sharky_state(sharkyState))
    # # Ensure params is a dictionary
    # if isinstance(params, str):
    #     try:
    #         params = json.loads(params)
    #     except json.JSONDecodeError:
    #         return "Invalid JSON input"
    query = state['optimized_flipside_sql_query'] if state['optimized_flipside_sql_query'] else state['verified_flipside_sql_query'] if state['verified_flipside_sql_query'] else state['improved_flipside_sql_query'] if state['improved_flipside_sql_query'] else state['flipside_sql_query']
    df, error, load_time = fs_load_data(query, timeout_minutes=30)
    df = df.dropna()
    # df = df.head(100)
    if error:
        log('execute_flipside_query error')
        log(error)
    # log('df.head(3)')
    # log(df.head(3))
    if 'category' in df.columns:
        df = df[df.category.notnull()]
        df['category'] = df['category'].apply(lambda x: str(x))
    if 'date_time' in df.columns:
        df = df[df.date_time.notnull()]
        df['timestamp'] = (pd.to_datetime(df['date_time']).astype(int) // 10**6).astype(int)
        # df['timestamp'] = df['timestamp'].apply(lambda x: str(x)[:19].replace('T', ' ') )
    if not 'date_time' in df.columns and not 'category' in df.columns:
        cols = [c for c in df.columns if 'time' in c]
        log('No date_time or category column found')
        log(f'Existing columns: {df.columns}')
        if len(cols) == 1:
            df['timestamp'] = (pd.to_datetime(df[cols[0]]).astype(int) // 10**6).astype(int)
            log(f'Added timestamp column from {cols[0]}')
        else:
            prompt = f"""
            You are an expert in parsing data from a dataframe.

            ---

            ## Task
            Find any columns in the dataframe that contains either the timestamp or date of the data OR a column that would indicate any kind of category or tag.

            ---

            ## Inputs
            Here are the first 3 rows of the dataframe:
            {df.head(3).to_markdown()}

            ---

            ## Output
            A valid JSON object with the following keys:
            - "timestamp_column": the name of the column that contains the timestamp or date of the data or an empty string if no timestamp column is found
            - "category_column": the name of the column that contains the category or tag of the data or an empty string if no category column is found

            If no timestamp or category column is found, return an empty string for both keys.

            Return ONLY the raw JSON. No explanations, no comments, no markdown. 

            """
            response = log_llm_call(prompt, state['llm'], state['user_message_id'], 'ParseTimestampAndCategoryColumns')
            response = parse_json_from_llm(response, state['llm'])
            log(f'response: {response}')
            if response['timestamp_column']:
                df['timestamp'] = (pd.to_datetime(df[response['timestamp_column']]).astype(int) // 10**6).astype(int)
                log(f'Added timestamp column from {response["timestamp_column"]}')
            if response['category_column']:
                df['category'] = df[response['category_column']].apply(lambda x: str(x))
                log(f'Added category column from {response["category_column"]}')
        
    attempts = state['flipside_sql_attempts'] + 1
    log('execute_flipside_query df')
    log(df)
    return {
        'flipside_sql_query_result': df
        , 'flipside_sql_queries': [query]
        , 'flipside_sql_errors': [error]
        , 'flipside_sql_query_resuls': [df]
        , 'flipside_sql_error': error
        , 'flipside_sql_attempts': attempts
        , 'completed_tools': ['ExecuteFlipsideQuery']
        , 'upcoming_tools': ['FormatForHighcharts']
    }
