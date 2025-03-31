import time
import pandas as pd
from utils.utils import log
from utils.db import fs_load_data
from classes.GraphState import GraphState

def execute_flipside_query(state: GraphState) -> GraphState:
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
    log('\n')
    log('='*20)
    log('\n')
    log('execute_flipside_query starting...')
    # log('state:')
    # log(print_sharky_state(sharkyState))
    # # Ensure params is a dictionary
    # if isinstance(params, str):
    #     try:
    #         params = json.loads(params)
    #     except json.JSONDecodeError:
    #         return "Invalid JSON input"
    df, error = fs_load_data(state['flipside_sql_query'])
    log('df.head(3)')
    log(df.head(3))
    if 'date_time' in df.columns:
        df['timestamp'] = pd.to_datetime(df['date_time']).astype(int) // 10**6
        # df['timestamp'] = df['timestamp'].apply(lambda x: str(x)[:19].replace('T', ' ') )
    attempts = state['flipside_sql_attempts'] + 1
    log('execute_flipside_query df')
    log(df)
    time_taken = round(time.time() - start_time, 1)
    log(f'execute_flipside_query finished in {time_taken} seconds')
    # state.update()
    # log('execute_flipside_query state')
    # print_state(state)
    return {'flipside_sql_query_result': df, 'flipside_sql_error': error, 'flipside_sql_attempts': attempts, 'completed_tools': ["ExecuteFlipsideQuery"], 'upcoming_tools': ["FormatForHighcharts"]}
