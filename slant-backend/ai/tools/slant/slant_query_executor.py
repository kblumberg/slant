import time
from utils.utils import log
from utils.db import pg_load_data
from classes.GraphState import GraphState

def slant_query_executor(state: GraphState) -> GraphState:
    """
    Executes a query on a postgres database.
    Input: a sql query (str).
    The search will return a result (str).
    """
    start_time = time.time()
    log('\n')
    log('='*20)
    log('\n')
    log('slant_query_executor starting...')
    # log(f'params: {params}')
    # Ensure params is a dictionary
    # if isinstance(params, str):
    #     try:
    #         params = json.loads(params)
    #     except json.JSONDecodeError:
    #         return "Invalid JSON input"
    df = pg_load_data(state['sql_query'], 15)
    log('df')
    log(df)
    sql_query_result = '\n'.join(('\n'+'='*20+'\n').join([str(x) for x in df.to_dict('records')]).split('\n'))
    time_taken = round(time.time() - start_time, 1)
    log(f'slant_query_executor finished in {time_taken} seconds')
    return {'sql_query_result': sql_query_result, 'completed_tools': ["SlantQueryExecutor"], 'upcoming_tools': ["RespondWithContext"]}
