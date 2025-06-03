from utils.utils import log
from classes.JobState import JobState
from ai.tools.flipside.optimize_flipside_query import flipside_optimize_query_fn

def flipside_optimize_query(state: JobState) -> JobState:

    sql_query = state['verified_flipside_sql_query'] if state['verified_flipside_sql_query'] else state['improved_flipside_sql_query'] if state['improved_flipside_sql_query'] else state['flipside_sql_query']
    sql_query = flipside_optimize_query_fn(state, sql_query)
    return {'optimized_flipside_sql_query': sql_query, 'completed_tools': ["FlipsideOptimizeQuery"], 'upcoming_tools': ["ExecuteFlipsideQuery"]}