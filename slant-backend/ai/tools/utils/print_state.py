from utils.utils import log
from classes.GraphState import GraphState

def print_state(state: GraphState):
    # log('print_state')
    # log(state)

    log(f"""

{('='*20)}
Graph State:
{('='*20)}

query:
{state['query']}

tweets:
{state['tweets']}

projects:
{state['projects']}

kols:
{state['kols']}

run_tools:
{state['run_tools']}

response:
{state['response']}

sql_query:
{state['sql_query']}

sql_query_result:
{state['sql_query_result']}

flipside_sql_query:
{state['flipside_sql_query']}

flipside_sql_query_result:
{state['flipside_sql_query_result']}

flipside_sql_error:
{state['flipside_sql_error']}

highcharts_config:
{state['highcharts_config']}
error: {state['error']}

flipside_sql_error:
{state['flipside_sql_error']}

highcharts_config:
{state['highcharts_config']}

upcoming_tools:
{sorted(list(set(state['upcoming_tools'])))}

completed_tools:
{sorted(list(set(state['completed_tools'])))}

error: {state['error']}

{('='*20)}

""")
    return {}