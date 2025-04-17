import time
import json
import psycopg
import markdown
import pandas as pd
from utils.utils import log
from langchain_openai import ChatOpenAI
from classes.GraphState import GraphState
from constants.keys import POSTGRES_ENGINE
from ai.tools.slant.news_finder import news_finder
from langgraph.graph import StateGraph, START, END
from ai.tools.utils.print_state import print_state
from utils.memory import PostgresConversationMemory
from ai.tools.utils.tool_selector import tool_selector
from ai.tools.utils.prompt_refiner import prompt_refiner
from constants.keys import ANTHROPIC_API_KEY, OPENAI_API_KEY
from ai.tools.slant.slant_query_writer import slant_query_writer
from ai.tools.twitter.rag_search_tweets import rag_search_tweets
from ai.tools.slant.rag_search_projects import rag_search_projects
from ai.tools.utils.respond_with_context import respond_with_context
from ai.tools.slant.slant_query_executor import slant_query_executor
from ai.tools.utils.format_for_highcharts import format_for_highcharts
from ai.tools.flipside.write_flipside_query import write_flipside_query
from ai.tools.flipside.execute_flipside_query import execute_flipside_query
from ai.tools.twitter.rag_search_twitter_kols import rag_search_twitter_kols
from ai.tools.sharky.answer_with_sharky_preds import answer_with_sharky_preds

def get_upcoming_tool(state: GraphState):
    upcoming_tools = sorted(list(set(state['upcoming_tools'])))
    completed_tools = sorted(list(set(state['completed_tools'])))
    states = [
        'RagSearchTweets',
        'RagSearchProjects',
        'RagSearchTwitterKols',
        'ProjectKolQueryRunner',
        'SlantQueryExecutor',
        'SharkyAgent',
        'DataAnalyst',
        'ExecuteFlipsideQuery',
        'FormatForHighcharts',
        'RespondWithContext',
    ]
    d = {
        'RagSearchTweets': 'Searching Twitter',
        'RagSearchProjects': 'Searching projects',
        'RagSearchTwitterKols': 'Analyzing Twitter accounts',
        'ProjectKolQueryRunner': 'Writing SQL query',
        'SlantQueryExecutor': 'Executing SQL query',
        'SharkyAgent': 'Analyzing Sharky data',
        'DataAnalyst': 'Writing Flipside SQL query',
        'ExecuteFlipsideQuery': 'Executing Flipside SQL query',
        'FormatForHighcharts': 'Generating chart',
        'RespondWithContext': 'Summarizing data',
        'NewsFinder': 'Searching latest news',
    }
    print('get_upcoming_tool')
    print(f'upcoming_tools: {upcoming_tools}')
    print(f'completed_tools: {completed_tools}')
    remaining_tools = list(set([x for x in upcoming_tools if x not in completed_tools]))
    print(f'remaining_tools: {remaining_tools}')
    if state['flipside_sql_error'] and state['flipside_sql_attempts'] < 3:
        return 'Refining Flipside SQL query'
    elif state['flipside_sql_query'] and state['upcoming_tools'][-1] == 'ExecuteFlipsideQuery':
        return 'Executing Flipside SQL query'
    # elif len(remaining_tools) == 0 and len(upcoming_tools) == 1:
    #     return 'Analyzing query'
    # elif len(remaining_tools) == 0:
    #     return 'Summarizing data'
    for s in states:
        if s in remaining_tools:
            return d[s]
    return 'Unknown tool'


# Conditional edges from ToolSelector based on `run_tools`
def tool_selection_logic(state: GraphState):
    """Determines the next nodes based on the selected tools."""
    next_nodes = []
    if "RagSearchTweets" in state["run_tools"]:
        next_nodes.append("RagSearchTweets")
    if "RagSearchProjects" in state["run_tools"]:
        next_nodes.append("RagSearchProjects")
    if "RagSearchTwitterKols" in state["run_tools"]:
        next_nodes.append("RagSearchTwitterKols")
    if "ProjectKolQueryRunner" in state["run_tools"]:
        next_nodes.append("ProjectKolQueryRunner")
    if "DataAnalyst" in state["run_tools"]:
        next_nodes.append("DataAnalyst")
    if "SharkyAgent" in state["run_tools"]:
        next_nodes.append("SharkyAgent")
    if "NewsFinder" in state["run_tools"]:
        next_nodes.append("NewsFinder")

    # If no tools were selected, go to PrintState
    return next_nodes if next_nodes else ["PrintState"]

def flipside_execution_logic(state: GraphState) -> str:
    if state["flipside_sql_attempts"] <= 2 and state["flipside_sql_error"] and not 'QUERY_RUN_TIMEOUT_ERROR' in state["flipside_sql_error"]:
        return "DataAnalyst"
    else:
        return "FormatForHighcharts"

def join_tools_gate(state: GraphState) -> str:
    """
    Determines whether to proceed to RespondWithContext or wait for more tool executions.
    
    Args:
        state (GraphState): The current state of the graph.
    
    Returns:
        str: Next node to execute or None to continue waiting.
    """
    print('Entering join_tools_gate')
    
    # Check if all upcoming tools have been completed
    remaining_tools = list(set(state['upcoming_tools']) - set(state['completed_tools']))
    
    print(f'Remaining tools: {remaining_tools}')
    print(f'Upcoming tools: {sorted(list(set(state["upcoming_tools"])))}')
    print(f'Completed tools: {sorted(list(set(state["completed_tools"])))}')
    
    # If no remaining tools, proceed to RespondWithContext
    if len(remaining_tools) == 0 or (len(remaining_tools) == 1 and remaining_tools[0] == 'RespondWithContext'):
        print('All tools completed. Moving to RespondWithContext.')
        return "RespondWithContext"
    
    # Otherwise, continue waiting
    print('Still waiting for tools to complete.')
    return "JoinTools"

def make_graph():


    # Initialize the graph
    builder = StateGraph(GraphState)

    builder.add_node("ToolSelector", tool_selector)
    builder.add_edge(START, "ToolSelector")

    builder.add_node("RagSearchTweets", rag_search_tweets)
    builder.add_node("NewsFinder", news_finder)
    builder.add_node("RagSearchProjects", rag_search_projects)
    builder.add_node("RagSearchTwitterKols", rag_search_twitter_kols)
    builder.add_node("ProjectKolQueryRunner", slant_query_writer)
    builder.add_node("SlantQueryExecutor", slant_query_executor)
    builder.add_node("DataAnalyst", write_flipside_query)
    builder.add_node("ExecuteFlipsideQuery", execute_flipside_query)
    builder.add_node("SharkyAgent", answer_with_sharky_preds)
    builder.add_node("FormatForHighcharts", format_for_highcharts)
    builder.add_node("PrintState", print_state)
    builder.add_node("JoinTools", lambda state: {})
    builder.add_node("RespondWithContext", respond_with_context)

    builder.add_conditional_edges("ToolSelector", tool_selection_logic)
    tool_nodes = [
        "RagSearchTweets",
        "RagSearchProjects",
        "RagSearchTwitterKols",
        "SharkyAgent",
        "NewsFinder",
    ]
    for node in tool_nodes:
        builder.add_edge(node, "JoinTools")
    builder.add_edge("DataAnalyst", "ExecuteFlipsideQuery")
    builder.add_conditional_edges("ExecuteFlipsideQuery", flipside_execution_logic)
    builder.add_edge("FormatForHighcharts", "JoinTools")

    builder.add_edge("ProjectKolQueryRunner", "SlantQueryExecutor")
    builder.add_edge("SlantQueryExecutor", "JoinTools")

    # builder.add_edge("PrintState", "JoinTools")
    builder.add_conditional_edges("JoinTools", join_tools_gate)
    builder.add_edge("RespondWithContext", "PrintState")
    builder.add_edge("PrintState", END)

    return builder.compile()

def ask_agent(query: str, conversation_id: str):
    # query = 'how many sharky nft loans have been taken in the last 5 days?'
    log('ask_agent')
    start_time = time.time()

    sync_connection = psycopg.connect(POSTGRES_ENGINE)

    memory = PostgresConversationMemory(
        conversation_id=conversation_id,
        sync_connection=sync_connection
    )
    # memory.messages = []
    log('memory')
    log(memory)
    graph = make_graph()
    # chat_history = memory.load_memory_variables({})['chat_history']
    # message = chat_history[-1].content
    # log('chat_history')
    # log(chat_history)

    # query = 'insert into user_prompts (conversation_id, user_id, user_prompt, timestamp) values (gen_random_uuid(), \'123\', \'{query}\', {time.time()})'
    # pg_execute_query(query)

    # llm = ChatAnthropic(
    #     model="claude-3-5-haiku-latest",
    #     # model="claude-3-7-sonnet-latest",
    #     # model="claude-3-5-sonnet-latest",
    #     anthropic_api_key=ANTHROPIC_API_KEY,
    #     temperature=0.01
    # )

    llm = ChatOpenAI(
        model="gpt-4o-mini",
        openai_api_key=OPENAI_API_KEY,
        temperature=0.01,
        # memory=memory
    )

    sql_llm = ChatOpenAI(
        model="gpt-4o",
        openai_api_key=OPENAI_API_KEY,
        temperature=0.0
    )
    # sql_llm = ChatAnthropic(
    #     # model="claude-3-5-haiku-latest",
    #     model="claude-3-7-sonnet-latest",
    #     # model="claude-3-5-sonnet-latest",
    #     anthropic_api_key=ANTHROPIC_API_KEY,
    #     max_tokens=2048 * 2,
    #     temperature=0.0,
    #     # memory=memory
    # )

    state = GraphState(
        query=query
        , clarified_query=query
        , refined_query=query
        , tweets=[]
        , projects=[]
        , kols=[]
        , run_tools=['RespondWithContext']
        , llm=llm
        , response=''
        , sql_query=''
        , sql_query_result=pd.DataFrame()
        , sql_llm=sql_llm
        , flipside_sql_query=''
        , flipside_sql_query_result=pd.DataFrame()
        , flipside_sql_error=None
        , flipside_sql_attempts=0
        , highcharts_config={}
        , error=None
        , sharky_agent_answer=''
        , current_message=''
        , upcoming_tools=[]
        , completed_tools=[]
        , memory=memory
        , conversation_id=conversation_id
        , refined_prompt_for_flipside_sql=''
        , start_timestamp=0
        , news_df=pd.DataFrame()
    )
    clarified_query, refined_query = prompt_refiner(state)
    state.update({'clarified_query': clarified_query, 'refined_query': refined_query})
    message = {
        "status": "Analyzing query",
    }
    val = f"data: {json.dumps(message)}\n\n"
    log('val')
    log(val)
    yield val


    # flipside_sql_query_result = pd.DataFrame(
    #     {
    #         'timestamp': pd.date_range(start='2025-01-01', periods=100),
    #         'value': np.random.randint(0, 100, size=100)
    #     }
    # )
    # col = 'value'

    response = {}
    for chunk in graph.stream(state, stream_mode='values'):
        log('UPDATING STATE')
        response = chunk.get('response')
        if response:
            response['response'] = response
        highcharts_config = chunk.get('highcharts_config')
        if type(highcharts_config) == str:
            highcharts_config = json.loads(highcharts_config)
        if highcharts_config:
            response['highcharts_config'] = highcharts_config
        flipside_sql_query_result = chunk.get('flipside_sql_query_result')
        if len(flipside_sql_query_result):
            x_col = 'timestamp' if 'timestamp' in flipside_sql_query_result.columns else 'category' if 'category' in flipside_sql_query_result.columns else ''
            log(f'x_col: {x_col}')
            # flipside_sql_query_result = flipside_sql_query_result.rename(columns={'timestamp': 'x'})
            # if x_col:
            chart_data = [
                { 'name': col, 'data': flipside_sql_query_result[[x_col, col]].dropna().values.tolist() }
                for col in flipside_sql_query_result.columns if col != x_col
            ] if x_col == 'timestamp' else [
                { 'name': col, 'data': flipside_sql_query_result[[col]].dropna().values.tolist() }
                for col in flipside_sql_query_result.columns if col != x_col
            ]
            response['highcharts_data'] = {
                'x': sorted(flipside_sql_query_result[x_col].unique().tolist()) if x_col else list(range(len(flipside_sql_query_result))),
                'series': chart_data,
                'mode': x_col
            }
            if 'timestamp' in flipside_sql_query_result.columns and 'category' in flipside_sql_query_result.columns:
                log('timestamp and category in flipside_sql_query_result')
                log(highcharts_config)
                log(highcharts_config.keys())
                if 'series' in highcharts_config.keys():
                    categories = flipside_sql_query_result['category'].unique().tolist()
                    columns = sorted(list(set([ x['column'] for x in highcharts_config['series'] ])))
                    print(f'categories: {categories}.')
                    print(f'columns: {columns}.')
                    chart_data = []
                    for cat in categories:
                        for col in columns:
                            cur = flipside_sql_query_result[flipside_sql_query_result['category'] == cat][[x_col, col]].dropna().values.tolist()
                            if len(cur):
                                chart_data.append({ 'name': cat, 'data': cur })
                    # chart_data = [
                    #     { 'name': cat, 'data': flipside_sql_query_result[[col]].dropna().values.tolist() }
                    #     for col in columns for cat in categories
                    # ]
                    log('chart_data')
                    log(chart_data)
                    response['highcharts_data']['series'] = chart_data
            log('highcharts_data')
            log(response['highcharts_data'])
        upcoming_tool = get_upcoming_tool(chunk)
        log(f'upcoming_tool: {upcoming_tool}')
        if not upcoming_tool in ['Unknown tool', 'Analyzing query']:
            message = {
                "status": upcoming_tool,
            }
            val = f"data: {json.dumps(message)}\n\n"
            yield val
    end_time = time.time()
    log('response')
    log(response)

    # val = memory.save_context(
    #     inputs={
    #         'input': query
    #     },
    #     outputs={
    #         'output': response['response']
    #     }
    # )
    # log('memory.save_context')
    # log(val)


    html_output = markdown.markdown(response['response'])
    log('html output created')

    data = {
    }
    if 'highcharts_config' in response and response['highcharts_config']:
        data['highcharts'] = response['highcharts_config']
    if 'highcharts_data' in response and response['highcharts_data']:
        data['highcharts_data'] = response['highcharts_data']

    message = {
        "response": html_output,
        "data": data
    }
    log('Finished!')
    log(message)
    print(f'Time taken: {int(end_time - start_time)} seconds')
    val = f"data: {json.dumps(message)}\n\n"
    log('val')
    log(val)
    yield val
    message = {
        "status": "done",
    }
    val = f"data: {json.dumps(message)}\n\n"
    log('val')
    log(val)
    return val

    # return val
    # return response