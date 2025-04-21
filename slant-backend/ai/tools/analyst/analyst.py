import time
import json
import psycopg
import pandas as pd
import os
from utils.utils import log
from classes.JobState import JobState
from langchain_openai import ChatOpenAI
from constants.keys import OPENAI_API_KEY, TAVILY_API_KEY, POSTGRES_ENGINE
from langgraph.graph import StateGraph, START, END
from utils.memory import PostgresConversationMemory
from ai.tools.analyst.human_input import human_input
from ai.tools.analyst.parse_analyses import parse_analyses
from ai.tools.utils.print_job_state import print_job_state
from ai.tools.flipside.decide_flipside_tables import decide_flipside_tables
from ai.tools.flipside.decide_flipside_tables_from_queries import decide_flipside_tables_from_queries
from ai.tools.twitter.rag_search_tweets import rag_search_tweets
from ai.tools.web.web_search import web_search
from ai.tools.analyst.ask_follow_up_questions import ask_follow_up_questions
from ai.tools.utils.respond_with_context import respond_with_context
import uuid
from tavily import TavilyClient
import markdown
from ai.tools.analyst.create_analysis_description import create_analysis_description
from ai.tools.analyst.pre_query_clarifications import pre_query_clarifications
from ai.tools.flipside.load_example_flipside_queries import load_example_flipside_queries
from ai.tools.slant.rag_search_projects import rag_search_projects
from ai.tools.flipside.write_flipside_query_or_investigate_data import write_flipside_query_or_investigate_data
from ai.tools.flipside.write_flipside_query import write_flipside_query
from ai.tools.flipside.execute_flipside_query import execute_flipside_query
from ai.tools.utils.format_for_highcharts import format_for_highcharts
from ai.tools.flipside.verify_flipside_query import verify_flipside_query
from ai.tools.flipside.fix_flipside_query import fix_flipside_query
from ai.tools.flipside.improve_flipside_query import improve_flipside_query
from constants.constant import MAX_FLIPSIDE_SQL_ATTEMPTS
from ai.tools.utils.tool_selector import tool_selector
from ai.tools.utils.tool_executor import tool_executor
from ai.tools.analyst.context_saver import context_saver

def execution_logic(state: JobState) -> str:
    log(f'execution_logic:')
    log('follow_up_questions')
    log(state['follow_up_questions'])
    log(f'tried_tools: {state["tried_tools"]}')
    if len(state["follow_up_questions"]) == 0:
        return "CreateAnalysisDescription"
    elif state["tried_tools"] == 0:
        return "ToolSelector"
    else:
        return "RespondWithContext"

def execution_logic_2(state: JobState) -> str:
    if len(state["flipside_tables"]):
        return "PrintJobState"
    else:
        return "HumanInput"

def get_upcoming_tool(state: JobState):
    upcoming_tools = ['ParseAnalyses']
    completed_tools = sorted(list(set(state['completed_tools'])))
    states = [
        'ParseAnalyses',
        'DecideFlipsideTables',
        'RespondWithContext',
    ]
    d = {
        'ParseAnalyses': 'Analyzing query',
        'DecideFlipsideTables': 'Deciding which flipside tables to use',
        'RespondWithContext': 'Summarizing data',
    }
    # print('get_upcoming_tool')
    # print(f'upcoming_tools: {upcoming_tools}')
    # print(f'completed_tools: {completed_tools}')
    remaining_tools = list(set([x for x in upcoming_tools if x not in completed_tools]))
    # print(f'remaining_tools: {remaining_tools}')
    for s in states:
        if s in remaining_tools:
            return d[s]
    return 'Unknown tool'

def join_tools_gate(state: JobState) -> str:
    # Check if all upcoming tools have been completed
    remaining_tools = list(set(state['upcoming_tools']) - set(state['completed_tools']))
    
    # log(f'Remaining tools: {remaining_tools}')
    # log(f'Upcoming tools: {sorted(list(set(state["upcoming_tools"])))}')
    # log(f'Completed tools: {sorted(list(set(state["completed_tools"])))}')
    
    # If no remaining tools, proceed to RespondWithContext
    if len(remaining_tools) == 0:
        # log('All tools completed. Moving to RespondWithContext.')
        return "AskFollowUpQuestions"
    
    # Otherwise, continue waiting
    # log('Still waiting for tools to complete.')
    return "JoinTools"

def post_flipside_execution_logic(state: JobState) -> str:
    if state["flipside_sql_attempts"] < MAX_FLIPSIDE_SQL_ATTEMPTS and state["flipside_sql_error"] and not 'QUERY_RUN_TIMEOUT_ERROR' in str(state["flipside_sql_error"]):
        # there is an error but it's not a timeout error
        # and we have fewer than 3 attempts
        return "FixFlipsideQuery"
    elif (state["flipside_sql_attempts"] >= MAX_FLIPSIDE_SQL_ATTEMPTS and state["flipside_sql_error"]) or 'QUERY_RUN_TIMEOUT_ERROR' in str(state["flipside_sql_error"]):
        # there is an error we cannot fix
        return "RespondWithContext"
    else:
        # we have successfully executed the query
        return "VerifyFlipsideQuery"

def verify_results_logic(state: JobState) -> str:
    if not state["verified_flipside_sql_query"]:
        # the existing query and results are good
        return "FormatForHighcharts"
    elif state["flipside_sql_attempts"] >= MAX_FLIPSIDE_SQL_ATTEMPTS:
        # we've exhausted our attempts
        return "FormatForHighcharts"
    elif state["flipside_sql_error"]:
        # we have an error
        return "FixFlipsideQuery"
    else:
        # we have an updated version of the query
        return "ExecuteFlipsideQuery"

def wrap_node(fn, name=None):
    node_name = name or fn.__name__
    def wrapped(state: JobState):
        log(f"🟡 Entering node: {node_name}")
        start_time = time.time()
        result = fn(state)
        end_time = time.time()
        log(f"✅ Finished node: {node_name} in {end_time - start_time:.2f}s")
        return result
    return wrapped

def make_graph():
    # Initialize the graph
    builder = StateGraph(JobState)

    builder.add_node("ParseAnalyses", wrap_node(parse_analyses))
    builder.add_node("WebSearch", wrap_node(web_search))
    builder.add_node("RagSearchTweets", wrap_node(rag_search_tweets))
    builder.add_node("LoadExampleFlipsideQueries", wrap_node(load_example_flipside_queries))
    builder.add_node("AskFollowUpQuestions", wrap_node(ask_follow_up_questions))
    builder.add_node("ToolSelector", wrap_node(tool_selector))
    builder.add_node("ToolExecutor", wrap_node(tool_executor))
    builder.add_node("HumanInput", wrap_node(human_input))
    builder.add_node("DecideFlipsideTables", wrap_node(decide_flipside_tables))
    builder.add_node("DecideFlipsideTablesFromQueries", wrap_node(decide_flipside_tables_from_queries))
    builder.add_node("PrintJobState", wrap_node(print_job_state))
    builder.add_node("CreateAnalysisDescription", wrap_node(create_analysis_description))
    builder.add_node("RespondWithContext", wrap_node(respond_with_context))
    builder.add_node("PreQueryClarifications", wrap_node(pre_query_clarifications))
    builder.add_node("RagSearchProjects", wrap_node(rag_search_projects))
    builder.add_node("WriteFlipsideQueryOrInvestigateData", wrap_node(write_flipside_query_or_investigate_data))
    builder.add_node("WriteFlipsideQuery", wrap_node(write_flipside_query))
    builder.add_node("FixFlipsideQuery", wrap_node(fix_flipside_query))
    builder.add_node("ExecuteFlipsideQuery", wrap_node(execute_flipside_query))
    builder.add_node("VerifyFlipsideQuery", wrap_node(verify_flipside_query))
    builder.add_node("ImproveFlipsideQuery", wrap_node(improve_flipside_query))
    builder.add_node("FormatForHighcharts", wrap_node(format_for_highcharts))
    builder.add_node("ContextSaver", wrap_node(context_saver))
    builder.add_node("JoinTools", wrap_node(lambda state: {}, name="JoinTools"))

    builder.add_edge(START, "ParseAnalyses")
    builder.add_edge("ParseAnalyses", "RagSearchTweets")
    builder.add_edge("ParseAnalyses", "RagSearchProjects")
    builder.add_edge("ParseAnalyses", "LoadExampleFlipsideQueries")
    builder.add_edge("ParseAnalyses", "WebSearch")

    tool_nodes = [
        "RagSearchTweets",
        "RagSearchProjects",
        "LoadExampleFlipsideQueries",
        "WebSearch",
    ]
    for node in tool_nodes:
        builder.add_edge(node, "JoinTools")

    builder.add_conditional_edges("JoinTools", join_tools_gate)

    builder.add_conditional_edges("AskFollowUpQuestions", execution_logic)
    # builder.add_edge("AskFollowUpQuestions", "CreateAnalysisDescription")
    # builder.add_edge("CreateAnalysisDescription", "ToolSelector")
    builder.add_edge("ToolSelector", "ToolExecutor")
    builder.add_edge("ToolExecutor", "AskFollowUpQuestions")
    builder.add_edge("CreateAnalysisDescription", "WriteFlipsideQueryOrInvestigateData")
    builder.add_edge("WriteFlipsideQueryOrInvestigateData", "WriteFlipsideQuery")
    builder.add_edge("WriteFlipsideQuery", "ImproveFlipsideQuery")
    builder.add_edge("ImproveFlipsideQuery", "ExecuteFlipsideQuery")
    builder.add_conditional_edges("ExecuteFlipsideQuery", post_flipside_execution_logic)
    builder.add_conditional_edges("VerifyFlipsideQuery", verify_results_logic)

    # builder.add_edge("RagSearchTweets", "AskFollowUpQuestions")
    # builder.add_edge("AskFollowUpQuestions", "RespondWithContext")
    builder.add_edge("FormatForHighcharts", "RespondWithContext")
    builder.add_edge("RespondWithContext", "ContextSaver")
    builder.add_edge("ContextSaver", "PrintJobState")
    # builder.add_edge("ParseAnalyses", "DecideFlipsideTables")
    # builder.add_conditional_edges("DecideFlipsideTablesFromQueries", execution_logic_2)
    # builder.add_edge("ParseAnalyses", "DecideFlipsideTablesFromQueries")
    # builder.add_edge("DecideFlipsideTablesFromQueries", "PrintJobState")
    builder.add_edge("PrintJobState", END)

    return builder.compile()

def ask_analyst(user_prompt: str, conversation_id: str, user_id: str):
    # query = 'how many sharky nft loans have been taken in the last 5 days?'
    log(f'ask_analyst: {user_prompt}')
    start_time = time.time()

    sync_connection = psycopg.connect(POSTGRES_ENGINE)

    memory = PostgresConversationMemory(
        conversation_id=conversation_id,
        sync_connection=sync_connection
    )
    user_message_id = str(uuid.uuid4())
    memory.save_user_message(user_message_id, user_prompt)
    memory.load_messages()
    # log('memory')
    # log(memory.messages)
    graph = make_graph()
    llm = ChatOpenAI(
        model="gpt-4o-mini",
        openai_api_key=OPENAI_API_KEY,
        temperature=0.01,
    )
    complex_llm = ChatOpenAI(
        model="gpt-4o",
        openai_api_key=OPENAI_API_KEY,
        temperature=0.01,
    )
    reasoning_llm = ChatOpenAI(
        model="o3-mini",
        # model="o1",
        openai_api_key=OPENAI_API_KEY
    )

    tavily_client = TavilyClient(api_key=TAVILY_API_KEY)

    # load flipside queries from rag db
    current_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(current_dir, "..", "..", "context", "flipside", "schema.sql")
    with open(schema_path, "r") as f:
        schema = f.read()


    state = JobState(
        user_prompt=user_prompt
        , user_message_id=user_message_id
        , user_id=user_id
        , response=''
        , schema=schema
        , pre_query_clarifications=''
        , follow_up_questions=[]
        , highcharts_config={}
        , conversation_id=conversation_id
        , analysis_description=''
        , messages=memory.messages
        , flipside_sql_queries=[]
        , flipside_sql_errors=[]
        , flipside_sql_query_results=[]
        , flipside_sql_query=''
        , improved_flipside_sql_query=''
        , verified_flipside_sql_query=''
        , flipside_sql_error=''
        , flipside_sql_attempts=0
        , tried_tools=0
        , run_tools=[]
        , analyses=[]
        , projects=[]
        , tweets=[]
        , llm=llm
        , complex_llm=complex_llm
        , reasoning_llm=reasoning_llm
        , memory=memory
        , additional_contexts=[]
        , completed_tools=[]
        , upcoming_tools=['RagSearchTweets','RagSearchProjects','LoadExampleFlipsideQueries','WebSearch']
        , flipside_tables=[]
        , flipside_example_queries=pd.DataFrame()
        , flipside_sql_query_result=pd.DataFrame()
        , web_search_results=''
        , tavily_client=tavily_client
        , context_summary=''
    )
    memory.save_conversation(state)
    message = {
        "status": "Analyzing query",
    }
    val = f"data: {json.dumps(message)}\n\n"
    # log('val')
    # log(val)
    yield val

    response = {}
    chunk = None
    for chunk in graph.stream(state, stream_mode='values'):
        # log('UPDATING STATE')
        message = chunk.get('response')
        if message:
            response['response'] = message
        upcoming_tool = get_upcoming_tool(chunk)

        highcharts_config = chunk.get('highcharts_config')
        if type(highcharts_config) == str:
            highcharts_config = json.loads(highcharts_config)
        if highcharts_config:
            response['highcharts_config'] = highcharts_config
        flipside_sql_query_result = chunk.get('flipside_sql_query_result')
        if len(flipside_sql_query_result):
            x_col = 'timestamp' if 'timestamp' in flipside_sql_query_result.columns else 'category' if 'category' in flipside_sql_query_result.columns else ''
            # log(f'x_col: {x_col}')
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
                # log('timestamp and category in flipside_sql_query_result')
                # log(highcharts_config)
                # log(highcharts_config.keys())
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
                    # log('chart_data')
                    # log(chart_data)
                    response['highcharts_data']['series'] = chart_data
            # log('highcharts_data')
            # log(response['highcharts_data'])

        if not upcoming_tool in ['Unknown tool', 'Analyzing query']:
            message = {
                "status": upcoming_tool,
            }
            val = f"data: {json.dumps(message)}\n\n"
            yield val
    end_time = time.time()
    memory.save_agent_message(chunk)
    memory.save_state_snapshot(chunk)
    # log('response')
    # log(response)

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
    # log('html output created')

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
    # log('Finished!')
    # log(message)
    print(f'Time taken: {int(end_time - start_time)} seconds')
    val = f"data: {json.dumps(message)}\n\n"
    # log('val')
    # log(val)
    yield val
    message = {
        "status": "done",
    }
    val = f"data: {json.dumps(message)}\n\n"
    # log('val')
    # log(val)
    return val

    # return val
    # return response