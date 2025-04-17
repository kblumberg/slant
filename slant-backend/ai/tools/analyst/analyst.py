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


def execution_logic(state: JobState) -> str:
    if len(state["follow_up_questions"]) == 0:
        return "CreateAnalysisDescription"
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
    
    print(f'Remaining tools: {remaining_tools}')
    print(f'Upcoming tools: {sorted(list(set(state["upcoming_tools"])))}')
    print(f'Completed tools: {sorted(list(set(state["completed_tools"])))}')
    
    # If no remaining tools, proceed to RespondWithContext
    if len(remaining_tools) == 0:
        print('All tools completed. Moving to RespondWithContext.')
        return "AskFollowUpQuestions"
    
    # Otherwise, continue waiting
    print('Still waiting for tools to complete.')
    return "JoinTools"

def flipside_execution_logic(state: JobState) -> str:
    if state["flipside_sql_attempts"] <= 2 and state["flipside_sql_error"] and not 'QUERY_RUN_TIMEOUT_ERROR' in state["flipside_sql_error"]:
        return "WriteFlipsideQuery"
    else:
        return "FormatForHighcharts"

def make_graph():
    # Initialize the graph
    builder = StateGraph(JobState)

    builder.add_node("ParseAnalyses", parse_analyses)
    builder.add_node("WebSearch", web_search)
    builder.add_node("RagSearchTweets", rag_search_tweets)
    builder.add_node("LoadExampleFlipsideQueries", load_example_flipside_queries)
    builder.add_node("AskFollowUpQuestions", ask_follow_up_questions)
    builder.add_node("HumanInput", human_input)
    builder.add_node("DecideFlipsideTables", decide_flipside_tables)
    builder.add_node("DecideFlipsideTablesFromQueries", decide_flipside_tables_from_queries)
    builder.add_node("PrintJobState", print_job_state)
    builder.add_node("CreateAnalysisDescription", create_analysis_description)
    builder.add_node("RespondWithContext", respond_with_context)
    builder.add_node("PreQueryClarifications", pre_query_clarifications)
    builder.add_node("RagSearchProjects", rag_search_projects)
    builder.add_node("WriteFlipsideQueryOrInvestigateData", write_flipside_query_or_investigate_data)
    builder.add_node("WriteFlipsideQuery", write_flipside_query)
    builder.add_node("ExecuteFlipsideQuery", execute_flipside_query)
    builder.add_node("FormatForHighcharts", format_for_highcharts)
    builder.add_node("JoinTools", lambda state: {})

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
    builder.add_edge("CreateAnalysisDescription", "WriteFlipsideQueryOrInvestigateData")
    builder.add_edge("WriteFlipsideQueryOrInvestigateData", "WriteFlipsideQuery")
    builder.add_edge("WriteFlipsideQuery", "ExecuteFlipsideQuery")
    builder.add_conditional_edges("ExecuteFlipsideQuery", flipside_execution_logic)

    # builder.add_edge("RagSearchTweets", "AskFollowUpQuestions")
    # builder.add_edge("AskFollowUpQuestions", "RespondWithContext")
    builder.add_edge("FormatForHighcharts", "RespondWithContext")
    builder.add_edge("RespondWithContext", "PrintJobState")
    # builder.add_edge("ParseAnalyses", "DecideFlipsideTables")
    # builder.add_conditional_edges("DecideFlipsideTablesFromQueries", execution_logic_2)
    # builder.add_edge("ParseAnalyses", "DecideFlipsideTablesFromQueries")
    # builder.add_edge("DecideFlipsideTablesFromQueries", "PrintJobState")
    builder.add_edge("PrintJobState", END)

    return builder.compile()

def ask_analyst(user_prompt: str, conversation_id: str, user_id: str):
    # query = 'how many sharky nft loans have been taken in the last 5 days?'
    log('ask_analyst')
    start_time = time.time()

    sync_connection = psycopg.connect(POSTGRES_ENGINE)

    memory = PostgresConversationMemory(
        conversation_id=conversation_id,
        sync_connection=sync_connection
    )
    user_message_id = str(uuid.uuid4())
    memory.save_user_message(user_message_id, user_prompt)
    memory.load_messages()
    log('memory')
    log(memory.messages)
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
    resoning_llm = ChatOpenAI(
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
        , flipside_sql_query=''
        , flipside_sql_error=''
        , flipside_sql_attempts=0
        , analyses=[]
        , projects=[]
        , tweets=[]
        , llm=llm
        , complex_llm=complex_llm
        , resoning_llm=resoning_llm
        , memory=memory
        , completed_tools=[]
        , upcoming_tools=['RagSearchTweets','RagSearchProjects','LoadExampleFlipsideQueries','WebSearch']
        , flipside_tables=[]
        , flipside_example_queries=pd.DataFrame()
        , flipside_sql_query_result=pd.DataFrame()
        , web_search_results=''
        , tavily_client=tavily_client
    )
    memory.save_conversation(state)
    message = {
        "status": "Analyzing query",
    }
    val = f"data: {json.dumps(message)}\n\n"
    log('val')
    log(val)
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
                    log('chart_data')
                    log(chart_data)
                    response['highcharts_data']['series'] = chart_data
            log('highcharts_data')
            log(response['highcharts_data'])

        if not upcoming_tool in ['Unknown tool', 'Analyzing query']:
            message = {
                "status": upcoming_tool,
            }
            val = f"data: {json.dumps(message)}\n\n"
            yield val
    end_time = time.time()
    memory.save_agent_message(chunk)
    memory.save_state_snapshot(chunk)
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