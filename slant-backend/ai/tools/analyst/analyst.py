import os
import uuid
import time
import json
import psycopg
import markdown
import pandas as pd
from utils.utils import log
from datetime import datetime
from tavily import TavilyClient
from classes.JobState import JobState
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from constants.keys import OPENAI_API_KEY, TAVILY_API_KEY, POSTGRES_ENGINE, ANTHROPIC_API_KEY
from langgraph.graph import StateGraph, START, END
from utils.memory import PostgresConversationMemory
from ai.tools.analyst.human_input import human_input
from ai.tools.analyst.parse_analyses import parse_analyses
from ai.tools.utils.print_job_state import print_job_state
from ai.tools.flipside.decide_flipside_tables import decide_flipside_tables
from ai.tools.twitter.rag_search_tweets import rag_search_tweets
from ai.tools.web.web_search import web_search
from ai.tools.analyst.ask_follow_up_questions import ask_follow_up_questions
from ai.tools.utils.respond_with_context import respond_with_context
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
from ai.tools.flipside.flipside_optimize_query import flipside_optimize_query
from constants.constant import MAX_FLIPSIDE_SQL_ATTEMPTS
from ai.tools.utils.tool_selector import tool_selector
from ai.tools.utils.tool_executor import tool_executor
from ai.tools.analyst.context_summarizer import context_summarizer
from ai.tools.analyst.extract_transactions import extract_transactions
from ai.tools.analyst.web_search_documentation import web_search_documentation
from ai.tools.twitter.summarize_tweets import summarize_tweets
from ai.tools.web.summarize_web_search import summarize_web_search
from ai.tools.flipside.refine_flipside_query_prompt import refine_flipside_query_prompt
from ai.tools.analyst.extract_program_ids import extract_program_ids
from ai.tools.analyst.determine_approach import determine_approach
from ai.tools.analyst.determine_start_timestamp import determine_start_timestamp
from ai.tools.flipside.check_decoded_flipside_tables import check_decoded_flipside_tables
from ai.tools.flipside.flipside_determine_approach import flipside_determine_approach
from ai.tools.flipside.flipside_check_query_subset import flipside_check_query_subset
from ai.tools.flipside.flipside_subset_example_queries import flipside_subset_example_queries
from ai.tools.utils.utils import get_flipside_schema_data
from ai.tools.flipside.flipside_write_investigation_queries import flipside_write_investigation_queries
from ai.tools.flipside.flipside_execute_investigation_queries import flipside_execute_investigation_queries
from ai.tools.flipside.flipside_basic_table_selection import flipside_basic_table_selection
from ai.tools.flipside.flipside_tables_from_example_queries import flipside_tables_from_example_queries
from io import StringIO

def follow_up_questions_logic(state: JobState) -> str:
    log(f'follow_up_questions_logic:')
    log('follow_up_questions')
    log(state['follow_up_questions'])
    log(f'tried_tools: {state["tried_tools"]}')
    if len(state["follow_up_questions"]) == 0:
        # no follow up questions
        # begin query process
        return "ExtractProgramIds"
    elif state["tried_tools"] == 0:
        # use the tools to answer the question
        return "ToolSelector"
    else:
        return "RespondWithContext"

def post_check_decoded_flipside_tables_logic(state: JobState) -> str:
    log(f'post_check_decoded_flipside_tables_logic:')
    log(f'use_decoded_flipside_tables: {state["use_decoded_flipside_tables"]}')
    if state['use_decoded_flipside_tables']:
        return "WriteFlipsideQuery"
    else:
        return "DecideFlipsideTables"

def post_flipside_execute_investigation_queries_logic(state: JobState) -> str:
    log(f'post_flipside_execute_investigation_queries_logic:')
    # first time go to DetermineApproach, otherwise go to VerifyFlipsideQuery
    if state['approach'] == '':
        return "DetermineApproach"
    else:
        return "VerifyFlipsideQuery"

def post_write_flipside_query_logic(state: JobState) -> str:
    log(f'post_write_flipside_query_logic:')
    start_timestamp = datetime.strptime(state['start_timestamp'], '%Y-%m-%d')
    total_days = (datetime.now() - start_timestamp).days
    log(f'total_days: {total_days}')
    log(f'approach: {state["approach"]}')
    log(f'use_decoded_flipside_tables: {state["use_decoded_flipside_tables"]}')
    if total_days >= 33 and state['approach'] == '2' and not state['use_decoded_flipside_tables']:
        return "FlipsideCheckQuerySubset"
    else:
        return "FlipsideOptimizeQuery"

def post_determine_approach_logic(state: JobState) -> str:
    log(f'post_determine_approach_logic:')
    log(f'approach: {state["approach"]}')
    if state['approach'] == '1':
        return "FlipsideDetermineApproach"
    else:
        return "ExtractProgramIds"

def context_summarizer_logic(state: JobState) -> str:
    # log(f'context_summarizer_logic:')
    # log('context_summary')
    # log(state['context_summary'])
    if len(state["follow_up_questions"]) == 0:
        return "ExtractProgramIds"
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
        'WebSearch',
        'RagSearchTweets',
        'RagSearchProjects',
        'ExtractTransactions',
        'LoadExampleFlipsideQueries',
        'WriteFlipsideQueryOrInvestigateData',
        'AskFollowUpQuestions',
        'ToolSelector',
        'ToolExecutor',
        'PrintJobState',
        'CreateAnalysisDescription',
        'RespondWithContext',
        'WriteFlipsideQuery',
        'FixFlipsideQuery',
        'ExecuteFlipsideQuery',
        'VerifyFlipsideQuery',
        'ImproveFlipsideQuery',
        'FormatForHighcharts',
        'ContextSummarizer',
        
    ]
    d = {
        'AskFollowUpQuestions': 'AskFollowUpQuestions',
        'ParseAnalyses': 'Analyzing query',
        'WebSearch': 'Searching the web',
        'RagSearchTweets': 'Searching twitter',
        'RagSearchProjects': 'Loading project data',
        'LoadExampleFlipsideQueries': 'Analyzing SQL query examples',
        'WriteFlipsideQueryOrInvestigateData': 'Analyzing additional needs',
        'WriteFlipsideQuery': 'Writing SQL query',
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
            return d[s] if s in d.keys() else s
    return 'Unknown tool'

def join_tools_gate_3(state: JobState) -> str:
    remaining_tools = list(set(state['upcoming_tools']) - set(state['completed_tools']))
    if len(remaining_tools) == 0:
        return "JoinTools4"
    return "JoinTools3"

def join_tools_gate_5(state: JobState) -> str:
    upcoming_tools = ['SummarizeWebSearch','SummarizeTweets']
    completed_tools = sorted(list(set(state['completed_tools'])))
    remaining_tools = list(set(upcoming_tools) - set(completed_tools))
    if len(remaining_tools) == 0:
        return "AskFollowUpQuestions"
    return "JoinTools5"

def join_tools_gate_1(state: JobState) -> str:
    remaining_tools = set(['ParseAnalyses','CreateAnalysisDescription']) - set(state['completed_tools'])
    if len(remaining_tools) == 0:
        return "JoinTools2"
    return "JoinTools1"

def post_flipside_execution_logic(state: JobState) -> str:
    log('post_flipside_execution_logic')

    numerical_columns = state['flipside_sql_query_result'].select_dtypes(include=['number']).columns.tolist()
    # cur = pd.DataFrame([{'category': 'a', 'num': 1}, {'category': 'b', 'num': 2}])
    # numerical_columns = cur.select_dtypes(include=['number']).columns.tolist()
    tot = 0
    for col in numerical_columns:
        if not col in ['timestamp']:
            tot += state['flipside_sql_query_result'][col].sum()
    no_results = len(state["flipside_sql_query_result"]) == 0 or tot == 0
    log(f'no_results: {no_results}')
    if state["flipside_sql_attempts"] < MAX_FLIPSIDE_SQL_ATTEMPTS and state["flipside_sql_error"] and not 'QUERY_RUN_TIMEOUT_ERROR' in str(state["flipside_sql_error"]):
        # there is an error but it's not a timeout error
        # and we have not yet exhausted our attempts
        log('there is an error but it\'s not a timeout error and we have not yet exhausted our attempts')
        return "FixFlipsideQuery"
    elif state["flipside_sql_attempts"] < MAX_FLIPSIDE_SQL_ATTEMPTS and not state["flipside_sql_error"] and no_results:
        # there is no error and no results
        # and we have not yet exhausted our attempts
        log('there is no error and no results and we have not yet exhausted our attempts')
        return "FlipsideDetermineApproach"
    elif (state["flipside_sql_attempts"] >= MAX_FLIPSIDE_SQL_ATTEMPTS and state["flipside_sql_error"]) or 'QUERY_RUN_TIMEOUT_ERROR' in str(state["flipside_sql_error"]):
        # there is an error we cannot fix
        log('there is an error we cannot fix')
        return "RespondWithContext"
    else:
        # we have successfully executed the query
        log('we have successfully executed the query')
        return "VerifyFlipsideQuery"

def verify_results_logic(state: JobState) -> str:
    log(f'verify_results_logic:')
    log(f'state["flipside_sql_error"]: {state["flipside_sql_error"]}')
    log(f'state["verified_flipside_sql_query"]: {state["verified_flipside_sql_query"]}')
    log(f'state["flipside_sql_attempts"]: {state["flipside_sql_attempts"]}')
    if state["verified_flipside_sql_query"].strip() == '' and len(state["flipside_sql_query_result"]) > 0 and state["flipside_sql_attempts"] > 0:
        # the existing query and results are good
        log('returning FormatForHighcharts')
        return "FormatForHighcharts"
    elif state["flipside_sql_attempts"] >= MAX_FLIPSIDE_SQL_ATTEMPTS:
        # we've exhausted our attempts
        log('returning FormatForHighcharts')
        return "FormatForHighcharts"
    elif state["flipside_sql_error"]:
        # we have an error
        log('returning FixFlipsideQuery')
        return "FixFlipsideQuery"
    else:
        # we have an updated version of the query
        log('returning FlipsideOptimizeQuery')
        return "FlipsideOptimizeQuery"

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
    builder.add_node("SummarizeTweets", wrap_node(summarize_tweets))
    builder.add_node("SummarizeWebSearch", wrap_node(summarize_web_search))
    builder.add_node("WebSearchDocumentation", wrap_node(web_search_documentation))
    builder.add_node("WebSearch", wrap_node(web_search))
    builder.add_node("RagSearchTweets", wrap_node(rag_search_tweets))
    builder.add_node("LoadExampleFlipsideQueries", wrap_node(load_example_flipside_queries))
    builder.add_node("AskFollowUpQuestions", wrap_node(ask_follow_up_questions))
    builder.add_node("ToolSelector", wrap_node(tool_selector))
    builder.add_node("ToolExecutor", wrap_node(tool_executor))
    builder.add_node("HumanInput", wrap_node(human_input))
    builder.add_node("DecideFlipsideTables", wrap_node(decide_flipside_tables))
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
    builder.add_node("ExtractProgramIds", wrap_node(extract_program_ids))
    builder.add_node("DetermineApproach", wrap_node(determine_approach))
    builder.add_node("FlipsideDetermineApproach", wrap_node(flipside_determine_approach))
    # TODO: we are hitting this multiple times
    builder.add_node("FlipsideSubsetExampleQueries", wrap_node(flipside_subset_example_queries))
    builder.add_node("FlipsideWriteInvestigationQueries", wrap_node(flipside_write_investigation_queries))
    builder.add_node("FlipsideExecuteInvestigationQueries", wrap_node(flipside_execute_investigation_queries))
    # builder.add_node("WriteFlipsideQueryOrInvestigateData", wrap_node(lambda state: {}))
    # builder.add_node("VerifyFlipsideQuery", wrap_node(lambda state: {}))
    builder.add_node("ImproveFlipsideQuery", wrap_node(improve_flipside_query))
    builder.add_node("FormatForHighcharts", wrap_node(format_for_highcharts))
    builder.add_node("ContextSummarizer", wrap_node(context_summarizer))
    builder.add_node("ExtractTransactions", wrap_node(extract_transactions))
    builder.add_node("RefineFlipsideQueryPrompt", wrap_node(refine_flipside_query_prompt))
    builder.add_node("DetermineStartTimestamp", wrap_node(determine_start_timestamp))
    builder.add_node("CheckDecodedFlipsideTables", wrap_node(check_decoded_flipside_tables))
    builder.add_node("FlipsideCheckQuerySubset", wrap_node(flipside_check_query_subset))
    builder.add_node("FlipsideOptimizeQuery", wrap_node(flipside_optimize_query))
    builder.add_node("FlipsideBasicTableSelection", wrap_node(flipside_basic_table_selection))
    builder.add_node("FlipsideTablesFromExampleQueries", wrap_node(flipside_tables_from_example_queries))
    builder.add_node("JoinTools1", wrap_node(lambda state: {}, name="JoinTools1"))
    builder.add_node("JoinTools2", wrap_node(lambda state: {}, name="JoinTools2"))
    builder.add_node("JoinTools3", wrap_node(lambda state: {}, name="JoinTools3"))
    builder.add_node("JoinTools4", wrap_node(lambda state: {}, name="JoinTools4"))
    builder.add_node("JoinTools5", wrap_node(lambda state: {}, name="JoinTools5"))

    builder.add_edge(START, "ParseAnalyses")
    builder.add_edge(START, "CreateAnalysisDescription")
    builder.add_edge("JoinTools1", "JoinTools2")



    tool_nodes = [
        "ParseAnalyses",
        "CreateAnalysisDescription",
    ]
    for node in tool_nodes:
        builder.add_edge(node, "JoinTools1")

    tool_nodes = [
        "RagSearchTweets",
        "RagSearchProjects",
        "LoadExampleFlipsideQueries",
        "WebSearch",
        "ExtractTransactions",
    ]
    for node in tool_nodes:
        builder.add_edge("JoinTools2", node)

    builder.add_edge("LoadExampleFlipsideQueries", "RefineFlipsideQueryPrompt")
    tool_nodes = [
        "RagSearchTweets",
        "RagSearchProjects",
        "RefineFlipsideQueryPrompt",
        "WebSearch",
        "ExtractTransactions",
        "CreateAnalysisDescription",
    ]
    for node in tool_nodes:
        builder.add_edge(node, "JoinTools3")

    builder.add_conditional_edges("JoinTools1", join_tools_gate_1)
    builder.add_conditional_edges("JoinTools3", join_tools_gate_3)

    tool_nodes = [
        "SummarizeWebSearch",
        "SummarizeTweets",
        "FlipsideBasicTableSelection",
        "FlipsideTablesFromExampleQueries",
    ]
    for node in tool_nodes:
        builder.add_edge("JoinTools4", node)
        builder.add_edge(node, "JoinTools5")
    builder.add_conditional_edges("JoinTools5", join_tools_gate_5)
    builder.add_conditional_edges("AskFollowUpQuestions", follow_up_questions_logic)
    builder.add_conditional_edges("ContextSummarizer", context_summarizer_logic)
    # builder.add_edge("AskFollowUpQuestions", "CreateAnalysisDescription")
    # builder.add_edge("CreateAnalysisDescription", "ToolSelector")
    builder.add_edge("ToolSelector", "ToolExecutor")
    builder.add_edge("ToolExecutor", "AskFollowUpQuestions")
    # builder.add_edge("AskFollowUpQuestions", "WriteFlipsideQueryOrInvestigateData")
    # builder.add_edge("WriteFlipsideQueryOrInvestigateData", "DetermineApproach")
    builder.add_edge("ExtractProgramIds", "DetermineStartTimestamp")
    builder.add_edge("DetermineStartTimestamp", "CheckDecodedFlipsideTables")
    builder.add_conditional_edges("CheckDecodedFlipsideTables", post_check_decoded_flipside_tables_logic)
    # builder.add_edge("FlipsideSubsetExampleQueries", "DecideFlipsideTables")
    builder.add_edge("DecideFlipsideTables", "FlipsideSubsetExampleQueries")
    builder.add_edge("FlipsideSubsetExampleQueries", "FlipsideWriteInvestigationQueries")
    builder.add_edge("FlipsideWriteInvestigationQueries", "FlipsideExecuteInvestigationQueries")
    builder.add_conditional_edges("FlipsideExecuteInvestigationQueries", post_flipside_execute_investigation_queries_logic)
    # builder.add_edge("FlipsideExecuteInvestigationQueries", "DetermineApproach")
    # builder.add_conditional_edges("DetermineApproach", post_determine_approach_logic)
    builder.add_edge("DetermineApproach", "FlipsideDetermineApproach")
    builder.add_edge("FlipsideDetermineApproach", "WriteFlipsideQuery")
    builder.add_edge("WriteFlipsideQuery", "FlipsideWriteInvestigationQueries")
    # builder.add_conditional_edges("FlipsideWriteInvestigationQueries", "FlipsideExecuteInvestigationQueries")
    # builder.add_conditional_edges("FlipsideExecuteInvestigationQueries", post_write_flipside_query_logic)
    # builder.add_conditional_edges("WriteFlipsideQuery", post_write_flipside_query_logic)
    builder.add_edge("FlipsideCheckQuerySubset", "FlipsideOptimizeQuery")
    # builder.add_edge("ImproveFlipsideQuery", "ExecuteFlipsideQuery")
    builder.add_edge("FixFlipsideQuery", "FlipsideOptimizeQuery")
    builder.add_edge("FlipsideOptimizeQuery", "ExecuteFlipsideQuery")
    builder.add_conditional_edges("ExecuteFlipsideQuery", post_flipside_execution_logic)
    builder.add_conditional_edges("VerifyFlipsideQuery", verify_results_logic)

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

def get_values_from_prev_state(prev_state: pd.DataFrame, key: str, default_value: any):
    return prev_state[key].values[0] if len(prev_state) and key in prev_state.columns else default_value

def ask_analyst(user_prompt: str, conversation_id: str, user_id: str):
    # query = 'how many sharky nft loans have been taken in the last 5 days?'
    log(f'ask_analyst: {user_prompt}')
    start_time = time.time()
    message = {
        "status": "done",
    }
    val = f"data: {json.dumps(message)}\n\n"

    sync_connection = psycopg.connect(POSTGRES_ENGINE)

    memory = PostgresConversationMemory(
        conversation_id=conversation_id,
        sync_connection=sync_connection
    )
    user_message_id = str(uuid.uuid4())
    memory.save_user_message(user_message_id, user_prompt)
    memory.load_messages()
    prev_state = memory.load_previous_state()
    log('prev_state')
    log(prev_state)
    flipside_sql_query_result = get_values_from_prev_state(prev_state, 'flipside_sql_query_result', [])
    flipside_sql_query_result = json.loads(str(flipside_sql_query_result))
    log('flipside_sql_query_result')
    log(flipside_sql_query_result)
    if len(flipside_sql_query_result):
        flipside_sql_query_result = pd.DataFrame(flipside_sql_query_result)
        log('flipside_sql_query_result')
        log(flipside_sql_query_result)
    else:
        flipside_sql_query_result = pd.DataFrame()

    # log('memory')
    # log(memory.messages)
    graph = make_graph()
    llm = ChatOpenAI(
        model="gpt-4.1-mini",
        openai_api_key=OPENAI_API_KEY,
        temperature=0.00,
    )
    complex_llm = ChatOpenAI(
        model="gpt-4.1",
        openai_api_key=OPENAI_API_KEY,
        temperature=0.00,
    )
    reasoning_llm_openai = ChatOpenAI(
        model="o4-mini",
        openai_api_key=OPENAI_API_KEY,
        # temperature=0.00,
    )
    reasoning_llm_anthropic = ChatAnthropic(
        # model="gpt-4.1",
        model="claude-opus-4-20250514",
        # model="o1",
        max_tokens=4096,
        anthropic_api_key=ANTHROPIC_API_KEY
    )
    # reasoning_llm_anthropic = ChatOpenAI(
    #     # model="gpt-4.1",
    #     model="o4-mini",
    #     # model="o1",
    #     openai_api_key=OPENAI_API_KEY
    # )

    tavily_client = TavilyClient(api_key=TAVILY_API_KEY)

    # load flipside queries from rag db
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    # schema_path = os.path.join(current_dir, "..", "..", "context", "flipside", "schema.sql")
    # with open(schema_path, "r") as f:
    #     schema = f.read()
    schema = get_flipside_schema_data()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    flipside_tables = pd.read_csv(os.path.join(current_dir, "..", "..", "..", "data", "flipside_tables.csv"))

    curated_tables = flipside_tables[flipside_tables['curated'] == 1]['table'].tolist()
    raw_tables = flipside_tables[flipside_tables['curated'] == 0]['table'].tolist()


    state = JobState(
        user_prompt=user_prompt
        , user_message_id=user_message_id
        , user_id=user_id
        , response=''
        , schema=schema
        , pre_query_clarifications=''
        , follow_up_questions=[]
        , highcharts_configs=[]
        , conversation_id=conversation_id
        , analysis_description=''
        , messages=memory.messages
        , write_flipside_query_or_investigate_data=''
        , flipside_basic_table_selection=[]
        , flipside_tables_from_example_queries=[]
        , flipside_investigations=[]
        , investigation_flipside_sql_queries=[]
        , investigation_flipside_sql_errors=[]
        , investigation_flipside_sql_query_results=[]
        , flipside_sql_queries=[]
        , flipside_sql_errors=[]
        , flipside_sql_query_results=[]
        , flipside_sql_query=''
        , flipside_sql_feedback=''
        , improved_flipside_sql_query=''
        , verified_flipside_sql_query=''
        , optimized_flipside_sql_query=''
        , flipside_sql_error=''
        , flipside_sql_attempts=0
        , tried_tools=0
        , run_tools=[]
        , transactions=[]
        , analyses=[]
        , projects=[]
        , tweets=[]
        , llm=llm
        , complex_llm=complex_llm
        , reasoning_llm_anthropic=reasoning_llm_anthropic
        , reasoning_llm_openai=reasoning_llm_openai
        , memory=memory
        , additional_contexts=[]
        , additional_context_summary=''
        , completed_tools=[]
        , upcoming_tools=['RagSearchTweets','RagSearchProjects','LoadExampleFlipsideQueries','WebSearch','RefineFlipsideQueryPrompt','ExtractTransactions','CreateAnalysisDescription']
        , flipside_tables=[]
        , flipside_example_queries=[]
        , flipside_sql_query_result=flipside_sql_query_result
        , web_search_results= get_values_from_prev_state(prev_state, 'web_search_results', '')
        , tavily_client=tavily_client
        , context_summary= get_values_from_prev_state(prev_state, 'context_summary', '')
        , tweets_summary= get_values_from_prev_state(prev_state, 'tweets_summary', '')
        , web_search_summary= get_values_from_prev_state(prev_state, 'web_search_summary', '')
        , curated_tables=curated_tables
        , raw_tables=raw_tables
        , approach=''
        , start_timestamp='2021-01-01'
        , program_ids=[]
        , use_decoded_flipside_tables=False
        , flipside_determine_approach=''
        , eta=0
        , flipside_subset_example_queries=[]
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
    for chunk in graph.stream(state, stream_mode='values', config={'recursion_limit': 50}):
        # log('UPDATING STATE')
        message = chunk.get('response')
        if message:
            response['response'] = message
        upcoming_tool = get_upcoming_tool(chunk)


        # save sql query to response
        verified_flipside_sql_query = chunk.get('verified_flipside_sql_query')
        flipside_sql_query = chunk.get('flipside_sql_query')
        optimized_flipside_sql_query = chunk.get('optimized_flipside_sql_query')
        query = optimized_flipside_sql_query if optimized_flipside_sql_query else verified_flipside_sql_query if verified_flipside_sql_query else flipside_sql_query
        response['flipside_sql_query'] = query

        # save data to response
        flipside_sql_query_result = chunk.get('flipside_sql_query_result')
        if len(flipside_sql_query_result):
            csv_buffer = StringIO()
            flipside_sql_query_result.to_csv(csv_buffer, index=False)
            csv_string = csv_buffer.getvalue()
            response['flipside_sql_query_result'] = csv_string

        # save highcharts configs to response
        highcharts_configs = chunk.get('highcharts_configs')
        if type(highcharts_configs) == str:
            highcharts_configs = json.loads(highcharts_configs)
        if highcharts_configs:
            response['highcharts_configs'] = highcharts_configs
        flipside_sql_query_result = chunk.get('flipside_sql_query_result')
        response['highcharts_datas'] = []
        if len(flipside_sql_query_result):
            # if len(flipside_sql_query_result):
            #     log('flipside_sql_query_result 426')
            #     log(flipside_sql_query_result)
            #     log('flipside_sql_query_result.columns')
            #     log(flipside_sql_query_result.columns)
            log(f'len(highcharts_configs): {len(highcharts_configs)}')
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
            highcharts_data = {
                'x': sorted(flipside_sql_query_result[x_col].unique().tolist()) if x_col else list(range(len(flipside_sql_query_result))),
                'series': chart_data,
                'mode': x_col
            }

            # if ('timestamp' in flipside_sql_query_result.columns and 'category' in flipside_sql_query_result.columns) or len(highcharts_configs) > 1:
                # log('timestamp and category in flipside_sql_query_result')
                # log(highcharts_config.keys())
            highcharts_datas = []
            new_highcharts_configs = []
            for highcharts_config in highcharts_configs:
                log('highcharts_config')
                log(highcharts_config)
                chart_data = []
                if 'series' in highcharts_config.keys():
                    for series in highcharts_config['series']:
                        log('series')
                        log(series)
                        cur = flipside_sql_query_result.copy()
                        numerical_columns = cur.select_dtypes(include=['number']).columns.tolist()
                        log(f'numerical_columns: {numerical_columns}.')
                        for c in numerical_columns:
                            mn = cur[c].min()
                            if mn >= 1000:
                                cur[c] = cur[c].apply(lambda x: round(x)).astype(int)
                            elif mn >= 100:
                                cur[c] = cur[c].apply(lambda x: round(x, 1)).astype(float)
                            elif mn >= 1:
                                cur[c] = cur[c].apply(lambda x: round(x, 2)).astype(float)
                            elif mn >= 0.1:
                                cur[c] = cur[c].apply(lambda x: round(x, 3)).astype(float)
                            elif mn >= 0.01:
                                cur[c] = cur[c].apply(lambda x: round(x, 4)).astype(float)
                        if 'category' in cur.columns:
                            cur['category_clean'] = cur['category'].apply(lambda x: x.lower().strip())
                        categories = cur['category'].unique().tolist() if 'category' in cur.columns else []
                        log(f'categories: {categories}.')
                        column = series['column']
                        log(f'column: {column}.')

                        filter_column = series['filter_column'] if 'filter_column' in series.keys() else ''
                        log(f'filter_column: {filter_column}.')
                        filter_value = series['filter_value'] if 'filter_value' in series.keys() else ''
                        log(f'filter_value: {filter_value}.')

                        is_timestamp = 1 if 'timestamp' in flipside_sql_query_result.columns else 0
                        is_filter_column = 1 if filter_column else 0
                        is_category = 1 if len(categories) else 0
                        chart_type = 'x_time_y_value'
                        if is_timestamp:
                            if is_category and is_filter_column:
                                chart_type = 'x_timestamp_y_value_z_category_filter'
                            elif is_category:
                                chart_type = 'x_timestamp_y_value_z_category'
                        else:
                            if is_filter_column:
                                chart_type = 'x_category_y_value_z_filter'
                            else:
                                chart_type = 'x_category_y_value'
                        log(f'chart_type: {chart_type}.')

                        x_col = 'timestamp' if is_timestamp else 'category'
                        log(f'x_col: {x_col}.')

                        if chart_type == 'x_time_y_value':
                            chart_data = cur[[x_col, column]].dropna().values.tolist()
                        elif chart_type == 'x_timestamp_y_value_z_category_filter':
                            # timestamp and no filter
                            cur = cur[cur[filter_column] == filter_value]
                            # for cat in categories:
                            cur_subset = cur[cur['category_clean'] == series['name'].lower().strip()][[x_col, column]].dropna().values.tolist()
                            if len(cur_subset):
                                # chart_data.append({ 'name': series['name'], 'data': cur_subset })
                                chart_data = cur_subset
                        elif chart_type == 'x_timestamp_y_value_z_category':
                            # timestamp and no filter
                            # for cat in categories:
                            cur_subset = cur[cur['category_clean'] == series['name'].lower().strip()][[x_col, column]].dropna().values.tolist()
                            if len(cur_subset):
                                # chart_data.append({ 'name': series['name'], 'data': cur_subset })
                                chart_data = cur_subset
                        elif chart_type == 'x_category_y_value':
                            # timestamp and no filter
                            if series['name'] in categories:
                                cur = cur[cur['category_clean'] == series['name'].lower().strip()]
                            chart_data = cur[[x_col, column]].dropna().values.tolist()
                        elif chart_type == 'x_category_y_value_z_filter':
                            # timestamp and no filter
                            cur = cur[cur[filter_column] == filter_value]
                            chart_data = cur[[x_col, column]].dropna().values.tolist()
                        log('chart_data')
                        log(chart_data)
                        series['data'] = chart_data
                        # if filter_column and filter_value and filter_column in cur.columns and not filter_value in categories:
                        #     # if there are categories and a filter
                        #     cur = cur[cur[filter_column] == filter_value]
                        #     for cat in categories:
                        #         cur_subset = cur[cur['category'] == cat][[x_col, column]].dropna().values.tolist()
                        #         log(f'cur_subset for {cat}')
                        #         log(cur_subset)
                        #         if 'timestamp' in flipside_sql_query_result.columns:
                        #             cur_subset = cur[cur['category'] == cat][[x_col, column]].dropna().values.tolist()
                        #             if len(cur_subset):
                        #                 chart_data.append({ 'name': series['name'], 'data': cur_subset })
                        #         else:
                        #             assert len(cur_subset) == 1
                        #             chart_data = cur_data + cur_subset
                        # else:
                        #     # if there is just categories but no filter
                        #     for cat in categories:
                        #         cur_subset = cur[cur['category'] == cat][[x_col, column]].dropna().values.tolist()
                        #         if len(cur_subset):
                        #             chart_data.append({ 'name': cat, 'data': cur_subset })
                        # log('chart_data')
                        # log(chart_data)
                #     highcharts_data = {
                #         'x': sorted(flipside_sql_query_result[x_col].unique().tolist()) if x_col else list(range(len(flipside_sql_query_result))),
                #         'series': chart_data,
                #         'mode': x_col
                #     }
                # highcharts_datas.append({
                #     'x': sorted(cur[x_col].unique().tolist()) if x_col else list(range(len(cur))),
                #     'series': chart_data,
                #     'mode': x_col
                # })
            # else:
            #     response['highcharts_datas'] = [highcharts_data]
            # log('highcharts_datas')
            # log(response['highcharts_datas'])
            # response['highcharts_datas'] = highcharts_datas
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

    data = response
    if 'highcharts_configs' in response and response['highcharts_configs']:
        highcharts_configs = response['highcharts_configs']
        for highcharts_config in highcharts_configs:
            for series in highcharts_config['series']:
                for c in ['column','filter_column','filter_value']:
                    if c in series.keys():
                        del series[c]
        log('highcharts_configs')
        log(highcharts_configs)
        data['highcharts'] = highcharts_configs
    # if 'highcharts_datas' in response and response['highcharts_datas']:
    #     data['highcharts_datas'] = response['highcharts_datas']
    if 'highcharts_configs' in data:
        del data['highcharts_configs']
    log('returning data')
    log(json.dumps(data, indent=2))
    message = {
        "response": html_output,
        "data": data
    }
    log('message')
    log(message)
    print(f'Time taken: {int(end_time - start_time)} seconds')
    val = f"data: {json.dumps(message)}\n\n"
    log('yield val 475')
    log(val)
    yield val
    message = {
        "status": "done",
    }
    val = f"data: {json.dumps(message)}\n\n"
    log('return val 482')
    log(val)
    yield val

    # return val
    # return response