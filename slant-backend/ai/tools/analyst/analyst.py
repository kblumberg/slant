import time
import json
import psycopg
import pandas as pd
from utils.utils import log
from classes.JobState import JobState
from langchain_openai import ChatOpenAI
from constants.keys import OPENAI_API_KEY
from constants.keys import POSTGRES_ENGINE
from langgraph.graph import StateGraph, START, END
from utils.memory import PostgresConversationMemory
from ai.tools.analyst.human_input import human_input
from ai.tools.analyst.parse_analyses import parse_analyses
from ai.tools.utils.print_job_state import print_job_state
from ai.tools.flipside.decide_flipside_tables import decide_flipside_tables
from ai.tools.flipside.decide_flipside_tables_from_queries import decide_flipside_tables_from_queries
from langchain_core.runnables import RunnableLambda


def execution_logic(state: JobState) -> str:
    if len(state["flipside_tables"]):
        return "PrintJobState"
    else:
        return "DecideFlipsideTablesFromQueries"

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
        'AnswerWithContext',
    ]
    d = {
        'ParseAnalyses': 'Analyzing query',
        'DecideFlipsideTables': 'Deciding which flipside tables to use',
        'AnswerWithContext': 'Summarizing data',
    }
    print('get_upcoming_tool')
    print(f'upcoming_tools: {upcoming_tools}')
    print(f'completed_tools: {completed_tools}')
    remaining_tools = list(set([x for x in upcoming_tools if x not in completed_tools]))
    print(f'remaining_tools: {remaining_tools}')
    for s in states:
        if s in remaining_tools:
            return d[s]
    return 'Unknown tool'

def make_graph():
    # Initialize the graph
    builder = StateGraph(JobState)

    builder.add_node("ParseAnalyses", parse_analyses)
    builder.add_node("HumanInput", human_input)
    builder.add_node("DecideFlipsideTables", decide_flipside_tables)
    builder.add_node("DecideFlipsideTablesFromQueries", decide_flipside_tables_from_queries)
    builder.add_node("PrintJobState", print_job_state)

    builder.add_edge(START, "ParseAnalyses")
    builder.add_edge("ParseAnalyses", "DecideFlipsideTables")
    builder.add_conditional_edges("DecideFlipsideTables", execution_logic)
    builder.add_conditional_edges("DecideFlipsideTablesFromQueries", execution_logic_2)
    # builder.add_edge("DecideFlipsideTables", "PrintJobState")
    builder.add_edge("PrintJobState", END)

    return builder.compile(interrupt_before=["HumanInput"])

def ask_analyst(query: str, session_id: str):
    # query = 'how many sharky nft loans have been taken in the last 5 days?'
    log('ask_analyst')
    start_time = time.time()

    sync_connection = psycopg.connect(POSTGRES_ENGINE)
    session_id = '2ba67ea8-5458-4ce8-8e5c-98352b5e4bbe'

    memory = PostgresConversationMemory(
        session_id=session_id,
        sync_connection=sync_connection
    )
    log('memory')
    log(memory)
    graph = make_graph()
    llm = ChatOpenAI(
        model="gpt-4o-mini",
        openai_api_key=OPENAI_API_KEY,
        temperature=0.01,
    )

    state = JobState(
        user_prompt=query
        , answer=''
        , user_id=session_id
        , session_id=session_id
        , messages=[]
        , analyses=[]
        , llm=llm
        , memory=memory
        , completed_tools=[]
        , flipside_tables=[]
        , flipside_example_queries=pd.DataFrame()
    )
    message = {
        "status": "Analyzing query",
    }
    val = f"data: {json.dumps(message)}\n\n"
    log('val')
    log(val)
    yield val

    response = {}
    for chunk in graph.stream(state, stream_mode='values'):
        log('UPDATING STATE')
        answer = chunk.get('answer')
        if answer:
            response['answer'] = answer
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

    val = memory.save_context(
        inputs={
            'input': query
        },
        outputs={
            'output': response['answer']
        }
    )
    log('memory.save_context')
    log(val)

    # html_output = markdown.markdown(response['answer'])
    # log('html output created')

    data = {
    }
    message = {
        "response": response['answer'],
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