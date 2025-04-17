import pandas as pd
from utils.utils import log
from classes.JobState import JobState

def get_scale(data: pd.DataFrame, col: str) -> int:
    mx_0 = data[data[col].notna()][col].max()
    mn_0 = data[data[col].notna()][col].min()
    mx = max(mx_0, -mn_0)

    if mx < 1_000:
        return 0
    else:
        return mx / mn

def read_schemas():
    schemas = ''
    with open('ai/context/flipside/schema.sql', 'r') as f:
        schemas = f.read()
    return schemas

def get_refined_prompt(state: JobState):
    projects = list(set([ x.project for x in state['analyses']]))
    activities = list(set([ x.activity for x in state['analyses']]))
    tokens = list(set([token for x in state['analyses'] for token in x.tokens]))
    query = list(set(projects + activities + tokens))
    refined_prompt = ' '.join(query)
    return refined_prompt

def parse_messages(state: JobState):
    role_map = {
        "human": "USER",
        "ai": "ASSISTANT",
        "system": "SYSTEM"
    }
    messages = '\n'.join([
        f"{role_map.get(m.type, m.type.upper())}: {m.content}" for m in state['messages']
    ])
    return messages

def print_tool_starting(current_tool: str):
    log('\n')
    log('='*20)
    log('\n')
    log(f'{current_tool} starting...')