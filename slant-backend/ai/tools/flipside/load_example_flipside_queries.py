import re
import time
import json
import pandas as pd
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from ai.tools.utils.utils import read_schemas
from db.flipside.rag_search_queries import rag_search_queries

def load_example_flipside_queries(state: JobState) -> JobState:
    start_time = time.time()
    # log('\n')
    # log('='*20)
    # log('\n')
    # log('load_example_flipside_queries starting...')
    # log(state['analyses'])
    tokens = list(set([token for x in state['analyses'] for token in x.tokens]))
    projects = list(set([ x.project for x in state['analyses']]))
    queries = rag_search_queries(state['analysis_description'], tokens + projects, top_k=40, n_queries=12)
    # for analysis in state['analyses']:
    #     # log('analysis')
    #     # log(analysis)
    #     cur = rag_search_queries(analysis.to_string(), [analysis.project] + analysis.tokens, top_k=40, n_queries=12)
    #     # log('cur')
    #     # log(cur)
    #     queries = pd.concat([queries, cur])
    # queries = queries.drop_duplicates(subset=['query_id'])

    return {'flipside_example_queries': queries, 'completed_tools': ['LoadExampleFlipsideQueries']}
