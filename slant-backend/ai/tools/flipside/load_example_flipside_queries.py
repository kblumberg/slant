import re
import time
import json
import pandas as pd
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from ai.tools.utils.utils import read_schemas
from db.flipside.rag_search_queries import rag_search_queries
from ai.tools.utils.utils import remove_sql_comments

def load_example_flipside_queries(state: JobState) -> JobState:
    tokens = list(set([token for x in state['analyses'] for token in x.tokens]))
    projects = list(set([ x.project for x in state['analyses']]))
    n_queries = 10 if len(state['flipside_example_queries']) > 0 else 15
    log(f"load_example_flipside_queries")
    log(f"tokens: {tokens}")
    log(f"projects: {projects}")
    log(f"state['analysis_description']: {state['analysis_description']}")
    queries = rag_search_queries(state['analysis_description'], tokens + projects, top_k=40, n_queries=n_queries)
    queries['text'] = queries.text.apply(lambda x: remove_sql_comments(x))
    return {'flipside_example_queries': queries, 'completed_tools': ['LoadExampleFlipsideQueries']}
