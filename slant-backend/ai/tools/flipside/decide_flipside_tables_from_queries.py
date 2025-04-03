import re
import time
import json
import pandas as pd
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from ai.tools.utils.utils import read_schemas
from db.flipside.rag_search_queries import rag_search_queries

def decide_flipside_tables_from_queries(state: JobState) -> JobState:
    start_time = time.time()
    log('\n')
    log('='*20)
    log('\n')
    log('decide_flipside_tables_from_queries starting...')
    queries = pd.DataFrame()
    for analysis in state['analyses']:
        log('analysis')
        log(analysis)
        cur = rag_search_queries(analysis.to_string(), [analysis.project])
        log('cur')
        log(cur)
        queries = pd.concat([queries, cur])
    
    schemas = read_schemas()

    prompt = """
    You are an expert blockchain data analyst specializing in Flipside Crypto's SQL database architecture.

    TASK: 
    Decide whether you can complete the required analyses with the available tables and example queries, or if you need more information from the user to complete the analyses.

    REQUIRED ANALYSES:
    {analyses}

    AVAILABLE FLIPSIDE TABLES:
    {tables}

    EXAMPLE QUERIES:
    {queries}

    OUTPUT FORMAT:
    If you can complete the analyses with the available tables, return an list of table names.
    [
    "solana.schema.table_name",
    "solana.schema.another_table_name",
    ...
    ]
    If you need more information from the user, return an empty list.

    DO NOT include explanations, notes, or any other text - return ONLY the JSON array of table names.
    """

    # Format the prompt with dynamic content
    formatted_prompt = prompt.format(
        analyses=[x.to_string() for x in state['analyses']],
        tables=schemas,
        queries='\n\n'.join(queries.text.apply(lambda x: x[:5000]).tolist())[:35000]
    )
    log('decide_flipside_tables_from_queries formatted_prompt')
    log(formatted_prompt[:10000])
    response = state['llm'].invoke(formatted_prompt).content
    response = re.sub(r'```json', '', response)
    response = re.sub(r'```', '', response)
    log('response')
    log(response)
    j = json.loads(response)
    log('j')
    log(j)
    time_taken = round(time.time() - start_time, 1)
    log(f'decide_flipside_tables_from_queries finished in {time_taken} seconds')
    answer = {
        'flipside_tables': str(j),
        'analyses': [x.to_string() for x in state['analyses']],
    }
    return {'flipside_tables': j, 'flipside_example_queries': queries, 'answer': json.dumps(answer), 'completed_tools': ["DecideFlipsideTablesFromQueries"]}
