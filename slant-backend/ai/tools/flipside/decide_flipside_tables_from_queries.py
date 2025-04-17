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
    log(state['analyses'])
    queries = pd.DataFrame()
    for analysis in state['analyses']:
        log('analysis')
        log(analysis)
        cur = rag_search_queries(analysis.to_string(), [analysis.project] + analysis.tokens, top_k=40, n_queries=25)
        log('cur')
        log(cur)
        queries = pd.concat([queries, cur])
    
    schemas = read_schemas()

    prompt = """
    You are an expert blockchain data analyst specializing in Flipside Crypto's SQL database architecture.

    TASK: 
    Decide whether you can complete the required analyses with the available tables and example queries, or if you need more information from the user to complete the analyses. If the same tables are used in the example queries, have a preference for that vs what you might think based on the available tables descriptions and columns. If you are not 99% sure on which tables to use, ask the user for more information.

    ORIGINAL USER PROMPT:
    {user_prompt}

    REQUIRED ANALYSES:
    {analyses}

    AVAILABLE FLIPSIDE TABLES:
    {tables}

    EXAMPLE QUERIES:
    {queries}

    OUTPUT FORMAT:
    Return a JSON object with two keys:
    - `proposed_query`: a SQL query that answers the analyses
    - `questions`: an array of questions to ask the user to clarify the analyses


    Example questions you might ask the user:
    - I'm thinking of starting the analysis on YYYY-MM-DD, is that the correct timeframe?
    - Is XXX a token or a project?
    - Is XXX the project you are referring to?

    DO NOT include explanations, notes, or any other text - return ONLY the JSON array of table names or questions.
    """

    # Format the prompt with dynamic content
    queries['text'] = queries.apply(lambda x: f"Tables: {x['tables']}\n\nQuery: {x['text'][:5000]}", axis=1)
    formatted_prompt = prompt.format(
        user_prompt=state['user_prompt'],
        analyses=[x.to_string() for x in state['analyses']],
        tables=schemas,
        queries='\n\n'.join(queries.text.apply(lambda x: x[:10000]).tolist())[:35000]
    )
    log('decide_flipside_tables_from_queries formatted_prompt')
    log(formatted_prompt[-10000:])
    response = state['complex_llm'].invoke(formatted_prompt).content
    response = re.sub(r'```json', '', response)
    response = re.sub(r'```', '', response)
    log('response')
    log(response)
    j = json.loads(response)
    log('j')
    log(j)
    log('proposed_query')
    log(j['proposed_query'])
    log('questions')
    log(j['questions'])
    time_taken = round(time.time() - start_time, 1)
    log(f'decide_flipside_tables_from_queries finished in {time_taken} seconds')
    response = {
        'flipside_tables': str(j),
        'analyses': [x.to_string() for x in state['analyses']],
    }
    return {'flipside_tables': j, 'flipside_example_queries': queries, 'response': json.dumps(response), 'completed_tools': ["DecideFlipsideTablesFromQueries"]}
