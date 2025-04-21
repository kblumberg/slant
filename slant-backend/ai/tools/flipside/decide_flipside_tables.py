import re
import time
import json
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from ai.tools.utils.utils import read_schemas

def decide_flipside_tables(state: JobState) -> JobState:
    start_time = time.time()
    # log('\n')
    # log('='*20)
    # log('\n')
    # log('decide_flipside_tables starting...')
    schemas = read_schemas()
    prompt = """
    You are an expert blockchain data analyst specializing in Flipside Crypto's SQL database architecture.

    TASK: 
    Decide whether you can complete the required analyses with the available tables, or if you need more information from the user to complete the analyses.

    REQUIRED ANALYSES:
    {analyses}

    AVAILABLE FLIPSIDE TABLES:
    {tables}

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
        tables=schemas
    )
    response = state['llm'].invoke(formatted_prompt).content
    response = re.sub(r'```json', '', response)
    response = re.sub(r'```', '', response)
    # log('response')
    # log(response)
    j = json.loads(response)
    # log('j')
    # log(j)
    time_taken = round(time.time() - start_time, 1)
    # log(f'decide_flipside_tables finished in {time_taken} seconds')
    response = {
        'flipside_tables': str(j),
        'analyses': [x.to_string() for x in state['analyses']],
    }
    return {'flipside_tables': j, 'response': json.dumps(response), 'completed_tools': ["DecideFlipsideTables"]}
