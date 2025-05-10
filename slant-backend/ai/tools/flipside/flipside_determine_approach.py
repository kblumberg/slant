import re
import time
import json
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from ai.tools.utils.utils import read_schemas
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from ai.tools.utils.utils import state_to_reference_materials

def flipside_determine_approach(state: JobState) -> JobState:
    reference_materials = state_to_reference_materials(state)
    raw_tables = '\n -'.join(state['raw_tables'])
    curated_tables = '\n -'.join(state['curated_tables'])
    excluded_tables = '\n -'.join(['solana.core.ez_events_decoded'])

    table_text = "You MUST use the `solana.core.ez_events_decoded` table in your query. You may use other tables in addition to it if needed."
    if state['approach'] == '1':
        table_text = "You must use at least one of the following tables: -" + curated_tables
    else:
        table_text = f"""
            You must use at least one of the following tables:
              - {raw_tables}

              You must NOT use any of the following tables:
              - {excluded_tables}
        """
    
    prompt = f"""
    You are an expert crypto data scientist trained in the Flipside SQL database. Your task is to determine the best approach to analyze the user's analysis goal using raw tables. This output will be used to write a SQL query to analyze the user's analysis goal.

    {table_text}

    Use the context below to help determine the best approach. Some of the context may or may not be relevant to the user's goal. Ignore the irrelevant context, while using the relevant context to determine the best approach.

    ---

    ## User Analysis Goal
    {state['analysis_description']}

    {reference_materials}

    ---

    **Instructions:**
    - Return a summary of the approach you will take to analyze the user's analysis goal using raw tables.
    - Use the query descriptions and SQL content to guide your selection.
    - Synergize all the context above to determine the best approach.
    - Include things like:
      - the tables you will use
      - the joins you will make
      - the filters you will apply to the tables
      - the logic you will use to write the query
      - the structure of the query (e.g., CTEs, final query)
      - any important optimizations you will make to the query

    Return only the summary.
    """
    flipside_determine_approach = state['llm'].invoke(prompt).content
    log(f"flipside_determine_approach: {flipside_determine_approach}")
    return {'flipside_determine_approach': flipside_determine_approach, 'completed_tools': ['FlipsideDetermineApproachUsingRawTables']}
