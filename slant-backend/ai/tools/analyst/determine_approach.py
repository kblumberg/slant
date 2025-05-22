import time
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from utils.utils import clean_project_tag
from ai.tools.utils.utils import parse_messages, log_llm_call
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from ai.tools.utils.utils import state_to_reference_materials

def determine_approach(state: JobState) -> JobState:
    analysis_description=state['analysis_description']
    curated_tables = '\n'.join(state['curated_tables'])
    raw_tables = '\n'.join(state['raw_tables'])
    reference_materials = state_to_reference_materials(state)
    prompt = f"""
    You are an expert crypto data scientist. Your task is to determine the best approach to analyze the user's analysis goal.

    Broadly speaking, there are two approaches to analyze a user's analysis goal:
    1. Use curated tables that have already parsed the data you need.
    2. Use raw tables and write a custom SQL query to get the data you need.

    The curated tables are optimized for specific use cases and are more efficient, so if possible, you should use them. Otherwise, you should write a custom SQL query.

    ---

    **User Analysis Goal:**
    {analysis_description}
    
    {reference_materials}

    **List of Curated Tables:**
    Here is a list of all the curated tables that are available to you.
    {curated_tables}

    **List of Raw Tables:**
    Here is a list of all the raw tables that are available to you.
    {raw_tables}

    ---

    **Instructions:**
    - Return either "1" or "2" to indicate which approach you should take.
    - Only return "1" if you can write a SQL query that will return the data you need using ONLY the curated tables.
    - If you cannot write a SQL query that will return the data you need using ONLY the curated tables, return "2".
    - Do not include any explanation, justification, or formatting.

    Return only the number.
    """
    approach = log_llm_call(prompt, state['complex_llm'], state['user_message_id'], 'DetermineApproach')
    log(f"approach: {approach}")
    return {'approach': approach, 'completed_tools': ['DetermineApproach']}
