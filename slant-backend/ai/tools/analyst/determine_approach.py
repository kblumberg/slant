import time
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from utils.utils import clean_project_tag
from ai.tools.utils.utils import parse_messages
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from ai.tools.utils.utils import state_to_reference_materials
def determine_approach(state: JobState) -> JobState:
    tokens = list(set([token for x in state['analyses'] for token in x.tokens]))
    projects = list(set([ x.project for x in state['analyses']]))
    example_queries='\n\n'.join(state['flipside_example_queries'].text.tolist())
    analysis_description=state['analysis_description']
    tweets_summary=state['tweets_summary']
    web_search_summary=state['web_search_summary']
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

    **Tokens (Optional):**
    {tokens}

    **Projects (Optional):**
    {projects}

    **Tweet Summary:**
    {tweets_summary}

    **Web Search Summary:**
    {web_search_summary}

    **Example SQL Queries:**
    Here are some example SQL queries that have already been written by other analysts. These queries may or may not be relevant to the user's analysis goal, so it is your job to use the **Query Title**, **Dashboard Title**, **Query Statement** and **Query Summary** to determine if the query is relevant.
    {example_queries}

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
    approach = state['reasoning_llm'].invoke(prompt).content
    log(f"approach: {approach}")
    return {'approach': approach, 'completed_tools': ['DetermineApproach']}
