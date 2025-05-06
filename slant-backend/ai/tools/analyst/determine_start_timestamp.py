import time
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from utils.utils import clean_project_tag
from ai.tools.utils.utils import parse_messages
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm

def determine_start_timestamp(state: JobState) -> JobState:
    example_queries='\n\n'.join(state['flipside_example_queries'].text.tolist())
    analysis_description=state['analysis_description']
    tweets_summary=state['tweets_summary']
    web_search_summary=state['web_search_summary']
    prompt = f"""
    You are a crypto data assistant. Your task is to determine the appropriate **start date** to filter data for a SQL query, based on the user's analysis goal.

    Return the date in **YYYY-MM-DD** format. If the analysis requires data from the beginning (e.g. cumulative metrics or no date is specified), return **"0"** instead.

    Use these principles:
    - If the user specifies a relative time period (e.g. "last 30 days", "past week"), subtract that duration from today and return the corresponding date.
    - If the user is asking for cumulative or "current" values (e.g. "total number of users", "current stakers", "TVL growth over time"), return **"0"**.
    - If the user gives no time period default to **"0"**.

    Today's date is: **{datetime.now().strftime("%Y-%m-%d")}**

    ---

    **User Analysis Goal**:
    {analysis_description}

    **Tweet Summary**:
    {tweets_summary}

    **Web Search Summary**:
    {web_search_summary}

    **Example SQL Queries**:
    Use the titles, summaries, and query text to guide your understanding. These may or may not be relevant.
    {example_queries}

    ---

    **Output Instructions**:
    - Return a single line: either a date in **YYYY-MM-DD** format or the string **"0"**
    - Do not return any explanation or formatting—just the date or **"0"**
    """

    start_timestamp = state['complex_llm'].invoke(prompt).content
    log(f"start_timestamp: {start_timestamp}")
    return {'start_timestamp': start_timestamp, 'completed_tools': ['DetermineStartTimestamp']}
