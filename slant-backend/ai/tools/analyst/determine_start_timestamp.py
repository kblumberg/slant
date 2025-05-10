import time
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from utils.utils import clean_project_tag
from ai.tools.utils.utils import parse_messages
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from datetime import datetime
from ai.tools.utils.utils import state_to_reference_materials

def determine_start_timestamp(state: JobState) -> JobState:
    reference_materials = state_to_reference_materials(state, include_keys=['flipside_example_queries',''])
    prompt = f"""
    You are a crypto data assistant. Your task is to determine the appropriate **start date** to filter data for a SQL query, based on the user's analysis goal and the reference materials provided.

    Return the date in **YYYY-MM-DD** format. If the analysis requires data from the beginning (e.g. cumulative metrics or no date is specified), return **"0"** instead.

    Use these principles:
    - If the user gives no time period default to **"0"**.
    - If the user is asking for a cumulative value that will require data from the start of the project or launch (e.g. "Show me the growth in TVL for the last 30 days", "How many current stakers are there?"), return the **date the project was launched**. If there is a launch date, use a 2 day buffer (e.g. if the launch date is 2025-01-01, return 2024-12-30). If you cannot find the launch date, return **"0"**.
    - If the user specifies a relative time period (e.g. "last 30 days", "past week"), subtract that duration from today and return the corresponding date.

    Today's date is: **{datetime.now().strftime("%Y-%m-%d")}**

    ---

    **User Analysis Goal**:
    {state['analysis_description']}

    {reference_materials}

    ---

    **Output Instructions**:
    - Return a single line: either a date in **YYYY-MM-DD** format or the string **"0"**
    - Do not return any explanation or formatting—just the date or **"0"**
    """

    start_timestamp = state['complex_llm'].invoke(prompt).content
    log(f"start_timestamp: {start_timestamp}")
    if start_timestamp == "0":
        start_timestamp = "2021-01-01"
    try:
        start_timestamp = str(datetime.strptime(start_timestamp, "%Y-%m-%d"))[:10]
    except:
        start_timestamp = "2021-01-01"
    return {'start_timestamp': start_timestamp, 'completed_tools': ['DetermineStartTimestamp']}
