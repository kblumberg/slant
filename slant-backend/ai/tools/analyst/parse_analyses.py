import re
import time
import json
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from utils.utils import clean_project_tag

def parse_analyses(state: JobState) -> JobState:
    """
    Parses the required analyses from the user prompt.
    Input:
        - user_prompt (str).
    Returns:
        - a list of tables to use
    """
    start_time = time.time()
    log('\n')
    log('='*20)
    log('\n')
    log('parse_analyses starting...')
    prompt = """
    You are an expert blockchain analyst specialized in extracting structured data from user queries.

    TASK: Parse the user prompt and identify specific analysis requests for blockchain metrics.

    USER PROMPT:
    {user_prompt}

    EXTRACTION RULES:
    1. Extract ALL instances of metrics and projects
    2. For the time period, just identify the overall start and end times
    3. For each analysis request, identify:
    - metric: The specific data point to analyze (e.g., price, volume, market cap, transactions, TVL, holders)
    - activity: The specific activity to analyze (e.g., nft buy, swap, stake, etc.)
    - project: The blockchain/cryptocurrency project name (e.g., Bitcoin, Ethereum, Solana)
    - time period: Convert to Unix timestamps
        - If specific dates are mentioned (e.g., "Jan 1, 2023"), convert to Unix timestamp
        - If relative time is mentioned (e.g., "last 7 days", "past month"), calculate from current time
        - If no time period specified, default to "0"

    TIME CONVERSION GUIDELINES:
    - Current timestamp: {current_timestamp}
    - Common periods:
    - "last 24 hours" = current_timestamp - 86400
    - "last week" = current_timestamp - 604800
    - "last month" = current_timestamp - 2592000
    - "last year" = current_timestamp - 31536000
    - "YTD" (Year-to-Date) = start of current year to current_timestamp

    OUTPUT FORMAT:
    Return ONLY a valid JSON array where each element represents one analysis request with the following structure:
    [
    {{
        "metric": "string",
        "activity": "string",
        "project": "string",
        "start_time": integer,
        "end_time": integer
    }},
    ...
    ]

    EXAMPLES:
    User: "Compare the number of unique wallets that bought NFTs on Magic Eden and Tensor over the last month"
    Output: [
    {{"metric": "unique wallets", "activity": "nft buy", "project": "Magic Eden", "start_time": 1638316800, "end_time": 1640995200}},
    {{"metric": "unique wallets", "activity": "nft buy", "project": "Tensor", "start_time": 1638316800, "end_time": 1640995200}}
    ]

    User: "Show me Solana transaction volume and TVL since January"
    Output: [
    {{"metric": "transaction volume", "activity": "transaction", "project": "Solana", "start_time": 1640995200, "end_time": 1672531200}},
    {{"metric": "TVL", "activity": "stake,unstake", "project": "Solana", "start_time": 1640995200, "end_time": 1672531200}}
    ]
    """.format(
        user_prompt=state['user_prompt'],
        current_timestamp=int(time.time())  # Gets current Unix timestamp
    )
    # log('prompt')
    # log(prompt)
    response = state['llm'].invoke(prompt).content
    response = re.sub(r'```json', '', response)
    response = re.sub(r'```', '', response)
    log('response')
    log(response)
    j = json.loads(response)
    log('j')
    log(j)
    analyses = []
    for analysis in j:
        project = clean_project_tag(analysis['project'])
        analysis['project'] = project if len(project) > 0 else ''
        analyses.append(Analysis(**analysis))
    log('analyses')
    log(analyses)
    time_taken = round(time.time() - start_time, 1)
    log(f'parse_analyses finished in {time_taken} seconds')
    return {'analyses': analyses, 'answer': '\n'.join([analysis.to_string() for analysis in analyses]), 'completed_tools': ["ParseAnalyses"]}
