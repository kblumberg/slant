import re
import time
import json
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from utils.utils import clean_project_tag
from ai.tools.utils.utils import parse_messages

def parse_tx_filters(state: JobState) -> JobState:
    start_time = time.time()
    # log('\n')
    # log('='*20)
    # log('\n')
    # log('parse_tx_filters starting...')
    messages = parse_messages(state)
    prompt = """
    You are an expert blockchain analyst specialized in extracting structured data from blocks of text.

    TASK: Parse the analysis description and identify specific transaction filters that we will use to filter the transactions.

    ANALYSIS DESCRIPTION:
    {analysis_description}

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
        - If no time period specified, default to "0" for start_time and end_time

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
        "tokens": ["string"],
        "project": "string",
        "start_time": integer,
        "end_time": integer
    }},
    ...
    ]

    EXAMPLES:
    User: "The user requests an analysis of the unique wallets that have engaged in any token lending or borrowing transactions using Sharkyfi's new token lending product since its launch on April 12, 2025, up to the current date. The analysis should include both lenders and borrowers, and all asset types should be considered. Test accounts should be included, and while the same program ID is used for both token and NFT lending, the user indicates that the distinction can be made through different instructions or logs."
    Output: [
    "Sharkyfi token loan lend", "Sharkyfi token loan borrow"
    ]

    User: "Show me the unique purchasers of $BONK over the last 30 days"
    Output: [
    "$BONK buy"
    ]

    User: "Show me the NFT sales volume on Magic Eden vs Tensor over the last 30 days"
    Output: [
    "NFT purchase on Magic Eden", "NFT purchase on Tensor"
    ]

    User: "Show me Solana transaction volume and TVL since January"
    Output: [
    {{"metric": "transaction volume", "activity": "transaction", "project": "Solana", "tokens": [], "start_time": 1640995200, "end_time": 1672531200}},
    {{"metric": "TVL", "activity": "stake,unstake", "project": "Solana", "tokens": [], "start_time": 1640995200, "end_time": 1672531200}}
    ]

    User: "Show me the unique purchasers of $BONK over the last 30 days"
    Output: [
    {{"metric": "unique purchasers", "activity": "token buy", "project": "", "tokens": ["BONK"], "start_time": 1640995200, "end_time": 1672531200}},
    ]
    """.format(
        messages=messages,
        current_timestamp=int(time.time())  # Gets current Unix timestamp
    )
    # log('prompt')
    # log(prompt)
    response = state['llm'].invoke(prompt).content
    response = re.sub(r'```json', '', response)
    response = re.sub(r'```', '', response)
    j = json.loads(response)
    analyses = []
    for analysis in j:
        project = clean_project_tag(analysis['project'])
        analysis['project'] = project if len(project) > 0 else ''
        analyses.append(Analysis(**analysis))
    # log('parse_tx_filters output')
    # log(analyses)
    time_taken = round(time.time() - start_time, 1)
    # log(f'parse_tx_filters finished in {time_taken} seconds')
    return {'analyses': analyses, 'response': '\n'.join([analysis.to_string() for analysis in analyses]), 'completed_tools': ["ParseAnalyses"]}
