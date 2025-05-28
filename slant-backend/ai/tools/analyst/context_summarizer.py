import time
from utils.utils import log
from classes.JobState import JobState
from ai.tools.utils.utils import state_to_reference_materials
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm

def context_summarizer(state: JobState) -> JobState:
    if state['context_summary']:
        return {}
    return {}
    prompt = """
    You are an expert blockchain analyst and prompt engineer.

    Your task is to extract **key information** from the reference materials that are relevant for completing the analysis described below. This includes any relevant technical details, such as:

    - Program IDs  
    - Token mints  
    - Wallet addresses  
    - Flipside tables  
    - Events and dates
    - Notable on-chain patterns or entities  

    ONLY include information that is relevant to the analysis. Ignore any other information.
    
    Focus on extracting **concrete data points** and **relevant metadata**.

    Avoid vague summaries—be specific and precise.

    ---

    **Analysis Description:**
    Here is the analysis that needs to be completed:
    {analysis_description}

    ---

    {reference_materials}

    ---

    **Output Format:**  
    Write at least 1 sentence (a paragraph at most) to summarize your findings.

    Example outputs:
    - "The program id for token loans on PROJECT_NAME is PROGRAM_ID."
    - "The token address for $TOKEN_NAME is MINT."
    - "To get XXXX transactions, use TABLE_NAME on flipside."
    - "The launch date of PROJECT_NAME is YYYY-MM-DD."
    - "The treasury wallet address for PROJECT_NAME is WALLET_ADDRESS."
    - "To get XXXX transactions, use TABLE_NAME on flipside with the following filters: FILTER_1, FILTER_2, FILTER_3."

    INCLUDE ONLY information that is relevant to the analysis. Exclude any other information.

    Make sure to record any wallet addresses, mints, or program ids EXACTLY as they are. Do not change or miss any characters.
    """


    formatted_prompt = prompt.format(
        analysis_description=state['analysis_description'],
        reference_materials=state_to_reference_materials(state, exclude_keys=[])
    )
    response = state['reasoning_llm'].invoke(formatted_prompt).content
    summary = parse_json_from_llm(response, state['llm'], to_json=False)
    log('context_summarizer')
    log(summary)
    return {'context_summary': summary, 'completed_tools': ["ContextSummarizer"]}
