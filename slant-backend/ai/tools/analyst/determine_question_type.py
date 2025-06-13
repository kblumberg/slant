import time
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from utils.utils import clean_project_tag
from ai.tools.utils.utils import parse_messages, log_llm_call
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from datetime import datetime
from ai.tools.utils.utils import state_to_reference_materials

def determine_question_type(state: JobState) -> JobState:
    prompt = f"""
    You are a crypto analyst assistant. Your job is to classify a user's question into one of two categories:

    - "data" — if the question requires accessing or analyzing on-chain or off-chain crypto data (e.g., token transfers, wallet activity, transaction volume, NFT sales, DEX usage, DAO proposals, metrics over time), typically via SQL or a blockchain data platform.
    - "other" — if the question is general, opinion-based, news-related, conceptual, or not answerable via direct data queries.

    Use this rule of thumb:
    - If the user is asking for specific numbers, metrics, trends, time series, or comparisons based on historical or current data, classify as "data".
    - If the user is asking for explanations, predictions, strategy, opinions, or anything that relies more on external knowledge (like Twitter, news, or experience), classify as "other".

    Be strict: Only return "data" if the answer would require or benefit from a structured dataset or query.

    You can assume what they are asking for is a question about the solana blockchain ecosystem.

    ---

    **User Prompt**:
    {state['analysis_description']}

    ---

    **Output Instructions**:
    Return only one word: "data" or "other"
    """


    question_type = log_llm_call(prompt, state['llm'], state['user_message_id'], 'DetermineQuestionType')
    log(f"question_type: {question_type}")
    return {'question_type': question_type, 'completed_tools': ['DetermineQuestionType']}
