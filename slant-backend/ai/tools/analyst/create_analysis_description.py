import re
import time
import json
from utils.utils import log
from classes.JobState import JobState
from ai.tools.utils.utils import parse_messages

def create_analysis_description(state: JobState) -> JobState:

    start_time = time.time()
    # log('\n')
    # log('='*20)
    # log('\n')
    # log('create_analysis_description starting...')

    messages = parse_messages(state)

    prompt = """
    You are an expert blockchain analyst and prompt engineer.

    Your task is to read a message history between a user and an AI assistant and produce a clear, concise summary of the *requested blockchain analysis*. This summary will be used to guide further technical steps (e.g. data queries, report generation, etc.).

    The summary should focus only on what the user ultimately wants analyzed, not on clarifying back-and-forth. Be specific, but concise.

    Make sure to include timeframes if mentioned.

    Ignore small talk or general curiosity—just extract the user’s core analytical request(s).

    Format your answer as a short paragraph.

    ---

    **Message History:**
    {messages}

    ---

    **Summary of Requested Analysis:**
    """

    formatted_prompt = prompt.format(
        messages=messages
    )
    # log('formatted_prompt')
    # log(formatted_prompt)
    response = state['llm'].invoke(formatted_prompt).content
    summary = re.sub(r'```json', '', response)
    summary = re.sub(r'```', '', summary)
    log('create_analysis_description')
    log(summary)
    time_taken = round(time.time() - start_time, 1)
    # log(f'create_analysis_description finished in {time_taken} seconds')
    return {'analysis_description': summary, 'completed_tools': ["CreateAnalysisDescription"]}
