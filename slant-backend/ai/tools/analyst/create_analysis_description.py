import time
from utils.utils import log
from classes.JobState import JobState
from ai.tools.utils.utils import parse_messages, log_llm_call
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm

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

    The summary should focus only on what the user ultimately wants answered, not on clarifying back-and-forth. Be specific, but concise.

    Make sure to include timeframes if mentioned.

    Ignore small talk or general curiosity—just extract the user’s core request(s).

    Format your answer as a short paragraph. Be concise and direct. Do not preface with anything like "Here is a summary of the requested analysis:" or "Here is a summary of the user's request:" or anything like that. Just provide the summary.

    Ignore questions from the "USER" that have already been answered by the "ASSISTANT".

    Prioritize the most recent question from the "USER" (end of the message history).

    ---

    **Message History:**
    {messages}

    ---

    **Summary of Requested Question:**
    """

    formatted_prompt = prompt.format(
        messages=messages
    )
    # log('formatted_prompt')
    # log(formatted_prompt)
    response = log_llm_call(formatted_prompt, state['llm'], state['user_message_id'], 'CreateAnalysisDescription')
    summary = parse_json_from_llm(response, state['llm'], to_json=False)
    log('create_analysis_description')
    log(summary)
    time_taken = round(time.time() - start_time, 1)
    # log(f'create_analysis_description finished in {time_taken} seconds')
    return {'analysis_description': summary, 'completed_tools': ["CreateAnalysisDescription"]}
