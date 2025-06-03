import time
from utils.utils import log
from classes.JobState import JobState
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from ai.tools.utils.utils import log_llm_call
def pre_query_clarifications(state: JobState) -> JobState:

    start_time = time.time()
    # log('\n')
    # log('='*20)
    # log('\n')
    # log('pre_query_clarifications starting...')

    role_map = {
        "human": "USER",
        "ai": "ASSISTANT",
        "system": "SYSTEM"
    }
    messages = '\n'.join([
        f"{role_map.get(m.type, m.type.upper())}: {m.content}" for m in state['messages']
    ])

    prompt = """
    You are an expert blockchain analyst and prompt engineer.

    Your task is to evaluate the conversation history and decide whether we have enough information to begin writing a blockchain data query, or if we need more clarification or input from the user.

    Please follow this decision logic:

    1. If the user's intent is clear and we have enough details to define filters (e.g., `program_id`, `wallet`, `block_timestamp`, etc.) and expected output (e.g., volume, count, breakdown), return:
    YES

    2. If there is missing or unclear information, return:

    NO: <brief explanation of what additional info is needed>

    Examples:
    - `NO: We need to know which token the user is referring to.`
    - `NO: Unclear which Telegram bots we need to include`

    ---

    **Message History:**
    {messages}

    """

    formatted_prompt = prompt.format(
        messages=messages
    )
    # log('pre_query_clarifications formatted_prompt')
    # log(formatted_prompt)
    response = log_llm_call(formatted_prompt, state['reasoning_llm_anthropic'], state['user_message_id'], 'PreQueryClarifications')
    response = parse_json_from_llm(response, state['llm'], to_json=False)
    # log('pre_query_clarifications response')
    # log(response)
    time_taken = round(time.time() - start_time, 1)
    # log(f'pre_query_clarifications finished in {time_taken} seconds')
    return {'pre_query_clarifications': response, 'completed_tools': ["PreQueryClarifications"]}
