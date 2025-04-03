from utils.utils import log
from classes.JobState import JobState

def print_job_state(state: JobState):
    log('print_job_state')
    # log(state)

    log(f"""

{('='*20)}
Job State:
{('='*20)}

user_prompt:
{state['user_prompt']}

analyses:
{state['analyses']}

flipside_tables:
{state['flipside_tables']}

answer:
{state['answer']}

{('='*20)}

""")
    return {}