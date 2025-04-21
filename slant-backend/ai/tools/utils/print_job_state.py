from utils.utils import log
from classes.JobState import JobState

def print_job_state(state: JobState):
    # log('print_job_state')
    # log(state)
    analyses = '\n'.join([str(analysis) for analysis in state['analyses']])
    log(f"""

{('='*20)}
Job State:
{('='*20)}

user_prompt:
{state['user_prompt']}

analyses:
{analyses}

flipside_tables:
{state['flipside_tables']}

response:
{state['response']}

{('='*20)}

""")
    return {}