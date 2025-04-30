from utils.utils import log
from classes.JobState import JobState

def print_job_state(state: JobState):
    # log('print_job_state')
    # log(state)
    analyses = '\n'.join([str(analysis) for analysis in state['analyses']])
    transactions = '\n'.join([str(transaction) for transaction in state['transactions']])
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

transactions:
{transactions}

response:
{state['response']}

{('='*20)}

""")
    return {}