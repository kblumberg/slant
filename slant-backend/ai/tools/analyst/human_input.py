import re
import time
import json
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from utils.utils import clean_project_tag

def human_input(state: JobState) -> JobState:
    start_time = time.time()
    # log('\n')
    # log('='*20)
    # log('\n')
    # log('human_input starting...')
    time_taken = round(time.time() - start_time, 1)
    # log(f'human_input finished in {time_taken} seconds')
    return {}
