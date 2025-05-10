import time
from utils.utils import log
from classes.JobState import JobState
from classes.Analysis import Analysis
from utils.utils import clean_project_tag
from ai.tools.utils.utils import parse_messages
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from ai.tools.utils.utils import state_to_reference_materials

def extract_program_ids(state: JobState) -> JobState:
    reference_materials = state_to_reference_materials(state)
    prompt = f"""
    You are a crypto data assistant. Your task is to extract a list of Solana program IDs relevant to the user's analysis goal.

    Use the context below to help identify the program IDs. Focus on identifying only the program IDs that are necessary to support the user's goal. Some of the context may be irrelevant to the user's goal, so ignore it.

    ---

    ## User Analysis Goal
    {state['analysis_description']}

    {reference_materials}

    ---

    **Instructions:**
    - Return a *list of relevant program IDs* (e.g., `["9xQeWvG816bUx9EPfAMp45Ffez9Gk7Zyb4fYkZ94dNab"]`)
    - Only include program IDs directly relevant to the user's analysis goal.
    - Use the query descriptions and SQL content to guide your selection.
    - If no relevant program ID can be identified, return an empty list (`[]`).
    - Do not include any explanation, justification, or formatting.
    - Typically, there will only be 0 or 1 program IDs relevant to the user's goal.

    Return only the list.
    """
    program_ids = state['llm'].invoke(prompt).content
    program_ids = parse_json_from_llm(program_ids, state['llm'], to_json=True)
    log(f"program_ids: {program_ids}")
    return {'program_ids': program_ids, 'completed_tools': ['ExtractProgramIds']}
