import re
import time
import json
from utils.utils import log
from classes.JobState import JobState
from ai.tools.utils.utils import parse_messages
from ai.tools.utils.utils import state_to_reference_materials

def context_saver(state: JobState) -> JobState:
    prompt = """
    You are an expert blockchain analyst and prompt engineer.

    Your task is to extract **all key information** from the reference materials that will be crucial for completing the analysis described below. This includes any relevant technical details, such as:

    - Program IDs  
    - Token mints  
    - Wallet addresses  
    - Flipside tables  
    - Time frames  
    - Any specific terminology or metrics mentioned  
    - Notable on-chain patterns or entities  

    Focus on extracting **concrete data points** and **relevant metadata** that will guide follow-up actions like query construction or report generation.

    Avoid vague summaries—be specific and precise. If something might influence how the analysis is done, **include it**.

    ---

    **Analysis Description:**  
    {analysis_description}

    ---

    {reference_materials}

    ---

    **Output Format:**  
    Write a **single, concise paragraph** summarizing the most important details to keep in mind when conducting the analysis.
    """


    formatted_prompt = prompt.format(
        analysis_description=state['analysis_description'],
        reference_materials=state_to_reference_materials(state)
    )
    response = state['reasoning_llm'].invoke(formatted_prompt).content
    summary = re.sub(r'```json', '', response)
    summary = re.sub(r'```', '', summary)
    log('context_saver')
    log(summary)
    return {'context_summary': summary, 'completed_tools': ["ContextSaver"]}
