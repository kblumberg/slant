import time
from utils.utils import log
from classes.GraphState import GraphState
from langchain.schema import SystemMessage, HumanMessage

def prompt_refiner_for_flipside_sql(state: GraphState) -> str:
    start_time = time.time()

    messages = [
        SystemMessage(content="""
        You are a specialized AI assistant trained in optimizing user queries related to cryptocurrency data analysis.
        
        Your goal is to transform a user query into a concise, information-rich version that extracts key entities such as protocols, tokens, or program names. You can ignore any specific timeframes or metrics referenced in the original query. Just focus on protocols, tokens, programs, project names, etc.

        The optimized query should:
        - Focus on keywords that would best retrieve relevant past SQL queries from a RAG database.
        - Remove vague or generic phrasing and replace it with concrete, searchable terms.
        - Highlight specific metrics (e.g., volume, active users), tokens, or projects if mentioned.
        - Avoid full sentences — keep the output short, clear, and search-friendly.
        """),
        HumanMessage(content=f"""
        Original Query: {state['clarified_query']}

        Respond only with the optimized query string for RAG search.
        """)
    ]

    response = state['llm'].invoke(messages).content
    time_taken = round(time.time() - start_time, 1)
    # log(f'prompt_refiner_for_flipside_sql finished in {time_taken} seconds')
    # log(response)
    return response