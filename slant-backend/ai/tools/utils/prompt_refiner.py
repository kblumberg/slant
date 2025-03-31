import json
import time
from utils.utils import log
from classes.GraphState import GraphState
from langchain.schema import SystemMessage, HumanMessage

def prompt_refiner(state: GraphState):
    start_time = time.time()
    # Get message history
    # history_message = state['memory'].get_history_message()
    history_message = None
    messages = [
        SystemMessage(content="""
        You are a specialized AI query optimization assistant for cryptocurrency analysis.
        
        Advanced Query Refinement Objectives:
        - Transform vague queries into precise, actionable cryptocurrency research requests
        - Incorporate domain-specific terminology and context
        - Identify specific cryptocurrencies, metrics, or research angles
        - Extract nuanced information needs
        
        Refinement Strategies:
        1. Clarify cryptocurrency-specific details
        2. Add technical or market context
        3. Specify time frames, metrics, or comparative elements
        4. Remove ambiguity while preserving original intent
        
        Key Considerations:
        - Focus on extracting maximum actionable insight
        - Use precise crypto industry language
        - Ensure query supports in-depth, targeted research
        """),
        # history_message,
        HumanMessage(content="""
        Detailed Query Context:
        Original Query: {query}
        
        Advanced Refinement Instructions:
        - Break down complex or multi-part queries
        - Identify specific information goals
        - Add cryptocurrency-specific precision
        - Consider market, technical, or fundamental analysis perspectives
        
        Desired Refinement Output:
        - Highly specific query
        - Clear research objective
        - Maximized information retrieval potential
                     

        
        Respond in JSON format with the following keys:
        - refined_query: the refined query
        - clarified_query: if the "original query" references the conversation context, then supplement the original query with the context. If the original query is already clear and complete, do not change it. Only supplement the original query if it is a follow up question that specifically references a subject from the conversation history (e.g. "what about this", "tell me more about that", etc. DO NOT add any additional context unless it is directly referenced in the history).
        """.format(query=state['query'])),
    ]

    # Call LLM to get the decision
    response = state['llm'].invoke(messages).content
    log(response)
    log('response')
    # Handle potential JSON parsing errors
    try:
        if isinstance(response, str):
            response = response.replace("```json", "").replace("```", "").strip()
            response = json.loads(response)
    except Exception as e:
        log(f"Error cleaning JSON string: {e}")
        raise
    refined_query = response['refined_query']
    clarified_query = response['clarified_query']
    time_taken = round(time.time() - start_time, 1)
    log(f'prompt_refiner finished in {time_taken} seconds')
    log(f'{state["query"]} -> Refined:\n{refined_query} \nClarified:\n{clarified_query}')
    return clarified_query, refined_query