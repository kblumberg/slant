import json
import time
from utils.utils import log
from classes.JobState import JobState
from langchain.schema import SystemMessage, HumanMessage
from ai.tools.utils.utils import parse_messages

def twitter_prompt_refiner(state: JobState):
    message_history = parse_messages(state)
    messages = [
        SystemMessage(content="""
            You are a specialized AI assistant that converts a conversation history into focused, effective keyword-based search strings that would help retrieve the most relevant tweets on the topic.

            Your goal is to transform a conversation history into focused, effective keyword-based search strings that would help retrieve the most relevant tweets on the topic. The tweets are typically short, informal, and may include hashtags, project names, tokens, symbols (e.g. $SOL), or event-based keywords.

            Guidelines:
            - Be specific: include relevant project names, tokens, or technical terms when possible.
            - Use crypto-native phrasing: like "airdrops", "$TOKEN", "staking", "TVL", "wallets", "whales", "dev update", etc.
            - Avoid long natural language sentences — output should be short and keyword-optimized for search.
            - Include multiple variations if appropriate (e.g. “MEV, sandwich attack, frontrunning”).
            - Don’t include hashtags unless they’re essential (e.g. #Solana, #airdrop).

            Return ONLY the final search string. No explanation.
        """)
        , HumanMessage(content=f"""
            Create a search query to search a RAG database of tweets for the following topic:

            Messages History:
            
            "{message_history}"

            Respond with a single optimized string for search.
        """)
    ]

    # Call LLM to get the decision
    response = state['llm'].invoke(messages).content
    # log(response)
    # log('response')
    # Handle potential JSON parsing errors
    response = response.replace("```json", "").replace("```", "").strip()
    # log(f'{state["user_prompt"]} -> Refined:\n{response}')
    return response

def prompt_refiner(state: JobState):
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
        """.format(query=state['user_prompt'])),
    ]

    # Call LLM to get the decision
    response = state['llm'].invoke(messages).content
    # log(response)
    # log('response')
    # Handle potential JSON parsing errors
    try:
        if isinstance(response, str):
            response = response.replace("```json", "").replace("```", "").strip()
            response = json.loads(response)
    except Exception as e:
        # log(f"Error cleaning JSON string: {e}")
        raise
    refined_query = response['refined_query']
    clarified_query = response['clarified_query']
    time_taken = round(time.time() - start_time, 1)
    # log(f'prompt_refiner finished in {time_taken} seconds')
    # log(f'{state["user_prompt"]} -> Refined:\n{refined_query} \nClarified:\n{clarified_query}')
    return clarified_query, refined_query