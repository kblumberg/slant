import json
import time
from utils.utils import log
from classes.GraphState import GraphState
from langchain.schema import SystemMessage, HumanMessage

def parse_timestamp(state: GraphState):
    # parse the query for a start timestamp
    start_time = time.time()
    messages = [
        SystemMessage(content="""You are a strict function that extracts the relevant time period (in days) a user is referring to in a natural language query. Your goal is to convert phrases like "today", "this week", "past month", etc. into an integer number of days. Do not explain. Do not output anything other than a number."""),
        HumanMessage(content=f"""
        **User Query:** {state['query']}

        Your job is to determine how many days back the user is referring to based on their query.

        Examples:
        - "What happened today?" → 1
        - "What’s the latest?" → 7
        - "...in the past day" → 1
        - "...in the past few hours" → 0
        - "Show me what’s been going on the last few days" → 3
        - "Any updates from this month?" → 30
        - "What’s been trending the past year?" → 365
        - "Anything new?" → 7
        - "What's been going on?" → 1
        - "Tell me about yesterday" → 2
        - Things that say "new" or "news" → 7

        **Output Format:**
        - Respond **ONLY** with a number (no words, no punctuation).
        - If no time period is mentioned or implied, respond with -1.
        """)
    ]
    response = state['llm'].invoke(messages).content
    # log('parse_start_timestamp llm response')
    # log(response)
    time_taken = round(time.time() - start_time, 1)
    # log(f'parse_start_timestamp finished in {time_taken} seconds')
    try:
        n_days = int(response)
        if n_days < 0:
            return 0
        n_days = max(n_days, 0.5)
        # log(f'n_days: {n_days}')
        unix_timestamp = int(time.time()) - (n_days * 24 * 60 * 60)
        return unix_timestamp
    except ValueError:
        # log(f"Error parsing LLM response: {response}")
        return 0

def tool_selector(state: GraphState):
    start_timestamp = parse_timestamp(state)
    start_time = time.time()
    # log('tool_selector starting...')
    query = state['query']
    # state.update({'current_message': 'Analyzing query...'})
    # yield state
    # llm = state['llm']


    # Construct prompt dynamically based on state

    history_message = state['memory'].get_history_message()
    messages = [
        SystemMessage(content="You are an AI assistant that selects the most relevant tools to process a given user query about crypto. Your goal is to choose the best set of tools while following strict constraints."),
        history_message,
        HumanMessage(content=f"""
            **User Query:** {query}

            **Available Tools & When to Use Them:**
            - `"RagSearchTweets"`: Use this for questions about current events, project activity, or public discussions on Solana. If unsure between this and `"RagSearchProjects"`, choose this.
            - `"RagSearchProjects"`: Use this to describe specific projects or to give an overview of projects in a category (e.g., DeFi, NFTs, gaming projects). NOT good for asking about current events, activity, or concepts. Generally better to use `"RagSearchTweets"` for that.
            - `"RagSearchTwitterKols"`: Use this for questions about influencers or prominent figures in Solana's ecosystem.
            - `"ProjectKolQueryRunner"`: Use this for questions about specific projects, KOLs, or tweets that require filtering or ranking, or customized SQL logic. ALWAYS include when user asks about "most" or "top" projects, KOLs, tweets, accounts, etc.
            - `"DataAnalyst"`: Use this for questions that require on-chain data analysis or blockchain-related metrics. ALWAYS include when user asks about data or results or price history or to create a chart.
            - `"SharkyAgent"`: Use this for questions around how to use Sharky, or guidance on which Sharky loans to take.
            - `"NewsFinder"`: Use this for questions around the latest news in the crypto space. (e.g. "whats the biggest news", "what's the latest", "what's happening") ALWAYS include when user asks about "news" or the like. If the user mentions a specific project, make sure to also include `"RagSearchTweets"`.

            **Selection Rules:**
            - Choose the **most relevant** tools based on the query.
            - **STRICT LIMIT:** Select **1-3 tools only**. **NEVER select more than 3 tools.** **MUST select at least 1 tool.** If you cannot select any tools, respond with `["RagSearchTweets"]`.
            - Respond **ONLY** with a valid JSON list of tool names (e.g., `["TOOL1", "TOOL2"]`).
            - **DO NOT** add explanations, text, or extra formatting—just return the JSON list.
        """)
    ]


    # Call LLM to get the decision
    response = state['llm'].invoke(messages).content
    # log('tool_selector llm response')
    # log(response)
    time_taken = round(time.time() - start_time, 1)
    # log(f'tool_selector finished in {time_taken} seconds')
    try:
        tools = json.loads(response)
        # log('tools')
        # log(tools)
        if not isinstance(tools, list):
            raise ValueError("Invalid tool list")
        upcoming_tools = tools
        if "DataAnalyst" in tools:
            upcoming_tools += ["ExecuteFlipsideQuery", "FormatForHighcharts"]
        if "ProjectKolQueryRunner" in tools:
            upcoming_tools += ["SlantQueryExecutor"]
        return {'run_tools': tools, 'upcoming_tools': upcoming_tools, 'completed_tools': ["ToolSelector"], 'start_timestamp': start_timestamp}
    except json.JSONDecodeError:
        # log(f"Error parsing LLM response: {response}")
        return {}
    