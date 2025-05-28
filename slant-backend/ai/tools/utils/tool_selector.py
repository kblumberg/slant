import json
import time
from utils.utils import log
from classes.JobState import JobState
from langchain.schema import SystemMessage, HumanMessage
from ai.tools.utils.utils import log_llm_call, parse_messages_fn

def tool_selector(state: JobState):
    # Construct prompt dynamically based on state
    questions = ''
    for i in range(len(state['follow_up_questions'])):
        questions += f"Question {i+1}: {state['follow_up_questions'][i]}\n"

    messages = [
        SystemMessage(content="""
            You are an AI assistant that selects the most relevant tools to answer questions about cryptocurrency and blockchain activity—especially on Solana.

            Your goal is to choose the best set of tools for each question based on the content and intent. Follow the constraints strictly.
            """
        )
        , HumanMessage(content=f"""
            ## 📥 Input

            You are given a list of **questions** that need to be answered.

            ### Questions:
            {questions}

            ---

            ## 🛠️ Available Tools & When to Use Them

            - `"WebSearch"`
                — Use for general crypto or project-related information available online. Best for documentation, protocol descriptions, roadmap info, etc.
                - Can also use to find program ids, mints, addresses, etc.
            - `"RagSearchTweets"`
                — Use for current events, sentiment, community discussions, project news, or token commentary—especially within the Solana ecosystem.
                - Can also use to find program ids, mints, addresses, etc.
            - `"RagSearchQueries"`
                — Use when it may help to reference past Flipside SQL queries.
                - Ideal for analytics ideas, patterns, or figuring out how others queried similar data.
                - Can also use to find program ids, mints, addresses, etc.
            - `"ExecuteFlipsideQuery"`
                — Use for actual on-chain data investigation to see what's in the Flipside database.
            - `"AskUser"`
                — Use when the question is unclear, missing key filters or assumptions, or needs more context before proceeding.

            ---

            ## 📤 Output Format

            - Your output must be a valid **JSON array of lists**.
            - For **each input question**, return a list of tool names (in quotes), including any tools that may be helpful to answer the question.
            - Select the most relevant tools for each question **up to a max of 3**.
            - Output **MUST contain the same number of lists** as there are input questions.
            - If you are unsure what to use, return `["AskUser"]` for that question, but only use if it is something that only the user can answer. Try to use other tools first.
            - **DO NOT** add comments, explanations, or any extra formatting—return only the raw JSON.

            ---

            ## ✅ Examples

            Input Questions:
            ```text
            ["What wallets have interacted with Jupiter in the last 7 days?", "What is the general sentiment around $TNSR?", "How many tokens does the top 10 Solana NFTs hold?"]
            ```

            Valid Output:
            [
            ["ExecuteFlipsideQuery"],
            ["RagSearchTweets", "WebSearch"],
            ["ExecuteFlipsideQuery", "RagSearchTweets"]
            ]

            Now, return the tool list for each question as specified. """
        )
    ]

    # Call LLM to get the decision
    response = log_llm_call(parse_messages_fn(messages), state['reasoning_llm'], state['user_message_id'], 'ToolSelector')
    try:
        tools = json.loads(response)
        if not isinstance(tools, list):
            raise ValueError("Invalid tool list")
        log('tool_selector')
        log(tools)
        return {'run_tools': tools}
    except json.JSONDecodeError:
        # log(f"Error parsing LLM response: {response}")
        return {}
