import time
from utils.utils import log
from classes.JobState import JobState
from ai.tools.utils.utils import parse_messages, state_to_reference_materials
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm

def ask_follow_up_questions(state: JobState) -> JobState:

    start_time = time.time()
    # log('\n')
    # log('='*20)
    # log('\n')
    # log('ask_follow_up_questions starting...')
    reference_materials = state_to_reference_materials(state, preface = "The following is additional context that might be relevant to the user's prompt. If it is relevant, use it to generate clarifying questions. If not, ignore it.")

    # log('ask_follow_up_questions additional_context')
    # log(additional_context)

    asked_followups_before = len(state['memory'].message_df) > 1
    previous_messages = ''

    if len(state['messages']) > 1:
        messages = parse_messages(state)
        previous_messages = f"""
        **PREVIOUS MESSAGES**:
        {messages}

        You should make reasonable assumptions about the user's intent based on these previous messages. Do NOT repeat questions that have already been answered or clarified. Only ask clarifying questions for gaps that remain.
        """
        if asked_followups_before:
            previous_messages = f"""
            {previous_messages}

            You have already asked the user a first round of clarifying questions. ONLY ask further questions **if there are still major ambiguities** in their latest reply.

            If the user has not already provided an example transaction id, ask for one if you are still unsure about how to identify the correct transaction types.

            Do NOT ask questions that are repetitive or overly detailed. If the user’s response resolved their intent clearly, then return an empty list like:
            ```json
            []
            ```
            """

    prompt = """
        You are an expert blockchain analyst and prompt engineer. Your job is to take a user's high-level prompt and any additional context, and generate a list of *clarifying questions* to ensure that an accurate and complete on-chain analysis can be performed.

        ## Objective:
        Identify what needs clarification before a junior blockchain analyst begins the actual work. Focus on missing details, ambiguous intent, and necessary filters, groupings, or thresholds.

        ### General Guidelines
        You should consider:
        - What specific data needs to be extracted or filtered (e.g., by `program_id`, `tx_from`, `tx_to`, `block_timestamp`, etc.)
        - If you are unsure how to identify the right transaction types, you may ask the user for an example transaction id to give you a guide on how to identify the correct transaction types.
        - What calculations or groupings are required (e.g., daily vs cumulative totals, wallet segmentation)
        - Whether additional definitions or thresholds are needed (e.g., define a "whale" wallet or "active" user)
        - Any ambiguity in the user's request that would require clarification
        - Avoid asking questions that are already answered in the **user’s prompt** or previous messages
        - The user **does not** see the additional context. If you reference something from it, you must **quote it fully and clearly**
        - Do not mention tables or columns specifically — only concepts

        ## 💡 Preference Rules
        - Do not mention “previous queries” "tweets" or "web search results" "the context" "the official sources" (the user does not see them) — instead, explicitly reference what you see.
        - If you have specifics, use that (e.g. instead of "since token launched", say "since token launched on YYYY-MM-DD")
        - If the context includes something like a launch date or token metadata, use it directly and precisely. E.g., "I see the token launched on 2024-03-12. Should I start the chart from that date?"
        - If you are not 99% sure about the correct program id, mint, address, etc., confirm with the user.

        ## 💡 Do not ask about
        Do NOT ask about:
        - How specifically to parse the data or transaction; can only ask high level concepts or ask them to provide example transactions
        - Technical questions about **which tables or columns to use** - keep it high level; Do NOT mention any specific tables names or columns in your questions
        - Why they are asking about something


        **If possible, make a check instead of asking a question**
        Examples:
        - Instead of asking "Which $XXX token are you referring to?", say "I see there is a $XXX token with address YYY. Is that the one you want?".
        - Instead of asking "What timeframe do you want me to analyze?", say "I see this program launched on YYYY-MM-DD. Do you want to analyze the data from then to now?".

        ---

        ## ⚠️ Assumptions to Make

        Unless otherwise stated, assume the following and do NOT ask about them unless the user explicitly mentions them ON THEIR OWN:
        - The user is asking about on-chain activity on **Solana mainnet**
        - The analysis runs up to the present day (no end date)
        - Desired time zone is **UTC**
        - The user is only interested in "successful" transactions
        - All standard decoded on-chain data is queryable

        ---

        ## 📥 Inputs

        {previous_messages}

        **USER PROMPT**:
        {user_prompt}

        {reference_materials}

        ---

        ## 📝 TASK

        Write only a **JSON array** of clarifying questions. Make your checks precise.  
        If no clarifying questions are needed, return an empty list.

        ---

        ---

        **OUTPUT FORMAT**:
        Return ONLY a valid JSON list. No commentary, no extra text. Just a JSON array of questions like this:

        ```json
        ["question 1", "question 2", "question 3"]
        ```

        Use the format and tone of the examples below:

        =============
        = Example 1 =
        =============
        User prompt: Show me the trading volume and user growth of all Solana trading bots over the last 3 months. What application is gaining the most market share with retail wallets vs whale wallets?


        Response:
        ["Here are the solana trading bots I have in mind: XXX, YYY, ZZZ. Are there any others I should include?", "I am defining whale wallets as $100k+ in volume. Is that a good threshold to use?", "What timeframe should I look at?", "Should I group weekly or monthly?"]


        =============
        = Example 2 =
        =============
        User prompt: Show me what users did after removing liquidity from Orca? Did they provide liquidity into a different pool on Orca or LP to another DEX?


        Response:
        ["How far back do you want me to analyze?", "What window of time should I use after a user removes liquidity from Orca? E.g Should I just look at their activities over the next 24h?"]


        =============
        = Example 3 =
        =============
        User prompt: Show me the daily rewards distributed by BaseBet

        Response:
        ["What timeframe do you want me to analyze?", "Do you want the total amount or the number of wallets?", "Can you provide an example transaction id of a reward?"]

        =============
        = Example 4 =
        =============
        User prompt: Show me the cumulative amount of $ME staking power over time, starting from 2024-01-01

        Response:
        []

        =============
        # Critical Reminders
        =============
        - Make sure to record any wallet addresses, mints, or program ids EXACTLY as they are. Do not change or miss any characters.

    """
    formatted_prompt = prompt.format(
        user_prompt=state['user_prompt'],
        reference_materials=reference_materials,
        previous_messages=previous_messages
    )
    # log('formatted_prompt')
    # log(formatted_prompt)
    response = state['reasoning_llm'].invoke(formatted_prompt).content
    follow_up_questions = parse_json_from_llm(response, state['llm'])
    log(f'ask_follow_up_questions (message #{len(state["messages"])})')
    log(follow_up_questions)
    time_taken = round(time.time() - start_time, 1)
    # log(f'ask_follow_up_questions finished in {time_taken} seconds')
    return {'follow_up_questions': follow_up_questions, 'completed_tools': ["AskFollowUpQuestions"]}
