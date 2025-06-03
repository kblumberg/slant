import os
import time
import pandas as pd
from utils.utils import log
from classes.JobState import JobState
from ai.tools.utils.utils import parse_messages, state_to_reference_materials
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from ai.tools.utils.utils import log_llm_call

def ask_follow_up_questions(state: JobState) -> JobState:

    start_time = time.time()
    # log('\n')
    # log('='*20)
    # log('\n')
    # log('ask_follow_up_questions starting...')
    reference_materials = state_to_reference_materials(state, preface = "The following is additional context that might be relevant to the user's prompt. If it is relevant, use it to generate clarifying questions. If not, ignore it.", exclude_keys=[])
    current_dir = os.path.dirname(os.path.abspath(__file__))
    path_name = os.path.join(current_dir, "..", "..", "..", "data", "follow_up_questions.csv")
    follow_up_questions_df = pd.read_csv(path_name)
    question_notes = '\n- '.join(follow_up_questions_df['question'].tolist())

    # log('ask_follow_up_questions additional_context')
    # log(additional_context)

    message_df = state['memory'].message_df
    user_messages = message_df[message_df.role == 'user']
    bot_messages = message_df[message_df.role == 'assistant']
    asked_followups_before = len(bot_messages) > 0 and len(user_messages) > 1
    previous_messages = ''

    log(f'ask_follow_up_questions')
    log(f'asked_followups_before: {asked_followups_before}')
    log(f"len(state['messages']): {len(state['messages'])}")

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

            Do NOT ask questions that are repetitive or overly detailed. If the user's response resolved their intent clearly, then return an empty list like:
            ```json
            []
            ```
            """

    prompt = """
        You are an expert blockchain analyst and prompt engineer. Your job is to generate a list of clarifying questions that help understand a user's high-level request so a junior analyst can perform accurate, complete on-chain analysis.

        ## 🔑 Objective
        Ask only **non-technical, high-level questions** in plain English. Your audience is **non-technical** (like a product manager). They do **not** know anything about program IDs, logs, token flows, tables, or schemas.

        ## 🔒 DO NOT EVER
        - 🚫 Ask about how to parse data, interpret transactions, or decode labels (e.g., "Should I use the 'stake' label?")
        - 🚫 Mention any specific program IDs, logs, decoded instructions, or schemas unless quoting directly from context AND asking for confirmation
        - 🚫 Ask which tables, columns, or Flipside schemas to use
        - 🚫 Ask implementation questions like "Should I count transfers into this address?"
        - 🚫 Reference "inner instructions", "routed swaps", "decoded logs", "program interactions", etc.
        - 🚫 Ask "why" the user wants the data — assume they want it for actionable insight

        ⚠️ If you cannot phrase a question without using technical terms or asking for implementation logic — **DO NOT ASK IT.**

        ## ✅ ONLY ASK ABOUT
        Ask about things a non-technical stakeholder would care about, such as:
        - Timeframes or start dates (e.g., "Should I start from the launch date: 2024-03-12?")
        - Which projects, protocols, or tokens to include/exclude
        - Thresholds (e.g., "Define whale wallet as over $100k — is that okay?")
        - Granularity (e.g., "Should I group results weekly or monthly?")
        - Definitions that are not clear (e.g., "What counts as a new wallet?")
        - Confirming specific addresses, mints, or IDs if already quoted in context
        - Requesting example transaction IDs if identification logic is unclear

        ## 🧠 Reminder
        - Users do **not** see reference materials. Quote things **fully** if needed.
        - Avoid repeating questions already answered by the user's prompt or prior messages.
        - Only return high-level clarifications — if none are needed, return an empty list.
        - Only ask essential clarifying questions; use reasonable assumptions about the user's intent. 

        ## 🤝 Assumptions
        **Unless the user explicitly mentions otherwise, assume the following:**
        - Volume should be in USD unless they are talking about a specific token.
        - Do not subset or exclude any data
        - If the analysis goes to present day, include today's data
        - Only include successful transactions
        **The above assumptions are SUPER important. We do not want to ask any unneccesary questions. Unless the user explicitly mentions otherwise, do NOT ask them the above questions.**

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

        **OUTPUT FORMAT**:
        Return ONLY a valid JSON list. No commentary, no extra text. Just a JSON array of questions like this:

        ```json
        ["question 1", "question 2", "question 3"]
        ```

        # Examples
        Use the format and tone of the examples below:
        ## Example 1
        User prompt: Show me the trading volume and user growth of all Solana trading bots over the last 3 months. What application is gaining the most market share with retail wallets vs whale wallets?


        Response:
        ["Here are the solana trading bots I have in mind: XXX, YYY, ZZZ. Are there any others I should include?", "I am defining whale wallets as $100k+ in volume. Is that a good threshold to use?", "What timeframe should I look at?", "Should I group weekly or monthly?"]


        ## Example 2
        User prompt: Show me what users did after removing liquidity from Orca? Did they provide liquidity into a different pool on Orca or LP to another DEX?


        Response:
        ["How far back do you want me to analyze?", "What window of time should I use after a user removes liquidity from Orca? E.g Should I just look at their activities over the next 24h?"]


        ## Example 3
        User prompt: Show me the daily rewards distributed by BaseBet

        Response:
        ["What timeframe do you want me to analyze?", "Do you want the total amount or the number of wallets?", "Can you provide an example transaction id of a reward?"]

        ## Example 4
        User prompt: Show me the cumulative amount of $ME staking power over time, starting from 2024-01-01

        Response:
        []

        Notes:
        - {question_notes}

        # Critical Reminders
        - Make sure to record any wallet addresses, mints, or program ids EXACTLY as they are. Do not change or miss any characters.
        - NEVER ask the user to answer questions about which tables or columns to use. They are not technical and don't know anything about the Flipside data schemas.
        - Remember the assumptions we made above. Unless the user explicitly mentions otherwise, do NOT ask questions from the assumptions section above.

    """
    formatted_prompt = prompt.format(
        user_prompt=state['user_prompt'],
        reference_materials=reference_materials,
        previous_messages=previous_messages,
        question_notes=question_notes
    )
    # log('formatted_prompt')
    # log(formatted_prompt)
    response = log_llm_call(formatted_prompt, state['complex_llm'], state['user_message_id'], 'AskFollowUpQuestions')
    follow_up_questions = parse_json_from_llm(response, state['llm'])
    log(f'ask_follow_up_questions (message #{len(state["messages"])})')
    log(follow_up_questions)
    time_taken = round(time.time() - start_time, 1)
    # log(f'ask_follow_up_questions finished in {time_taken} seconds')
    return {'follow_up_questions': follow_up_questions, 'completed_tools': ["AskFollowUpQuestions"]}
