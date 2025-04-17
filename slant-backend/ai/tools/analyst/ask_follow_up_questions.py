import re
import time
import json
from utils.utils import log
from classes.JobState import JobState
from ai.tools.utils.utils import parse_messages

def ask_follow_up_questions(state: JobState) -> JobState:

    start_time = time.time()
    log('\n')
    log('='*20)
    log('\n')
    log('ask_follow_up_questions starting...')
    additional_context = ''
    if len(state['tweets']) > 0:
        additional_context = additional_context + '**TWEETS**: \n' + '\n'.join([ str(tweet.text) for tweet in state['tweets']])
    if len(state['web_search_results']) > 0:
        additional_context = additional_context + '**WEB SEARCH RESULTS**: \n' + state['web_search_results']
    if len(state['projects']) > 0:
        additional_context = additional_context + '**PROJECTS**: \n' + '\n'.join([ str(project.name) + ': ' + str(project.description) for project in state['projects']])
    if len(state['flipside_example_queries']) > 0:
        example_queries = '\n\n'.join(state['flipside_example_queries'].text.apply(lambda x: x[:10000]).values)
        additional_context = additional_context + '**RELATED FLIPSIDE QUERIES**: \n' + example_queries

    log('ask_follow_up_questions additional_context')
    log(additional_context)

    asked_followups_before = len(state['memory'].message_df) > 1
    previous_messages = ''

    if len(state['messages']) > 0:
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

            Do NOT ask questions that are repetitive or overly detailed. If the user’s response resolved their intent clearly, then return an empty list like:
            ```json
            []
            ```
            """

    prompt = """
        You are an expert blockchain analyst and prompt engineer. Your job is to take a user's high-level prompt and any additional context, and generate a list of *clarifying questions* to ensure that an accurate and complete on-chain analysis can be performed.

        ## Objective:
        Identify what needs clarification before a junior blockchain analyst begins the actual work. Focus on missing details, ambiguous intent, and necessary filters, groupings, or thresholds.

        You should consider:

        - What specific data needs to be extracted or filtered (e.g., by `program_id`, `tx_from`, `tx_to`, `block_timestamp`, etc.)
        - What calculations or groupings are required (e.g., daily vs cumulative totals, wallet segmentation)
        - Whether additional definitions or thresholds are needed (e.g., define a "whale" wallet or "active" user)
        - Any ambiguity in the user's request that would require follow-up
        - Avoid asking questions that are already addressed in prior messages
        - Do not ask specifics about how to perform the analysis, only ask clarifying questions about the user's request
        - Do not ask any technical questions about which tables or columns to use

        **GENERAL ASSUMPTIONS**:
        Unless specifically states, assume the following:
        - All standard decoded transaction-level data is available for querying and we know how to pull the data.
        - The user is interested in **Solana mainnet** blockchain on-chain analysis.
        - There is no end date for the analysis (can go to present time).
        - Use UTC time zone.

        ---

        **TASK**: Given the user prompt and additional context, return ONLY a valid JSON list of clarifying questions that should be asked before analysis begins. If you do not need to ask any questions, return an empty list.

        {previous_messages}

        **USER PROMPT**:
        {user_prompt}

        **ADDITIONAL CONTEXT**:
        The following is additional context that might be relevant to the user's prompt. If it is relevant, use it to generate clarifying questions. If not, ignore it.
        {additional_context}

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
        ["What timeframe do you want me to analyze?", "Do you want the total amount or the number of wallets?"]

        =============
        = Example 4 =
        =============
        User prompt: Show me the cumulative amount of $ME staking power over time, starting from 2024-01-01

        Response:
        []

    """
    formatted_prompt = prompt.format(
        user_prompt=state['user_prompt'],
        additional_context=additional_context,
        previous_messages=previous_messages
    )
    log('formatted_prompt')
    log(formatted_prompt)
    response = state['resoning_llm'].invoke(formatted_prompt).content
    follow_up_questions = re.sub(r'```json', '', response)
    follow_up_questions = re.sub(r'```', '', follow_up_questions)
    follow_up_questions = json.loads(follow_up_questions)
    log('follow_up_questions')
    log(follow_up_questions)
    time_taken = round(time.time() - start_time, 1)
    log(f'ask_follow_up_questions finished in {time_taken} seconds')
    return {'follow_up_questions': follow_up_questions, 'completed_tools': ["AskFollowUpQuestions"]}
