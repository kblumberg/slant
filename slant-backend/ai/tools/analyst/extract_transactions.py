from classes.JobState import JobState
from classes.Transaction import Transaction
from ai.tools.utils.utils import log, parse_tx
from ai.tools.utils.utils import parse_messages
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm

def extract_transactions(state: JobState) -> JobState:
    messages = parse_messages(state)
    prompt = """
    You are an expert in extracting transaction ids from user messages.

    TASK: Parse the conversation history and identify any transaction ids mentioned and their corresponding context (e.g. what kind of transaction it is).

    For context, transaction ids are all 88 characters long.

    CONVERSATION HISTORY:
    {messages}

    EXTRACTION RULES:
    1. Extract ALL instances of transaction ids and their corresponding context (e.g. what kind of transaction it is).
    2. For each transaction id, identify:
    - transaction id: The specific transaction id (e.g., `2c2Z1memTLUw4VhbyRtKUke49wXCvUuoaYPU5P24eY2mWhAGX9wyHsP5gm7ZLzzkdgVGYpoqLsV4auHFm9uPmxt3`)
    - context: What type of activity the transaction is (e.g., nft buy, swap, stake, etc.)

    OUTPUT FORMAT:
    Return ONLY a valid JSON array where each element represents one transaction with the following structure:
    [
    {{
        "id": "string",
        "context": "string"
    }},
    ...
    ]

    EXAMPLES:
    User: "Here is the tx id for a $BONK token transfer: 3r5e7TKd8wTkxkQQUbvswSEm7RjCjH421DjzTc8ShneK4okc1MjsUf5TxYR6o7yoyV3EZejjf69sDyNtGbxFk6BR"
    Output: [
    {{"id": "3r5e7TKd8wTkxkQQUbvswSEm7RjCjH421DjzTc8ShneK4okc1MjsUf5TxYR6o7yoyV3EZejjf69sDyNtGbxFk6BR", "context": "$BONK token transfer"}},
    ]
    """.format(
        messages=messages
    )
    response = state['llm'].invoke(prompt).content
    log('extract_tx_ids response')
    log(response)
    j = parse_json_from_llm(response, state['llm'])
    transactions = []
    for transaction in j:
        if len(transaction['id']) != 88:
            # log('Invalid transaction id: ' + transaction['id'])
            continue
        transaction['data'] = parse_tx(transaction['id'])
        transactions.append(Transaction(**transaction))
    log('extract_transactions output')
    for transaction in transactions:
        log(str(transaction))
    return {'transactions': transactions, 'completed_tools': ["ExtractTransactions"]}
