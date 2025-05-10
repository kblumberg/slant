import json
import pandas as pd
from utils.utils import log
from classes.JobState import JobState
from pinecone import Pinecone
from langchain_openai import OpenAIEmbeddings
from constants.keys import PINECONE_API_KEY
from tavily import TavilyClient
from solana.rpc.api import Client
from constants.keys import SOLANA_RPC_URL
from solana.transaction import Signature


def get_flipside_schema_data(include_tables: list[str] = [], include_performance_notes: bool = False):
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    current_dir = '.'
    include_tables = []
    df = pd.read_csv(f'{current_dir}/data/flipside_columns.csv')[['table_schema','table_name','table','column_name','data_type','column_description','example_values','ordinal_position','ignore_column','ignore_description']]
    flipside_tables = pd.read_csv(f'{current_dir}/data/flipside_tables.csv')[['table','description']]
    flipside_table_notes = pd.read_csv(f'{current_dir}/data/flipside_table_usage_notes.csv')[['table','usage_note']]
    df = df[df.ignore_column == 0]

    if len(include_tables) > 0:
        df = df[df['table'].isin(include_tables)]

    flipside_column_performance_notes = pd.read_csv(f'{current_dir}/data/flipside_column_performance_notes.csv')[['column_name','performance_note']]
    flipside_column_performance_notes = flipside_column_performance_notes[(flipside_column_performance_notes.column_name.isin(df.column_name)) | (flipside_column_performance_notes.column_name == 'all')].dropna()

    schema_text = []
    for table_schema in df['table_schema'].unique():
        schema_text.append(f"# Schema: {table_schema}")
        cur = df[df.table_schema == table_schema]
        for table in cur['table'].unique():
            schema_text.append(f"## Table: {table}")
            flipside_table = flipside_tables[flipside_tables.table == table]
            cur_notes = flipside_table_notes[flipside_table_notes.table == table].dropna()
            if len(flipside_table) > 0:
                schema_text.append(f"**Description**: {flipside_table.description.values[0]}")
            if len(cur_notes) > 0:
                schema_text.append(f"**Notes**:")
                for _, row in cur_notes.iterrows():
                    schema_text.append(f" - {row['usage_note']}")
            schema_text.append(f"**Columns**:")
            for _, row in cur[cur.table == table].sort_values(by='ordinal_position').iterrows():
                col_info = f"- {row['column_name']} ({row['data_type']})"
                if not row['ignore_description']:
                    col_info += f": {row['column_description']}"
                if not pd.isna(row['example_values']):
                    col_info += f" (Examples: {row['example_values']})"
                schema_text.append(col_info)
            schema_text.append("")  # Empty line between tables
    if include_performance_notes:
        schema_text.append(f"## Performance Tips")
        for _, row in flipside_column_performance_notes.iterrows():
            schema_text.append(f" - {row['performance_note']}")
    # print("\n".join(schema_text))
    return "\n".join(schema_text)

def get_scale(data: pd.DataFrame, col: str) -> int:
    mx_0 = data[data[col].notna()][col].max()
    mn_0 = data[data[col].notna()][col].min()
    mx = max(mx_0, -mn_0)

    if mx < 1_000:
        return 0
    else:
        return mx / mn

def read_schemas():
    schemas = ''
    with open('ai/context/flipside/schema.sql', 'r') as f:
        schemas = f.read()
    return schemas

def get_refined_prompt(state: JobState):
    projects = list(set([ x.project for x in state['analyses']]))[:3]
    activities = list(set([ x.activity for x in state['analyses']]))[:3]
    tokens = list(set([token for x in state['analyses'] for token in x.tokens]))
    query = list(set(projects + activities + tokens))
    query_text = ' '.join(query)
    l = 390 - len(query_text)
    refined_prompt = state['analysis_description'][:l] + '\n\n' + query_text
    return refined_prompt[:390]

def parse_messages(state: JobState):
    role_map = {
        "human": "USER",
        "ai": "ASSISTANT",
        "system": "SYSTEM"
    }
    messages = '\n'.join([
        f"{role_map.get(m.type, m.type.upper())}: {m.content}" for m in state['messages']
    ])
    return messages

def print_tool_starting(current_tool: str):
    log('\n')
    log('='*20)
    log('\n')
    log(f'{current_tool} starting...')

def get_sql_notes():
    return """
        ## Important Notes
        - Tokens are typically filtered by `token_address` or `mint` or `___mint` in the schema.
        - Programs are typically filtered by `program_id` in the schema.
        - Do NOT use any placeholders. Only use real mints, addresses, program ids, dates, etc.
        - Make sure to use LIMIT if you are only looking for a specific number of results.
        - Project program_ids can change over time; you may need to include multiple program ids in the WHERE clause to get the correct results.
        - All timestamps are in UTC; default to that unless the user explicitly mentions a different timezone.
        - To do time comparisons, use `dateadd` and `current_timestamp()` or `current_date()` like so: `and block_timestamp >= dateadd(hour, -12, current_timestamp())`
        - If there is a "launch date" you are referencing, its usually a good idea to give a 2 day buffer to make sure you get all the data. (e.g. `and block_timestamp >= dateadd(day, -2, <launch_date>)`)
        - market cap = price * circulating supply
        - ONLY use the columns that are listed in the schema.
    """

def get_optimization_sql_notes_for_flipside():
    return """
        ## Optimization Notes
        1. ALWAYS filter and join on `block_timestamp` when possible and appropriate to optimize performance because the tables are indexed on `block_timestamp`.
        2. If you are joining on `tx_id`, ALWAYS also join on `block_timestamp` as well to improve performance and put the join on `block_timestamp` first. e.g. `FROM table1 t1 JOIN table2 t2 ON t1.block_timestamp = t2.block_timestamp AND t1.tx_id = t2.tx_id`
        3. AWLAYS put the joins and filters on `block_timestamp` first to optimize performance.
            - Example A: 
                Bad: `FROM table1 t1
                WHERE t1.program_id = 'XXX'
                AND t1.block_timestamp >= DATEADD(day, -30, CURRENT_DATE())`
                ->
                Good: `FROM table1 t1
                WHERE t1.block_timestamp >= DATEADD(day, -30, CURRENT_DATE())
                AND t1.program_id = 'XXX'`
                Explanation:
                - By putting the `block_timestamp` filter first, we can filter first on the indexed column, which can improve performance.
            - Example B:
                Bad: `FROM table1 t1 JOIN table2 t2 ON t1.tx_id = t2.tx_id AND t1.block_timestamp = t2.block_timestamp WHERE t1.program_id = 'XXX' and t1.block_timestamp >= DATEADD(day, -30, CURRENT_DATE())`
                ->
                Good: `FROM table1 t1 JOIN table2 t2 ON t1.block_timestamp = t2.block_timestamp AND t1.tx_id = t2.tx_id WHERE t1.block_timestamp >= DATEADD(day, -30, CURRENT_DATE()) AND t1.program_id = 'XXX'`
                Explanation:
                - By putting the `block_timestamp` filter and join first, we can filter and join first on the indexed column, which can improve performance.
        3. Even when you are filtering one table for `block_timestamp`, filter the other table for `block_timestamp` as well to improve performance.
            - Example A:
                Bad: 
                `FROM table1 t1
                JOIN table2 t2
                    ON t1.tx_id = t2.tx_id
                    AND t1.block_timestamp = t2.block_timestamp
                WHERE t1.block_timestamp >= DATEADD(day, -30, CURRENT_DATE())`
                ->
                Good:
                `FROM table1 t1
                JOIN table2 t2
                    ON t1.block_timestamp = t2.block_timestamp
                    AND t1.tx_id = t2.tx_id
                WHERE t1.block_timestamp >= DATEADD(day, -30, CURRENT_DATE())
                    AND t2.block_timestamp >= DATEADD(day, -30, CURRENT_DATE())`
            - Example B:
                Bad:
                `FROM table1 t1
                JOIN table2 t2
                    ON t1.tx_id = t2.tx_id
                    AND t1.block_timestamp = t2.block_timestamp
                WHERE t1.program_id = 'XXX'
                    AND t1.block_timestamp >= DATEADD(day, -30, CURRENT_DATE())`
                ->
                Good:
                `FROM table1 t1
                JOIN table2 t2
                    ON t1.block_timestamp = t2.block_timestamp
                    AND t1.tx_id = t2.tx_id
                WHERE t1.block_timestamp >= DATEADD(day, -30, CURRENT_DATE())
                    AND t2.block_timestamp >= DATEADD(day, -30, CURRENT_DATE())
                    AND t1.program_id = 'XXX'`
                Explanation:
                - By including the `block_timestamp` filter on the both tables, we can first filter the data on the indexed column, which can improve performance.
    """

def get_other_info():
    return """
        ## Other Info
        - For tokens, make sure to have the correct mint / token address.
        - Unless specified otherwise, you can assume that prices are measured in USD.
    """


def state_to_reference_materials(state: JobState, exclude_keys: list[str] = [], preface: str = '', use_summary = False, include_keys: list[str] = [], include_performance_notes: bool = False):
    additional_context = '## 📚 Reference Materials\n\n'


    schema = get_flipside_schema_data(state['flipside_tables'], include_performance_notes)

    if preface:
        additional_context = additional_context + preface + '\n\n'

    if use_summary:
        exclude_keys = exclude_keys + ['tweets', 'web_search_results', 'projects', 'additional_context_summary']
        additional_context = additional_context + '**RELATED INFORMATION**: \n' + 'These are just recommendations, not requirements. Factor other information into your analysis, but use this if it is helpful:\n\n' + state['context_summary']

    possible_keys = ['tweets_summary', 'web_search_summary', 'projects', 'flipside_example_queries','schema','transactions','additional_context_summary','other_info','program_ids','flipside_determine_approach','start_timestamp']
    transaction_text = """
        - Use the following example transactions for inspiration and to understand what the correct addresses, program ids, etc. are.
        - Prioritize this information over the other reference materials, since it is provided directly from the user
        - Pay particular attention to the `programId` fields; if there is a single `programId` or the same one used multiple times, that is likely the one we want to use.

        Transactions:
    """
    indices = state['flipside_subset_example_queries']
    flipside_example_queries = state['flipside_example_queries'].text.apply(lambda x: x[:10000]).values
    flipside_example_queries = '\n'.join([flipside_example_queries[i] for i in indices])
    d = {
        'tweets_summary': ('SUMMARY OF TWEETS', '', None)
        , 'web_search_summary': ('SUMMARY OF WEB SEARCH RESULTS', '', None)
        , 'projects': ('PROJECTS', '', lambda state: '\n'.join([ str(project.name) + ': ' + str(project.description) for project in state['projects']]))
        , 'flipside_example_queries': ('RELATED FLIPSIDE QUERIES', 'Here are some example queries written by other analysts. They may not be respresent the best or most optimized way to approach your analysis, but feel free to use them for inspiration and to understand available schema and patterns, incorporating them into your query if you think they are helpful:\n\n', lambda state: flipside_example_queries)
        , 'schema': ('FLIPSIDE DATA SCHEMA', '', lambda state: schema)
        , 'transactions': ('EXAMPLE TRANSACTIONS', transaction_text, lambda state: '\n'.join([ str(transaction) for transaction in state['transactions']]))
        , 'additional_context_summary': ('ADDITIONAL CONTEXT', '', None)
        , 'program_ids': ('PROGRAM IDS', 'These are the program ids that will be used to analyze the user\'s analysis goal\n\n', lambda state: '\n'.join(state['program_ids']))
        , 'flipside_determine_approach': ('FLIPSIDE DETERMINE APPROACH USING RAW TABLES', 'Here is the approach we will take to analyze the user\'s analysis goal using raw tables **prioritize this information over the other reference materials**:\n\n', None)
        , 'other_info': ('OTHER INFO', get_other_info(), None)
        , 'start_timestamp': ('START TIMESTAMP', 'Your SQL query should start at this timestamp. Prioritize this information over the other reference materials:\n\n', None)
    }
    keys = [k for k in possible_keys if k not in exclude_keys]
    keys = [k for k in keys if k in include_keys or len(include_keys) == 0]
    for k in keys:
        # log(k)
        if not k in state.keys():
            additional_context = additional_context + '\n**' + d[k][0] + '**:\n' + d[k][1] + '\n\n'
        elif len(state[k]) > 0:
            if d[k][2]:
                additional_context = additional_context + '\n**' + d[k][0] + '**:\n' + d[k][1] + '\n\n' + d[k][2](state)
            else:
                additional_context = additional_context + '\n**' + d[k][0] + '**:\n' + d[k][1] + '\n\n' + state[k]
            # if k == 'transactions':
            #     transactions = '\n'.join([ str(transaction) for transaction in state['transactions']])
            #     transaction_text = f"""
            #     **EXAMPLE TRANSACTIONS**:
            #     - Use the following example transactions for inspiration and to understand what the correct addresses, program ids, etc. are.
            #     - Prioritize this information over the other reference materials, since it is provided directly from the user
            #     - Pay particular attention to the `programId` fields; if there is a single `programId` or the same one used multiple times, that is likely the one we want to use.

            #     Transactions:
            #     {transactions}
            #     """
            #     additional_context = additional_context + transaction_text
            # else:
            #     additional_context = additional_context + '\n**' + d[k][0] + '**:\n' + d[k][1] + state[k]
    # if len(state['tweets_summary']) > 0 and 'tweets_summary' not in exclude_keys and (no_include_keys or 'tweets_summary' in include_keys):
    #     additional_context = additional_context + '**SUMMARY OF TWEETS**: \n' + state['tweets_summary']
    # if len(state['web_search_summary']) > 0 and 'web_search_summary' not in exclude_keys and (no_include_keys or 'web_search_summary' in include_keys):
    #     additional_context = additional_context + '**SUMMARY OF WEB SEARCH RESULTS**: \n' + state['web_search_summary']
    # if len(state['projects']) > 0 and 'projects' not in exclude_keys and (no_include_keys or 'projects' in include_keys):
    #     additional_context = additional_context + '**PROJECTS**: \n' + '\n'.join([ str(project.name) + ': ' + str(project.description) for project in state['projects']])
    # if len(state['flipside_example_queries']) > 0 and 'flipside_example_queries' not in exclude_keys and (no_include_keys or 'flipside_example_queries' in include_keys):
    #     example_queries = '\n\n'.join(state['flipside_example_queries'].text.apply(lambda x: x[:10000]).values)
    #     additional_context = additional_context + '**RELATED FLIPSIDE QUERIES**: \n' + 'Here are some example queries written by other analysts. They may not be respresent the best or most optimized way to approach your analysis, but feel free to use them for inspiration and to understand available schema and patterns, incorporating them into your query if you think they are helpful:\n\n' + example_queries
    # if len(state['schema']) > 0 and 'schema' not in exclude_keys and (no_include_keys or 'schema' in include_keys):
    #     additional_context = additional_context + '**FLIPSIDE DATA SCHEMA**: \n' + state['schema'] + '\n\n' + get_sql_notes()
    #     additional_context = additional_context + get_sql_notes()
    # if len(state['transactions']) > 0 and 'transactions' not in exclude_keys and (no_include_keys or 'transactions' in include_keys):
    #     transactions = '\n'.join([ str(transaction) for transaction in state['transactions']])
    #     transaction_text = f"""
    #     **EXAMPLE TRANSACTIONS**:
    #     - Use the following example transactions for inspiration and to understand what the correct addresses, program ids, etc. are.
    #     - Prioritize this information over the other reference materials, since it is provided directly from the user
    #     - Pay particular attention to the `programId` fields; if there is a single `programId` or the same one used multiple times, that is likely the one we want to use.

    #     Transactions:
    #     {transactions}
    #     """
    #     additional_context = additional_context + transaction_text
    # if len(state['additional_contexts']) > 0 and 'additional_contexts' not in exclude_keys and (no_include_keys or 'additional_contexts' in include_keys):
    #     additional_context = additional_context + '**ADDITIONAL CONTEXT**: \n' + '\n'.join(state['additional_contexts'])
    # additional_context = additional_context + '**OTHER INFO**: \n' + get_other_info()
    log(f'Reference materials: {format(len(additional_context), ",")}')
    return additional_context

def get_web_search(question: str, tavily_client: TavilyClient) -> str:
    question = 'solana blockchain ' + question
    web_search_results = tavily_client.search(question[:400], search_depth="advanced", include_answer=True, include_images=False, max_results=5, include_raw_content=True)
    if 'answer' in web_search_results.keys():
        answer = web_search_results['answer']
        if 'results' in web_search_results.keys():
            for r in web_search_results['results']:
                if r['raw_content']:
                    answer = answer + '\n' + r['raw_content']
        return answer
    return ''

def rag_search_tweets(question: str) -> str:
    pc = Pinecone(api_key=PINECONE_API_KEY)
    index = pc.Index("slant", namespace="tweets")

    # Get embeddings for query
    embeddings = OpenAIEmbeddings()
    query_embedding = embeddings.embed_query(question)

    # Search Pinecone
    results = index.query(
        vector=query_embedding
        , top_k=5
        , include_metadata=True
        , namespace="tweets"
    )

    # Format results
    tweets = []
    for match in results['matches']:
        tweet = match.metadata
        tweet['id'] = match['id']
        tweets.append(tweet['text'])
    return '\n'.join(tweets)


def parse_tx(tx_id: str):
    if len(tx_id) != 88:
        return {
            'accountKeys': [],
            'instructions': [],
            'logMessages': []
        }
    client = Client(SOLANA_RPC_URL)
    sig = Signature.from_string(tx_id)
    tx_data = client.get_transaction(sig, encoding="jsonParsed", max_supported_transaction_version=0).to_json()
    tx_data = json.loads(tx_data)
    accountKeys = [x['pubkey'] for x in tx_data['result']['transaction']['message']['accountKeys']]
    instructions = [x for x in tx_data['result']['transaction']['message']['instructions'] if not x['programId'] in ['ComputeBudget111111111111111111111111111111']]
    logMessages = [x for x in tx_data['result']['meta']['logMessages'] ]
    return {
        'accountKeys': accountKeys,
        'instructions': instructions,
        'logMessages': logMessages
    }