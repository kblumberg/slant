import json
import re
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
from utils.db import pg_upload_data
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from datetime import datetime
import time
from langchain_core.messages import BaseMessage

def log_llm_call(prompt: str, llm: ChatOpenAI | ChatAnthropic, user_message_id: str, function_name: str) -> str:
    start_time = time.time()
    response = llm.invoke(prompt)
    model_name = response.response_metadata['model_name']
    duration = round(time.time() - start_time, 2)
    duration_m = f"{int(duration // 60):02d}:{int(duration % 60):02d}"
    log(f'\n\nLLM call: {function_name}')
    log(f'- Chars: {format(len(prompt), ",")}')
    log(f'- Tokens: {format(response.usage_metadata["input_tokens"], ",")} -> {format(response.usage_metadata["output_tokens"], ",")}')
    log(f'- Model: {model_name}')
    log(f'- Duration: {duration_m}')
    end_time = time.time()
    # log(f'LLM response: {response}')
    cur = pd.DataFrame([{
        'user_message_id': user_message_id,
        'function_name': function_name,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'llm': llm.__class__.__name__,
        'prompt': prompt,
        'model_name': response.response_metadata['model_name'],
        'input_tokens': response.usage_metadata['input_tokens'],
        'output_tokens': response.usage_metadata['output_tokens'],
        'response': response.content,
        'duration': round(end_time - start_time, 2)
    }])
    pg_upload_data(cur, 'llm_calls')
    return response.content


def get_flipside_schema_data(include_tables: list[str] = [], include_performance_notes: bool = False, include_column_types: bool = True, include_column_descriptions: bool = True, include_example_values: bool = True, include_usage_tips: bool = True):
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    current_dir = '.'
    # include_tables = []
    df = pd.read_csv(f'{current_dir}/data/flipside_columns.csv')[['table_schema','table_name','table','column_name','data_type','column_description','example_values','usage_tips','ordinal_position','ignore_column','ignore_description']]
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
            # if include_column_types:
            #     schema_text.append(f"**Data Types**:")
            #     for _, row in cur.iterrows():
            #         schema_text.append(f" - {row['column_name']}: {row['data_type']}")
            # if include_column_descriptions:
            #     schema_text.append(f"**Column Descriptions**:")
            #     for _, row in cur.iterrows():
            #         schema_text.append(f" - {row['column_name']}: {row['column_description']}")
            if len(cur_notes) > 0:
                schema_text.append(f"**Notes**:")
                for note in cur_notes.usage_note.unique():
                    schema_text.append(f" - {note}")
            schema_text.append(f"**Columns**:")
            for _, row in cur[cur.table == table].sort_values(by='ordinal_position').iterrows():
                data_type = "(" + row['data_type'] + ")" if include_column_types else ''
                col_info = f"- {row['column_name']} {data_type}"
                if not row['ignore_description'] and include_column_descriptions:
                    col_info += f": {row['column_description']}"
                if not pd.isna(row['example_values']) and include_example_values:
                    col_info += f" (Examples: {row['example_values']})"
                if not pd.isna(row['usage_tips']) and include_usage_tips:
                    col_info += f" (Usage Tips: {row['usage_tips']})"
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
    return parse_messages_fn(state['messages'])

def parse_messages_fn(messages: list[BaseMessage]):
    role_map = {
        "human": "USER",
        "ai": "ASSISTANT",
        "system": "SYSTEM"
    }
    messages = '\n'.join([
        f"{role_map.get(m.type, m.type.upper())}: {m.content}" for m in messages
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
        4. AWLAYS put ALL of the joins and filters on `block_timestamp` first before any of the other joins and filters to optimize performance.
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
                Bad: `FROM table1 t1
                JOIN table2 t2
                    ON t1.tx_id = t2.tx_id
                    AND t1.block_timestamp = t2.block_timestamp
                WHERE t1.program_id = 'XXX'
                    AND t1.block_timestamp >= DATEADD(day, -30, CURRENT_DATE())`
                ->
                Good: `FROM table1 t1
                JOIN table2 t2
                    ON t1.block_timestamp = t2.block_timestamp
                    AND t1.tx_id = t2.tx_id
                WHERE t1.block_timestamp >= DATEADD(day, -30, CURRENT_DATE())
                    AND t2.block_timestamp >= DATEADD(day, -30, CURRENT_DATE())
                    AND t1.program_id = 'XXX'`
                Explanation:
                - By putting the `block_timestamp` filter and join first, we can filter and join first on the indexed column, which can improve performance.
    """

def get_other_info():
    return """
        ## Other Info
        - For tokens, make sure to have the correct mint / token address.
        - Unless specified otherwise, you can assume that prices are measured in USD.
    """


def state_to_reference_materials(state: JobState, exclude_keys: list[str] = [], preface: str = '', use_summary = False, include_keys: list[str] | None = None, include_performance_notes: bool = False, include_column_types: bool = True, include_column_descriptions: bool = True, include_example_values: bool = True, include_usage_tips: bool = True):
    additional_context = '## 📚 Reference Materials\n\n'
    log(f'\n\n{additional_context}')

    # log(f'state["flipside_tables"]: {state["flipside_tables"]}')
    # log(f'state["flipside_investigations"]:')
    # log(state["flipside_investigations"])
    tables = state['flipside_tables'] if len(state['flipside_tables']) > 0 else list(set(state['flipside_tables_from_example_queries'] + state['flipside_basic_table_selection'])) if len(state['flipside_tables_from_example_queries']) > 0 and len(state['flipside_basic_table_selection']) > 0 else []
    log(f'tables: {tables}')
    schema = get_flipside_schema_data(tables, include_performance_notes, include_column_types, include_column_descriptions, include_example_values, include_usage_tips)
    state['schema'] = schema
    log(f'schema length: {format(len(schema), ",")}')
    log(f"state['flipside_tables']: {state['flipside_tables']}")

    if preface:
        additional_context = additional_context + preface + '\n\n'

    if use_summary:
        exclude_keys = exclude_keys + ['tweets', 'web_search_results', 'projects', 'additional_context_summary']
        additional_context = additional_context + '**RELATED INFORMATION**: \n' + 'These are just recommendations, not requirements. Factor other information into your analysis, but use this if it is helpful:\n\n' + state['context_summary']

    # possible_keys = ['tweets_summary', 'web_search_summary', 'projects', 'flipside_example_queries','schema','transactions','additional_context_summary','other_info','program_ids','flipside_determine_approach','start_timestamp']
    possible_keys = ['tweets_summary', 'web_search_summary', 'projects', 'flipside_example_queries','schema','transactions','additional_context_summary','other_info','program_ids','flipside_determine_approach','flipside_investigations']


    if state['question_type'] == 'other':
        exclude_keys = exclude_keys + ['schema','transactions','program_ids']
        possible_keys = possible_keys + ['tweets', 'web_search']

    transaction_text = """
        - Use the following example transactions for inspiration and to understand what the correct addresses, program ids, etc. are.
        - Prioritize this information over the other reference materials, since it is provided directly from the user
        - Pay particular attention to the `programId` fields; if there is a single `programId` or the same one used multiple times, that is likely the one we want to use.

        Transactions:
    """
    indices = state['flipside_subset_example_queries']
    log(f'indices: {indices}')
    queries_text = state['flipside_example_queries'].text.apply(lambda x: x[:10000]).values if len(state['flipside_example_queries']) > 0 else []
    flipside_example_queries = ''
    indices = indices if len(indices) else [int (x) for x in range(len(queries_text))] if len(queries_text) > 0 else []
    for i in range(len(indices)):
        ind = indices[i]
        flipside_example_queries = flipside_example_queries + '### Example Query #' + str(i + 1) + ':\n' + remove_sql_comments(queries_text[ind]) + '\n\n'
    # log(f'flipside_example_queries length: {len(flipside_example_queries)}')
    def flipside_investigations_text(state: JobState):
        if len(state['flipside_investigations']) == 0:
            return ''
        val = 'Here are some light investigatory queries we have written with the results to help you understand the data. You can use this information to get a better understanding of the data and to help you write your own queries:\n\n'
        for i in state['flipside_investigations']:
            if i['load_time'] > 0:
                if len(i['result']) > 0:
                    result = i['result'].to_markdown() if len(i['result']) <= 10 else pd.concat([i['result'].head(5), i['result'].tail(5)]).to_markdown()
                    val = val + f"""Query: {i['query']}\nResult sample (first and last 5 rows): {result}\n\n"""
                elif i['error']:
                    val = val + f"""Query: {i['query']}\nError: {i['error']}\n\n"""
                else:
                    val = val + f"""Query: {i['query']}\nResult: No rows returned\n\n"""
        return val
    d = {
        'tweets_summary': ('SUMMARY OF TWEETS', '', None)
        , 'tweets': ('TWEETS', '', lambda state: '\n'.join([ str(tweet) for tweet in state['tweets']]))
        , 'web_search': ('WEB SEARCH RESULTS', '', None)
        , 'web_search_summary': ('SUMMARY OF WEB SEARCH RESULTS', '', None)
        , 'projects': ('PROJECTS', '', lambda state: '\n'.join([ str(project.name) + ': ' + str(project.description) for project in state['projects']]))
        , 'flipside_example_queries': ('RELATED FLIPSIDE QUERIES', 'Here are some example queries written by other analysts. They may not be respresent the best or most optimized way to approach your analysis, but feel free to use them for inspiration and to understand available schema and patterns, incorporating them into your query if you think they are helpful. If you know how to write the query just using the schemas and other reference materials, feel free to ignore these queries:\n\n', lambda state: flipside_example_queries)
        , 'schema': ('FLIPSIDE DATA SCHEMA', '', None)
        , 'transactions': ('EXAMPLE TRANSACTIONS', transaction_text, lambda state: '\n'.join([ str(transaction) for transaction in state['transactions']]))
        , 'additional_context_summary': ('ADDITIONAL CONTEXT', '', None)
        , 'program_ids': ('PROGRAM IDS', 'These are the program ids that will be used to analyze the user\'s analysis goal\n\n', lambda state: '\n'.join(state['program_ids']))
        , 'flipside_determine_approach': ('RECOMMENDED SQL APPROACH', 'Here is the approach we recommend to write the Flipside SQL query to analyze the user\'s analysis goal using raw tables. Generally try to adhere to this approach, but feel free to deviate if you think it is more appropriate:\n\n', None)
        , 'other_info': ('OTHER INFO', get_other_info(), None)
        , 'start_timestamp': ('START TIMESTAMP', 'Your SQL query should start at this timestamp. Prioritize this information over the other reference materials:\n\n', None)
        , 'flipside_investigations': ('SAMPLE FLIPSIDE RESULTS', '', flipside_investigations_text)
    }
    keys = [k for k in possible_keys if k not in exclude_keys]
    keys = [k for k in keys if include_keys is None or k in include_keys]
    for k in keys:
        log(k)
        new_context = ''
        if not k in state.keys():
            new_context = '\n**' + d[k][0] + '**:\n' + d[k][1] + '\n\n'
        elif len(state[k]) > 0:
            if d[k][2]:
                new_context = '\n**' + d[k][0] + '**:\n' + d[k][1] + '\n\n' + d[k][2](state)
            else:
                new_context = '\n**' + d[k][0] + '**:\n' + d[k][1] + '\n\n' + state[k]
        log(f'{k} length: {format(len(new_context), ",")}')
        additional_context = additional_context + new_context
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

def remove_sql_comments(sql):
    # Remove -- comments (from -- to end of line)
    return re.sub(r'--.*?$', '', sql, flags=re.MULTILINE).strip()