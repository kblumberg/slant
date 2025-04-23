import pandas as pd
from utils.utils import log
from classes.JobState import JobState
from pinecone import Pinecone
from langchain_openai import OpenAIEmbeddings
from constants.keys import PINECONE_API_KEY
from tavily import TavilyClient


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
    projects = list(set([ x.project for x in state['analyses']]))
    activities = list(set([ x.activity for x in state['analyses']]))
    tokens = list(set([token for x in state['analyses'] for token in x.tokens]))
    query = list(set(projects + activities + tokens))
    refined_prompt = ' '.join(query)
    return refined_prompt

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
        - The `solana.price.ez_token_prices_hourly` table is deprecated. Use `solana.price.ez_prices_hourly` instead.
        - Use example queries for structure and patterns, but tailor them to the user’s question.
        - Use `block_timestamp` filtering where applicable.
        - Avoid `SELECT *` and limit columns to what is needed.
        - Use `GROUP BY` when using aggregate functions.
        - Make sure token addresses and program IDs are correct (cross-reference with examples).
        - Ensure the query performs well by limiting time range and data volume where possible.
        - Return ONLY the raw SQL. No explanation, markdown, or formatting.
        - Any time-based column should be aliased as `date_time`.
        - Any categorical column should be aliased as `category`.
        - If you are doing some kind of running total, make sure there are no gaps in the data by using the `crosschain.core.dim_dates` table.
        - If the analysis is "to now", "to present", etc., you don't need to do a <= `block_timestamp` filter.
        - Consider the "Notes" within the schema listed.
        - Tokens are typically filtered by `token_address` or `mint` or `___mint` in the schema.
        - Programs are typically filtered by `program_id` in the schema.
        - Do NOT use any placeholders. Only use real mints, addresses, program ids, dates, etc.
    """

def get_other_info():
    return """
        ## Other Info
        - For tokens, make sure to have the correct mint / token address.
        - Unless specified otherwise, you can assume that prices are measured in USD.
    """


def state_to_reference_materials(state: JobState, exclude_keys: list[str] = [], preface: str = '', use_summary = False):
    additional_context = '## 📚 Reference Materials\n\n'
    if use_summary:
        exclude_keys = exclude_keys + ['tweets', 'web_search_results', 'projects', 'additional_contexts']
        additional_context = additional_context + '**RELATED INFORMATION**: \n' + 'Factor this into your analysis, along with the other information provided:\n\n' + state['context_summary']
    if preface:
        additional_context = additional_context + preface + '\n\n'
    if len(state['tweets']) > 0 and 'tweets' not in exclude_keys:
        additional_context = additional_context + '**TWEETS**: \n' + '\n'.join([ str(tweet.text) for tweet in state['tweets']])
    if len(state['web_search_results']) > 0 and 'web_search_results' not in exclude_keys:
        additional_context = additional_context + '**WEB SEARCH RESULTS**: \n' + state['web_search_results']
    if len(state['projects']) > 0 and 'projects' not in exclude_keys:
        additional_context = additional_context + '**PROJECTS**: \n' + '\n'.join([ str(project.name) + ': ' + str(project.description) for project in state['projects']])
    if len(state['flipside_example_queries']) > 0 and 'flipside_example_queries' not in exclude_keys:
        example_queries = '\n\n'.join(state['flipside_example_queries'].text.apply(lambda x: x[:10000]).values)
        additional_context = additional_context + '**RELATED FLIPSIDE QUERIES**: \n' + 'Use the following example queries for inspiration and to understand available schema and patterns:\n\n' + example_queries
    if len(state['schema']) > 0 and 'schema' not in exclude_keys:
        additional_context = additional_context + '**FLIPSIDE DATA SCHEMA**: \n' + state['schema'] + '\n\n' + get_sql_notes()
        additional_context = additional_context + get_sql_notes()
    if len(state['additional_contexts']) > 0 and 'additional_contexts' not in exclude_keys:
        additional_context = additional_context + '**ADDITIONAL CONTEXT**: \n' + '\n'.join(state['additional_contexts'])
    additional_context = additional_context + '**OTHER INFO**: \n' + get_other_info()
    return additional_context

def get_web_search(question: str, tavily_client: TavilyClient) -> str:
    web_search_results = tavily_client.search('solana blockchain ' + question, search_depth="advanced", include_answer=True, include_images=False, max_results=5, include_raw_content=True)
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

