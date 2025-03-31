import re
import json
import time
import requests
import pandas as pd
from constants.db import FLIPSIDE_QUERIES_RAG_COLS
from utils.db import pg_upsert_data, pg_load_data, pc_upload_data
from constants.keys import POSTGRES_ENGINE, PINECONE_API_KEY
from sqlalchemy import create_engine, MetaData, Table, Column, BigInteger, Text, ARRAY
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain.schema import SystemMessage, HumanMessage
from pinecone import Pinecone
from langchain_openai import OpenAIEmbeddings
from utils.db import pg_upload_data


def generate_query_text(row: pd.Series) -> str:
    text = f"Query Summary: {row['summary']}"
    for c in [('Query Title', 'name'), ('Dashboard Title', 'dashboard_title'), ('Dashboard Description', 'dashboard_description'), ('Dashboard Tags', 'dashboard_tags'), ('Query Statement', 'statement')]:
        if row[c[1]] and row[c[1]] == row[c[1]] and len(row[c[1]]) > 2:
            text += f"\n======\n{c[0]}: {row[c[1]]}"
    return text.strip()[:40000]

def generate_summary_prompt_for_fs_query(row: pd.Series) -> str:
    text = f"Query Title: {row['name']}"
    for c in [('Dashboard Title', 'dashboard_title'), ('Dashboard Description', 'dashboard_description'), ('Dashboard Tags', 'dashboard_tags'), ('Query Statement', 'statement')]:
        if row[c[1]] and row[c[1]] == row[c[1]] and len(row[c[1]]) > 2:
            text += f"\n======\n{c[0]}: {row[c[1]]}"
    return text[:40000]

def scrape_new_flipside_dashboards(n_pages: int = 1):
    dashboards = []
    dashboard_queries = []
    for page in range(n_pages):
        print(f'Page {page} of {n_pages + 1} ({len(dashboards)} dashboards and {len(dashboard_queries)} queries found so far)')
        # if page > 8 and page < 24:
        #     time.sleep(5)
        url = f'https://flipsidecrypto.xyz/insights/dashboards/solana?sortBy=new{"&page=" + str(page + 1) if page > 0 else ""}'
        r = requests.get(url)
        items = re.split(r'window\.__remixContext = (.*?});', r.text, re.DOTALL)
        j = json.loads(items[1])
        for dashboard in j['state']['loaderData']['routes/__shell/insights/dashboards.$project']['items']:
            dashboard_id = dashboard.get('id')
            dashboard_subset = {
                'id': dashboard_id,
                'title': dashboard.get('title'),
                'latest_slug': dashboard.get('latestSlug'),
                'description': dashboard.get('description'),
                'updated_at': dashboard.get('updatedAt'),
                'created_at': dashboard.get('createdAt'),
                'created_by_id': dashboard.get('createdById'),
                'tags': [ x['name'] for x in dashboard.get('tags') if not x in ['solana']],
                'user_id': dashboard.get('createdById'),
                'user_name': dashboard.get('profile', {}).get('user', {}).get('username') if dashboard.get('profile', {}).get('user') else dashboard.get('profile', {}).get('team', {}).get('name')
            }
            dashboards.append(dashboard_subset)
            qs = []
            if 'publishedConfig' in dashboard.keys() and dashboard['publishedConfig']:
                for k, v in dashboard['publishedConfig']['contents'].items():
                    if 'queryId' in v.keys():
                        qs.append(v['queryId'])
            qs = list(set(qs))
            dashboard_queries += [[q, dashboard_id] for q in qs]
    dashboards_df = pd.DataFrame(dashboards)
    dashboards_df['updated_at'] = pd.to_datetime(dashboards_df['updated_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
    dashboards_df['created_at'] = pd.to_datetime(dashboards_df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
    dashboards_df = dashboards_df.drop_duplicates(subset=['id'], keep='last')
    # pg_upload_data(dashboards_df, 'flipside_dashboards')
    dashboard_queries_df = pd.DataFrame(dashboard_queries, columns=['query_id', 'dashboard_id']).drop_duplicates(subset=['query_id', 'dashboard_id'], keep='first')
    # pg_upload_data(dashboard_queries_df, 'flipside_dashboards_queries')

    return dashboards_df, dashboard_queries_df

def scrape_new_flipside_queries(n_pages: int = 1):
    queries_df = pd.DataFrame()
    for i in range(n_pages):
        url = f'https://flipsidecrypto.xyz/insights/queries/solana?sortBy=new{"&page=" + str(i+1) if i > 0 else ""}'
        print(f'Page {i+1} of {n_pages}: {len(queries_df)} queries found so far.')
        time.sleep(5)
        r = requests.get(url)
        items = re.split(r'window\.__remixContext = (.*?});', r.text, re.DOTALL)
        len(items)
        j = json.loads(items[1])
        j['state']['loaderData'].keys()
        j['state']['loaderData']['routes/__shell/insights/queries.$project']['items'][0]
        items = j['state']['loaderData']['routes/__shell/insights/queries.$project']['items']
        cols = ['id','name','slug','slugId','latestSlug','statement','resultLastAccessedAt','lastSuccessfulExecutionAt','createdById','createdAt','updatedAt','forkedFromId','profile']
        cur = pd.DataFrame(items)[cols]
        cur['user_name'] = cur.profile.apply(lambda x: x['user']['username'] if x['user'] is not None else x['team']['name'])
        del cur['profile']
        queries_df = pd.concat([queries_df, cur]).drop_duplicates(subset=['id'])

    return queries_df

def scrape_all_flipside_queries():
    users = ['0xHaM-d', 'damidez', 'heliusresearch', 'Saleh', 'bintuparis', 'jsbmudit', 'seckinss', 'tkvresearch', 'Ario', 'dethective', 'feyikemi', 'Motilola', 'superfly', 'gigiokoba', 'datavortex', 'DataDriven_Web3', 'dannyamah', 'hrst79', 'saeedmzn', 'HitmonleeCrypto', 'marqu', 'Masi', 'ahkek76', 'flyingfish', 'cryptotiosam', 'CyberaResearch', 'steven-sabol-dir-of-economy', 'Aephia', 'Afonso_Diaz', 'pine', 'MostlyData_', 'Sbhn_NP', 'Hessish', 'staccoverflow-QGHtL7', 'piper', 'crypto_edgar', 'h4wk', 'kellen', 'mrwildcat', 'jupdevrel', 'chispas', 'MetaLight', 'zpencer','0xallyzach','Sajjadiii','solwhitey','banx','tweb3girl','0xsloane','dogi','tarikflipside','srijani','zackmendel','lj1024','sir_ambrose','snowdev','0xBlackfish','zapokorny','goatindex','Axl_Cast-d3zsuX','nebulalabs','toly','Kilann']
    users = ['kellen']
    users = ['dethective', 'feyikemi', 'Motilola', 'superfly', 'gigiokoba', 'datavortex', 'DataDriven_Web3', 'dannyamah', 'hrst79', 'saeedmzn', 'HitmonleeCrypto', 'marqu', 'Masi', 'ahkek76', 'flyingfish', 'cryptotiosam', 'CyberaResearch', 'steven-sabol-dir-of-economy', 'Aephia', 'Afonso_Diaz', 'pine', 'MostlyData_', 'Sbhn_NP', 'Hessish', 'staccoverflow-QGHtL7', 'piper', 'crypto_edgar', 'h4wk', 'mrwildcat', 'jupdevrel', 'chispas', 'MetaLight', 'zpencer','0xallyzach','Sajjadiii','solwhitey','banx','tweb3girl','0xsloane','dogi','tarikflipside','srijani','zackmendel','lj1024','sir_ambrose','snowdev','0xBlackfish','zapokorny','goatindex','Axl_Cast-d3zsuX','nebulalabs','toly','Kilann']
    queries_df = pd.DataFrame()
    seen = queries_df.user.unique() if len(queries_df) > 0 else []
    for user in users:
        if user in seen:
            continue
        print(f'scraping {user}. {len(queries_df)} queries found so far.')
        has_more = True
        for page in range(1, 10):
            if has_more:
                url = f'https://flipsidecrypto.xyz/{user}/queries/solana?{"page=" + str(page) if page > 1 else ""}&sortBy=new'
                if user in ['pine','toly']:
                    url = f'https://flipsidecrypto.xyz/teams/{user}/queries/solana?{"page=" + str(page) if page > 1 else ""}&sortBy=new'
                print(url)
                time.sleep(5)
                r = requests.get(url)
                items = re.split(r'window\.__remixContext = (.*?});', r.text, re.DOTALL)
                len(items)
                j = json.loads(items[1])
                # j.keys()
                j['state']['loaderData'].keys()
                # j['state']['loaderData']['routes/__shell/$owner/__profile/queries.$project'].keys()
                # j['state']['loaderData']['routes/__shell/teams/$owner'].keys()
                # j['state']['loaderData']['routes/__shell'].keys()
                # j['state']['loaderData']['routes/__shell/teams/$owner/dashboards'].keys()
                try:
                    queries = j['state']['loaderData']['routes/__shell/$owner/__profile/queries.$project']['items']
                except:
                    try:
                        queries = j['state']['loaderData']['routes/__shell/$owner/__profile/queries']['items']
                    except:
                        try:
                            queries = j['state']['loaderData']['routes/__shell/teams/$owner/queries.$project']['items']
                        except:
                            print('Error with user', user)
                            continue
                if len(queries) == 0:
                    has_more = False
                    continue
                cols = ['id','name','slug','slugId','latestSlug','statement','resultLastAccessedAt','lastSuccessfulExecutionAt','createdById','createdAt','updatedAt']
                cur = pd.DataFrame(queries)[cols]
                cur['user'] = user
                queries_df = pd.concat([queries_df, cur])
            

    cur = queries_df.rename(columns={'createdById': 'user_id'})
    for c in ['createdAt', 'updatedAt', 'lastSuccessfulExecutionAt']:
        cur[c] = pd.to_datetime(cur[c]).dt.strftime('%Y-%m-%d %H:%M:%S')
    cur.head(1).to_dict('records')

    rename_columns = {
        'lastSuccessfulExecutionAt': 'last_successful_execution_at'
        , 'createdAt': 'created_at'
        , 'updatedAt': 'updated_at'
        , 'user': 'user_name'
        , 'slugId': 'slug_id'
    }
    cur = cur.rename(columns=rename_columns)
    cur.head(1).to_dict('records')
    cur['user_name'] = cur['user_name'].fillna('')
    cur = cur[cur.last_successful_execution_at.notnull()]
    # cur = cur[cur.updated_at >= '2025-01-01']

    return cur

def add_summaries_to_queries(queries: pd.DataFrame) -> pd.DataFrame:
    queries = queries.reset_index(drop=True)
    queries['summary'] = ''
    total = len(queries)
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    for i, row in queries.iterrows():
        # row = row[1]
        print(f'{i}/{total} - ID: {row.id} - Name: {row["name"]}')
        # summary_text = f"Query Title: {row['name']} \n ===== \n Query Statement: {row.statement}"
        # summary_text = f"Query Title: {row['query_title']}\n======\nDashboard Title: {row['dashboard_title']}\n======\nDashboard Description:\n{row['dashboard_description']}\n======\nDashboard Tags: {row['dashboard_tags']}\n======\nQuery Statement:\n{row['query_statement']}"
        # print(summary_text[:400])
        summary = summarize_query(row['summary_prompt'], llm)
        print(f'ID: {row.id} - Name: {row["name"]}')
        print(summary)
        print('='*100)
        print('')
        queries.loc[queries.id == row.id, 'summary'] = summary

    queries['text'] = queries.apply(lambda row: f"Query Summary: {row['summary']}\n======\nQuery Title: {row['query_title']}\n======\nDashboard Title: {row['dashboard_title']}\n======\nDashboard Description:\n{row['dashboard_description']}\n======\nDashboard Tags: {row['dashboard_tags']}\n======\nQuery Statement:\n{row['query_statement']}", axis=1)
    return queries

def upsert_flipside_queries(cur):
    engine = create_engine(POSTGRES_ENGINE)
    metadata = MetaData()
    cols = ['id','text','slug','slug_id','statement','summary','created_at','updated_at','last_successful_execution_at','user_name','user_id','name','project_tags']
    cur = cur[cols]
    flipside_queries_table = Table(
        "flipside_queries", metadata,
        Column("id", BigInteger),
        Column("text", Text),
        Column("slug", Text),
        Column("slug_id", Text),
        Column("statement", Text),
        Column("summary", Text),
        Column("created_at", Text),
        Column("updated_at", Text),
        Column("last_successful_execution_at", Text),
        Column("user_name", Text),
        Column("user_id", Text),
        Column("name", Text),
        Column("project_tags", ARRAY(Text)),
    )
    pg_upsert_data(cur, flipside_queries_table, engine)

    return True


def summarize_query(sql_query: str, llm: ChatOpenAI | ChatAnthropic) -> str:
    """Generate a concise summary of an SQL query for a RAG database."""
    # Instantiate LLM globally for efficiency (if used repeatedly)
    # llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    # Static system message (no need to reformat every call)
    system_prompt = """You are a blockchain research expert. 
    Summarize the given SQL query in 1-2 sentences optimized for a RAG database.
    If there are specifics about the project, platform, token, or topic the query is about in the title, include that.
    Often the program id or token address filters in the WHERE clause are also relevant.
    Only include names of things in the summary, not addresses or ids.
    If the dashboard title or query title is relevant, factor it in to the summary.
    Don't include anything like "Here's a summary of the query" or "The SQL query..." or "This query..." - just return the summary."""
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=f"Summarize this SQL query: {sql_query}")
    ]
    
    try:
        return llm(messages).content
    except Exception as e:
        return f"Error generating summary: {e}"

def upload_flipside_queries_to_pinecone():
    query = 'select * from flipside_queries where name is not null'
    df = pg_load_data(query)

    queries = df.copy()
    cur = queries[queries.name != '']
    def f(x):
        s = re.split('=====', x['text'])
        text = f"Query Title: {x['name']} \n \n ===== \n {s[1]} \n===== \n Query Statement: \n \n {s[2][19:]}"
        return text
    cur['text'] = cur.apply(lambda x: f(x), 1)
    upsert_flipside_queries(cur)
    pg_upsert_data(cur[['text', 'tmp']], 'flipside_queries', if_exists='append')
    # cur = 
    # queries['text'] = queries.apply(lambda row: f'Query Title: {row.name} \n ===== \n Query Summary: {row.summary} \n ===== \n Query Statement: {row.statement}', axis=1)

    query = 'select * from flipside_queries'
    df = pg_load_data(query)
    df['text'] = df.text.apply(lambda x: x[:35000])
    # df[df.id == 'fa97ea3c-6767-4223-8622-2e711a0cecdd']

    # g = df.copy()
    # g['tmp'] = g.text.apply(lambda x: len(x))
    # g = g.sort_values('tmp', ascending=0)
    # g.head(10)[['tmp']]

    # len(g[g.tmp >= 30000])
    # df[FLIPSIDE_QUERIES_RAG_COLS].count()
    df = df[df.last_successful_execution_at.notnull()]


    pc_upload_data(df, 'text', FLIPSIDE_QUERIES_RAG_COLS, namespace='flipside_queries')
    pc_upload_data(df[df.id == 'fa97ea3c-6767-4223-8622-2e711a0cecdd'], 'text', FLIPSIDE_QUERIES_RAG_COLS)

def clean_project_tag(tag: str) -> str:
    try:
        tag = tag.lower()
        phrases = [' ','.gg', '$', '_']
        for phrase in phrases:
            tag = tag.replace(phrase, '')
        phrases = ['jupiter lfg','famous fox']
        for phrase in phrases:
            if tag[:len(phrase)] == phrase:
                tag = phrase
        phrases = ['finance','fi','protocol','network']
        for phrase in phrases:
            if tag[-len(phrase):] == phrase:
                tag = tag.replace(phrase, '')
        tag = tag.replace('.', '')
        d = {}
        if tag in d.keys():
            tag = d[tag]
        return tag
    except Exception as e:
        print(f"Error cleaning project tag: {e}")
        return tag

def clean_project_tags(tags: str) -> list[str]:
    try:
        tags = tags.replace('```json', '').replace('```', '').strip()
        tags = json.loads(tags)
        tags = [clean_project_tag(x) for x in tags]
        tags = [x for x in tags if not x in ['solana']]
        return tags
    except Exception as e:
        print(f"Error cleaning project tags: {e}")
        return []

def extract_project_tags_from_user_prompt(query: str, llm: ChatOpenAI | ChatAnthropic) -> list[str]:
    system_prompt = """
        You are a blockchain research assistant.

        Your job is to extract **project names** mentioned or referred to in a block of text. These are usually protocols, platforms, or dApps.

        Only include **real, recognizable project names** that the text is directly related to or analyzing. Use your knowledge of the crypto ecosystem to infer implied project names if they're clear from context (e.g. from titles or tags), but don't guess.

        If possible, keep tags to a single word.

        Limit the total number of tags to 5-10 at most.

        Output must be a **JSON list of strings**, and nothing else.

        Do NOT include:
        - Token addresses
        - Vague terms like “platform” or “protocol”
        - Empty strings
    """

    human_prompt = f"""
        Text:
        {query}

        Return the JSON list of crypto project names mentioned or analyzed:
    """ 
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=human_prompt)
    ]
    try:
        response = llm(messages).content
        print('response')
        print(response)
        tags = clean_project_tags(response)
        return tags
    except Exception as e:
        print(f"Error parsing project list: {e}")
        return []

def extract_project_tags_from_query(query: str, llm: ChatOpenAI | ChatAnthropic) -> list[str]:
    system_prompt = """
        You are a blockchain research assistant.

        Your job is to extract **project names** mentioned or referred to in a block of text. These are usually protocols, platforms, or dApps.

        They are often mentioned in the query summary, query title, dashboard title, dashboard description, SQL code comments, SQL table aliases or table names.

        Only include **real, recognizable project names** that the text is directly related to or analyzing. Use your knowledge of the crypto ecosystem to infer implied project names if they're clear from context (e.g. from titles or tags), but don't guess.

        If possible, keep tags to a single word.

        Limit the total number of tags to 5-10 at most.

        Output must be a **JSON list of strings**, and nothing else.

        Do NOT include:
        - Token addresses
        - Vague terms like “platform” or “protocol”
        - Empty strings
    """

    human_prompt = f"""
        Text:
        {query}

        Return the JSON list of crypto project names mentioned or analyzed:
    """ 
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=human_prompt)
    ]
    try:
        response = llm(messages).content
        print('response')
        print(response)
        tags = clean_project_tags(response)
        return tags
    except Exception as e:
        print(f"Error parsing project list: {e}")
        return []

# llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
# prompts = [
#     'What has the trading volume been on Jupiter since January 1st 2025 to now?',
#     'How has TVL grown over the past year on loopscale?'
# ]

# for prompt in prompts:
#     tags = extract_project_tags_from_user_prompt(prompt, llm)
#     print(prompt)
#     print(tags)
#     print('='*100)
#     print('')
