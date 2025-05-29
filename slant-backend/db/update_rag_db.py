from pinecone import Pinecone
from langchain_openai import OpenAIEmbeddings
from utils.db import pg_load_data, pc_load_data, pc_upload_data, get_content_for_twitter_kol, load_tweets_for_pc, clean_tweets_for_pc, pg_upsert_data
from constants.db import PROJECTS_RAG_COLS, TWITTER_KOLS_RAG_COLS, TWEETS_RAG_COLS, FLIPSIDE_QUERIES_RAG_COLS
from constants.keys import POSTGRES_ENGINE
from utils.utils import log
from utils.db import pg_upload_data
import requests
from bs4 import BeautifulSoup
import re
import json
import time
from langchain.schema import SystemMessage, HumanMessage
from langchain_openai import ChatOpenAI
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import Column, BigInteger, Text, MetaData, Table
from utils.flipside import scrape_all_flipside_queries, summarize_query

def upload_projects_to_rag():
    # Query projects from Postgres
    query = """
        SELECT 
            id,
            name,
            description,
            coalesce(ecosystem, 'solana') as ecosystem,
            tags,
            coalesce(score, 0) as score
        FROM projects
    """
    projects_df = pg_load_data(query)
    projects_df['ecosystem'].unique()
    projects_df['tags'] = projects_df['tags'].apply(lambda x: x if x and x == x else [])
    # projects_df = projects_df.head()
    # log('\n'.join(('\n'+'='*20+'\n').join([str(x) for x in projects_df.to_dict('records')]).split('\n')))
    seen = pc_load_data('slant', 'projects')
    seen_ids = seen.id.unique()
    projects_df = projects_df[~projects_df.id.isin([int(x) for x in seen_ids])]

    projects_df['content'] = projects_df.apply(lambda row: f"""Project: {row['name']} 
    Description: {row['description']} 
    Ecosystem: {row['ecosystem']} 
    Tags: {row['tags']}""", 1)
    # log(projects_df.content.values[0])

    pc_upload_data(projects_df, 'content', PROJECTS_RAG_COLS, index_name='slant', namespace='projects', truncate=True)
    

def upload_twitter_kols_to_rag():
    seen = pc_load_data('slant', 'twitter_kols')
    seen_ids = seen.id.unique()
    
    # Query projects from Postgres
    query = """
        SELECT 
            tk.*
            , p.name as project_name
        FROM twitter_kols tk
        left join projects p
            on tk.associated_project_id = p.id
    """
    twitter_kols_df = pg_load_data(query)
    twitter_kols_df = twitter_kols_df[~twitter_kols_df.id.isin([int(x) for x in seen_ids])]
    twitter_kols_df['content'] = twitter_kols_df.apply(lambda row: get_content_for_twitter_kol(row), axis=1)

    twitter_kols_df.associated_project_id = twitter_kols_df.associated_project_id.fillna(0).astype(int)

    pc_upload_data(twitter_kols_df, 'content', TWITTER_KOLS_RAG_COLS, index_name='slant', namespace='twitter_kols')

def upload_tweets_to_rag():
    seen = pc_load_data('slant', 'tweets')
    seen_ids = seen.id.unique()

    df = load_tweets_for_pc(0)
    df = df[~df.id.isin([int(x) for x in seen_ids])]
    len(df)
    df = clean_tweets_for_pc(df)
    pc_upload_data(df, 'text', TWEETS_RAG_COLS, batch_size=100, index_name='slant', namespace='tweets')

def migrate_rag_data():
    queries = pc_load_data('flipsidesql', '')
    queries.head(1).metadata.values[0].keys()
    for query in queries:
        df = pg_load_data(query)
        pc_upload_data(df, 'text', TWEETS_RAG_COLS, batch_size=100, index_name='slant', namespace='flipside_queries')

def create_flipside_queries_table():
    queries_df = scrape_all_flipside_queries()

    # load existing queries from pinecone
    existing_queries = pc_load_data('flipsidesql', '')
    existing_queries.metadata.values[0]
    existing_queries_df = pd.DataFrame(list(existing_queries.metadata.values))
    existing_queries_df['id'] = existing_queries['id'].values
    print(existing_queries_df.head())
    print(existing_queries_df.columns)
    print(existing_queries_df.timestamp.max())
    exclude = existing_queries.id.unique()
    len(queries_df[queries_df.id.isin(exclude)])
    len(queries_df)
    queries_df = queries_df[~queries_df.id.isin(exclude)]
    queries_df.to_csv('flipside_queries_to_scrape.csv', index=False)

    # create summaries for queries
    queries_df['summary'] = ''
    todo = queries_df[(queries_df.summary == '')].reset_index(drop=True)
    total = len(todo)
    for i, row in todo.iterrows():
        # row = row[1]
        print(f'{i}/{total} - ID: {row.id} - {row.name}')
        print(row.statement)
        summary = summarize_query(f'Query Title: {row.name} \n ===== \n Query Statement: {row.statement}')
        print(f'ID: {row.id} - {row.name}')
        print(summary)
        print('-'*100)
        print('')
        queries_df.loc[queries_df.id == row.id, 'summary'] = summary
    len(queries_df.summary.unique())
    queries_df.head(1).to_dict('records')
    existing_queries_df.head(1).to_dict('records')

    cols = ['id','text','slug','slugId','statement','createdAt','updatedAt','lastSuccessfulExecutionAt','user','user_id','name']

    user_ids = pd.read_csv('~/Downloads/user_ids.csv')
    a = existing_queries_df.rename(columns={'timestamp': 'createdAt'})
    a = pd.merge(a, user_ids, on='user_id', how='left')
    a['updatedAt'] = a['createdAt']
    a['lastSuccessfulExecutionAt'] = a['createdAt']
    for c in cols:
        if c not in a.columns:
            a[c] = ''
    a = a[cols]
    a.head(1).to_dict('records')
    

    b = queries_df.rename(columns={'createdById': 'user_id'})
    b['text'] = b.apply(lambda row: f'Query Title: {row.name} \n ===== \n Query Summary: {row.summary} \n ===== \n Query Statement: {row.statement}', axis=1)
    for c in ['createdAt', 'updatedAt', 'lastSuccessfulExecutionAt']:
        b[c] = pd.to_datetime(b[c]).dt.strftime('%Y-%m-%d %H:%M:%S')
    b.head(1).to_dict('records')

    # combine existing and new queries
    b = pd.concat([a, b[cols]])
    b.head(1).to_dict('records')

    rename_columns = {
        'lastSuccessfulExecutionAt': 'last_successful_execution_at'
        , 'createdAt': 'created_at'
        , 'updatedAt': 'updated_at'
        , 'user': 'user_name'
    }
    b = b.rename(columns=rename_columns)
    b.head(1).to_dict('records')
    b['user_name'] = b['user_name'].fillna('')
    b = b[b.last_successful_execution_at.notnull()]
    
    pc_upload_data(b, 'text', FLIPSIDE_QUERIES_RAG_COLS, index_name='slant', namespace='flipside_queries')

    engine = create_engine(POSTGRES_ENGINE)
    pg_upsert_data(b, 'flipside_queries', engine)


def scrape_flipside_queries():
    
    url = 'https://flipsidecrypto.xyz/kellen/q/-7GCknz0WUcy/stupid-amber--_lN9l'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    val = extract_query_info(r.text)
    print(val['query_statement'])
    query_text = soup.find_all('div')
    for i in range(len(query_text)):
        print(i)
        print(query_text[i].text)
        print('-'*100)
    len(query_text)
    query_text = soup.find_all('div', {'dir': 'ltr'})
    # .text
    query_text = soup.find_all('div', class_='cm-content').text
    queries = pc_load_data('flipsidesql', '')
    for query in queries:
        df = pg_load_data(query)
        pc_upload_data(df, 'text', TWEETS_RAG_COLS, batch_size=100, index_name='slant', namespace='flipside_queries')

def convert_to_timestamp():
    query = 'select * from flipside_queries'
    df = pg_load_data(query)
    df['created_at'] = (pd.to_datetime(df['created_at']).astype('int64') // 1e9).astype(int)
    df['last_successful_execution_at'] = (pd.to_datetime(df['last_successful_execution_at']).astype('int64') // 1e9).astype(int)
    df.created_at.max()
    df.created_at.min()
    pc_upload_data(df, FLIPSIDE_QUERIES_RAG_COLS, ['id'], index_name='slant', namespace='flipside_queries')

    index_name = 'slant'
    namespace='flipside_queries'

    existing_vectors = pc_load_data(index_name, namespace)
    d = {}
    for row in existing_vectors.iterrows():
        d[row[1]['id']] = row[1]['values']
    vectors = []
    for row in df.itertuples():
        metadata = {
            "created_at": int(row.created_at),
            "last_successful_execution_at": int(row.last_successful_execution_at),
            "text": row.text,
            "project_tags": row.project_tags,
            "user_name": row.user_name,
            "user_id": row.user_id
        }
        vectors.append({
            "id": row.id,
            "values": d.get(row.id, []),
            "metadata": metadata
        })

    pc = Pinecone(api_key=PINECONE_API_KEY)
    index = pc.Index(index_name)
    for i in range(6500, len(vectors), 100):
        print(f'{i}/{len(vectors)}')
        batch = vectors[i:i + 100]
        batch = [v for v in batch if len(v['values']) > 0]
        index.upsert(batch, namespace=namespace)
