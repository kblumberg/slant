import math
import pandas as pd
from datetime import datetime
from slack_sdk import WebClient
from constants.keys import SLACK_TOKEN, POSTGRES_ENGINE
from constants.db import FLIPSIDE_QUERIES_RAG_COLS
from constants.constant import SLACK_CHANNEL_ID, KELLEN_SLACK_ID
from utils.db import pg_load_data, pg_upload_data, pc_upload_data, pg_upsert_data, pg_upsert_flipside_dashboards, pg_upsert_flipside_dashboards_queries
from utils.flipside import scrape_new_flipside_queries, upsert_flipside_queries, scrape_new_flipside_dashboards, generate_summary_prompt_for_fs_query, summarize_query, generate_query_text, extract_project_tags_from_query
from langchain_openai import ChatOpenAI

def update_flipside_data():
    client = WebClient(token=SLACK_TOKEN)
    try:

        query = 'select max(updated_at) as updated_at from flipside_queries'
        df = pg_load_data(query)
        # df['updated_at'] = pd.to_datetime(df.updated_at).apply(lambda x: int(x.timestamp()))
        fq_mx = df.updated_at.max()

        query = 'select max(updated_at) as updated_at from flipside_dashboards'
        fd = pg_load_data(query)
        mx_fd = fd.updated_at.max()
        mx = min(fq_mx, mx_fd)

        hours_ago = math.ceil((int(datetime.now().timestamp()) - int(mx.timestamp()) ) / 3600)

        n_fd_pages = math.ceil(hours_ago / 10) + 2
        n_pages = math.ceil(hours_ago / 6) + 2

        print(f'Most recent query was {hours_ago} hours ago, scraping {n_pages} pages')

        queries_df = scrape_new_flipside_queries(n_pages=n_pages)
        rename_columns = {
            'lastSuccessfulExecutionAt': 'last_successful_execution_at'
            , 'createdAt': 'created_at'
            , 'updatedAt': 'updated_at'
            , 'createdById': 'user_id'
            , 'user': 'user_name'
            , 'slugId': 'slug_id'
        }
        queries_df = queries_df.rename(columns=rename_columns)
        # df = pg_load_data(query)

        for c in ['updated_at', 'created_at', 'last_successful_execution_at']:
            queries_df[c] = pd.to_datetime(queries_df[c]).dt.strftime('%Y-%m-%d %H:%M:%S')

        print(f'Most recent dashboard was {hours_ago} hours ago, scraping {n_fd_pages} pages')

        dashboards_df, dashboard_queries_df = scrape_new_flipside_dashboards(n_pages=n_fd_pages)

        valid_queries = pd.merge(queries_df, dashboard_queries_df.rename(columns={'query_id': 'id'}), on='id', how='left').drop_duplicates(subset=['id'], keep='last')
        print(f'v0. {len(valid_queries[(valid_queries.updated_at >= str(mx)) ])} valid queries found')

        cols = ['id', 'title', 'description', 'updated_at', 'tags']
        rename_columns = {
            'id': 'dashboard_id'
            , 'title': 'dashboard_title'
            , 'description': 'dashboard_description'
            , 'tags': 'dashboard_tags'
            , 'updated_at': 'dashboard_updated_at'
        }
        d_2 = dashboards_df[cols].rename(columns=rename_columns)
        valid_queries = pd.merge(valid_queries, d_2, on='dashboard_id', how='left')

        cols = ['id','slug','slug_id','statement','created_at','updated_at','last_successful_execution_at','user_name','user_id','name','dashboard_id','dashboard_title','dashboard_description','dashboard_tags','dashboard_updated_at']
        valid_queries = valid_queries[(valid_queries.updated_at >= str(mx)) | (valid_queries.dashboard_updated_at >= str(mx))][cols].reset_index(drop=True)
        print(f'v1. {len(valid_queries)} valid queries found')

        valid_queries['summary_prompt'] = valid_queries.apply(lambda x: generate_summary_prompt_for_fs_query(x), axis=1)

        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        valid_queries['summary'] = ''
        valid_queries = valid_queries.reset_index(drop=True)
        tot = len(valid_queries[valid_queries.summary == ''])
        for i, row in valid_queries[valid_queries.summary == ''].iterrows():
            summary = summarize_query(row['summary_prompt'], llm)
            valid_queries.loc[i, 'summary'] = summary
            print(f'{i}/{tot}')

        valid_queries['text'] = valid_queries.apply(lambda x: generate_query_text(x), axis=1)
        valid_queries['project_tags'] = valid_queries.apply(lambda x: extract_project_tags_from_query(x['text'], llm), axis=1)
        # print(valid_queries[valid_queries.dashboard_updated_at.notnull()].text.values[0])

        # upload_queries = pd.merge(valid_queries, q_2[['id', 'text', 'summary']], on='id', how='left')
        # len(valid_queries)
        # len(q_2)
        upsert_flipside_queries(valid_queries)
        pc_data = valid_queries.copy()
        pc_data['text'] = pc_data['text'].apply(lambda x: x[:35000])
        for c in ['created_at', 'updated_at', 'last_successful_execution_at']:
            pc_data[c] = (pd.to_datetime(pc_data[c]).astype('int64') // 1e9).astype(int)
        pc_data.loc[pc_data.project_tags.isnull(), 'project_tags'] = pc_data.project_tags.apply(lambda x: [])
        pc_data['dashboard_id'] = pc_data['dashboard_id'].fillna('')

        pc_upload_data(pc_data, 'text', FLIPSIDE_QUERIES_RAG_COLS, batch_size=100, index_name='slant', namespace='flipside_queries')

        pg_upsert_flipside_dashboards(dashboards_df)
        pg_upsert_flipside_dashboards_queries(dashboard_queries_df)
        client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=f"Updated {len(valid_queries)} flipside queries and {len(dashboards_df)} dashboards")
        return len(valid_queries)
        
    except Exception as e:
        print(f'error: {e}')
        # Send Slack DM about the error
        client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=f"<@{KELLEN_SLACK_ID}> Error in update_flipside_data: {str(e)}")
    return 0

def update_flipside_queries_from_pinecone():
    query = "select q.*, d.dashboard_id from flipside_queries q left join flipside_dashboards_queries d on q.id = d.query_id"
    df = pg_load_data(query).drop_duplicates(subset=['id'], keep='last')
    df['created_at'] = (pd.to_datetime(df['created_at']).astype('int64') // 1e9).astype(int)
    df['updated_at'] = (pd.to_datetime(df['updated_at']).astype('int64') // 1e9).astype(int)
    df['last_successful_execution_at'] = (pd.to_datetime(df['last_successful_execution_at']).astype('int64') // 1e9).astype(int)
    df[['created_at', 'updated_at', 'last_successful_execution_at']]
    df['text'] = df['text'].apply(lambda x: x[:35000])
    df['dashboard_id'] = df['dashboard_id'].fillna('')
    df.loc[df.project_tags.isnull(), 'project_tags'] = df.project_tags.apply(lambda x: [])
    df[FLIPSIDE_QUERIES_RAG_COLS].count()
    pc_upload_data(df, 'text', FLIPSIDE_QUERIES_RAG_COLS, batch_size=100, index_name='slant', namespace='flipside_queries')