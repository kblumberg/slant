import time
from utils.utils import log
from classes.JobState import JobState
from ai.tools.web.web_crawl import web_crawl
from utils.db import pg_load_data, pg_upsert_data, pc_upload_data
from utils.utils import clean_project_tag
from sqlalchemy import create_engine, MetaData, Table, Column, BigInteger, Text, Integer
from constants.keys import POSTGRES_ENGINE
import pandas as pd

def web_search_documentation(state: JobState) -> JobState:

    projects = [ x['name'] for x in state['projects'] if x and x['name'] ]
    projects = [ 'loopscale' ]
    projects = [ clean_project_tag(x) for x in projects ]
    projects = projects[:3]
    all_results = []
    query = f"""
        SELECT id, name
        FROM projects
        WHERE lower(name) IN ({', '.join([ f"'{x}'" for x in projects ])})
    """
    projects_df = pg_load_data(query)
    for project in projects:
        search_query = f'Solana blockchain {project} documentation'
        log(f'web_search_documentation search_query: {search_query}')
        project_id = projects_df[projects_df.name == project]
        project_id = project_id.id.values[0] if len(project_id) > 0 else None
        current_results = web_crawl(search_query, project, project_id)
        all_results.extend(current_results)
    all_results_df = pd.DataFrame(all_results)
    all_results_df['user_message_id'] = 'test'
    del all_results_df['allowed_domain']
    # all_results_df.columns
    # Index(['base_url', 'allowed_domain', 'search_query', 'project', 'project_id',
    #    'url', 'text'],
    #     project VARCHAR(255) NOT NULL,
    # project_id INTEGER DEFAULT NULL,
    # user_message_id VARCHAR(255) NOT NULL,
    # search_query TEXT NOT NULL,
    # base_url TEXT NOT NULL,
    # url TEXT NOT NULL,
    # text TEXT NOT NULL

    # Upload tweets to postgres, replacing existing records with same id
    engine = create_engine(POSTGRES_ENGINE)
    metadata = MetaData()
    table = Table(
        "web_searches", metadata,
        Column("url", Text, primary_key=True),
        Column("timestamp", Integer),
        Column("project", Text),
        Column("project_id", BigInteger),
        Column("user_message_id", Text),
        Column("search_query", Text),
        Column("base_url", Text),
        Column("text", Text)
    )
    pg_upsert_data(all_results_df, table, engine, ['url'])
    all_results_df['text'] = all_results_df['text'].apply(lambda x: x[:35000])
    all_results_df['embedding'] = all_results_df.apply(lambda x: x.project + ' ' + x.text, axis=1)
    all_results_df['id'] = all_results_df.url
    all_results_df['project_id'] = all_results_df.project_id.fillna(0).astype(int)
    pc_upload_data(all_results_df, 'embedding', ['project', 'project_id', 'search_query', 'base_url', 'url', 'text'], batch_size=100, index_name='slant', namespace='web_searches')
    return {'pre_query_clarifications': response, 'completed_tools': ["PreQueryClarifications"]}
