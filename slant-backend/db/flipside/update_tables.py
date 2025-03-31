
import pandas as pd
from utils.db import pg_load_data, pg_execute_query
from utils.flipside import extract_project_tags_from_query, clean_project_tag

def update_column_types():

    query = "select id, created_at, updated_at, last_successful_execution_at from flipside_queries where last_successful_execution_at like '%T%'"
    df = pg_load_data(query)
    df['updated_at'] = df['updated_at'].apply(lambda x: str(x)[:19].replace('T', ' ') )
    df['created_at'] = df['created_at'].apply(lambda x: str(x)[:19].replace('T', ' ') )
    df['last_successful_execution_at'] = df['last_successful_execution_at'].apply(lambda x: str(x)[:19].replace('T', ' ') )
    df['updated_at'] = pd.to_datetime(df['updated_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['last_successful_execution_at'] = pd.to_datetime(df['last_successful_execution_at']).dt.strftime('%Y-%m-%d %H:%M:%S')

    tot = len(df)
    df = df.reset_index(drop=True)
    for i, row in df.iterrows():
        print(f'{i}/{tot}')
        query = f'update flipside_queries set last_successful_execution_at = \'{row.last_successful_execution_at}\' where id = \'{row.id}\''
        print(query)
        pg_execute_query(query)
    

    query = 'alter table flipside_queries alter column created_at type timestamp using created_at::timestamp;'
    pg_execute_query(query)

    query = 'alter table flipside_queries alter column updated_at type timestamp using updated_at::timestamp;'
    pg_execute_query(query)

    query = 'alter table flipside_queries alter column last_successful_execution_at type timestamp using last_successful_execution_at::timestamp;'
    pg_execute_query(query)
    
    query = 'alter table flipside_queries add column summary text;'
    pg_execute_query(query)
    
    query = 'alter table flipside_queries add column project_tags text[];'
    pg_execute_query(query)
    
    query = 'alter table context_queries add column is_filtered int default 0;'
    pg_execute_query(query)
    
def update_project_tags():
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    query = "select id, text from flipside_queries"
    df = pg_load_data(query)

    print(df['text'].values[0])

    tot = len(df)
    df['project_tags'] = None
    df = df.reset_index(drop=True)
    for i, row in df.tail(1169).iterrows():
        print(f'{i}/{tot}')
        df.loc[i, 'project_tags'] = extract_project_tags_from_query(row['text'], llm)
    df['project_tags'] = df['project_tags'].apply(lambda x: [ clean_project_tag(tag) for tag in x ])

    df = df.reset_index(drop=True)
    df['tmp'] = df.project_tags.apply(len)
    for i, row in df[df.tmp > 0].iterrows():
        if i < 0:
            continue
        print(f'{i}/{tot}')
        if row.project_tags:
            tags_str = '{' + ','.join(f'"{tag}"' for tag in row.project_tags) + '}'
            tags_str = re.sub("'", "", tags_str)
        else:
            tags_str = '{}'
        query = f'update flipside_queries set project_tags = \'{tags_str}\' where id = \'{row.id}\''
        pg_execute_query(query)

    df['project_tags'] = df['text'].apply(lambda x: extract_project_tags_from_query(x, llm))
    all_tags = []
    for tags in df['project_tags']:
        all_tags.extend(tags)
    all_tags = pd.DataFrame(all_tags, columns=['tag'])
    all_tags = all_tags.groupby('tag').size().reset_index(name='count')
    all_tags.to_csv('~/Downloads/all_tags.csv', index=False)

    all_tags = list(set(all_tags))  # Remove duplicates
    print(f"Found {len(all_tags)} unique project tags")

    df = df[['id', 'project_tags']]
    df.to_csv('project_tags.csv', index=False)
