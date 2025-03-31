import pandas as pd
from langchain_openai import ChatOpenAI
from constants.db import FLIPSIDE_QUERIES_RAG_COLS
from utils.db import pg_load_data, pc_upload_data
from utils.flipside import summarize_query, upsert_flipside_queries

def create_and_upload_query_summaries():
    query = """
    SELECT q.id as query_id
    , q.name as query_title
    , q.statement as query_statement
    , db.title as dashboard_title
    , db.description as dashboard_description
    , db.tags as dashboard_tags
    , row_number() over (partition by q.id order by db.updated_at desc) as rn
    FROM flipside_queries q
    JOIN flipside_dashboards_queries dq ON q.id = dq.query_id
    JOIN flipside_dashboards db ON dq.dashboard_id = db.id
    WHERE q.statement != ''
    """
    df = pg_load_data(query)
    df.head(1).to_dict('records')

    query = 'select * from flipside_queries'
    queries = pg_load_data(query)

    df['tags'] = df['dashboard_tags'].apply(lambda x: ','.join([y for y in x if not y in ['solana'] ][:3] ))
    sorted(df.tags.unique())

    df['summary_prompt'] = df.apply(lambda x: f"Query Title: {x['query_title']}\n\nDashboard Title: {x['dashboard_title']}\n\n{ 'Dashboard Tags: {}'.format(x['tags']) if x['tags'] else '' }\n{ 'Dashboard Description: {}'.format(x['dashboard_description']) if x['dashboard_description'] else '' }\nQuery Statement:\n{x['query_statement']}".strip(), axis=1)
    print(df['summary_prompt'].values[0])
    # df['tmp'] = df.summary_prompt.apply(lambda x: 'loopscale' in x.lower())
    # df[df.tmp == True]
    val = df['summary_prompt'].values[0]
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    summary = summarize_query(val, llm)
    print(summary)

    df['summary'] = ''
    df = df.reset_index(drop=True)
    tot = len(df[df.summary == ''])
    for i, row in df[df.summary == ''].iterrows():
        summary = summarize_query(row['summary_prompt'], llm)
        df.loc[i, 'summary'] = summary
        print(f'{i}/{tot}')
    

    upload_df = queries.copy()
    del upload_df['text']
    df['dashboard_description'] = df['dashboard_description'].apply(lambda x: x.strip())
    def query_text(row):
        if len(row['dashboard_description']) > 2:
            return f"Query Summary: {row['summary']}\n======\nQuery Title: {row['query_title']}\n======\nDashboard Title: {row['dashboard_title']}\n======\nDashboard Description: {row['dashboard_description']}\n======\nDashboard Tags: {row['tags']}\n======\nQuery Statement:\n{row['query_statement']}"
        else:
            return f"Query Summary: {row['summary']}\n======\nQuery Title: {row['query_title']}\n======\nDashboard Title: {row['dashboard_title']}\n======\nQuery Statement:\n{row['query_statement']}"
    df['text'] = df.apply(query_text, axis=1)
    df['text'] = df['text'].apply(lambda x: x[:40000])
    print(df[df.query_id == '013b6b43-49ba-45de-bdda-00de4f3c085a'].text.values[0])
    print(df[df.query_id == '013b6b43-49ba-45de-bdda-00de4f3c085a'].dashboard_description.values[0])
    len(df[df.query_id == '013b6b43-49ba-45de-bdda-00de4f3c085a'].dashboard_description.values[0])
    upload_df = pd.merge(df[['query_id', 'summary', 'text']].rename(columns={'query_id': 'id'}), upload_df, on='id', how='left')
    upload_df.head()
    upload_df[upload_df.user_id.isnull()]
    upsert_flipside_queries(upload_df)
    pc_upload_data(upload_df, 'text', FLIPSIDE_QUERIES_RAG_COLS, batch_size=100, index_name='slant', namespace='flipside_queries')
