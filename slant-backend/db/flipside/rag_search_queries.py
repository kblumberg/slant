import pandas as pd
from datetime import datetime
from utils.db import pc_execute_query
from utils.utils import log

def rag_search_queries(query: str, project_tags: list[str], top_k: int = 40, n_queries: int = 15) -> pd.DataFrame:
    metadata_cols = ['text', 'tables', 'user_id', 'dashboard_id', 'created_at', 'project_tags']
    cur_0 = pc_execute_query(query, index_name="slant", namespace="flipside_queries", filter_conditions={'project_tags': {'$in': project_tags}}, top_k=top_k)
    cur_1 = pc_execute_query(query, index_name="slant", namespace="flipside_queries", top_k=top_k)
    results = [[x['id'], x['score']] + [x['metadata'][c] for c in metadata_cols] for x in cur_0['matches'] + cur_1['matches']]
    results = pd.DataFrame(results, columns=['query_id', 'original_score'] + metadata_cols)
    results['days_ago'] = results['created_at'].apply(lambda x: max(0, int((datetime.now().timestamp() - x) / (24 * 3600)))).apply(lambda x: min(180, x) )
    results['time_mult'] = results['days_ago'].apply(lambda x: pow(0.9995, x))
    results['project_mult'] = results['project_tags'].apply(lambda x: 1 if len(set(x) & set(project_tags)) else 0.9)
    results['dashboard_mult'] = results['dashboard_id'].apply(lambda x: 1 if x else 0.9)
    results['mult'] = results['time_mult'] * results['dashboard_mult'] * results['project_mult']
    results['score'] = results['original_score'] * results['mult']
    results['rk'] = results.groupby('user_id')['score'].rank(ascending=0, method='first')
    results['user_mult'] = results.rk.apply(lambda x: 0.985 ** x)
    results['score'] = results['score'] * results['user_mult']
    results = results.sort_values(['score'], ascending=[0]).drop_duplicates(subset=['query_id'], keep='first')
    log('rag_search_queries results')
    log(results[['query_id', 'score', 'original_score', 'user_mult', 'time_mult', 'project_mult', 'dashboard_mult']].head(30))
    results = results.head(n_queries)
    return results

