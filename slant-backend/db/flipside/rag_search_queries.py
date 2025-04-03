import pandas as pd
from datetime import datetime
from utils.db import pc_execute_query

def rag_search_queries(query: str, project_tags: list[str], top_k: int = 40, n_queries: int = 10) -> pd.DataFrame:
    metadata_cols = ['text', 'user_id', 'dashboard_id', 'created_at', 'project_tags']
    cur_0 = pc_execute_query(query, index_name="slant", namespace="flipside_queries", filter_conditions={'project_tags': {'$in': project_tags}}, top_k=top_k)
    cur_1 = pc_execute_query(query, index_name="slant", namespace="flipside_queries", top_k=top_k)
    results = [[x['id'], x['score']] + [x['metadata'][c] for c in metadata_cols] for x in cur_0['matches'] + cur_1['matches']]
    results = pd.DataFrame(results, columns=['query_id', 'score'] + metadata_cols)
    results['days_ago'] = results['created_at'].apply(lambda x: max(0, int((datetime.now().timestamp() - x) / (24 * 3600))))
    results['mult'] = results['days_ago'].apply(lambda x: pow(0.9995, x)) * results['dashboard_id'].apply(lambda x: 1 if x else 0.9) * results['project_tags'].apply(lambda x: 1 if len(set(x) & set(project_tags)) else 0.9)
    results['score'] = results['score'] * results['mult']
    results = results.sort_values(['score'], ascending=[0]).drop_duplicates(subset=['query_id'], keep='first')
    results = results.head(n_queries)
    return results

