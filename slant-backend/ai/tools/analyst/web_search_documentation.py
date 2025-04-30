import time
from utils.utils import log
from classes.JobState import JobState
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from ai.tools.web.web_crawl import web_crawl
from utils.db import pg_load_data, pg_upload_data, pc_upload_data
from utils.utils import clean_project_tag

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

    pg_upload_data(all_results_df, 'web_searches', if_exists='append')
    all_results_df['text'] = all_results_df['text'].apply(lambda x: x[:35000])
    all_results_df['embedding'] = all_results_df.apply(lambda x: x.project + ' ' + x.text, axis=1)
    pc_upload_data(all_results_df, 'embedding', ['project', 'project_id', 'search_query', 'base_url', 'url', 'text'], batch_size=100, index_name='slant', namespace='web_searches')
    return {'pre_query_clarifications': response, 'completed_tools': ["PreQueryClarifications"]}
