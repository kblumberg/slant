import os
import time
import pandas as pd
from utils.utils import log
from datetime import datetime, timedelta
from utils.db import pg_upload_data
from utils.db import pc_execute_query
from classes.GraphState import GraphState
from utils.flipside import extract_project_tags_from_user_prompt
from ai.tools.utils.prompt_refiner_for_flipside_sql import prompt_refiner_for_flipside_sql
from constants.keys import OPENAI_API_KEY
from langchain_openai import ChatOpenAI

def write_flipside_query(state: GraphState) -> GraphState:
    # """
    #     Writes a query to pull Sharky data from the database.
    #     Input:
    #         a dictionary with the following keys:
    #             - question: a question or topic you want to find information about (str)
    #     Examples: Show me the unique number of lenders each day over the past 5 days
    # """
    # refined_query = prompt_refiner(state, 'Write a SQL query to answer the following question.')
    refined_query = state['refined_prompt_for_flipside_sql']
    if not refined_query:
        refined_query = prompt_refiner_for_flipside_sql(state)
    start_time = time.time()
    log('\n')
    log('='*20)
    log('\n')
    log('write_flipside_query starting...')
    # log('sharkyState:')
    # log(print_sharky_state(sharkyState))
    # state = {
    #     'query': 'Show me the unique number of sharky lenders each day over the past 5 days. Only include lenders who had their loan taken.'
    #     # 'query': 'What are the top 10 LSTs by market cap?'
    # }


    # load flipside queries from rag db
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # # Construct absolute paths to the files
    # example_queries_path = os.path.join(current_dir, "..", "context", "sharky_queries.sql")
    schema_path = os.path.join(current_dir, "..", "..", "context", "flipside", "schema.sql")

    # with open(example_queries_path, "r") as f:
    #     example_queries = f.read()

    with open(schema_path, "r") as f:
        schema = f.read()
    
    if state['flipside_sql_query'] and state['flipside_sql_error']:
        log('Trying to fix the previous query')
        log("state['flipside_sql_query']")
        log(state['flipside_sql_query'])
        log("state['flipside_sql_error']")
        log(state['flipside_sql_error'])
        prompt = f"""
            You are an expert in writing accurate, efficient, and idiomatic Snowflake SQL queries for blockchain analytics using the Flipside database.

            You are given a previous attempt at a query and the error it returned.

            Your task is to correct the query to fix the error.
            
            ## Reference

            Use the following example queries for inspiration and to understand available schema and patterns:
            {state['flipside_example_queries']}

            Schema:
            {schema}


            ### ❌ Previous SQL Query:
            {state['flipside_sql_query']}

            ### 🛠️ Error Message:
            {state['flipside_sql_error']}

            🧠 Think carefully about the cause of the error. Was it a syntax issue, incorrect table/column, logic problem, or missing filter?

            Then, write a corrected version of the SQL query below.


            ## ✍️ Output

            Write a **correct, performant, and idiomatic** Snowflake SQL query that fixes the error from above.

            Return ONLY the raw SQL (no extra text):
        """

        sql_query = state['sql_llm'].invoke(prompt).content

        # Remove SQL code block markers if present
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        return {'flipside_sql_query': sql_query}
    else:
        # llm = ChatOpenAI(
        #     model="gpt-4o-mini",
        #     openai_api_key=OPENAI_API_KEY,
        #     temperature=0.0
        # )
        # state = {
        #     'query': 'What has the trading volume on Jupiter been since January 1 2025 to now?',
        #     'llm': llm,
        #     'session_id': '2ba67ea8-5458-4ce8-8e5c-98352b5e4bbe',
        #     'clarified_query': 'What has the trading volume on Jupiter been since January 1 2025 to now?'
        # }
        # refined_query = 'What has the trading volume on Jupiter been since January 1 2025 to now?'
        project_tags = extract_project_tags_from_user_prompt(state['query'], state['llm'])
        results_filtered = pc_execute_query(refined_query, index_name="slant", namespace="flipside_queries", filter_conditions={'project_tags': {'$in': project_tags}}, top_k=40)
        ninety_days_ago = int((datetime.now() - timedelta(days=90)).timestamp())
        results_filtered_new = pc_execute_query(refined_query, index_name="slant", namespace="flipside_queries", filter_conditions={'project_tags': {'$in': project_tags}, 'created_at': {'$gte': ninety_days_ago}}, top_k=40)
        results_new = pc_execute_query(refined_query, index_name="slant", namespace="flipside_queries", filter_conditions={'created_at': {'$gte': ninety_days_ago}}, top_k=40)
        results = pc_execute_query(refined_query, index_name="slant", namespace="flipside_queries", filter_conditions={}, top_k=40)
        log(f'project_tags: {project_tags}')
        log(f'results_filtered: {len(results_filtered["matches"])}')
        log(f'results_filtered_new: {len(results_filtered_new["matches"])}')
        log(f'results_new: {len(results_new["matches"])}')
        log(f'results: {len(results["matches"])}')
        metadata_cols = ['text', 'user_id', 'dashboard_id', 'created_at', 'project_tags']
        a = [[x['id'], x['score'], 0] + [x['metadata'][c] for c in metadata_cols] for x in results['matches']]
        b = [[x['id'], x['score'], 1] + [x['metadata'][c] for c in metadata_cols] for x in results_filtered['matches']] if len(results_filtered['matches']) else []
        c = [[x['id'], x['score'], 2] + [x['metadata'][c] for c in metadata_cols] for x in results_new['matches']] if len(results_new['matches']) else []
        d = [[x['id'], x['score'], 3] + [x['metadata'][c] for c in metadata_cols] for x in results_filtered_new['matches']] if len(results_filtered_new['matches']) else []
        results_all = pd.DataFrame(a + b + c + d, columns=['query_id','score','is_filtered'] + metadata_cols)
        results_all['days_ago'] = results_all['created_at'].apply(lambda x: max(0, int((datetime.now().timestamp() - x) / (24 * 3600))))
        sorted(results_all['days_ago'].unique())
        results_all['mult'] = results_all['days_ago'].apply(lambda x: pow(0.999, x)) * results_all['dashboard_id'].apply(lambda x: 1 if x else 0.9) * results_all['project_tags'].apply(lambda x: 1 if len(set(x) & set(project_tags)) else 0.9)
        results_all['score_1'] = results_all['score'] * results_all['mult']
        results_all = results_all.sort_values(['score_1', 'is_filtered'], ascending=[0,0]).drop_duplicates(subset=['query_id'], keep='first')
        results_all.head()[['score', 'mult', 'score_1', 'is_filtered']]
        results_all['match_index'] = results_all.groupby('is_filtered').score_1.rank(ascending=False).astype(int)
        results_all['user_index'] = results_all.groupby('user_id').score_1.rank(ascending=False).astype(int)
        results_all['rk'] = results_all.score_1.rank(ascending=False).astype(int)
        # results_all = results_all[(results_all['rk'] <= 15) | (results_all['match_index'] <= 3) | ((results_all['user_index'] <= 1) & (results_all['rk'] <= 30))].rename(columns={'score_1': 'score'})
        del results_all['score']
        results_all = results_all[(results_all['rk'] <= 15)].rename(columns={'score_1': 'score'})

        # results_all = results_all[(results_all.query_id.isin(g.query_id)) | (results_all['match_index'] <= 3) | (results_all['user_index'] <= 1)].drop_duplicates(subset=['query_id'], keep='last')

        upload_df = results_all[['query_id','score','is_filtered']]
        upload_df['rank'] = upload_df.score.rank(ascending=False).astype(int)
        upload_df['session_id'] = state['session_id']
        upload_df['user_prompt'] = state['query']
        upload_df['clarified_prompt'] = state['clarified_query']
        upload_df['created_at'] = int(datetime.now().timestamp())
        pg_upload_data(upload_df, 'context_queries')
        # log(results)
        example_queries = '\n\n'.join(results_all.text.apply(lambda x: x[:10000]).values)
        # Create prompt template
        prompt = f"""
            You are an expert in writing accurate, efficient, and idiomatic Snowflake SQL queries for blockchain analytics using the Flipside database.

            ---

            ## Task

            Write a **valid and optimized Snowflake SQL query** that answers the following user question:

            ### ❓ Question:
            {state['query']}

            ---

            ## Reference

            Use the following example queries for inspiration and to understand available schema and patterns:

            {example_queries}

            Schema:
            {schema}

            Make sure to remember any IMPORTANT notes from the schema. Override anything in the example queries based on the IMPORTANT notes.

            The `solana.price.ez_token_prices_hourly` table is deprecated. Use `solana.price.ez_prices_hourly` instead.

            ---

            ## Important Notes
            - Do NOT use `solana.price.ez_token_prices_hourly` (deprecated). Use `solana.price.ez_prices_hourly` instead.
            - Use example queries for structure and patterns, but tailor them to the user’s question.
            - Use `block_timestamp` filtering where applicable.
            - Avoid `SELECT *` and limit columns to what is needed.
            - Use `GROUP BY` when using aggregate functions.
            - Make sure token addresses and program IDs are correct (cross-reference with examples).
            - Ensure the query performs well by limiting time range and data volume where possible.
            - Return ONLY the raw SQL. No explanation, markdown, or formatting.
            - Any time-based column should be aliased as `date_time`.
            - Any categorical column should be aliased as `category`.

            ---

            ## ✍️ Output

            Write a **correct, performant, and idiomatic** Snowflake SQL query that answers the user’s question.

            Return ONLY the raw SQL (no extra text):
        """

        sql_query = state['sql_llm'].invoke(prompt).content

        # Remove SQL code block markers if present
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        time_taken = round(time.time() - start_time, 1)
        log(f'write_flipside_query finished in {time_taken} seconds')
        log(f"Generated SQL Query:")
        log(sql_query)
        return {'flipside_sql_query': sql_query, 'completed_tools': ["DataAnalyst"], 'upcoming_tools': ["ExecuteFlipsideQuery"], 'refined_prompt_for_flipside_sql': refined_query, 'flipside_example_queries': example_queries}