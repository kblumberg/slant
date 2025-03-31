import time
from utils.utils import log
from datetime import datetime
from classes.GraphState import GraphState
from langchain.schema import SystemMessage, HumanMessage

def answer_with_context(state: GraphState):
    start_time = time.time()
    log('answer_with_context starting...')
    # Construct prompt dynamically based on state
    tweets = '## **Tweets**: \n' + ''.join([str(tweet) for tweet in state['tweets']]) if len(state['tweets']) else ''
    projects = '## **Projects**: \n' + ''.join([str(project) for project in state['projects']]) if len(state['projects']) else ''
    kols = '## **KOLs**: \n' + ''.join([str(kol) for kol in state['kols']]) if len(state['kols']) else ''
    sql_query_result = '## **Project + KOL Data**: \n' + state['sql_query_result'] if len(state['sql_query_result']) else ''
    sharky_agent_answer = '## **Sharky NFT Loan Expert Answer**: \n' + state['sharky_agent_answer'] if len(state['sharky_agent_answer']) else ''
    flipside_sql_query_result = '## **Data Analyst Result**: \n Here is some data that may be relevant to the question. Prioritize this data over other information to answer the question. The tweet data may be outdated, so use the Data Analyst Result to answer the question if possible. However, DO NOT give a list of data points in your response, just focus on key insights and overviews. \n```json' + state['flipside_sql_query_result'].to_json(orient='records') + '```' if len(state['flipside_sql_query_result']) else ''
    news_df = '## **News**: \n' + state['news_df'].to_markdown() if len(state['news_df']) else ''
    tot = len(projects) + len(kols) + len(sql_query_result)
    score_context = f'"Score" is a number between 0 and 100 that represents the importance or relevance of the object. Do not use the score to answer the question, but use it to prioritize the results.' if tot > 0 else ''

    history_message = state['memory'].get_history_message()
    messages = [
        SystemMessage(content="You are an expert in the solana blockchain ecosystem and helping to answer a question with the context of information."),
        history_message,
        HumanMessage(content=f"""
            Original user question: {state['query']}

            Helpful context to answer the question: 

            {tweets}
            {projects}
            {kols}
            {sql_query_result}
            {score_context}

            {news_df}

            {sharky_agent_answer}

            {flipside_sql_query_result}

            Based on this information, answer the user query. Feel free to use the context to answer the question, but ignore the context if it is not relevant to the question.

            Note that the current date and time is {datetime.now().strftime("%Y-%m-%d %H:%M")}. If there are dates mentioned in the question or context, keep this in mind.

            Provide your response in Markdown format. Use:
                - Headers for sections (### Section Name)
                - Bullet points for lists (- Item 1)
                - Code blocks for code snippets (```python ... ```)
                - Bold for emphasis (**bold text**)
                - Links in markdown format ([text](url))
            Make the response easy to digest and understand.
            Include links to tweets, docs, twitter accounts, or other sources when relevant.
            If you reference data from tweets, be sure to link the tweet.
            Unless specifically asked, do not include any code in your response.

            If the question is open ended, be thorough and provide a detailed answer.

            If the question is specific, be concise and provide a clear answer.
        """),
    ]

    # Call LLM to get the decision
    answer = state['llm'].invoke(messages).content
    time_taken = round(time.time() - start_time, 1)
    log(f'answer_with_context finished in {time_taken} seconds')
    return {'answer': answer, 'completed_tools': ["AnswerWithContext"]}