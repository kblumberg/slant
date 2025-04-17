import time
from utils.utils import log
from datetime import datetime
from classes.JobState import JobState
from langchain.schema import SystemMessage, HumanMessage

def respond_with_context(state: JobState):
    start_time = time.time()
    log('respond_with_context starting...')
    if len(state['follow_up_questions']) > 0:
        # Construct prompt dynamically based on state

        follow_up_questions = '\n'.join(state['follow_up_questions'])

        messages = [
            SystemMessage(content="You are an expert in the solana blockchain ecosystem and helping to answer a question with the context of information."),
            HumanMessage(content=f"""
                Original user question: {state['user_prompt']}

                Respond to the user with these follow up questions:

                {follow_up_questions}
            """),
        ]

        # Call LLM to get the decision
        response = state['llm'].invoke(messages).content
        time_taken = round(time.time() - start_time, 1)
        log(f'respond_with_context finished in {time_taken} seconds')
        return {'response': response, 'completed_tools': ["RespondWithContext"]}
    elif len(state['write_flipside_query_or_investigate_data']) > 0:

        flipside_sql_query_result = '## **Data Analyst Result**: \n Here is some data that may be relevant to the question. Prioritize this data over other information to answer the question. However, DO NOT give a list of data points in your response, just focus on key insights and overviews. \n```json' + state['flipside_sql_query_result'].to_json(orient='records') + '```' if len(state['flipside_sql_query_result']) else ''

        messages = [
            SystemMessage(content="You are an expert in the solana blockchain ecosystem and helping to answer a question with the context of information."),
            HumanMessage(content=f"""
                Original user question: {state['analysis_description']}

                Helpful context to answer the question: 

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
        return {'response': answer, 'completed_tools': ["RespondWithContext"]}
    elif len(state['write_flipside_query_or_investigate_data']) > 0:
        return {'response': state['write_flipside_query_or_investigate_data'], 'completed_tools': ["RespondWithContext"]}
    elif len(state['analysis_description']) > 0:
        return {'response': state['analysis_description'] + '\n\n' + state['pre_query_clarifications'], 'completed_tools': ["RespondWithContext"]}
    return {}