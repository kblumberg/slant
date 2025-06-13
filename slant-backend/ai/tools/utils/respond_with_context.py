import time
from utils.utils import log
from datetime import datetime
from classes.JobState import JobState
from langchain.schema import SystemMessage, HumanMessage
from ai.tools.utils.utils import log_llm_call, parse_messages_fn, state_to_reference_materials

def respond_with_context(state: JobState):
    start_time = time.time()
    # log('respond_with_context starting...')
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
        response = log_llm_call(parse_messages_fn(messages), state['llm'], state['user_message_id'], 'RespondWithContext')
        time_taken = round(time.time() - start_time, 1)
        # log(f'respond_with_context finished in {time_taken} seconds')
        return {'response': response, 'completed_tools': ["RespondWithContext"]}
    elif len(state['flipside_sql_query_result']) > 0:

        flipside_sql_query_result = '## **Data Analyst Result**: \n Here is some data that may be relevant to the question. Prioritize this data over other information to answer the question. However, DO NOT give a list of data points in your response, just focus on key insights and overviews. \n```' + state['flipside_sql_query_result'].to_markdown() + '```' if len(state['flipside_sql_query_result']) else ''


        messages = [
            SystemMessage(content="You are an expert in the solana blockchain ecosystem and helping to answer a question with the context of information."),
            HumanMessage(content=f"""
                Original user question: {state['analysis_description']}

                Helpful context to answer the question: 

                {flipside_sql_query_result}

                Based on this information, provide a response to the user query in 1-3 key points and a summary. There will be a chart in the response, so you do not have to be verbose, just a summary and 1-3 things that stand out.

                Note that the current date and time is {datetime.now().strftime("%Y-%m-%d %H:%M")}. If there are dates mentioned in the question or context, keep this in mind.
                
                =============
                ## Methodology Section
                =============
                - Also include a **methodology** section that explains how you queried the data and what methods you used.
                - Explicity mention any specific addresses, mints, or program ids that you used (particularly in the WHERE clause) or any assumptions you made.
                - Be detailed but concise.

                **SQL Query Used**:
                ```sql
                {state['flipside_sql_query']}
                ```

                Provide your response in Markdown format. Use:
                    - Headers for sections (### Section Name)
                    - Bullet points for lists (- Item 1)
                    - Code blocks for code snippets (```python ... ```)
                    - Bold for emphasis (**bold text**)
                    - Links in markdown format ([text](url))
                Make the response easy to digest and understand.
                Unless specifically asked, do not include any code in your response or anything besides the summary and 1-3 key points.
            """),
        ] if len(flipside_sql_query_result) <= 50000 else [
            SystemMessage(content="You are an expert in the solana blockchain ecosystem and helping to answer a question with the context of information."),
            HumanMessage(content=f"""
                You have just answered a question with the following context: {state['analysis_description']}

                Tell the user that you have answered the question (with a chart that will be provided above) and ask if they have any follow up questions.
            """),
        ]

        # Call LLM to get the decision
        answer = log_llm_call(parse_messages_fn(messages), state['complex_llm'], state['user_message_id'], 'RespondWithContext')
        time_taken = round(time.time() - start_time, 1)
        # log(f'answer_with_context finished in {time_taken} seconds')
        return {'response': answer, 'completed_tools': ["RespondWithContext"]}
    elif state['flipside_sql_error']:
        return {'response': 'Sorry, I had an error with the query. Please try again, adding as much context and details as possible.', 'completed_tools': ["RespondWithContext"]}
    elif len(state['write_flipside_query_or_investigate_data']) > 0:
        return {'response': state['write_flipside_query_or_investigate_data'], 'completed_tools': ["RespondWithContext"]}
    elif state['question_type'] == 'other':
        reference_materials = state_to_reference_materials(state, include_keys=['tweets', 'web_search', 'projects'])
        prompt = f"""
        You are an expert in the solana blockchain ecosystem and helping to answer a question with the context of information.

        Original user question: {state['analysis_description']}

        {reference_materials}
        
        Provide your response in Markdown format. Use:
            - Headers for sections (### Section Name)
            - Bullet points for lists (- Item 1)
            - Code blocks for code snippets (```python ... ```)
            - Bold for emphasis (**bold text**)
            - Links in markdown format ([text](url))
        Make the response easy to digest and understand.
        """
        answer = log_llm_call(prompt, state['llm'], state['user_message_id'], 'RespondWithContext')
        return {'response': answer, 'completed_tools': ["RespondWithContext"]}
    elif len(state['analysis_description']) > 0:
        return {'response': state['analysis_description'] + '\n\n' + state['pre_query_clarifications'], 'completed_tools': ["RespondWithContext"]}
    return {}