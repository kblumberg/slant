import json
from typing import Dict, List, Any, Tuple, TypedDict
import pandas as pd
from langchain_core.chat_history import BaseChatMessageHistory, BaseMessage
from langchain_core.messages import HumanMessage, AIMessage
from langchain_postgres import PostgresChatMessageHistory
from constants.keys import POSTGRES_ENGINE
import psycopg2
from utils.utils import log, get_timestamp
from langchain_core.messages import SystemMessage
from classes.JobState import JobState
from utils.db import pg_load_data, pg_execute_query
from ai.tools.utils.utils import state_to_reference_materials

class PostgresConversationMemory(BaseChatMessageHistory):
    def __init__(
        self, 
        conversation_id: str, 
        sync_connection: psycopg2.connect,
        table_name: str = "chat_history"
    ):
        """
        Initialize PostgreSQL-backed conversation memory.
        
        Args:
            connection_string (str): PostgreSQL database connection string
            conversation_id (str): Unique identifier for the conversation
            table_name (str, optional): Name of the table to store chat history
        """
        self.memory = PostgresChatMessageHistory(
            table_name
            , conversation_id
            , sync_connection=sync_connection
        )
        self.conversation_id = conversation_id
        self.messages = []
        self.message_df = pd.DataFrame()

    
    def load_messages(self):
        query = f"""
        with conversations as (
            select '{self.conversation_id}' as conversation_id
        )
        , t0 as (
            select message
            , 'user' as role
            , null::JSONB as state
            , timestamp
            , id::uuid as id
            from user_messages um
            join conversations c on um.conversation_id = c.conversation_id
        )
        , t1 as (
            select ss.state->>'response'::TEXT as message
            , 'system' as role
            , ss.state
            , ss.timestamp
            , ss.id::uuid as id
            from state_snapshots ss
            join t0 on t0.id::uuid = ss.user_message_id::uuid
        )
        , t2 as (
            select *
            from t0
            union
            select *
            from t1
        )
        select *
        from t2
        order by timestamp asc
        """
        df = pg_load_data(query)
        # log('load_messages df')
        # log(df)

        messages = []
        for _, row in df.iterrows():
            role = row['role']
            content = row['message']
            if role == 'user':
                messages.append(HumanMessage(content=content))
            elif role == 'system':
                messages.append(SystemMessage(content=content))
            else:
                raise ValueError(f"Unknown role: {role}")
        
        self.messages = messages
        self.message_df = df

    def save_conversation(self, state: JobState) -> None:
        conversation_id = state['conversation_id']

        # if the conversation_id is not in the conversations table, create an entry with
        # the user_id, created_at, updated_at, and title
        # if the conversation_id is in the conversations table, update the updated_at
        query_1 = f"SELECT id FROM conversations WHERE id = '{conversation_id}'"
        df = pg_load_data(query_1)
        if len(df):
            # update the updated_at
            query = f"UPDATE conversations SET updated_at = NOW() WHERE id = '{conversation_id}'"
        else:
            # create a new entry
            title = state['user_prompt'][:100]
            query = f"INSERT INTO conversations (id, user_id, created_at, updated_at, title) VALUES (%s, %s, %s, %s, %s)"
            values = (conversation_id, state['user_id'], get_timestamp(), get_timestamp(), title)
            conn = psycopg2.connect(POSTGRES_ENGINE)
            cur = conn.cursor()
            cur.execute(query, values)
            conn.commit()
            cur.close()
            conn.close()
            
        return True
    
    def save_user_message(self, user_message_id: str, user_prompt: str) -> None:
        # Escape single quotes in user_prompt to prevent SQL injection
        escaped_prompt = user_prompt.replace("'", "''")
        query = f"INSERT INTO user_messages (id, timestamp, message, conversation_id) VALUES ('{user_message_id}', '{get_timestamp()}', '{escaped_prompt}', '{self.conversation_id}')"
        pg_execute_query(query)
            
        return True
    
    def save_agent_message(self, state: JobState) -> None:
        query = f"INSERT INTO agent_messages (message, conversation_id, user_message_id) VALUES (%s, %s, %s)"
        values = (state['response'], state['conversation_id'], state['user_message_id'])
        conn = psycopg2.connect(POSTGRES_ENGINE)
        cur = conn.cursor()
        cur.execute(query, values)
        conn.commit()
        cur.close()
        conn.close()
            
        return True

    def save_state_snapshot(self, state: JobState) -> None:
        # log('save_state_snapshot')
        # log(state)

        # tools = list(set(state['completed_tools']))

        # # Escape special characters in tools list to prevent SQL injection
        # escaped_tools = [tool.replace("'", "''") for tool in tools]
        # tools_str = str(escaped_tools).replace("'", "''")
        
        # # Escape special characters in analyses
        # escaped_analyses = [str(analysis).replace("'", "''") for analysis in state['analyses']]
        # analyses_str = str(escaped_analyses).replace("'", "''")
        
        # # Convert highcharts config to JSON string and escape
        # highcharts_str = json.dumps(state['highcharts_config']).replace("'", "''")
        
        # # Convert flipside queries DataFrame to JSON string and escape
        # flipside_queries_str = state['flipside_example_queries'][['query_id','original_score','mult','score']].to_json(orient='records').replace("'", "''")
        # query = f"INSERT INTO state_snapshots (user_message_id, analyses, tools, highcharts_config, flipside_example_queries) VALUES ('{state['user_message_id']}', '{analyses_str}', '{tools_str}', '{highcharts_str}', '{flipside_queries_str}')"


        # Convert to JSON-serializable Python objects
        query = """
            INSERT INTO state_snapshots (
                user_message_id,
                state
            ) VALUES (%s, %s)
        """

        values = (
            state['user_message_id'],
            self.state_to_json(state)
        )

        # Example connection and insert
        conn = psycopg2.connect(POSTGRES_ENGINE)
        cur = conn.cursor()
        cur.execute(query, values)
        conn.commit()
        cur.close()
        conn.close()
        # pg_execute_query(query)
 
    def load_memory_variables(self, inputs: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Load previous conversation messages.
        
        Args:
            inputs (Dict): Current conversation context
        
        Returns:
            Dict: Loaded chat history
        """
        return {
            "chat_history": self.memory.messages
        }
    
    def clear(self) -> None:
        """
        Clear the conversation history for the current session.
        """
        # Implement logic to clear messages for the specific session
        pass

    def parse_chat_history(self, n: int = 4):
        """
        Parse chat history to extract just the message contents.
        
        Args:
            chat_history (list): Raw chat history messages
        
        Returns:
            str: Formatted conversation context
        """
        chat_history = self.load_memory_variables({})['chat_history']
        chat_history = [x for x in chat_history if x.content != '']
        parsed_history = []
        for message in chat_history[-n:]:  # Limit to last 3 messages
            # Assuming messages have 'type' and 'content' keys
            if isinstance(message, HumanMessage):
                parsed_history.append(f"User: {message.content}")
            elif isinstance(message, AIMessage):
                parsed_history.append(f"AI: {message.content}")
        # log('parsed_history')
        # log(parsed_history)
        return "\n".join(parsed_history)
    
    def get_history_message(self, n: int = 4):
        """
        Get the chat history for the current session.
        """

        chat_history = self.parse_chat_history()
        if len(chat_history):
            history_message = SystemMessage(content=f"""
            **Conversation Context:**
            The following is the recent conversation history to help provide context for the current query:
            {chat_history}
            Only use prior conversation history if the user's query references it directly.
            If the query is unrelated, ignore the prior context entirely.
            """)
        else:
            history_message = None
        return history_message

    def state_to_json(self, state: JobState):
        j = {
            'user_prompt': state['user_prompt'],
            'response': state['response'],
            'analysis_description': state['analysis_description'],
            'web_search_results': state['web_search_results'],
            'flipside_example_queries': state['flipside_example_queries'].query_id.tolist(),
            'flipside_sql_query': state['flipside_sql_query'],
            'improved_flipside_sql_query': state['improved_flipside_sql_query'],
            'verified_flipside_sql_query': state['verified_flipside_sql_query'],
            'analyses': [str(x) for x in state['analyses']],
            'reference_materials': state_to_reference_materials(state),
            'context_summary': state['context_summary'],
            # 'follow_up_questions': state['follow_up_questions'],
            # 'tweets': state['tweets'],
            # 'user_id': state['user_id'],
            # 'conversation_id': state['conversation_id'],
            # 'highcharts_config': state['highcharts_config'],
            # 'analyses': state['analyses'],
            # 'completed_tools': state['completed_tools'],
            # 'flipside_tables': state['flipside_tables'],
            # 'flipside_example_queries': state['flipside_example_queries'],
            # 'web_search_results': state['web_search_results']
        }
        return json.dumps(j)