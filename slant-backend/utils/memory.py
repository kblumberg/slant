from typing import Dict, List, Any, Tuple, TypedDict
from langchain_core.chat_history import BaseChatMessageHistory, BaseMessage
from langchain_core.messages import HumanMessage, AIMessage
from langchain_postgres import PostgresChatMessageHistory
from constants.keys import POSTGRES_ENGINE
import psycopg2
from utils.utils import log
from langchain_core.messages import SystemMessage

class PostgresConversationMemory(BaseChatMessageHistory):
    def __init__(
        self, 
        session_id: str, 
        sync_connection: psycopg2.connect,
        table_name: str = "chat_history"
    ):
        """
        Initialize PostgreSQL-backed conversation memory.
        
        Args:
            connection_string (str): PostgreSQL database connection string
            session_id (str): Unique identifier for the conversation
            table_name (str, optional): Name of the table to store chat history
        """
        self.memory = PostgresChatMessageHistory(
            table_name
            , session_id
            , sync_connection=sync_connection
        )
        self.messages = []
    
    def save_context(self, inputs: Dict[str, Any], outputs: Dict[str, Any]) -> None:
        """
        Save the current conversation context to the database.
        
        Args:
            inputs (Dict): User input context
            outputs (Dict): Agent's response context
        """
        human_message = inputs.get('input', '')
        ai_message = outputs.get('output', '')

        log('save_context')
        log('human_message')
        if isinstance(human_message, BaseMessage):
            human_message = human_message.content
        log(human_message)
        log('ai_message')
        if isinstance(ai_message, BaseMessage):
            ai_message = ai_message.content
        log(ai_message)

        # Ensure we're working with strings
        human_message = str(human_message)
        ai_message = str(ai_message)
        val = self.memory.add_user_message(human_message)
        log('self.memory.add_user_message')
        log(val)
        val = self.memory.add_ai_message(ai_message)
        log('self.memory.add_ai_message')
        log(val)
        log('self.messages')
        log(self.messages)
        return val
    
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

# Example usage in LangGraph agent configuration
def create_crypto_agent(connection_string: str, session_id: str):
    # Memory initialization
    memory = PostgresConversationMemory(
        connection_string=connection_string, 
        session_id=session_id
    )
    
    # LangGraph agent configuration
    from langgraph.graph import StateGraph, END
    
    class AgentState(TypedDict):
        input: str
        chat_history: List[BaseMessage]
        intermediate_steps: List[Tuple]
    
    def agent_node(state: AgentState):
        # Your agent logic here
        # Use state['chat_history'] to access previous context
        pass
    
    graph = StateGraph(AgentState)
    # Configure graph with memory-aware nodes
    # ...
    
    return graph