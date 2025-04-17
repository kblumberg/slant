from utils.db import pg_execute_query

def create_user_prompts_table():

    query = """
DROP TABLE chat_history;
    """
    query = """
CREATE TABLE chat_history (
    id SERIAL PRIMARY KEY,
    conversation_id VARCHAR(255) NOT NULL,
    message JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
    """
    pg_execute_query(query)