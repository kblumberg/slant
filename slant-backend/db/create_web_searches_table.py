from utils.db import pg_execute_query

def create_agent_messages_table():

    query = """
DROP TABLE web_searches;
    """
    query = """
TRUNCATE TABLE web_searches;
    """
    query = """
CREATE TABLE web_searches (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    project VARCHAR(255) NOT NULL,
    project_id INTEGER DEFAULT NULL,
    user_message_id VARCHAR(255) NOT NULL,
    search_query TEXT NOT NULL,
    base_url TEXT NOT NULL,
    url TEXT NOT NULL,
    text TEXT NOT NULL
);
    """
    pg_execute_query(query)