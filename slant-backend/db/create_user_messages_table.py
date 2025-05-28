from utils.db import pg_execute_query

def create_user_messages_table():

    query = """
DROP TABLE flipside_dashboards;
    """
    query = """
TRUNCATE TABLE flipside_dashboards;
    """
    query = """
CREATE TABLE user_messages (
    id VARCHAR(255) PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    message TEXT NOT NULL,
    conversation_id VARCHAR(255) NOT NULL
);
    """
    pg_execute_query(query)