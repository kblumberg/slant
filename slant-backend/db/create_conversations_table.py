from utils.db import pg_execute_query

def create_conversations_table():

    query = """
DROP TABLE flipside_dashboards;
    """
    query = """
TRUNCATE TABLE flipside_dashboards;
    """
    query = """
CREATE TABLE conversations (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    title VARCHAR(255) NOT NULL
);
    """
    pg_execute_query(query)