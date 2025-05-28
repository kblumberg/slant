from utils.db import pg_execute_query

def create_state_snapshots_table():

    query = """
DROP TABLE state_snapshots;
    """
    query = """
TRUNCATE TABLE flipside_dashboards;
    """
    query = """
CREATE TABLE state_snapshots (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    user_message_id VARCHAR(255) NOT NULL,
    state JSONB
);
    """
    pg_execute_query(query)