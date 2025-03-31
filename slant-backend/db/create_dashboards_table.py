from utils.db import pg_execute_query

def create_dashboards_table():

    query = """
DROP TABLE flipside_dashboards;
    """
    query = """
TRUNCATE TABLE flipside_dashboards;
    """
    query = """
CREATE TABLE flipside_dashboards (
    id VARCHAR(255) PRIMARY KEY,
    title TEXT,
    latest_slug VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    created_by_id VARCHAR(255) NOT NULL,
    tags TEXT[],
    user_id VARCHAR(255) NOT NULL,
    user_name VARCHAR(255) NOT NULL
);
    """
    pg_execute_query(query)