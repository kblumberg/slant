from utils.db import pg_execute_query

def create_flipside_dashboards_queries_table():

    query = """
DROP TABLE flipside_dashboards_queries;
    """
    query = """
TRUNCATE TABLE flipside_dashboards_queries;
    """
    query = """
CREATE TABLE flipside_dashboards_queries (
    query_id VARCHAR(255) NOT NULL,
    dashboard_id VARCHAR(255) NOT NULL,
    PRIMARY KEY (query_id, dashboard_id)
);
    """
    pg_execute_query(query)