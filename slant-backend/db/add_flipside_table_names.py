
from constants.flipside import FLIPSIDE_TABLES
from utils.db import pg_load_data, pg_execute_query

def parse_tables_from_query(query_text: str) -> list[str]:
    query_text = query_text.lower()
    tables = [f for f in FLIPSIDE_TABLES if f in query_text]
    return tables

def create_agent_messages_table():
    query = """
        SELECT id::text as id, text::text as query_text
        FROM flipside_queries
        WHERE id is not null
            and text is not null
    """
    flipside_queries = pg_load_data(query)
    flipside_queries['tables'] = flipside_queries['query_text'].apply(parse_tables_from_query)

    flipside_queries = flipside_queries[flipside_queries['tables'].apply(len) > 0]

    query = """
        ALTER TABLE flipside_queries ADD COLUMN tables TEXT[];
    """
    pg_execute_query(query)

    values=','.join([f"('{x[0]}', ARRAY{x[1]})" for x in flipside_queries[['id', 'tables']].values])

    query = f"""
        UPDATE flipside_queries AS fd
        SET tables = v.tables
        FROM (
            VALUES {values}
        ) AS v(id, tables)
        WHERE fd.id = v.id;
    """
    pg_execute_query(query)
