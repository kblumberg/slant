from utils.db import pg_execute_query

def create_agent_messages_table():

    query = """
DROP TABLE news;
    """
    query = """
TRUNCATE TABLE news;
    """
    query = """
        CREATE TABLE news (
            headline text,
            summary text,
            key_takeaway text,
            sources jsonb,
            projects jsonb,
            tag text,
            score double precision,
            timestamp bigint,
            original_tweets jsonb,
            updated_at timestamp
        );
    """
    pg_execute_query(query)