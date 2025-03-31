from utils.db import pg_load_data

def load_orderbooks():
    query = 'select * from orderbooks'
    df = pg_load_data(query)
    return df

