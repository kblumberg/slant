from utils.db import fs_load_data

query = """
select *
from solana.information_schema.columns
where table_schema in ('CORE','DEFI','GOV','NFT','PRICE','STATS')
"""
df, error = fs_load_data(query)

df['table'] = df.table_catalog + '.' + df.table_schema + '.' + df.table_name
tables = sorted(df.table.unique())
df[['column_name', 'data_type']].drop_duplicates().to_csv('~/Downloads/columns.csv', index=False)
columns = sorted(df.column_name.unique())

def column_is_valid(row: pd.Series) -> bool:
    column = row['column_name'].lower()
    data_type = row['data_type'].lower()
    exclude = ['_id', 'time','_authority','address']
    exclude_data_types = ['array', 'boolean', 'date', 'float','number','object','timestamp','variant']
    for e in exclude:
        if e in column:
            return False
    for e in exclude_data_types:
        if e in data_type:
            return False
    return True
df['valid'] = df.apply(column_is_valid, 1)
df[['column_name', 'data_type', 'valid']].drop_duplicates().to_csv('~/Downloads/columns.csv', index=False)
df[['table', 'column_name', 'data_type', 'valid']].drop_duplicates().to_csv('~/Downloads/columns.csv', index=False)
valid_columns = set([
    'ACTION'
    , 'ACTION_TYPE'
    , 'DIRECTION'
    , 'EVENT_TYPE'
    , 'LABEL_SUBTYPE'
    , 'LABEL_TYPE'
    , 'MARKETPLACE'
    , 'PLATFORM'
    , 'POOL_NAME'
    , 'PROGRAM_NAME'
    , 'STAKE_POOL_NAME'
    , 'VALIDATOR_NAME'
    , 'VOTE_TYPE'
])

for table in tables:
    if table in ['SOLANA.CORE.DIM_IDLS','SOLANA.CORE.DIM_LABELS']:
        continue
    cols = set(df[df.table == table].column_name.unique()).intersection(valid_columns)
    for c in cols:
        print(table, c)
        query = f"""
            select {c}, count(1) as n
            from {table}
            where 1=1
            {"and block_timestamp > current_date - interval '1 day'" if 'block_timestamp' in [ x.lower() for x in df[df.table == table].column_name.unique()] else ''}
            and {c} is not null
            group by 1
            order by 2 desc
            limit 10
        """
        result, error = fs_load_data(query)
        result.to_csv(f'~/Downloads/fs_{table}_{c}.csv', index=False)
        print(result.head(3))
        print('-'*100)
