import os
import re
import ast
import psycopg2
import pandas as pd
from tqdm import tqdm
from pinecone import Pinecone
from sqlalchemy import create_engine, MetaData, Table, Column, BigInteger, Integer, Text, select, and_, VARCHAR, TIMESTAMP, ARRAY
import numpy as np
import time
from langchain_openai import OpenAIEmbeddings
from constants.keys import PINECONE_API_KEY, POSTGRES_ENGINE, FLIPSIDE_API_KEY
from utils.utils import log
from flipside import Flipside


def pg_execute_query(query, values=None):
	# log('pg_execute_query')
	# log(query)
	conn = psycopg2.connect(POSTGRES_ENGINE)
	cursor = conn.cursor()
	if values:
		cursor.execute(query, values)
	else:
		cursor.execute(query)
	conn.commit()
	conn.close()

def pc_execute_query(query, index_name="slant", namespace="flipside_queries", filter_conditions={}, top_k=8):
	# log('pc_execute_query')
	# log(query)
	pc = Pinecone(api_key=PINECONE_API_KEY)
	index = pc.Index(index_name, namespace=namespace)
	embeddings = OpenAIEmbeddings()
	query_embedding = embeddings.embed_query(query)
	results = index.query(
		vector=query_embedding
		, top_k=top_k
		, include_metadata=True
		, filter=filter_conditions
		, namespace=namespace
	)
	return results

def pc_load_data(index, namespace=None):
	pc = Pinecone(api_key=PINECONE_API_KEY)
	index = pc.Index(index)

	# Get total vector count
	total_vectors = index.describe_index_stats()['total_vector_count']
	# log(f"Total vectors: {total_vectors}")

	all_records = pd.DataFrame()

	# Iterate over namespaces (if applicable)
	for n in index.describe_index_stats()['namespaces']:
		# log(f"Namespace: {n}")
		it = 0
		if namespace is None or namespace == n:
			for ids_chunk in index.list(namespace=n):
				it += 1
				# log(f"#{it}")
				# Fetch records in chunks
				records_chunk = index.fetch(ids=ids_chunk, namespace=n)
				cur = pd.DataFrame(records_chunk.to_dict()['vectors'])
				cur = cur.transpose()
				all_records = pd.concat([all_records, cur])
	return all_records

def pg_load_data(query, timeout_in_seconds=0, values=None):
	try:
		# log('pg_load_data')
		# log(query)
		conn = psycopg2.connect(POSTGRES_ENGINE)
		cursor = conn.cursor()
		if timeout_in_seconds > 0:
			cursor.execute(f"SET statement_timeout = {timeout_in_seconds * 1000};")
		if values:
			cursor.execute(query, values)
		else:
			cursor.execute(query)
		df = pd.DataFrame(cursor.fetchall(), columns=[x[0] for x in cursor.description])
		df.columns = [x.lower() for x in df.columns]
		return df
	except Exception as e:
		# log('pg_load_data error: {}'.format(e))
		return pd.DataFrame()

def fs_load_data(query: str, timeout_minutes=3) -> tuple[pd.DataFrame, Exception]:
	try:
		# log('fs_load_data')
		# log('query')
		# log(query)
		# query = """WITH buyback_data AS (
		# SELECT 
		# 	date_trunc('day', block_timestamp) AS day,
		# 	SUM(amount * p.price) / SUM(amount) AS avg_purchase_price
		# FROM solana.core.fact_transfers t
		# JOIN solana.price.ez_prices_hourly p 
		# 	ON date_trunc('day', p.hour) = date_trunc('day', t.block_timestamp)
		# 	AND t.mint = p.token_address
		# WHERE t.tx_to = '6tZT9AUcQn4iHMH79YZEXSy55kDLQ4VbA3PMtfLVNsFX'
		# 	AND t.mint = 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN'
		# GROUP BY 1
		# ),
		# current_price AS (
		# SELECT 
		# 	price AS current_price
		# FROM solana.price.ez_prices_hourly
		# WHERE token_address = 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN'
		# ORDER BY hour DESC
		# LIMIT 1
		# )
		# SELECT 
		# AVG(b.avg_purchase_price) AS avg_purchase_price,
		# c.current_price,
		# AVG(b.avg_purchase_price) - c.current_price AS price_difference
		# FROM buyback_data b
		# CROSS JOIN current_price c;"""
		fs = Flipside(api_key=FLIPSIDE_API_KEY)
		# log('loaded fs')
		# log(query)
		df = fs.query(query, timeout_minutes=timeout_minutes)
		# df = fs.query('select 1 from solana.core.fact_transactions limit 1')
		# log('loaded df')
		df = pd.DataFrame(df.dict()['records'])
		if '__row_index' in df.columns:
			df = df.drop(columns=['__row_index'])
		return df, ''
	except Exception as e:
		try:
			# log('e.args')
			# log(e.args)
			s = re.split('errorData=', e.args[0])
			# log('s[1]')
			# log(s[1])
			j = ast.literal_eval(s[1])
			# log('j')
			# log(j)
			return pd.DataFrame(), j['message']
		except Exception as e2:
			# log(e2)
			return pd.DataFrame(), e.args[0]

def pg_upload_data(df, table, if_exists="append"):
	engine = create_engine(POSTGRES_ENGINE)
	df.to_sql(table, engine, if_exists=if_exists, index=False)

def upload_tweet_data(tweets, includes, tweets_includes):
	# log('upload_tweet_data')
	tweets_df = pd.DataFrame(tweets)
	twitter_users_df = pd.DataFrame(includes)
	tweets_includes_df = pd.DataFrame(tweets_includes)

	referenced_tweets = tweets_df[['id','referenced_tweets']].dropna()

	for c in ['retweet_count','reply_count','like_count','quote_count','impression_count']:
		tweets_df[c] = tweets_df.public_metrics.apply(lambda x: x[c])
	cols = ['id','conversation_id','author_id','created_at','text','retweet_count','reply_count','like_count','quote_count','impression_count']
	tweets_df = tweets_df[cols]
	tweets_df['created_at'] = pd.to_datetime(tweets_df['created_at']).astype(np.int64) // 10**9

	# Upload tweets to postgres, replacing existing records with same id
	engine = create_engine(POSTGRES_ENGINE)
	metadata = MetaData()
	tweets_table = Table(
		"tweets", metadata,
		Column("id", BigInteger, primary_key=True),
		Column("conversation_id", BigInteger),
		Column("author_id", BigInteger),
		Column("created_at", Integer),
		Column("text", Text),
		Column("retweet_count", Integer),
		Column("reply_count", Integer),
		Column("like_count", Integer),
		Column("quote_count", Integer),
		Column("impression_count", Integer)
	)
	for c in ['id','conversation_id','author_id']:
		tweets_df[c] = tweets_df[c].astype(int)

	pg_upsert_data(tweets_df, tweets_table, engine)

	# Explode referenced_tweets into separate rows
	referenced_tweets = referenced_tweets.explode('referenced_tweets')
	referenced_tweets['referenced_tweet_id'] = referenced_tweets.referenced_tweets.apply(lambda x: x['id'])
	referenced_tweets['referenced_tweet_type'] = referenced_tweets.referenced_tweets.apply(lambda x: x['type'])
	referenced_tweets = referenced_tweets[['id','referenced_tweet_id','referenced_tweet_type']]
	referenced_tweets = pd.merge(referenced_tweets, tweets_includes_df[['id','author_id']].rename(columns={'id':'referenced_tweet_id'}), on='referenced_tweet_id', how='left')
	referenced_tweets['author_id'] = referenced_tweets.author_id.fillna(0)
	for c in ['id','referenced_tweet_id','author_id']:
		referenced_tweets[c] = referenced_tweets[c].astype(int)

	metadata = MetaData()
	tweets_table = Table(
		"referenced_tweets", metadata,
		Column("id", BigInteger),
		Column("referenced_tweet_id", BigInteger),
		Column("referenced_tweet_type", Text),
		Column("author_id", BigInteger)
	)
	# referenced_tweets[referenced_tweets.author_id.isnull()]
	pg_upsert_data(referenced_tweets, tweets_table, engine, ['id','referenced_tweet_id','referenced_tweet_type','author_id'])

	for c in ['id']:
		twitter_users_df[c] = twitter_users_df[c].astype(int)
	metadata = MetaData()
	twitter_users_table = Table(
		"twitter_users", metadata,
		Column("id", BigInteger),
		Column("name", Text),
		Column("username", Text)
	)
	pg_upsert_data(twitter_users_df[['id','name','username']], twitter_users_table, engine, ['id'])

def pg_upsert_flipside_dashboards(dashboards: pd.DataFrame):
	# log('pg_upsert_flipside_dashboards')
	engine = create_engine(POSTGRES_ENGINE)

	metadata = MetaData()
	dashboards_table = Table(
		"flipside_dashboards", metadata,
		Column("id", VARCHAR(255), primary_key=True),
		Column("title", VARCHAR(255)),
		Column("latest_slug", VARCHAR(255)),
		Column("description", Text),
		Column("updated_at", TIMESTAMP),
		Column("created_at", TIMESTAMP),
		Column("created_by_id", VARCHAR(255)),
		Column("tags", ARRAY(Text)),
		Column("user_id", VARCHAR(255)),
		Column("user_name", Text),
	)

	pg_upsert_data(dashboards, dashboards_table, engine, ['id'])

	return True

def pg_upsert_flipside_dashboards_queries(dashboard_queries: pd.DataFrame):
	# log('pg_upsert_flipside_dashboards_queries')
	engine = create_engine(POSTGRES_ENGINE)

	metadata = MetaData()
	table = Table(
		"flipside_dashboards_queries", metadata,
		Column("query_id", VARCHAR(255)),
		Column("dashboard_id", VARCHAR(255))
	)

	pg_upsert_data(dashboard_queries, table, engine, ['query_id', 'dashboard_id'])

	return True

def pc_upload_data(df, embedding_col, metadata_cols, batch_size=100, index_name='slant', namespace=''):
	"""
	Upload data to Pinecone with embeddings and metadata
	
	Args:
		df: DataFrame containing the data
		embedding_col: Column name containing text to be embedded
		metadata_cols: List of column names to include as metadata
		batch_size: Number of records to process in each batch
		index: Pinecone index to upload to
		namespace: Pinecone namespace to upload to
	"""
	for i in range(0, len(df), batch_size):
		batch = df.iloc[i:i+batch_size]
		# log(f"Processing batch {i//batch_size + 1} of {len(df)//batch_size + 1}")
		
		# Get embeddings for batch
		texts = batch[embedding_col].tolist()
		embeddings = OpenAIEmbeddings()
		embeddings_batch = embeddings.embed_documents(texts)
		
		# Prepare vectors for batch upload
		vectors = []
		for row, emb in zip(batch.itertuples(), embeddings_batch):
			metadata = {col: getattr(row, col) for col in metadata_cols}
			
			vectors.append({
				'id': str(row.id),
				'values': emb,
				'metadata': metadata
			})
			
		# Upload batch to Pinecone
		pc = Pinecone(api_key=PINECONE_API_KEY)
		index = pc.Index(index_name)
		index.upsert(vectors=vectors, namespace=namespace)
		
		# Add small delay between batches to avoid rate limits
		time.sleep(0.5)

def pg_upsert_data(df, table, engine, index_elements=['id']):
	# Load existing rows from table where index element matches df
	# df = twitter_users_df.copy()
	# table = tweets_table
	# table = twitter_users_table
	df = df.drop_duplicates(subset=index_elements)

	existing_rows = pd.DataFrame()
	query = f'select {", ".join(index_elements)} from {table}'
	existing_rows = pg_load_data(query)
	existing_rows = existing_rows[index_elements]
	existing_rows['exists'] = 1

	if len(existing_rows):
		df = pd.merge(df, existing_rows, on=index_elements, how='left')
		df['exists'] = df['exists'].fillna(0)
	else:
		df['exists'] = 0
	# log(df.exists.mean())
	
	# Delete existing rows that need to be updated
	if df.exists.sum() > 0:
		with engine.begin() as conn:
			if len(index_elements) > 0:
				# Build WHERE clause for multiple index columns
				where_conditions = [table.c[col].in_([row[col] for row in df[df['exists'] == 1].to_dict('records')]) for col in index_elements]
				delete_stmt = table.delete().where(and_(*where_conditions))
				conn.execute(delete_stmt)
				conn.commit()
			conn.close()
	
	# Drop the exists column before inserting
	df = df.drop(columns=['exists'])

	# Insert all rows at once
	# df.to_sql(table.name, engine, if_exists='append', index=False)

	print(f'Uploading {len(df)} rows to {table.name}')
	pg_upload_data(df, table.name, if_exists='append')


	# """Upserts data into PostgreSQL table using ON CONFLICT (id) DO UPDATE."""
	# with engine.begin() as conn:
	# 	for _, row in df.iterrows():
	# 		# log(row.to_dict())
	# 		stmt = insert(table).values(**row.to_dict())
	# 		stmt = stmt.on_conflict_do_update(
	# 			index_elements=index_elements,
	# 			set_={col.name: stmt.excluded[col.name] for col in table.columns if col.name != index_elements}
	# 		)
	# 		conn.execute(stmt)

def clean_tweets_for_pc(df):
	# remove twitter links
    df['text'] = df['text'].str.replace(r'https?://t\.co\S+', '', regex=True)

    # Remove any leftover whitespace
    df['text_length'] = df['text'].apply(len)

    # Group by conversation_id and author_id
    grouped_df = df.sort_values(by='id', ascending=True).groupby(['conversation_id', 'author_id']).agg({
        'text': lambda x: ' '.join(x),
        'retweet_count': 'max',
        'impression_count': 'max', 
        'created_at': 'min',
        'id': 'first',
        'name': 'first',
        'username': 'first'
    }).reset_index()
	# clean up the text
    grouped_df['text'] = grouped_df['text'].str.replace(r'^RT\s*@\w+:\s*', '', regex=True)
    grouped_df['text'] = grouped_df['text'].apply(lambda x: x[1:] if x.startswith('.') else x)
    grouped_df['text'] = grouped_df['text'].apply(lambda x: x.strip())

	# check tweet length
    grouped_df['text_length'] = grouped_df['text'].apply(len)
    grouped_df = grouped_df[grouped_df.text_length >= 30]
    grouped_df['tweet_url'] = grouped_df.apply(lambda x: f'https://x.com/{x["username"]}/status/{x["id"]}', 1)
    return grouped_df

def get_content_for_twitter_kol(kol):
	project_name = f"Project: {kol['project_name']}" if kol['project_name'] is not None else ''
	kol_text = f"""Name: {kol['name']}
        Description: {kol['description']}
        {project_name}"""
	return kol_text

def load_tweets_for_pc(start_time = 0):

	# Query projects from Postgre
	query = f"""
		with t0 as (
			SELECT coalesce(rt.referenced_tweet_id, t.id) as id
			, coalesce(rt.referenced_tweet_id, t.conversation_id) as conversation_id
			, coalesce(tur.id, tu.id) as author_id
			, coalesce(tur.name, tu.name) as name
			, coalesce(tur.username, tu.username) as username
			, t.text
			, t.created_at
			, t.like_count
			, t.quote_count
			, t.reply_count
			, t.retweet_count
			, t.impression_count
			, rt.referenced_tweet_type
			, concat('https://x.com/', tu.username, '/status/', t.id) as tweet_url
			, length(t.text) as text_length
			, case when coalesce(t2.author_id, 0) = coalesce(rep.author_id, t2.author_id, 0) then 1 else 0 end as is_valid
			, row_number() over (partition by coalesce(rt.referenced_tweet_id, t.id) order by case when rt.referenced_tweet_id is not null then 1 else 0 end, t.like_count desc, t.created_at desc) as rn
			FROM tweets t
			left join referenced_tweets rt
				on t.id = rt.id
				and rt.referenced_tweet_type = 'retweeted'
			left join referenced_tweets rep
				on t.id = rep.id
				and rep.referenced_tweet_type = 'replied_to'
			left join tweets t2
				on t2.id = rep.id
			left join twitter_users tu
				on t.author_id = tu.id
			left join twitter_users tur
				on rt.author_id = tur.id
			where t.created_at >= {start_time}
		)
		select *
		from t0
		where rn = 1
			and text_length >= 20
			and is_valid = 1
	"""
	df = pg_load_data(query)
	return df