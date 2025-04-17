import time
import pandas as pd
import numpy as np
from utils.twitter import get_user_tweets
from datetime import datetime, timedelta
from utils.db import pg_load_data, pg_execute_query

def score_accounts():
	# query = 'ALTER TABLE twitter_kols ADD COLUMN score float DEFAULT 0'
	# pg_execute_query(query)

	# query = 'ALTER TABLE twitter_kols ADD COLUMN ecosystem varchar(255) DEFAULT NULL'
	# pg_execute_query(query)

	days_60_ago = int((datetime.now() - timedelta(days=60)).timestamp())
	days_30_ago = int((datetime.now() - timedelta(days=30)).timestamp())
	days_10_ago = int((datetime.now() - timedelta(days=10)).timestamp())
	query = f"""
		SELECT tk.id
		, tk.username
		, coalesce(tk.ecosystem, p.ecosystem) as ecosystem
		, t.id as tweet_id
		, coalesce(t.impression_count, 0) as impression_count
		, coalesce(t.retweet_count, 0) as retweet_count
		, row_number() over (partition by tk.id order by t.impression_count desc) as rn
		, count(1) over (partition by tk.id) as n_tweets
		FROM twitter_kols tk
		left join tweets t
			on tk.id = t.author_id
			and t.created_at > {days_30_ago}
		left join referenced_tweets rt
			on t.id = rt.id
		left join projects p
			on tk.associated_project_id = p.id
		where 
			tk.tracking = true
			and rt.id is null
	"""
	tweets = pg_load_data(query)
	len(tweets)
	
	g = tweets[((tweets.rn >= 5) & (tweets.rn <= 15)) | ((tweets.n_tweets <= 10) & (tweets.rn <= 10))].groupby(['id','username','ecosystem']).agg({'tweet_id':'count', 'impression_count':'sum', 'retweet_count':'sum'}).reset_index()
	g.columns = ['id','username','ecosystem','n_tweets','total_impressions','total_retweets']
	g['score'] = (g.total_impressions * g.n_tweets / (g.n_tweets + 2)) ** 0.7
	g.loc[g.ecosystem != 'solana', 'score'] = g.score * 0.25
	q99 = g.score.quantile(0.99)
	g = g.sort_values('score', ascending=False)
	g.loc[g.score > q99, 'score'] = q99
	g['score'] = (g.score * 100 / q99).apply(lambda x: round(x, 2))
	g[g.username == 'runkellen']
	g.head(20)


	query = 'UPDATE twitter_kols SET score = 0'
	pg_execute_query(query)

	it = 0
	tot = len(g)
	for row in g[['id','username','score','ecosystem']].itertuples():
		it += 1
		print(it, '/', tot)
		query = f"""
			UPDATE twitter_kols
			SET score = {row.score}
			WHERE id = {row.id}
		"""
		pg_execute_query(query)


def score_projects():
	
	# query = 'ALTER TABLE projects ADD COLUMN score float DEFAULT 0'
	# pg_execute_query(query)
	query = 'UPDATE projects SET score = 0'
	pg_execute_query(query)

	query = f"""
		SELECT p.id
		, p.name
		, max(tk.score) as score
		FROM twitter_kols tk
		join projects p
			on tk.associated_project_id = p.id
		group by 1, 2
		order by 3 desc
	"""
	df = pg_load_data(query)
	df.head(20)

	it = 0
	tot = len(df)
	for row in df[['id','score']].itertuples():
		it += 1
		print(it, '/', tot)
		query = f"""
			UPDATE projects
			SET score = {row.score}
			WHERE id = {row.id}
		"""
		pg_execute_query(query)
