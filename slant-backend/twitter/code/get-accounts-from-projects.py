import os
import time
import requests
import psycopg2
import numpy as np
import pandas as pd
from utils.db import pg_load_data, pg_upsert_data, upload_tweet_data
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, BigInteger, Text, Integer, Column, Table, MetaData, select, and_, text


from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
from constants.keys import TWITTER_BEARER_TOKEN_NDS, POSTGRES_ENGINE, TWITTER_BEARER_TOKEN_TRAILS, SLACK_TOKEN

bearer_token = TWITTER_BEARER_TOKEN_TRAILS
AUTH = f'Bearer {bearer_token}'

def read_csv(fname):
	return pd.read_csv('/Users/kellen/git/slant-backend/twitter/data/{}.csv'.format(fname))

def write_csv(fname, df):
	df.to_csv('/Users/kellen/git/slant-backend/twitter/data/{}.csv'.format(fname), index=False)


def tmp_1():
	accounts = pd.read_csv('~/Downloads/twitter-accounts.csv')
	accounts = accounts[accounts.INCLUDE == 1]
	accounts.columns = [x.lower().strip() for x in accounts.columns]
	tmp = accounts[['twitter_handle','n_followers']]
	tmp.columns = ['project','project_followers']

	projects_accounts = pd.read_csv('~/Downloads/projects-accounts-info.csv')
	projects_accounts.head()
	projects_accounts = projects_accounts[['project_x','username_x','handle','bio','id','followers']]
	projects_accounts.columns = ['project','username','handle','bio','id','followers']
	projects_accounts = pd.merge(projects_accounts, tmp, on='project', how='left').fillna(0)
	projects_accounts = projects_accounts.sort_values('project_followers', ascending=False).drop_duplicates(subset=['id'], keep='first')
	print(len(projects_accounts))
	projects_accounts = projects_accounts[projects_accounts.project.apply(lambda x: x.lower().strip()) != projects_accounts.handle.apply(lambda x: x.lower().strip())]
	projects_accounts = projects_accounts[projects_accounts.followers > 500]
	projects_accounts = projects_accounts.sort_values(['project_followers','followers'], ascending=[0, 0])
	projects_accounts['id'] = projects_accounts.id.apply(lambda x: 't-'+str(x))
	projects_accounts.head(3)
	projects_accounts.columns
	projects_accounts.to_csv('~/Downloads/projects-accounts-check.csv', index=False)



def update_scores():
	query = 'select '
	ids = sorted(projects_accounts.id.unique())

	data = []
	tot = len(ids)
	seen = [x[0] for x in data]
	for i, id in enumerate(ids):
		id = id[2:]
		if id in seen:
			continue
	print(i, '/', tot, ': ', id, len(data))
	time.sleep(1)
	tweets = get_user_tweets(id)
	med_impression_count = np.median([x['public_metrics']['impression_count'] for x in tweets])
	avg_impression_count = np.mean([x['public_metrics']['impression_count'] for x in tweets])
	n = len(tweets)
	data += [[id, med_impression_count, avg_impression_count, n]]
	print(data[-1])
	user_detail = pd.DataFrame(data, columns=['id','med_impression_count','avg_impression_count','n'])
	user_detail['score'] = user_detail.med_impression_count * user_detail.n / (user_detail.n + 2)
	user_detail['tid'] = 't-'+user_detail.id

	projects_accounts_with_details = pd.merge(projects_accounts.rename(columns={'id':'tid'}), user_detail, on='tid', how='left')
	projects_accounts_with_details = projects_accounts_with_details.sort_values('score', ascending=False)
	exist = [x.lower() for x in accounts.twitter_handle.unique()]
	projects_accounts_with_details['included'] = projects_accounts_with_details.handle.apply(lambda x: x.lower() in exist).astype(int)
	projects_accounts_with_details['valid'] = 0
	projects_accounts_with_details.to_csv('~/Downloads/projects-accounts-check-with-details.csv', index=False)

def get_following(user_id):
	user_id = '964441502'

	# Try OAuth 1.0a (Consumer keys + Access tokens)
	api_key = api_keys['consumer_key']	
	api_secret = api_keys['consumer_secret']
	access_token = api_keys['access_token']
	access_secret = api_keys['access_token_secret']

	client = tweepy.Client(bearer_token=bearer_token)
	client = tweepy.Client(
		consumer_key=api_key,
		consumer_secret=api_secret,
		access_token=access_token,
		access_token_secret=access_secret,
	)
	your_followers = set()
	pagination_token = None
	while True:
		try:
			response = client.get_users_followers(
				user_id,
				max_results=10,  # Maximum allowed per request
				pagination_token=pagination_token
			)
			
			if response.data:
				for follower in response.data:
					your_followers.add(follower.id)
			
			print(f"Collected {len(your_followers)} of your followers so far...")
			
			# Check if we need to paginate further
			pagination_token = response.meta.get('next_token')
			if not pagination_token:
				break
				
			# Respect rate limits
			time.sleep(1)
			
		except tweepy.TooManyRequests:
			print("Rate limit reached, waiting 15 minutes...")
			time.sleep(15 * 60)
		except Exception as e:
			print(f"Error fetching your followers: {e}")
			break

	your_followers = []
	for follower in tweepy.Paginator(client.get_users_followers, user_id, max_results=1000):
		your_followers.extend(follower.data if follower.data else [])

	return following

def get_user_following(user_id):

	soup = BeautifulSoup(driver.page_source, "html.parser")

	# Scroll down a few times to load more results
	body = driver.find_element(By.TAG_NAME, "body")

	data = []

	for i in range(15):
		if i >= 0:
			body.send_keys(Keys.PAGE_DOWN)
			print(len(data))
			time.sleep(1)
			# body.send_keys(Keys.PAGE_DOWN)
			# time.sleep(0.5)
		# Extract user profile cards
		soup2 = BeautifulSoup(driver.page_source, "html.parser")
		users = soup2.find_all("div", {"data-testid": "cellInnerDiv"})
		len(users)

		for user in users:
			username_elem = user.find("div", {"dir": "ltr"})
			bio_elem = user.find_all("div", {"dir": "auto"})
			
			username = username_elem.text.strip() if username_elem else None
			handle = user.find_all('a')
			handle = handle[0].attrs['href'].split('/')[-1].strip() if len(handle) else None
			bio = bio_elem[-1].text.strip() if len(bio_elem) else None
			data += [[ username, handle, bio ]]
	df = pd.DataFrame(data, columns=['username','handle','bio']).drop_duplicates(subset=['username'])
	df.to_csv('~/Downloads/miles_following.csv', index=False)

def get_list_members(list_id):
	lists = [
		['1460640108177149966','Solana Geniuses']
		, ['1634565075905159171','Solana NFT']
		, ['1465525633413357576','Solana Ecosystem']
		, ['1632798602979336193','Gecko dao']
		, ['1452915697189679109','NFT SOL KOLS']
		, ['1574726268448374785','People who are legit']
		, ['1548876062074867712','Collectooors']
		, ['1373318498366095360','Sol project']
		, ['1561975722440159232','Solana Gang']
		, ['1566376199802306560','All signal, no noise']
		, ['1529169261666639874','Analytics Insiders']
		, ['1608181740392288256','The Solana Ecosystem']
		, ['1612717377220915201','Defi']
		, ['1626388816562098177','Gigabrain']
		, ['1490425925330112512','Solana Projects']
	]
	members = pd.DataFrame()
	for list_id, list_name in lists:
		print(list_id)
		if len(members) and list_id in members.list_id.unique():
			print('already done')
			continue
		url = f"https://api.twitter.com/2/lists/{list_id}/members"
		params = {
			"max_results": 100
			, "pagination_token": None
			, "user.fields": "id,username,name,description,public_metrics"
		}
		headers = {
			"Authorization": f"Bearer {bearer_token}",
		}
		it = 0
		while next_token or it == 0:
			it += 1
			print(it)

			response = requests.get(url, headers=headers, params=params)
			j = response.json()
			cur = pd.DataFrame(j['data'])
			cur['list_id'] = list_id
			cur['list_name'] = list_name
			members = pd.concat([members, cur])
			print(j['meta'])
			next_token = j['meta']['next_token'] if 'next_token' in j['meta'] else None
			params['pagination_token'] = next_token
	members = members.drop_duplicates(subset=['id','list_id'])
	n = members.groupby('id').count().rename(columns={'list_id':'n_lists'})
	members = pd.merge(members, n, on='id', how='left')
	members.to_csv('~/Downloads/list-members.csv', index=False)


def run_get_list_tweets():
	while True:
		try:
			query = 'select max(created_at) as mx, min(created_at) as mn, max(id) as mx_id, min(id) as mn_id from tweets'
			df = load_data_from_postgres(query)
			created_at = df['mx'].max()
			get_list_tweets(start_time=created_at)
		except Exception as e:
			print(f'error: {e}')
			# Send Slack DM about the error
			client = WebClient(token=SLACK_TOKEN)
			channel_id = 'D08HD8DC03F'
			client.chat_postMessage(channel=channel_id, text=f"Error in run_get_list_tweets: {str(e)}")
		next_run = datetime.now() + timedelta(hours=3)
		print(f'sleeping until {next_run.strftime("%Y-%m-%d %H:%M:%S")}')
		time.sleep(60 * 60 * 3)

def get_users_by_ids(user_ids):
	"""Fetch Twitter users by their IDs in groups of 100."""
	url = "https://api.twitter.com/2/users"
	headers = {"Authorization": f"Bearer {bearer_token}"}
	user_id_batches = [user_ids[i:i + 100] for i in range(0, len(user_ids), 100)]

	all_users = []
	for batch in user_id_batches:
		params = {
			"ids": ",".join(batch)
			, "user.fields": "id,username,name,description,public_metrics"
		}
		response = requests.get(url, headers=headers, params=params)
		print('response')
		print(response)
		print(response.json())
		if response.status_code == 200:
			all_users.extend(response.json().get("data", []))
		else:
			print(f"Error {response.status_code}: {response.text}")
	all_users_df = pd.DataFrame(all_users)
	print('all_users_df.head()')
	print(all_users_df.head())
	all_users_df['followers_count'] = all_users_df.public_metrics.apply(lambda x: x['followers_count'])
	cols = ['id', 'username', 'name', 'description', 'followers_count']
	all_users_df = all_users_df[cols]
	all_users_df['tracking'] = False
	# all_users_df.to_csv('~/Downloads/all_users.csv', index=False)

	return all_users_df

def create_twitter_kols_table():
	engine = create_engine(POSTGRES_ENGINE)
	metadata = MetaData()

	# Create twitter_accounts table if it doesn't exist
	twitter_accounts = Table(
		"twitter_accounts", metadata,
		Column("id", BigInteger, primary_key=True),
		Column("handle", Text),
		Column("username", Text),
		Column("followers", Integer),
		Column("account_type", Text),
		Column("ecosystem", Text),
		Column("bio", Text),
		Column("sector", Text),
		Column("project_handle", Text)
	)

	# Create table if it doesn't exist
	metadata.create_all(engine)

	# Get all IDs from twitter_accounts table
	conn = psycopg2.connect(POSTGRES_ENGINE)
	cursor = conn.cursor()
	cursor.execute("SELECT id, account_type, handle FROM twitter_accounts")
	result = cursor.fetchall()
	cur = pd.DataFrame(result, columns=['id','account_type','handle'])
	account_ids = cur.id.unique()
	cursor.close()
	conn.close()

	users_df = get_users_by_ids(account_ids)
	tmp = pd.merge(cur, all_users_df, on='id', how='left')
	tmp[tmp.username.isnull()]
	tmp = tmp[tmp.username.notnull()]
	tmp['associated_project_id'] = None
	cols = ['id', 'account_type', 'tracking', 'username', 'name', 'description', 'followers_count','associated_project_id']
	tmp = tmp[cols]




	# add KOLS to projects table
	query = 'select * from twitter_kols'
	df = load_data_from_postgres(query)
	tmp = pd.read_csv('~/Downloads/twitter_kols-1.csv')
	df = pd.merge(df[['name','username','description']], tmp, on='username', how='inner')
	df.loc[df.username == 'GaiminGladiator', 'name'] = 'Gaimin Gladiators'
	df.loc[df.username == 'HawkFi_', 'name'] = 'HawkFi'
	df.loc[df.username == 'soldexai', 'name'] = 'Soldex'
	df['name'] = df.name.apply(lambda x: ''.join(c for c in x if c.isalpha() or c == ' ').strip())
	df['ecosystem'] = 'solana'
	projects = df.copy()
	projects['parent_project_id'] = None
	projects = projects[['name','description','parent_project_id','tags','ecosystem']]
	projects = projects[projects.tags != 'na']
	projects['tags'] = projects.tags.apply(lambda x: x.split(',') if x == x else [])
	projects['tags'] = projects['tags'].apply(json.dumps)
	engine = create_engine(POSTGRES_ENGINE)
	projects.to_sql('projects', engine, if_exists="append", index=False)


	# update tracked field
	users = get_users_in_list()
	users_df = pd.DataFrame(users)
	users_df['id'] = users_df['id'].astype(int)
	users_df.to_csv('~/Downloads/users.csv', index=False)
	ids = users_df.id.unique()
	for i in ids:
		query = f"update twitter_kols set tracking = true where id = {i}"
		execute_query(query)

	# add users from list to twitter_kols table
	query = 'select * from twitter_kols'
	df = load_data_from_postgres(query)
	users_df = pd.merge(users_df, df[['id','tracking']], on='id', how='left')
	users_df[users_df.tracking.isnull()]

	upload = get_users_by_ids([str(x) for x in users_df[users_df.tracking.isnull()].id.unique()])
	upload['tracking'] = True
	upload['account_type'] = 'influencer'
	# upload['ecosystem'] = 'solana'
	# del upload['ecosystem']
	upload.loc[upload.username == 'solendprotocol', 'account_type'] = 'project'
	upload.loc[upload.username == 'thewhalesmusic', 'account_type'] = 'project'
	upload.loc[upload.username == 'jump_firedancer', 'account_type'] = 'project'
	upload.loc[upload.username == 'bonfida', 'account_type'] = 'project'
	upload.loc[upload.username == 'okaybears', 'account_type'] = 'project'
	upload.to_sql('twitter_kols', engine, if_exists="append", index=False)

	query = 'delete from twitter_kols where id = 1769403091453923328'
	execute_query(query)



	
	query = 'select * from twitter_kols where tracking = true'
	df = load_data_from_postgres(query)
	df.to_csv('~/Downloads/twitter_kols-2.csv', index=False)

	query = 'select id, name from projects'
	df = load_data_from_postgres(query)
	df.to_csv('~/Downloads/projects.csv', index=False)

	pmap = pd.merge(projects, df, on='name', how='left')
	cur = tmp.copy()
	cur['name'] = cur.name.apply(lambda x: ''.join(c for c in x if c.isalpha() or c == ' ').strip())
	pmap = pd.merge(pmap, cur, on='name', how='left')
	pmap[['id_y']]
	pmap[pmap.id_y.isnull()]

	cur = tmp.copy()
	cur['name'] = cur.name.apply(lambda x: ''.join(c for c in x if c.isalpha() or c == ' ').strip())
	pmap = pd.merge(pmap, cur, on='name', how='left')


	conn = psycopg2.connect(POSTGRES_ENGINE)
	cursor = conn.cursor()
	query = 'alter table twitter_kols alter column followers_count type int USING followers_count::int'
	cursor.execute(query)
	conn.commit()
	conn.close()
	query = 'alter table twitter_kols add column associated_project_id bigint'

	conn = psycopg2.connect(POSTGRES_ENGINE)
	cursor = conn.cursor()
	for row in pmap.iterrows():
		row = row[1]
		query = f"update twitter_kols set associated_project_id = {row['id_x']} where id = {row['id_y']}"
		cursor.execute(query)
	conn.commit()
	cursor.close()
	conn.close()

	query = 'select id, handle, project_handle from twitter_accounts'
	twitter_accounts = load_data_from_postgres(query)
	# df.to_csv('~/Downloads/twitter_accounts.csv', index=False)

	twitter_kol_project_map = pd.merge(twitter_accounts.rename(columns={'project_handle':'username'}), pmap[['username_x','id_x']].rename(columns={'username_x':'username'}), on='username', how='left')
	upload = twitter_kol_project_map[(twitter_kol_project_map.id_x.notnull()) & (twitter_kol_project_map.id.notnull())]
	conn = psycopg2.connect(POSTGRES_ENGINE)
	cursor = conn.cursor()
	for row in upload.iterrows():
		row = row[1]
		query = f"update twitter_kols set associated_project_id = {int(row['id_x'])} where id = {row['id']}"
		cursor.execute(query)
	conn.commit()
	conn.close()

	todo = twitter_kol_project_map[(twitter_kol_project_map.id_x.isnull()) & (twitter_kol_project_map.username.notnull())]
	a = pmap[['username_x','id_x']].rename(columns={'username_x':'username'})
	a['username'] = a.username.apply(lambda x: x.lower())
	todo = pd.merge(todo, a, on='username', how='left')
	todo = todo[todo.id_x_y.notnull()]

	conn = psycopg2.connect(POSTGRES_ENGINE)
	cursor = conn.cursor()
	for row in todo.iterrows():
		row = row[1]
		query = f"update twitter_kols set associated_project_id = {int(row['id_x_y'])} where id = {row['id']}"
		cursor.execute(query)
	conn.commit()
	conn.close()

	query = 'select * from twitter_kols tk left join projects p on tk.associated_project_id = p.id'
	a = load_data_from_postgres(query)
	a
	a.to_csv('~/Downloads/twitter_kols.csv', index=False)

	query = "update twitter_kols set account_type = 'influencer' where username in ('0xrooter','AngelicTheGame','panicselling','AndyRewNFT','mattytay','CloakdDev','Pland__','usgoose')"
	execute_query(query)


	query = 'select * from twitter_kols where account_type is null'
	a = load_data_from_postgres(query)
	a.to_csv('~/Downloads/twitter_kols.csv', index=False)
	a = pd.read_csv('~/Downloads/twitter_kols.csv')
	for row in a.iterrows():
		row = row[1]
		query = f"update twitter_kols set account_type = '{row['account_type']}' where username = '{row['username']}'"
		execute_query(query)

	query = 'select * from twitter_kols tk left join projects p on tk.associated_project_id = p.id'
	a = load_data_from_postgres(query)
	a.to_csv('~/Downloads/twitter_kols-1.csv', index=False)
	a = pd.read_csv('~/Downloads/twitter_kols.csv')
	for row in a.iterrows():
		row = row[1]
		query = f"update twitter_kols set account_type = '{row['account_type']}' where username = '{row['username']}'"
		execute_query(query)

	twitter_kol_project_map[twitter_kol_project_map.id_x.isnull()]
	twitter_kol_project_map[twitter_kol_project_map.id_x.notnull()].to_csv('~/Downloads/twitter_kol_project_map.csv', index=False)


	# Get user data for all account IDs
	user_data = []
	for account_id in account_ids:
		url = f"https://api.twitter.com/2/users/{account_id}"
		
		headers = {
			"Authorization": f"Bearer {bearer_token}"
		}
		
		params = {
			"user.fields": "id,username,name,description,public_metrics"
		}
		
		try:
			response = requests.get(url, headers=headers, params=params)
			
			if response.status_code == 200:
				data = response.json().get("data", {})
				user_data.append({
					'id': data.get('id'),
					'username': data.get('username'),
					'name': data.get('name'),
					'bio': data.get('description'),
					'followers': data['public_metrics']['followers_count'],
					'following': data['public_metrics']['following_count'],
					'tweets': data['public_metrics']['tweet_count']
				})
			else:
				print(f"Error getting data for user {account_id}: {response.status_code}")
				
			# Rate limiting - 900 requests per 15 minutes
			time.sleep(1)
			
		except Exception as e:
			print(f"Exception getting data for user {account_id}: {str(e)}")
			continue

	return account_ids