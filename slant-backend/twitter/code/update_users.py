import pandas as pd
import requests
import psycopg2
from utils.utils import get_base_path, read_csv
from utils.db import pg_upload_data
from utils.twitter import get_user_by_username
from constants.db import TWITTER_KOL_PG_COLS
from constants.keys import ACTIVE_TWITTER_TOKENS
from utils.utils import log
from constants.keys import POSTGRES_ENGINE

def update_users(df = None):
	cols = ['associated_project_id', 'username', 'tracking', 'ecosystem', 'account_type', 'score']
	if df is None:
		# we need: associated_project_id | username | tracking | ecosystem | account_type
		fname = get_base_path() + 'data/update_users.csv'
		update_users = pd.read_csv(fname)[cols]
	else:
		update_users = df[cols]

	update = []
	for _, row in update_users.iterrows():
		user = get_user_by_username(row)
		update.append(user)

	update_df = pd.DataFrame(update)
	log(update_df)
	update_df = update_df[TWITTER_KOL_PG_COLS]
	pg_upload_data(update_df, 'twitter_kols')
	return update_df

def update_profile_image_urls():
	query = '''
	select u.id
	from twitter_users u
	where u.profile_image_url is null
	'''
	df = pg_load_data(query)
	users = df.id.unique()
	for i in range(0, len(users), 100):
		user_ids = users[i:i+100]
		url = f"https://api.twitter.com/2/users"
		headers = {
			"Authorization": f"Bearer {ACTIVE_TWITTER_TOKENS[0]}"
		}
		params = {
			"user.fields": "profile_image_url",
			"ids": ",".join([str(x) for x in user_ids])
		}
		response = requests.get(url, headers=headers, params=params)
		if response.status_code == 200:
			user_data = response.json().get("data", {})
			update_pairs = [(user["id"], user["profile_image_url"]) for user in user_data if "profile_image_url" in user]
			if update_pairs:
				values_clause = ",".join(["(%s, %s)"] * len(update_pairs))
				flat_values = [item for pair in update_pairs for item in pair]

				update_sql = f"""
				UPDATE twitter_users AS t SET profile_image_url = v.profile_image_url
				FROM (VALUES {values_clause}) AS v(id, profile_image_url)
				WHERE t.id = v.id::bigint
				"""
				conn = psycopg2.connect(POSTGRES_ENGINE)
				cursor = conn.cursor()
				cursor.execute(update_sql, flat_values)
				conn.commit()
				cursor.close()
				conn.close()
		else:
			print(f"Error fetching user {user_id}: {response.status_code} - {response.text}")


def update_users(df = None):
	cols = ['associated_project_id', 'username', 'tracking', 'ecosystem', 'account_type', 'score']
	if df is None:
		# we need: associated_project_id | username | tracking | ecosystem | account_type
		fname = get_base_path() + 'data/update_users.csv'
		update_users = pd.read_csv(fname)[cols]
	else:
		update_users = df[cols]

	update = []
	for _, row in update_users.iterrows():
		user = get_user_by_username(row)
		update.append(user)

	update_df = pd.DataFrame(update)
	log(update_df)
	update_df = update_df[TWITTER_KOL_PG_COLS]
	pg_upload_data(update_df, 'twitter_kols')
	return update_df

def update_ecosystems():
	query = 'ALTER TABLE twitter_kols ADD COLUMN ecosystem varchar(255) DEFAULT NULL'
	pg_execute_query(query)

	query = 'ALTER TABLE twitter_kols ADD COLUMN ecosystem varchar(255) DEFAULT NULL'
	pg_execute_query(query)
