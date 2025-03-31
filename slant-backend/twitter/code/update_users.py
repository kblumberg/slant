import pandas as pd
from utils.utils import get_base_path, read_csv
from utils.db import pg_upload_data
from utils.twitter import get_user_by_username
from constants.db import TWITTER_KOL_PG_COLS
from utils.utils import log

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
