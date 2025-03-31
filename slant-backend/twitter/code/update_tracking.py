import pandas as pd
from utils.twitter import get_users_in_list
from utils.db import pg_load_data, pg_execute_query

def update_tracking():
	users = get_users_in_list()
	users_df = pd.DataFrame(users)
	users_df['id'] = users_df['id'].astype(int)

	query = 'select * from twitter_kols'
	df = pg_load_data(query)
	tmp = users_df[-users_df.id.isin(df.id)]
	tmp.to_csv('~/Downloads/tmp.csv', index=False)
	ids = [int(user['id']) for user in users]
	df['tracking_x'] = df.id.isin(ids)
	g = df[df.tracking != df.tracking_x]
	for _, row in g.iterrows():
		query = f"update twitter_kols set tracking = {row['tracking_x']} where id = {row['id']}"
		print(query)
		pg_execute_query(query)
