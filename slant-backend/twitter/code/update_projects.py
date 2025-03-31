import pandas as pd
import json
from utils.utils import get_base_path, read_csv
from utils.db import pg_load_data, pg_upload_data, pg_execute_query
from constants.db import PROJECT_PG_COLS
from twitter.code.update_users import update_users

def update_projects():
	# we need: associated_project_id | username | tracking | ecosystem | account_type
	cols = ['username', 'tracking', 'ecosystem', 'tags', 'parent_project_id','score']
	fname = get_base_path() + 'data/update_projects.csv'
	update_projects = read_csv(fname)[cols]
	print(update_projects)
	update_projects['account_type'] = 'project'
	update_projects['tags'] = update_projects['tags'].apply(lambda x: x.split(',') if x == x else [])
	update_projects['tags'] = update_projects['tags'].apply(json.dumps)
	# update_users['parent_project_id'] = None

	update_projects['associated_project_id'] = None
	update_users_df = update_users(update_projects)

	update_df = pd.merge(update_users_df[['username','name','description']], update_projects, on='username', how='left')
	update_df['name'] = update_df['name'].apply(lambda x: ''.join(c for c in x if c.isalpha() or c == ' ').strip())
	update_users_df['name'] = update_users_df['name'].apply(lambda x: ''.join(c for c in x if c.isalpha() or c == ' ').strip())
	d = {
		'elizaOS': 'eliza',
	}
	update_df['name'] = update_df['name'].apply(lambda x: d.get(x, x))
	update_users_df['name'] = update_users_df['name'].apply(lambda x: d.get(x, x))
	update_df = update_df[PROJECT_PG_COLS]

	pg_upload_data(update_df, 'projects')
	
	query = 'select * from projects'
	new_projects = pg_load_data(query)
	new_projects = new_projects[new_projects.name.isin(update_df.name)]
	new_users = pd.merge(new_projects, update_users_df[['name','id']], on='name', how='inner')

	for _, row in new_users.iterrows():
		query = f"update twitter_kols set associated_project_id = {row['id_x']} where id = {row['id_y']}"
		print(query)
		pg_execute_query(query)
