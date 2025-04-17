PROJECTS_RAG_COLS = ['name', 'description', 'ecosystem', 'tags', 'score']
PROJECT_PG_COLS = ['name', 'parent_project_id', 'description', 'ecosystem', 'tags', 'score']

TWITTER_KOLS_RAG_COLS = ['associated_project_id', 'description', 'name', 'username', 'score']
TWITTER_KOL_PG_COLS = ['id', 'name', 'username', 'description', 'followers_count', 'associated_project_id', 'account_type', 'tracking', 'ecosystem', 'score']

TWEETS_RAG_COLS = ['created_at', 'author_id', 'impression_count', 'retweet_count', 'text', 'tweet_url']

FLIPSIDE_QUERIES_RAG_COLS = ['text', 'tables', 'created_at', 'last_successful_execution_at', 'user_name', 'user_id', 'project_tags', 'tokens', 'dashboard_id']
# FLIPSIDE_QUERIES_RAG_COLS = ['text', 'tables', 'created_at', 'last_successful_execution_at', 'user_name', 'user_id', 'project_tags', 'dashboard_id']
