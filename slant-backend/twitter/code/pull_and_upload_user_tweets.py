import time
from utils.db import pg_load_data, upload_tweet_data
from utils.twitter import get_user_tweets
from constants.keys import ACTIVE_TWITTER_TOKENS

def pull_and_upload_user_tweets():

	# load all twitter accounts that we are tracking
	query = 'SELECT * FROM twitter_kols where tracking = true'
	result = pg_load_data(query)

	result['exists'] = result.id.isin([int(x['id']) for x in users])
	result[result.exists == False][['username']]

	# check to see if the tweets have already been uploaded
	query = 'SELECT distinct t.author_id FROM tweets t where t.created_at < 1741309200'
	exists = pg_load_data(query)

	# filter out the accounts that have already been uploaded
	result['exists'] = result.id.isin(exists.author_id)
	print(result[result.exists == False])
	ids = [1460384703597269001]
	ids = [x for x in result.id.astype(int).unique() if x not in exists.author_id.astype(int).unique()]


	tot = len(ids)
	print(tot)
	all_tweets = []
	all_includes = []
	all_tweets_includes = []
	tokens = ACTIVE_TWITTER_TOKENS
	sleep_time = 0 if tot < 15 else 90 / len(tokens)
	for i, id in enumerate(ids):
		hours = (tot - i) * sleep_time * 1.5 / (60 * 60)
		print('#', i, '/', tot, ': ', id, ' (', hours, 'hours remaining)')
		try:
			tweets, includes, tweets_includes = get_user_tweets(id, tokens[i % len(tokens)], sleep_time)
			all_tweets += tweets
			all_includes += includes
			all_tweets_includes += tweets_includes
			print(len(all_tweets), len(all_includes), len(all_tweets_includes))
		except Exception as e:
			print(f"Error: {e} for user {id}")
		time.sleep(sleep_time)
	upload_tweet_data(all_tweets, all_includes, all_tweets_includes)
