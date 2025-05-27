import time
import requests
import pandas as pd
from datetime import datetime, timezone
from utils.db import pg_load_data, pg_execute_query
from constants.keys import ACTIVE_TWITTER_TOKENS

def get_user_by_username(row):
	url = f"https://api.twitter.com/2/users/by/username/{row['username']}"

	headers = {
		"Authorization": f"Bearer {ACTIVE_TWITTER_TOKENS[0]}",
	}

	params = {
		"user.fields": "id,username,name,description,public_metrics"
	}

	response = requests.get(url, headers=headers, params=params)

	if response.status_code == 200:
		user_data = response.json().get("data", {})
		d = {
			'id': user_data.get('id'),
			'name': user_data.get('name'),
			'username': user_data.get('username'),
			'ecosystem': row['ecosystem'],
			'account_type': row['account_type'],
			'description': user_data.get('description'),
			'followers_count': user_data['public_metrics']['followers_count'],
			'associated_project_id': row['associated_project_id'],
			'tracking': row['tracking'],
			'score': row['score']
		}
		return d
	else:
		print(f"Error: {response.status_code} - {response.text}")

def get_users_in_list(list_id='1896993199560032554'):
	headers = {"Authorization": f"Bearer {ACTIVE_TWITTER_TOKENS[0]}"}
	url = f"https://api.twitter.com/2/lists/{list_id}/members"

	response = requests.get(url, headers=headers)
	users = []
	if response.status_code == 200:
		users = response.json().get("data", [])
		next_token = response.json().get("meta", {}).get("next_token")
		while next_token:
			url = f"https://api.twitter.com/2/lists/{list_id}/members?pagination_token={next_token}"
			response = requests.get(url, headers=headers)
			users += response.json().get("data", [])
			next_token = response.json().get("meta", {}).get("next_token")
	else:
		print("Error:", response.json())
	return users


def get_list_tweets(list_id="1896993199560032554", start_time=None, batch_size=20):
	url = f"https://api.twitter.com/2/lists/{list_id}/tweets"

	headers = {
		"Authorization": f"Bearer {ACTIVE_TWITTER_TOKENS[0]}"
	}

	if start_time:
		start_time = datetime.fromtimestamp(start_time, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
	else:
		start_time = '2025-03-03T00:00:00Z'
	print(f'start_time: {start_time}')

	params = {
		"max_results": batch_size,
		"tweet.fields": "author_id,created_at,text,public_metrics,referenced_tweets,conversation_id,in_reply_to_user_id",
		"expansions": "author_id,referenced_tweets.id.author_id,referenced_tweets.id",
		"user.fields": "username,public_metrics,profile_image_url"
	}

	tweets = []
	includes = []
	tweets_includes = []
	next_token = None

	it = 0
	while True and it <= 100:
		print(it)
		it += 1
		if next_token:
			params["pagination_token"] = next_token

		response = requests.get(url, headers=headers, params=params)
		print(response.status_code, len(tweets))

		if response.status_code == 200:
			data = response.json()
			if "data" in data:
				tweets.extend(data["data"])
				includes.extend(data["includes"]['users'])
				tweets_includes.extend(data["includes"]['tweets'])

			next_token = data.get("meta", {}).get("next_token")
			if not next_token:
				break
		else:
			print(f"Error: {response.status_code} - {response.text}")
			break

		tweets_df = pd.DataFrame(tweets)
		mn = tweets_df.created_at.min()
		print(mn)
		if mn < start_time:
			break
		tweets_df['date'] = pd.to_datetime(tweets_df.created_at).apply(lambda x: x.date())
		print(tweets_df.groupby('date').size().sort_values(ascending=False))
	return tweets, includes, tweets_includes


def get_user_tweets(user_id, bearer_token = None, sleep_time = 90):
	print(f'Getting tweets for user {user_id}...')
	url = f"https://api.twitter.com/2/users/{user_id}/tweets"

	if bearer_token is None:
		bearer_token = os.getenv('ACTIVE_TWITTER_TOKENS[0]')
	print(f'bearer_token = {bearer_token}')

	headers = {
		"Authorization": f"Bearer {bearer_token}"
	}

	params = {
		"max_results": 100,
		"tweet.fields": "author_id,created_at,text,public_metrics,referenced_tweets,conversation_id,in_reply_to_user_id",
		"start_time": "2025-02-01T00:00:00Z",
		"expansions": "referenced_tweets.id.author_id,referenced_tweets.id",
		"exclude": "replies"
	}

	tweets = []
	includes = []
	tweets_includes = []
	next_token = None
	iterations = 0

	while iterations < 2:
		if next_token:
			params["pagination_token"] = next_token

		response = requests.get(url, headers=headers, params=params)

		if response.status_code == 200:
			data = response.json()
			if "data" in data:
				tweets.extend(data["data"])
				includes.extend(data["includes"]['users'])
				tweets_includes.extend(data["includes"]['tweets'])

			next_token = data.get("meta", {}).get("next_token")
			if next_token:
				print('Has more...')
				if iterations < 2:
					time.sleep(sleep_time)
			else:
				print('No more...')
			if not next_token:
				break

		else:
			print(f"Error: {response.status_code} - {response.text}")
			break

		iterations += 1

	return tweets, includes, tweets_includes

def get_tweet(tweet_id):
	url = f"https://api.twitter.com/2/tweets/{tweet_id}"

	headers = {
		"Authorization": f"Bearer {ACTIVE_TWITTER_TOKENS[0]}"
	}

	params = {
		"tweet.fields": "text,entities,attachments"
		, "expansions": "referenced_tweets.id"
	}

	response = requests.get(url, headers=headers, params=params)
	response.json()


	url = f"https://api.twitter.com/2/articles/{1899823082900259017}"
	url = 'https://x.com/metaproph3t/article/1899823082900259017'

	# You'll need to authenticate with your API bearer token
	headers = {
		"Authorization": "Bearer YOUR_BEARER_TOKEN"
	}

	response = requests.get(url, headers=headers)
	print(response.json())
	response = requests.get(url, headers=headers)
	print(response.json())
	r.json()


	if response.status_code == 200:
		return response.json()
	else:
		print(f"Error: {response.status_code} - {response.text}")


def get_tweets(tweet_ids):
	url = f"https://api.twitter.com/2/tweets"

	headers = {
		"Authorization": f"Bearer {ACTIVE_TWITTER_TOKENS[0]}"
	}

	params = {
		"tweet.fields": "text,entities,attachments,author_id"
		, "expansions": "referenced_tweets.id"
	}

	data = []
	for i in range(0, len(tweet_ids), 100):
		params["ids"] = ",".join([str(x) for x in tweet_ids[i:i+100]])
		response = requests.get(url, headers=headers, params=params)
		data.extend(response.json()['data'])
	tweets_df = pd.DataFrame(data)
	tweets_df.article
	articles = tweets_df[tweets_df.article.notnull()]
	for i, row in articles.iterrows():
		print(i)
		print(row['article']['title'])
		article_id = row['id']
		url = f'https://x.com/metaproph3t/article/{article_id}'
		driver.get(url)
		time.sleep(10)
		html = driver.page_source
		soup = BeautifulSoup(html, 'html.parser')
		divs = soup.find_all('div', {'data-testid': 'longformRichTextComponent'})
		len(divs)
		text = row['article']['title'] + ' - ' + divs[0].text
		text = text.replace("'", "''")  # Escape single quotes for SQL
		text = text.replace("\\", "\\\\")  # Escape backslashes
		query = "update tweets set text = '" + text + "' where id = " + article_id
		pg_execute_query(query)


	response = requests.get(url, headers=headers)
	print(response.json())
	response = requests.get(url, headers=headers)
	print(response.json())
	r.json()


	if response.status_code == 200:
		return response.json()
	else:
		print(f"Error: {response.status_code} - {response.text}")

