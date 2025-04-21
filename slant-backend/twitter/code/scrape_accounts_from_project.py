import os
import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from utils.db import pg_load_data, pg_execute_query, clean_tweets_for_pc, pc_upload_data
from constants.keys import ACTIVE_TWITTER_TOKENS
from constants.db import TWEETS_RAG_COLS
import requests

def load_articles():

	options = Options()

	un = os.getenv('TWITTER_UN')
	pw = os.getenv('TWITTER_PW')

	driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
	driver.get('https://x.com/i/flow/login')
	time.sleep(5)
	username_input = driver.find_element(By.CSS_SELECTOR, 'input[autocomplete="username"]')
	username_input.send_keys(un)
	time.sleep(1)
	username_input.send_keys(Keys.ENTER)

	time.sleep(5)
	password_input = driver.find_element(By.CSS_SELECTOR, 'input[autocomplete="current-password"]')
	password_input.send_keys(pw)
	time.sleep(1)
	password_input.send_keys(Keys.ENTER)
	time.sleep(5)

	query = "select distinct conversation_id, text from tweets where id = conversation_id and text like 'https://t.co/%' and created_at > 1742472000 "
	df = pg_load_data(query)

	tweet_ids = df.conversation_id.unique()
	len(tweet_ids)

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
	

	tweet_ids = "', '".join(articles.id.tolist())
	query = f"select t.* , coalesce(ta.username, ta.handle, 'None') as name, coalesce(ta.handle, 'None') as username from tweets t left join twitter_accounts ta on t.author_id = ta.id where t.id in ('{tweet_ids}')"
	df = pg_load_data(query)
	df['id'] = df['id'].astype(str)
	df = clean_tweets_for_pc(df)
	df['text'] = df['text'].apply(lambda x: x[:35000])
	pc_upload_data(df, 'text', TWEETS_RAG_COLS, batch_size=100, index_name='slant', namespace='tweets')
	return len(df)

def get_accounts_from_projects():
	accounts = pd.read_csv('~/Downloads/twitter-accounts.csv')
	accounts = accounts[accounts.INCLUDE == 1]
	projects = accounts[accounts.ACCOUNT_TYPE == 'project']

	options = Options()

	un = os.getenv('TWITTER_UN')
	pw = os.getenv('TWITTER_PW')

	accounts = pd.read_csv('~/Downloads/projects-accounts.csv')
	existing = accounts.project.unique()
	print(f'len(existing): {len(existing)}')
	project = 'MagicEden'
	data = [list(x) for x in accounts.values]
	tot = len(projects.TWITTER_HANDLE)
	for i, project in enumerate(projects.TWITTER_HANDLE):
		try:
			if i < 143 or project in ['blockassetco', 'magFOMO']:
				continue
			print('#', i, '/', tot, ': ', project)
			if project in existing:
				continue
			if i % 12 == 11:
				driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
				driver.get('https://x.com/i/flow/login')
				time.sleep(5)
				username_input = driver.find_element(By.CSS_SELECTOR, 'input[autocomplete="username"]')
				username_input.send_keys(un)
				time.sleep(1)
				username_input.send_keys(Keys.ENTER)

				time.sleep(5)
				password_input = driver.find_element(By.CSS_SELECTOR, 'input[autocomplete="current-password"]')
				password_input.send_keys(pw)
				time.sleep(1)
				password_input.send_keys(Keys.ENTER)
				time.sleep(5)

			search_url = f"https://x.com/search?q=%40{project}&src=typed_query&f=user"
			driver.get(search_url)

			time.sleep(30)  # Wait for page to load

			soup = BeautifulSoup(driver.page_source, "html.parser")

			# Scroll down a few times to load more results
			body = driver.find_element(By.TAG_NAME, "body")

			for i in range(15):
				if i > 0:
					body.send_keys(Keys.PAGE_DOWN)
					time.sleep(2)
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
					data += [[ project, username, handle, bio ]]
			cur = pd.DataFrame(data, columns=['project', 'username', 'handle', 'bio']).drop_duplicates(subset=['handle'])
			# cur[cur.handle == 'rexzh0u']
			print(len(cur))
			cur.to_csv(f'~/Downloads/projects-accounts.csv', index=False)
		except Exception as e:
			print(f"Error: {e}")

