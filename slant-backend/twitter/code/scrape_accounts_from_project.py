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
from utils.db import pg_load_data
def load_articles():


	url = f"https://x.com/metaproph3t/article/1899823082900259017"
	driver.get(url)



	soup = BeautifulSoup(driver.page_source, "html.parser")

	# Scroll down a few times to load more results
	body = driver.find_element(By.TAG_NAME, "body")

	divs = soup.find_all('div', class_='DraftEditor-root')
	divs = soup.find_all('div', {'data-testid': 'longformRichTextComponent'})
	len(divs)

	text = divs[0].text

	query = "update tweets set text = '" + text + "' where id = '1899823082900259017'"
	execute_query(query)

	query = "select distinct conversation_id, text from tweets where id = conversation_id and text like 'https://t.co/%' "
	df = pg_load_data(query)

	tweet_ids = df.conversation_id.unique()


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

