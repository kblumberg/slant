import time
import requests
from utils.db import pg_load_data

url = 'https://stats-mainnet.magiceden.us/collection_stats/search/solana?limit=50&window=30d&sort=volume&direction=desc&filter=%7B%22qc%22:%7B%22isVerified%22:true,%22minOwnerCount%22:30,%22minTxns%22:5%7D%7D'
r = requests.get(url)
data = r.json()
twitter_accounts = []
for collection in data:
    time.sleep(1)
    url = f'https://api-mainnet.magiceden.us/collections/{collection["collectionId"]}?edge_cache=true'
    r = requests.get(url)
    details = r.json()
    if 'twitter' in details.keys():
        twitter_accounts.append(details['twitter'])

query = f"""
    select * from twitter_kols
"""
twitter_accounts_df = pg_load_data(query)
missing_accounts = set([ re.split('/', x)[-1].lower() for x in twitter_accounts]) - set([ x.lower() for x in twitter_accounts_df.username.tolist()])
print(missing_accounts)

for account in missing_accounts:
    url = f'https://api-mainnet.magiceden.us/collections/{account}?edge_cache=true'
    r = requests.get(url)
    details = r.json()
    print(details)
    break