import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import re
from tavily import TavilyClient
from constants.keys import TAVILY_API_KEY
from ai.tools.utils.utils import get_web_search
# Config
BASE_URL = "https://docs.loopscale.com/introduction/overview"
ALLOWED_DOMAIN = "docs.loopscale.com"

# Set to keep track of visited URLs


# Headers to simulate a browser visit
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
}

def crawl(url, allowed_domain, data = [], visited = set()):
    if url in visited:
        return data

    try:
        print(f"Visiting: {url}")
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Failed to retrieve {url}")
            return

        visited.add(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all links on the page
        for link_tag in soup.find_all('a', href=True):
            href = link_tag['href']
            full_url = urljoin(url, href)
            full_url = re.split('#', full_url)[0]

            # Check if link is in allowed domain
            parsed_url = urlparse(full_url)
            if parsed_url.netloc == allowed_domain:
                if full_url not in visited:
                    data = crawl(full_url, allowed_domain, data, visited)

        # Optional: Sleep to be polite to the server
        time.sleep(0.5)
        return data + [{'text': soup.text, 'url': url}]

    except Exception as e:
        print(f"Error visiting {url}: {e}")


def web_crawl(search_query, project, project_id):
    tavily_client = TavilyClient(api_key=TAVILY_API_KEY)
    # search_query = 'solana blockchain loopscale documentation'
    web_search_results = tavily_client.search(search_query[:400], search_depth="basic", include_answer=False, include_images=False, max_results=1, include_raw_content=False)
    base_url = web_search_results['results'][0]['url']
    allowed_domain = base_url.split('/')[2]
    web_crawl_results = crawl(base_url, allowed_domain)
    web_crawl_results = [
        {
            'base_url': base_url,
            'allowed_domain': allowed_domain,
            'search_query': search_query,
            'project': project,
            'project_id': project_id,
            'url': result['url'],
            'text': result['text'],
        }
        for result in web_crawl_results
    ]
    log(f'web_crawl search_query: {search_query}')
    log(f'web_crawl base_url: {base_url}')
    log(f'web_crawl allowed_domain: {allowed_domain}')
    log(f'web_crawl results: {len(web_crawl_results)}')
    return web_crawl_results



# web_searches
# timestamp
# project
# project_id
# user_message_id
# search_query
# url
# text