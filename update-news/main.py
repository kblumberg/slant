import requests
import logging
from dotenv import load_dotenv
import os

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def update_news():
    url = "https://slant-backend-production.up.railway.app/api/update_news"
    try:
        data = {
            'api_key': os.getenv('SLANT_API_KEY')
        }
        response = requests.post(url, json=data)
        if response.status_code == 200:
            logging.info("Successfully updated news.")
        else:
            logging.error(f"Failed to update news. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Error while calling update_news: {e}")

if __name__ == "__main__":
    update_news()
