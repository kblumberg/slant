import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def update_news():
    url = "https://slant-backend-production.up.railway.app/api/update_news"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            logging.info("Successfully updated news.")
        else:
            logging.error(f"Failed to update news. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Error while calling update_news: {e}")

if __name__ == "__main__":
    update_news()
