# import time
# import json
# import requests
# import pandas as pd
# from datetime import datetime
# from typing import List, Dict, Optional

# import os
# import pinecone
# import anthropic
# import pandas as pd
# from typing import List
# import snowflake.connector
# from flask import Flask
# from flask_cors import CORS
# from openai import OpenAI
# from constants.keys import OPENAI_API_KEY, PINECONE_API_KEY

# import psycopg2

# MAX_ATTEMPTS = 3

# app = Flask(__name__)

# CORS(app, resources={
#     r"/*": {
#         "origins": "http://localhost:5173",
#         "allow_credentials": True,
#         "methods": ["GET", "POST", "OPTIONS"],
#         "allow_headers": ["Origin", "Content-Type", "Accept"]
#     }
# })

# os.chdir("/Users/kellen/git/blaizer")

# from vector_store import VectorStore

# BASE_PATH = '/Users/kellen'

# client = anthropic.Anthropic(api_key=API_KEY)
# openai_client = OpenAI(api_key=OPENAI_API_KEY)

# pc = pinecone.Pinecone(api_key=PINECONE_API_KEY)


# # LLM_MODEL = 'o1-mini'
# LLM_MODEL = 'claude-3-5-sonnet-latest'
# LLM_CLIENT = client


# class DiscordScraper:
#     def __init__(self, auth_token: str):
#         """
#         Initialize the Discord scraper with authentication token
        
#         Args:
#             auth_token (str): Your Discord authentication token
#         """
#         # Remove quotes if present and clean the token
#         self.token = auth_token.strip('"').strip("'")
#         self.base_url = "https://discord.com/api/v9"
#         self.headers = {
#             "Authorization": f"Bot {self.token}" if self.token.startswith('Bot ') else self.token,
#             "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
#             "Content-Type": "application/json"
#         }

#     def validate_token(self) -> bool:
#         """
#         Validate the Discord token by making a test request
        
#         Returns:
#             bool: True if token is valid, False otherwise
#         """
#         try:
#             response = requests.get(
#                 f"{self.base_url}/users/@me",
#                 headers=self.headers
#             )
#             return response.status_code == 200
#         except requests.exceptions.RequestException:
#             return False

#     def fetch_messages(
#         self, 
#         channel_id: str, 
#         limit: int = 100, 
#         before_id: Optional[str] = None
#     ) -> List[Dict]:
#         """
#         Fetch messages from a Discord channel
        
#         Args:
#             channel_id (str): ID of the channel to scrape
#             limit (int): Number of messages to fetch (max 100 per request)
#             before_id (str, optional): Message ID to fetch messages before
            
#         Returns:
#             List[Dict]: List of message objects
#         """
#         url = f"{self.base_url}/channels/{channel_id}/messages"
#         params = {"limit": min(limit, 100)}
        
#         if before_id:
#             params["before"] = before_id
            
#         try:
#             response = requests.get(url, headers=self.headers, params=params)
            
#             if response.status_code == 401:
#                 print("Error: Invalid token or unauthorized. Please check your Discord token.")
#                 print("Make sure you're using a valid user token, not a bot token.")
#                 return []
#             elif response.status_code == 403:
#                 print(f"Error: No permission to access channel {channel_id}")
#                 return []
#             elif response.status_code == 404:
#                 print(f"Error: Channel {channel_id} not found")
#                 return []
            
#             response.raise_for_status()
#             return response.json()
            
#         except requests.exceptions.RequestException as e:
#             print(f"Error fetching messages: {e}")
#             if hasattr(e.response, 'text'):
#                 print(f"Discord API Response: {e.response.text}")
#             return []

#     def scrape_channel(
#         self, 
#         channel_id: str, 
#         max_messages: int = 1000,
#         save_file: Optional[str] = None
#     ) -> List[Dict]:
#         """
#         Scrape all messages from a channel up to max_messages
        
#         Args:
#             channel_id (str): ID of the channel to scrape
#             max_messages (int): Maximum number of messages to fetch
#             save_file (str, optional): File path to save the messages
            
#         Returns:
#             List[Dict]: List of all fetched messages
#         """
#         if not self.validate_token():
#             print("Error: Invalid Discord token. Please check your token and try again.")
#             return []

#         print(f"Starting to scrape channel {channel_id}...")
#         all_messages = []
#         last_message_id = None
        
#         while len(all_messages) < max_messages:
#             messages = self.fetch_messages(
#                 channel_id,
#                 limit=100,
#                 before_id=last_message_id
#             )
            
#             if not messages:
#                 break
                
#             all_messages.extend(messages)
#             last_message_id = messages[-1]["id"]
            
#             print(f"Fetched {len(all_messages)} messages...")
            
#             # Respect rate limits
#             time.sleep(1)
            
#         # Trim to max_messages if needed
#         all_messages = all_messages[:max_messages]
        
#         if save_file and all_messages:
#             self.save_messages(all_messages, save_file)
            
#         return all_messages

#     def save_messages(self, messages: List[Dict], filename: str) -> None:
#         """
#         Save messages to a JSON file
        
#         Args:
#             messages (List[Dict]): List of message objects
#             filename (str): Path to save the file
#         """
#         processed_messages = []
        
#         for msg in messages:
#             processed_msg = {
#                 "id": msg["id"],
#                 "content": msg["content"],
#                 "author": {
#                     "id": msg["author"]["id"],
#                     "username": msg["author"]["username"],
#                     "discriminator": msg.get("author", {}).get("discriminator", "0")
#                 },
#                 "timestamp": msg["timestamp"],
#                 "attachments": [att["url"] for att in msg["attachments"]],
#                 "embeds": msg["embeds"]
#             }
#             processed_messages.append(processed_msg)

#         with open(filename, 'w', encoding='utf-8') as f:
#             json.dump(
#                 {
#                     "messages": processed_messages,
#                     "total_messages": len(processed_messages),
#                     "scrape_time": datetime.now().isoformat()
#                 },
#                 f,
#                 indent=2,
#                 ensure_ascii=False
#             )

# def main():
#     # Example usage
#     TOKEN = ""  # Use your actual Discord token
#     CHANNEL_ID = ""
    
#     scraper = DiscordScraper(TOKEN)
    
#     # Validate token before proceeding
#     if not scraper.validate_token():
#         print("Invalid token. Please check your Discord token and try again.")
#         return
        
#     messages = scraper.scrape_channel(
#         CHANNEL_ID,
#         max_messages=500,
#         save_file="discord_messages.json"
#     )
#     len(messages[-1]['reactions'])
#     cur = pd.DataFrame(messages)
#     max([ y['count'] for y in cur.reactions[0]])
#     cur['n_reactions'] = cur.reactions.apply(lambda x: max( [y['count'] for y in x]) if x== x and len(x) > 0 else 0)
#     cur['author_id'] = cur.author.apply(lambda x: x['id'])
#     cur['author_username'] = cur.author.apply(lambda x: x['username'])
#     cur['author_global_name'] = cur.author.apply(lambda x: x['global_name'])
#     cur[['type','content','timestamp','id','channel_id','author_id','author_username','author_global_name','n_reactions']]

#     conn = psycopg2.connect(
#         host='database-1-instance-1.cqm10arvzr6g.us-east-1.rds.amazonaws.com',
#         database='discord',
#         user='postgres',
#         password='',
#         port='5432'
#     )

#     # Connect to PostgreSQL database
#     conn = snowflake.connector.connect(
#         user='kellen',
#         password='',
#         account='myaccount',
#         warehouse='COMPUTE_WH',
#         database='DISCORD',
#         schema='PUBLIC'
#     )
    
#     # Create cursor
#     cur = conn.cursor()
    
#     # Create table if it doesn't exist
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS discord_messages (
#             message_id VARCHAR PRIMARY KEY,
#             message_type INTEGER,
#             content TEXT,
#             timestamp TIMESTAMP,
#             channel_id VARCHAR,
#             author_id VARCHAR, 
#             author_username VARCHAR,
#             author_global_name VARCHAR,
#             reaction_count INTEGER
#         )
#     """)
    
#     # Insert data
#     for _, row in cur.iterrows():
#         cur.execute("""
#             INSERT INTO discord_messages (
#                 message_id, message_type, content, timestamp, channel_id,
#                 author_id, author_username, author_global_name, reaction_count
#             ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#             ON CONFLICT (message_id) DO UPDATE SET
#                 message_type = EXCLUDED.message_type,
#                 content = EXCLUDED.content,
#                 timestamp = EXCLUDED.timestamp,
#                 channel_id = EXCLUDED.channel_id,
#                 author_id = EXCLUDED.author_id,
#                 author_username = EXCLUDED.author_username,
#                 author_global_name = EXCLUDED.author_global_name,
#                 reaction_count = EXCLUDED.reaction_count
#         """, (
#             row['id'],
#             row['type'],
#             row['content'],
#             row['timestamp'],
#             row['channel_id'],
#             row['author_id'],
#             row['author_username'],
#             row['author_global_name'],
#             row['n_reactions']
#         ))
    
#     # Commit changes and close connection
#     conn.commit()
#     cur.close()
#     conn.close()
    
#     if messages:
#         print(f"Successfully scraped {len(messages)} messages")
#     else:
#         print("No messages were scraped. Please check the channel ID and permissions.")

# if __name__ == "__main__":
#     main()