from constants.keys import SLACK_TOKEN
from utils.db import pg_load_data, upload_tweet_data, clean_tweets_for_pc, pc_upload_data, load_tweets_for_pc
from utils.twitter import get_list_tweets
from slack_sdk import WebClient
from constants.constant import SLACK_CHANNEL_ID, KELLEN_SLACK_ID
from constants.db import TWEETS_RAG_COLS
from datetime import datetime
import math
import time

def update_tweets():
    client = WebClient(token=SLACK_TOKEN)

    try:
        query = 'select max(created_at) as mx, min(created_at) as mn, max(id) as mx_id, min(id) as mn_id from tweets'
        df = pg_load_data(query)
        created_at = df['mx'].max()
        minutes_ago = 60 * 3
        minutes_ago = math.ceil((int(datetime.now().timestamp()) - created_at) / 60)
        needed_tweets = int(minutes_ago * 600 / 60)
        batch_size = min(100, max(10, int(needed_tweets / 15)))
        
        tweets, includes, tweets_includes = get_list_tweets(start_time=created_at, batch_size=batch_size)
        upload_tweet_data(tweets, includes, tweets_includes)

        df = load_tweets_for_pc(created_at)
        df = clean_tweets_for_pc(df)
        pc_upload_data(df, 'text', TWEETS_RAG_COLS, batch_size=100, index_name='slant', namespace='tweets')
        client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=f"Updated {len(tweets)} tweets")
        return len(df)
        
    except Exception as e:
        print(f'error: {e}')
        # Send Slack DM about the error
        client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=f"<@{KELLEN_SLACK_ID}> Error in update_tweets: {str(e)}")
    return 0

def manually_update_tweets(tweet_ids):
    query = f"select * from tweets where id in ({', '.join(tweet_ids)})"
    df = pg_load_data(query)
    df = clean_tweets_for_pc(df)
    pc_upload_data(df, 'text', TWEETS_RAG_COLS, batch_size=100, index_name='slant', namespace='tweets')
    return len(df)
