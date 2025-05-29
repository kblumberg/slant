from utils.db import pg_load_data
import time
from constants.keys import ACTIVE_TWITTER_TOKENS
import requests
import pandas as pd

def find_community_members(n_days: int):
    start_timestamp = int(time.time()) - 50 * 24 * 60 * 60
    query = f"""
        with t0 as (
            SELECT coalesce(rt.referenced_tweet_id, t.id) as conversation_id
            , coalesce(tur.name, tu.name) as name
            , coalesce(tur.username, tu.username) as username
            , concat('https://x.com/', coalesce(tur.username, tu.username), '/status/', coalesce(rt.referenced_tweet_id, t.id)) as twitter_url
            , coalesce(tk.score, 0) as kol_score
            , coalesce(tkr.score, 0) as kol_score_retweeter
            , max(t.retweet_count) as retweet_count
            , max(t.reply_count) as reply_count
            , max(t.like_count) as like_count
            , max(t.quote_count) as quote_count
            , max(t.impression_count) as impression_count
            , count(distinct rt.id) as n_retweeters
            , min(t.created_at) as created_at
            FROM tweets t
            left join referenced_tweets rt
                on t.id = rt.referenced_tweet_id
                and rt.referenced_tweet_type in ('retweeted','quoted')
            left join twitter_users tu
                on t.author_id = tu.id
            left join twitter_users tur
                on rt.author_id = tur.id
            left join twitter_kols tk
                on t.author_id = tk.id
            left join twitter_kols tkr
                on rt.author_id = tkr.id
            where 
                t.created_at >= {start_timestamp}
                and not t.author_id in (1568628960929501184)
            group by 1, 2, 3, 4, 5, 6
            order by n_retweeters desc, impression_count desc
            limit 200
        )
        , t1 as (
            -- get the text of the tweet
            select distinct coalesce(t2.text, t.text) as text
            , t0.*
            , EXTRACT(EPOCH FROM NOW())::INT as cur_timestamp
            from t0
            left join referenced_tweets rt
                on rt.referenced_tweet_id = t0.conversation_id
                and rt.referenced_tweet_type in ('retweeted')
            left join tweets t
                on t.id = rt.id
            left join tweets t2
                on t2.id = t0.conversation_id
        )
        , t2 as (
            select *
            , (cur_timestamp - created_at) / (60 * 60.0) as hours_ago
            from t1
        )
        select *
        , n_retweeters * {mult} as score
        , concat('https://x.com/', username, '/status/', conversation_id) as twitter_url
        from t2
        where text is not null
        order by score desc
        limit 100
    """
    news_df = pg_load_data(query)
    retweeters_df = pd.DataFrame()
    for row in news_df.itertuples():
        print(row.twitter_url)
        if row.conversation_id in retweeters_df.conversation_id.unique():
            print('already done')
            continue
        # Get retweeters for this tweet
        try:
            # time.sleep(60 * 3)
            retweeters_url = f"https://api.twitter.com/2/tweets/{row.conversation_id}/retweeted_by"
            headers = {
                "Authorization": f"Bearer {ACTIVE_TWITTER_TOKENS[0]}",
            }
            response = requests.get(retweeters_url, headers=headers)
            if response.status_code == 200:
                retweeters = response.json().get('data', [])
                cur = pd.DataFrame(retweeters)
                cur['conversation_id'] = row.conversation_id
                retweeters_df = pd.concat([retweeters_df, cur])
                g = retweeters_df.groupby(['id','username']).count().rename(columns={'conversation_id':'n_tweets'}).sort_values('n_tweets', ascending=False)
                g.to_csv('~/Downloads/retweeters.csv', index=True)
                print(g)
            else:
                print(f"Failed to get retweeters: {response.status_code}")
                print(f"Sleeping for 15 minutes until {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + 60 * 15))}")
                time.sleep(60 * 15)
        except Exception as e:
            print(f"Error getting retweeters: {e}")