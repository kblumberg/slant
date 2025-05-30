from utils.db import pg_load_data
import time
from constants.keys import ACTIVE_TWITTER_TOKENS
import requests
import pandas as pd

def find_community_members(n_days: int):
    start_timestamp = int(time.time()) - 60 * 24 * 60 * 60
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
        limit 300
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
    
    query = f"""
        select distinct id
        from twitter_kols
    """
    kols_df = pg_load_data(query)

    g = retweeters_df.groupby(['id','username']).count().reset_index().rename(columns={'conversation_id':'n_tweets'}).sort_values('n_tweets', ascending=False)
    g['exists'] = g['id'].isin(kols_df['id'].astype(str)).astype(int)
    g = g[(g.exists == 0) & (g.n_tweets >= 2)].reset_index(drop=True)
    all_users_df = pd.DataFrame()
    for i in range(0, len(g), 100):
        cur = g.iloc[i:i+100]
        # Get user details from Twitter API
        users_url = "https://api.twitter.com/2/users"
        headers = {
            "Authorization": f"Bearer {ACTIVE_TWITTER_TOKENS[0]}"
        }
        params = {
            "ids": ",".join(cur.id.astype(str)),
            "user.fields": "description,public_metrics"
        }
        try:
            response = requests.get(users_url, headers=headers, params=params)
            if response.status_code == 200:
                users_data = response.json().get('data', [])
                users_df = pd.DataFrame(users_data)
                # Extract follower count from public_metrics
                users_df['followers_count'] = users_df['public_metrics'].apply(lambda x: x.get('followers_count', 0))
                users_df = users_df[['id', 'description', 'followers_count']]
                # Merge with current dataframe
                cur = cur.merge(users_df, on='id', how='left')
                all_users_df = pd.concat([all_users_df, cur])
            else:
                print(f"Failed to get user details: {response.status_code}")
                print(f"Sleeping for 15 minutes until {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() + 60 * 15))}")
                time.sleep(60 * 15)
        except Exception as e:
            print(f"Error getting user details: {e}")
    all_users_df.to_csv('~/Downloads/all_users.csv', index=False)
    add_users = pd.read_csv('~/Downloads/add_users.csv')
    add_users = add_users[add_users.use == 1]
    add_users = pd.merge(add_users[['username']], all_users_df[['username','id','description','followers_count']], on='username', how='left')
    add_users.to_csv('~/Downloads/tmp-5.csv', index=False)

    add_users['exists'] = add_users['id'].isin(kols_df['id'].astype(str)).astype(int)
    add_users = add_users[add_users.exists == 0].reset_index(drop=True)
    add_users.to_csv('~/Downloads/add_users.csv', index=False)