from utils.utils import log
from utils.db import pg_load_data, load_tweets_for_pc, clean_tweets_for_pc
import pandas as pd
import time
from news.generate_news import generate_news

def news_finder() -> pd.DataFrame:
    print('\n')
    print('='*20)
    print('\n')
    print('news_finder starting...')
    # print(f'params: {params}')
    # Ensure params is a dictionary
    current_hour = int(time.localtime().tm_hour)
    days = [1, 7, 30] if current_hour == 5 else [1]
    days = [7, 30]
    for n_days in days:
        start_timestamp = int(time.time()) - n_days * 24 * 60 * 60
        mult = 'greatest(1, (5/(hours_ago+0.6)) + 0.87)' if n_days == 1 else '1'
        mult = 'greatest(1, (5/(hours_ago+0.6)) + 0.87)'
        for hours_ago in range(1, 24):
            print(f'{hours_ago}: {(5/(hours_ago+0.6)) + 0.87}')
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
            limit 3000
        """
        news_df = pg_load_data(query)
        news_df[news_df.conversation_id == 1927846852822495361]
        log(news_df)

        all_tweets = load_tweets_for_pc(start_timestamp, news_df.conversation_id.tolist())
        clean_tweets = clean_tweets_for_pc(all_tweets)
        clean_tweets = pd.merge(news_df, clean_tweets[['conversation_id','text']], on='conversation_id', how='left')
        clean_tweets['text'] = clean_tweets.text_y.fillna(clean_tweets.text_x)
        clean_tweets[['text_x','text_y','conversation_id']]
        clean_tweets['ind'] = range(len(clean_tweets))
        generate_news(clean_tweets, n_days)
    return True