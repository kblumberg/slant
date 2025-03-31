from utils.db import pg_load_data

def get_new_accounts():
    # FIND accounts that are being RT-d a lot
    query = """
        with t0 as (
            SELECT tu.id
            , tu.name
            , tur.id as original_id
            , tur.username as original_username
            , tur.name as original_name
            , count(distinct t.id) as tweet_count
            FROM tweets t
            join referenced_tweets rt
                on t.id = rt.id
                and rt.referenced_tweet_type in ('retweeted','quoted')
            join twitter_users tu
                on t.author_id = tu.id
            join twitter_users tur
                on rt.author_id = tur.id
            where t.author_id != tur.id
                and not tur.id in (select distinct tk.id from twitter_kols tk)
                and t.created_at > extract(epoch from (current_date - interval '7 days'))
            group by 1, 2, 3, 4, 5
        )
        , t1 as (
            select original_username
            , original_name
            , count(1) as score
            from t0
            where tweet_count > 1
            group by 1, 2
        )
        select *
        from t1
        order by score desc
    """
    tweets_df = pg_load_data(query)
    g = tweets_df.groupby(['original_username','original_name']).agg({'score':'sum'}).reset_index()
    g = g.sort_values('score', ascending=False)
    g.head()
    g.to_csv('~/Downloads/rts_df.csv', index=False)