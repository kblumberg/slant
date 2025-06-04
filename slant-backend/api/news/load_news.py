from utils.db import pg_load_data


def load_news():
    query = """
        with t0 as (
            SELECT
                n.*
                , n.original_tweets->>0 as original_tweet_0
                , (EXTRACT(EPOCH FROM now()) - n.timestamp) / (60 * 60 * 24) AS time_ago_d
                , row_number() over (partition by n.original_tweets->>0 order by n.updated_at desc) as rn
                , tu.username
                , tu.profile_image_url     
            FROM
                news n
            left join tweets t
                on (n.original_tweets->>0)::bigint = t.id::bigint
            left join twitter_users tu
                on t.author_id = tu.id
        )
        , t1 as (
            select *
            , concat('https://x.com/', username, '/status/', original_tweet_0) as twitter_url
            , power(0.75, time_ago_d) as score_decay
            , score * power(0.75, time_ago_d) as score_decayed
            from t0
            where rn = 1
        )
        select *
        from t1
        order by score_decayed desc
    """
    return pg_load_data(query)