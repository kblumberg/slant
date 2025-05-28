from utils.db import pg_load_data


def load_news():
    query = """
        with t0 as (
            select n_days
            , max(updated_at) as updated_at
            from news
            group by 1
        )
        , t1 as (
            select n.*
            , n.original_tweets->>0 as original_tweet
            , tu.username
            , tu.profile_image_url        
            from news n
            left join tweets t
                on (n.original_tweets->>0)::bigint = t.id::bigint
            left join twitter_users tu
                on t.author_id = tu.id
            join t0
                on n.n_days = t0.n_days
                and n.updated_at = t0.updated_at
        )
        select *
        from t1
        order by n_days, score desc
        
        
    """
    return pg_load_data(query)