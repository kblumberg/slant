from utils.utils import log
from utils.db import pg_load_data
from classes.GraphState import GraphState

def news_finder(state: GraphState) -> GraphState:
    # refined_query = prompt_refiner(state, 'Search a RAG database of projects.')
    # state = {
    #     'start_timestamp': 1742930531
    # }
    print('\n')
    print('='*20)
    print('\n')
    print('news_finder starting...')
    # print(f'params: {params}')
    # Ensure params is a dictionary
    query = f"""
        with t0 as (
            SELECT coalesce(rt.referenced_tweet_id, t.id) as conversation_id
            , coalesce(tur.name, tu.name) as name
            , coalesce(tur.username, tu.username) as username
            , max(t.retweet_count) as retweet_count
            , max(t.reply_count) as reply_count
            , max(t.like_count) as like_count
            , max(t.quote_count) as quote_count
            , max(t.impression_count) as impression_count
            , count(distinct t.author_id) as n_retweeters
            , min(t.created_at) as created_at
            FROM tweets t
            left join referenced_tweets rt
                on t.id = rt.id
                and rt.referenced_tweet_type in ('retweeted','quoted')
                and t.author_id != rt.author_id
            left join twitter_users tu
                on t.author_id = tu.id
            left join twitter_users tur
                on rt.author_id = tur.id
            where 
                t.created_at >= {state['start_timestamp']}
            group by 1, 2, 3
            order by n_retweeters desc, created_at desc
            limit 200
        )
        , t1 as (
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
        , n_retweeters * greatest(1, (5/(hours_ago+0.6)) + 0.87) as score
        , concat('https://x.com/', username, '/status/', conversation_id) as twitter_url
        from t2
        where text is not null
        order by score desc
        limit 25
    """
    news_df = pg_load_data(query)
    log(news_df)
    return {'news_df': news_df[['text','twitter_url']], 'completed_tools': ["NewsFinder"], 'upcoming_tools': ["RespondWithContext"]}
