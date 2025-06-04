with timeframe as (
    select 1748906329 as start_timestamp
    , 1748992729 as end_timestamp
)
, tot as (
    select count(distinct t.author_id) as n_authors
    FROM tweets t
    left join timeframe tf
        on t.created_at >= tf.start_timestamp - (30 * 24 * 60 * 60)
        and t.created_at <= tf.end_timestamp
    where 
        not t.author_id in (1568628960929501184)
)
, t0 as (
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
        t.created_at >= 1748906329 - (30 * 24 * 60 * 60)
        and t.created_at <= 1748992729
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
, t3 as (
    select *
    , greatest(1, (5/(hours_ago+0.6)) + 0.87) as mult
    , concat('https://x.com/', username, '/status/', conversation_id) as twitter_url
    from t2
    join tot t
        on true
    where text is not null
)
select *
, 100 * (1 - power(1 / (n_retweeters * mult * 650 / n_authors), 0.6)) as score
from t3
order by score desc
limit 30