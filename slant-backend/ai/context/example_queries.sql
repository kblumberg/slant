-- load tweets from postgres
with t0 as (
    -- if it is a retweet, use the referenced_tweet values since they are the original tweet
    SELECT coalesce(rt.referenced_tweet_id, t.id) as id
    , coalesce(rt.referenced_tweet_id, t.conversation_id) as conversation_id
    , coalesce(tur.id, tu.id) as author_id
    , coalesce(tur.name, tu.name) as name
    , coalesce(tur.username, tu.username) as username
    , t.text
    , t.created_at
    , t.like_count
    , t.quote_count
    , t.reply_count
    -- if we don't have the original tweet (only have the retweet), then like_count, quote_count, reply_count, and impression_count are 0, but retweet_count will be valid
    , t.retweet_count
    , t.impression_count
    , rt.referenced_tweet_type
    , concat('https://x.com/', tu.username, '/status/', t.id) as tweet_url
    , length(t.text) as text_length
    , case when coalesce(t2.author_id, 0) = coalesce(rep.author_id, t2.author_id, 0) then 0 else 1 end as is_reply
    -- if there are multiple users retweeting the same tweet, use the original tweet if we have it
    , row_number() over (partition by coalesce(rt.referenced_tweet_id, t.id) order by case when rt.referenced_tweet_id is not null then 1 else 0 end, t.like_count desc, t.created_at desc) as rn
    FROM tweets t
    -- check to see if the tweet is a retweet
    left join referenced_tweets rt
        on t.id = rt.id
        and rt.referenced_tweet_type = 'retweeted'
    -- check to see if the tweet is a reply
    left join referenced_tweets rep
        on t.id = rep.id
        and rep.referenced_tweet_type = 'replied_to'
    left join tweets t2
        on t2.id = rep.id
    left join twitter_users tu
        on t.author_id = tu.id
    left join twitter_users tur
        on rt.author_id = tur.id
)
select *
from t0
where rn = 1
    and text_length >= 20
    and is_reply = 0


-- get top tweets by n_retweeters
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
        t.created_at > extract(epoch from (current_date - interval '1 days'))
    group by 1, 2, 3
    order by n_retweeters desc, created_at desc
    limit 100
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
    , least(24, (cur_timestamp - created_at) / (60 * 60.0)) as hours_ago
    from t1
)
select *
, n_retweeters * greatest(1, (5/(hours_ago+0.6)) + 0.87) as score
from t2
order by score desc