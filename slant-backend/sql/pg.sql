-- pg_dump --schema-only --no-owner --dbname=POSTGRES_ENGINE

SELECT t.*, rt.referenced_tweet_id, rt.referenced_tweet_type
, rt.author_id
, coalesce(tur.id, tu.id) as original_author_id
, coalesce(tur.name, tu.name) as original_author_name
, coalesce(tur.username, tu.username) as original_author_username
FROM tweets t
LEFT JOIN referenced_tweets rt
    ON t.id = rt.source_tweet_id
    AND rt.referenced_tweet_type = 'retweeted'
LEFT JOIN twitter_users tu ON t.author_id = tu.id
LEFT JOIN twitter_users tur ON rt.author_id = tur.id


select tk.*, p.name as project_name
from twitter_kols tk
left join projects p
    on tk.associated_project_id = p.id


with project_kols as (
SELECT 
    *
FROM projects p
join twitter_kols tk
    on p.id = tk.associated_project_id
where  tk.account_type = 'project'
)
select * from projects p
left join project_kols pk
    on p.id = pk.associated_project_id
where pk.associated_project_id is null


with t0 as (
    select id
    , username
    , count(1) as n
    from twitter_kols
    group by 1, 2
    having n > 1
)
select t.*
from t0
join twitter_kols t
    on t0.id = t.id
order by t.id


with t0 as (
    select associated_project_id
    , count(1) as n
    from twitter_kols
    group by 1
    having count(1) > 1
)
select t.*, p.*
from t0
join twitter_kols t
    on t0.associated_project_id = t.associated_project_id
join projects p
    on t0.associated_project_id = p.id
order by n desc, t.associated_project_id

delete from twitter_kols
where username = 'MadLads'



-- pg_dump --schema-only --no-owner --dbname=POSTGRES_ENGINE > schema.sql




with t0 as (
    SELECT coalesce(rt.referenced_tweet_id, t.id) as tweet_id
    , coalesce(rt.referenced_tweet_id, t.conversation_id) as conversation_id
    , coalesce(tur.id, tu.id) as author_id
    , coalesce(tur.name, tu.name) as author_name
    , coalesce(tur.username, tu.username) as author_username
    , t.text as tweet_text
    , t.created_at as tweet_created_at
    , t.like_count as tweet_like_count
    , t.quote_count as tweet_quote_count
    , t.reply_count as tweet_reply_count
    , t.retweet_count as tweet_retweet_count
    , rt.referenced_tweet_type
    , length(t.text) as text_length
    , row_number() over (partition by coalesce(rt.referenced_tweet_id, t.id) order by case when rt.referenced_tweet_id is not null then 1 else 0 end , t.created_at desc) as rn
    FROM tweets t
    left join referenced_tweets rt
        on t.id = rt.id
        and rt.referenced_tweet_type = 'retweeted'
    left join twitter_users tu
        on t.author_id = tu.id
    left join twitter_users tur
        on rt.author_id = tur.id
)
select *
from t0
where rn = 1
    and text_length >= 20
