with t0 as (
    select c.value:author_id::string as author_id
    , c.value:username::string as username
    , count(distinct c.value:id::string) as n_tweets
    from crosschain.bronze.data_science_uploads u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'trending-tweets%'
        and c.value:ecosystem::string = 'solana'
    group by 1,2
    order by 3 desc
)
select t0.*
, u.account_type
, u.ecosystems[0]::string as ecosystem
from t0
left join crosschain.bronze.twitter_accounts u
    on t0.author_id = u.twitter_id
order by n_tweets desc


with score as (
    select twitter_id
    , sum(score) as acct_score
    from bi_analytics.silver.user_community_scores_monthly
    where month >= '2024-11'
    group by 1
)
select distinct concat('t-', twitter_id) as twitter_id
, twitter_handle
, account_type
, n_followers
, lower(ecosystems[0]::string) as ecosystem
, coalesce(acct_score, 0) as score
, 1 as include
from crosschain.bronze.twitter_accounts a
left join score s
    on a.twitter_id = s.twitter_id
qualify row_number() over (partition by twitter_id order by n_followers desc) = 1
order by score, n_followers desc