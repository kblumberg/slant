with t0 as (
  select conversation_id, min(created_at) as first_tweet_at
  from tweets
  where impression_count > 0
  group by 1
)
, t1 as (
  select 
    substring(cast(date_trunc('day', to_timestamp(first_tweet_at)) as varchar), 1, 10) as tweet_date
    , count(1) as num_tweets
    , count(distinct date_trunc('hour', to_timestamp(first_tweet_at))) as n_hours
  from t0
  group by 1
)
select *
, round(num_tweets * 24 / n_hours) as tweets_per_day
, to_char(to_date(tweet_date, 'YYYY-MM-DD'), 'IW') as week_number
, to_char(to_date(tweet_date, 'YYYY-MM-DD'), 'D') as day_of_week
, sum(num_tweets) over (order by tweet_date rows between 6 preceding and current row) / 
  nullif(sum(n_hours) over (order by tweet_date rows between 6 preceding and current row), 0) * 24 as rolling_7d_tweets_per_day
, case to_char(to_date(tweet_date, 'YYYY-MM-DD'), 'D')
    when '1' then 'Sun'
    when '2' then 'Mon' 
    when '3' then 'Tue'
    when '4' then 'Wed'
    when '5' then 'Thu'
    when '6' then 'Fri'
    when '7' then 'Sat'
  end as day_name
from t1
order by 1 desc
