WITH t0 AS (
  SELECT *
  , (state ->> 'flipside_example_queries')::jsonb as flipside_example_queries
  FROM state_snapshots
  order by timestamp desc
  LIMIT 1
)
, t1 as (
  select *
  , jsonb_array_elements_text(flipside_example_queries) AS query_id
  from t0
)
, t2 as (
  select t1.*
  , q.*
  , um.message as user_message
  from t1
  left join flipside_queries q
      on q.id = t1.query_id
  left join user_messages um
      on um.id = t1.user_message_id
)
select user_message_id
, user_message
, timestamp
-- , datediff(minute, timestamp, current_timestamp) as minutes_ago
, text as query_text
, summary
, project_tags
, user_name
, updated_at
from t2
-- where query_text like '%DEkqHyPN7GMRJ5cArtQFAWefqbZb33Hyf6s5iCwjEonT%'
order by timestamp desc, user_message_id
