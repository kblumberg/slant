select *
, state ->> 'tweets_summary' as tweets_summary
, state ->> 'web_search_summary' as web_search_summary
, state ->> 'flipside_sql_query' as flipside_sql_query
, state ->> 'flipside_sql_query_result' as flipside_sql_query_result
, state ->> 'user_prompt' as response
, state ->> 'web_search_results' as web_search_results
, state ->> 'analyses' as analyses
, state ->> 'analysis_description' as analysis_description
, state ->> 'flipside_sql_query' as flipside_sql_query
, state ->> 'response' as response
, state ->> 'flipside_example_queries' as flipside_example_queries
, state ->> 'agent_message_id' as agent_message_id
, state ->> 'conversation_id' as conversation_id
, state ->> 'user_message_id' as user_message_id
, state ->> 'user_id' as user_id
from state_snapshots
order by timestamp desc
limit 1
