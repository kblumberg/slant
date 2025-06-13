with states as (
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
    , state ->> 'agent_message_id'::string as agent_message_id
    , state ->> 'conversation_id'::string as conversation_id
    , state ->> 'user_message_id'::string as user_message_id
    , state ->> 'user_id'::string as user_id
    , state ->> 'highcharts_configs'::string as highcharts_configs
    from state_snapshots
    where state ->> 'conversation_id' = '{conversation_id}'
)
, messages as (
    select am.id
    , am.timestamp
    , am.message
    , s.highcharts_configs
    , s.flipside_sql_query_result
    from agent_messages am
    left join states s
        on am.id = s.agent_message_id
    where am.conversation_id = '{conversation_id}'
    union 
    select um.id
    , um.timestamp
    , um.message
    , null as highcharts_configs
    , null as flipside_sql_query_result
    from user_messages um
    where um.conversation_id = '{conversation_id}'
)
select *
from messages
order by timestamp