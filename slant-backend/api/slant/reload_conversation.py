
from flask import jsonify
from utils.utils import log
from utils.db import pg_load_data
import markdown

def reload_conversation(conversation_id: str):
    log(f'reloading conversation {conversation_id}')

    query = f"""
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
            , state ->> 'response' as response
            , state ->> 'flipside_example_queries' as flipside_example_queries
            , state ->> 'agent_message_id' as agent_message_id
            , state ->> 'conversation_id' as conversation_id
            , state ->> 'user_message_id' as user_message_id
            , state ->> 'user_id' as user_id
            , state ->> 'highcharts_configs' as highcharts
            from state_snapshots
            where state ->> 'conversation_id' = '{conversation_id}'
        )
        , messages as (
            select am.id::text as id
            , am.timestamp
            , am.message as content
            , s.highcharts
            , s.flipside_sql_query
            , s.flipside_sql_query_result
            , 'bot' as sender
            from agent_messages am
            left join states s
                on am.id = (s.agent_message_id)::uuid
            where am.conversation_id = '{conversation_id}'
            union 
            select um.id
            , um.timestamp
            , um.message as content
            , null as highcharts
            , null as flipside_sql_query
            , null as flipside_sql_query_result
            , 'user' as sender
            from user_messages um
            where um.conversation_id = '{conversation_id}'
        )
        select *
        from messages
        order by timestamp
    """
    df = pg_load_data(query)
    df['content'] = df.apply(lambda x: markdown.markdown(x['content']) if x['sender'] == 'bot' else x['content'], axis=1)
    log(df)

    try:
        return jsonify({
            'messages': df.to_dict(orient='records')
            , 'code': 200
        })

    except Exception as e:
        print(f"Error generating presigned URL: {e}")
        return jsonify({'error': 'Could not generate URL'}), 500

