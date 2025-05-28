with conversations as (
    select '338932e6-80d3-41d3-98de-b36fefb3b534' as conversation_id
)
, t0 as (
    select message
    , 'user' as role
    , null::JSONB as state
    , timestamp
    , id::uuid as id
    from user_messages um
    join conversations c on um.conversation_id = c.conversation_id
)
, t1 as (
    select ss.state->>'response'::TEXT as message
    , 'system' as role
    , ss.state
    , ss.timestamp
    , ss.id::uuid as id
    from state_snapshots ss
    join t0 on t0.id::uuid = ss.user_message_id::uuid
)
, t2 as (
    select *
    from t0
    union
    select *
    from t1
)
select *
from t2
order by timestamp asc
limit 10
