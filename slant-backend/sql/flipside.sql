
with t0 as (
    select block_timestamp::date as date
    , date_trunc('week', block_timestamp)::date as week
    , sum(coalesce(p.close, 0) * s.swap_from_amount) as swap_amount_usd
    -- select *
    from solana.defi.fact_swaps s
    left join solana.core.ez_token_prices_hourly p
        on date_trunc('hour', p.recorded_hour) = date_trunc('hour', s.block_timestamp)
        and p.token_address = s.swap_from_mint
    where week >= CURRENT_DATE - 180
        and s.succeeded = TRUE
        and date < CURRENT_DATE
    group by 1
)
select week
, COUNT(distinct date) as n_days
, avg(swap_amount_usd) as swap_amount_usd
from t0
group by 1
order by 3 desc


with dex_swaps as (
    select swapper as address
    , sum(case
        when swap_from_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' then -swap_from_amount
        when swap_to_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' then swap_from_amount
        else 0 end
    ) as net_swap_amout
    , sum(case when swap_from_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' then swap_from_amount else 0 end) as swap_from_amount
    , sum(case when swap_to_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' then swap_to_amount else 0 end) as swap_to_amount
    , sum(case when swap_to_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' then 1 else 0 end) as n_swaps_to
    from solana.defi.fact_swaps
    where block_timestamp >= '2022-12-24'
        and (
            swap_from_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
            or swap_from_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
        )
    group by 1
), cex_transfers as (
    select case when lf.label_type = 'cex' then t.tx_to else t.tx_from end as address
    , sum(case when lf.label_type = 'cex' then amount else -amount end) as net_cex_transfer_amout
    from solana.core.fact_transfers t
    left join solana.core.dim_labels lf
        on lf.address = t.tx_from
    left join solana.core.dim_labels lt
        on lt.address = t.tx_to
    where block_timestamp >= '2022-12-24'
        and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
        and (
            coalesce(lf.label_type, '') = 'cex'
            or coalesce(lt.label_type, '') = 'cex'
        )
        and not (
            coalesce(lf.label_type, '') = 'cex'
            and coalesce(lt.label_type, '') = 'cex'
        )
    group by 1
), airdrops as (
    select tx_to as address
    , sum(amount) as airdrop_amount
    from solana.core.fact_transfers
    where block_timestamp >= '2022-12-24'
        and block_timestamp <= '2023-04-01'
        and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
        and tx_from in (
            '9AhKqLR67hwapvG8SA2JFXaCshXc9nALJjpKaHZrsbkw' -- original airdrop address
            , '6JZoszTBzkGsskbheswiS6z2LRGckyFY4SpEGiLZqA9p' -- new airdrop address
        )
        and amount <  40000000000 -- 40B
    group by 1
), direct_transfers0 as (
    select distinct tx_id
    from solana.core.fact_transactions t
    join airdrops a
        on a.address = t.signers[0]::string
    where t.block_timestamp >= '2022-12-24'
        and succeeded
        and ARRAY_SIZE(inner_instructions) = 0
        and ARRAY_SIZE(instructions) = 1
        and instructions[0]:parsed:info:source is not null
        and instructions[0]:parsed:info:destination is not null
        and left(instructions[0]:parsed:info:source, 5) <> '11111'
        and left(instructions[0]:parsed:info:destination, 5) <> '11111'
        and post_token_balances[0]:mint::string = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
),  direct_transfers as (
    select tx_from as source
    , tx_to as destination
    , sum(amount) as amount
    from solana.core.fact_transfers t
    join direct_transfers0 dt
        on dt.tx_id = t.tx_id
    where t.block_timestamp >= '2022-12-24'
    group by 1, 2
), n0 as (
    select destination
    , COUNT(1) as n_sources
    from direct_transfers
    group by 1
), id_map0 as (
    -- make sure we are checking for cases where a user transfers and then sells
    select a.*, d.destination, d.amount as t_amt
    , row_number() over (partition by d.destination order by amount desc) as rn
    from direct_transfers d
    join airdrops a
        on a.address = d.source
    left join airdrops ad
        on ad.address = d.destination
    join n0
        on n0.destination = d.destination
    where n_sources <= 5
        and ad.address is null
), id_map as (
    select d.address, d.destination, t_amt
    from id_map0 d
    where rn = 1
), sales as (
    select coalesce(d.address, c.address) as address
    , sum(
        coalesce(d.net_swap_amout, 0)
         + coalesce(c.net_cex_transfer_amout, 0)
    ) as net_swap_amout
    from dex_swaps d
    FULL OUTER join cex_transfers c
        on c.address = d.address
    group by 1
)
-- select a.address
-- , i.destination
-- , i.t_amt
-- , s.net_swap_amout
-- , (
--     case
--         when net_swap_amout is null then 0
--         when net_swap_amout < 0 
--             and s.address <> a.address
--             and net_swap_amout < -t_amt then -t_amt
--         else net_swap_amout end
-- ) as net_swap_amout
-- from airdrops a
-- -- join to the id map generated by the transfers data
-- left join id_map i
--     on i.address = a.address
-- -- sum the sales of this account or 
-- left join sales s
--     on s.address = a.address
--     or s.address = i.destination
-- -- group by 1, 2

, t0 as (
    select a.address
    , a.airdrop_amount
    , sum(
        case
            when net_swap_amout is null then 0
            when net_swap_amout < 0 
                and s.address <> a.address
                and net_swap_amout < -t_amt then -t_amt
            else net_swap_amout end
    ) as net_swap_amout
    from airdrops a
    -- join to the id map generated by the transfers data
    left join id_map i
        on i.address = a.address
    -- sum the sales of this account or 
    left join sales s
        on s.address = a.address
        or s.address = i.destination
    group by 1, 2
)
select *
//ROUND(100 * avg(case when airdrop_amount + net_swap_amout >= 0.95 * airdrop_amount then 1 else 0 end), 2) as pct_held_95_pct
//, ROUND(sum(GREATEST(0, airdrop_amount + LEAST(0, net_swap_amout)))) as held_amt
//, ROUND(sum(airdrop_amount)) as tot_airdrop_amount
//, ROUND(100 * held_amt / tot_airdrop_amount, 2) as pct_held_agg
from t0




with p0 as (
    select token_address
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as hourly_price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= '2023-01-01'::date
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address
    , date_trunc('day', recorded_hour) as date
    , avg(close) as daily_price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= '2023-01-01'::date
        and is_imputed = FALSE
    group by 1, 2
), n as (
    select swap_to_mint as mint
    , COUNT(distinct swapper) as n_swappers
    from solana.defi.fact_swaps s
    where block_timestamp::date >= '2023-01-01'::date
        and succeeded = TRUE
    group by 1
), t0 as (
    select block_timestamp::date as date
    , date_trunc('week', block_timestamp)::date as week
    -- , tx_id
    , sum(
        case
        when coalesce(nf.n_swappers, 0) > coalesce(nt.n_swappers, 0) and coalesce(p0a.hourly_price, p1a.daily_price, 0) > 0 then coalesce(p0a.hourly_price, p1a.daily_price, 0) * s.swap_from_amount
        else coalesce(p0b.hourly_price, p1b.daily_price, 0) * s.swap_to_amount
        end
    ) as swap_amount_usd
    , sum(case when (
        case
        when coalesce(nf.n_swappers, 0) > coalesce(nt.n_swappers, 0)
            and coalesce(p0a.hourly_price, p1a.daily_price, 0) > 0
            then coalesce(p0a.hourly_price, p1a.daily_price, 0) * s.swap_from_amount
        else coalesce(p0b.hourly_price, p1b.daily_price, 0) * s.swap_to_amount
        end
    ) >= 10 then 1 else 0 end) as n_swaps_10p
    , COUNT(1) as n_swaps
    -- select s.*
    -- , p0a.hourly_price as from_hourly_price
    -- , p1a.daily_price as from_daily_price
    -- , p0b.hourly_price as to_hourly_price
    -- , p1b.daily_price as to_daily_price
    -- , coalesce(p0a.hourly_price, p1a.daily_price, 0) as from_price
    -- , coalesce(p0b.hourly_price, p1b.daily_price, 0) as to_price
    -- , nf.n_swappers as from_swappers
    -- , nt.n_swappers as to_swappers
    -- , case
    --     when coalesce(nf.n_swappers, 0) > coalesce(nt.n_swappers, 0) and coalesce(p0a.hourly_price, p1a.daily_price, 0) > 0 then coalesce(p0a.hourly_price, p1a.daily_price, 0) * s.swap_from_amount
    --     else coalesce(p0b.hourly_price, p1b.daily_price, 0) * s.swap_to_amount
    --     end as swap_amount_usd
    from solana.defi.fact_swaps s
    left join n nf
        on nf.mint = s.swap_from_mint
    left join n nt
        on nt.mint = s.swap_to_mint
    left join p0 p0a
        on p0a.hour = date_trunc('hour', s.block_timestamp)
        and p0a.token_address = s.swap_from_mint
    left join p1 p1a
        on p1a.date = date_trunc('day', s.block_timestamp)
        and p1a.token_address = s.swap_from_mint
    left join p0 p0b
        on p0b.hour = date_trunc('hour', s.block_timestamp)
        and p0b.token_address = s.swap_to_mint
    left join p1 p1b
        on p1b.date = date_trunc('day', s.block_timestamp)
        and p1b.token_address = s.swap_to_mint
    where s.block_timestamp >= '2023-01-01'
    -- where s.block_timestamp::date = '2023-02-02'::date
        and s.succeeded = TRUE
        and s.block_timestamp::date < CURRENT_DATE
        and s.swap_to_amount > 0
        and s.swap_from_amount > 0
    group by 1, 2
)
select *
from t0
order by swap_amount_usd ASC
LIMIT 100000


with chain0 as (
    select d.id as dashboard_id
    , case when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
        or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
        or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
        or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%'
        then 'Axelar' else INITCAP(t.name) end as chain
    , COUNT(distinct q.id) as n_queries
    from bi_analytics.velocity_app_prod.users u
    join bi_analytics.velocity_app_prod.dashboards d
        on d.created_by_id = u.id
    join bi_analytics.velocity_app_prod._dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    join bi_analytics.velocity_app_prod._queries_to_tags qtt
        on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and t.type = 'project'
    group by 1, 2
), chain as (
    select *
    , row_number() over (
        partition by dashboard_id
        order by
        case when chain in (
              'Avalanche'
            , 'Axelar'
            , 'Flow'
            , 'Near'
            , 'Sei'
            , 'Solana'
        ) then 1 else 2 end
        , n_queries desc
        , chain
    ) as rn
    from chain0
), t0 as (
  select
    -- 11/02: grab the team owner username if is a team
    coalesce(m.user_id, d.created_by_id) as user_id,
    d.profile_id as profile_id,
    coalesce(tu.username, u.username) as username,
    dr.currency,
    d.id as dashboard_id,
    p.type,
    coalesce(c.chain, 'Polygon') as ecosystem,
    row_number() over (
      order by
        dr.ranking_trending
    ) as current_rank
  from
    bi_analytics.content_rankings.dashboard_rankings dr
    join bi_analytics.velocity_app_prod.dashboards d on d.id = dr.dashboard_id
    left join bi_analytics.velocity_app_prod.profiles p
        on p.id = d.profile_id
    left join bi_analytics.velocity_app_prod.teams t
        on t.profile_id = p.id
    left join bi_analytics.velocity_app_prod.members m
        on t.id = m.team_id
        and m.role = 'owner'
    left join bi_analytics.velocity_app_prod.users tu
        -- kellen changed this line
        on tu.id = m.user_id
    join bi_analytics.velocity_app_prod.users u on u.id = d.created_by_id
    left join chain c
        on c.dashboard_id = dr.dashboard_id
        and c.rn = 1
  where
    coalesce(u.role, '') <> 'internal'
    and not u.username in (
      'Polaris_9R',
      'dsaber',
      'flipsidecrypto',
      'metricsdao',
      'drethereum',
      'Orion_9R',
      'sam',
      'forgash',
      'nftchance__',
      'danner',
      'charliemarketplace',
      'theericstone'
    )
)
select
  t0.user_id,
  t0.username,
  t0.dashboard_id,
  t0.profile_id,
  type,
  case when ecosystem in (
          'Solana'
        , 'Avalanche'
        , 'Axelar'
        , 'Flow'
        , 'Near'
        , 'Sei'
  ) then ecosystem else 'Polygon' end as ecosystem,
  case
        when ecosystem = 'Solana' then 'SOL'
        when ecosystem = 'Avalanche' then 'AVAX'
        when ecosystem = 'Axelar' then 'AXL'
        when ecosystem = 'Flow' then 'FLOW'
        when ecosystem = 'Near' then 'NEAR'
        when ecosystem = 'Sei' then 'SEI'
        else 'USDC' end as currency,
  case when current_rank <= 10 then 1.5 else 1 end as base_amount,
  case when ecosystem in (
    'Flow'
    ,'Near'
    ,'Sei'
  ) then 1.5 else 1 end as boost,
  base_amount * boost as amount
from
  t0
left join bi_analytics.silver.user_boost ub
    on ub.user_id = t0.user_id
where
  current_rank <= 30
order by
  7 desc





select *
from solana.defi.fact_swaps
where block_timestamp >= '2022-12-01'
    and swapper in ('9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj','AoNVE2rKCE2YNA44V7NQt8N73JdPM7b6acZ2vzSpyPyi')
    and swap_from_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'


select *
from solana.price.ez_token_prices_hourly p
where token_address = 'o1Mw5Y3n68o8TakZFuGKLZMGjm72qv4JeoZvGiCLEvK'
    and recorded_hour >= '2023-02-01'
    and recorded_hour < '2023-03-01'
order by close





with t0a as (
    select u.id as user_id
    , u.username
    , d.id as dashboard_id
    , coalesce(dr.ecosystem, '') as ecosystem
    , coalesce(dr.currency, '') as currency
    , d.title
    , coalesce(dr.dashboard_url, '') as dashboard_url
    , coalesce(dr.start_date_days_ago, 0) as start_date_days_ago
    , coalesce(dr.pct_twitter, 0) as pct_twitter
    , date_trunc('hour', dbt_updated_at) as hour
    , avg(hr.ranking_trending) as rk
    from bi_analytics.snapshots.hourly_dashboard_rankings hr
    left join bi_analytics.content_rankings.dashboard_rankings dr
        on dr.dashboard_id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where 1=1
        and dbt_updated_at >= '2023-10-24 15:00:00'
        and coalesce(u.role, '') <> 'internal'
        and not u.username in (
            'Polaris_9R','dsaber','flipsidecrypto','metricsdao','drethereum','Orion_9R','sam','forgash','nftchance__','danner'
        )
        and hr.ranking_trending <= 40
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
), imp as (
    select d.id as dashboard_id
    , sum(impression_count) as impression_count
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    group by 1
), t0 as (
    select user_id
    , username
    , dashboard_id
    , ecosystem
    , currency
    , title
    , dashboard_url
    , start_date_days_ago
    , pct_twitter
    , hour
    , row_number() over (partition by hour order by rk, dashboard_id) as rk
    from t0a
    -- group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
), t1 as (
    select user_id
    , username
    , t0.dashboard_id
    , title
    , dashboard_url
    , ecosystem
    , currency
    , start_date_days_ago
    , pct_twitter
    , coalesce(imp.impression_count, 0) as impression_count
    , MIN(rk) as max_ranking
    , sum(case when rk <= 8 then 1 else 0 end) as n_hours_in_top_8
    , sum(case
        when rk <= 8 and hour >= '2023-06-26 16:00:00' and hour <= '2023-07-17 16:00:00' and user_id in (
            ''
        ) then 1.5 
            when rk <= 8 then 1 else 0 end) as payment_amount
    , sum(41 - rk) as points
    from t0
    left join imp
        on imp.dashboard_id = t0.dashboard_id
    where rk <= 100
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
), t0d as (
    select distinct dashboard_id
    from t1
    where n_hours_in_top_8 > 0
), t1c as (
    select d.id as dashboard_id
    , case when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
        or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
        or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
        or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%'
        then 'Axelar' else INITCAP(t.name) end as chain
    , d.title
    , d.latest_slug
    , u.username
    , u.id as user_id
    , COUNT(distinct q.id) as n_queries
    from bi_analytics.velocity_app_prod.dashboards d
    join t0d
        on t0d.dashboard_id = d.id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    join bi_analytics.velocity_app_prod._dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    join bi_analytics.velocity_app_prod._queries_to_tags qtt
        on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and t.type = 'project'
    group by 1, 2, 3, 4, 5, 6
), t2c as (
    select *
    , case when chain in (
            'Avalanche'
            , 'Axelar'
            , 'Flow'
            , 'Near'
            , 'Sei'
            , 'Solana'
    ) then chain else null end as partner_chain
    , row_number() over (
        partition by dashboard_id
        order by
        n_queries desc
        , case when chain in (
            'Avalanche'
            , 'Axelar'
            , 'Flow'
            , 'Near'
            , 'Sei'
            , 'Solana'
        ) then 1 when chain = 'Ethereum' then 2 else 3 end
        , chain
    ) as rn
    , sum(n_queries) over (partition by dashboard_id) as tot_queries
    , n_queries / tot_queries as pct
    from t1c
), tc0 as (
    select t1.*
    , coalesce(t2c1.partner_chain, t2c2.partner_chain, t2c3.partner_chain, 'Ethereum') as chain
    from t1
    left join t2c t2c1
        on t2c1.dashboard_id = t1.dashboard_id
        and t2c1.rn = 1
    left join t2c t2c2
        on t2c2.dashboard_id = t1.dashboard_id
        and t2c2.rn = 2
    left join t2c t2c3
        on t2c3.dashboard_id = t1.dashboard_id
        and t2c3.rn = 1
    where n_hours_in_top_8 > 0
)
select *
from tc0


with avax as (
    select h.address
    , sum(t.amount_usd) as avax_amount_usd
    , MIN(DATEADD(SECOND, h.timestamp / 1000, '1970-01-01'))::date as date
    from bi_analytics.bronze.arprograms_hike h
    join avalanche.core.ez_avax_transfers t
        on t.tx_hash = h.txId
    where result = 'VERIFIED'
        and blockchain = 'Avalanche'
    group by 1
), tokens as (
    select h.address
    , sum(t.amount_usd) as token_amount_usd
    , MIN(DATEADD(SECOND, h.timestamp / 1000, '1970-01-01'))::date as date
    from bi_analytics.bronze.arprograms_hike h
    join avalanche.core.ez_token_transfers t
        on t.tx_hash = h.txId
    where result = 'VERIFIED'
        and blockchain = 'Avalanche'
    group by 1
)
select coalesce(a.address, t.address) as wallet
, coalesce(a.avax_amount_usd, 0) as avax_amount_usd
, coalesce(t.token_amount_usd, 0) as token_amount_usd
, coalesce(a.avax_amount_usd, 0) + coalesce(t.token_amount_usd, 0) as amount_usd
, LEAST(coalesce(a.date, t.date), coalesce(t.date, a.date)) as date
from avax a
FULL OUTER join tokens t
    on t.address = a.address



-- get all the copper burnt by day
with
  ffflip as (
    select
      sum(amount) as sol,
      'ffflip' as label
    from
      solana.core.fact_transfers
    where
      BLOCK_TIMESTAMP >= (CURRENT_DATE - interval '7 days')
      and BLOCK_TIMESTAMP <= CURRENT_DATE
        --   
      and TX_TO = 'Fz2tay9wmAhaz1GuEETorL55LBMECaT85DX9Tj5zqQKb'
      and AMOUNT >= 0
      and AMOUNT <= 0.3
      and mint = 'So11111111111111111111111111111111111111112'
  ),
  raffles as (
    select
      sum(amount) as sol,
      'raffles' as label
    from
      solana.core.fact_transfers
    where
      BLOCK_TIMESTAMP >= (CURRENT_DATE - interval '7 days')
      and BLOCK_TIMESTAMP < CURRENT_DATE
      and tx_to = 'GFDko6sYw96a3nUmPqNLZwq7YstQ1u1SNUaVhnoCPW7o'
      and MINT = 'So11111111111111111111111111111111111111112'
  ),
  royalties as (
    select
      sum(amount) as sol,
      'royalties' as label
    from
      solana.core.fact_transfers
    where
      BLOCK_TIMESTAMP >= (CURRENT_DATE - interval '7 days')
      and BLOCK_TIMESTAMP < CURRENT_DATE
      and tx_to in (
        '3vedckD9AnCNp7vEEnTUzVK6bWHEQgSpPRB7A5aS67kj',
        'MwW2RfWAfGrKmh9QAkDELdEV7Ue39FoP885dhuHmqr9',
        '3pMvTLUA9NzZQd4gi725p89mvND1wRNQM3C8XEv1hTdA'
      )
      and mint = 'So11111111111111111111111111111111111111112'
      and amount <= 5
  ),
  token_market as (
    select
      sum(amount) as sol,
      'token_market' as label
    from
      solana.core.fact_transfers
    where
      BLOCK_TIMESTAMP >= (CURRENT_DATE - interval '7 days')
      and BLOCK_TIMESTAMP < CURRENT_DATE
      and TX_TO = '2x3yujqB7LCMdCxV7fiZxPZStNy7RTYqWLSvnqtqjHR6'
      and amount >= 0
  ),
  tools as (
    select
      sum(amount) as sol,
      'tools' as label
    from
      solana.core.fact_transfers
    where
      tx_to = '98Ni7vVRR1tggtWWruPVcfFXHTH11bPbNryJZGkCGvaD'
      and BLOCK_TIMESTAMP >= (CURRENT_DATE - interval '7 days')
      and BLOCK_TIMESTAMP < CURRENT_DATE
      and amount <= 0.012
      and amount > 0
  ),
  citrus as (
    select
      sum(amount) as sol,
      'citrus' as label
    from
      solana.core.fact_transfers
    where
      BLOCK_TIMESTAMP >= (CURRENT_DATE - interval '7 days')
      and BLOCK_TIMESTAMP < CURRENT_DATE
      and TX_TO = '7e7qhwnJuLVDGBiLAGjB9FxfpizsPhQjoBxkNA5wVCCc'
      and MINT = 'So11111111111111111111111111111111111111112'
  ),
  total as (
    select
      'SOL' as token,
      sum(sol) as sol,
      label
    from
      (
        select
          *
        from
          citrus
        union all
        select
          *
        from
          tools
        union all
        select
          *
        from
          token_market
        union all
        select
          *
        from
          royalties
        union all
        select
          *
        from
          raffles
        union all
        select
          *
        from
          ffflip
      )
    group by
      label
  ),
  prices as (
    select
      symbol,
      close
    from
      solana.price.fact_token_prices_hourly
    where
      provider = 'coingecko'
      and id = 'solana'
    order by
      RECORDED_HOUR desc
    limit
      1
  )
select
  LABEL,
  CLOSE * SOL as VALUE
from
  total
  join prices on TOKEN = SYMBOL 




 -- forked from marqu / Solana Liquid Staking Pools - LST TVL @ https://flipsidecrypto.xyz/marqu/q/2T5l5faCLNJC/solana-liquid-staking-pools---lst-tvl
with lst_labels as (
  select
    *
  from
    (
      values
        (
          '6iQKfEyhr3bZMotVkW6beNZz5CPAkiwvgV2CTje9pVSS',
          'jitoSOL',
          'Jito'
        )
    ) as t(authority, symbol, label)
), stake_changes as (

      select
        distinct tx_id,
        stake_account,
        symbol,
        label
      from
        solana.gov.ez_staking_lp_actions actions
        inner join lst_labels on actions.stake_authority = lst_labels.authority
        or actions.withdraw_authority = lst_labels.authority
      where
        succeeded qualify row_number() over (
          partition by date_trunc('day', block_timestamp),
          stake_account
          order by
            actions.block_timestamp desc
        ) = 1
)
, all_actions as (
  select
    date_trunc('day', actions.block_timestamp) as date,
    tx_id,
    stake_account,
    iff(
      stake_authority = authority
      or withdraw_authority = authority,
      post_tx_staked_balance,
      0
    ) as post_tx_staked_balance,
    stake_active,
    event_type,
    stake_authority,
    withdraw_authority,
    symbol,
    label,
    (POST_TX_STAKED_BALANCE - PRE_TX_STAKED_BALANCE) / 1e9 as delta_sol,
    iff(
      POST_TX_STAKED_BALANCE - PRE_TX_STAKED_BALANCE > 0,
      (POST_TX_STAKED_BALANCE - PRE_TX_STAKED_BALANCE) / 1e9,
      0
    ) as deposits,
    iff(
      POST_TX_STAKED_BALANCE - PRE_TX_STAKED_BALANCE < 0,
      (POST_TX_STAKED_BALANCE - PRE_TX_STAKED_BALANCE) / 1e9,
      0
    ) as withdrawals
  from
    solana.gov.ez_staking_lp_actions actions
    inner join stake_changes using(tx_id, stake_account)
    left join lst_labels using(label) qualify row_number() over (
      partition by date,
      stake_account
      order by
        actions.block_timestamp desc,
        actions.index desc
    ) = 1
),
dates as (
  select
    distinct date_trunc('day', date_day) as date,
    label,
    stake_account
  from
    crosschain.core.dim_dates
    cross join (
      select
        distinct stake_account,
        label
      from
        all_actions
    )
    left join lst_labels using(label)
  where
    date_day between '2021-08-01'
    and current_date()
),
prices as (
  select
    date_trunc('day', recorded_hour) as date,
    avg(close) as price
  from
    solana.price.ez_token_prices_hourly
  where
    symbol = 'sol'
    and recorded_hour between (
      select
        min(date)
      from
        all_actions
    )
    and current_date()
  group by
    1
),
filled_dates as (
  select
    date,
    label,
    stake_account,
    coalesce(
      post_tx_staked_balance,
      lag(post_tx_staked_balance) ignore nulls over (
        partition by stake_account,
        label
        order by
          date
      ),
      0
    ) / pow(10, 9) as balance_sol,
    balance_sol * price as balance_usd
  from
    dates
    left join all_actions using(date, stake_account, label)
    left join prices using(date)
),
aggregated as (
  select
    date,
    label,
    sum(balance_sol) as sol,
    sum(balance_usd) as usd
  from
    filled_dates
  group by
    1,
    2
)
select
  t2.label,
  t1.*,
  t2.sol as TVL_SOL
from
  (
    select
      date :: date as dt,
      sum(delta_sol) as net_change,
      sum(deposits) as deposits,
      sum(withdrawals) as withdrawals -- sum(delta_sol) over (partition by date::date order by date) as cumulative_sum
      -- sum(delta_sol) over (order by dt asc) as cumsum0
    from
      all_actions
    group by
      dt
    order by
      dt asc
  ) t1
  left join aggregated t2 on t1.dt = t2.date 

with t0 as (
    select block_timestamp::date as date
    , sum(case when action like 'deposit%' then amount else 0 end ) / pow(10,9) as deposit_stake_amount
    , sum(case when action like 'withdraw%' then amount else 0 end ) / pow(10,9) as withdraw_stake_amount
    , sum(
        case when action like 'deposit%' then amount
        else -amount
        end
    ) / pow(10,9) as net_stake_amount
    from solana.defi.fact_stake_pool_actions
    where succeeded
        and (action like 'deposit%' or action like 'withdraw%')
    group by 1
)
select *
, sum(net_stake_amount) over (order by date) as total_stake_amount
from t0
order by 1 desc

select stake_pool_name, action, COUNT(1), sum(amount / POW(10, 9)) as amt
from solana.defi.fact_stake_pool_actions
where stake_pool_name ILIKE '%jit0%'
group by 1, 2

select *
from solana.defi.fact_swaps
where 
LIMIT 100


select *
from solana.defi.fact_swaps
where block_timestamp >= CURRENT_DATE - 1
    and swap_program like 'jupiter%'
LIMIT 111

select distinct program_id, tx_id
from solana.core.fact_events
where block_timestamp >= CURRENT_DATE - 1
    and program_id ILIKE 'jup%'


-- Collection token lists CTE
with collection_tokens as (
    select distinct
    mint,
    project_name,
    token_id,
    GET (token_metadata, 'Body') as body,
    GET (token_metadata, 'Armor') as armor,
    GET (token_metadata, 'Attribute Count') as attys,
    GET (token_metadata, 'Ears') as ears,
    GET (token_metadata, 'Eyes') as eyes,
    GET (token_metadata, 'Faction') as faction,
    GET (token_metadata, 'Helmet') as helmet,
    GET (token_metadata, 'Mouth') as mouth
    from
        solana.nft.dim_nft_metadata
    where
        project_name in ('GGSG: Galactic Geckos', 'Galactic Geckos')
), sales as (
    select
      a.sales_amount,
      a.marketplace,
      a.mint
    from
      solana.nft.fact_nft_sales a
      inner join collection_tokens b on a.mint = b.mint
    where
      block_timestamp >= DATEADD(day, - 90 , CURRENT_DATE)
      and succeeded
), summary as (
    select
    body as Attribute,
    ROUND(max(sales_amount),1) as max_sale,
    ROUND(min(sales_amount),1) as min_sale,
    ROUND(avg(sales_amount),1) as avg_sale,
    ROUND(median(sales_amount),1) as mdn_sale,
    COUNT(a.mint) as num_sales
    from
    collection_tokens a
    inner join sales b on a.mint = b.mint
    group by 1
)
select *
, row_number() over (order by mdn_sale) as rk
, concat(case when rk <10, '0', '', rk, '. ', Attribute) as label
from summary

with imp as (
    select d.id as dashboard_id
    , sum(impression_count) as impression_count
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    group by 1
)
select d.title
, u.username
, q.statement
, imp.impression_count
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.velocity_app_prod._dashboards_to_queries dtq
    on d.id = dtq.A
join bi_analytics.velocity_app_prod.queries q
    on dtq.B = q.id
join bi_analytics.velocity_app_prod.users u
    on u.id = q.created_by_id
join imp
    on imp.dashboard_id = d.id
where statement ILIKE '%live%' or statement ILIKE '%helius%'


select *
from solana.core.fact_events
where block_timestamp >= CURRENT_DATE - 1
    and program_id ='JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'


select *
from solana.core.fact_events
where block_timestamp::date = '2023-10-31'::date
    and tx_id = 
    and t.tx_id = '4U5eEb3ShixbwMLZ7L2VTk4xfHTBav3Ptfez6RX276An3UYpPp9XpRBg3dYGE132v9DrQg2hevGZkzZ1q4dqmnGh'


with pages as (
    select 
    *
    , row_number() over (partition by coalesce(user_id, anonymous_id), context_session_id order by timestamp) as page_in_session
    from bi_analytics.gumby.pages p
    where timestamp >= '2023-08-01'
), views as (
    select d.id as dashboard_id
    , u.username
    -- , concat('https://flipsidecrypto.xyz/',u.username,'/', d.latest_slug) as dashboard_url
    , p.context_page_tab_url as dashboard_url
    -- , COUNT(distinct p.id) as n_views
    -- , COUNT(distinct anonymous_id) as n_viewers
    -- , sum( case when p.context_page_referring_domain = 't.co' then 1 else 0 end ) as n_views_twitter
    , COUNT( distinct case when p.context_page_referring_domain = 't.co' then anonymous_id else null end ) as n_viewers_twitter
    from pages p
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(p.context_page_tab_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where timestamp >= '2023-08-01'
        and p.context_page_referring_domain = 't.co'
        and page_in_session = 1
    group by 1, 2, 3
), imp as (
    select d.id as dashboard_id
    , sum(impression_count) as impression_count
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    group by 1
)
select v.*
, coalesce(impression_count, 0) as impression_count
from views v
left join imp i
    on i.dashboard_id = v.dashboard_id
where i.dashboard_id is null
order by n_viewers_twitter desc


select *
from solana_dev.silver.swaps
where block_timestamp >= CURRENT_DATE - 3
    and tx_id = '4U5eEb3ShixbwMLZ7L2VTk4xfHTBav3Ptfez6RX276An3UYpPp9XpRBg3dYGE132v9DrQg2hevGZkzZ1q4dqmnGh'
LIMIT 100


select COUNT(distinct tx_id) from solana.core.fact_events
where block_timestamp::date >= '2023-10-22'::date
    and block_timestamp::date <= '2023-10-25'::date
    and program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'

with t0 as (
    select distinct tx_id
    from solana.core.fact_events
    where block_timestamp::date >= '2023-10-22'::date
        and block_timestamp::date <= '2023-10-25'::date
        and program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
        and succeeded
), t1 as (
    select distinct tx_id
    from solana_dev.silver.swaps
    where block_timestamp::date >= '2023-10-22'::date
        and block_timestamp::date <= '2023-10-25'::date
        and program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
        and succeeded
)
select t0.*
, case when t1.tx_id is null then 0 else 1 end as has_swap
from t0
left join t1
    on t1.tx_id = t0.tx_id

select COUNT(distinct tx_id) from solana_dev.silver.swaps
where block_timestamp::date >= '2023-10-22'::date
    and block_timestamp::date <= '2023-10-25'::date
    and program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'


with t0 as (
    select q.created_at::date as date
    , q.id
    , MIN(case when t.name is null then 1 else 0 end) as missing_chain
    from bi_analytics.velocity_app_prod.queries q
    join bi_analytics.velocity_app_prod._queries_to_tags qtt
        on q.id = qtt.A
    left join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and t.type = 'project'
    where q.created_at::date >= CURRENT_DATE - 7
    group by 1, 2
)
select date
, avg(missing_chain) as pct_missing_chain
from t0
group by 1
order by 1 desc

select distinct signers[0]::string as address
, l.label
from solana.core.fact_events e
join solana.core.dim_labels l
    on l.address = e.program_id
where e.block_timestamp >= CURRENT_DATE - 3
    and l.label ILIKE 'jup%'


select *
from bi_analytics.velocity_app_prod.users
where email ILIKE '%bananadyn%'


with chain0 as (
    -- this with chain0 statement tags dashboards to chains based on the queries that are included in it.
    select d.id as dashboard_id
    -- contract ids for squid etc. specific to axelar
    , case when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
        or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
        or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
        or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%'
        then 'Axelar' else INITCAP(t.name) end as chain
    , COUNT(distinct q.id) as n_queries
    from bi_analytics.velocity_app_prod.users u
    join bi_analytics.velocity_app_prod.dashboards d
        on d.created_by_id = u.id
    join bi_analytics.velocity_app_prod._dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    join bi_analytics.velocity_app_prod._queries_to_tags qtt
        on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and t.type = 'project'
    group by 1, 2
), chain as (
    -- this ranks dashboards to chains based on # of queries per chain with priority given to partner chains.
    select *
    , row_number() over (
        partition by dashboard_id
        order by
        case when chain in (
              'Avalanche'
            , 'Axelar'
            , 'Flow'
            , 'Near'
            , 'Sei'
            , 'Solana'
        ) then 1 else 2 end
        , n_queries desc
        , chain
    ) as rn
    from chain0
), t0 as (
  select
    -- pulling in user metadata and ranking dashboards
    case when t.created_by_id is null then 1 else 0 end as is_team,
    coalesce(t.created_by_id, d.created_by_id) as user_id,
    coalesce(u2.username, u.username) as team_username,
    d.profile_id,
    coalesce(t.name, u.username) as username,
    dr.currency,
    d.id as dashboard_id,
    coalesce(c.chain, 'Polygon') as ecosystem,
    row_number() over (
      order by
        dr.ranking_trending
    ) as current_rank
  from
    bi_analytics.content_rankings.dashboard_rankings dr
    join bi_analytics.velocity_app_prod.dashboards d on d.id = dr.dashboard_id
    join bi_analytics.velocity_app_prod.users u on u.id = d.created_by_id
    -- teams update
    left join bi_analytics.velocity_app_prod.profiles p
        on p.id = d.profile_id
    left join bi_analytics.velocity_app_prod.teams t
        on t.profile_id = p.id
    join bi_analytics.velocity_app_prod.users u2 on u.id = t.created_by_id
    left join chain c
        on c.dashboard_id = dr.dashboard_id
        and c.rn = 1
  where
    -- users that can't earn.
    coalesce(u.role, '') <> 'internal'
    and not u.username in (
      'Polaris_9R',
      'dsaber',
      'flipsidecrypto',
      'metricsdao',
      'drethereum',
      'Orion_9R',
      'sam',
      'forgash',
      'nftchance__',
      'danner'
    )
)

select 
  -- this user id
  is_team,
  user_id,
  username,
  t0.dashboard_id,
  profile_id,
  case when ecosystem in (
          'Solana'
        , 'Avalanche'
        , 'Axelar'
        , 'Flow'
        , 'Near'
        , 'Sei'
  ) then ecosystem else 'Polygon' end as ecosystem,
  case
        when ecosystem = 'Solana' then 'SOL'
        when ecosystem = 'Avalanche' then 'AVAX'
        when ecosystem = 'Axelar' then 'AXL'
        when ecosystem = 'Flow' then 'FLOW'
        when ecosystem = 'Near' then 'NEAR'
        when ecosystem = 'Sei' then 'SEI'
        else 'USDC' end as currency,
  COUNT(1) as amount
from
  t0
left join chain c
    on c
where
  current_rank <= 800
group by
  1,
  2,
  3,
  4,
  5,
  6,
  7
order by
  8 desc


select *
from bi_analytics.velocity_app_prod.profiles p
join bi_analytics.velocity_app_prod.teams t
    on t.profile_id = p.id
where p.type = 'team'



select t.*
, p.*
from bi_analytics.velocity_app_prod.dashboards d
left join bi_analytics.velocity_app_prod.profiles p
    on p.id = d.profile_id
left join bi_analytics.velocity_app_prod.teams t
    on t.profile_id = p.id
where d.id = '69b04c0f-749b-4c88-bf31-34e0e02426aa'



with chain0 as (
    select d.id as dashboard_id
    , case when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
        or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
        or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
        or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%'
        then 'Axelar' else INITCAP(t.name) end as chain
    , COUNT(distinct q.id) as n_queries
    from bi_analytics.velocity_app_prod.users u
    join bi_analytics.velocity_app_prod.dashboards d
        on d.created_by_id = u.id
    join bi_analytics.velocity_app_prod._dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    join bi_analytics.velocity_app_prod._queries_to_tags qtt
        on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and t.type = 'project'
    group by 1, 2
), chain as (
    select *
    , row_number() over (
        partition by dashboard_id
        order by
        case when chain in (
              'Avalanche'
            , 'Axelar'
            , 'Flow'
            , 'Near'
            , 'Sei'
            , 'Solana'
        ) then 1 else 2 end
        , n_queries desc
        , chain
    ) as rn
    from chain0
), t0 as (
  select
    -- 11/02: grab the team owner username if is a team
    coalesce(m.user_id, d.created_by_id) as user_id,
    d.profile_id as profile_id,
    coalesce(tu.username, u.username) as username,
    dr.currency,
    d.id as dashboard_id,
    p.type,
    coalesce(c.chain, 'Polygon') as ecosystem,
    row_number() over (
      order by
        dr.ranking_trending
    ) as current_rank
  from
    bi_analytics.content_rankings.dashboard_rankings dr
    join bi_analytics.velocity_app_prod.dashboards d on d.id = dr.dashboard_id
    left join bi_analytics.velocity_app_prod.profiles p
        on p.id = d.profile_id
    left join bi_analytics.velocity_app_prod.teams t
        on t.profile_id = p.id
    left join bi_analytics.velocity_app_prod.members m
        on t.id = m.team_id
        and m.role = 'owner'
    left join bi_analytics.velocity_app_prod.users tu
        -- changed this join to the team owner
        on tu.id = m.user_id
    join bi_analytics.velocity_app_prod.users u on u.id = d.created_by_id
    left join chain c
        on c.dashboard_id = dr.dashboard_id
        and c.rn = 1
  where
    coalesce(u.role, '') <> 'internal'
    and not u.username in (
      'Polaris_9R',
      'dsaber',
      'flipsidecrypto',
      'metricsdao',
      'drethereum',
      'Orion_9R',
      'sam',
      'forgash',
      'nftchance__',
      'danner',
      'charliemarketplace',
      'theericstone'
    )
)
select
  user_id,
  username,
  dashboard_id,
  profile_id,
  type,
  case when ecosystem in (
          'Solana'
        , 'Avalanche'
        , 'Axelar'
        , 'Flow'
        , 'Near'
        , 'Sei'
  ) then ecosystem else 'Polygon' end as ecosystem,
  case
        when ecosystem = 'Solana' then 'SOL'
        when ecosystem = 'Avalanche' then 'AVAX'
        when ecosystem = 'Axelar' then 'AXL'
        when ecosystem = 'Flow' then 'FLOW'
        when ecosystem = 'Near' then 'NEAR'
        when ecosystem = 'Sei' then 'SEI'
        else 'USDC' end as currency,
  case when current_rank <= 10 then 1.5 else 1 end as base_amount,
  case when ecosystem in (
    'Flow'
    ,'Near'
    ,'Sei'
  ) then 1.5 else 1 end as boost,
  base_amount * boost as amount
from
  t0
where
  current_rank <= 30
order by
  7 desc





with pages as (
    select 
    *
    , row_number() over (partition by coalesce(user_id, anonymous_id), context_session_id order by timestamp) as page_in_session
    from bi_analytics.gumby.pages p
    where timestamp >= '2023-06-01'
), views as (
    select d.id as dashboard_id
    , u.username
    -- , concat('https://flipsidecrypto.xyz/',u.username,'/', d.latest_slug) as dashboard_url
    -- , p.context_page_tab_url as dashboard_url
    -- , COUNT(distinct p.id) as n_views
    -- , COUNT(distinct anonymous_id) as n_viewers
    -- , sum( case when p.context_page_referring_domain = 't.co' then 1 else 0 end ) as n_views_twitter
    , COUNT( distinct case when p.context_page_referring_domain = 't.co' then anonymous_id else null end ) as n_viewers_twitter
    from pages p
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(p.context_page_tab_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where timestamp >= '2023-06-01'
        and p.context_page_referring_domain = 't.co'
        and page_in_session = 1
    group by 1, 2
)
select d.id as dashboard_id
, d.title
, u.username
, coalesce(v.n_viewers_twitter, 0) as n_viewers_twitter
, t.*
, t.impression_count / GREATEST(t.like_count, 1) as imp_likes
, t.impression_count / coalesce(v.n_viewers_twitter, 1) as imp_views
, 0 as is_bot
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
left join views v
    on v.dashboard_id = d.id
join bi_analytics.twitter.tweet t
    on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
qualify(
    row_number() over (partition by conversation_id, d.id order by impression_count desc) = 1
)
order by imp_views


select * from  "FLIPSIDE_PROD_DB"."BRONZE"."PROD_DATA_SCIENCE_UPLOADS_1748940988"
LIMIT 100


select c.*
from crosschain.bronze.data_science_uploads
, lateral flatten(
    input => record_content
) c
where record_metadata:key like 'dashboard-tags%'


with labels as (
    select c.value:dashboard_id::string as dashboard_id
    , c.value:tag::string as dashboard_tag
    from crosschain.bronze.data_science_uploads
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'dashboard-tags%'
), t0 as (
    select date_trunc('week', t.created_at)::date as week
    , case
        when d.id in (
            select dashboard_id from labels where dashboard_tag = 'nic-carter-bounty'
        ) then 'Nic Carter Bounty'
        when u.username = 'tkvresearch' then 'TK Research'
        else 'Other' end as dashboard_type
    , (impression_count) as impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where not d.id in (
        select dashboard_id from labels where dashboard_tag = 'bot'
    )
    qualify (
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
)
select week
, dashboard_type
, sum(impression_count) as impression_count
from t0
group by 1, 2


with labels as (
    select c.value:dashboard_id::string as dashboard_id
    , c.value:tag::string as dashboard_tag
    from crosschain.bronze.data_science_uploads
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'dashboard-tags%'
), t0 as (
    select date_trunc('day', t.created_at)::date as date
    , date_trunc('week', t.created_at)::date as week
    , date_trunc('month', t.created_at)::date as month
    , impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    where not d.id in (
        select dashboard_id from labels where dashboard_tag = 'bot'
    )
    qualify (
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
)
select date
, week
, month
, sum(impression_count) as impression_count
from t0
group by 1, 2, 3


with labels as (
    select c.value:dashboard_id::string as dashboard_id
    , c.value:tag::string as dashboard_tag
    from crosschain.bronze.data_science_uploads
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'dashboard-tags%'
), t0 as (
    select date_trunc('week', t.created_at)::date as week
    , (case when t.platform = 'Dune' then impression_count else 0 end) as dune_impressions
    , (case when t.platform = 'Flipside' then impression_count else 0 end) as flipside_impressions
    from bi_analytics.twitter.tweet t
    left join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    where not coalesce(d.id, '') in (
        select dashboard_id from labels where dashboard_tag = 'bot'
    ) and (d.id is not null or (tweet_type = 'Dashboard' and platform = 'Dune'))
    qualify (
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
), t1 as (
    select week
    , sum(dune_impressions) as dune_impressions
    ,  sum(flipside_impressions) as flipside_impressions
    from t0
    group by 1
)
select *
, flipside_impressions * 100 / dune_impressions as impressions_ratio
from t0


select *
from bi_analytics.twitter.tweet t
LIMIT 100

with t0 as (
    select program_id
    , COUNT(distinct tx_id) as n_tx
    from solana.core.fact_events
    where block_timestamp >= CURRENT_DATE - 90
        and program_id ILIKE 'jup%'
        and succeeded
    group by 1
)
select *
, ROUND(100 * n_tx / sum(n_tx), 1) over () as pct_tx
from t0
order by pct_tx desc



select t.*
from solana.core.fact_transfers t
left join solana.nft.fact_nft_sales s
    on s.block_timestamp = t.block_timestamp
    and s.tx_id = t.tx_id
left join solana.nft.fact_nft_mints m
    on m.block_timestamp = t.block_timestamp
    and m.tx_id = t.tx_id
where t.block_timestamp >= current_date - 1
    and t.tx_to = 'rFqFJ9g7TGBD8Ed7TPDnvGKZ5pWLPDyxLcvcH2eRCtt'
    and s.tx_id is null
    and m.tx_id is null
order by amount desc
LIMIT 100




with p0 as (
    select token_address
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as hourly_price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= '2023-01-01'::date
        and is_imputed = FALSE
    group by 1, 2
), t0 as (
    select distinct tx_id
    , block_timestamp
    , program_id
    , signers[0]::string as signer
    from solana.core.fact_events s
    where block_timestamp >= CURRENT_DATE - 90
), t1 as (
    select program_id
    , tx_to
    , COUNT(1) as n_tx
    , COUNT(distinct signer) as n_signers
    , sum(case when t.mint like 'So%' then amount else 0 end) as sol_amount
    , sum(amount * coalesce(p0.hourly_price, 0)) as usd_amount
    from t0
    join solana.core.fact_transfers t
        on t.block_timestamp = t0.block_timestamp
        and t.tx_id = t0.tx_id
    left join p0
        on p0.token_address = t.mint
        and p0.hour = date_trunc('hour', t.block_timestamp)
    where t.block_timestamp >= CURRENT_DATE - 90
    group by 1, 2
), t2 as (
    select coalesce(l.label, program_id) as project
    , program_id
    , tx_to
    , n_signers
    , sum(n_tx) as n_tx
    , sum(sol_amount) as sol_amount
    , sum(usd_amount) as usd_amount
    from t1
    left join solana.core.dim_labels l
        on l.address = t1.program_id
    group by 1, 2, 3, 4
), t3 as (
    select *
    , row_number() over (partition by program_id order by n_tx desc) as rn
    , row_number() over (partition by program_id order by usd_amount desc) as rn2
    , row_number() over (partition by program_id order by sol_amount desc) as rn3
    from t2
)
select *
from t3
where (
    rn <= 10
    or rn2 <= 10
    or rn3 <= 10
)


with t0 as (
    select distinct signers[0]::string as signer
    , program_id
    from solana.core.fact_events
    where block_timestamp >= CURRENT_DATE - 30
        and succeeded
), t1 as (
    select signer
    , coalesce(label, program_id) as program
    from t0
    left join solana.core.dim_labels l
        on l.address = t0.program_id
)
select program
, COUNT(1) as n_signers
from t1
where signer != 'solana'
group by 1


with p0 as (
    select token_address
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as hourly_price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= CURRENT_DATE - 90
        and is_imputed = FALSE
    group by 1, 2
), t0 as (
    select block_timestamp::date as date
    , tx_to
    , COUNT(1) as n_tx
    , sum(case when t.mint like 'So%' then amount else 0 end) as sol_amount
    , sum(amount * coalesce(p0.hourly_price, 0)) as usd_amount
    from t0
    join solana.core.fact_transfers t
        on t.block_timestamp = t0.block_timestamp
        and t.tx_id = t0.tx_id
    left join p0
        on p0.token_address = t.mint
        and p0.hour = date_trunc('hour', t.block_timestamp)
    where t.block_timestamp >= CURRENT_DATE - 90
        and t.tx_to in (
            '8d7dmXFxP9YKubdfo3JeFYUYfC1C65HHtw9tAGsg934z'
            , 'rFqFJ9g7TGBD8Ed7TPDnvGKZ5pWLPDyxLcvcH2eRCtt'
        )
    group by 1, 2
)
select *
from t0

with t0 as (
    select block_timestamp
    , tx_id
    , MAX(case when tx_to = '4zdNGgAtFsW1cQgHqkiWyRsxaAgxrSRRynnuunxzjxue' then 1 else 0 end) as has_1
    , MAX(case when tx_to = '36tfiBtaDGjAMKd6smPacHQhe4MXycLL6f9ww9CD1naT' then 1 else 0 end) as has_2
    -- , MAX(case when tx_to = 'EyaSjUtSgo9aRD1f8LWXwdvkpDTmXAW54yoSHZRF14WL' then 1 else 0 end) as has_3
    -- , MAX(case when tx_to = '89SrbjbuNyqSqAALKBsKBqMSh463eLvzS4iVWCeArBgB' then 1 else 0 end) as has_4
    -- , MAX(case when tx_to = '2ZixuuJXyZbkwbRTsLVXfEDaukEeoM2L9mfPCdHV249v' then 1 else 0 end) as has_5
    -- , MAX(case when tx_to = 'tenEpSp5GQM3Ko211Nrugvt7fk6cL7VUwAHmAY9rFNq' then 1 else 0 end) as has_6
    from solana.core.fact_transfers t
    where block_timestamp >= CURRENT_DATE - 7
    group by 1, 2
)
select *
-- , has_1+has_2+has_3+has_4+has_5 as sm
, has_1+has_2 as sm
-- +has_6 as sm
from t0
where GREATEST(
    has_1
    , has_2
    -- , has_3
    -- , has_4
    -- , has_5
    -- , has_6
) = 1
LIMIT 1000


select COUNT(distinct q.created_by_id) as n_users
, COUNT(distinct q.id) as n_queries
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.query_runs r
    on r.query_id = q.id
    and r.status = 'finished'
where statement ILIKE '%solana.%'
group by 1
order by 1 desc

select date_trunc('month', q.created_at) as month
, COUNT(distinct q.created_by_id) as n_users
, COUNT(distinct q.id) as n_queries
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.query_runs r
    on r.query_id = q.id
    and r.status = 'finished'
where q.statement ILIKE '%solana.%'
group by 1
order by 1

select COUNT(distinct q.created_by_id) as n_users
, COUNT(distinct q.id) as n_queries
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.query_runs r
    on r.query_id = q.id
    and r.status = 'finished'
where q.statement ILIKE '%solana.%'

select *
from solana.core.fact_transfers
where block_timestamp >= CURRENT_DATE - 7
    and tx_to = '4qD717qKoj3Sm8YfHMSR7tSKjWn5An817nArA6nGdcUR'
 (
    tx_from = '4qD717qKoj3Sm8YfHMSR7tSKjWn5An817nArA6nGdcUR'
    or tx_to = '4qD717qKoj3Sm8YfHMSR7tSKjWn5An817nArA6nGdcUR'
    )

with labels as (
    select c.value:dashboard_id::string as dashboard_id
    , c.value:tag::string as dashboard_tag
    from crosschain.bronze.data_science_uploads
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'dashboard-tags%'
), t0 as (
    select d.title
    , u.username
    , t.*
    , case
        when d.id in (
            select dashboard_id from labels where dashboard_tag = 'nic-carter-bounty'
        ) then 'Nic Carter Bounty'
        when u.username = 'tkvresearch' then 'TK Research'
        else 'Other' end as dashboard_type
    , (impression_count) as impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where not d.id in (
        select dashboard_id from labels where dashboard_tag = 'bot'
    )
    qualify (
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
)
select *
from t0
where dashboard_type = 'nic-carter-bounty'
order by impression_count desc

with t0 as (
    select tx_to
    , COUNT(1) as n_tx
    , COUNT(distinct tx_from) as n_addy
    from solana.core.fact_transfers
    where block_timestamp >= CURRENT_DATE - 30
    group by 1
), t1 as (
    select t.tx_to
    , COUNT(1) as n_tx_2
    , COUNT(distinct tx_from) as n_addy_2
    from solana.core.fact_transfers t
    where block_timestamp >= CURRENT_DATE - 30
        and amount in (10, 20, 30, 40, 50)
        and mint = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
    group by 1
)
select t1.*
join t0
    on t0.tx_to = t1.tx_to

select *
from solana.core.fact_transfers
where block_timestamp >= CURRENT_DATE - 90 and tx_to = 'FTxKK76v6JVvmXfbYF7SgJ3c3QEgmicTApH32Y3xm5mx'

select *
from solana.core.dim_labels

with t0 as (
    select l.label
    , s.tx_id
    , s.block_timestamp
    , MAX(amount) as mx_amt
    from solana.nft.fact_nft_sales s
    join solana.core.dim_labels l
        on l.address = s.mint
    join solana.core.fact_transfers t
        on t.block_timestamp = s.block_timestamp
        and t.tx_id = s.tx_id
    where s.block_timestamp >= CURRENT_DATE - 7
        and t.block_timestamp >= CURRENT_DATE - 7
        and t.mint like 'So%'
    group by 1, 2, 3
), t1 as (
    select t0.label
    , t.tx_to
    , COUNT(1) as n_tx
    , sum(amount) as amount
    from solana.nft.fact_nft_sales s
    join solana.core.fact_transfers t
        on t.block_timestamp = s.block_timestamp
        and t.tx_id = s.tx_id
    join t0
        on t0.block_timestamp = s.block_timestamp
        and t0.tx_id = s.tx_id
    where s.block_timestamp >= CURRENT_DATE - 7
        and t.block_timestamp >= CURRENT_DATE - 7
        and t.amount < (t0.mx_amt * 0.1)
        and t.mint like 'So%'
    group by 1, 2
), t1 as (
    select *
    , row_number() over (partition by label order by n_tx desc) as rn
    from t0
)
select *
from t1
where rn <= 5



select s.tx_id
, s.sales_amount
, s.mint
, case when s.marketplace like 'magic eden%' then 'Magic Eden' else INITCAP(s.marketplace) end as marketplace
, s.block_timestamp
, coalesce(INITCAP(l.label), 'Other') as collection
, tx_to
, sum(
    case when tx_to in (
        '2NZukH2TXpcuZP4htiuT8CFxcaQSWzkkR6kepSWnZ24Q' -- magic eden v1
        , 'rFqFJ9g7TGBD8Ed7TPDnvGKZ5pWLPDyxLcvcH2eRCtt' -- magic eden v2
        , 'Fz7HjwoXiDZNRxXMfLAAJLbArqjCTVWrG4wekit2VpSd' -- yawww
        , '39fEpihLATXPJCQuSiXLUSiCbGchGYjeL39eyXh3KbyT' -- solanart
        , '6QEJwoTfHg4vkwE6nbprtwiwEw7msvNuZJ1tp22SPACE' -- hyperspace
        , '6LQWHVXVyauAUS4KQ1wW1EvwHoauEunPN923LWhaYQx7' -- coral cube
        , '6482e33zrerYfhKAjPR2ncMSrH2tbTy5LDjdhB5PXzxd' -- exchange art
        , '8mcjXbJ8j4VryYFNpcBCFS37Au8zVYU53WTVaruJWcKt' -- opensea
        , 'bDmnDkeV7xqWsEwKQEgZny6vXbHBoCYrjxA4aCr9fHU' -- solana monkey business marketplace
        , 'DKeBWDK1jGkDvo6TGjZ2bGFBCTyZZstFhAJjWR7y2a1E' -- solport
        , '4zdNGgAtFsW1cQgHqkiWyRsxaAgxrSRRynnuunxzjxue' -- solport
    ) then coalesce(t.amount, 0) else 0 end
) as m_amt
, sum(
    case when tx_to in (
        '2NZukH2TXpcuZP4htiuT8CFxcaQSWzkkR6kepSWnZ24Q' -- magic eden v1
        , 'rFqFJ9g7TGBD8Ed7TPDnvGKZ5pWLPDyxLcvcH2eRCtt' -- magic eden v2
        , 'Fz7HjwoXiDZNRxXMfLAAJLbArqjCTVWrG4wekit2VpSd' -- yawww
        , '39fEpihLATXPJCQuSiXLUSiCbGchGYjeL39eyXh3KbyT' -- solanart
        , '6QEJwoTfHg4vkwE6nbprtwiwEw7msvNuZJ1tp22SPACE' -- hyperspace
        , '6LQWHVXVyauAUS4KQ1wW1EvwHoauEunPN923LWhaYQx7' -- coral cube
        , '6482e33zrerYfhKAjPR2ncMSrH2tbTy5LDjdhB5PXzxd' -- exchange art
        , '8mcjXbJ8j4VryYFNpcBCFS37Au8zVYU53WTVaruJWcKt' -- opensea
        , 'bDmnDkeV7xqWsEwKQEgZny6vXbHBoCYrjxA4aCr9fHU' -- solana monkey business marketplace
        , 'DKeBWDK1jGkDvo6TGjZ2bGFBCTyZZstFhAJjWR7y2a1E' -- solport
        , '4zdNGgAtFsW1cQgHqkiWyRsxaAgxrSRRynnuunxzjxue' -- solport
    )
    or amount > sales_amount * 0.5
    or marketplace = 'hadeswap'
    then 0 else coalesce(t.amount, 0) end
) as r_amt
from solana.core.fact_nft_sales s
join solana.core.fact_transfers t
    on t.tx_id = s.tx_id
    and t.mint = 'So11111111111111111111111111111111111111112'
left join solana.core.dim_labels l
    on l.address = s.mint
where s.block_timestamp >= CURRENT_DATE - 3
    and s.block_timestamp::date < CURRENT_DATE::date
    and s.succeeded
    -- and s.mint in (
    -- '8boZTJUBNN3ezQsX5iqEoHVGEyd2SFQRpKUrbdbJq6Gb'
    -- , '9Bo2tA9RZ4UNsJCMEYtjRT6ZjAYorkTo7xxWJpGgvvnG'
    -- , 'HC3W4pvY7uuoSV8GXJy5PsZqRxdWA5JsemF7mjz8bS6o'
    -- , 'HLhCMmh1uYSYfoiDGoFrUgjeHWYkhPmhvgHsnrw1kSxK'
    -- , 'HLhCMmh1uYSYfoiDGoFrUgjeHWYkhPmhvgHsnrw1kSxK'
    -- )
group by 1, 2, 3, 4, 5, 6, 7

select l.label
, s.*
, t.*
, t.amount / s.sales_amount as pct
from solana.nft.fact_nft_sales s
join solana.core.dim_labels l
    on l.address = s.mint
join solana.core.fact_transfers t
    on t.block_timestamp = s.block_timestamp
    and t.tx_id = s.tx_id
where s.block_timestamp >= CURRENT_DATE - 7
    and l.label ilike 'famous%'
LIMIT 10000


select t.*
, case when s.tx_id is null then 0 else 1 end as is_nft_sale
from solana.core.fact_transfers t
left join solana.nft.fact_nft_sales s
    on s.block_timestamp = t.block_timestamp
    and s.tx_id = t.tx_id
where t.block_timestamp >= CURRENT_DATE - 90
    and t.tx_to = '4ANmPVAUxwiWPVdPVQ4AMp1YyChcad4cWrMS9pEG2CmM'

-- TODO
-- The heist in-game revenue
-- Pretty much all DeFi
-- 
with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as hourly_price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= '2023-01-01'::date
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address
    , date_trunc('day', recorded_hour) as date
    , avg(close) as daily_price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= '2023-01-01'::date
        and is_imputed = FALSE
    group by 1, 2
), s as (
    select distinct block_timestamp
    , tx_id
    from solana.nft.fact_nft_sales
    where block_timestamp >= CURRENT_DATE - 30
)
select case
    when tx_to = 'DnucdCxho6URdV93gU5ctwcqcH43KDdNz3YbktpK7bDM' then 'Solcasino'
    when tx_to = '4ANmPVAUxwiWPVdPVQ4AMp1YyChcad4cWrMS9pEG2CmM' then 'The Heist'
    when tx_to = '4aS5ZiAikSW9EdF9QVTxVnefggnuX144u4ndME9L7hfg' then 'Photo Finish'
    when tx_to = 'rFqFJ9g7TGBD8Ed7TPDnvGKZ5pWLPDyxLcvcH2eRCtt' then 'Magic Eden'
    when tx_to = '6482e33zrerYfhKAjPR2ncMSrH2tbTy5LDjdhB5PXzxd' then 'Exchange Art'
    when tx_to = '39fEpihLATXPJCQuSiXLUSiCbGchGYjeL39eyXh3KbyT' then 'Exchange Art'
    when tx_to = '4zdNGgAtFsW1cQgHqkiWyRsxaAgxrSRRynnuunxzjxue' then 'Tensor'
    when tx_to in (
        'HAryckvjyViFQEmhmMoCtqqBMJnpXEYViamyDhZUJfnG'
        , 'HqqiyJcm3yWPyzwisRKAQa2bJAj14V837yJRGaxwRFaG'
        , '8vttKbtbXaUcCfJdPNnZjMfKMBCnTXsxy96U4WSLSJHU'
    ) then 'Solana Monkey Business'
    else 'Others' end as project
, sum(amount * coalesce(p0.price, p1.price, 0)) as project_revenue
from solana.core.fact_transfers t
left join s
    on s.block_timestamp = t.block_timestamp
    and s.tx_id = t.tx_id
left join p0
    on p0.mint = t.mint
    and p0.hour = date_trunc('hour', t.block_timestamp)
where block_timestamp >= CURRENT_DATE - 30
    and (
        -- only nft royalties count
        tx_to in (
            -- solcasino
            'DnucdCxho6URdV93gU5ctwcqcH43KDdNz3YbktpK7bDM'
            -- the heist
            , '4ANmPVAUxwiWPVdPVQ4AMp1YyChcad4cWrMS9pEG2CmM'
            -- photo finish
            , '4aS5ZiAikSW9EdF9QVTxVnefggnuX144u4ndME9L7hfg'
            -- SMB
            , 'HAryckvjyViFQEmhmMoCtqqBMJnpXEYViamyDhZUJfnG'
            -- SMB (Gen 3)
            , 'HqqiyJcm3yWPyzwisRKAQa2bJAj14V837yJRGaxwRFaG'
            , '8vttKbtbXaUcCfJdPNnZjMfKMBCnTXsxy96U4WSLSJHU'
            -- magic eden
            , 'rFqFJ9g7TGBD8Ed7TPDnvGKZ5pWLPDyxLcvcH2eRCtt'
            -- exchange art
            , '6482e33zrerYfhKAjPR2ncMSrH2tbTy5LDjdhB5PXzxd'
            -- solanart
            , '39fEpihLATXPJCQuSiXLUSiCbGchGYjeL39eyXh3KbyT'
            -- tensor
            , '4zdNGgAtFsW1cQgHqkiWyRsxaAgxrSRRynnuunxzjxue'
        )
        and s.tx_id is not null
        and t.mint = 'So11111111111111111111111111111111111111112'
    )
group by 1


with t0 as (
    select block_timestamp
    , tx_id
    , amount
    , tx_from
    , tx_to
    from solana.core.fact_transfers
    where block_timestamp >= CURRENT_DATE - 1
        and mint = 'HxRELUQfvvjToVbacjr9YECdfQMUqGgPYB68jVDYxkbr'
), t1 as (
    select e.program_id
    , e.signers[0]::string as signer
    , t0.*
    from solana.core.fact_events e
    join t0
        on t0.block_timestamp = e.block_timestamp
        and t0.tx_id = e.tx_id
    where e.block_timestamp >= CURRENT_DATE - 1
        and not e.program_id in (
            'ComputeBudget111111111111111111111111111111'
            , 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
        )
)
select program_id
, COUNT(distinct signer) as n_signers
, COUNT(distinct tx_id) as n_tx
from t1
group by 1
order by 2 desc


select *
from solana.nft.fact_nft_sales
where mint in (
    '8boZTJUBNN3ezQsX5iqEoHVGEyd2SFQRpKUrbdbJq6Gb'
    , '9Bo2tA9RZ4UNsJCMEYtjRT6ZjAYorkTo7xxWJpGgvvnG'
    , 'HC3W4pvY7uuoSV8GXJy5PsZqRxdWA5JsemF7mjz8bS6o'
    , 'HLhCMmh1uYSYfoiDGoFrUgjeHWYkhPmhvgHsnrw1kSxK'
    , 'HLhCMmh1uYSYfoiDGoFrUgjeHWYkhPmhvgHsnrw1kSxK'
)



with t0 as (
    select s.tx_id
    , s.block_timestamp
    , MAX(amount) as mx_amt
    from solana.nft.fact_nft_sales s
    join solana.core.fact_transfers t
        on t.block_timestamp = s.block_timestamp
        and t.tx_id = s.tx_id
    where s.block_timestamp >= CURRENT_DATE - 7
        and t.block_timestamp >= CURRENT_DATE - 7
        and t.mint like 'So%'
    group by 1, 2
), t1 as (
    select t.tx_to
    , COUNT(1) as n_tx
    , sum(amount) as amount
    from solana.nft.fact_nft_sales s
    join solana.core.fact_transfers t
        on t.block_timestamp = s.block_timestamp
        and t.tx_id = s.tx_id
    join t0
        on t0.block_timestamp = s.block_timestamp
        and t0.tx_id = s.tx_id
    where s.block_timestamp >= CURRENT_DATE - 7
        and t.block_timestamp >= CURRENT_DATE - 7
        and t.amount < (t0.mx_amt * 0.1)
        and t.mint like 'So%'
    group by 1
), t1 as (
    select *
    -- , row_number() over (partition by label order by n_tx desc) as rn
    from t0
)
select *
from t1
-- where rn <= 5



select s.marketplace
, s.sales_amount
, s.mint as nft
, l.label
, t.*
, ROUND(100 * t.amount / s.sales_amount, 2) as pct
from solana.core.fact_transfers t
left join solana.nft.fact_nft_sales s
    on t.block_timestamp = s.block_timestamp
    and t.tx_id = s.tx_id
left join solana.core.dim_labels l
    on l.address = s.mint
where t.block_timestamp >= current_date - 1
    and t.block_timestamp >= current_date - 1
    and t.mint like 'So%'
    and t.tx_to in (
        '8d7dmXFxP9YKubdfo3JeFYUYfC1C65HHtw9tAGsg934z'
        , 'rFqFJ9g7TGBD8Ed7TPDnvGKZ5pWLPDyxLcvcH2eRCtt'
    )
    and amount > 0
order by t.tx_id
LIMIT 1000


with t0 as (
    select distinct block_timestamp
    , tx_id
    from solana.core.fact_events
    where block_timestamp >= CURRENT_DATE - 1
        and program_id = 'TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp'
)
select t0.*
, t.*
from t0
left join solana.core.fact_transfers t
    on t0.block_timestamp = t.block_timestamp
    and t0.tx_id = t.tx_id
order by t0.tx_id
LIMIT 10000



with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as hourly_price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= '2023-01-01'::date
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address
    , date_trunc('day', recorded_hour) as date
    , avg(close) as daily_price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= '2023-01-01'::date
        and is_imputed = FALSE
    group by 1, 2
), t0 as (
    select t.block_timestamp::date as date
    , t.tx_to
    , t.mint
    , sum(amount * coalesce(p0.price, p1.price, 0)) as usd_amount
    from solana.core.fact_transfers t
    left join p0
        on p0.hour = date_trunc('hour', t.block_timestamp)
        and p0.mint = t.mint
    left join p1
        on p1.date = t.block_timestamp::date
        and p1.mint = t.mint
    where tx_to in (
        '5w1nmqvpus3UfpP67EpYuHhE63aSFdF5AT8VHZTkvnp5',
        'Cj9Asoa9k2RzkvP2WHWWzoGHp4qokHMHnQUaAN8jfSx9',
        'DJnoqj9mtQzAxwkK7Uv3mQEunamovdCQZYGwZZy3tZ6r'
    )
    group by 1, 2, 3
)
select * from t0
  full_transaction_table as (
    select
      *
    from
      solana.core.fact_transfers as token_transfers
      INNER join solana.core.ez_token_prices_hourly as token_prices on (
        token_prices.TOKEN_ADDRESS = token_transfers.MINT
        and date_trunc('hour', token_transfers.BLOCK_TIMESTAMP) = token_prices.RECORDED_HOUR
      )
    where
      TX_TO in (
        '5w1nmqvpus3UfpP67EpYuHhE63aSFdF5AT8VHZTkvnp5',
        'Cj9Asoa9k2RzkvP2WHWWzoGHp4qokHMHnQUaAN8jfSx9',
        'DJnoqj9mtQzAxwkK7Uv3mQEunamovdCQZYGwZZy3tZ6r'
      )
      and BLOCK_TIMESTAMP >= '2022-06-08'
  ),
  sol_usdc as (
    select
      date_trunc('DAY', BLOCK_TIMESTAMP) as DAY,
      AMOUNT * CLOSE as AMOUNT_USD,
      'SOL_USDC' as
    TYPE
    from
      full_transaction_table
    where
      TX_TO = '5w1nmqvpus3UfpP67EpYuHhE63aSFdF5AT8VHZTkvnp5'
  ),
  gst_usdc as (
    select
      date_trunc('DAY', BLOCK_TIMESTAMP) as DAY,
      AMOUNT * CLOSE as AMOUNT_USD,
      'GST_USDC' as
    TYPE
    from
      full_transaction_table
    where
      TX_TO = 'Cj9Asoa9k2RzkvP2WHWWzoGHp4qokHMHnQUaAN8jfSx9'
  ),
  gmt_usdc as (
    select
      date_trunc('DAY', BLOCK_TIMESTAMP) as DAY,
      AMOUNT * CLOSE as AMOUNT_USD,
      'GMT_USDC' as
    TYPE
    from
      full_transaction_table
    where
      TX_TO = 'DJnoqj9mtQzAxwkK7Uv3mQEunamovdCQZYGwZZy3tZ6r'
  ),
  all_dooar_tx as (
    (
      select
        *
      from
        sol_usdc
    )
    UNION
    (
      select
        *
      from
        gst_usdc
    )
    UNION
    (
      select
        *
      from
        gmt_usdc
    )
  ),
  fees_earned_per_day as (
    select
      DAY,
      sum(AMOUNT_USD) * 0.01 as FEES_EARNED
    from
      all_dooar_tx
    group by
      DAY
  )
  select sum(FEES_EARNED) as TOTAL_FEES_EARNED
  from fees_earned_per_day

select swap_program, COUNT(1)
from solana.defi.fact_swaps
where block_timestamp >= CURRENT_DATE - 1
group by 1
order by 2 desc
LIMIT 100

with t0 as (
    select distinct e.block_timestamp
    , e.tx_id
    from solana.core.fact_events e
    join solana.core.fact_transactions t
        on t.block_timestamp = e.block_timestamp
        and t.tx_id = e.tx_id
    where e.block_timestamp >= CURRENT_DATE - 3
        and t.block_timestamp >= CURRENT_DATE - 3
        and e.succeeded
        and e.program_id = 'Dooar9JkhdZ7J3LHN3A7YCuoGRUggXhQaG4kijfLGU2j'
        and t.log_messages::string like '%Program log: Instruction: Swap%'
)
select tx_to
, mint
, COUNT(1)
from solana.core.fact_transfers t
join t0
    on t0.block_timestamp = t.block_timestamp
    and t0.tx_id = t.tx_id
where t.block_timestamp >= CURRENT_DATE - 3
group by 1, 2
order by 3 desc


with
  cte_date (date_rec) as (
    select
      to_date('2022-05-15')
    union all
    select
      to_date(dateadd(day, 1, date_rec)) --or week, month, week, hour, minute instead of day
    from
      cte_date
    where
      date_rec < CURRENT_DATE()
  ),
  all_incoming_team_profit_tx as (
    select
      BLOCK_TIMESTAMP,
      TX_ID,
      AMOUNT,
      MINT
    from
      solana.core.fact_transfers
    where
      BLOCK_TIMESTAMP >= TIMESTAMP '2022-05-15'
      and TX_TO = 'Ffbor3Zx46oGPK59S7drZjcTSt8mygZGWc5qkcHLPtWV'
  ),
  all_outgoing_team_profit_tx as (
    select
      BLOCK_TIMESTAMP,
      TX_ID,
      AMOUNT * -1,
      MINT
    from
      solana.core.fact_transfers
    where
      BLOCK_TIMESTAMP >= TIMESTAMP '2022-05-15'
      and TX_from = 'Ffbor3Zx46oGPK59S7drZjcTSt8mygZGWc5qkcHLPtWV'
  ),
  all_team_usdc_profit_tx as (
    select
      BLOCK_TIMESTAMP,
      TX_ID,
      AMOUNT * CLOSE as AMOUNT,
      MINT
    from
      solana.core.fact_transfers as token_transfers
      INNER join solana.core.ez_token_prices_hourly as token_prices on (
        token_prices.TOKEN_ADDRESS = token_transfers.MINT
        and date_trunc('hour', token_transfers.BLOCK_TIMESTAMP) = token_prices.RECORDED_HOUR
      )
    where
      BLOCK_TIMESTAMP >= TIMESTAMP '2022-05-15'
      and TX_from = 'Ffbor3Zx46oGPK59S7drZjcTSt8mygZGWc5qkcHLPtWV'
  ),
  all_team_profit_tx as (
    (
      select
        *
      from
        all_incoming_team_profit_tx
    )
    UNION
    (
      select
        *
      from
        all_outgoing_team_profit_tx
    )
  ),
  all_sol_tx as (
    select
      date_trunc('DAY', BLOCK_TIMESTAMP) as DAY,
      MINT,
      sum(AMOUNT) as DAILY_INCOMING_AMOUNT
    from
      all_team_profit_tx
    where
      MINT = 'So11111111111111111111111111111111111111112'
    group by
      DAY,
      MINT
  ),
  all_sol_tx_corrected as (
    select
      to_timestamp(cte_date.date_rec) as DAY,
      'So11111111111111111111111111111111111111112' as MINT,
      case
        when s.DAILY_INCOMING_AMOUNT is null then 0
        else s.DAILY_INCOMING_AMOUNT
      end as DAILY_INCOMING_AMOUNT
    from
      cte_date
      left outer join all_sol_tx s on s.DAY = date_trunc('DAY', cte_date.date_rec)
  ),
  all_sol_cumulative as (
    select
      DAY,
      sum(DAILY_INCOMING_AMOUNT) over (
        order by
          DAY asc rows between unbounded preceding
          and current row
      ) as CUMULATIVE_AMOUNT,
      'SOL' as Token
    from
      all_sol_tx_corrected
  ),
  all_usdc_tx as (
    select
      date_trunc('DAY', BLOCK_TIMESTAMP) as DAY,
      sum(AMOUNT) as DAILY_INCOMING_AMOUNT
    from
      all_team_usdc_profit_tx
    group by
      DAY
  ),
  all_usdc_tx_corrected as (
    select
      to_timestamp(cte_date.date_rec) as DAY,
      case
        when s.DAILY_INCOMING_AMOUNT is null then 0
        else s.DAILY_INCOMING_AMOUNT
      end as DAILY_INCOMING_AMOUNT
    from
      cte_date
      left outer join all_usdc_tx s on s.DAY = date_trunc('DAY', cte_date.date_rec)
  ),
  all_usdc_cumulative as (
    select
      DAY,
      sum(DAILY_INCOMING_AMOUNT) over (
        order by
          DAY asc rows between unbounded preceding
          and current row
      ) as CUMULATIVE_AMOUNT,
      'USDC' as Token
    from
      all_usdc_tx_corrected
  ),
  all_gmt_tx as (
    select
      date_trunc('DAY', BLOCK_TIMESTAMP) as DAY,
      MINT,
      sum(AMOUNT) as DAILY_INCOMING_AMOUNT
    from
      all_team_profit_tx
    where
      MINT = '7i5KKsX2weiTkry7jA4ZwSuXGhs5eJBEjY8vVxR4pfRx'
    group by
      DAY,
      MINT
  ),
  all_gmt_tx_corrected as (
    select
      to_timestamp(cte_date.date_rec) as DAY,
      '7i5KKsX2weiTkry7jA4ZwSuXGhs5eJBEjY8vVxR4pfRx' as MINT,
      case
        when s.DAILY_INCOMING_AMOUNT is null then 0
        else s.DAILY_INCOMING_AMOUNT
      end as DAILY_INCOMING_AMOUNT
    from
      cte_date
      left outer join all_gmt_tx s on s.DAY = date_trunc('DAY', cte_date.date_rec)
  ),
  all_gmt_cumulative as (
    select
      DAY,
      sum(DAILY_INCOMING_AMOUNT) over (
        order by
          DAY asc rows between unbounded preceding
          and current row
      ) as CUMULATIVE_AMOUNT,
      'GMT' as Token
    from
      all_gmt_tx_corrected
  ),
  final_table as (
    select
      all_sol_cumulative.DAY,
      all_sol_cumulative.CUMULATIVE_AMOUNT as SOL,
      all_gmt_cumulative.CUMULATIVE_AMOUNT as GMT,
      all_usdc_cumulative.CUMULATIVE_AMOUNT as USDC
    from
      all_sol_cumulative left outer join all_usdc_cumulative on all_sol_cumulative.DAY = all_usdc_cumulative.DAY
      left outer join all_gmt_cumulative on all_sol_cumulative.DAY = all_gmt_cumulative.DAY
  )
select
  *
from
  final_table


with e as (
    select distinct e.block_timestamp
    , e.tx_id
    , e.program_id
    , e.signers[0]::string as signer
    from solana.core.fact_events e
    join solana.core.fact_transfers t
        on t.block_timestamp = e.block_timestamp
        and t.tx_id = e.tx_id
    where e.block_timestamp >= CURRENT_DATE - 3
        and t.block_timestamp >= CURRENT_DATE - 3
        and t.tx_to = 'GFDko6sYw96a3nUmPqNLZwq7YstQ1u1SNUaVhnoCPW7o'
)
select e.program_id
, t.mint
, sum(t.amount) as amount
, COUNT(distinct e.tx_id) as n_tx
, COUNT(distinct e.signer) as n_signers
from solana.core.fact_transfers t
join e
    on t.block_timestamp = e.block_timestamp
    and t.tx_id = e.tx_id
where t.block_timestamp >= CURRENT_DATE - 3
    and t.tx_to = 'GFDko6sYw96a3nUmPqNLZwq7YstQ1u1SNUaVhnoCPW7o'
group by 1, 2
order by 3 desc


-- which addresses are related to the program id
select t.tx_to
, t.mint
, COUNT(distinct t.tx_id) as n_tx
, COUNT(distinct e.signers[0]::string) as n_signers
, sum(amount) as amount
from solana.core.fact_events e
join solana.core.fact_transfers t
    on t.block_timestamp = e.block_timestamp
    and t.tx_id = e.tx_id
where e.block_timestamp >= CURRENT_DATE - 30
    and t.block_timestamp >= CURRENT_DATE - 30
    and e.program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
group by 1, 2
order by 3 desc


with t0 as (
    select t.tx_to
    , t.tx_id
    , SPLIT(SPLIT(tx.log_messages::string, 'Program log: Instruction: ')[1]::string, '"')[0]::string as log_message
    -- , COUNT(distinct e.tx_id) as n_tx
    -- , COUNT(distinct e.signers[0]::string) as n_signers
    , sum(amount) as amount
    from solana.core.fact_events e
    join solana.core.fact_transfers t
        on t.block_timestamp = e.block_timestamp
        and t.tx_id = e.tx_id
    join solana.core.fact_transactions tx
        on tx.block_timestamp = e.block_timestamp
        and tx.tx_id = e.tx_id
    where e.block_timestamp >= CURRENT_DATE - 10
        and t.block_timestamp >= CURRENT_DATE - 10
        and tx.block_timestamp >= CURRENT_DATE - 10
        and e.program_id in (
            '2RoWfh3xNtwGGpoXmXAWbVgiqqYiNEb9QrPYshQjRDGu'
        )
        and t.mint like 'So%'
        and amount > 0
        and tx.succeeded
    group by 1, 2, 3
), t1 as (
    select *
    , sum(amount) over (partition by tx_id) as tot_amt
    , ROUND(100 * amount / tot_amt, 1) as pct
    from t0
)
select *
from t1
order by tx_id
LIMIT 10000


-- which addresses are related to the program id
with t0 as (
    select t.tx_to
    , t.mint
    , t.tx_id
    , e.signers[0]::string as signer
    , MAX(case
        when e.program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP' then 1 else 0 end
    ) as is_program
    , sum(amount) as amount
    from solana.core.fact_events e
    join solana.core.fact_transfers t
        on t.block_timestamp = e.block_timestamp
        and t.tx_id = e.tx_id
    where e.block_timestamp >= CURRENT_DATE - 30
        and t.block_timestamp >= CURRENT_DATE - 30
        and t.tx_to in (
            'Fz2tay9wmAhaz1GuEETorL55LBMECaT85DX9Tj5zqQKb'
            , 'feegKBq3GAfqs9G6muPjdn8xEEZhALLTr2xsigDyxnV'
        )
    group by 1, 2, 3, 4
), t1 as (
    select tx_to
    , mint
    , is_program
    , COUNT(distinct tx_id) as n_tx
    , COUNT(distinct signer) as n_signers
    , sum(amount) as amount
    from t0
    group by 1, 2, 3
)
select *
, sum(n_tx) over (partition by tx_to) as tot_tx
, n_tx / tot_tx as pct_tx
from t1
order by pct_tx desc

select *
from solana.defi.fact_swaps
where block_timestamp >= CURRENT_DATE - 1
    and swap_program ILIKE 'orca%'
LIMIT 1000



select *
from solana.core.fact_decoded_instructions
where block_timestamp >= CURRENT_DATE - 7
    and program_id = 'GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY'
    and tx_id = '56Dz4xmfUZSLXv3daP8cXbEFzyGEEaunysqvc512EiWHspM6Ag7RM2UWaQTYw3rYy1p51n9tiHM8f7aFzMJpsVvz'
LIMIT 1000


select *
from solana.defi.fact_swaps s
join solana.core.fact_transactions t
    on t.block_timestamp = s.block_timestamp
    and t.tx_id = s.tx_id
where s.block_timestamp >= CURRENT_DATE - 1
    and t.block_timestamp >= CURRENT_DATE - 1
    and s.swap_program ILIKE 'orca%'
    and s.succeeded
LIMIT 1000

with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= current_date - 1
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= current_date - 1
        and is_imputed = FALSE
    group by 1, 2
)
select t.tx_to
, t.mint
, s.swap_program
, COUNT(distinct s.swapper) as n_swappers
, COUNT(distinct s.tx_id) as n_tx
, sum(t.amount * coalesce(p0.price, p1.price, 0)) as usd_amount
from solana.defi.fact_swaps s
join solana.core.fact_transfers t
    on t.block_timestamp = s.block_timestamp
    and t.tx_id = s.tx_id
left join p0
    on p0.hour = date_trunc('hour', t.block_timestamp)
    and p0.mint = t.mint
left join p1
    on p1.date = t.block_timestamp::date
    and p1.mint = t.mint
where s.block_timestamp >= CURRENT_DATE - 1
    and t.block_timestamp >= CURRENT_DATE - 1
    -- and s.swap_program ILIKE 'orca%'
    and s.succeeded
    and t.tx_to <> s.swapper
group by 1, 2, 3
order by 4 desc

select *
from solana.core.fact_events e
join solana.core.fact_transactions t
    on t.
where program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'




select *
from solana.defi.fact_swaps s
LIMIT 100

with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= CURRENT_DATE - 10
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= CURRENT_DATE - 10
        and is_imputed = FALSE
    group by 1, 2
), mints as (
    select swap_to_mint as mint
    , COUNT(distinct swapper) as n_swappers
    from solana.defi.fact_swaps s
    where block_timestamp >= CURRENT_DATE - 30
    group by 1
), t0 as (
    select s.swap_program
    , s.tx_id
    , s.swapper
    , s.block_timestamp
    , case when coalesce(tm.n_swappers, 0) > coalesce(fm.n_swappers, 0) then s.swap_to_mint else swap_from_mint end as mint
    , case when coalesce(tm.n_swappers, 0) > coalesce(fm.n_swappers, 0) then s.swap_to_amount else swap_from_amount end as amount
    from solana.defi.fact_swaps s
    left join mints tm
        on tm.mint = s.swap_to_mint
    left join mints fm
        on fm.mint = s.swap_from_mint
    where s.block_timestamp >= CURRENT_DATE - 10
        and s.succeeded
)
select swap_program
-- , tx_id
, COUNT(distinct t0.swapper) as n_swappers
, COUNT(distinct t0.tx_id) as n_tx
, sum(amount * coalesce(p0.price, p1.price, 0)) as usd_amount
from t0
left join p0
    on p0.hour = date_trunc('hour', t0.block_timestamp)
    and p0.mint = t0.mint
left join p1
    on p1.date = t0.block_timestamp::date
    and p1.mint = t0.mint
-- group by 1, 2
-- order by 5 desc
group by 1
order by 4 desc


select *
from solana.core.dim_labels
where address in (
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
    , 'routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS'
    , 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK'
)


with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= CURRENT_DATE - 1
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= CURRENT_DATE - 1
        and is_imputed = FALSE
    group by 1, 2
), mints as (
    select swap_to_mint as mint
    , COUNT(distinct swapper) as n_swappers
    from solana.defi.fact_swaps s
    where block_timestamp >= CURRENT_DATE - 1
    group by 1
), e as (
    select distinct block_timestamp
    , tx_id
    , program_id
    from solana.core.fact_events
    where block_timestamp >= CURRENT_DATE - 1
        and program_id in (
            '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
            , 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK'
            , 'routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS'
        )
), t0 as (
    select e.*
    , s.*
    , t.tx_from
    , t.tx_to
    , t.amount
    , swap_to_amount * coalesce(p0.price, p1.price, 0) as swap_to_amount_usd
    , swap_from_amount * coalesce(p0f.price, p1f.price, 0) as swap_from_amount_usd
    , e.program_id
    , case when coalesce(tm.n_swappers, 0) > coalesce(fm.n_swappers, 0) then s.swap_to_mint else swap_from_mint end as mint
    , case when coalesce(tm.n_swappers, 0) > coalesce(fm.n_swappers, 0) then s.swap_to_amount else swap_from_amount end as amount
    from e
    left join solana.defi.fact_swaps s
        on s.block_timestamp = e.block_timestamp
        and s.tx_id = e.tx_id
    left join solana.core.fact_transfers t
        on t.block_timestamp = e.block_timestamp
        and t.tx_id = e.tx_id
    left join mints tm
        on tm.mint = s.swap_to_mint
    left join mints fm
        on fm.mint = s.swap_from_mint
    left join p0
        on p0.hour = date_trunc('hour', s.block_timestamp)
        and p0.mint = s.swap_to_mint
    left join p1
        on p1.date = s.block_timestamp::date
        and p1.mint = s.swap_to_mint
    left join p0 p0f
        on p0f.hour = date_trunc('hour', s.block_timestamp)
        and p0f.mint = s.swap_from_mint
    left join p1 p1f
        on p1f.date = s.block_timestamp::date
        and p1f.mint = s.swap_from_mint
    where s.block_timestamp >= CURRENT_DATE - 1
        and s.succeeded
)
select *
from t0
order by GREATEST(swap_to_amount_usd, swap_from_amount) desc
LIMIT 1000


with t0 as (
    select distinct block_timestamp
    , tx_id

)
select *
from solana.core.fact_decoded_instructions
where block_timestamp >= current_date - 1
    and tx_id = 'T8bVTAWCgo1b2tW3yerHE8j9b3K3EemfBCUDbPZu5vmJyqJYSGUkDDtboJ4rsuDbustYJbyw5671yoiEByzwYMX'

select tx_from
, COUNT(distinct tx_to)
from solana.core.fact_transfers
where block_timestamp >= CURRENT_DATE - 7
    and mint = 'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn'
group by 1
order by 2 desc
LIMIT 1000

select *
from solana.defi.fact_stake_pool_actions
where block_timestamp >= CURRENT_DATE - 7
    and stake_pool = 'Jito4APyf642JPZPx3hGc6WWJ8zPKtRbRs4P815Awbb'
    and succeeded
LIMIT 10000

with p as (
    select avg(close) as price
    from solana.price.ez_token_prices_hourly
    where token_address like 'So111%'
        and recorded_hour >= CURRENT_DATE - 7
)
select sum(case when action like 'deposit%' then amount else -amount end * p.price) * POWER(10, -9) * 0.04 * 7/ 365 as staking_commission
, sum(case when block_timestamp >= DATEADD('hours', 24 * -7, CURRENT_TIMESTAMP) and action = 'deposit' then amount else 0 end * p.price) * 0.001 * POWER(10, -9) as withdrawal_fees
, staking_commission + withdrawal_fees as project_revenue
from solana.defi.fact_stake_pool_actions
join p on TRUE
where stake_pool = 'Jito4APyf642JPZPx3hGc6WWJ8zPKtRbRs4P815Awbb'
    and succeeded


select *
from solana.defi.fact_stake_pool_actions
where block_timestamp >= CURRENT_DATE - 3
    and stake_pool = 'Jito4APyf642JPZPx3hGc6WWJ8zPKtRbRs4P815Awbb'
    and succeeded
order by tx_id
LIMIT 100000

select distinct swap_program
from solana.defi.fact_swaps
where swap_program like 'jupiter%'




with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= '2021-01-01'
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= '2021-01-01'
        and is_imputed = FALSE
    group by 1, 2
), mints as (
    select swap_to_mint as mint
    , COUNT(distinct swapper) as n_swappers
    from solana.defi.fact_swaps s
    where block_timestamp >= DATEADD('hours', -24, CURRENT_TIMESTAMP)
    group by 1
), t0 as (
    select s.swapper
    , s.tx_id
    , s.swap_program
    , case when coalesce(tm.n_swappers, 0) > coalesce(fm.n_swappers, 0) then s.swap_to_mint else swap_from_mint end as swap_mint
    , case when coalesce(tm.n_swappers, 0) > coalesce(fm.n_swappers, 0) then s.swap_to_amount else swap_from_amount end as amount
    , amount * coalesce(p0.price, p1.price, 0) as amount_usd
    from solana.defi.fact_swaps s
    left join mints tm
        on tm.mint = s.swap_to_mint
    left join mints fm
        on fm.mint = s.swap_from_mint
    left join mints m
        on m.mint = swap_mint
    left join p0
        on p0.hour = date_trunc('hour', s.block_timestamp)
        and p0.mint = swap_mint
    left join p1
        on p1.date = s.block_timestamp::date
        and p1.mint = swap_mint
    where s.block_timestamp >= '2021-01-01'
        and s.succeeded
        and s.swap_program like 'jup%'
        and s.swap_from_amount > 0
        and s.swap_to_amount > 0
), t1 as (
    select swap_program
    , COUNT(1) as n_tx
    , sum(amount_usd) as amount_usd
    from t0
    group by 1
)
select *
from t1
qualify(
    row_number() over (order by n_tx desc, amount_usd desc, swapper) <= 100000
)


select COUNT(distinct swapper)
from solana.defi.fact_swaps
where swap_program like 'jup%'


Page 1: https://api.flipsidecrypto.com/api/v2/queries/a8a7da5f-d732-4ff5-906b-95efe9797ef3/data/latest
Page 2: https://api.flipsidecrypto.com/api/v2/queries/d6602ae0-b3d9-4b8e-9fb0-0e52574c27c6/data/latest
Page 3: https://api.flipsidecrypto.com/api/v2/queries/6db4f253-806c-4055-a5f0-993f2efaacc0/data/latest
Page 4: https://api.flipsidecrypto.com/api/v2/queries/e79f2e57-80bb-4199-9509-d3cee48a7289/data/latest
Page 5: https://api.flipsidecrypto.com/api/v2/queries/e4add666-bf95-4894-abb8-3ea598185502/data/latest
Page 6: https://api.flipsidecrypto.com/api/v2/queries/1ee91b39-3442-4a79-b592-c188ea267bab/data/latest
Page 7: https://api.flipsidecrypto.com/api/v2/queries/1e602137-8d0a-4e7f-9d48-4f89113076bb/data/latest
Page 8: https://api.flipsidecrypto.com/api/v2/queries/98a4777d-df22-4aff-808f-06e03e4479ad/data/latest
Page 9: https://api.flipsidecrypto.com/api/v2/queries/8155ffc2-4339-4cf3-b9f7-9169f3c52034/data/latest
Page 10: https://api.flipsidecrypto.com/api/v2/queries/c8b3cba3-731c-4e5f-bb2d-e4a70ad4b1fc/data/latest

select *
from bi_analytics.velocity_app_prod.query_runs r
join bi_analytics.velocity_app_prod.queries q
    on q.id = r.query_id
where q.created_by_id = 'e87e2545-c55f-443d-903d-443504346bc6'
order by q.created_at desc
LIMIT 100000

select q.*
from bi_analytics.velocity_app_prod.query_runs r
join bi_analytics.velocity_app_prod.queries q
    on q.id = r.query_id
where q.created_by_id = 'e87e2545-c55f-443d-903d-443504346bc6'
    and execution_type = 'REFRESH'
    -- and q.slug
    and q.id = 'a8385646-d21d-441e-9fec-9b9d4f2a1dc4'
order by r.updated_at desc
LIMIT 100

select program_id
, COUNT(1)
from solana.core.fact_events
where block_timestamp >= current_date - 10
    and program_id ILIKE 'jup%'
group by 1
order by 2 desc


select q.*
from bi_analytics.velocity_app_prod.queries q
where q.id = 'a8385646-d21d-441e-9fec-9b9d4f2a1dc4'
order by r.updated_at desc
LIMIT 100

select *
from bi_analytics.velocity_app_prod.queries
where created_by_id = 'e87e2545-c55f-443d-903d-443504346bc6'
LIMIT 19

select *
from bi_analytics.velocity_app_prod.users
where username = 'Spot-Wiggum'


with chain0 as (
  select
    d.id as dashboard_id,
    case
      when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
      or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
      or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
      or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%' then 'Axelar'
      else INITCAP(t.name)
    end as chain,
    COUNT(distinct q.id) as n_queries
  from
    bi_analytics.velocity_app_prod.users u
    join bi_analytics.velocity_app_prod.dashboards d on d.created_by_id = u.id
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_tags dtq on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q on dtq.B = q.id
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t on qtt.B = t.id
    and t.type = 'project'
  group by
    1,
    2
),
chain as (
  select
    *,
    row_number() over (
      partition by dashboard_id
      order by
        case
          when chain in (
            'Avalanche',
            'Axelar',
            'Flow',
            'Near',
            'Sei',
            'Solana'
          ) then 1
          else 2
        end,
        n_queries desc,
        chain
    ) as rn
  from
    chain0
),
t0 as (
  select
    coalesce(m.user_id, d.created_by_id) as user_id,
    d.profile_id as profile_id,
    coalesce(tu.username, u.username) as username,
    dr.currency,
    d.id as dashboard_id,
    p.type,
    coalesce(c.chain, 'Polygon') as ecosystem,
    row_number() over (
      order by
        dr.ranking_trending
    ) as current_rank,
    coalesce(u.role, '') = 'internal'
    or u.username in (
      'Polaris_9R',
      'dsaber',
      'flipsidecrypto',
      'metricsdao',
      'drethereum',
      'Orion_9R',
      'sam',
      'forgash',
      'danner',
      'charliemarketplace',
      'theericstone'
    ) as internal_user
  from
    bi_analytics.content_rankings.dashboard_rankings dr
    join bi_analytics.velocity_app_prod.dashboards d on d.id = dr.dashboard_id
    left join bi_analytics.velocity_app_prod.profiles p on p.id = d.profile_id
    left join bi_analytics.velocity_app_prod.teams t on t.profile_id = p.id
    left join bi_analytics.velocity_app_prod.members m on t.id = m.team_id
    and m.role = 'owner'
    left join bi_analytics.velocity_app_prod.users tu -- changed this join to the team owner
    on tu.id = m.user_id
    join bi_analytics.velocity_app_prod.users u on u.id = d.created_by_id
    left join chain c on c.dashboard_id = dr.dashboard_id
    and c.rn = 1
    where internal_user = FALSE
)
select
  user_id,
  username,
  dashboard_id,
  profile_id,
  type,
  case
    when ecosystem in (
      'Solana',
      'Avalanche',
      'Axelar',
      'Flow',
      'Near',
      'Sei'
    ) then ecosystem
    else 'Polygon'
  end as ecosystem,
  case
    when ecosystem = 'Solana' then 'SOL'
    when ecosystem = 'Avalanche' then 'AVAX'
    when ecosystem = 'Axelar' then 'AXL'
    when ecosystem = 'Flow' then 'FLOW'
    when ecosystem = 'Near' then 'NEAR'
    when ecosystem = 'Sei' then 'SEI'
    else 'USDC'
  end as currency,
  case
    when current_rank <= 10 then 1.5
    else 1
  end as base_amount,
  case
    when ecosystem in ('Flow', 'Near', 'Sei') then 1.5
    else 1
  end as boost,
  case
    when internal_user = false then base_amount * boost
    else 0
  end as amount
from
  t0
where
  current_rank <= 30
order by
  7 desc



select d.id as dashboard_id
, case when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
    or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
    or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
    or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%'
    then 'Axelar' else INITCAP(t.name) end as chain
, COUNT(distinct q.id) as n_queries
from bi_analytics.velocity_app_prod.users u
join bi_analytics.velocity_app_prod.dashboards d
    on d.created_by_id = u.id
join bi_analytics.velocity_app_prod._dashboards_to_queries dtq
    on d.id = dtq.A
join bi_analytics.velocity_app_prod.queries q
    on dtq.B = q.id
join bi_analytics.velocity_app_prod._queries_to_tags qtt
    on q.id = qtt.A
join bi_analytics.velocity_app_prod.tags t
    on qtt.B = t.id
    and t.type = 'project'
group by 1, 2



select distinct d.id as dashboard_id
, dtq.B
, q.id as query_id
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.velocity_app_prod._dashboards_to_queries dtq
    on d.id = dtq.A
left join bi_analytics.velocity_app_prod.queries q
    on dtq.B = q.id
where d.created_at::date = '2023-10-01'
    and d.id = '7987be2c-ced2-44f5-9259-ae9d3f09947a'


select distinct d.id as dashboard_id
, dtq.B
, q.id as query_id
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.velocity_app_prod._dashboards_to_queries dtq
    on d.id = dtq.A
left join bi_analytics.velocity_app_prod.queries q
    on dtq.B = q.id
where d.created_at::date = '2023-10-01'
    and d.id = '7987be2c-ced2-44f5-9259-ae9d3f09947a'

select distinct d.id as dashboard_id
, dtq.*
, t.*
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_tags dtq
    on d.id = dtq.A
join bi_analytics.velocity_app_prod.tags t
    on dtq.B = t.id
    and t.type = 'project'
    and t.name = 'thorchain'
where d.created_at::date = '2023-10-01'
    and d.id = '7987be2c-ced2-44f5-9259-ae9d3f09947a'

with chain0 as (
  select
    d.id as dashboard_id,
    case
      when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
      or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
      or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
      or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%' then 'Axelar'
      else INITCAP(t.name)
    end as chain,
    COUNT(distinct q.id) as n_queries
  from
    bi_analytics.velocity_app_prod.users u
    join bi_analytics.velocity_app_prod.dashboards d on d.created_by_id = u.id
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q on dtq.B = q.id
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t on qtt.B = t.id
    and t.type = 'project'
  group by
    1,
    2
),
chain as (
  select
    *,
    row_number() over (
      partition by dashboard_id
      order by
        case
          when chain in (
            'Avalanche',
            'Axelar',
            'Flow',
            'Near',
            'Sei',
            'Solana'
          ) then 1
          else 2
        end,
        n_queries desc,
        chain
    ) as rn
  from
    chain0
),
t0 as (
  select
    coalesce(m.user_id, d.created_by_id) as user_id,
    d.profile_id as profile_id,
    coalesce(tu.username, u.username) as username,
    dr.currency,
    d.id as dashboard_id,
    p.type,
    coalesce(c.chain, 'Polygon') as ecosystem,
    row_number() over (
      order by
        dr.ranking_trending
    ) as current_rank,
    coalesce(u.role, '') = 'internal'
    or u.username in (
      'Polaris_9R',
      'dsaber',
      'flipsidecrypto',
      'metricsdao',
      'drethereum',
      'Orion_9R',
      'sam',
      'forgash',
      'danner',
      'charliemarketplace',
      'theericstone'
    ) as internal_user
  from
    bi_analytics.content_rankings.dashboard_rankings dr
    join bi_analytics.velocity_app_prod.dashboards d on d.id = dr.dashboard_id
    left join bi_analytics.velocity_app_prod.profiles p on p.id = d.profile_id
    left join bi_analytics.velocity_app_prod.teams t on t.profile_id = p.id
    left join bi_analytics.velocity_app_prod.members m on t.id = m.team_id
    and m.role = 'owner'
    left join bi_analytics.velocity_app_prod.users tu -- changed this join to the team owner
    on tu.id = m.user_id
    join bi_analytics.velocity_app_prod.users u on u.id = d.created_by_id
    left join chain c on c.dashboard_id = dr.dashboard_id
    and c.rn = 1
)
select
  user_id,
  username,
  dashboard_id,
  profile_id,
  type,
  case
    when ecosystem in (
      'Solana',
      'Avalanche',
      'Axelar',
      'Flow',
      'Near',
      'Sei'
    ) then ecosystem
    else 'Polygon'
  end as ecosystem,
  case
    when ecosystem = 'Solana' then 'SOL'
    when ecosystem = 'Avalanche' then 'AVAX'
    when ecosystem = 'Axelar' then 'AXL'
    when ecosystem = 'Flow' then 'FLOW'
    when ecosystem = 'Near' then 'NEAR'
    when ecosystem = 'Sei' then 'SEI'
    else 'USDC'
  end as currency,
  case
    when current_rank <= 10 then 1.5
    else 1
  end as base_amount,
  case
    when ecosystem in ('Flow', 'Near', 'Sei') then 1.5
    else 1
  end as boost,
  case
    when internal_user = false then base_amount * boost
    else 0
  end as amount
from
  t0
where
  current_rank <= 30
order by
  7 desc



select coalesce(l.label, 'Other') as smart_contract
, COUNT(1) as n_tx
, COUNT(distinct from_address) as n_signers
-- , t.* 
from ethereum.core.fact_transactions t
left join ethereum.core.dim_labels l
    on LOWER(l.address) = LOWER(t.to_address)
where t.block_timestamp >= CURRENT_DATE - 1
    and status = 'SUCCESS'
group by 1
order by 2 desc


select *
from solana.core.fact_events
where block_timestamp >= CURRENT_DATE - 1
    and program_id = 'SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy'



with t0 as (
    select livequery.live.udf_api(
        'https://api.flipsidecrypto.com/api/v2/queries/768b662d-a670-4abd-882c-56cbeaee5e52/data/latest'
    ):data as data
)
select data[0]:project as project
, data[0]:project_revenue as project_revenue
, row_number() over (order by project_revenue desc) as rk
, concat(case when rk < 10 then '0' else '' end, rk, '. ', project) as label
from t0
order by rk

select * from solana.nft.dim_nft_metadata
where mint in (
    '6L3FNS1GWMAG9FACwB2HyLHTRNpnCprUMwEJzK5oMnoQ'
    , 'ACUA1b4cZASQpy9WZ2A4titUcbGucyHdM35EwgsQxrLj'
)
select *
from solana.silver.helius_nft_metadata
where mint in (
    '6L3FNS1GWMAG9FACwB2HyLHTRNpnCprUMwEJzK5oMnoQ'
    , 'ACUA1b4cZASQpy9WZ2A4titUcbGucyHdM35EwgsQxrLj'
)
-- , lateral flatten(
--     input => published:cells
-- ) c
LIMIT 100

with t0a as (
    select u.id as user_id
    , u.username
    , d.id as dashboard_id
    , coalesce(dr.ecosystem, '') as ecosystem
    , coalesce(dr.currency, '') as currency
    , d.title
    , coalesce(dr.dashboard_url, '') as dashboard_url
    , coalesce(dr.start_date_days_ago, 0) as start_date_days_ago
    , coalesce(dr.pct_twitter, 0) as pct_twitter
    , date_trunc('hour', dbt_updated_at) as hour
    , avg(hr.ranking_trending) as rk
    from bi_analytics.snapshots.hourly_dashboard_rankings hr
    left join bi_analytics.content_rankings.dashboard_rankings dr
        on dr.dashboard_id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where 1=1
        and coalesce(u.role, '') <> 'internal'
        and not u.username in (
            'Polaris_9R','dsaber','flipsidecrypto','metricsdao','drethereum','Orion_9R','sam','forgash','nftchance__','danner'
        )
        and hr.ranking_trending <= 40
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
), t0 as (
    select user_id
    , username
    , dashboard_id
    , ecosystem
    , currency
    , title
    , dashboard_url
    , start_date_days_ago
    , pct_twitter
    , hour
    , row_number() over (partition by hour order by rk, dashboard_id) as rk
    from t0a
    -- group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
), t1 as (
    select user_id
    , username
    , t0.dashboard_id
    , title
    , dashboard_url
    , ecosystem
    , currency
    , start_date_days_ago
    , pct_twitter
    , MIN(rk) as max_ranking
    , sum(case when rk <= 8 then 1 else 0 end) as n_hours_in_top_8
    from t0
    where rk <= 8 or (hour >= '2023-11-01' and rk <= 30)
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)
select COUNT(distinct user_id)
from t1



select COUNT(distinct q.created_by_id) as n_users
, COUNT(distinct q.id) as n_queries
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq on q.id = qtt.A
join bi_analytics.velocity_app_prod.query_runs r
    on r.query_id = q.id
    and r.status = 'finished'
-- where q.statement ILIKE '%solana.%'


with dashboards as (
    select distinct d.id as dashboard_id
    , d.latest_slug
    , u.username
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.velocity_app_prod._dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where q.statement ILIKE '%solana.%'
        and not u.username in (
            'ben-AMBBr7'
        )
), pages as (
    select id
    , user_id
    , context_page_tab_url
    , timestamp
    , anonymous_id
    , context_ip
    , case when context_page_referring_domain = 't.co' then 1 else 0 end as n_views_twitter
    from bi_analytics.gumby.pages
    UNION
    select id
    , user_id
    , context_page_tab_url
    , timestamp
    , anonymous_id
    , context_ip
    , case when context_page_referring_domain = 't.co' then 1 else 0 end as n_views_twitter
    from bi_analytics.flipside_app_prod.pages
), t0 as (
    select distinct d.dashboard_id
    , p.timestamp::date as date
    , d.username
    , coalesce(p.user_id, p.anonymous_id) as user_id
    from dashboards d
    join pages p
        on right(d.latest_slug, 6) = right(SPLIT(p.context_page_tab_url, '?')[0]::string, 6)
)
select 
date_trunc('month', date) as month
-- , username
, case when username in ('h4wk','marqu') then 1 else 0 end as is_ambassadors
, COUNT(1) as n_views
from t0
-- where is_ambassadors = 1
group by 1, 2

select *
from solana.core.fact_decoded_instructions
where program_id = 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
LIMIT 100

with t0 as (
    select distinct LOWER(h.address) as address
    , h.txId
    from bi_analytics.bronze.arprograms_hike h
    where result = 'VERIFIED'
        and blockchain = 'Avalanche'
        and trailheadId = 40
)
select t0.address
, *
from avalanche.core.fact_transaction t
join t0
    on t0.txId = t.tx_hash

select *
from avalanche.core.fact_transactions
where block_timestamp >= CURRENT_DATE - 3
    and ORIGIN_FUNCTION_SIGNATURE = '0x57ecfd28'


with bridges as (
    -- multichain addresses
select distinct address, address_name, project_name from avalanche.core.dim_labels where project_name = 'multichain' and address_name like '%any%'
union
    -- should cover allbridge, avalanche canonical, biconomy, celer, cerby, checkdot, debridge,
    -- layerzero, multichain, optics, rubic, spore, synapse, stargate, polynetwork, wormhole
select distinct address, address_name, project_name from crosschain.silver.address_labels where label_subtype = 'bridge' and blockchain = 'avalanche'

),
  to_bridge as (
  select distinct tx_hash
  from avalanche.core.ez_token_transfers
    left join bridges on avalanche.core.ez_token_transfers.TO_ADDRESS = bridges.address 
where 
TO_ADDRESS in (select address from bridges) and 
 BLOCK_TIMESTAMP >= current_date - 10
group by user_address, token_contract, bridge_name
    --and FROM_ADDRESS = '0x228406cecfeb7a478ef21fe415800f93019732ae'
),
from_bridge as (
  select distinct tx_hash
  from avalanche.core.ez_token_transfers
    left join bridges on avalanche.core.ez_token_transfers.FROM_ADDRESS = bridges.address 
where 
FROM_ADDRESS in (select address from bridges) and 
 BLOCK_TIMESTAMP >= current_date - 10
group by user_address, token_contract, bridge_name
),
  
  -- user_address | bridge_name | token_contract | token_symbol | n_in | n_out | in_token_volume | in_usd_volume | out_token_volume | out_usd_volume
allbridge as (
select tx_hash from to_bridge
UNIon select tx_hash from from_bridge
), t0 as (
    select distinct LOWER(h.address) as address
    , h.txId
    from bi_analytics.bronze.arprograms_hike h
    where result = 'VERIFIED'
        and blockchain = 'Avalanche'
        and trailheadId = 40
)
select t0.*
, case when a.tx_hash is null then 0 else 1 end as included
from t0
left join allbridge a
    on LOWER(a.tx_hash) = LOWER(t0.tx_hash)

to_address in (
    '0x8186359af5f57fbb40c6b14a588d2a59c0c29880','0xcd2e3622d483c7dc855f72e5eafadcd577ac78b4'
)





with t0 as (
    select q.created_by_id, MIN(q.created_at)::date as date
    from bi_analytics.velocity_app_prod.queries q
)
select date, count(1)
from t0
group by 1
order by 1


select *
from solana.core.ez_events_decoded
where block_timestamp >= current_date - 1
    and program_id = 'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu'
LIMIT 100


with t0 as (
    select distinct program_id
    from solana.core.ez_events_decoded
    where block_timestamp >= current_date - 1
), t1 as (
    select program_id
    , COUNT(distinct tx_id) as n_tx
    , COUNT(distinct signers[0]) as n_signers
    from solana.core.fact_events
    where block_timestamp >= CURRENT_DATE - 7
    group by 1
)
select case when t0.program_id is null then 0 else 1 end as has_program_id
, t1.*
from t1
left join t0
    on t0.program_id = t1.program_id
order by n_signers desc



select *
from crosschain.core.fact_token_prices_hourly
where recorded_hour >= '2022-12-24'
    and id = 'bonk'
    and provider = 'coingecko'
LIMIT 100




with t0 as (
    select *
	from flipside_prod_db.bronze.prod_address_label_sink_291098491 dbc 
    where _inserted_timestamp >= '2023-11-05'
        and record_metadata:topic::string = 'twitter-likes'
), t1 as (
    select t0._inserted_timestamp
    , c.value:user_id::string as user_id
    , c.value:username::string as username
    , c.value:user_followers::int as user_followers
    , c.value:tweet_id::string as tweet_id
    from t0
    , lateral flatten(
        input => record_content
    ) c
), t2 as (
    select tweet_id
    , user_id
    , MAX(user_followers) as user_followers
    from t1
    group by 1, 2
), tw0 as (
select tweet_id
, user_id
, user_followers
from t2

)
, tw1 as (
    select distinct l.tweet_id
    , l.user_id
    , l.user_followers
    , d.id as dashboard_id
    , t.created_at
    , d.title
    , u.username
    from bi_analytics.twitter.tweet t
	-- join bi_analytics.twitter.likes l
	join tw0 l
		on l.tweet_id = t.conversation_id
	join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
	join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
	-- where t.created_at >= CURRENT_DATE - 14
	qualify(
		-- double check to make sure we only get 1 for each user-tweet-like
		row_number() over (partition by l.tweet_id, l.user_id order by t.created_at desc) = 1
	)
)
, tot as (
	-- total number of tweets a user has liked
    select tw1.user_id
    , COUNT(distinct tw1.tweet_id) as n_likes
    from tw1
    group by 1
)
, wt as (
	-- the more they've liked, the lower their likes are weighted (e.g. people spam-liking each other's tweets)
    select user_id
    , GREATEST(0.2, POWER(0.975, GREATEST(0, n_likes - 3))) as wt
    from tot
)
, twitter_likes as (
	select tw1.dashboard_id, tw1.title, tw1.created_at
	, sum(
		case when tw1.user_followers < 100 then 0
		else LOG(tw1.user_followers, 10) end
        * wt.wt
	) as like_wt
    from tw1
    join wt
        on wt.user_id = tw1.user_id
	group by 1, 2, 3
)
-- select * from twitter_likes

, twitter as (
	select dashboard_id
    , title
	, MAX(POWER(0.95, (DATEDIFF('minutes', created_at, CURRENT_TIMESTAMP) / 60)) * POWER(like_wt, 0.5)) as twitter_wt_0
	, MAX(POWER(0.975, (DATEDIFF('minutes', created_at, CURRENT_TIMESTAMP) / 60)) * POWER(like_wt, 0.5)) as twitter_wt_1
	, MAX(POWER(0.9985, (DATEDIFF('minutes', created_at, CURRENT_TIMESTAMP) / 60)) * POWER(like_wt, 0.5)) as twitter_wt_2
	, MAX(POWER(like_wt, 0.35)) as twitter_wt_3
	, MAX(0) as impression_count
	from twitter_likes
    group by 1, 2
)
select *
from twitter
order by twitter_wt_1 desc





with t0 as (
    select distinct program_id
    from solana.core.ez_events_decoded
    where block_timestamp >= CURRENT_DATE - 1
), t1 as (
    select tx_id
    , sum(amount) as amount
    from solana.core.fact_transfers
    where block_timestamp >= CURRENT_DATE - 1
        and mint like 'So111%'
    group by 1
), t2 as (
    select distinct program_id
    , tx_id
    , signers[0]::string as signer
    from solana.core.fact_events
    where block_timestamp >= CURRENT_DATE - 1
), t3 as (
    select program_id
    , sum(amount) as amount
    , COUNT(distinct tx_id) as n_tx
    , COUNT(distinct signer) as n_signers
    from t2
    left join t1
        on t1.tx_id = t2.tx_id
    group by 1
)
select case when t0.program_id is null then 0 else 1 end as has_program_id
, t1.*
from t1
left join t0
    on t0.program_id = t1.program_id
order by n_signers desc


select *
from solana.core.fact_decoded_instructions
where block_timestamp >= DATEADD('hours', -5, CURRENT_TIMESTAMP)
qualify(
    row_number() over (partition by program_id order by tx_id) <= 3
)
LIMIT 10000

select u.username, q.*
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.users u
    on u.id = q.created_by_id
where statement ILIKE '%mdVo394XANGMrVXZCVAaX3AMHYvtTxXwg1sQmDSY1W1%'


select *
from bi_analytics.twitter.tweet t
left join bi_analytics.twitter.user tu
    on tu.id = t.user_id
where conversation_id = '1724142874004988201'


select u.username, u.created_at::date as date, p.*
from bi_analytics.gumby.pages p
left join bi_analytics.velocity_app_prod.users u
    on u.id = p.user_id
where p.context_page_tab_url ILIKE '%er8M_UuatE8K/jupiter-events%'
order by p.timestamp desc

with t0 as (
    select MAX(created_at) as created_at
    from bi_analytics.twitter.tweet t
    where platform = 'Dune'
)
select t.*
, u.user_name
, created_at::date as date
from bi_analytics.twitter.tweet t
join t0
    on t.created_at <= t0.created_at
left join bi_analytics.twitter.user u
    on u.id = t.user_id
qualify(
    row_number() over (partition by t.conversation_id order by t.impression_count desc) = 1
)

select d.title
, d.id
, d.latest_slug
, dr.ranking_trending
from bi_analytics.twitter.likes l
join bi_analytics.twitter.tweet t
join bi_analytics.velocity_app_prod.dashboards d
    on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
left join bi_analytics.content_rankings.dashboard_rankings dr
    on dr.dashboard_id = d.id
where u.username ILIKE 'sociocrypto'
order by ranking_trending


select MIN(block_timestamp)
from solana.core.fact_events
where program_id = 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'

with p0 as (
    select date_trunc('hour', hour) as hour
    , avg(price) as price
    from avalanche.price.ez_hourly_token_prices p
    where recorded_hour::date >= '2021-01-01'
        and is_imputed = FALSE
        and symbol = 'WAVAX'
    group by 1, 2
), p1 as (
    select date_trunc('day', recorded_hour) as date
    , avg(price) as price
    from avalanche.price.ez_hourly_token_prices p
    where hour >= '2023-08-01'
        and is_imputed = FALSE
        and symbol = 'WAVAX'
    group by 1, 2
)
select t.*
, coalesce(p0.price, p1.price) as price
, coalesce(p0.price, p1.price) *  as price
from avalanche.core.ez_avax_transfers t
left join p0
    on p0.hour = date_trunc('hour', t.block_timestamp)
left join p1
    on p1.date = date_trunc('day', t.block_timestamp)
where t.block_timestamp >= '2023-08-01'
    and avax_from_address = '0xc2f41b3a1ff28fd2a6eee76ee12e51482fcfd11f'
LIMIT 10000


select *
from AVALANCHE.PRICE.EZ_HOURLY_TOKEN_PRICES
LIMIT 100

with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= CURRENT_DATE - 10
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 10
        and is_imputed = FALSE
    group by 1, 2
), e as (
    select distinct block_timestamp
    , tx_id
    from solana.core.fact_events
    where block_timestamp >= CURRENT_DATE - 7
        and program_id = 'GFXsSL5sSaDfNFQUYsHekbWBW1TsFdjDYzACh62tEHxn'
), t0 as (
    select t.mint
    , tx_to as address
    , 'to' as direction
    , COUNT(distinct tx_id) as n_tx
    , COUNT(distinct tx_from) as n_address
    , sum(amount) as amount
    , sum(amount * coalesce(p0.price, p1.price, 0)) as amount_usd
    from solana.core.fact_transfers t
    join e
        on e.block_timestamp = t.block_timestamp
        and e.tx_id = t.tx_id
    left join p0
        on p0.hour = date_trunc('hour', t.block_timestamp)
        and p0.mint = t.mint
    left join p1
        on p1.date = date_trunc('day', t.block_timestamp)
        and p1.mint = t.mint
    where block_timestamp >= CURRENT_DATE - 7
    group by 1, 2, 3
    UNION
    select t.mint
    , tx_from as address
    , 'from' as direction
    , COUNT(distinct tx_id) as n_tx
    , COUNT(distinct tx_to) as n_address
    , sum(amount) as amount
    , sum(amount * coalesce(p0.price, p1.price, 0)) as amount_usd
    from solana.core.fact_transfers t
    join e
        on e.block_timestamp = t.block_timestamp
        and e.tx_id = t.tx_id
    left join p0
        on p0.hour = date_trunc('hour', t.block_timestamp)
        and p0.mint = t.mint
    left join p1
        on p1.date = date_trunc('day', t.block_timestamp)
        and p1.mint = t.mint
    where block_timestamp >= CURRENT_DATE - 7
    group by 1, 2, 3
), t1 as (
    select mint
    , address
    , direction
    , sum(n_tx) as n_tx
    , sum(n_address) as n_address
    , sum(amount) as amount
    , sum(amount_usd) as amount_usd
    from t0
    group by 1, 2, 3
)
select t1.*
, l.label as mint_label
, l2.label as address_label
from t1
left join solana.core.dim_labels l
    on l.address = t1.mint
left join solana.core.dim_labels l2
    on l2.address = t1.address
order by n_tx desc

with t0 as (
    select date_trunc('month', t.created_at)::date as month
    , d.id
    , d.title
    , u.username
    , case
        when d.id in (
            select dashboard_id from labels where dashboard_tag = 'nic-carter-bounty'
        ) then 'Nic Carter Bounty'
        when u.username = 'tkvresearch' then 'TK Research'
        else 'Other' end as dashboard_type
    , t.impression_count
    , row_number() over (partition by month order by impression_count desc) as month_rk
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where not d.id in (
        select dashboard_id from labels where dashboard_tag = 'bot'
    )
    qualify (
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
)
select *
from t0


with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= CURRENT_DATE - 10
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 10
        and is_imputed = FALSE
    group by 1, 2
)
select tx.signers[0]::string as signer
, tx_from
, e.program_id
, t.*
, coalesce(p0.price, p1.price) * amount as amount_usd
from solana.core.fact_transfers t
join solana.core.fact_transactions tx
    on tx.block_timestamp = t.block_timestamp
    and tx.tx_id = t.tx_id
join solana.core.fact_events e
    on e.block_timestamp = t.block_timestamp
    and e.tx_id = t.tx_id
left join p0
    on p0.hour = date_trunc('hour', t.block_timestamp)
    and p0.mint = t.mint
left join p1
    on p1.date = date_trunc('day', t.block_timestamp)
    and p1.mint = t.mint
where t.block_timestamp >= CURRENT_DATE - 7
    and tx.block_timestamp >= CURRENT_DATE - 7
    and e.block_timestamp >= CURRENT_DATE - 7
    -- and mint = 'hntyVP6YFm1Hg25TN9WGLqM12b8TQmcknKrdu1oxWux'
    -- and tx_from = '2AdZQmGikAMWahuJRb27PGABQyF6iyQ8aUUYyDDwRRG6'
    -- and (
    --     tx_from = 'treaf4wWBBty3fHdyBpo35Mz84M8k3heKXmjmi9vFt5'
    --     or tx_to = 'treaf4wWBBty3fHdyBpo35Mz84M8k3heKXmjmi9vFt5'
    -- )
    and e.program_id = 'GFXsSL5sSaDfNFQUYsHekbWBW1TsFdjDYzACh62tEHxn'


select *
from solana.core.fact_decoded_instructions
where block_timestamp >= CURRENT_DATE - 1
    and program_id = 'GFXsSL5sSaDfNFQUYsHekbWBW1TsFdjDYzACh62tEHxn'
LIMIT 100


with t0 as (
    select stake_pool
    , sum(case when action like 'deposit%' then amount else -amount end ) * POWER(10, -9) as stake
    from solana.defi.fact_stake_pool_actions s
    where succeeded
    group by 1
)
select t0.*
, l.*
from t0
left join solana.core.dim_labels l
    on l.address = t0.stake_pool



with t0 as (
    select c.value:parsed:type::string as type
    , c.value:parsed:info:amount::int * POWER(10, -9) as amount
    , c.value:parsed:info:lamports::int * POWER(10, -9) as stake_amount
    , e.instruction:accounts[0]::STRING as stake_account
    , c.value:parsed:info:mint::string as mint
    , e.tx_id
    , e.signers[0]::string as address
    from solana.core.fact_events e
    , lateral flatten(
        input => parse_json(e.inner_instruction:instructions)
    ) c
    where block_timestamp >= '2023-08-03'
        and block_timestamp <= '2023-08-06'
        and program_id in (
            'SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy'
        )
        -- and c.value:parsed:info:mint::string = 'bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1'
        and succeeded = TRUE
)
select type, mint, stake_account, sum(amount) as amount, sum(stake_amount) as stake_amount
from t0
group by 1, 2, 3
order by 5 desc

select decoded_instruction:name::string as name
, decoded_instruction:args:limit::int as limit
, decoded_instruction:args:limitPrice::int as limit
, *
from solana.core.fact_decoded_instructions
where block_timestamp >= CURRENT_DATE - 1
    and program_id = '4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg'
    and name = 'flashLoanBegin'
LIMIT 10000

select decoded_instruction:name::string as name
, COUNT(1)
from solana.core.fact_decoded_instructions i
join solana.core.fact_transactions t
    on t.tx_id = i.tx_id
where i.block_timestamp >= CURRENT_DATE - 8
    and t.block_timestamp >= CURRENT_DATE - 8
    and t.succeeded
    and program_id = '4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg'
group by 1
order by 2 desc

select t.signers[0]::string as signer
, COUNT(distinct t.tx_id) as n_tx
from solana.core.fact_decoded_instructions i
join solana.core.fact_transactions t
    on t.tx_id = i.tx_id
where i.block_timestamp >= CURRENT_DATE - 8
    and t.block_timestamp >= CURRENT_DATE - 8
    and t.succeeded
    and decoded_instruction:name::string = 'perpPlaceOrderV2'
    and program_id = '4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg'
group by 1
order by 2 desc


select *
, decoded_instruction:name::string as name
, c.value:pubkey::string as pubkey
from solana.core.fact_decoded_instructions
, lateral flatten(
    input => decoded_instruction:accounts
) c
where block_timestamp >= current_date - 1
    and program_id = '4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg'
    -- and decoded_instruction:name::string = 'perpConsumeEvents'
    and pubkey = '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj'
LIMIT 100



select *
, c:value:pubkey
from solana.core.fact_decoded_instructions
, lateral flatten(
    input => decoded_instruction:accounts
) c
where block_timestamp >= CURRENT_DATE - 1
    and program_id = '4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg'
    and decoded_instruction:name::string = 'perpPlaceOrderV2'
LIMIT 100


with base as (
    select i.decoded_instruction:args:clientOrderId::int as clientOrderId
    , i.decoded_instruction:args:limit::int as limit
    , i.decoded_instruction:args:maxBaseLots::int as maxBaseLots
    , i.decoded_instruction:args:priceLots::int as priceLots
    , i.decoded_instruction:accounts[3]::pubkey::string as perpMarket
    , i.decoded_instruction:name::string as name
    , t.signers[0]::string as signer
    , i.decoded_instruction:args:side as side
    -- , i.*
    from solana.core.fact_decoded_instructions i
    join solana.core.fact_transactions t
        on t.tx_id = i.tx_id
    where i.block_timestamp >= CURRENT_DATE - 1
        and t.block_timestamp >= CURRENT_DATE - 1
        and t.succeeded
        -- and decoded_instruction:name::string = 'perpPlaceOrderV2'
        and program_id = '4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg'
)
select b.*
, b2.decoded_instruction as decoded_instruction_2
, b2.name as name_2
from base b
join base b2
    on b2.clientOrderId = b.clientOrderId
where b.name = 'perpPlaceOrderV2'
    and b2.name <> 'perpPlaceOrderV2'

select t.signers[0]::string as signer
, i.*
from solana.core.fact_decoded_instructions i
join solana.core.fact_transactions t
    on t.tx_id = i.tx_id
where i.block_timestamp >= CURRENT_DATE - 1
    and t.block_timestamp >= CURRENT_DATE - 1
    and t.succeeded
    and decoded_instruction:name::string = 'perpPlaceOrderV2'
    and program_id = '4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg'
LIMIT 10000



select stake_pool
, stake_pool_name
, action
, sum(amount) * POWER(10, -9) as amount
, sum(case when action like 'deposit%' then amount else -amount end ) * POWER(10, -9) as stake
from solana.defi.fact_stake_pool_actions s
where succeeded
group by 1, 2, 3
order by 1, 2, 3, 4 desc


with t0 as (
    select stake_pool
    , stake_pool_name
    -- , date_trunc('month', block_timestamp) as month
    , sum(case when action like 'deposit%' then amount else -amount end ) * POWER(10, -9) as stake
    from solana.defi.fact_stake_pool_actions s
    where succeeded
    group by 1, 2
    order by 3 desc
)
select t0.*
, l.*
from t0
left join solana.core.dim_labels l
    on l.address = t0.stake_pool


select d.id as dashboard_id
, d.title
, d.created_at::date as date
, u.username
, t.*
from bi_analytics.twitter.tweet t
join bi_analytics.velocity_app_prod.dashboards d
    on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
where u.username = 'crypto_edgar'
order by date desc

with pages as (
    select d.title
    , d.id as dashboard_id
    , u.username
    , COUNT(distinct anonymous_id) as n_users
    from bi_analytics.gumby.pages p
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(p.context_page_tab_url, '?')[0]::string, 6)
    where p.timestamp >= '2023-11-01'
        and context_page_referring_domain = 't.co'
    group by 1, 2, 3
), t0 as (
    select d.id as dashboard_id
    , d.title
    , u.username
    , sum(t.impression_count) as impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    qualify (
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
    group by 1, 2, 3
)
select coalesce(p.dashboard_id, t0.dashboard_id) as dashboard_id
, coalesce(p.title, t0.title) as title
, coalesce(p.username, t0.username) as username
, coalesce(n_users, 0) as n_users
, coalesce(impression_count, 0) as impression_count
from pages p
FULL OUTER join t0
    on t0.dashboard_id = p.dashboard_id
order by n_users desc




with airdrop as (
    select tx_to as address
    , sum(amount) as airdrop_amount
    from solana.core.fact_transfers t
    where t.block_timestamp >= '2023-11-19'
        and mint = 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3'
        and tx_from in ('CvCRzciWKD6k9z23bBsQC1jHWmTJd7AKVKr8YvxEzNYz','75WTeS4ZKruLJH4mz6omtbMNz8H9HEG1kY4G6VNQLX5i')
    group by 1
), gini as (
    select airdrop_amount
    , row_number() over (order by airdrop_amount) as row_num
    , COUNT(1) over () as total_count
    , sum(airdrop_amount) over (order by airdrop_amount) as cumu_airdrop_amt
    , sum(airdrop_amount) over () as tot_airdrop_amt
    from airdrop
)
select sum(cumu_airdrop_amt) as tot_area
, sum(tot_airdrop_amt * row_num / total_count) as even_dist
, tot_area / even_dist as gini_coef
from gini

select t.*
tx.*
from solana.core.fact_transfers t
join solana.core.fact_transactions tx
    on tx.block_timestamp = t.block_timestamp
    and tx.tx_id = t.tx_id
where t.block_timestamp >= '2023-11-19'
    and tx.block_timestamp >= '2023-11-19'
    and mint = 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3'
    and tx_from in ('CvCRzciWKD6k9z23bBsQC1jHWmTJd7AKVKr8YvxEzNYz')
LIMIT 1000



with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= '2023-11-20'
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= '2023-11-20'
        and is_imputed = FALSE
    group by 1, 2
), t0 as (
    select swap_from_mint as mint
    , s.swapper
    , sum(swap_from_amount) as amount
    , sum(swap_from_amount * coalesce(p0.price, p1.price, 0)) as amount_usd
    from solana.defi.fact_swaps s
    left join p0
        on p0.hour = date_trunc('hour', s.block_timestamp)
        and p0.mint = s.swap_from_mint
    left join p1
        on p1.date = date_trunc('day', s.block_timestamp)
        and p1.mint = s.swap_from_mint
    where s.block_timestamp >= '2023-11-20'
    group by 1, 2
    UNIon 
    select swap_to_mint as mint
    , s.swapper
    , sum(swap_to_amount) as amount
    , sum(swap_to_amount * coalesce(p0.price, p1.price, 0)) as amount_usd
    from solana.defi.fact_swaps s
    left join p0
        on p0.hour = date_trunc('hour', s.block_timestamp)
        and p0.mint = s.swap_to_mint
    left join p1
        on p1.date = date_trunc('day', s.block_timestamp)
        and p1.mint = s.swap_to_mint
    where s.block_timestamp >= '2023-11-20'
    group by 1, 2
), t1 as (
    select mint
    , COUNT(distinct swapper) as n_swappers
    , sum(amount) as amount
    , sum(amount_usd) as amount_usd
    from t0
    group by 1
), t2 as (
    select t1.*
    , l.*
    from t1
    left join solana.core.dim_labels l
        on l.address = t1.mint
)
select *
from t2
order by amount_usd desc



with t0 as (
    select swap_from_mint as mint
    , s.block_timestamp::date as date
    , s.swapper
    from solana.defi.fact_swaps s
    where s.block_timestamp::date >= CURRENT_DATE - 30
        and not s.swap_from_mint in (
            'So11111111111111111111111111111111111111112'
            , 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
        )
    group by 1, 2, 3
    UNIon 
    select swap_to_mint as mint
    , s.block_timestamp::date as date
    , s.swapper
    from solana.defi.fact_swaps s
    where s.block_timestamp::date >= CURRENT_DATE - 30
        and not s.swap_to_mint in (
            'So11111111111111111111111111111111111111112'
            , 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
        )
    group by 1, 2, 3
), t1 as (
    select mint
    , date
    , COUNT(distinct swapper) as n_swappers
    from t0
    group by 1, 2
), t2 as (
    select t1.*
    , l.*
    , row_number() over (partition by date order by n_swappers desc) as rn
    from t1
    left join solana.core.dim_labels l
        on l.address = t1.mint
)
select *
from t2
where rn <= 2
order by date desc, rn



with t0 as (
    select tx_to as address
    , sum(amount) as airdrop_amount
    from solana.core.fact_transfers t
    join solana.core.fact_transactions tx
        on tx.block_timestamp = t.block_timestamp
        and tx.tx_id = t.tx_id
    where t.block_timestamp >= '2023-11-19'
        and tx.block_timestamp >= '2023-11-19'
        and mint = 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3'
        and tx_from in (
            '75WTeS4ZKruLJH4mz6omtbMNz8H9HEG1kY4G6VNQLX5i'
            , 'CvCRzciWKD6k9z23bBsQC1jHWmTJd7AKVKr8YvxEzNYz'
        )
    group by 1
), t1 as (
    select *
    , sum(airdrop_amount) over () as tot_airdrop_amount
    , sum(airdrop_amount) over (order by airdrop_amount desc) as cumu_airdrop_amt
    , row_number() over (order by airdrop_amount desc) as rn
    from t0
)
select *
, ROUND(100 * airdrop_amount / tot_airdrop_amount, 1) as pct
, ROUND(100 * cumu_airdrop_amt / tot_airdrop_amount, 1) as cumu_pct
from t1
order by airdrop_amount desc


-- forked from Pyth Airdrop Distribution @ https://flipsidecrypto.xyz/edit/queries/07c73ca9-df31-4940-bc88-4662a4ebbf8f


with t0 as (
    select tx_to as address
    , sum(amount) as airdrop_amount
    from solana.core.fact_transfers t
    join solana.core.fact_transactions tx
        on tx.block_timestamp = t.block_timestamp
        and tx.tx_id = t.tx_id
    where t.block_timestamp >= '2023-11-19'
        and tx.block_timestamp >= '2023-11-19'
        and mint = 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3'
        and tx_from in (
            '75WTeS4ZKruLJH4mz6omtbMNz8H9HEG1kY4G6VNQLX5i'
            , 'CvCRzciWKD6k9z23bBsQC1jHWmTJd7AKVKr8YvxEzNYz'
        )
        and not tx_to in (
            '9Mb26cH5A1c9YaJ95A95HNZwPD3WxLEnucG446umE8bc'
        )
    group by 1
), t1 as (
    select *
    , sum(airdrop_amount) over () as tot_airdrop_amount
    , sum(airdrop_amount) over (order by airdrop_amount) as cumu_airdrop_amt
    , row_number() over (order by airdrop_amount) as rn
    , COUNT(1) over () as tot_rows
    from t0
), t2 as (
    select *
    , ROUND(100 * airdrop_amount / tot_airdrop_amount, 1) as pct
    , ROUND(100 * cumu_airdrop_amt / tot_airdrop_amount, 1) as cumu_pct
    from t1
    order by airdrop_amount desc 
)
select CEIL(pct) as pct
, MAX(cumu_pct) as cumu_pct
from t2


select e.block_timestamp::date as date
, l.label as program_label
, l2.label as address_label
, e.*
, t.*
from solana.core.fact_events e 
left join solana.core.fact_transfers t
    on t.block_timestamp = e.block_timestamp
    and t.tx_id = e.tx_id
left join solana.core.dim_labels l
    on l.address = e.program_id
left join solana.core.dim_labels l2
    on l2.address = t.tx_to
where e.block_timestamp >= '2023-11-01'
    and t.block_timestamp >= '2023-11-01'
    and e.signers[0]::string = '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj'
order by e.block_timestamp desc

with t0 as (
    select t.block_timestamp::date as date
    , l2.label as address_label
    , t.*
    from solana.core.fact_transfers t
    left join solana.core.dim_labels l2
        on l2.address = t.tx_to
    where t.block_timestamp >= '2023-11-01'
        and t.tx_from = '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj'
    order by t.block_timestamp desc
)
, t1 as (
    select e.block_timestamp::date as date
    , l.label as program_label
    , e.*
    from solana.core.fact_events e
    left join solana.core.dim_labels l
        on l.address = e.program_id
    where e.block_timestamp >= '2023-11-01'
        and e.signers[0]::string = '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj'
        and coalesce(l.label, '') <> 'solana'
)
select t0.*
, t1.program_id
, t1.program_label
from t0
left join t1
    on t1.block_timestamp = t0.block_timestamp
    and t1.tx_id = t0.tx_id
order by t0.block_timestamp desc



with t0 as (
    select t.block_timestamp::date as date
    , l2.label as address_label
    , t.*
    from solana.core.fact_transfers t
    left join solana.core.dim_labels l2
        on l2.address = t.tx_to
    where t.block_timestamp >= '2023-11-01'
        and t.tx_to = '98Ni7vVRR1tggtWWruPVcfFXHTH11bPbNryJZGkCGvaD'
    order by t.block_timestamp desc
)
, t1 as (
    select e.block_timestamp::date as date
    , l.label as program_label
    , e.*
    from solana.core.fact_events e
    left join solana.core.dim_labels l
        on l.address = e.program_id
    where e.block_timestamp >= '2023-11-01'
        -- and e.signers[0]::string = '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj'
        and coalesce(l.label, '') <> 'solana'
        and e.tx_id in (
            select distinct tx_id from t0
        )
)
select t0.*
, t1.program_id
, t1.program_label
from t0
left join t1
    on t1.block_timestamp = t0.block_timestamp
    and t1.tx_id = t0.tx_id
order by t0.block_timestamp desc




select decoded_instruction:name as name
, *
from solana.core.fact_decoded_instructions
where block_timestamp::date >= CURRENT_DATE - 1
    and program_id = 'SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f'
LIMIT 10000

with t0 as (
    select l.label
    , t.tx_to
    , COUNT(distinct s.tx_id) as n_tx
    , sum(t.amount) as amount
    from solana.nft.fact_nft_sales s
    join solana.core.fact_transfers t
        on t.block_timestamp = s.block_timestamp
        and t.tx_id = s.tx_id
    left join solana.core.dim_labels l
        on l.address = s.mint
    where s.block_timestamp >= CURRENT_DATE - 7
        and t.block_timestamp >= CURRENT_DATE - 7
        and s.succeeded
        and t.mint = 'So11111111111111111111111111111111111111112'
        and t.amount < (s.sales_amount * 0.3)
        and t.amount > 0
    group by 1, 2
)
select *
, row_number() over (partition by label order by n_tx desc) as rn
from t0
qualify(
    rn <= 5
)

select record_metadata:CreateTime::int as CreateTime
, *
from crosschain.bronze.prod_address_label_sink_291098491
order by CreateTime desc
LIMIT 100

select *
from solana.core.dim_labels
LIMIT 100

with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - {{days}}
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - {{days}}
        and is_imputed = FALSE
    group by 1, 2
), t0 as (
    select *
    from solana.core.fact_transfers t
    where t.block_timestamp >= CURRENT_DATE - {{days}}
        and t.tx_id = '{{tx_id}}'    
)
select t.tx_id
, t.block_timestamp::date as date
, t.tx_from
, l.*
, t.tx_to
, l2.*
, t.mint
, l3.*
from t0 t
left join solana.core.dim_labels l
    on l.address = t.tx_from
left join solana.core.dim_labels l2
    on l2.address = t.tx_to
left join solana.core.dim_labels l3
    on l3.address = t.mint
left join p0
    on p0.hour = date_trunc('hour', t.block_timestamp)
    and p0.mint = t.mint
left join p1
    on p1.date = date_trunc('day', t.block_timestamp)
    and p1.mint = t.mint


select label
, sum(sales_amount) as sales_amount
, COUNT(1) as n_sales
from solana.nft.fact_nft_sales s
join solana.core.dim_labels l
    on l.address = s.mint
where s.block_timestamp >= CURRENT_DATE - 30
    -- label = 'mad lads'
    and s.succeeded
group by 1
order by 2 desc
LIMIT 10

with t0 as (
    select program_id
    , COUNT(distinct signers[0]::string) as n_signers
    , COUNT(distinct tx_id) as n_tx
    from solana.core.fact_events
    where block_timestamp >= CURRENT_DATE - 7
        and succeeded
    group by 1
), t1 as (
    select t0.*
    , coalesce(l.label, concat(left(program_id, 6), '...')) as label
    from t0
    left join solana.core.dim_labels l
        on l.address = t0.program_id
)
select *
, row_number() over (order by sales_amount desc) as rk
, concat(case when rk < 10 then '0' else '' end, rk, '. ', label) as program
from t1
where label <> 'solana' and rk <= 100
order by rk

with t0 as (
    select label
    , sum(sales_amount) as sales_amount
    , COUNT(1) as n_sales
    from solana.nft.fact_nft_sales s
    join solana.core.dim_labels l
        on l.address = s.mint
    where s.block_timestamp >= CURRENT_DATE - 30
        -- label = 'mad lads'
        and s.succeeded
    group by 1
    order by 2 desc
    LIMIT 10
)
select *
, row_number() over (order by sales_amount desc) as rk
, concat(case when rk < 10 then '0' else '' end, rk, '. ', INITCAP(label)) as collection
from t0
order by collection




with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= '2023-11-20'
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= '2023-11-20'
        and is_imputed = FALSE
    group by 1, 2
), s as (
    select distinct block_timestamp
    , tx_id
    from solana.nft.fact_nft_sales
    where block_timestamp >= CURRENT_DATE - 7
        and succeeded
), t0 as (
    select tx_to as address
    , sum(amount * coalesce(p0.price, p1.price, 0)) as amount_usd
    from solana.core.fact_transfers t
    join s
        on s.block_timestamp = t.block_timestamp
        and s.tx_id = t.tx_id
    left join p0
        on p0.hour = date_trunc('hour', t.block_timestamp)
        and p0.mint = t.mint
    join p1
        on p1.date = date_trunc('day', t.block_timestamp)
        and p1.mint = t.mint
    where t.block_timestamp >= CURRENT_DATE - 7
    group by 1
), t1 as (
    select l.label
    , sum(amount_usd) as fees_usd
    from t0
    join solana.core.dim_labels l
        on l.address = t0.address
    where l.label_subtype = 'fee_wallet'
    group by 1
), t2 as (
    select t1.*
    , row_number() over (order by fees_usd) as rk
    , concat(case when rk < 10 then '0' else '' end, rk, '. ', INITCAP(label)) as collection
    from t1
)
select *
from t2
where fees_usd >= 100
order by fees_usd desc

with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= '2023-11-20'
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= '2023-11-20'
        and is_imputed = FALSE
    group by 1, 2
), s as (
    select distinct address
    from solana.core.dim_labels
    where label = 'Degen Coin Flip'
        and label_subtype = 'fee_wallet'
), t0 as (
    select block_timestamp::date as date
    , sum(amount * coalesce(p0.price, p1.price, 0)) as amount_usd
    from solana.core.fact_transfers t
    join s
        on s.address = t.tx_to
    left join p0
        on p0.hour = date_trunc('hour', t.block_timestamp)
        and p0.mint = t.mint
    join p1
        on p1.date = date_trunc('day', t.block_timestamp)
        and p1.mint = t.mint
    where t.block_timestamp >= CURRENT_DATE - 7
    group by 1
)
select *
from t0


with t0 as (
    select distinct address
    from solana.core.dim_labels
    where address_name = 'jupiter perpetuals'
), t1 as (
    select block_timestamp::date as date
    , COUNT(distinct signers[0]::string) as n_signers
    , COUNT(distinct tx_id) as n_tx
    from solana.core.fact_events e
    join t0
        on t0.address = e.program_id
    where block_timestamp >= CURRENT_DATE - 60
        and succeeded
    group by 1
)
select *
, sum(n_tx) over (order by date) as cumu_tx
from t1
order by date




with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= '2023-11-20'
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= '2023-11-20'
        and is_imputed = FALSE
    group by 1, 2
), s as (
    select distinct address
    from solana.core.dim_labels
    where label_subtype = 'cex'
), t0 as (
    select block_timestamp::date as date
    , case when st.address is null then 'From' Else 'To' end as direction
    , sum(
        case when direction = 'From' then -amount else amount end
        * coalesce(p0.price, p1.price, 0)
    ) as amount_usd
    from solana.core.fact_transfers t
    left join s st
        on st.address = t.tx_to
    left join s sf 
        on sf.address = t.tx_from
    left join p0
        on p0.hour = date_trunc('hour', t.block_timestamp)
        and p0.mint = t.mint
    join p1
        on p1.date = date_trunc('day', t.block_timestamp)
        and p1.mint = t.mint
    where t.block_timestamp >= CURRENT_DATE - 7
        and (
            st.address is null or sf.address is null
        )
        and coalesce(st.address, sf.address) is not null
    group by 1, 2
)
select *
from t0

with t0 as (
    select distinct mint
    , tx_from as address
    from solana.core.fact_transfers t
    where block_timestamp >= CURRENT_DATE - 7
    UNION
    select distinct mint
    , tx_to as address
    from solana.core.fact_transfers t
    where block_timestamp >= CURRENT_DATE - 7
), t1 as (
    select distinct mint
    , tx_from as address
    from solana.core.fact_transfers t
    where block_timestamp >= CURRENT_DATE - 7
    UNION
    select distinct mint
    , tx_to as address
    from solana.core.fact_transfers t
    where block_timestamp >= CURRENT_DATE - 7
)
select mint
, l.label


with t0 as (
    select distinct address
    from solana.core.dim_labels
    where label = 'jitosol'
), t1 as (
    select distinct block_timestamp::date as date
    , tx_from as address
    from solana.core.fact_transfers t
    join t0
        on t0.address = t.mint
    where block_timestamp >= CURRENT_DATE - 90
    UNION
    select distinct block_timestamp::date as date
    , tx_to as address
    from solana.core.fact_transfers t
    join t0
        on t0.address = t.mint
    where block_timestamp >= CURRENT_DATE - 90
)
select date
, COUNT(distinct address) as n_wallets
from t1
group by 1
order by 1


select u.username
, d.title
, COUNT(distinct date_trunc('hour', dbt_updated_at)) as n_hours
from bi_analytics.SNAPSHOTS.HOURLY_DASHBOARD_RANKINGS dr
join bi_analytics.velocity_app_prod.dashboards d
    on d.id = dr.dashboard_id
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
where dbt_updated_at >= '2023-11-20'
    and ranking_trending <= 10
group by 1, 2
order by 3 desc


with swaps as (
  select
    date_trunc('day', BLOCK_TIMESTAMP) as date,
    COUNT(distinct TX_ID) as swap_amount,
    case
      when date_trunc('day', BLOCK_TIMESTAMP) < DATE '2023-11-01' then '1. Before Airdrop Announcement'
      when date_trunc('day', BLOCK_TIMESTAMP) = DATE '2023-11-01' then '2. Strong Airdrop Rumor'
      else '3. After Airdrop Announcement'
    end as airdrop_status
  from
    SOLANA.defi.fact_swaps
  where
    SUCCEEDED
    and swap_program like 'jupiter%'
    and BLOCK_TIMESTAMP >= CURRENT_TIMESTAMP - INTERVAL '90 days'
    and date_trunc('day', BLOCK_TIMESTAMP) <= current_date
  group by
    date
  HAVING
    COUNT(distinct TX_ID) > 0
)
select
  date,
  swap_amount,
  airdrop_status
from
  swaps
order by
   airdrop_status, date desc



select contract_address
, MAX(tx_hash) as n_txn
, COUNT(tx_hash) as n_txn
from AVALANCHE.CORE.EZ_DECODED_EVENT_LOGS
where topics[0] = '0x13ed6866d4e1ee6da46f845c46d7e54120883d75c5ea9a2dacc1c4ca8984ab80'
and block_timestamp > CURRENT_DATE - 1
group by 1
order by 3 desc

select COUNT(distinct address) from (
select distinct from_address as address
from thorchain.core.fact_transfers 
where block_timestamp <= '2022-06-01'
and block_timestamp >= '2022-04-01'
UNION
select distinct to_address as address
from thorchain.core.fact_transfers 
where block_timestamp <= '2022-06-01'
and block_timestamp >= '2022-04-01')


select date_trunc('month', block_timestamp)::date as date
, COUNT(distinct to_address) as n_to
from thorchain.core.fact_transfers
where from_address in ('thor12vcn44pdlqzpvm9kr2d4a245jfsg7ufmfr04g9')
group by 1
order by 1




with t0 as (
    select distinct LOWER(h.address) as address
    , h.txId
    , h.trailheadId
    , h.trailId
    from bi_analytics.bronze.arprograms_hike h
    where result = 'VERIFIED'
        and blockchain = 'Avalanche'
)
select *
from t0
join avalanche.core.fact_transactions t
    on t.tx_hash = t0.txId
where t.block_timestamp >= '2023-08-10'





with chain0 as (
  select
    d.id as dashboard_id,
    case
      when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
      or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
      or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
      or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%' then 'Axelar'
      else INITCAP(t.name)
    end as chain,
    COUNT(distinct q.id) as n_queries
  from
    bi_analytics.velocity_app_prod.users u
    join bi_analytics.velocity_app_prod.dashboards d on d.created_by_id = u.id
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q on dtq.B = q.id
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t on qtt.B = t.id
    and t.type = 'project'
  group by
    1,
    2
),
chain as (
  select
    *,
    row_number() over (
      partition by dashboard_id
      order by
        case
          when chain in (
            'Aptos',
            'Avalanche',
            'Axelar',
            'Flow',
            'Near',
            'Sei',
            'Solana'
          ) then 1
          else 2
        end,
        n_queries desc,
        chain
    ) as rn
  from
    chain0
),
t0 as (
  select
    coalesce(m.user_id, d.created_by_id) as user_id,
    d.profile_id as profile_id,
    coalesce(tu.username, u.username) as username,
    dr.currency,
    d.id as dashboard_id,
    p.type,
    coalesce(c.chain, 'Polygon') as chain,
    row_number() over (
      order by
        dr.ranking_trending
    ) as current_rank,
    coalesce(u.role, '') = 'internal'
    or u.username in (
      'Polaris_9R',
      'dsaber',
      'flipsidecrypto',
      'metricsdao',
      'drethereum',
      'Orion_9R',
      'sam',
      'forgash',
      'danner',
      'charliemarketplace',
      'theericstone'
    ) as internal_user
  from
    bi_analytics.content_rankings.dashboard_rankings dr
    join bi_analytics.velocity_app_prod.dashboards d on d.id = dr.dashboard_id
    left join bi_analytics.velocity_app_prod.profiles p on p.id = d.profile_id
    left join bi_analytics.velocity_app_prod.teams t on t.profile_id = p.id
    left join bi_analytics.velocity_app_prod.members m on t.id = m.team_id
    and m.role = 'owner'
    left join bi_analytics.velocity_app_prod.users tu -- changed this join to the team owner
    on tu.id = m.user_id
    join bi_analytics.velocity_app_prod.users u on u.id = d.created_by_id
    left join chain c on c.dashboard_id = dr.dashboard_id
    and c.rn = 1
)
select
  user_id,
  username,
  dashboard_id,
  profile_id,
  type,
  case
    when chain in (
        -- we pay out aptos in usdc
        -- 'Aptos',
        'Avalanche',
        'Axelar',
        'Flow',
        'Near',
        'Sei',
        'Solana'
    ) then chain
    else 'Polygon'
  end as ecosystem,
  case
    when chain = 'Solana' then 'SOL'
    when chain = 'Avalanche' then 'AVAX'
    when chain = 'Axelar' then 'AXL'
    when chain = 'Flow' then 'FLOW'
    when chain = 'Near' then 'NEAR'
    when chain = 'Sei' then 'SEI'
    else 'USDC'
  end as currency,
  case
    when current_rank <= 10 then 1.5
    else 1
  end as base_amount,
  case
    when DATEADD('hours', -5, CURRENT_TIMESTAMP)::date >= '2023-12-06'
    and DATEADD('hours', -5, CURRENT_TIMESTAMP)::date <= '2023-12-13'
    and chain in ('Avalanche', 'Aptos') then 2
    else 1
  end as boost,
  case
    when internal_user = false then base_amount * boost
    else 0
  end as amount
from
  t0
where
  current_rank <= 30
order by
  7 desc


select *
from avalanche.core.ez_avax_transfers
where avax_to_address = '0x23c14e77e980e8d90851c72678ec5f4255af7874'


select program_id, COUNT(1)
from solana.core.fact_decoded_instructions
where block_timestamp >= CURRENT_DATE - 3
group by 1
order by 2 desc

 with drift as (
select 
  tx_id,
  block_timestamp 
  from solana.core.fact_events
  where program_id = 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
and block_timestamp::date >= dateadd('day', -1, current_date)
)

select 
  case when f.value = 'Program log: Instruction: PlacePerpOrder' then 'Perp'
       when f.value = 'Program log: Instruction: PlaceSpotOrder' then 'Spot'
       when f.value = 'Program log: Instruction: BeginSwap' then 'Swap' end as trade,
  date_trunc('day',block_timestamp) as date,
  t.*

from solana.core.fact_transactions t,
lateral flatten (LOG_MESSAGES) f 
where tx_id in (select tx_id from drift)
and block_timestamp::date >= dateadd('day', -1, current_date)
and f.value in ('Program log: Instruction: PlacePerpOrder',
'Program log: Instruction: PlaceSpotOrder',
'Program log: Instruction: BeginSwap'
)

with e as (
    select distinct tx_id
    from solana.core.fact_events
    where block_timestamp >= CURRENT_DATE - 100
        and signers[0]::string in (
            '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj'
            , 'AoNVE2rKCE2YNA44V7NQt8N73JdPM7b6acZ2vzSpyPyi'
        ) and program_id = 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
)
select *
from e
left join solana.core.fact_decoded_instructions i
    on i.tx_id = e.tx_id
order by block_timestamp

select *
from solana.core.fact_decoded_instructions
where block_timestamp >= CURRENT_DATE - 1
    and program_id in (
        '_____'
        -- , 'ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXpkf3aD'
        -- , '4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg'
        -- , 'SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f'
        -- , 'SAGEqqFewepDHH6hMDcmWy7yjHPpyKLDnRXKb3Ki8e6'
        -- , 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
        -- , 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
        -- , 'DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M'
        , 'jupoNjAxXgZ4rjzxzPMP4oxduvQsQtZzyknqvzYNrNu'
    )
LIMIT 1000


with e as (
    select distinct tx_id
    from solana.core.fact_events
    where block_timestamp >= CURRENT_DATE - 3
        and program_id = '8LPjGDbxhW4G2Q8S6FvdvUdfGWssgtqmvsc63bwNFA7E'
)
select t.*
, tx.*
from e
join solana.core.fact_transfers t
    on t.tx_id = e.tx_id
join solana.core.fact_transactions tx
    on tx.tx_id = e.tx_id
where t.block_timestamp >= CURRENT_DATE - 3
    and tx.block_timestamp >= CURRENT_DATE - 3

select program_id
, count(1)
from solana.core.fact_decoded_instructions
where block_timestamp >= CURRENT_DATE - 1
group by 1
order by 2 desc

select *
from solana.core.fact_decoded_instructions
where block_timestamp >= CURRENT_DATE - 1
    and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
limit 10
order by 2 desc
    and program_id in (
        '_____'
        -- , 'ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXpkf3aD'
        -- , '4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg'
        -- , 'SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f'
        -- , 'SAGEqqFewepDHH6hMDcmWy7yjHPpyKLDnRXKb3Ki8e6'
        -- , 'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH'
        -- , 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
        -- , 'DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M'
        , 'jupoNjAxXgZ4rjzxzPMP4oxduvQsQtZzyknqvzYNrNu'
    )
LIMIT 1000




with e as (
    select distinct tx_id
    from solana.core.fact_events
    where block_timestamp >= current_date - 1
        and program_id = '8LPjGDbxhW4G2Q8S6FvdvUdfGWssgtqmvsc63bwNFA7E'
)
select COUNT(distinct t.tx_id)
, COUNT(distinct t.tx_to)
from e
join solana.core.fact_transfers t
    on t.tx_id = e.tx_id
join solana.core.fact_transactions tx
    on tx.tx_id = e.tx_id
where t.block_timestamp >= current_date - 1
    and tx.block_timestamp >= current_date - 1
-- order by t.block_timestamp, t.tx_id

with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 30
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 30
        and is_imputed = FALSE
    group by 1, 2
), mayan_tx as (
    select distinct block_timestamp
    , tx_id
    from solana.core.fact_events
    where block_timestamp >= CURRENT_DATE - 30
        and program_id = '8LPjGDbxhW4G2Q8S6FvdvUdfGWssgtqmvsc63bwNFA7E'
), mayan_vol as (
    select t.tx_id
    , t.block_timestamp::date as date
    , case when ARRAY_SIZE(tx.signers) > 1 then 'Off' else 'On' end as direction
    , MAX(
        amount * coalesce(p0.price, p1.price, 0)
    ) as amount_usd
    from solana.core.fact_transfers t
    join mayan_tx m
        on m.block_timestamp = t.block_timestamp
        and m.tx_id = t.tx_id
    left join p0
        on p0.hour = date_trunc('hour', t.block_timestamp)
        and p0.mint = t.mint
    join p1
        on p1.date = date_trunc('day', t.block_timestamp)
        and p1.mint = t.mint
    join solana.core.fact_transactions tx
        on tx.block_timestamp = t.block_timestamp
        and tx.tx_id = t.tx_id
    where t.block_timestamp >= CURRENT_DATE - 30
    group by 1, 2
)
select date
, direction
, COUNT(1) as bridge_tx
, sum(amount_usd) as bridge_volume_usd
from mayan_vol
group by 1, 2
order by 1, 2




with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 90
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 90
        and is_imputed = FALSE
    group by 1, 2
), t0 as (
select i.value:parsed:info:mint::string as mint
, i.value:parsed:type::string as type
, t.*
from
solana.core.fact_events e
join solana.core.fact_transactions t on e.tx_id = t.tx_id,
lateral flatten (input => inner_instruction:instructions) i
where
program_id = 'BBbD1WSjbHKfyE3TSFWF6vx1JV51c8msKSQy4ess6pXp'
and i.value:parsed:type = 'mintTo'
and e.succeeded = 'True'
and i.value:parsed:info:amount > 0
and e.block_timestamp >= current_date - 90
and t.block_timestamp >= current_date - 90
), t1 as (
    select t0.block_timestamp::date as date
    , COUNT(distinct t0.tx_id) as n_tx
    , sum(t.amount * coalesce(p0.price, p1.price)) as volume
    from t0
    left join solana.core.dim_tokens t
        on t.token_address = t0.mint
    left join p0
        on p0.hour = date_trunc('hour', t0.block_timestamp)
        and p0.mint = t0.mint
    join p1
        on p1.date = date_trunc('day', t0.block_timestamp)
        and p1.mint = t0.mint
    group by 1
)
select *
, sum(n_tx) as cumu_n_tx
, sum(volume) as cumu_volume
from t1


select q.name
, u.username
, q.created_at::date as date
, q.statement
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.users u
    on u.id = q.created_by_id
where statement like '%8LPjGDbxhW4G2Q8S6FvdvUdfGWssgtqmvsc63bwNFA7E%'


with t0 as (
    select distinct block_timestamp
    , tx_id
    from solana.core.fact_events
    where block_timestamp >= current_date - 1
        and program_id = '8LPjGDbxhW4G2Q8S6FvdvUdfGWssgtqmvsc63bwNFA7E'
)
select t0.tx_id
, t.*
-- t0.block_timestamp
-- , t0.tx_id
-- , sum(case when tx_to = 'EroEBUxixXh3pZ53u4xRbyRjRBifsiZuHChDQXQ7eMrn' then amount else 0 end) as amt_ero
-- , sum(case when tx_to = '2vTpTGkRUKiMdxzd5NejUup2Ai7mjNMnER5SvnK3cL5s' then amount else 0 end) as amt_2vt
from t0
left join solana.core.fact_transfers t
    on t.block_timestamp = t0.block_timestamp
    and t.tx_id = t0.tx_id
-- group by 1, 2
-- order by 1 desc
order by block_timestamp desc

with t0 as (
    select distinct trailId
    from bi_analytics.bronze.arprograms_hike h
    where h.result = 'VERIFIED'
        and h.xp > 5
), t1 as (
    select t.name as project
    , COUNT(distinct case
        when t0.trailId is null then LOWER(x.address)
        else coalesce(h.txId, LOWER(x.address))
    ) as n_trails_completed
    from bi_analytics.bronze.arprograms_xp x
    join bi_analytics.bronze.arprograms_trailhead t
        on t.id = h.trailheadId
    left join t0
        on t0.trailId = x.trailId
    left join bi_analytics.bronze.arprograms_hike h
        on h.address = x.address
        and h.trailId = x.trailId
        and h.result = 'VERIFIED'
), t2 as (
    select t.name as project
    , COUNT(distinct LOWER(h.address)) as unique_wallets
    , COUNT(distinct case when h.result = 'VERIFIED' then h.txId else null end) as transactions_verified
    from bi_analytics.bronze.arprograms_hike h
    join bi_analytics.bronze.arprograms_trailhead t
        on t.id = h.trailheadId
    where t.blockchain = 'Solana'
    group by 1
)
select t2.*
, t1.n_trails_completed
from t2
left join t1
    on t1.project = t2.project

with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= '2023-03-01'
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= '2023-03-01'
        and is_imputed = FALSE
    group by 1, 2
), t0 as (
    select h.txId
    , th.name
    , MAX(amount * coalesce(p0.price, p1.price)) as amount_usd
    from bi_analytics.bronze.arprograms_hike h
    join bi_analytics.bronze.arprograms_trailhead th
        on th.id = h.trailheadId
        and th.blockchain = 'Solana' 
    join solana.core.fact_transfers t
        on t.tx_id = h.txId
    left join p0
        on p0.hour = date_trunc('hour', t.block_timestamp)
        and left(p0.mint, 10) = left(t.mint, 10)
    left join p1
        on p1.date = t.block_timestamp::date
        and left(p1.mint, 10) = left(t.mint, 10)
    where t.block_timestamp >= '2023-03-01'
        and h.result = 'VERIFIED'
)
select name
, sum(amount_usd) as amount_usd
from t0
group by 1

with t0 as (
    select distinct s.block_timestamp
    , s.tx_id
    , s.address
    from bi_analytics.bronze.arprograms_hike h
    join bi_analytics.bronze.arprograms_trailhead th
        on th.id = h.trailheadId
        and th.blockchain = 'Solana' 
    join solana.defi.fact_stake_pool_actions s
        on s.tx_id = h.txId
)
select s.*
from t0
join solana.defi.fact_stake_pool_actions s
    on s.address = t0.address
    and s.block_timestamp >= DATEADD('minutes', -60, t0.block_timestamp)
    and s.block_timestamp <= DATEADD('minutes', 60, t0.block_timestamp)


select *
from solana.core.fact_events
where block_timestamp >= CURRENT_DATE - 1
    and tx_id = '3ggdQBTqr7DrwnezwNousYKctZPD4hBe4y82Xg1ytCgJeXTY65RMTkRJ9iWvGgE2PUtPk2QDqaEMzcohW2wCHSg3'

select program_id
, block_timestamp::date as date
, COUNT(1)
from solana.core.fact_events
where block_timestamp >= CURRENT_DATE - 10
    and signers[0] = 'Levytx9LLPzAtDJJD7q813Zsm8zg9e1pb53mGxTKpD7'
group by 1, 2
order by 3 desc


select *
from ethereum.core.fact_transactions
where block_timestamp >= CURRENT_DATE - 1
    LIMIT 100

with ep as (
    select hour::date as date
    , avg(price) as eth_price
    from ethereum.price.EZ_HOURLY_TOKEN_PRICES
    where hour >= '2023-01-01'
        and token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        and is_imputed = FALSE
    group by 1
), bp as (
    select recorded_hour::date as date
    , avg(close) as bonk_price
    from solana.price.ez_token_prices_hourly
    where recorded_hour >= '2023-01-01'
        and token_address = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
        and is_imputed = FALSE
    group by 1
), cur_bp as (
    select bonk_price as cur_bonk_price
    from bp
    order by date desc
    LIMIT 1
)
, t0 as (
    select from_address as address
    -- , t.tx_hash
    -- , block_timestamp::date as date
    -- , eth_price
    -- , bonk_price
    , sum(tx_fee) as eth_gas_paid
    , sum(tx_fee * eth_price) as eth_gas_paid_usd
    , sum(tx_fee * eth_price / bonk_price) as bonk_amount
    , sum(tx_fee * eth_price * cur_bonk_price / bonk_price) as bonk_amount_usd
    from ethereum.core.fact_transactions t
    join ep
        on ep.date = t.block_timestamp::date
    join bp
        on bp.date = t.block_timestamp::date
    join cur_bp 
        on TRUE
    where t.block_timestamp::date >= '2023-01-01'::date
    group by 1
)
select *
from t0


with t0 as (
    select distinct s.block_timestamp
    , s.tx_id
    , s.address
    from bi_analytics.bronze.arprograms_hike h
    join bi_analytics.bronze.arprograms_trailhead th
        on th.id = h.trailheadId
        and th.blockchain = 'Solana' 
        and th.name = 'Jito'
    join solana.defi.fact_stake_pool_actions s
        on s.tx_id = h.txId
), t1 as (
    select s.address
    , sum(s.amount) * POWER(10, -9) as sol_amount
    from t0
    join solana.defi.fact_stake_pool_actions s
        on s.address = t0.address
        and s.block_timestamp >= DATEADD('minutes', -60, t0.block_timestamp)
        and s.block_timestamp <= DATEADD('minutes', 60, t0.block_timestamp)
        and s.stake_pool_name = 'jito'
    group by 1
)
, claim_txs as (

  select distinct

    block_timestamp
    , tx_id

  from solana.core.fact_events
  where succeeded
    and program_id = 'mERKcfxMC5SqJn4Ld4BUris3WKZZ1ojjWJ3A3J5CKxv'
    and block_timestamp > '2023-12-07 16:00:00'
),

transfers as (

  select
    tx_to
    , sum(amount) as jto_claimed
  from solana.core.fact_transfers
  inner join claim_txs
    using(block_timestamp, tx_id)
  where mint = 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL'
    and tx_from = 'HS8EQ8QkQSBJggY8r255AKdWbtYRtyNMoRt4LjNkkWm1'
    and block_timestamp > '2023-12-07 16:00:00'
    group by 1
)
select * 
from t1
left join transfers t
    on t.tx_to = t1.address


with t1 as (
select 'YpEuA9RetRXD1N1ymrZCXR6PFXsJKNGbLSF6SPqUCCB' as address, 1 as sol_staked
UNIon select 'XKGSV9jCDHQgCxqAGsKPmzQtYUBcPBwFFsvRL3aEvv3' as address, 0.2 as sol_staked
UNIon select 'X5eigJYBEQh5xyvf82HH8KQRRwhmtV2kJXvLtDEA6n2' as address, 7.4 as sol_staked
UNIon select 'JDZDkBxswCZKJ9xviX4ctny48DouJhToa6yAY9ANfczb' as address, 80 as sol_staked
UNIon select 'Hr61YhAQM4fG4x1vfAa3oUWaS2LttcA3K5QZmHqPDYvQ' as address, 5 as sol_staked
UNIon select 'HH8ZKyikSENap1KMdRhwdkRBhKoyUct7w2e3hKc9bC55' as address, 0.26 as sol_staked
UNIon select 'GsuyNHX76ZGXwisQ2qSyP6nNUgx2DxNtCQXESVswzC6F' as address, 0.9 as sol_staked
UNIon select 'Gm3LghzwaPCL4xSnmk7Py4dHrfbk1bomLBWPkKVqDcHx' as address, 13 as sol_staked
UNIon select 'FJLaAbbeJ8H7N2FZxe7WxNARPoicNdUk632vYct99jKN' as address, 0.001 as sol_staked
UNIon select 'D4XQN3dw3qMSo7ZSFVZPD5nJ5R4pmd2BfLaXAatoYAac' as address, 1.0161 as sol_staked
UNIon select 'BTgvyWkuRABn2LY34YAt31JLnnBCRv8WyUAtcr7zLmD' as address, 0.01 as sol_staked
UNIon select 'AoNVE2rKCE2YNA44V7NQt8N73JdPM7b6acZ2vzSpyPyi' as address, 15 as sol_staked
UNIon select '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj' as address, 2.1 as sol_staked
UNIon select '9QbvQbN5Qr9NPvoNargxPvS3yFAD8QBETYD2PWSaqTbQ' as address, 10 as sol_staked
UNIon select '8e1DosWW5SKPcrR6usJAasaGaTWNj6CRqo5SQMdGXQKc' as address, 8 as sol_staked
UNIon select '7RVpCCtkVHaJsQ8G21eCKeurpGkPMfy4E24LFNc6o6HM' as address, 2.746964066 as sol_staked
UNIon select '7ND1CdhR1nhuf6uyMiQzScfsqX6FUAkGz2dnCCcTHTWX' as address, 5 as sol_staked
UNIon select '77vNr9KEbCBpgp4Rvx7ynKSDaqgqc95LyJ7eYRcCPkhg' as address, 4.5 as sol_staked
UNIon select '6MupHwVuaZnxeXbw2DEvH96ouaWdg5TygPaSruB4H7YG' as address, 15.615783827 as sol_staked
UNIon select '6Hayp9GJ3QBqaXyUukdyYn9LC3sTStfJCtJMeSgTagYd' as address, 120 as sol_staked
UNIon select '5pcjuTTR5Fy44gJzmj236RLeTh5psh5SiZkASYt9TETC' as address, 600 as sol_staked
UNIon select '4NfNib3vDRVJre6D7gZbsojiXSyNjCzAKJcjhS9okTRg' as address, 0.01 as sol_staked
UNIon select '4JFvLyyrKakNiWmrJbNbxjntMEnrEEB9KhBBCiJA3ssy' as address, 1.5 as sol_staked
UNIon select '3ZKsqP78eggi3ahnD4B5E4No8qZfwDwfsqhas56pLBGu' as address, 0.15 as sol_staked
UNIon select '3edU4Tde4jvq5K4ngjm61DA6soxEGzmXbHXBTsUdHg3k' as address, 0.5 as sol_staked
UNIon select '37nNFPyEza9Ajq7BYiYmrGX3fmaLunYQyv2rExWfdQbY' as address, 2.43 as sol_staked
UNIon select 'BZDAM2MkhsZSBcUJbQfJM6syN7Rgp928dMTpr1Ch1T4J' as address, 0.0176 as sol_staked
UNIon select 'HYWMDZQdkAh6T74LECFxPtAj9yyx6KDVhm4WVFtJcwZs' as address, 0.02 as sol_staked
UNIon select 'FqRZP1NoLukdcc3Bfj8wh7ePg9xuwME27rqrkA23HZfk' as address, 0.9 as sol_staked
UNIon select '5fxDwVnPhhKB2t3GJcjRKH1ftrPzaQZRGxxC3ChAzKqa' as address, 0.001 as sol_staked
UNIon select 'FXqyyzZ98ncfQMpoGXR3iNkzeMcqWthc8CsvFk4wRZrL' as address, 0.052 as sol_staked
UNIon select 'BJa7dXatu6ALaPzYDQkpEWHbK7v9PrfdFftyeoErQ27u' as address, 0.002 as sol_staked
UNIon select '98itoCb1ReEVDGHK5ysRxdPRgoeR7hNENEvEkAuW3J3u' as address, 0.02 as sol_staked
UNIon select '7xV2r3jqbiRWggnodyKALip5Q3Fct4xLf2QH5kg7ZvQp' as address, 0.0001 as sol_staked
UNIon select 'EfWsA8zY2YZmrk2YWcCJMPQqxbfPL764XS7nF5VGYc2K' as address, 0.0341 as sol_staked
UNIon select '84YwggDxtCxPWsgHWtKcCzx17X7r5o791TX9b9rru6Aq' as address, 0.01 as sol_staked
UNIon select 'Do32mNetsYkGkhYPMYnRxXNBJxrbfXhTVEhhiBZvrbBk' as address, 0.021015509 as sol_staked
UNIon select 'C1GgvT5pcZuWkWJsPCMnVuJZFJidogY3TBGBZDw2Spu7' as address, 0.1 as sol_staked
UNIon select 'CKKqtVFVjzdHvmxRUG6BzRsNTu6sZTgHsuBRWg5XQ1WS' as address, 2 as sol_staked
UNIon select 'ChxFMF8YxDvJHNBVgKLxEuu9mhGepREjswivhZsW6a2X' as address, 40 as sol_staked
UNIon select 'CmkDNFUvzMfbiuQUG8D5cpDdDmutVxbZUTPr6NytJK1B' as address, 0.017109399 as sol_staked
UNIon select '2MS3BqdZfZyDA2wAWdqqyLxwLq7CYFmGw7JDmWoLWQuU' as address, 46.999000001 as sol_staked
UNIon select '232PpcrPc6Kz7geafvbRzt5HnHP4kX88yvzUCN69WXQC' as address, 0.1 as sol_staked
UNIon select '6wpizL3gyTpqTVdhX3SMVDkrzz6AaSrma7Ggkur9rM6m' as address, 0.07497537 as sol_staked
UNIon select 'HzCKRzeTaNZ4LgVkLmtSR2exbVauRHiJVAWxDoP8cVyB' as address, 0.02 as sol_staked
UNIon select '45xMWbUy6QruNwFUPb6wuE59aGHKWTCrVRJpLQ4SQaFo' as address, 0.01 as sol_staked
UNIon select '6mvhp5JGdKwm73NznWhzqeAguFnm3GBbYc5qMPAS4xJB' as address, 0.12 as sol_staked
UNIon select 'FJZrQtQ9Hxc2sSAYVfv8XMREsMR8RN4meJS49KreX3t7' as address, 0.0055 as sol_staked
UNIon select 'GJwxb4iPkzSXxrj263WXT6fT1aYh1QsxB8PF5YuhuC7g' as address, 0.04 as sol_staked
UNIon select 'J259rJC1zRmZvXguP5dMpznXnKiN7k8sLgLqyZCVKxwe' as address, 0.051 as sol_staked
UNIon select '9Sf1ShndgNgi6F9fbN8iDRss61gN3zaya58p1qzqZYUX' as address, 0.01 as sol_staked
UNIon select 'AFBu88LbWWdBwT7u5zKrLmBVELj9zHQHnpfDg9LgM3QP' as address, 1 as sol_staked
UNIon select 'J8Z32jKLHrDx1t3eMXQ8fsXqVy6ZvCCA8huJVCMgdF3n' as address, 0.005 as sol_staked
UNIon select 'Cj5rQxxyWnrNS1wU8se7mgV5erMZoaq7MJ4tC1b8hqLn' as address, 0.215623576 as sol_staked
UNIon select 'A1tBwR8HycRzG5qEShUUokDRW6ypPytMp8mfxRRgwDUk' as address, 0.13993 as sol_staked
UNIon select 'HYN6HN9ecoJmt8GiNZ1BzRRrwD4QceRwZAcqQHcacp5E' as address, 1 as sol_staked
UNIon select 'Bdi5n71yNkQhc2w2TFz8B9ftDadRSvVrgSCp7F3GByEJ' as address, 0.002 as sol_staked
UNIon select '9qERW2XSn2va2YHgFVMpNnDdBjsRZZq4GKV6NnFgJo13' as address, 0.05 as sol_staked
UNIon select '23KNgY2eu7GVrL9FCJcHbhzkEaUMGCEUF9gTwbvRXeJk' as address, 0.02 as sol_staked
UNIon select 'fEDwzyvHGK8bA4mnTNPCvVncZYSx6oVes3X9m1BCPe2' as address, 0.16 as sol_staked
UNIon select 'HzFcc5ewxht36YkizX4KgVpY9y3udYgnCgwJuX2Nbcsa' as address, 5.077 as sol_staked
UNIon select '4hQhi6jgzk7EuKQ1qppAE6UUQsNsh8yGiTsRf1Z2SYwM' as address, 0.0129 as sol_staked
UNIon select '59iS8sZau2A7hCcEyrx8XvZcjkghEPGajoMLLxo7VC4z' as address, 0.001 as sol_staked
UNIon select 'GVxS5F1cYxS5DKRaDExF3kxGmX6TNfQYP5FWUeN9RKL1' as address, 2.019813853 as sol_staked
UNIon select '5b3xFz6oajSBnbvQpEM1E8nkGTw7UGfyc5nXGhemmt9g' as address, 0.001 as sol_staked
UNIon select '78rKG49qzEsK5bQGaLANpHbfA2MLWawXiXfrQ5UCPhD7' as address, 0.01 as sol_staked
UNIon select '2aaLxpE8XR9Jc8tdEPNdfRV7Lh9qx4qknejJqPSHMzsf' as address, 0.118 as sol_staked
UNIon select '2Bw31eaLVATkB4zcC4bXMCXDChrsnDAHsoncVq99eXcK' as address, 0.01 as sol_staked
UNIon select 'AgScCEWcKTXRxa79esLbESEKbwFTURntRB6LPCREZ8Yw' as address, 0.16 as sol_staked
UNIon select 'HPrDcYup7DAXMPgUJKG6SnaZVr9eEECYyxaTWvGi2wRv' as address, 0.0001 as sol_staked
UNIon select '52MzaCN3azWFw3dBpiav71EUBcVLa3rbk5FqwnGEfKgy' as address, 0.01 as sol_staked
UNIon select 'HSbZvDmApnkwJa8RzJuD2kgC9vBb7LVXEWa74YkxxQo9' as address, 0.01 as sol_staked
UNIon select 'J7dKGXBPV7dsphEitzoorw1AaQ3oNFr72CkjTBjZcqDt' as address, 0.002 as sol_staked
UNIon select 'H5DTKJMqkAExexNpurYVFMwcxNtXPVg6WmsLXYf1DprD' as address, 0.02 as sol_staked
UNIon select '77r3SwMcwiEx4L5cJNrwvzpEjSmBC6BxWcQx3iFBxtGd' as address, 0.3 as sol_staked
UNIon select '4zUq3oXWGg79Am4ZSyc4aCstcT3tCDMbN5MRUz8HmJhX' as address, 0.0062 as sol_staked
UNIon select '4dKoyKhCVSbfDRQMNNL8CuTRWmu9QdQQr9fPmaaUrsWW' as address, 0.1 as sol_staked
UNIon select 'DYiPERTLaRfU2EXUPBX691eorRQt59w9Qu3TDNBQAYnH' as address, 0.01 as sol_staked
UNIon select 'DqVq2eEy5UVKaosS1XcDi5ijVLnYdHh31MPDzv8Zj1Xq' as address, 0.01929 as sol_staked
UNIon select 'C7EbRUFUP7JJ7cDWTqQrhziuZwSV3r94BqSh6q818PwY' as address, 0.1 as sol_staked
UNIon select 'CSyCFh6fcUSN44V8H3VRd6a9c8d2qepGYbiWC1a4gntZ' as address, 0.01 as sol_staked
UNIon select '3mJ7FPqZ8gFzbePW3BfbJbTrDpYtL5M4i6JTouBcsyDy' as address, 0.001 as sol_staked
UNIon select 'EuB41aFaVU7ncK7KFvTrx9SZQdYhqh3NMMmLXEN41fpa' as address, 0.01 as sol_staked
UNIon select 'DMdQKThAgUJeAZTH9x29vtTmw5bkQKXPuGDF5pEnbJYG' as address, 0.03 as sol_staked
UNIon select 'EUFoxdtxBm5ELzLatzGtKFRK4DB1okfeNLFqKNv8iBnV' as address, 0.1 as sol_staked
UNIon select 'H2hBrGNvDuBEW1iSsyo95uarUhSKbzsnWxrs2Z8qTdHq' as address, 0.001 as sol_staked
UNIon select 'ATSSj1cgjt4JcXN7zKBFewY2xAKF2ohbKEwpcQ856ed8' as address, 0.05 as sol_staked
UNIon select 'AqsKuYXToEGneeKLFAxuXyPKHE4RCmgbXS4anj6NEfpr' as address, 0.0023 as sol_staked
UNIon select 'G6DtectifUx89FaeK78gwsZKphXrGDo6GKQcZwNtS4mA' as address, 0.1 as sol_staked
UNIon select 'DmLWjWvFr9goJQEA2jpW11mpQ2k8nrn7RmCvYzemZKuR' as address, 0.16 as sol_staked
UNIon select 'DRPeMowpMdNNLzmsq6TydXPiMUosLAgtcJGmxPAVDNYA' as address, 0.1 as sol_staked
UNIon select 'vohFQ9j3pgk551xyD6pohCkwxxapasX2qNTkhetPYU7' as address, 0.01011 as sol_staked
UNIon select '3nGhJ8wmZ7vAVRUCsuWTdq8An7N9YMSZ8sH2a3ynmipe' as address, 0.0058918 as sol_staked
UNIon select 'DswnewcJN71eiAKVxLgwE27scemeNgUBb25mXzU9jeoK' as address, 0.0123 as sol_staked
UNIon select 'FpV1AqkZGxr9iCUQx5yJU8Fzg2oqXuAbjTcuTzYrqCAN' as address, 0.06 as sol_staked
UNIon select '3bQT8FvumD1a1u3nM2ZHsojzELpEBwbWK6ps9tHrrT9G' as address, 0.116 as sol_staked
UNIon select '2Xbs34prndSh5mx2eTDdYXKKYoRsH5kcDArVajZrCUMd' as address, 0.0001 as sol_staked
UNIon select '81iDM43W1hyNqddhycWpajAxScyrYdF1z3QSdpoMKSbJ' as address, 0.007905 as sol_staked
UNIon select 'C1dgWTBLKjZ8zSxAsbzL79Qck2WpLjysia2WFgGhPQ9V' as address, 0.011 as sol_staked
UNIon select '3tPqP9dgAAVJjhqhL3GPRDc6rZ85VooiPARK1BtPca43' as address, 0.001 as sol_staked
UNIon select '2fngHvPw8r85PyXZWqBeSHJahq2upv5YwQpxMRe1ongB' as address, 0.116 as sol_staked
UNIon select 'Y8J88WnmbzNocKot8sLo6rJbri8gP1K99cgwM1wzEA4' as address, 0.5 as sol_staked
UNIon select '7zZHjmiz9qvsVhLCcq1RgXMWsBAFrpEDEZJqNeKvPw21' as address, 0.002 as sol_staked
UNIon select 'FL5foNqcom2yZ4eE4hLghTgmuEwrnahuYpJPs9D1UqER' as address, 0.01 as sol_staked
UNIon select 'EGnae4vAmDb4Q3Tj6BDT27Bpp5zMKVXN3gS3oXmcJnd5' as address, 0.16 as sol_staked
UNIon select 'EtqDLeSgpZ4jVxaqGx94mZkr9rjnykpgF95djzjFyYDs' as address, 0.00001 as sol_staked
UNIon select '4cb63HRNAWHNyh9zyTrEijcMbjY5BQuk5AcugAezLBng' as address, 0.0282 as sol_staked
UNIon select 'EWEWa4jZANb7VmDD6E3KHVkvUceHQQkeTANrJtb9P7dw' as address, 0.22 as sol_staked
UNIon select 'BQjyrHLcVX891EvT3XhdHMh9sATUdnMtcvSicDbHv9Di' as address, 0.1 as sol_staked
UNIon select 'FFvpDtXJYb2VumjckoJoicWBANUEK2SzeYNrUndTpNgF' as address, 0.001 as sol_staked
UNIon select '5XyzwMtJm3215xU3VvWC27jUkQ2auLnwzez3tUJMLNLP' as address, 0.01 as sol_staked
UNIon select 'GxtoKTtsaaTmYR2q5yJm3bJt8yP2zYzCpNonx7zymFGd' as address, 0.001 as sol_staked
UNIon select '6EfFnZuU6cZT6sfE4otzCTJ25UgjfgbeaAFSMm7gwzUd' as address, 0.27 as sol_staked
UNIon select 'CDZtnz5A63UomuVhcxFLW34Xy4vSMxGDzm59Lsj92VXN' as address, 0.25 as sol_staked
UNIon select '2Wna6QUddkjQcFoQPs8hrRbWadxvmasbpN5spEfj3hd9' as address, 0.1 as sol_staked
UNIon select '5SRuCf95tEkpEMDiWLFiziLAvDhEhckPKXp6esQ9HZhG' as address, 0.002 as sol_staked
UNIon select '78e2EnhJJ7PcYboCmJxLjrWLBNUSwBiQRkDz9nXAGgFQ' as address, 0.001 as sol_staked
UNIon select 'AEJhrEEZ98MnxN62WTKZW2Kt2GLu8A4KmyXWqT9jCiSE' as address, 0.001943731 as sol_staked
UNIon select 'Di1n56Xy6a8YuDLCkfigjFtFfmFNyZR4XxLr9EeKHzKA' as address, 0.1 as sol_staked
UNIon select 'F8zTdsQyA4wBCKP2e7iZ7qBsRBiTH6zsWnmVYgHmJvLo' as address, 0.1 as sol_staked
UNIon select '3hrqeWfzNxMwMy5TaGnsEhTrjRAotHetcJsFoND7B9yv' as address, 2 as sol_staked
UNIon select 'E4ht3T9TTQYjP5E2qmh4KhXtGmWe8q1Cy2N7HbzEivqa' as address, 0.03 as sol_staked
UNIon select 'Lzrtd8jtM3JKeDESdkHRDKpL7Zpu5uSYNoXXhRfaAzC' as address, 0.4 as sol_staked
UNIon select '8hVb3kunjQTrz3fPw89JqEgaBN7FRRSkg2LNvkxv8AR4' as address, 0.001 as sol_staked
UNIon select '9HdTq94vWxSv69dNfTwUrxfrrMsxwpEHmHnBS8CiAaeC' as address, 0.01 as sol_staked
UNIon select '4ayga4YENMCQY6S3yvSLrMW9WGFzj2EnuVowJ6QQbuAT' as address, 0.02 as sol_staked
UNIon select '3SjowGHPunJbS7yuZvh9rnmyEcMH47LCSDVU5Qv2d9WD' as address, 0.001 as sol_staked
UNIon select '7HsnxMzr3qghMPRqiLLzxFQdTQSuDLAas7JknQVKZeWe' as address, 0.15 as sol_staked
UNIon select '4bTuuvBcDt9LGvd6pPDhrvQ7Y5MsKbu9CwuPkmoY9gxk' as address, 1 as sol_staked
UNIon select 'GxEzfQzjh5vBreYwyiVySo5Ti78R2Uj4pPKeTxPkMewb' as address, 0.0161 as sol_staked
UNIon select 'E4pGsu49FxHiZq5hnZaADMcNUoQAc7mfJsqBvQcDyrdU' as address, 0.2 as sol_staked
)
, claim_txs as (

  select distinct

    block_timestamp
    , tx_id

  from solana.core.fact_events
  where succeeded
    and program_id = 'mERKcfxMC5SqJn4Ld4BUris3WKZZ1ojjWJ3A3J5CKxv'
    and block_timestamp > '2023-12-07 16:00:00'
),

transfers as (

  select
    tx_to
    , sum(amount) as jto_claimed
  from solana.core.fact_transfers
  inner join claim_txs
    using(block_timestamp, tx_id)
  where mint = 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL'
    and tx_from = 'HS8EQ8QkQSBJggY8r255AKdWbtYRtyNMoRt4LjNkkWm1'
    and block_timestamp > '2023-12-07 16:00:00'
    group by 1
)
select * 
from t1
left join transfers t
    on t.tx_to = t1.address




select t.impression_count
, coalesce(u.username, '') as username
, coalesce(d.title, '') as title
, coalesce(d.id, '') as dashboard_id
, concat('https://twitter.com/adriaparcerisas/status/', t.conversation_id) as tweet_url
, COUNT(distinct l.user_id) as n_likes
from bi_analytics.twitter.tweet t
left join bi_analytics.twitter.likes l
    on l.tweet_id = t.conversation_id
left join bi_analytics.velocity_app_prod.dashboards d
    on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
left join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
where t.created_at >= CURRENT_DATE - 1
group by 1, 2, 3, 4, 5
order by 6 desc

select u.username
, u.created_at::date as date
, p.*
from bi_analytics.gumby.pages p
left join bi_analytics.velocity_app_prod.users u
    on u.id = p.user_id
where timestamp >= current_date - 1
    and initial_referrer ILIKE '%science%'
LIMIT 100



with impressions as (
    select date_trunc('week', t.created_at) as week
    , d.id as dashboard_id
    , sum(impression_count) as impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    where t.created_at >= '2023-10-27'
    group by 1, 2
), chain0 as (
  select
    d.id as dashboard_id,
    case
      when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
      or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
      or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
      or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%' then 'Axelar'
      else INITCAP(t.name)
    end as chain,
    COUNT(distinct q.id) as n_queries
  from
    bi_analytics.velocity_app_prod.users u
    join bi_analytics.velocity_app_prod.dashboards d on d.created_by_id = u.id
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q on dtq.B = q.id
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t on qtt.B = t.id
    and t.type = 'project'
  group by
    1,
    2
),
chain as (
  select
    *,
    row_number() over (
      partition by dashboard_id
      order by
        case
          when chain in (
            'Aptos',
            'Avalanche',
            'Axelar',
            'Flow',
            'Near',
            'Sei',
            'Solana',
            'Thorchain'
          ) then 1
          else 2
        end,
        n_queries desc,
        chain
    ) as rn
  from
    chain0
),
t0 as (
    select
        date_trunc('hour', dr.dbt_updated_at) as hour,
        coalesce(m.user_id, d.created_by_id) as user_id,
        coalesce(tu.username, u.username) as username,
        d.id as dashboard_id,
        d.title,
        coalesce(c.chain, 'Polygon') as chain,
        coalesce(u.role, '') = 'internal'
        or u.username in (
            'Polaris_9R',
            'dsaber',
            'flipsidecrypto',
            'metricsdao',
            'drethereum',
            'Orion_9R',
            'sam',
            'forgash',
            'danner',
            'charliemarketplace',
            'theericstone'
        ) as internal_user,
        avg(dr.ranking_trending) as current_rank
    from
        bi_analytics.snapshots.hourly_dashboard_rankings dr
        join bi_analytics.velocity_app_prod.dashboards d
            on d.id = dr.dashboard_id
        left join bi_analytics.velocity_app_prod.profiles p
            on p.id = d.profile_id
        left join bi_analytics.velocity_app_prod.teams t
            on t.profile_id = p.id
        left join bi_analytics.velocity_app_prod.members m
            on t.id = m.team_id
            and m.role = 'owner'
        left join bi_analytics.velocity_app_prod.users tu
            on tu.id = m.user_id
        join bi_analytics.velocity_app_prod.users u
            on u.id = d.created_by_id
        left join chain c
            on c.dashboard_id = dr.dashboard_id
            and c.rn = 1
    group by 1, 2, 3, 4, 5, 6, 7
), t1 as (
    select username
    , dashboard_id
    , title
    , date_trunc('week', hour) as week
    , case
        when chain in (
            'Aptos',
            'Avalanche',
            'Axelar',
            'Flow',
            'Near',
            'Sei',
            'Solana',
            'Thorchain'
        ) then chain
        else 'Other'
    end as ecosystem,
    case
        when current_rank <= 10 then 1.5
        else 1
    end as base_amount,
    case
        when DATEADD('hours', -5, CURRENT_TIMESTAMP)::date >= '2023-12-06'
        and DATEADD('hours', -5, CURRENT_TIMESTAMP)::date <= '2023-12-13'
        and chain in ('Avalanche', 'Aptos') then 2
        when DATEADD('hours', -5, CURRENT_TIMESTAMP)::date >= '2023-11-01'
        and DATEADD('hours', -5, CURRENT_TIMESTAMP)::date < '2023-12-01'
        and chain in ('Flow', 'Sei', 'Near') then 1.5
        else 1
    end as boost,
    case
        when internal_user = false then base_amount * boost
        else 0
    end as amount
    from
    t0
    where
    current_rank <= 30
    and hour >= '2023-11-01'
), t2 as (
    select t1.username
    , t1.dashboard_id
    , t1.title
    , t1.week
    , t1.ecosystem
    , coalesce(i.impression_count, 0) as impression_count
    , sum(amount) as amount_usd
    from t1
    left join impressions i
        on i.dashboard_id = t1.dashboard_id
        and i.week = t1.week
    group by 1, 2, 3, 4, 5, 6
), t3 as (
    select ecosystem
    , week
    , sum(impression_count) as impression_count
    , sum(amount_usd) as amount_usd
    from t2
)
select *
, impression_count / amount_usd as impressions_per_dollar
from t3


with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 7
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 7
        and is_imputed = FALSE
    group by 1, 2
)
select t.block_timestamp::date as date
, t.tx_id
, t.tx_from
, t.tx_to
, t.mint
, t.amount
, t.amount * coalesce(p0.price, p1.price) as amount_usd
, tx.log_messages
from solana.core.fact_transactions tx
join solana.core.fact_transfers t
    on t.block_timestamp = tx.block_timestamp
    and t.tx_id = tx.tx_id
left join p0
    on p0.hour = date_trunc('hour', t.block_timestamp)
    and p0.mint = t.mint
join p1
    on p1.date = date_trunc('day', t.block_timestamp)
    and p1.mint = t.mint
where tx.block_timestamp >= CURRENT_DATE - 7
    and t.block_timestamp >= CURRENT_DATE - 7
    and tx.signers[0] = 'GbbTBXhumMZiBQrDRQTR1SUH1oU2sVsCULmGiFgmZTW9'


select t.block_timestamp::date as date
, count(1)
, count(distinct t.tx_id)
from solana.core.fact_transfers t
join solana.core.fact_transactions tx
    on t.block_timestamp = tx.block_timestamp
    and t.tx_id = tx.tx_id
where t.block_timestamp >= '2023-04-12'
    and tx.block_timestamp >= '2023-04-12'
    and t.tx_to = '41zCUJsKk6cMB94DDtm99qWmyMZfp4GkAhhuz4xTwePu'
    and tx.log_messages::string like '%Program MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr invoke [1]%'
group by 1
order by 2 desc

with labels as (
    select c.value:dashboard_id::string as dashboard_id
    , c.value:tag::string as dashboard_tag
    from crosschain.bronze.data_science_uploads
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'dashboard-tags%'
), pages as (
    select id
    , user_id
    , context_page_tab_url
    , timestamp
    , anonymous_id
    , context_ip
    , case when context_page_referring_domain = 't.co' then 1 else 0 end as n_views_twitter
    from bi_analytics.gumby.pages
    UNION
    select id
    , user_id
    , context_page_tab_url
    , timestamp
    , anonymous_id
    , context_ip
    , case when context_page_referring_domain = 't.co' then 1 else 0 end as n_views_twitter
    from bi_analytics.flipside_app_prod.pages
), impressions as (
    select date_trunc('week', t.created_at) as week
    , d.id as dashboard_id
    , d.title
    , u.username
    , count(distinct t.conversation_id) as n_tweets
    , sum(impression_count)::float as impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where week >= '2023-06-01'
        and not d.id in (
        select dashboard_id from labels where dashboard_tag = 'bot'
    )
    group by 1, 2, 3, 4
), chain0 as (
  select
    d.id as dashboard_id,
    case
      when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
      or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
      or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
      or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%' then 'Axelar'
      else INITCAP(t.name)
    end as chain,
    COUNT(distinct q.id) as n_queries
  from
    bi_analytics.velocity_app_prod.users u
    join bi_analytics.velocity_app_prod.dashboards d on d.created_by_id = u.id
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q on dtq.B = q.id
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t on qtt.B = t.id
    and t.type = 'project'
  group by
    1,
    2
), chain_pct as (
    select *
    , n_queries / sum(n_queries) over (partition by dashboard_id) as pct
    from chain0
), page_views1 as (
    select date_trunc('week', p.timestamp) as week
    , d.id as dashboard_id
    , d.title
    , u.username
    , COUNT(distinct concat(d.id, p.timestamp::date, coalesce(p.user_id, p.anonymous_id))) as page_views
    from pages p
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(p.context_page_tab_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where not d.id in (
        select dashboard_id from labels where dashboard_tag = 'bot'
    )
    group by 1, 2, 3, 4
), weekly_impressions as (
    select week
    , title
    , i.dashboard_id
    , username
    , case when chain in (
        'Aptos',
        'Avalanche',
        'Axelar',
        'Flow',
        'Near',
        'Sei',
        'Solana',
        'Thorchain'
    ) then chain else 'Other' end as ecosystem
    , sum(impression_count * pct)::float as weighted_impressions
    , sum(n_tweets * pct)::float as weighted_tweets
    from impressions i
    join chain_pct c
        on c.dashboard_id = i.dashboard_id
    where not i.dashboard_id in (
        'c511da88-7d93-49ca-9d6f-66e0e7dcc99d'
        , '56ee7f04-e182-4d65-aa30-55d73f9d58e3'
        , 'c45877dd-b132-4234-8b61-764daebd1353'
        , 'd24b2a75-1a04-434e-bc8c-287920de2fc5'
        , '39bd3a3e-1ab3-45c9-bb50-daf1ed6fc976'
        , '3ee56d10-38c5-435e-85c0-7a335d946a51'
    )
    group by 1, 2, 3, 4, 5
), weekly_page_views as (
    select week
    , p.title
    , p.dashboard_id
    , username
    , case when chain in (
        'Aptos',
        'Avalanche',
        'Axelar',
        'Flow',
        'Near',
        'Sei',
        'Solana',
        'Thorchain'
    ) then chain else 'Other' end as ecosystem
    , sum(page_views * pct)::float as weighted_page_views
    from page_views1 p
    join chain_pct c
        on c.dashboard_id = p.dashboard_id
    where not p.dashboard_id in (
        'c511da88-7d93-49ca-9d6f-66e0e7dcc99d'
        , '56ee7f04-e182-4d65-aa30-55d73f9d58e3'
        , 'c45877dd-b132-4234-8b61-764daebd1353'
        , 'd24b2a75-1a04-434e-bc8c-287920de2fc5'
        , '39bd3a3e-1ab3-45c9-bb50-daf1ed6fc976'
        , '3ee56d10-38c5-435e-85c0-7a335d946a51'
    )
    group by 1, 2, 3, 4, 5
), t1 as (
    select coalesce(i.week, p.week) as week
    -- , coalesce(i.dashboard_id, p.dashboard_id) as dashboard_id
    -- , coalesce(i.title, p.title) as title
    -- , coalesce(i.username, p.username) as username
    , coalesce(i.ecosystem, p.ecosystem) as ecosystem
    , count(distinct i.username) as n_users
    , sum(coalesce(weighted_impressions, 0)) as weighted_impressions
    , sum(coalesce(weighted_page_views, 0)) as weighted_page_views
    from weekly_impressions i
    FULL OUTER join weekly_page_views p
        on p.week = i.week
        and p.ecosystem = i.ecosystem
        and p.username = i.username
        and p.title = i.title
    group by 1, 2
)
select *
, weighted_impressions / sum(weighted_impressions) over (partition by week) as pct_impressions
, weighted_page_views / sum(weighted_page_views) over (partition by week) as pct_page_views
, n_users / sum(n_users) over (partition by week) as pct_users
from t1




with labels as (
    select c.value:dashboard_id::string as dashboard_id
    , c.value:tag::string as dashboard_tag
    from crosschain.bronze.data_science_uploads
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'dashboard-tags%'
), pages as (
    select id
    , user_id
    , context_page_tab_url
    , timestamp
    , anonymous_id
    , context_ip
    , case when context_page_referring_domain = 't.co' then 1 else 0 end as n_views_twitter
    from bi_analytics.gumby.pages
    UNION
    select id
    , user_id
    , context_page_tab_url
    , timestamp
    , anonymous_id
    , context_ip
    , case when context_page_referring_domain = 't.co' then 1 else 0 end as n_views_twitter
    from bi_analytics.flipside_app_prod.pages
), impressions as (
    select case 
        when t.created_at < '2023-01-01' then '2022'
        when t.created_at < '2023-04-01' then '2023 Q1'
        when t.created_at < '2023-07-01' then '2023 Q2'
        when t.created_at < '2023-10-01' then '2023 Q3'
        else '2023 Q4' end as week
    , d.id as dashboard_id
    , d.title
    , u.username
    , sum(impression_count)::float as impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where 1=1
        -- week >= '2023-06-01'
        and not d.id in (
        select dashboard_id from labels where dashboard_tag = 'bot'
    )
    group by 1, 2, 3, 4
), chain0 as (
  select
    d.id as dashboard_id,
    case
      when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
      or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
      or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
      or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%' then 'Axelar'
      else INITCAP(t.name)
    end as chain,
    COUNT(distinct q.id) as n_queries
  from
    bi_analytics.velocity_app_prod.users u
    join bi_analytics.velocity_app_prod.dashboards d on d.created_by_id = u.id
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q on dtq.B = q.id
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t on qtt.B = t.id
    and t.type = 'project'
  group by
    1,
    2
), chain_pct as (
    select *
    , n_queries / sum(n_queries) over (partition by dashboard_id) as pct
    from chain0
), page_views1 as (
    select case 
        when p.timestamp < '2023-01-01' then '2022'
        when p.timestamp < '2023-04-01' then '2023 Q1'
        when p.timestamp < '2023-07-01' then '2023 Q2'
        when p.timestamp < '2023-10-01' then '2023 Q3'
        else '2023 Q4' end as week
    , d.id as dashboard_id
    , d.title
    , u.username
    , COUNT(distinct concat(d.id, p.timestamp::date, coalesce(p.user_id, p.anonymous_id))) as page_views
    from pages p
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(p.context_page_tab_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where not d.id in (
        select dashboard_id from labels where dashboard_tag = 'bot'
    )
        and p.timestamp >= '2022-07-01'
    group by 1, 2, 3, 4
), weekly_impressions as (
    select week
    , title
    , i.dashboard_id
    , username
    , case when chain in (
        'Aptos',
        'Avalanche',
        'Axelar',
        'Flow',
        'Near',
        'Sei',
        'Solana',
        'Thorchain'
    ) then chain else 'Other' end as ecosystem
    , sum(impression_count * pct)::float as weighted_impressions
    from impressions i
    join chain_pct c
        on c.dashboard_id = i.dashboard_id
    where not i.dashboard_id in (
        'c511da88-7d93-49ca-9d6f-66e0e7dcc99d'
        , '56ee7f04-e182-4d65-aa30-55d73f9d58e3'
        , 'c45877dd-b132-4234-8b61-764daebd1353'
        , 'd24b2a75-1a04-434e-bc8c-287920de2fc5'
        , '39bd3a3e-1ab3-45c9-bb50-daf1ed6fc976'
        , '3ee56d10-38c5-435e-85c0-7a335d946a51'
    )
    group by 1, 2, 3, 4, 5
), weekly_page_views as (
    select week
    , p.title
    , p.dashboard_id
    , username
    , case when chain in (
        'Aptos',
        'Avalanche',
        'Axelar',
        'Flow',
        'Near',
        'Sei',
        'Solana',
        'Thorchain'
    ) then chain else 'Other' end as ecosystem
    , sum(page_views * pct)::float as weighted_page_views
    from page_views1 p
    join chain_pct c
        on c.dashboard_id = p.dashboard_id
    where not p.dashboard_id in (
        'c511da88-7d93-49ca-9d6f-66e0e7dcc99d'
        , '56ee7f04-e182-4d65-aa30-55d73f9d58e3'
        , 'c45877dd-b132-4234-8b61-764daebd1353'
        , 'd24b2a75-1a04-434e-bc8c-287920de2fc5'
        , '39bd3a3e-1ab3-45c9-bb50-daf1ed6fc976'
        , '3ee56d10-38c5-435e-85c0-7a335d946a51'
    )
    group by 1, 2, 3, 4, 5
), t1 as (
    select coalesce(i.week, p.week) as month
    -- , coalesce(i.dashboard_id, p.dashboard_id) as dashboard_id
    -- , coalesce(i.title, p.title) as title
    -- , coalesce(i.username, p.username) as username
    , coalesce(i.ecosystem, p.ecosystem) as ecosystem
    , sum(coalesce(weighted_impressions, 0))::float as weighted_impressions
    , sum(coalesce(weighted_page_views, 0))::float as weighted_page_views
    from weekly_impressions i
    FULL OUTER join weekly_page_views p
        on p.week = i.week
        and p.ecosystem = i.ecosystem
        and p.username = i.username
        and p.title = i.title
    group by 1, 2
), t2 as (
    select *
    , 100.0 * (case when weighted_impressions = 0 then 0.0 else weighted_impressions / sum(weighted_impressions) over (partition by month) end)::float as pct_impressions
    , 100.0 * (weighted_page_views / sum(weighted_page_views) over (partition by month))::float as pct_page_views
    from t1
)
select * from t2
where ecosystem = 'Solana'




with labels as (
    select c.value:dashboard_id::string as dashboard_id
    , c.value:tag::string as dashboard_tag
    from crosschain.bronze.data_science_uploads
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'dashboard-tags%'
), pages as (
    select id
    , user_id
    , context_page_tab_url
    , timestamp
    , anonymous_id
    , context_ip
    , case when context_page_referring_domain = 't.co' then 1 else 0 end as n_views_twitter
    from bi_analytics.gumby.pages
    UNION
    select id
    , user_id
    , context_page_tab_url
    , timestamp
    , anonymous_id
    , context_ip
    , case when context_page_referring_domain = 't.co' then 1 else 0 end as n_views_twitter
    from bi_analytics.flipside_app_prod.pages
), impressions as (
    select date_trunc('week', t.created_at) as week
    , d.id as dashboard_id
    , d.title
    , u.username
    , count(distinct t.conversation_id) as n_tweets
    , sum(impression_count)::float as impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where 1=1
        -- week >= '2023-06-01'
        and not d.id in (
            select dashboard_id from labels where dashboard_tag = 'bot'
        )
    group by 1, 2, 3, 4
), chain0 as (
  select
    d.id as dashboard_id,
    case
      when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
      or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
      or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
      or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%' then 'Axelar'
      else INITCAP(t.name)
    end as chain,
    COUNT(distinct q.id) as n_queries
  from
    bi_analytics.velocity_app_prod.users u
    join bi_analytics.velocity_app_prod.dashboards d on d.created_by_id = u.id
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q on dtq.B = q.id
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t on qtt.B = t.id
    and t.type = 'project'
  group by
    1,
    2
), chain_pct as (
    select *
    , n_queries / sum(n_queries) over (partition by dashboard_id) as pct
    from chain0
), page_views1 as (
    select date_trunc('week', p.timestamp) as week
    , d.id as dashboard_id
    , d.title
    , u.username
    , COUNT(distinct concat(d.id, p.timestamp::date, coalesce(p.user_id, p.anonymous_id))) as page_views
    from pages p
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(p.context_page_tab_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where not d.id in (
        select dashboard_id from labels where dashboard_tag = 'bot'
    )
        and p.timestamp >= '2022-07-01'
    group by 1, 2, 3, 4
), weekly_impressions as (
    select week
    , title
    , i.dashboard_id
    , username
    , case when chain in (
        'Aptos',
        'Avalanche',
        'Axelar',
        'Flow',
        'Near',
        'Sei',
        'Solana',
        'Thorchain'
    ) then chain else 'Other' end as ecosystem
    , sum(impression_count * pct)::float as weighted_impressions
    , sum(n_tweets * pct)::float as weighted_tweets
    from impressions i
    join chain_pct c
        on c.dashboard_id = i.dashboard_id
    where not i.dashboard_id in (
        'c511da88-7d93-49ca-9d6f-66e0e7dcc99d'
        , '56ee7f04-e182-4d65-aa30-55d73f9d58e3'
        , 'c45877dd-b132-4234-8b61-764daebd1353'
        , 'd24b2a75-1a04-434e-bc8c-287920de2fc5'
        , '39bd3a3e-1ab3-45c9-bb50-daf1ed6fc976'
        , '3ee56d10-38c5-435e-85c0-7a335d946a51'
    )
    group by 1, 2, 3, 4, 5
), weekly_page_views as (
    select week
    , p.title
    , p.dashboard_id
    , username
    , case when chain in (
        'Aptos',
        'Avalanche',
        'Axelar',
        'Flow',
        'Near',
        'Sei',
        'Solana',
        'Thorchain'
    ) then chain else 'Other' end as ecosystem
    , sum(page_views * pct)::float as weighted_page_views
    from page_views1 p
    join chain_pct c
        on c.dashboard_id = p.dashboard_id
    where not p.dashboard_id in (
        'c511da88-7d93-49ca-9d6f-66e0e7dcc99d'
        , '56ee7f04-e182-4d65-aa30-55d73f9d58e3'
        , 'c45877dd-b132-4234-8b61-764daebd1353'
        , 'd24b2a75-1a04-434e-bc8c-287920de2fc5'
        , '39bd3a3e-1ab3-45c9-bb50-daf1ed6fc976'
        , '3ee56d10-38c5-435e-85c0-7a335d946a51'
    )
    group by 1, 2, 3, 4, 5
), t1 as (
    select username
    , sum( case when week < '2023 Q3' then weighted_page_views else 0 end) as n_1
    , sum( case when week < '2023 Q3' then 0 else weighted_page_views end) as n_2
    from weekly_page_views
    where chain = 'Solana'
    group by 1
)
select *
, n_2 - n_1 as dff
from t1
order by dff desc


select trailheadId
, count(distinct address)
from bi_analytics.bronze.arprograms_hike
where coalesce(blockchain, 'Solana') = 'Solana'
    and result = 'VERIFIED'
group by 1
order by 2 desc



with hikes as (
    select distinct txId
    from bi_analytics.bronze.arprograms_hike
    where coalesce(blockchain, 'Solana') = 'Solana'
), t0 as (
    select e.signers[0]::string as address
    , coalesce(a.stake_pool_name, e.program_id) as program_id
    , (e.block_timestamp)::date as date
    , sum(1) as n_tx
    , MAX(case when e.tx_id in (select txId from hikes) then 1 else 0 end) as is_trails
    from solana.core.fact_events e
    left join solana.defi.fact_stake_pool_actions a
        on a.block_timestamp = e.block_timestamp
        and a.tx_id = e.tx_id
    where e.block_timestamp >= '2023-04-25'
        and e.succeeded = TRUE
        and program_id in (
            'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB',
            'JUP5pEAZeHdHrLxh5UCwAbpjGwYKKoquCpda2hfP4u8',
            'JUP5cHjnnCx2DppVsufsLrXs8EBZeEZzGtEK9Gdz6ow',
            'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',
            'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN',
            'TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp',
            '9ehXDD5bnhSpFVRf99veikjgq8VajtRH7e3D9aVPLqYd',
            'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN',
            'TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp',
            'BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p',
            'jupoNjAxXgZ4rjzxzPMP4oxduvQsQtZzyknqvzYNrNu',
            'jupoNjAxXgZ4rjzxzPMP4oxduvQsQtZzyknqvzYNrNu',
            'ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXpkf3aD',
            'ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXpkf3aD',
            'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN',
            'TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp',
            'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN',
            'TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp',
            'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN',
            'TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp',
            'BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p',
            'JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp',
            'JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp',
            'QMNeHCGYnLVDn1icRAfQZpjPLBNkfGbSKRB83G5d8KB',
            'tovt1VkTE2T4caWoeFP6a2xSFoew5mNpd7FWidyyMuk',
            'MR2LqxoSbw831bNy68utpu5n4YqBH3AzDmddkgk9LQv',
            'MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD',
            'QMNeHCGYnLVDn1icRAfQZpjPLBNkfGbSKRB83G5d8KB',
            'QMNeHCGYnLVDn1icRAfQZpjPLBNkfGbSKRB83G5d8KB',
            '6q5ZGhEj6kkmEjuyCXuH4x8493bpi9fNzvy9L8hX83HQ',
            'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH',
            'updg8JyjrmFE2h3d71p71zRXDR8q4C6Up8dDoeq3LTM',
            'GLsSp8Dr9EAe5UL67XmmjA3c8qqwYgNeD63pLZEcVGCw',
            'So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo',
            'So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo',
            'So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo',
            'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA',
            'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA',
            'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB',
            'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH',
            'PLENDj46Y4hhqitNV2WqLqGLrWKAaH2xJHm2UyHgJLY',
            'PSYFiYqguvMXwpDooGdYV6mju92YEbFobbvW617VNcq',
            'PLENDj46Y4hhqitNV2WqLqGLrWKAaH2xJHm2UyHgJLY',
            'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP',
            'ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXpkf3aD',
            'ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXpkf3aD',
            'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
            , 'SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy'
            , 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'
            , '6LtLpnUFNByNXLyCoK9wA2MykKAmQNZKBdY8s47dehDc'
        )
    group by 1, 2, 3
), t1 as (
    select coalesce(
        case
            when l.label = 'tensorswap' then 'tensor'
            when l.label = 'marinade finance' then 'marinade'
            else l.label end
        , case when program_id = 'TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp' then 'tensor'
        when program_id = 'JUP5pEAZeHdHrLxh5UCwAbpjGwYKKoquCpda2hfP4u8' then 'jupiter'
        when program_id = 'JUP5cHjnnCx2DppVsufsLrXs8EBZeEZzGtEK9Gdz6ow' then 'jupiter'
        when program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4' then 'jupiter'
        when program_id = 'jupoNjAxXgZ4rjzxzPMP4oxduvQsQtZzyknqvzYNrNu' then 'jupiter' -- limit order
        when program_id = 'MR2LqxoSbw831bNy68utpu5n4YqBH3AzDmddkgk9LQv' then 'marinade'
        when program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA' then 'marginfi'
        when program_id = 'updg8JyjrmFE2h3d71p71zRXDR8q4C6Up8dDoeq3LTM' then 'solarplex'
        when program_id = 'GLsSp8Dr9EAe5UL67XmmjA3c8qqwYgNeD63pLZEcVGCw' then 'solarplex'
        when program_id = 'BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p' then 'bonkswap'
        when program_id = 'PLENDj46Y4hhqitNV2WqLqGLrWKAaH2xJHm2UyHgJLY' then 'psyfinance'
        when program_id = 'tovt1VkTE2T4caWoeFP6a2xSFoew5mNpd7FWidyyMuk' then 'marinade'
        when program_id = '6q5ZGhEj6kkmEjuyCXuH4x8493bpi9fNzvy9L8hX83HQ' then 'aver'
        when program_id = 'SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy' then 'staking'
        when program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA' then 'jito'
        else null end
        , t0.program_id
    ) as program
    , t0.address
    , (date) as date
    , max(is_trails) as is_trails
    , sum(n_tx) as n_tx
    from t0
    left join solana.core.dim_labels l
        on l.address = t0.program_id
    group by 1, 2, 3
), t2 as (
    select program
    , address
    , MIN(date) as first_date
    , max(is_trails) as is_trails
    from t1
    group by 1, 2
), t3 as (
    select t2.program
    , date_trunc('month', date) as month
    , t2.address
    , max(t1.is_trails) as is_trails
    , sum(n_tx) as n_tx
    from t1
    join t2
        on t2.program = t1.program
        and t2.address = t1.address
    group by 1, 2, 3
), t4 as (
    select month
    , program
    , sum(is_trails) as tot_trails
    , avg(is_trails) as avg_trails
    , count(1) as n_wallets
    from t3
    group by 1
), t5 as (
    select *
    , row_number() over (partition by program order by pct_trails desc) as rn
    from t4
)
select month
, program
, avg_trails::float as pct_users_from_trails
from t5
where rn = 1
and project in (
    'aver'
    , 'blazestake'
    , 'bonkswap'
    , 'drift'
    , 'famous foxes'
    , 'jito'
    , 'jupiter'
    , 'kamino'
    , 'marginfi'
    , 'marinade'
    , 'psyfinance'
    , 'solarplex'
    , 'solend'
    , 'tensor'
    , 'zeta'
)




select * from solana.price.ez_token_prices_hourly
where hour >= '2023-12-11'
    and hour <= '2023-12-14'
    and token_address = 'n54ZwXEcLnc3o7zK48nhrLV4KTU5wWD4iq7Gvdt5tik'
order by hour


select *
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.users u
    on u.id = q.created_by_id
    where u.username = 'marky'

 -- forked from Goose FX Fees @ https://flipsidecrypto.xyz/edit/queries/c889683d-5e3e-4b45-b12c-295c35f502b9

with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= CURRENT_DATE - 30
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 30
        and is_imputed = FALSE
    group by 1, 2
), p2 as (
    select token_address as mint
    , date_trunc('week', recorded_hour) as week
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 30
        and is_imputed = FALSE
    group by 1, 2
), t0 as (
    select block_timestamp::date as date
    , sum(amount * coalesce(p0.price, p1.price, 0)) as fees_usd
    from solana.core.fact_transfers t
    left join p0
        on p0.hour = date_trunc('hour', t.block_timestamp)
        and p0.mint = t.mint
    left join p1
        on p1.date = date_trunc('day', t.block_timestamp)
        and p1.mint = t.mint
    left join p2
        on p2.week = date_trunc('week', t.block_timestamp)
        and p2.mint = t.mint
    where t.block_timestamp >= CURRENT_DATE - 30
        and t.tx_to in ('73a2yN8Qd4dN8g9RJBeHCUywMdXaKoh7LRuF7Gj9dL8Z')
    group by 1
)
select *
, sum(fees_usd) over (order by date) as cumu_fees_usd
from t0 

 

 -- forked from Goose FX Fees @ https://flipsidecrypto.xyz/edit/queries/c889683d-5e3e-4b45-b12c-295c35f502b9

with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= CURRENT_DATE - 60
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 60
        and is_imputed = FALSE
    group by 1, 2
), p2 as (
    select token_address as mint
    , date_trunc('week', recorded_hour) as week
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 60
        and is_imputed = FALSE
    group by 1, 2
), t0 as (
    select block_timestamp::date as date
    , sum(amount * coalesce(p0.price, p1.price, 0)) as fees_usd
    from solana.core.fact_transfers t
    left join p0
        on p0.hour = date_trunc('hour', t.block_timestamp)
        and p0.mint = t.mint
    left join p1
        on p1.date = date_trunc('day', t.block_timestamp)
        and p1.mint = t.mint
    left join p2
        on p2.week = date_trunc('week', t.block_timestamp)
        and p2.mint = t.mint
    where t.block_timestamp >= CURRENT_DATE - 60
        and t.tx_to in ('73a2yN8Qd4dN8g9RJBeHCUywMdXaKoh7LRuF7Gj9dL8Z')
    group by 1
)
select *
, sum(fees_usd) over (order by date) as cumu_fees_usd
from t0 

 





with p0 as (
    select token_address
    , DATE_TRUNC('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= CURRENT_DATE - 7
        and is_imputed = FALSE
        and close < 100000
    group by 1, 2
), p1 as (
    select token_address
    , recorded_hour::date as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= CURRENT_DATE - 7
        and is_imputed = FALSE
        and close < 100000
    group by 1, 2
), t0 as (
    select distinct e.tx_id
    , e.block_timestamp
    , e.signers[0]::string as signer
    from solana.core.fact_events e
    join solana.core.fact_transactions tx
        on tx.block_timestamp = e.block_timestamp
        and tx.tx_id = e.tx_id
    where e.block_timestamp >= CURRENT_DATE - 7
        and tx.block_timestamp >= CURRENT_DATE - 7
        and e.program_id = 'FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X'
        and tx.log_messages::string like '%Program log: Instruction: Swap%'
)
, t1 as (
    select s.tx_id
    , s.block_timestamp
    , max(amount * coalesce(p0.price, p1.price, 0)) as usd_amount
    from t0
    join solana.core.fact_transfers s
        on s.block_timestamp = t0.block_timestamp
        and s.tx_id = t0.tx_id
    left join p0
        on p0.token_address = s.mint
        and p0.hour = DATE_TRUNC('hour', s.block_timestamp)
    join p1
        on p1.token_address = s.mint
        and p1.date = s.block_timestamp::date
    where s.block_timestamp >= CURRENT_DATE - 7
    group by 1, 2
)
, t2 as (
    select block_timestamp::date as date
    , sum(usd_amount) as volume_usd
    , sum(usd_amount * 0.0075) as fees_usd
    from t1
    group by 1
)
select *
, sum(volume_usd) over (order by date) as cumu_volume_usd
, sum(fees_usd) over (order by date) as cumu_fees_usd
from t2


-- used for Anybodies, Kamino, Famous Foxes, Dual Finance, Degen Coin Flip, Star Atlas, Banx, Sharky, and Goose Fx
with p0 as (
    select token_address as mint
    , DATE_TRUNC('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 7
        and is_imputed = FALSE
        and close < 1000000
    group by 1, 2
), p1 as (
    select token_address as mint
    , DATE_TRUNC('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 7
        and is_imputed = FALSE
        and close < 1000000
    group by 1, 2
), wallets as (
    select distinct address
    , l.label
    , l.label_subtype
    from solana.core.dim_labels l
    where l.label in (
        'banx'
    )
    and l.label_subtype in ('fee_wallet','rewards','treasury','general_contract')
), e as (
    select e.block_timestamp
    , e.tx_id
    , e.program_id
    , l.label
    from solana.core.fact_events e
    join wallets l
        on l.address = e.program_id
        and l.label_subtype = 'general_contract'
    where block_timestamp >= CURRENT_DATE - 7
        and e.program_id in (select address from wallets where label_subtype = 'general_contract')
    qualify(
        -- make sure we are only pulling 1 row per tx
        -- (accounting for the case where there are 2+ program ids from our list in a single tx)
        -- (probably a better way to do this, can improve at some point)
        row_number() over (partition by tx_id, l.label order by program_id) = 1
    )
), t0 as (
    select INITCAP(coalesce(l.label, l2.label)) as project
    , t.block_timestamp::date as date
    , t.tx_id
    , 1 as mult
    , t.tx_from
    , t.tx_to
    , t.mint
    , t.amount
    , (amount * coalesce(p0.price, p1.price, 0) * mult) as fees_usd
    from solana.core.fact_transfers t
    left join p0
        on p0.mint = t.mint
        and p0.hour = DATE_TRUNC('hour', t.block_timestamp)
    join p1
        on p1.mint = t.mint
        and p1.date = t.block_timestamp::date
    left join e
        on e.block_timestamp = t.block_timestamp
        and e.tx_id = t.tx_id
    left join wallets l
        on l.address = t.tx_to
        -- and l.label = e.label
    left join wallets l2
        on l2.address = t.tx_from
        -- and l2.label = e.label
    where t.block_timestamp >= CURRENT_DATE - 7
        and (
            (
                -- program id and to address
                t.tx_to in (select address from wallets)
                and l.label_subtype in ('fee_wallet','rewards','treasury','allbridge')
                and l.label = e.label
            )
        )
    group by 1, 2, 3
)
select *
from t0

with t0 as (
    select split(clean_url,'/')[3]::string as user
    , platform
    , conversation_id
    , impression_count
    from bi_analytics.twitter.tweet t
    where created_at >= '2023-11-01'
        and tweet_type = 'Dashboard'
    qualify (
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
), t1 as (
    select user
    , platform
    , count(distinct conversation_id) as n_tweets
    , sum(impression_count) as n_impressions
    from t0
    group by 1, 2
), t2 as (
    select case
        when n_tweets = 1 then 'A: 1'
        when n_tweets < 5 then 'B: 2-4'
        when n_tweets < 10 then 'C: 5-9'
        when n_tweets < 20 then 'D: 10-19'
        else 'E: 20+' end as n_tweets
    , platform
    , count(1) as n_users
    from t1
    group by 1, 2
)
select *
from t2



with t0 as (
    select split(clean_url,'/')[3]::string as user
    , platform
    , tweet_url
    , conversation_id
    , impression_count
    from bi_analytics.twitter.tweet t
    where created_at >= '2023-11-01'
        and tweet_type = 'Dashboard'
    qualify (
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
), t1 as (
    select user
    , platform
    , count(distinct conversation_id) as n_tweets
    , sum(impression_count) as n_impressions
    from t0
    group by 1, 2
)
, t2 as (
    select t0.*
    , t1.n_tweets
    , t1.n_impressions
    from t0
    join t1
        on t0.user = t1.user
        and t0.platform = t1.platform
)
select *
from t2

with p0 as (
    select token_address as mint
    , DATE_TRUNC('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 7
        and is_imputed = FALSE
        and close < 1000000
    group by 1, 2
), p1 as (
    select token_address as mint
    , DATE_TRUNC('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 7
        and is_imputed = FALSE
        and close < 1000000
    group by 1, 2
), t0 as (
    select t.block_timestamp::date as date
    , t.tx_id
    , 1 as mult
    , t.tx_from
    , t.tx_to
    , t.mint
    , t.amount
    , (amount * coalesce(p0.price, p1.price, 0) * mult) as fees_usd
    from solana.core.fact_transfers t
    left join p0
        on p0.mint = t.mint
        and p0.hour = DATE_TRUNC('hour', t.block_timestamp)
    join p1
        on p1.mint = t.mint
        and p1.date = t.block_timestamp::date
      where t.block_timestamp >= current_date - 7 and tx_to in (
        'FLUXR4McuD2iXyP3wpP4XTjSWmB86ppMiyoA52UA9bKb', -- FeeWallet 
        '4RNnWnJeyy6myqFW4anPDJtmhnZTdSMDo2HWjfBiDcLc' -- JupiterReferralWallet
      )
    
), t1 as (
    select date
    , sum(fees_usd) as fees_usd
    from t0
    group by 1
)
select *
, sum(fees_usd) over (order by date) as cumu_fees_usd
from t1

with t0 as (
    select e.tx_id
    , e.block_timestamp::date as date
    , i.value:parsed:type::string as type
    , i.value:parsed:info:amount::int as amount
    , i.value:parsed:info:mint::string as mint
    , amount * 0.00001 as amount_usd
    from solana.core.fact_events e
    , lateral flatten (
        input => inner_instruction:instructions
    ) i
    where block_timestamp >= current_date - 30
        and program_id = 'hemjuPXBpNvggtaUnN1MwT3wrdhttKEfosTcc2P9Pg8'
        and type = 'burn'
        and mint = 'dcuc8Amr83Wz27ZkQ2K9NS6r8zRpf1J6cvArEBDZDmm'
), t1 as (
    select date
    , sum(amount_usd) as fees_usd
    from t0
    group by 1
    order by 1 desc
)
select *
, sum(fees_usd) over (order by date) as cumu_fees_usd
from t1
order by date

with t0 as (
    select distinct l.address
    , e.tx_id
    from solana.core.dim_labels l
    join solana.core.fact_events e
        on e.program_id = l.address
    where e.block_timestamp >= current_date - 1
        and l.label = 'metaplex'
        and l.label_subtype != 'nf_token_contract'
        and succeeded
    qualify(
        row_number() over (partition by e.program_id order by e.tx_id) <= 20
    )
)
select t0.*
, l.value::string as message
from t0
join solana.core.fact_transactions tx
    on tx.tx_id = t0.tx_id
, lateral flatten(
    input => log_messages
) l
where tx.block_timestamp >= current_date - 1

select * 
from solana.nft.fact_nft_mints
limit 10


with e as (
    select distinct block_timestamp
    , tx_id
    , program_id
    from solana.core.fact_events
    where block_timestamp >= current_date - 7
        and program_id = 'AYGdvqsQruZoaJPWsViLqUgtbfXGRnxzgxzW4zmbbckL'
)
select t.tx_id
, t.block_timestamp::date as date
, amount
, case 
    when amount = 1 then 100
    when amount = 4.95 then 500
    when amount = 9.80 then 1000      
    when amount = 23.75 then 2500
    when amount = 90 then 10000
    when amount = 212.5 then 25000
    when amount = 800 then 100000
    end as droplets_purchased
, e.program_id
from solana.core.fact_transfers t
left join e
    on e.block_timestamp = t.block_timestamp
    and e.tx_id = t.tx_id
where tx_to = 'DRiPRi6xGZ2pEiLXBxzoecJ9urdx7AL7JHwyn3V9pwXZ'
    and t.block_timestamp > current_date - 7
order by t.amount desc

with p0 as (
    select token_address as mint
    , DATE_TRUNC('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where DATE_TRUNC('hour', recorded_hour) >= CURRENT_DATE - 30
        and is_imputed = FALSE
        and close < 1000000
    group by 1, 2
), p1 as (
    select token_address as mint
    , DATE_TRUNC('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where DATE_TRUNC('hour', recorded_hour) >= CURRENT_DATE - 30
        and is_imputed = FALSE
        and close < 1000000
    group by 1, 2
)
select t.block_timestamp
, t.tx_id
, t.amount * coalesce(p0.price, p1.price, 0) as amount_usd
from solana.core.fact_transfers t
join solana.core.fact_transactions tx
    on tx.block_timestamp = t.block_timestamp
    and tx.tx_id = t.tx_id
left join p0
    on p0.hour = DATE_TRUNC('hour', t.block_timestamp)
    and p0.mint = t.mint
left join p1
    on p1.date = t.block_timestamp::date
    and p1.mint = t.mint
where t.block_timestamp >= CURRENT_DATE - 1
    and t.block_timestamp >= CURRENT_DATE - 1
    and tx.signers[0]::string != 'ZG98FUCjb8mJ824Gbs6RsgVmr1FhXb2oNiJHa2dwmPd'
    and t.tx_to = 'ZG98FUCjb8mJ824Gbs6RsgVmr1FhXb2oNiJHa2dwmPd'
order by amount_usd desc


with p0 as (
    select token_address as mint
    , DATE_TRUNC('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where DATE_TRUNC('hour', recorded_hour) >= CURRENT_DATE - 30
        and is_imputed = FALSE
        and close < 1000000
    group by 1, 2
), p1 as (
    select token_address as mint
    , DATE_TRUNC('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where DATE_TRUNC('hour', recorded_hour) >= CURRENT_DATE - 30
        and is_imputed = FALSE
        and close < 1000000
    group by 1, 2
), t0 as (
    select distinct block_timestamp::date as date
    , mint
    , coalesce(p0.price, p1.price, 0) * 0.01 as amount_usd
    from solana.nft.fact_nft_mints m
    left join p0
        on p0.hour = DATE_TRUNC('hour', m.block_timestamp)
        and p0.mint = 'So11111111111111111111111111111111111111112'
    left join p1
        on p1.date = m.block_timestamp::date
        and p1.mint = 'So11111111111111111111111111111111111111112'
    where block_timestamp >= current_date - 30
        and succeeded
        and coalesce(is_compressed, false) = false
        and program_id in (
            'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
            , 'CndyV3LdqHUfDLmE5naZjVN8rBZz4tqhdefbAnjHG3JR'
            , 'cndy3Z4yapfJBmL3ShUp5exZKqR3z33thTzeNMm2gRZ'
        )
    group by 1
), t1 as (
    select date
    , sum(amount_usd) as amount_usd
    from t0
    group by 1
)
select *
, sum(amount_usd) over (order by date) as cumu_amount_usd
from t1

select *
from solana.core.dim_labels l
where l.label ilike 'famous fox%'
    and l.label_subtype != 'nf_token_contract'



with a as (
    select distinct qtt.A as query_id
    
    , t.name
    from bi_analytics.velocity_app_prod.queries
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
        on qtt.A = q.id
    join bi_analytics.velocity_app_prod.tags t on qtt.B = t.id
    and t.type = 'project'
), b as (
    select *
    , count(1) over (partition by query_id) as tot_chains
    , 1.0 / tot_chains as pct
    from a
), t0 as (
    select r.query_id
    , created_by_id
    , b.name
    , b.pct
    , timestampdiff(seconds,started_at,ended_at) as query_runtime
    from bi_analytics.VELOCITY_APP_PROD.QUERY_RUNS r
    join b
        on b.query_id = r.query_id
    where started_at > '2023-01-01'
)
select created_by_id as gumby_id,
username,
name,
count(distinct(query_id)) as n_queries,
sum(pct * coalesce(query_runtime, 0)) as studio_queryseconds,
sum(coalesce(query_runtime, 0)) as total_studio_queryseconds,
studio_queryseconds * 0.02 as cost
from t0
join bi_analytics.velocity_app_prod.users u
    on u.id = created_by_id
group by 1,2, 3
order by 5 desc

select u.username
, q.created_at::date as date
, q.*
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.users u
    on u.id = q.created_by_id
where q.title ilike '%shark%'
order by q.created_at desc
limit 100


with t0 as (
    select token_address
    , recorded_hour
    , close
    , lag(close, 1) over (
        partition by token_address
        order by recorded_hour
    ) as prv_price
    , close / prv_price as ratio
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 7
    and is_imputed = false
)
select *
from t0
left join solana.core.dim_tokens t
    on t.token_address = t0.token_address
where ratio >= 10
or ratio <= 0.1
order by ratio desc
limit 10000


select *
from solana.core.fact_events e
join solana.core.fact_transfers t
    on t.block_timestamp = e.block_timestamp
    and t.tx_id = e.tx_id
-- join solana.core.dim_labels l
--     on l.
where e.block_timestamp >= current_date - 3
    and t.block_timestamp >= current_date - 3
    and e.program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
    and e.succeeded
order by e.block_timestamp desc
, e.tx_id
limit 10000


select e.tx_id
, sum(amount) as amount
, count(distinct tx_to) as n_to
from solana.core.fact_events e
join solana.core.fact_transfers t
    on t.block_timestamp = e.block_timestamp
    and t.tx_id = e.tx_id
where e.block_timestamp >= current_date - 3
    and t.block_timestamp >= current_date - 3
    and e.program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
    and e.succeeded
group by 1
order by 3 desc

with t0 as (
    select distinct tx_id
    from solana.core.fact_events e
    where e.block_timestamp >= current_date - 1
        and e.program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
        and e.succeeded
), t1 as (
    select t0.tx_id
    , l.value::string as message
    from t0
    join solana.core.fact_transactions tx
        on tx.tx_id = t0.tx_id
    , lateral flatten(
        input => log_messages
    ) l
    where tx.block_timestamp >= current_date - 1
        and message in (
            'Program log: Instruction: TakeLoanV3'
            -- , 'Program log: Instruction: RepayLoanV3'
            -- , 'Program log: Instruction: ForecloseLoanV3'
            -- , 'Program log: Instruction: BorrowLoan'
            -- , 'Program log: Instruction: RepayLoan'
        )
)
select e.inner_instruction:instructions[0]:mint::string as mint
, l.label
, t1.*
, tx.*
, e.*
from t1
join solana.core.fact_transactions tx
    on tx.block_timestamp >= current_date - 1
    and tx.tx_id = t1.tx_id
join solana.core.fact_events e
    on e.block_timestamp >= current_date - 1
    and e.tx_id = t1.tx_id
left join solana.core.dim_labels l
    on l.address = mint
order by t1.tx_id


select e.tx_id
, sum(amount) as amount
, count(distinct tx_to) as n_to
from solana.core.fact_events e
join solana.core.fact_transfers t
    on t.block_timestamp = e.block_timestamp
    and t.tx_id = e.tx_id
where e.block_timestamp >= current_date - 3
    and t.block_timestamp >= current_date - 3
    and e.program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
    and e.succeeded
group by 1
order by 3 desc




with t0 as (
    select distinct tx_id
    from solana.core.fact_events e
    where e.block_timestamp >= current_date - 7
        and e.program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
        and e.succeeded
), t1 as (
    select t0.tx_id
    , l.value::string as message
    from t0
    join solana.core.fact_transactions tx
        on tx.tx_id = t0.tx_id
    , lateral flatten(
        input => log_messages
    ) l
    where tx.block_timestamp >= current_date - 7
        and message in (
            'Program log: Instruction: TakeLoanV3'
            -- , 'Program log: Instruction: RepayLoanV3'
            -- , 'Program log: Instruction: ForecloseLoanV3'
            -- , 'Program log: Instruction: BorrowLoan'
            -- , 'Program log: Instruction: RepayLoan'
        )
), sales as (
    select coalesce(m.nft_collection_name, l.label, 'Other') as collection
    , median(sales_amount) as price
    from solana.nft.fact_nft_sales s
    left join solana.core.dim_labels l
        on l.address = s.mint
    left join solana.nft.dim_nft_metadata m
        on m.mint = s.mint
    where block_timestamp >= current_date - 1
        and succeeded
    group by 1
), l0 as (
    select e.inner_instruction:instructions[0]:parsed:info:mint::string as mint_address
    , coalesce(m.nft_collection_name, l.label, 'Other') as collection
    -- , l.label
    -- , m.*
    -- , t1.*
    -- , tx.*
    -- , e.*
    from t1
    join solana.core.fact_transactions tx
        on tx.block_timestamp >= current_date - 7
        and tx.tx_id = t1.tx_id
    join solana.core.fact_events e
        on e.block_timestamp >= current_date - 7
        and e.tx_id = t1.tx_id
    left join solana.core.dim_labels l
        on l.address = mint_address
    left join solana.nft.dim_nft_metadata m
        on m.mint = mint_address
    where mint_address is not null
), l1 as (
    select l0.collection
    , s.price
    , count(1) as n
    from l0
    join sales s
        on s.collection = l0.collection
    group by 1, 2
)
select *
, price * n as volume
from l1
order by volume desc




select coalesce(m.nft_collection_name, l.label, 'Other') as collection
, s.block_timestamp::date
, median(sales_amount) as med_price
, min(sales_amount) as min_price
from solana.nft.fact_nft_sales s
left join solana.core.dim_labels l
    on l.address = s.mint
left join solana.nft.dim_nft_metadata m
    on m.mint = s.mint
where block_timestamp >= current_date - 60
    and succeeded
    and sales_amount >= 0.05
group by 1, 2


select *
from FLIPSIDE_PROD_DB.BRONZE.PROD_ADDRESS_LABEL_SINK_291098491
limit 10

select *
from crosschain.bronze.data_science_uploads
limit 10

select *
from crosschain.bronze.PROD_ADDRESS_LABEL_SINK_291098491
limit 10

select *
from crosschain.bronze.PROD_ADDRESS_TAG_SYNC_1480319581
limit 10

with t0 as (
    select instructions[1]:accounts[0]::string as acct
    , instructions[1]:accounts[5]::string as acct_5
    , tx.block_timestamp::date as date
    , tx.*
    , e.*
    -- , array_size(tx.account_keys) as sz
    -- , tx.account_keys[0]:pubkey::string as ak_0
    -- , tx.account_keys[1]:pubkey::string as ak_1
    -- , tx.account_keys[2]:pubkey::string as ak_2
    -- , tx.account_keys[3]:pubkey::string as ak_3
    -- , tx.account_keys[4]:pubkey::string as ak_4
    -- , tx.account_keys[5]:pubkey::string as ak_5
    -- , tx.account_keys[6]:pubkey::string as ak_6
    -- , tx.account_keys[7]:pubkey::string as ak_7
    -- , tx.account_keys[8]:pubkey::string as ak_8
    -- , tx.account_keys[9]:pubkey::string as ak_9
    -- , tx.account_keys[10]:pubkey::string as ak_10
    -- , tx.account_keys[11]:pubkey::string as ak_11
    -- , tx.account_keys[12]:pubkey::string as ak_12
    -- , tx.account_keys[13]:pubkey::string as ak_13
    -- , tx.account_keys[14]:pubkey::string as ak_14
    -- , tx.account_keys[15]:pubkey::string as ak_15
    -- , tx.account_keys[16]:pubkey::string as ak_16
    -- , tx.account_keys[17]:pubkey::string as ak_17
    from solana.nft.fact_nft_mints m
    join solana.core.fact_transactions tx
        on tx.block_timestamp = m.block_timestamp
        and tx.tx_id = m.tx_id
    join solana.core.fact_events e
        on e.block_timestamp = m.block_timestamp
        and e.tx_id = m.tx_id
    where m.block_timestamp >= current_date - 60
        and m.mint in (
            'CzoARt5quwbuD2wTadSSy3NXtp9xWPwnFJiXQ84CiFow'
, 'Eeee1AjPP1ascHEZ8LB8y1SU96CDQJKqKYEtpJBbMi3o'
, 'FPv58bBq2XhYoCaPhXRWawQk8V3V9TqJsD11qF3MiQTj'
, '2q2oy27EjUX5iUcJ8gzTqEeDr5gifcMpo9LPCeVRgCAd'
, '39szcxWLYPoPsEQWpX9eXhaKUJKgJkGzv7X3frowdgvY'
    )
)
select distinct mint
from solana.core.fact_nft_mints m
where m.block_timestamp >= '2023-12-14'
    and m.block_timestamp <= '2023-12-24'
    and mint_price = '0.01993192'
-- select 'Lucky Louie' as collection
-- select 'Froganas' as collection
select 'D3fenders' as collection
-- select 'Sols SPL20' as collection
, m.mint
, m.mint_price
, m.mint_currency
-- tx.*
-- , instructions[1]:accounts[0]::string as acct
-- , array_size(tx.account_keys) as sz
-- , tx.account_keys[0]:pubkey::string as ak_0
-- , tx.account_keys[1]:pubkey::string as ak_1
-- , tx.account_keys[2]:pubkey::string as ak_2
-- , tx.account_keys[3]:pubkey::string as ak_3
-- , tx.account_keys[4]:pubkey::string as ak_4
-- , tx.account_keys[5]:pubkey::string as ak_5
-- , tx.account_keys[6]:pubkey::string as ak_6
-- , tx.account_keys[7]:pubkey::string as ak_7
-- , tx.account_keys[8]:pubkey::string as ak_8
-- , tx.account_keys[9]:pubkey::string as ak_9
-- , tx.account_keys[10]:pubkey::string as ak_10
-- , tx.account_keys[11]:pubkey::string as ak_11
-- , tx.account_keys[12]:pubkey::string as ak_12
-- , tx.account_keys[13]:pubkey::string as ak_13
-- , tx.account_keys[14]:pubkey::string as ak_14
-- , tx.account_keys[15]:pubkey::string as ak_15
-- , tx.account_keys[16]:pubkey::string as ak_16
-- , tx.account_keys[17]:pubkey::string as ak_17
from solana.nft.fact_nft_mints m
join solana.core.fact_transactions tx
    on tx.block_timestamp = m.block_timestamp
    and tx.tx_id = m.tx_id
where m.block_timestamp >= '2023-11-09'
    and m.block_timestamp <= '2023-11-13'
    -- and ak_0 = '93Jxd94A4YWJc992tAK6mF8A1DtQfJ4uWNr9izYhKjNM'
    -- and instructions[1]:accounts[0]::string = '3UGy3nQxcqRVgRKT3zcjYswFmKZhbW9v9XoqBTH3YNk9'
    and instructions[1]:accounts[5]::string = 'Gbu2GXQZDzMLYbAhAwnAoQ2nKN45nHe2EqHd4GzPscWx'
    -- and tx.account_keys[5]:pubkey::string = 'F593oTeqW1BYiyvnhjUH5mVemk2H3Jgb54uqHcwU5piw'
    and m.succeeded

select label
, count(1) as n
, count(distinct address) as n_add
from solana.core.dim_labels
where label_subtype = 'nf_token_contract'
group by 1
order by 2 desc

with t0 as (
    select mint
    , count(distinct tx_to) as n_wallets_to
    , count(distinct tx_from) as n_wallets_from
    , sum(amount) as amount
    from solana.core.fact_transfers
    where block_timestamp >= current_date - 7
    group by 1
), p as (
    select token_address as mint
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= current_date - 1
        and is_imputed = FALSE
        and close < 1000000
    group by 1
)
select t0.*
, l.label
, p.price
, p.price * t0.amount as amount_usd
from t0
join p
    on p.mint = t0.mint
left join solana.core.dim_labels l
    on l.address = t0.mint
where n_wallets_to >= 1000
    and n_wallets_from >= 1000
order by amount_usd desc



with p as (
    select token_address as mint
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= current_date - 1
        and is_imputed = false
        and close < 1000000
    group by 1
), signers as (
    select signers[0]::string as signer
    , count(1) as n_tx
    from solana.core.fact_transactions
    where block_timestamp >= '2023-01-01'
    group by 1
)
, t0a as (
    select tx_to
    , tx_from
    , mint
    , sum(amount) as amount
    from solana.core.fact_transfers
    where block_timestamp >= '2021-01-01'
        and mint in (
            'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn'
            , 'So11111111111111111111111111111111111111112'
            , 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
            , 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
            , 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
            , '7i5KKsX2weiTkry7jA4ZwSuXGhs5eJBEjY8vVxR4pfRx'
            , 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So'
            , 'bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1'
            , 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm'
            , 'hntyVP6YFm1Hg25TN9WGLqM12b8TQmcknKrdu1oxWux'
            , 'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn'
            , '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R'
            , 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3'
            , 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL'
            , '7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs'
            , 'MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey'
            , 'orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE'
            , 'SHDWyBxihqiCj6YekG2GUr7wqKLeLAMK1gHZck9pL6y'
            , 'FoXyMu5xwXre7zEoSvzViRk3nGawHUp9kUh97y2NDhcq'
        )
    group by 1, 2, 3
), t0 as (
    select t0a.*
    , p.price * t0a.amount as amount_usd
    from t0a
    join p
        on p.mint = t0a.mint
), t1 as (
    select tx_to as address
    , sum(amount_usd) as amount_usd
    , sum(amount_usd) as volume_usd
    from t0
    group by 1
    union
    select tx_from as address
    , sum(-amount_usd) as amount_usd
    , sum(amount_usd) as volume_usd
    from t0
    group by 1
), t2 as (
    select address
    , sum(amount_usd) as amount_usd
    , sum(volume_usd) as volume_usd
    from t1
    group by 1
), t3 as (
    select t2.*
    , s.n_tx
    , l.label
    , l.label_type
    , l.label_subtype
    from t2
    left join solana.core.dim_labels l
        on l.address = t2.address
    join signers s
        on s.signer = t2.address
)
select *
from t3
order by amount_usd desc
limit 100000

select *
from solana.core.fact_transfers
where block_timestamp >= '2023-01-01'
    and tx_to = '8CFo8bL8mZQK8abbFyypFMwEDd8tVJjHTTojMLgQTUSZ'

with p as (
    select token_address as mint
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= current_date - 1
        and is_imputed = false
        and close < 1000000
    group by 1
), t0 as (
    select program_id
    , tx_id
    , mint
    , liquidity_provider as address
    , amount
    -- , sum(amount) as amount
    from solana.defi.fact_liquidity_pool_actions
    where action = 'deposit'
    -- group by 1, 2, 3
)
select t0.*
, p.price * t0.amount as amount_usd
from t0
join p
    on p.mint = t0.mint
order by amount_usd desc
limit 200000

select action
, count(distinct liquidity_provider) as n
from solana.defi.fact_liquidity_pool_actions
group by 1
order by 2 desc



select *
from solana.defi.fact_swaps
limit 10



select *
from solana.defi.fact_liquidity_pool_actions
where block_timestamp >= current_date - 1
    and tx_id = '3MyZFy3PBmACYcpXgyAkptQSDEEJupw17La6cxvdEu7zuZeEtmXB1o3kH917PgyG1ARjyKqm8Rk4n3euVmmuEUVZ'
limit 10


with t0 as (
    select address
    , max(amount * power(10, -9)) * 100 as max_stake_usd
    from solana.defi.fact_stake_pool_actions
    where amount is not null
    group by 1
)

select * 
from solana.defi.fact_stake_pool_actions
limit 10

with t0 as (
    select address
    , sum(case when action like 'deposit%' then amount else 0 end * 100 ) / pow(10,9) as deposit_stake_usd
    , sum(case when action like 'withdraw%' then amount else 0 end * 100 ) / pow(10,9) as withdraw_stake_usd
    , sum(
        case when action like 'deposit%' then amount
        else -amount
        end * 100
    ) / pow(10,9) as net_stake_usd
    , max(amount * power(10, -9)) * 100 as max_stake_usd
    from solana.defi.fact_stake_pool_actions
    where succeeded
        and (action like 'deposit%' or action like 'withdraw%')
    group by 1
    order by 4 desc
)
select *
from t0


select *
from bi_analytics.bronze.arprograms_hike
where txid = '0x4ffc78bcc7a2736d8167739ebfec1ed08c9241cc68eeb05d832ac71bfe28cbaa'





with p as (
    select token_address as mint
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= current_date - 1
        and is_imputed = false
        and close < 1000000
    group by 1
), p1 as (
    select token_address as mint
    , recorded_hour::date as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where is_imputed = false
        and close < 1000000
    group by 1, 2
), lp0 as (
    select liquidity_provider
    , mint
    , sum(case when action = 'deposit' then amount else -amount end) as net_amount
    from solana.defi.fact_liquidity_pool_actions
    where action in ('deposit','withdraw')
    group by 1, 2
), lp1 as (
    select lp0.liquidity_provider as address
    , sum(p.price * lp0.net_amount) as amount_usd
    from lp0
    join p
        on p.mint = lp0.mint
    group by 1
), stake0 as (
    select address
    , sum(case when action like 'deposit%' then amount else 0 end * 100 ) / pow(10,9) as deposit_stake_usd
    , sum(case when action like 'withdraw%' then amount else 0 end * 100 ) / pow(10,9) as withdraw_stake_usd
    , sum(
        case when action like 'deposit%' then amount
        else -amount
        end * 100
    ) / pow(10,9) as net_stake_usd
    , max(amount * power(10, -9)) * 100 as max_stake_usd
    from solana.defi.fact_stake_pool_actions
    where succeeded
        and (action like 'deposit%' or action like 'withdraw%')
    group by 1
), swaps0 as (
    select swapper as address
    , sum(least(swap_from_amount * coalesce(p1f.price, 0), swap_to_amount * coalesce(p1t.price, 0))) as tot_swap
    , max(least(swap_from_amount * coalesce(p1f.price, 0), swap_to_amount * coalesce(p1t.price, 0))) as max_swap
    from solana.defi.fact_swaps s
    left join p1 p1f
        on p1f.mint = s.swap_from_mint
        and p1f.date = s.block_timestamp::date
    left join p1 p1t
        on p1t.mint = s.swap_to_mint
        and p1t.date = s.block_timestamp::date
    where swap_from_amount > 0
        and swap_to_amount > 0
        and succeeded
    group by 1
), signers as (
    select signers[0]::string as address
    , count(1) as n_sign
    from solana.core.fact_transactions
    group by 1
)
, t0a as (
    select tx_to
    , tx_from
    , mint
    , sum(amount) as amount
    from solana.core.fact_transfers
    where block_timestamp >= '2021-01-01'
        and mint in (
            'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn'
            , 'So11111111111111111111111111111111111111112'
            , 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
            , 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
            , 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
            , '7i5KKsX2weiTkry7jA4ZwSuXGhs5eJBEjY8vVxR4pfRx'
            , 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So'
            , 'bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1'
            , 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm'
            , 'hntyVP6YFm1Hg25TN9WGLqM12b8TQmcknKrdu1oxWux'
            , 'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn'
            , '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R'
            , 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3'
            , 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL'
            , '7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs'
            , 'MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey'
            , 'orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE'
            , 'SHDWyBxihqiCj6YekG2GUr7wqKLeLAMK1gHZck9pL6y'
            , 'FoXyMu5xwXre7zEoSvzViRk3nGawHUp9kUh97y2NDhcq'
        )
    group by 1, 2, 3
), t0 as (
    select t0a.*
    , p.price * t0a.amount as amount_usd
    from t0a
    join p
        on p.mint = t0a.mint
), t1 as (
    select tx_to as address
    , sum(amount_usd) as amount_usd
    , sum(amount_usd) as volume_usd
    from t0
    group by 1
    union
    select tx_from as address
    , sum(-amount_usd) as amount_usd
    , sum(amount_usd) as volume_usd
    from t0
    group by 1
), t2 as (
    select address
    , sum(amount_usd) as amount_usd
    , sum(volume_usd) as volume_usd
    from t1
    group by 1
)
, final as (
    select coalesce(t.address, s.address, st.address, l.address) as address
    , coalesce(t.amount_usd, 0) as amount_usd
    , coalesce(t.volume_usd, 0) as volume_usd
    , coalesce(s.max_swap, 0) as max_swap
    , coalesce(s.tot_swap, 0) as tot_swap
    , coalesce(st.net_stake_usd, 0) as net_stake_usd
    , coalesce(l.amount_usd, 0) as lp_amount_usd
    , coalesce(si.n_sign, 0) as n_sign
    from t2 t
    FULL OUTER join swaps0 s USING (address)
    FULL OUTER join stake0 st USING (address)
    FULL OUTER join lp1 l USING (address)
    left join signers si using (address)
)
, t3 as (
    select f.*
    , l.label
    , l.label_type
    , l.label_subtype
    from final f
    left join solana.core.dim_labels l
        on l.address = f.address
)
select *
, greatest(amount_usd, 0) + greatest(tot_swap, 0) + (net_stake_usd, 0) + greatest(lp_amount_usd, 0) as tot_value_usd
from t3
order by tot_value_usd desc
limit 250000

with t0 as (
    select distinct program_id
    , signers[0]::string as signer
    , date_trunc('month', block_timestamp) as month
    from solana.core.fact_events
    where block_timestamp >= '2023-01-01'
        and succeeded
    group by 1
), t1 as (
    select coalesce(l.label, program_id)
    , month
    , count(distinct signer) as n_signers
    from t0
    left join solana.core.dim_labels l
        on l.address = t0.program_id
    group by 1, 2
), t2 as (
    select *
    , row_number() over (partition by month order by n_signers desc) as rk
    from t1
    where n_signers >= 100
)
select *
from t2
order by month desc, rk


select *
from solana.core.dim_labels 
where address = '132EMXwzuW2bdKk7vSNzQp1tREshYCscTsXFXFxdmxfb'


select *
from solana.core.ez_events_decoded
where block_timestamp >= current_date - 1
    and program_id = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
limit 10

FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X

select swap_program, count(1)
from solana.defi.fact_swaps
where block_timestamp >= current_date - 1
    and succeeded
group by 1
order by 2 desc



select coalesce(l.label, p.program_id) as program
, count(1) as n
from solana.defi.fact_liquidity_pool_actions p
left join solana.core.dim_labels l
    on l.label = p.program_id
where block_timestamp >= current_date - 1
group by 1
order by 2 desc
limit 100

select l.label
, sum(sales_amount) as volume
, median(sales_amount) as median_price
, count(1) as n_sales
from solana.nft.fact_nft_sales s
join solana.core.dim_labels l
    on l.address = s.mint
where s.block_timestamp >= current_date - 7
    and succeeded
    and l.label like 'honeyland%'
group by 1
order by 2 desc

select l.label
, count(distinct l.address)
, count(1) as n_sales
from solana.core.dim_labels l
where l.label like 'honeyland%'
group by 1
order by 2 desc



select *
from solana.core.fact_events
where block_timestamp >= '2023-11-01'
    and program_id = 'SPLsCpfTUEZe43PDw9KXnw6eJfKVZoKYCGGPY29S3fN'
    and succeeded
order by block_timestamp
limit 30000


select tx_id
, block_timestamp
, block_timestamp::date as date
, signers[0]::string as signer
, instruction:accounts[4]::string as mint
from solana.core.fact_events
where block_timestamp >= '2023-11-01'
    and program_id = 'SPLsCpfTUEZe43PDw9KXnw6eJfKVZoKYCGGPY29S3fN'
    and succeeded
order by block_timestamp
limit 30000



with t0 as (
    select 
    tx_id
    , block_timestamp
    , block_timestamp::date as date
    , signers[0]::string as signer
    , instruction:accounts[0]::string as a1
    , instruction:accounts[1]::string as a1
    , instruction:accounts[2]::string as a1
    , instruction:accounts[3]::string as a3
    , instruction:accounts[4]::string as mint
    , instruction:accounts[5]::string as a5
    , instruction:accounts[6]::string as a6
    , instruction:accounts[7]::string as a7
    , instruction:accounts[8]::string as a8
    , instruction:accounts[9]::string as a9
    , instruction:accounts[10]::string as a10
    -- , *
    from solana.core.fact_events
    where block_timestamp >= '2023-11-01'
        and program_id = 'SPLsCpfTUEZe43PDw9KXnw6eJfKVZoKYCGGPY29S3fN'
        and succeeded
    order by block_timestamp
    -- limit 30000
)
select t0.*
, m.*
from solana.nft.fact_nft_mints m
join t0
    on t0.block_timestamp = m.block_timestamp
    and t0.tx_id = m.tx_id
where m.block_timestamp >= '2023-11-01'
order by m.mint, m.block_timestamp



select 
e.tx_id
, array_size(tx.account_keys) as ak_sz
, array_size(tx.instructions) as in_sz
, t.tx_from
, t.tx_to
, t.mint
, t.amount
, e.block_timestamp
, e.block_timestamp::date as date
, e.signers[0]::string as signer
, instruction:accounts[0]::string as a0
, instruction:accounts[1]::string as a1
, instruction:accounts[2]::string as a2
, instruction:accounts[3]::string as a3
, instruction:accounts[4]::string as a4
, instruction:accounts[5]::string as a5
, instruction:accounts[6]::string as a6
, instruction:accounts[7]::string as a7
, instruction:accounts[8]::string as a8
, instruction:accounts[9]::string as a9
, instruction:accounts[10]::string as a10
-- , *
from solana.core.fact_events e
join solana.core.fact_transfers t
    on t.block_timestamp = e.block_timestamp
    and t.tx_id = e.tx_id
join solana.core.fact_transactions tx
    on tx.block_timestamp = e.block_timestamp
    and tx.tx_id = e.tx_id
where e.block_timestamp >= '2023-11-01'
    and e.program_id = 'SPLsCpfTUEZe43PDw9KXnw6eJfKVZoKYCGGPY29S3fN'
    and e.succeeded
    and tx.log_messages::string like '%Program log: Instruction: Validate%'
order by e.block_timestamp, e.tx_id
limit 90000


with t0 as (
    select 
    e.tx_id
    , array_size(tx.account_keys) as ak_sz
    , array_size(tx.instructions) as in_sz
    , t.tx_from
    , t.tx_to
    , t.mint
    , t.amount
    , e.block_timestamp
    , e.block_timestamp::date as date
    , e.signers[0]::string as signer
    , instruction:accounts[0]::string as a0
    , instruction:accounts[1]::string as a1
    , instruction:accounts[2]::string as a2
    , instruction:accounts[3]::string as a3
    , instruction:accounts[4]::string as a4
    , instruction:accounts[5]::string as a5
    , instruction:accounts[6]::string as a6
    , instruction:accounts[7]::string as a7
    , instruction:accounts[8]::string as a8
    , instruction:accounts[9]::string as a9
    , instruction:accounts[10]::string as a10
    -- , *
    from solana.core.fact_events e
    join solana.core.fact_transfers t
        on t.block_timestamp = e.block_timestamp
        and t.tx_id = e.tx_id
        and t.tx_to = '2sTLfEb5dTTF6Kvg8spwCC5kvjeLxP9uwcaP2XBtMhV4'
    join solana.core.fact_transactions tx
        on tx.block_timestamp = e.block_timestamp
        and tx.tx_id = e.tx_id
    where e.block_timestamp >= '2023-11-21'
        and e.block_timestamp <= '2023-12-10'
        and e.program_id = 'SPLsCpfTUEZe43PDw9KXnw6eJfKVZoKYCGGPY29S3fN'
        and e.succeeded
        and tx.log_messages::string like '%Program log: Instruction: Validate%'
    order by e.block_timestamp, e.tx_id
), t1 as (
    select distinct t0.a4 as mint
    , 1 as has_mint
    from solana.nft.fact_nft_mints m
    join t0
        on t0.a4 = m.mint
    where m.block_timestamp >= '2023-11-01'
), t2 as (
    select distinct t0.a4 as mint
    , 1 as has_sale
    from solana.nft.fact_nft_sales m
    join t0
        on t0.a4 = m.mint
    where m.block_timestamp >= '2023-11-01'
), t3 as (
    select distinct t0.a4 as mint
    , 1 as has_xfer
    from solana.core.fact_transfers m
    join t0
        on t0.a4 = m.mint
    where m.block_timestamp >= '2023-11-01'
)
select t0.*
, coalesce(has_mint, 0) as has_mint
, coalesce(has_sale, 0) as has_sale
, coalesce(has_xfer, 0) as has_xfer
from t0
left join t1 on t1.mint = t0.a4
left join t2 on t2.mint = t0.a4
left join t3 on t3.mint = t0.a4


select date_trunc('hour', block_timestamp) as hour
, case when swap_to_mint = 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3' then 'to' else 'from' end as direction
, count(distinct swapper) as n_swappers
, sum(case when direction = 'to' then swap_to_amount else swap_from_amount end) as amount
from solana.defi.fact_swaps
where block_timestamp >= '2023-11-19'
    and block_timestamp <= '2023-11-30'
    and swap_to_mint = 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3'
    or swap_from_mint = 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3'
group by 1
order by 1


select *
from solana.core.dim_labels
where address = '2sYUkZm8VAb1P7D9Eq1o78uNZJdrTMNnAD6Pzx4CES36'

select *
, tx.account_keys[2]:pubkey::string as pubkey_2
from solana.nft.fact_nft_mints m
join solana.core.fact_transactions tx
    on tx.block_timestamp = m.block_timestamp
    and tx.tx_id = m.tx_id
where m.block_timestamp >= '2024-01-13' 
    and tx.account_keys[2]:pubkey::string = '23W5dZSNiNtKXCGvcjkRAJZ6admMXQ26s3ttBaH6Lb2k'
    -- and m.mint in (
    --     '2sYUkZm8VAb1P7D9Eq1o78uNZJdrTMNnAD6Pzx4CES36'
    --     , '4jWW4sbukZCHUXiGqgAKJQYki1PxGNqyiU2Ru8B65CHy'
    --     , '55phYTMCGweDcadHynVDvjgXAoa2NBz8GkQ5GYcSZCG8'
    -- )

with t0 as (
    select c.value:ecosystem::string as ecosystem
    , c.value:handle::string as handle
    , c.value:type::string as type
    , u.record_metadata:CreateTime::int as CreateTime
    , row_number() over (partition by ecosystem, handle order by CreateTime desc) as rn
    from crosschain.bronze.data_science_uploads u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'twitter-accounts%'
)
select *
from t0
where rn = 1



with t0 as (
    select c.value:ecosystem::string as ecosystem
    , c.value:twitter_handle::string as twitter_handle
    , c.value:account_type::string as account_type
    , c.value:twitter_id::string as twitter_id
    , u.record_metadata:CreateTime::int as CreateTime
    , row_number() over (partition by ecosystem, twitter_handle order by CreateTime desc) as rn
    from crosschain.bronze.data_science_uploads u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'twitter-accounts%'
        and twitter_handle != 'fknmarqu'
)
select t.impression_count
, t0.twitter_handle
, t0.account_type
, coalesce(u.username, '') as username
, coalesce(d.title, '') as title
, coalesce(d.id, '') as dashboard_id
, concat('https://twitter.com/',u.username,'/status/', t.conversation_id) as tweet_url
from bi_analytics.twitter.tweet t
join bi_analytics.twitter.likes l
    on l.tweet_id = t.conversation_id
join t0
    on t0.twitter_id = l.user_id
join bi_analytics.velocity_app_prod.dashboards d
    on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id


select d.title, u.username, dr.*
from bi_analytics.content_rankings.dashboard_rankings dr
join bi_analytics.velocity_app_prod.dashboards d
    on d.id = dr.dashboard_id
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
order by dr.ranking_trending


select d.created_at::date as date, d.title, u.username
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
where u.username = 'hess'
order by d.created_at


select *
from solana.core.dim_labels
where mint in (
    '2EKfmwzwtoj7JHHHggHS6mfBuL9jCvEwRPr4xC6LkPea'
    , 'HBXHSqsU6RRHuhwjBwLmgim3ZqeYxTEz4ScqHs7ATsjS'
    , '9HdG17axrqBb1q7DwH7tWqJvF39t4JZvnkoCRZXsypbN'
)

select t.created_at::date as date
, count(1)
from bi_analytics.twitter.tweet t
join bi_analytics.twitter.likes l
    on l.tweet_id = t.conversation_id
group by 1
order by 1


with t0 as (
    select lower(address) as address
    , min(timestamp) as first_connect
    from bi_analytics.bronze.arprograms_walletconnect
    where blockchain = 'Avalanche'
    group by 1
), t1 as (
    select *
    -- , extract(year from date_trunc('quarter', to_timestamp(first_connect))) as year
    -- , extract(quarter from date_trunc('quarter', to_timestamp(first_connect))) as quarter
    , date_trunc('quarter', to_timestamp(first_connect)) as yq
    from t0
)
select *
from t1


select *
from solana.core.fact_decoded_instructions
where block_timestamp >= current_date - 1
    and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
limit 10



select *
from
    solana.core.fact_transfers
where
    BLOCK_TIMESTAMP >= current_date - 1
    and TX_TO = '7e7qhwnJuLVDGBiLAGjB9FxfpizsPhQjoBxkNA5wVCCc'
    and MINT = 'So11111111111111111111111111111111111111112'
limit 10

select *
from
    solana.nft.fact_mints m
    join solana.core.fact_transactions tx
        on m.block_timestamp = tx.block_timestamp
        and m.tx_id = tx.tx_id
where
    m.BLOCK_TIMESTAMP >= current_date - 1
    and MINT in ('9sxiaePbdEyejUzaQ9HDF9EVNPsjCokZpAYgzdYApGom','9YxpqecjvDjLcZJ4vHoUziUb7uQtAyPp28F4svb4RPo9','GLhZWy4Qnz5Xrvs28TpU22Psp9NayoYNjicaSdRuZrUK')
limit 10


select *
from solana.core.fact_transactions
where block_timestamp >= current_date - 11
    and tx.signers[0] = 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC'


select *
from solana.core.dim_labels where address = '47Qh62MoqGsfV3My9QQFhfEM3QszjJcggSnK2zL6m6TJ'


select tx.log_messages[0]::string as message
, *
from solana.core.fact_events e
join solana.core.fact_transactions tx
    on tx.tx_id = e.tx_id
where e.block_timestamp >= '2023-01-22'
    and e.program_id = 'TLoCKic2wGJm7VhZKumih4Lc35fUhYqVMgA4j389Buk'
    

select tx.log_messages[1]::string as message
, tx.signers[0]::string as signer
, array_size(tx.log_messages) as sz_m
, array_size(tx.signers) as sz_s
, array_size(tx.instructions) as sz_i
, array_size(tx.inner_instructions) as sz_ii
, tx.tx_id
-- , *
from solana.core.fact_events e
join solana.core.fact_transactions tx
    on tx.tx_id = e.tx_id
where e.block_timestamp >= '2024-01-22'
    and tx.block_timestamp >= '2024-01-22'
    and e.program_id = 'TLoCKic2wGJm7VhZKumih4Lc35fUhYqVMgA4j389Buk'
limit 10000


with t0 as (
    select *
	from flipside_prod_db.bronze.prod_address_label_sink_291098491 dbc 
    where _inserted_timestamp >= '2023-06-05'
        and record_metadata:topic::string = 'twitter-tweet'
), t1 as (
    select t0._inserted_timestamp
    , c.value:id::string as id
    , c.value:user_id::string as user_id
    , c.value:user_followers::int as user_followers
    , c.value:conversation_id::int as conversation_id
    , c.value:created_at::datetime as created_at
    , c.value:like_count::int as like_count
    , c.value:impression_count::int as impression_count
    , c.value:retweet_count::int as retweet_count
    , c.value:quote_count::int as quote_count
    , c.value:tweet_type::string as tweet_type
    , c.value:clean_url::string as clean_url
    , c.value:platform::string as platform
    , c.value:tweet_url::string as tweet_url
    from t0
    , lateral flatten(
        input => record_content
    ) c
), t2 as (
    select *
    from t1
    qualify (
        row_number() over (partition by id order by _inserted_timestamp desc) = 1
    )
)
select id
, user_id
, user_followers
, conversation_id
, created_at
, like_count
, impression_count
, retweet_count
, quote_count
, tweet_type
, clean_url
, platform
, tweet_url
from t2




with labels as (
    select distinct c.value:address::string as address
    , c.value:project_name::string as label
    from crosschain.bronze.address_labels u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:CreateTime::int >= 1706011200000
        and record_metadata:key::string like 'labels-solana%'
    union
    select address
    , label
    from solana.core.dim_labels
), t0 as (
    select coalesce(m.nft_collection_name, l.label, 'Other') as collection
    , s.*
    , lag(sales_amount, -1) over (partition by s.mint order by block_timestamp) as nxt_sale
    , lag(purchaser, 1) over (partition by s.mint order by block_timestamp) as prv_owner
    , nxt_sale - sales_amount as profit
    from solana.nft.fact_nft_sales s
    left join labels l
        on l.address = s.mint
    left join solana.nft.dim_nft_metadata m
        on m.mint = s.mint
    where s.succeeded
), t1 as (
    select purchaser
    , round(avg(profit), 1) as avg_profit
    , round(sum(profit)) as tot_profit
    , round(avg(case when profit > 0 then 1 else 0 end), 2) as profit_pct
    , round(sum(case when profit > 0 then 1 else 0 end)) as tot_n_profit
    from t0
    where nxt_sale is not null
    group by 1
), t2 as (
    select purchaser
    from t1
    where avg_profit >= 3
        and tot_profit >= 500
        and profit_pct >= 0.6
        and tot_n_profit >= 5
)
select collection
, block_timestamp::date as date
, sum(case when t2.purchaser = t0.purchaser then 1 else -1 end) as net_sales
, median(case when t2.purchaser = t0.purchaser then sales_amount else null end) as med_buy_price
, median(case when t2.purchaser = t0.purchaser then null else sales_amount end) as med_sell_price
, count(1) as n_sales
from t0
join t2
    on (t2.purchaser = t0.purchaser or t2.purchaser = t0.seller)
where nxt_sale is null
    and block_timestamp >= current_date - 30
group by 1, 2
order by 2 desc

with labels as (
    select distinct c.value:address::string as address
    , c.value:project_name::string as label
    from crosschain.bronze.address_labels u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:CreateTime::int >= 1706011200000
        and record_metadata:key::string like 'labels-solana%'
    union
    select address
    , label
    from solana.core.dim_labels
), t0 as (
    select coalesce(m.nft_collection_name, l.label, 'Other') as collection
    , s.*
    , lag(sales_amount, -1) over (partition by s.mint order by block_timestamp) as nxt_sale
    , lag(purchaser, 1) over (partition by s.mint order by block_timestamp) as prv_owner
    , nxt_sale - sales_amount as profit
    from solana.nft.fact_nft_sales s
    left join labels l
        on l.address = s.mint
    left join solana.nft.dim_nft_metadata m
        on m.mint = s.mint
    where s.succeeded
), t1 as (
    select purchaser
    , round(avg(profit), 1) as avg_profit
    , round(sum(profit)) as tot_profit
    , round(avg(case when profit > 0 then 1 else 0 end), 2) as profit_pct
    , round(sum(case when profit > 0 then 1 else 0 end)) as tot_n_profit
    from t0
    where nxt_sale is not null
    group by 1
), t2 as (
    select purchaser
    from t1
    where avg_profit >= 3
        and tot_profit >= 500
        and profit_pct >= 0.6
        and tot_n_profit >= 5
)
select t0.mint
, t0.block_timestamp::date as date
, t0.sales_amount
from t0
join t2
    on (t2.purchaser = t0.purchaser or t2.purchaser = t0.seller)
where nxt_sale is null
    and block_timestamp >= current_date - 11
    and collection = 'Other'



select *
from solana.core.fact_events
where block_timestamp >= '2023-01-16'
    and block_timestamp <= '2023-01-19'
    and program_id = '1NSCRfGeyo7wPUazGbaPBUsTM49e1k2aXewHGARfzSo'



select distinct c.value:quote_tweet_id::string as quote_tweet_id
, c.value:tweet_id::string as tweet_id
, c.value:user_id::string as user_id
, lateral flatten(
    input => record_content
) c
from crosschain.bronze.data_science_uploads
where record_metadata:key::string like 'twitter-quotes%'

select *
from bi_analytics.twitter.user
limit 10


select distinct h.address
, h.trailheadId
from bi_analytics.bronze.arprograms_hike h
join avalanche.core.ez_avax_transfers t
    on t.tx_hash = h.txId
where result = 'VERIFIED'
    and blockchain = 'Avalanche'


select id
from bi_analytics.velocity_app_prod.users
where (username) in (
    '0xHedman'
    , 'cryptopaper'
)

dt <- read.csv('~/Downloads/user_bans.csv')
KafkaGeneric(
    'prod-data-science-uploads'
    , .project = 'user-bans'
    , .data = dt
)



select distinct c.value:user_id::string as user_id
from crosschain.bronze.data_science_uploads
, lateral flatten(
    input => record_content
) c
where record_metadata:key::string like 'user-bans%'




  select
    -- 11/02: grab the team owner username if is a team
    coalesce(m.user_id, d.created_by_id) as user_id,
    d.profile_id as profile_id,
    d.title,
    coalesce(tu.username, u.username) as username,
    dr.currency,
    d.id as dashboard_id,
    p.type,
    coalesce(c.chain, 'Polygon') as ecosystem,
    row_number() over (
      order by
        dr.ranking_trending
    ) as current_rank
  from
    bi_analytics_dev.content_rankings.dashboard_rankings dr
    join bi_analytics.velocity_app_prod.dashboards d on d.id = dr.dashboard_id
    left join bi_analytics.velocity_app_prod.profiles p
        on p.id = d.profile_id
    left join bi_analytics.velocity_app_prod.teams t
        on t.profile_id = p.id
    left join bi_analytics.velocity_app_prod.members m
        on t.id = m.team_id
        and m.role = 'owner'
    left join bi_analytics.velocity_app_prod.users tu
        -- kellen changed this line
        on tu.id = m.user_id
    join bi_analytics.velocity_app_prod.users u on u.id = d.created_by_id
    where dr.ranking_trending <= 100
    order by dr.ranking_trending



select *
from solana.core.fact_transactions
where block_timestamp >= current_date - 1
    and tx_id = 'L7jQ3ktHo1EA5uzLPQTojkUFcyrwBjxHFGRNFZ5ZjXZLTPNkztUXRekWGs6YUpuPEkZVSnWSCvBQKgFP7ugfbSs'


select *
from solana.gov.fact_rewards_voting
limit 10



with t0 as (
    select c.value:ecosystem::string as ecosystem
    , c.value:twitter_handle::string as twitter_handle
    , c.value:account_type::string as account_type
    , c.value:twitter_id::string as twitter_id
    , c.value:n_followers::int as n_followers
    , u.record_metadata:CreateTime::int as CreateTime
    , row_number() over (partition by twitter_id order by CreateTime desc) as rn
    from crosschain.bronze.data_science_uploads u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'twitter-accounts%'
), t1 as (
    select *
    from t0
    where rn = 1
)
select t1.*
, q.quote_tweet_id
, t.impression_count
from bi_analytics.twitter.tweet t
join bi_analytics.twitter.quotes q
    on q.tweet_id = t.conversation_id
join t1
    on (
        t1.twitter_id = q.user_id
        or t1.twitter_id = t.user_id
    )
where t1.ecosystem = 'Sei'
order by t.impression_count desc


with t0 as (
    select c.value:ecosystem::string as ecosystem
    , c.value:twitter_handle::string as twitter_handle
    , c.value:account_type::string as account_type
    , c.value:twitter_id::string as twitter_id
    , c.value:n_followers::int as n_followers
    , u.record_metadata:CreateTime::int as CreateTime
    , row_number() over (partition by twitter_id order by CreateTime desc) as rn
    from crosschain.bronze.data_science_uploads u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'twitter-accounts%'
    and not twitter_handle in ('0xhess')
), t1 as (
    select *
    from t0
    where rn = 1
)
select distinct case when t1.twitter_id = q.user_id then q.quote_tweet_id else t.id end as tweet
, t1.twitter_handle
, concat('https://twitter.com/',t1.twitter_handle,'/status/',tweet) as url
, t.impression_count
from bi_analytics.twitter.tweet t
join bi_analytics.twitter.quotes q
    on q.tweet_id = t.conversation_id
join t1
    on (
        t1.twitter_id = q.user_id
        or t1.twitter_id = t.user_id
    )
where t1.ecosystem = 'Sei'
order by t.impression_count desc

with t0 as (
    select c.value:ecosystem::string as ecosystem
    , c.value:twitter_handle::string as twitter_handle
    , c.value:account_type::string as account_type
    , c.value:twitter_id::string as twitter_id
    , c.value:n_followers::int as n_followers
    , u.record_metadata:CreateTime::int as CreateTime
    , row_number() over (partition by twitter_id order by CreateTime desc) as rn
    from crosschain.bronze.data_science_uploads u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'twitter-accounts%'
    and not twitter_handle in ('0xhess')
), t1 as (
    select *
    from t0
    where rn = 1
)
select concat('https://twitter.com/SeiNetwork/status/',q.quote_tweet_id) as tweet
, *
from bi_analytics.twitter.quotes q
left join t1
    on t1.twitter_id = q.user_id
where tweet_id = '1694008743527317915'



INSERT INTO crosschain_dev.bronze.twitter_accounts (
    twitter_id,
    twitter_handle,
    account_type,
    ecosystems,
    n_followers,
    score
)
select '1491783252570357763', 'bungeeexchange', 'Project', PARSE_JSON('["Ethereum","Binance","Polygon","Avalanche","Fantom","Optimism","Aurora","Arbitrum","zkSync","Base"]'), 100943, 100



select id
, user_id as author_id
, user_followers
, created_at
, like_count
, impression_count
, tweet_type
, clean_url
, platform
, tweet_url
, conversation_id::string as conversation_id
from bi_analytics.twitter.tweet
where 1=1
    and platform = 'Flipside'
    and tweet_type = 'Dashboard'
    and created_at >= dateadd('hours', -48, current_timestamp)


select twitter_id
, ecosystems[0]::string as ecosystem
from crosschain_dev.bronze.twitter_accounts
qualify(
    row_number() over (partition by twitter_id order by updated_at desc) = 1
)

select * from solana.core.dim_labels where address = '6snS2qehjiqp6h3XNavkt75XkM3JTY2YtwPK1z9JAH19'


select date_trunc('month', block_timestamp) as month
, count(distinct signers[0]::string) as n_wallets
from solana.core.fact_events e
where program_id in (
    'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'
    , 'MRGNWSHaWmz3CPFcYt9Fyh8VDcvLJyy2SCURnMco2bC'
)
group by 1
order by 1


select block_timestamp::date as date
, case when swap_from_mint = 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN' then 'from' else 'to' end as direction
, case when direction = 'from' then swap_from_amount else swap_to_amount end as amount
, case when direction = 'from' then -amount else amount end as net_amount
from solana.dex.fact_swaps
where block_timestamp >= '2024-01-30'
    and (swap_from_mint = 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN' or swap_to_mint = 'JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN')
    and swap_from_mint != swap_to_mint
group by 1, 2
order by 1, 2

select *
from solana.price.fact_token_prices_hourly
limit 10
where 



select recorded_hour::date
, close as price
from solana.price.fact_token_prices_hourly
where recorded_hour >= '2024-01-30'
  and symbol = 'JUP'
  and id = '29210'
qualify(
    row_number() over (partition by recorded_hour::date order by recorded_hour desc) = 1
)

with t0 as (
    select *
    from solana.core.fact_transfers
    where block_timestamp >= '2023-12-20'
        and (tx_from = 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC' or tx_to = 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC')
), t1 as (
    select e.block_timestamp
    , e.tx_id
    , e.program_id
    , t0.tx_from
    , t0.tx_to
    , t0.mint
    , t0.amount
    from solana.core.fact_events e
    join t0
        on t0.block_timestamp = e.block_timestamp
        and t0.tx_id = e.tx_id
    where e.block_timestamp >= '2023-12-20'
)
select *
from t1

select *
from solana.defi.fact_swaps
where block_timestamp >= '2023-12-20'
    and swapper = 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC'


with t0 as (
    select *
    from solana.core.fact_transfers
    where block_timestamp >= '2023-12-20'
        and (tx_from = 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC' or tx_to = 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC')
), t1 as (
    select e.block_timestamp
    , e.tx_id
    , e.program_id
    , t0.tx_from
    , t0.tx_to
    , t0.mint
    , t0.amount
    from solana.core.fact_events e
    join t0
        on t0.block_timestamp = e.block_timestamp
        and t0.tx_id = e.tx_id
    where e.block_timestamp >= '2023-12-20'
)
select *
from t1

with e as (
    select distinct tx_id
    from solana.core.fact_events
    where block_timestamp >= '2024-01-04'
        and program_id in (
            'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
            , 'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN'
            , 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
            , 'TL1ST2iRBzuGTqLn1KXnGdSnEow62BzPnGiqyRXhWtW'
            , 'TLoCKic2wGJm7VhZKumih4Lc35fUhYqVMgA4j389Buk'
            , 'TCMPhJdwDryooaGtiocG1u3xcYbRpiJzb283XfCZsDp'
            , 'DeJBGdMFa1uynnnKiwrVioatTuHmNLpyFKnmB5kaFdzQ'
            , 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
            , 'meRjbQXFNf5En86FXT2YPz1dQzLj4Yb3xK8u1MVgqpb'
            , 'TB1Dqt8JeKQh7RLDzfYDJsq8KS4fS2yt87avRjyRxMv'
        )
)
select tx_id
, block_timestamp
, tx_from
, tx_to
, mint
, amount
, case when tx_from = 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC' then -amount else amount end as net_amount
from solana.core.fact_transfers
where block_timestamp >= '2024-01-04'
    and (tx_from = 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC' or tx_to = 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC')
    and not tx_id in (
        select tx_id from e
    )

4148 4601 8901 3890
4148460192891720

with t0 as (
    select distinct address
    from solana.core.dim_labels
    where label = 'saga monkes'
)
select *
from t0
left join solana.nft.fact_nft_sales s
    on s.block_timestamp >= current_date - 7
    and s.mint = t0.address

select *
from solana.core.fact_transactions
where block_timestamp::date = '2024-02-14'::date
    and tx_id in (
        '3km8gnunyq2qTuPongzqwGdWGKGDEXTmbnusYbAAieUSwNpxBprQRM6XkzGhgbWkn6sBj3TLcBZHk8ihRfYgk57N'
        , '64bwz3DAJQXnqbn9Mo3wYwUsSfpFYJFXaj74sFFhyWFure5b7scy7CakeYLscMpzVhs7iW7yozaq5K26mQpMirZV'
        , 'gGAojbdTGiK4txU3ED9VpET3ncz3VwTSJcSAZ9V88HJ58fqiAzMwpuNmHWBVRbaT1kjLdkLSNrbt7C3KTm9r9fX'
    )



with t0 as (
    select distinct address
    from solana.core.dim_labels
    where label = 'saga monkes'
)
select s.*
from solana.nft.fact_nft_sales s
join solana.core.dim_labels l
    on l.address = s.mint
where s.block_timestamp >= current_date - 7
    and l.label = 'homeowners association (parcl)'

with t0 as (
    select *
    from solana.core.fact_transfers
    where mint = 'So11111111111111111111111111111111111111112'
        and (
            from_address in (
                '8w3FgNEbAKvetyMx7qfsVqH6kEjiyCctjkfPcN54Rb1B'
                , 'CoePTwkRYV5HXN2KkKrBFpNsVVk7GyQMr8RBnFVTMfu6'
                , '9SziZx5RRjGbQ4cWaYC9UsvkEJSKM6pMY3ojRZXXEe2q'
                , '9gGNgCDC6jwdiNBHEQG6i6PVGnMU6UN6DXnAU85tgRU8'
            ) or to_address in (
                '8w3FgNEbAKvetyMx7qfsVqH6kEjiyCctjkfPcN54Rb1B'
                , 'CoePTwkRYV5HXN2KkKrBFpNsVVk7GyQMr8RBnFVTMfu6'
                , '9SziZx5RRjGbQ4cWaYC9UsvkEJSKM6pMY3ojRZXXEe2q'
                , '9gGNgCDC6jwdiNBHEQG6i6PVGnMU6UN6DXnAU85tgRU8'
            )
        )
), t1 as (
    select distinct from_address as address from t0
    union select distinct to_address as address from t0
), t2 as (
    select *
    from solana.core.fact_transfers
    where from_address in (select address from t1)
        or to_address in (select address from t1)
)



select program_id
, count(1) as n
from solana.core.fact_events
where signers[0] in (
    '8w3FgNEbAKvetyMx7qfsVqH6kEjiyCctjkfPcN54Rb1B'
)
group by 1
order by 2 desc

select date_trunc('month', block_timestamp) as month
, count(1)
from solana.core.fact_events
where block_timestamp <= '2022-06-01'
    and program_id = 'GQTzsFjz7RbnVL6V8SVgYKE7CYauteRvNAQmHT6H7Ui8'
group by 1
order by 1


select recorded_hour::date as date
, avg(close) as price
from solana.price.fact_token_prices_hourly
where date >= '2024-01-01' and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
group by 1
order by 1
-- forked from Weekly Sharky Loan Volume by Lender @ https://flipsidecrypto.xyz/edit/queries/c9234615-9f69-4e68-b51b-9ac47ed221fe



select case when decoded_instruction:name::string = 'takeLoanV3' then decoded_instruction:accounts[0]:pubkey::string else decoded_instruction:accounts[5]:pubkey::string end as lender
, case when decoded_instruction:name::string = 'takeLoanV3' then decoded_instruction:accounts[3]:pubkey::string else decoded_instruction:accounts[11]:pubkey::string end as mint
, date_trunc('week', block_timestamp) as week
, decoded_instruction:name::string as name
, coalesce(l.label, 'other') as collection
-- , count(1) as n_loans
, (split(decoded_instruction:args:expectedLoan::string, ',')[1]::int) * power(10, -9) as volume
-- , *
from solana.core.fact_decoded_instructions i
left join solana.core.dim_labels l
    on l.address = mint
where block_timestamp >= current_date - 1
    and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
    and decoded_instruction:name::string in ('takeLoanV3','extendLoanV3')

with t0 as (
    select coalesce(l.label, 'other') as collection
    , (case when decoded_instruction:name::string = 'takeLoanV3' then decoded_instruction:accounts[0]:pubkey::string else decoded_instruction:accounts[5]:pubkey::string end) as lender
    , sum(split(decoded_instruction:args:expectedLoan::string, ',')[1]::int) * power(10, -9) as volume
    from solana.core.fact_decoded_instructions i
    left join solana.core.dim_labels l
        on l.address = case when decoded_instruction:name::string = 'takeLoanV3' then decoded_instruction:accounts[3]:pubkey::string else decoded_instruction:accounts[11]:pubkey::string end
    where block_timestamp >= '2024-01-01'
        and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
        and decoded_instruction:name::string in ('takeLoanV3','extendLoanV3')
        and (case when decoded_instruction:name::string = 'takeLoanV3' then decoded_instruction:accounts[0]:pubkey::string else decoded_instruction:accounts[5]:pubkey::string end) in (
            'ySoLLxJfRkecrD4wNL6NXmSw6P6fSmeR7tt1fn4Lqvw'
            , 'H9ko65q5RzfVCPLoti1FE1EP5cjh83UN6gBAqXXHkP3M'
            , '7Wgz6LB4gkd7hr1hjyTur8tZSXC5sx1QYbYGQ7N2w5z7'
            , 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC'
        )
    group by 1, 2
), t1 as (
    select *
    , sum(volume) over (partition by collection) as collection_volume
    , volume / collection_volume as pct_collection_volume
    from t0
) 
select * from t1
, valid as (
    select collection
    , row_number() over (order by volume desc) as rk
    , concat(
        case when rk < 10 then '0' else '' end, rk, '. ', collection
    ) as label
    from t0
    where lender_type = 'Others'
    order by volume desc
    limit 10
), t1 as (
    select *
    , sum(volume) over (partition by lender_type) as lender_type_volume
    , volume * 100 / lender_type_volume as pct_volume
    from t0
), t2 as (
    select t1.*
    from t1
    join valid
        on valid.collection = t1.collection
)
select * from t2
-- group by 1, 2


'7Wgz6LB4gkd7hr1hjyTur8tZSXC5sx1QYbYGQ7N2w5z7'
, 'ySoLLxJfRkecrD4wNL6NXmSw6P6fSmeR7tt1fn4Lqvw'
, 'Hkg99Cz41FkvGKK2RSWfFgFbZUi8xhPjJyDMFZkNVVx8'
, 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC'
, '7woFekwSQ4txRTegnTN57NvnBLLdjLjDBFbwHdd6YnDK'
, '5WdaiasPs86NgtJQkffiW7FqBXg1KgNd3e3zmXUg5DRk'
-- , '6QvvZKGEHxyTKgkHQpfpJXPAgWUAzefXVHYPBFrJGPYP'
-- , 'DHVZfycL8WYxjKkXjFoe2YAMZAdVHDSTNmW4WhSD4YhK'
-- , 'CmbwG6X9t49TbkpRbPsHzKEcNdVSjMMYE3jjoRV8JErP'
-- , 'BK1W2fphfja4TcZE7D58mzh9vuRUVbxNcFQXS7vzgWih'
-- , 'DVtPTMGQVnxoBgqMmh94aF76VDYMGuzrmXYTAaLjnqbt'
-- , 'A3ECFC6kN8Jd6pY4YVW2wbSX6E1QCi3nyF2ao1JVcAKB'


select *
from bi_analytics.twitter.tweet
where created_at >= '2024-01-01'

with t0 as (
    select distinct lower(address) as address
    from bi_analytics.bronze.arprograms_walletconnect
)
select *
from avalanche.core.fact_native_transfers t
-- join t0
--     on t0.address = lower(t.to_address)
where block_timestamp >= '2023-08-01'
    and block_timestamp <= '2024-02-21'
    and lower(from_address) = '0x0487018a6c24ff678435624cef7f5f463175769e'
    and tx_hash in (
        '0x012fc8461d77b6725a27ed0846cd7dbeabf85e74f1a54ec045b76f2a56427573'
        , '0x1293161806da12d7924daf625066664bf98cdba0e8d9cb4a3da5882af8747baa'
        , '0x1554e527a12252308f9602d105f40c21c06e3adc980b091d87fd28481126a01d'
        , '0x15f987f37f0c1dd05b738f7f7a27b7530faefe2b252e7d97e6a855800edf5418'
        , '0x1f3cf588e304a33086061ceec03b2a94b52427203cf039be456a5b6c5e18296d'
        , '0x23ecec9d32447c525351429cc697fd84af43b2d788736fec0ee1d3aa4af3e263'
        , '0x24e27769539ef9dac23f7f889f774393087f6631c045f49a68eb3b3fe7e0d47d'
        , '0x34402612d4fc90898cc214c7bfe385dea8564614db103f28a28104cecebb1ce3'
        , '0x366360a286999cf28c3875681b3eaa4713c6d797ca3bc062dc7c8a8569b1559a'
        , '0x3b308dffdaa853d3d7ca7bc8786ef51abfe969d4b824e7ae5076bb7786b6bbb1'
        , '0x3f25082f91fb88d4e5b4891f94156d24c2e1348564c586f9616e2003cafaa001'
        , '0x501b81a27c67df0cbf058bfbb4f68fef47785b491372bc10a4d42f5d2c3344c5'
        , '0x5cb4cac888703f2a722232618e64590608fcad146ad0a5af7827a4ba5f69482c'
        , '0x5d666cef8d23f7af0591c406f3f3040c8ae4057727c07066529385911d78a899'
        , '0x6a4dbb99e5a056aae3565ef3720c3052414f2a765dd8198f8c1a61fe3008f226'
        , '0x78529ab9eac6f05efd634b6939fc4fbbb7d9481ca85cd3c5b24ffb51605e962d'
        , '0x897b61fcc4d87467183c90e5e18587796bc7b3d7cb1b5c88c427e490e190bfd0'
        , '0x8e1adb376fecab01de634b4aaa0635b16d655e50ffe07f160a18e4c1708ba5d8'
        , '0x8e558605e7e5708143a2151aad7294d77fd729f8b26c80821836f243b6ad9828'
        , '0x9533fee403b62327a7dbbf6bbb81b64ce6c809f9c9442944ef678806119f4bdd'
        , '0xa71edbe4456e8fcfd49bccf247a874ea38bb11e489c982e522254692908c6c25'
        , '0xb478aa342a139c05a5b5ac07c5362ac061b0bb81579ffb1adecd7f8cab94ab84'
        , '0xb71300f0cbf4f0810c084aeb83f51a4a04dc39a179f2677b83d867c8d117db1f'
        , '0xc0d318ce0351b02ea8f565ee70a1707b3ed64a0054a846ea4c5a3fc5fe44931c'
        , '0xc131edb13797d86a14bfd37a8ae820f6f06066bfc7c3852898637d11df7f967e'
        , '0xc57243684020cc21afa740979c49e4fc313fe0adf254b651bd0dd7faeb68f78e'
        , '0xc8b8e0583a99194290ca034fe2a58fec3f88095ed33a231084fb50c5b3d54eb8'
        , '0xdd61299bbd94b28fd78917589e014b96fe2767f0582231f7d3fdf58b75a0096f'
        , '0xe88385c004715010658b4f6dceedb5312c1bf8aabf0c2a7fc2015c8286c26933'
        , '0xfe632490d67c3d3f38af57c743a75a8615c024a7efe45efc78472f92e0e4ed1d'
    )
    and round(amount, 2) in (5, 2, 1, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1)



select *
from avalanche.core.ez_native_transfers t
where block_timestamp >= '2023-08-01'
    and block_timestamp <= '2024-02-21'
    and lower(from_address) = '0x0487018a6c24ff678435624cef7f5f463175769e'
    and tx_hash in (
        '0x012fc8461d77b6725a27ed0846cd7dbeabf85e74f1a54ec045b76f2a56427573'
        , '0x1293161806da12d7924daf625066664bf98cdba0e8d9cb4a3da5882af8747baa'
        , '0x1554e527a12252308f9602d105f40c21c06e3adc980b091d87fd28481126a01d'
        , '0x15f987f37f0c1dd05b738f7f7a27b7530faefe2b252e7d97e6a855800edf5418'
        , '0x1f3cf588e304a33086061ceec03b2a94b52427203cf039be456a5b6c5e18296d'
        , '0x23ecec9d32447c525351429cc697fd84af43b2d788736fec0ee1d3aa4af3e263'
        , '0x24e27769539ef9dac23f7f889f774393087f6631c045f49a68eb3b3fe7e0d47d'
        , '0x34402612d4fc90898cc214c7bfe385dea8564614db103f28a28104cecebb1ce3'
        , '0x366360a286999cf28c3875681b3eaa4713c6d797ca3bc062dc7c8a8569b1559a'
        , '0x3b308dffdaa853d3d7ca7bc8786ef51abfe969d4b824e7ae5076bb7786b6bbb1'
        , '0x3f25082f91fb88d4e5b4891f94156d24c2e1348564c586f9616e2003cafaa001'
        , '0x501b81a27c67df0cbf058bfbb4f68fef47785b491372bc10a4d42f5d2c3344c5'
        , '0x5cb4cac888703f2a722232618e64590608fcad146ad0a5af7827a4ba5f69482c'
        , '0x5d666cef8d23f7af0591c406f3f3040c8ae4057727c07066529385911d78a899'
        , '0x6a4dbb99e5a056aae3565ef3720c3052414f2a765dd8198f8c1a61fe3008f226'
        , '0x78529ab9eac6f05efd634b6939fc4fbbb7d9481ca85cd3c5b24ffb51605e962d'
        , '0x897b61fcc4d87467183c90e5e18587796bc7b3d7cb1b5c88c427e490e190bfd0'
        , '0x8e1adb376fecab01de634b4aaa0635b16d655e50ffe07f160a18e4c1708ba5d8'
        , '0x8e558605e7e5708143a2151aad7294d77fd729f8b26c80821836f243b6ad9828'
        , '0x9533fee403b62327a7dbbf6bbb81b64ce6c809f9c9442944ef678806119f4bdd'
        , '0xa71edbe4456e8fcfd49bccf247a874ea38bb11e489c982e522254692908c6c25'
        , '0xb478aa342a139c05a5b5ac07c5362ac061b0bb81579ffb1adecd7f8cab94ab84'
        , '0xb71300f0cbf4f0810c084aeb83f51a4a04dc39a179f2677b83d867c8d117db1f'
        , '0xc0d318ce0351b02ea8f565ee70a1707b3ed64a0054a846ea4c5a3fc5fe44931c'
        , '0xc131edb13797d86a14bfd37a8ae820f6f06066bfc7c3852898637d11df7f967e'
        , '0xc57243684020cc21afa740979c49e4fc313fe0adf254b651bd0dd7faeb68f78e'
        , '0xc8b8e0583a99194290ca034fe2a58fec3f88095ed33a231084fb50c5b3d54eb8'
        , '0xdd61299bbd94b28fd78917589e014b96fe2767f0582231f7d3fdf58b75a0096f'
        , '0xe88385c004715010658b4f6dceedb5312c1bf8aabf0c2a7fc2015c8286c26933'
        , '0xfe632490d67c3d3f38af57c743a75a8615c024a7efe45efc78472f92e0e4ed1d'
    )
    and round(amount, 2) in (5, 2, 1, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1)

select *
from solana.core.dim_labels
where address in (
    '4eERrmPJJ8TbvYmJbzz3tzWnnfgrfpkEk3RXhWXiSFme'
    , '4NYPnQ2ifD6uvoBFtWxQUUFfKTyGSUjkjtx3etWnsWXc'
    , '3jdGUaPSp4CJtzAoMhbQhu61ebDGdQoWUTURPqnqt1BQ'
    , '8xrWcpbEsJH353v93KZv7AAPitR3hyriGHqpRCTSjqbF'
    , 'xmmaThWDBLmmgH2qhYgfzkp5BQcYuoTqCfjNhudZrL1'
    , '5c1FxKF1ZTve2Fh5mcq5rKB7ZgCPG8jXrsar9kSUiHXF'
    , '3A3rwWCyTZM7WFgKxmhJF6TRjon49T9bRjKffQ7Uj3sU'
    , '5CfQAKHqowP95Jwy99o6bNsCEvEvAvMPkAjo9eZbpN7a'
    , 'F9tPPQNVYYvvvvnBpHaTbhiZ5MkZtmVURsxSqPtNiG4J'
    , '9G7zfGks6scRmdyN3QmL45rcrBMompbRL5RWbVLhNdm4'
    , 'DKRMFBLhCXYx2RfuYTfekN5q3SLazQoiCSguUXY2GqDj'
    , '5YMThZKmzamyQorYLxDkgNtw6VoP9meZAW9tNAdZnsFL'
    , '7dyeuEY13cuZNjLiBvyaHczJVbvXdstzPe8vRVTa2GrX'
    , '9g8JeTRMQ5bxEE2UinF4PBZuZpbtpn7oV5Gsv8YF7XDN'
    , '9mrKzarVBn2xRcuJj8GFbU671kLtfP3YT171sVz63YTt'
    , 'BH4EQykX2WJNmEBoWqYJqbufVMvH8DyQeFQjgtE9aQz2'
    , 'A2ZGnUyXTHcZQhS7qUpP1NJKqRETbC4FXxyVBSMq6TeR'
    , 'GaN8jMuNzBuomMZPiXmQaW4U57YcmThTGFJLZNp3MG1m'
    , 'CiBiXAZsmVE2nMDRMgJWUpHcwQu2QzuxRb5qmobsBUai'
    , '8RT1VjiRDFhNbvwydMJr4icycbxvFKoLLhuvFjVYo3UC'
    , '9yJsgZz4PopsS38m7taVy26B6Ego1PVxL9ByMFshZb6h'
    , 'Ds5HBAt8wfL9frZxgjp6A75pMmdViAf68YphGjwxjcsk'
    , '8HRQPDkBgrkKTxUBZgGL8H78Ek581byJhsymJD7CvyKG'
    , '44GtQL54u9aoY5MrhuiqYeVrvDN7fyNyDqUqQhSbc7DW'
    , 'Ezs4kcKJqYEUaqunJXC7HcNaFzMMSGo2HzPTxA5VB1ZS'
    , '9PmhCCjSRzGBAXVi96x1XEhGZKuDwiQXuAwnXxZaMWmi'
    , 'CAxR32uuvZpqFfi17t87zjkenVVAk4pnVuifvd2e2gYf'
    , '5ZKqqvdcNL3xw3vDwe8FfB7PmqC7RrAHvT8ht8W1Y2Yc'
    , '9xz3Da5cYuCrfrJZE61Qwga4zeSjAfgtNdFPBSFW8yAU'
    , 'B9phG6Yzh3gsHRRzAVEgjC4dScRLD8uGVtA8Nj7SZMsw'
    , 'Bu9p8ou2ViSs5Ns5a7MwmTdieVbsEdndShppfzCvHD15'
    , '3ui8zUqWMyUBZC4BCKU8PSaAyyjXrekzpZgNu12trMB9'
    , 'CkU7vwtvKZZ6vm93Z76yBCAswa3fU7UdThkSsdsKBQaB'
    , '7G9QBMjb71AA6rV4cpgPJGRAZaFDQ1B2oNnqGPxVNmCR'
    , '9pVazKdfFh6bkNcPvcWL3PcRp8jTU2Wo72vP7qP4gg35'
    , 'Ho7kAcXJK4kCpoRhdiwH7HWVeAmE22ALArMZT5MVberS'
    , '3sRLdNNS3M3jdkaQYAkHEfHqXkAJ3AcZDrxidzAXToDM'
    , '43mEodjhiTatFHtG4KvfQsfk8HGwsGTnWnf81aw4NTL5'
    , 'G69tD9hUAtM6xe1HPZKfnnz9m6GUBpCQijrgaQPwmCAA'
    , 'EX1aYcT8KeiefieJAbi22KvNuMGzG8XvH4c6xUaZ9MpR'
    , 'HTxvAxP7L71eUvdTcMjaiD5XhmErwMaaGKSv7eJZco1a'
)


with t0 as (
    select coalesce(l.label, 'other') as collection
    , tx_id
    , block_timestamp
    , dateadd('minute', -10, block_timestamp) as prv_timestamp
    , split(decoded_instruction:args:expectedLoan::string, ',')[1]::int * power(10, -9) as volume
    from solana.core.fact_decoded_instructions i
    left join solana.core.dim_labels l
        on l.address = case when decoded_instruction:name::string = 'takeLoanV3' then decoded_instruction:accounts[3]:pubkey::string else decoded_instruction:accounts[11]:pubkey::string end
    where block_timestamp >= current_date - 7
        and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
        and decoded_instruction:name::string in ('takeLoanV3','extendLoanV3')
), burst as (
    select t0.collection
    , t0.tx_id
    , sum(volume) as burst_volume
    , count(distinct tx_id) as n_tx
    from t0
    join t0 t0b
        on t0b.block_timestamp >= t0.prv_timestamp
        and t0b.block_timestamp <= t0.block_timestamp
        and t0b.collection = t0.collection
    group by 1, 2
), b0 as (
    select collection
    , max(burst_volume) as burst_volume
    , max(n_tx) as burst_tx
    from burst
    group by 1
), t1 as (
    select collection
    , sum(volume) as tot_volume
    , count(1) as n_loans
    from t0
    group by 1
)
select t1.*
, b0.burst_volume
, b0.burst_tx
from t1
join b0
    on b0.collection = t1.collection
order by tot_volume desc



with labels as (
    select distinct c.value:address::string as address
    , c.value:project_name::string as label
    from crosschain.bronze.address_labels u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:CreateTime::int >= 1706011200000
        and record_metadata:key::string like 'labels-solana%'
        and c.value:l1_label::string = 'nft'
    union
    select address
    , label
    from solana.core.dim_labels
), t0 as (
    select coalesce(m.nft_collection_name, l.label, 'Other') as collection
    , case when decoded_instruction:name::string = 'takeLoanV3' then decoded_instruction:accounts[3]:pubkey::string else decoded_instruction:accounts[11]:pubkey::string end as loan_mint
    -- , tx_id
    -- , block_timestamp
    , split(decoded_instruction:args:expectedLoan::string, ',')[1]::int * power(10, -9) as volume
    , *
    from solana.core.fact_decoded_instructions i
    left join labels l
        on l.address = loan_mint
    left join solana.nft.dim_nft_metadata m
        on m.mint = loan_mint
    where block_timestamp >= current_date - 7
        and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
        and decoded_instruction:name::string in ('takeLoanV3','extendLoanV3')
)
select *
from t0
where collection ilike '%mad lads%' or collection ilike '%smyth%' or collection ilike '%monkey baby%'
order by collection

select *
from solana.core.fact_decoded_instructions i
where block_timestamp >= current_date - 7
    and program_id = 'TLoCKic2wGJm7VhZKumih4Lc35fUhYqVMgA4j389Buk'

-- forked from jackguy / Tensor Lock Final 11 @ https://flipsidecrypto.xyz/jackguy/q/BPZgwaVX2ki-/tensor-lock-final-11

with maker_txs as (
  select
    block_timestamp as bt1
    , tx_id as maker_tx 
    , signers[0]::string as maker_address
    , fact_events.instruction :accounts[2]::string as order_address
    , regexp_replace(f_logs.value, '^Program log: Instruction: ') as instruction_log
    , fact_events.instruction:accounts[3]::string as collection_address
    , case
        when collection_address = '3PJCoXPcswZEx8ZimR2xiQvr6sJGJWTp3cpsLhhc7pmP' then 'mad lads'
        when collection_address = '5SmBrw3z7wqXTZXxxZXRLDoiqY6yRzN57hiLULnRGihA' then 'sols spl20'
        when collection_address = '5ceCcEQ2PjYMXbsh956GZZPC5R1sZGnw4UBaJpVdDPE4' then 'froganas'
        when collection_address = 'AgnGG7RtqQncFcdJAT27GhnRWchk3wBDfsESkVXhmejq' then 'tensorians'
        when collection_address = 'EGdryU8HzwqhT8NpPno6CVT8D2Nr2CL4gJTF57NvX1UV' then 'cryptoundeads'
        when collection_address = 'EKPqEMbgMuf6qD69FMGnQamW2f5vCssDrqmoXJAiyjW1' then 'flash trade'
        when collection_address = 'CtXrSG5dzVbRoBpDyAhM69p7tjNuL3oTy18PfoKNQHyy' then 'claynosaurz'
        when collection_address = '2EywrATVadqRPPa1FoxtXV8QCkQgBrtNrcjWkw3g4HKV' then 'lifinity flares'
        when collection_address = '8uoZmfyBYmSumPzDwABz7gN7u6FAY4ziKLAWw7LYTYa7' then 'okay bears'
        when collection_address = 'HXESogjkv3jfw1qc82VZBtBQAan1h4pZ4trBjVaT4R4b' then 'sujiko warrior'
        when collection_address = 'WudU7ovgexnZLQoGhXcRYCtuzMwyFUftnkysZwCCZTW' then 'elixir: ovols'
        when collection_address = '5wnPJvyhgTMRU94MPxNLSLeYkfzCJeXMtvEjPrwPiB6A' then 'bodoggos'
        when collection_address = 'CvRbqkLj7cVztw8PRqyUncFQBrHWLaSMMXoSZ2kYiBPJ' then 'chads'
        when collection_address = 'Ddk7szjyjEtzwqajNY6ospWZjrQEYTYAg9GK65zUDc2J' then 'banx'
        when collection_address = 'CzSoeDrGdHVQFATmdx2LmarDNxvLn41xFxfJfaijfedS' then 'smb gen3'
        when collection_address = 'B9tkGi9aRz9YsxfLH5PTWf32PsWSQqccHv1DqZqj4khh' then 'marms'
        when collection_address = 'b8YmUZ2Z7dd3M7vXGcuK3t71KTrtbSBFHxFmxwP2WjP' then 'homeowners association (parcl)'
        when collection_address = 'F3nUN87ta1MXWHQVxtx9RSV95MMJoL173i9JveGd73Qs' then 'quekz'
        when collection_address = '8z2oktNUCRKJNCw6D5Hy8Fj8Rs5HQmKANa9KL2yWz86J' then 'famous fox federation'
        when collection_address = 'GFqNEdvnY3T2iX2NQenGNuXnkmhRfPgrnCKFJ51J6TDC' then 'sharx'
        when collection_address = 'DjLrH2MxHEUj79Jb9oB7KpJ8u85JT681Do1jboVP9okq' then 'smb gen2'

        else collection_address
      end as collection_name
    , case
        when array_contains('Program log: Instruction: DepositLegacy' ::variant, log_messages)
          or array_contains('Program log: Instruction: DepositCompressed' ::variant, log_messages)
          then 'Long'
        else 'Short'
      end as order_direction

  from solana.core.fact_events
  inner join solana.core.fact_transactions
    using(tx_id, block_timestamp, succeeded)
  inner join lateral flatten (input => fact_transactions.log_messages) f_logs
  where block_timestamp >= current_date - 7
    and succeeded
    and fact_events.program_id ='TLoCKic2wGJm7VhZKumih4Lc35fUhYqVMgA4j389Buk'
    and f_logs.value = 'Program log: Instruction: UpsertOrder'
    and fact_transactions.log_messages[f_logs.index -1] = 'Program TLoCKic2wGJm7VhZKumih4Lc35fUhYqVMgA4j389Buk invoke [1]'    
    --and order_direction like 'Short'
    --and block_timestamp < '2024-01-23 16:00:00'
  qualify row_number() over (partition by order_address order by block_timestamp desc) = 1
),

lock_txs as (

  select

    block_timestamp as bt
    , tx_id 
    --, signers[0] as taker_address
    , fact_events.instruction:accounts[4]::string as order_address1
    , regexp_replace(f_logs.value, '^Program log: Fees ')::string as fees_string
    , parse_json(fees_string) :premium ::number / pow(10, 9) as premium
    , parse_json(fees_string) :premium_net_fees ::number / pow(10, 9) as premium_net_fee
    , parse_json(fees_string) :total_fee ::number / pow(10, 9) as total_fee

  from solana.core.fact_events
  inner join solana.core.fact_transactions
    using(tx_id, block_timestamp, succeeded, block_id)
  inner join lateral flatten (input => fact_transactions.log_messages) f_logs
  where block_timestamp >= current_date - 7
    and succeeded
    and fact_events.program_id = 'TLoCKic2wGJm7VhZKumih4Lc35fUhYqVMgA4j389Buk'
    and array_contains('Program log: Instruction: LockOrder' ::variant, log_messages)
    and f_logs.value rlike '^Program log: Fees \{.*\}'
    and fact_transactions.log_messages[f_logs.index -3] = 'Program TLoCKic2wGJm7VhZKumih4Lc35fUhYqVMgA4j389Buk invoke [1]'
    and fact_transactions.log_messages[f_logs.index -2] = 'Program log: Instruction: LockOrder'
)

select m.collection_name
, m.order_direction
, m.maker_address
, m.maker_tx
, l.tx_id
, l.order_address1
, l.premium
, l.premium_net_fee
, l.total_fee
, case when collection_name in (
    'tensorians'
    , 'claynosaurz'
    , 'mad lads'
    , 'smb gen2'
) then 0.03 else 0.041 end as fee_pct
, premium / fee_pct as fund_amount
from maker_txs m
  left outer join lock_txs l
    on order_address1 = order_address
where not bt is null
-- group by 1,2,3



select
coalesce(m.user_id, d.created_by_id) as user_id,
d.profile_id as profile_id,
coalesce(tu.username, u.username) as username,
d.id as dashboard_id,
d.title,
p.type,
coalesce(c.chain, 'Polygon') as ecosystem,
row_number() over (
    order by
    dr.ranking_trending
) as current_rank
, dr.*
from
bi_analytics.content_rankings.dashboard_rankings dr
join bi_analytics.velocity_app_prod.dashboards d on d.id = dr.dashboard_id
left join bi_analytics.velocity_app_prod.profiles p
    on p.id = d.profile_id
left join bi_analytics.velocity_app_prod.teams t
    on t.profile_id = p.id
left join bi_analytics.velocity_app_prod.members m
    on t.id = m.team_id
    and m.role = 'owner'
left join bi_analytics.velocity_app_prod.users tu
    -- kellen changed this line
    on tu.id = m.user_id
join bi_analytics.velocity_app_prod.users u on u.id = d.created_by_id
order by dr.ranking_trending




select hour::date as date
-- , token_address
-- , symbol
-- , price
, *
from crosschain.price.ez_hourly_token_prices
where token_address = '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
qualify(
    row_number() over (partition by hour::date order by price desc) = 1
)
order by price desc
limit 10000


select *
from solana.core.dim_labels
where label ilike 'tensor%'

select *
from solana.nft.fact_nft_sales s
join solana.core.dim_labels
where block_timestamp >= current_date - 3
qualify(
    row_number() over (partition by program_id order by block_timestamp desc) <= 3
)
limit 100


with labels as (
    select distinct c.value:address::string as address
    , c.value:project_name::string as label
    from crosschain.bronze.address_labels u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:CreateTime::int >= 1706011200000
        and record_metadata:key::string like 'labels-solana%'
    union
    select address
    , label
    from solana.core.dim_labels
), t0 as (
    select coalesce(l.label, m.nft_collection_name, 'Other') as collection
    , sum(s.sales_amount) as volume
    , count(1) as n_sales
    from solana.nft.fact_nft_sales s
    left join labels l
        on l.address = s.mint
    left join solana.nft.dim_nft_metadata m
        on m.mint = s.mint
    where s.succeeded
        and s.block_timestamp >= '2021-09-01'
        and s.block_timestamp <= '2022-01-01'
    group by 1
)
select *
from t0
order by volume desc



with labels as (
    select distinct c.value:address::string as address
    , c.value:project_name::string as label
    from crosschain.bronze.address_labels u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:CreateTime::int >= 1706011200000
        and record_metadata:key::string like 'labels-solana%'
    union
    select address
    , label
    from solana.core.dim_labels
), t0 as (
    select coalesce(m.nft_collection_name, l.label, 'Other') as collection
    , block_timestamp::date as date
    , median(s.sales_amount) as floor_price
    from solana.nft.fact_nft_sales s
    left join labels l
        on l.address = s.mint
    left join solana.nft.dim_nft_metadata m
        on m.mint = s.mint
    where s.succeeded
        and s.block_timestamp >= '2021-08-01'
        and s.block_timestamp <= '2022-03-01'
        and collection in (
            'aurory'
            , 'degen ape academy'
            , 'galactic geckos'
            , 'shadowy super coder'
            , 'Meerkat Millionaires CC'
            , 'thugbirdz'
            , 'roguesharks'
            , 'Solsteads Surreal Estate'
            , 'Portals'
            , 'Pesky Penguins'
            , 'Boryoku Dragonz'
            , 'monkey kingdom'
            , 'The Catalina Whale Mixer'
            , 'bold badgers'
            , 'solchicks'
            , 'turtles'
            , 'Cyber Frogs'
            , 'Degenerate Trash Pandas'
            , 'Famous Fox Federation'
            , 'Danger Valley Ducks'
            , 'nyan heroes'
            , 'solana monkette business'
            , 'Doge Capital'
            , 'Stoned Ape Crew'
            , 'Photo Finish PFP Collection'
            , 'Unirexcity'
            , 'Grim Syndicate'
            , 'solcities nft'
            , 'TYR'
            , 'boss bulls club'
            , 'Lifinity Flares'
            , 'apexducks'
            , 'fine fillies'
            , 'trippy bunny tribe'
            , 'cyber samurai'
        )
    group by 1, 2
    union
    select 'SOL' as collection
    , recorded_hour::date as date
    , avg(close) as floor_price
    from solana.price.ez_token_prices_hourly
    where token_address = '2021-08-01'
        and recorded_hour >= '2022-03-01'
        and is_imputed = FALSE
        and close < 1000000
    group by 1, 2
)
select *
from t0



select d.id as dashboard_id
, tm.*
-- , t.id as tweet_id
-- , t.conversation_id
-- , d.title
-- , d.latest_slug
-- , u.username
-- , u.id as user_id
-- , t.impression_count
-- , case when u.user_name is null then t.tweet_url else CONCAT('https://twitter.com/',u.user_name,'/status/',t.id) end as tweet_url
-- , t.created_at::date as tweet_date
from bi_analytics.velocity_app_prod.dashboards d
-- join bi_analytics.twitter.tweet t
--     on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
-- left join bi_analytics.twitter.user tu
--     on tu.id = t.user_id
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
left join bi_analytics.velocity_app_prod.profiles p
    on p.id = d.profile_id
left join bi_analytics.velocity_app_prod.teams tm
    on tm.profile_id = p.id
where d.created_at >= current_date - 10
where not d.id in (
    select dashboard_id from labels where dashboard_tag = 'bot'
)
qualify(
    row_number() over (partition by t.conversation_id order by impression_count desc) = 1
    and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
)


with t0 as (
    select distinct tx_id
    , block_timestamp
    from solana.core.fact_events
    where block_timestamp >= '2024-02-28'
        and program_id = 'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN'
        and signers[0]::string in (
            'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC'
            , '6YUm9FiU8Ub9Lf4PHPocyu7RojrpRRvW8eq8XDiHHjM9'
            , '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj'
            , 'AoNVE2rKCE2YNA44V7NQt8N73JdPM7b6acZ2vzSpyPyi'
            , 'GFCYkPusxYWJ1yEoHNrFwjgLUmuCYiwC4nhwrV2UpMBk'
            , 'DwoMs2cjnPYqZEuU7erayAKVf5nSkjgEEWCcrzKDSiCU'
        )
), t1 as (
    select distinct coalesce(l.label, 'other') as label
    , t.mint
    , t.block_timestamp
    , t.tx_id
    from solana.core.fact_transfers t
    join t0
        on t0.block_timestamp = t.block_timestamp
        and t0.tx_id = t.tx_id
    join solana.core.dim_labels l
        on l.address = t.mint
    where t.block_timestamp >= '2024-02-28'
        and t.amount = 1
        and t.tx_from in (
            'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC'
            , '6YUm9FiU8Ub9Lf4PHPocyu7RojrpRRvW8eq8XDiHHjM9'
            , '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj'
            , 'AoNVE2rKCE2YNA44V7NQt8N73JdPM7b6acZ2vzSpyPyi'
            , 'GFCYkPusxYWJ1yEoHNrFwjgLUmuCYiwC4nhwrV2UpMBk'
            , 'DwoMs2cjnPYqZEuU7erayAKVf5nSkjgEEWCcrzKDSiCU'
        )
)
select t1.tx_id
, t1.mint as collateralMint
, t1.label
-- , sum(t.amount) as soldPrice
, t.mint
, t.tx_from
, t.tx_to
from t1
left join solana.core.fact_transfers t
    on t1.block_timestamp = t.block_timestamp
    and t1.tx_id = t.tx_id
    and t.block_timestamp >= '2024-02-28'
    -- and t.mint = 'So11111111111111111111111111111111111111112'
    -- and t.tx_to in (
    --     'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC'
    --     , '6YUm9FiU8Ub9Lf4PHPocyu7RojrpRRvW8eq8XDiHHjM9'
    --     , '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj'
    --     , 'AoNVE2rKCE2YNA44V7NQt8N73JdPM7b6acZ2vzSpyPyi'
    --     , 'GFCYkPusxYWJ1yEoHNrFwjgLUmuCYiwC4nhwrV2UpMBk'
    --     , 'DwoMs2cjnPYqZEuU7erayAKVf5nSkjgEEWCcrzKDSiCU'
    -- )
group by 1, 2, 3


with t2 as (
    select decoded_instruction:args:config:startingPrice::int * pow(10, -9) as amount
    , decoded_instruction:accounts[6]::string as collateralMint
    , tx_id
    , block_timestamp
    from solana.core.fact_decoded_instructions
    where block_timestamp >= '2024-02-21'
        and program_id = 'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN'
        and event_type = 'sellNftTokenPool'
        and signers[0]::string in (
                'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC'
                , '6YUm9FiU8Ub9Lf4PHPocyu7RojrpRRvW8eq8XDiHHjM9'
                , '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj'
                , 'AoNVE2rKCE2YNA44V7NQt8N73JdPM7b6acZ2vzSpyPyi'
                , 'GFCYkPusxYWJ1yEoHNrFwjgLUmuCYiwC4nhwrV2UpMBk'
                , 'DwoMs2cjnPYqZEuU7erayAKVf5nSkjgEEWCcrzKDSiCU'
            )
), t3 as (
    select t2.collateralMint
    , t2.amount
    , (post_balances[0] - pre_balances[0]) * pow(10, -9) as soldPrice
    , tx.tx_id
    , coalesce(l.label, 'other') as label
    , tx.account_keys
    , tx.account_keys[0]:pubkey::string as pubkey
    from t2
    join solana.core.fact_transactions tx
        on tx.block_timestamp = t2.block_timestamp
        and tx.tx_id = t2.tx_id
    left join solana.core.dim_labels l
        on l.address = t2.collateralMint
    where tx.block_timestamp >= '2024-02-21'
        -- and tx.account_keys[0]:pubkey::string in (
        --     'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC'
        --     , '6YUm9FiU8Ub9Lf4PHPocyu7RojrpRRvW8eq8XDiHHjM9'
        --     , '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj'
        --     , 'AoNVE2rKCE2YNA44V7NQt8N73JdPM7b6acZ2vzSpyPyi'
        --     , 'GFCYkPusxYWJ1yEoHNrFwjgLUmuCYiwC4nhwrV2UpMBk'
        --     , 'DwoMs2cjnPYqZEuU7erayAKVf5nSkjgEEWCcrzKDSiCU'
        -- )
)
select * from t3


https://twitter.com/durdenwannabe/status/1737029117160341505
https://twitter.com/SolanaFloor/status/1736303167208411549


select
-- timestamp
-- , CONTEXT_PAGE_TAB_URL
-- , INITIAL_REFERRER
-- , ANONYMOUS_ID
-- , CONTEXT_PAGE_INITIAL_REFERRING_DOMAIN
-- , REFERRER
-- , CONTEXT_PAGE_REFERRER
-- , CONTEXT_PAGE_INITIAL_REFERRER
-- , REFERRING_DOMAIN
-- , USER_ID
-- , CONTEXT_CAMPAIGN_SOURCE
-- , CONTEXT_CAMPAIGN_MEDIUM
*
from bi_analytics.gumby.pages
order by timestamp desc
limit 100


with t0 as (
    select distinct tx_id
    from solana.core.fact_decoded_instructions i
    where block_timestamp >= current_date - 1
        and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
)
select t.tx_id
, sum(amount) as amount
from t0
join solana.core.fact_transfers t
    on t.tx_id = t0.tx_id
where t.block_timestamp >= current_date - 1
    and t.tx_to in ('feegKBq3GAfqs9G6muPjdn8xEEZhALLTr2xsigDyxnV','Comm6vCS1FYZfj2fKe9zs1ySGPH1RPk763esntruvV4Y')
    and t.mint = 'So11111111111111111111111111111111111111112'
group by 1
order by 2 desc


select *
-- decoded_instruction:name::string, count(1)
-- , case when decoded_instruction:name::string = 'takeLoanV3' then decoded_instruction:accounts[3]:pubkey::string else decoded_instruction:accounts[11]:pubkey::string end as loan_mint
-- , tx_id
-- , block_timestamp
-- , split(decoded_instruction:args:expectedLoan::string, ',')[1]::int * power(10, -9) as volume
-- , *
from solana.core.fact_decoded_instructions i
where block_timestamp >= current_date - 1
    and program_id = 'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu'
    -- and decoded_instruction:name::string in ('takeLoanV3','extendLoanV3')
group by 1
order by 2 desc


select u.id
, u.username
, count(distinct q.id) as n_queries
from bi_analytics.velocity_app_prod.users u
join bi_analytics.velocity_app_prod.queries q
    on q.created_by_id = u.id
where q.created_at >= current_date - 60
group by 1, 2
order by 3 desc

select q.created_at::date as date
, q.title
from bi_analytics.velocity_app_prod.users u
join bi_analytics.velocity_app_prod.queries q
    on q.created_by_id = u.id
where q.created_at >= current_date - 60
    and u.id = 'd7a2f7be-31e5-4dff-b551-8a3737af84f0'
order by date desc



with p0 as (
    select token_address as mint
    , date_trunc('hour', recorded_hour) as hour
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour::date >= CURRENT_DATE - 60
        and is_imputed = FALSE
    group by 1, 2
), p1 as (
    select token_address as mint
    , date_trunc('day', recorded_hour) as date
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 60
        and is_imputed = FALSE
    group by 1, 2
), p2 as (
    select token_address as mint
    , date_trunc('week', recorded_hour) as week
    , avg(close) as price
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= CURRENT_DATE - 60
        and is_imputed = FALSE
    group by 1, 2
), t0 as (
    select swapper
    , block_timestamp
    , block_timestamp::date as date
    , case when swap_to_mint = 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm' then 'Buy'
        when swap_from_mint = 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm' then 'Sell'
        else 'Other' end as swap_type
    , case when swap_type = 'Buy' then swap_to_amount else swap_from_amount as amount
    , case when swap_type = 'Buy' then amount else -amount end as net_amount
    , case when swap_type = 'Buy' then swap_from_mint else swap_to_mint end as other_mint
    , case when swap_type = 'Buy' then swap_from_amount else swap_to_amount end as other_amount
    , other_amount * coalesce(p0.price, p1.price, p2.price, 0) as volume_usd
    from solana.defi.fact_swaps s
    left join p0
        on p0.hour = date_trunc('hour', s.block_timestamp)
        and p0.mint = other_mint
    left join p1
        on p1.date = date_trunc('day', s.block_timestamp)
        and p1.mint = other_mint
    left join p2
        on p2.week = date_trunc('week', s.block_timestamp)
        and p2.mint = other_mint
    where s.block_timestamp >= '2024-01-01' 
        and swapper = 'MfDuWeqSHEqTFVYZ7LoexgAK9dxk7cy4DFJWjWMGVWa'
        and (
            swap_to_mint = 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm'
            or swap_from_mint = 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm'
        )
        and succeeded
)
select *
from t0

, buys as (
    select *
    from t0
    where swap_type = 'Buy'
)
, sales as (
    select *
    from t0
    where swap_type = 'Sell'
)
, get_prv_buy as (
    select s.*
    b.
    from sales s
    join buys b
        on b.swapper = s.swapper
        and b.block_timestamp <= s.block_timestamp
)
, t1 as (
    select date
    , count(1) as n_swaps
    , sum(amount) as amount
    , sum(net_amount) as net_amount
    from t0
    group by 1
)
select *
, sum(net_amount) over (order by date) as cumu_net_amount
from t1


select *
-- decoded_instruction:name::string, count(1)
-- , case when decoded_instruction:name::string = 'takeLoanV3' then decoded_instruction:accounts[3]:pubkey::string else decoded_instruction:accounts[11]:pubkey::string end as loan_mint
-- , tx_id
-- , block_timestamp
-- , split(decoded_instruction:args:expectedLoan::string, ',')[1]::int * power(10, -9) as volume
-- , *
from solana.core.fact_decoded_instructions i
where block_timestamp >= current_date - 1
    and program_id = 'voTpe3tHQ7AjQHMapgSue2HJFAh2cGsdokqN3XqmVSj'
    -- and decoded_instruction:name::string in ('takeLoanV3','extendLoanV3')
-- group by 1
-- order by 2 desc
limit 100


select
timestamp
, CONTEXT_PAGE_TAB_URL
, INITIAL_REFERRER
, ANONYMOUS_ID
, CONTEXT_PAGE_INITIAL_REFERRING_DOMAIN
, REFERRER
, CONTEXT_PAGE_REFERRER
, CONTEXT_PAGE_INITIAL_REFERRER
, REFERRING_DOMAIN
, USER_ID
, CONTEXT_CAMPAIGN_SOURCE
, CONTEXT_CAMPAIGN_MEDIUM
from bi_analytics.gumby.pages
where CONTEXT_CAMPAIGN_SOURCE = 'ds-app'
order by timestamp desc
limit 1000

with t0 as (
    select distinct tx_id
    , block_timestamp
    , program_id
    , signers[0]::string as address
    from solana.core.fact_events
    where signers[0] in (
        '3xKcZ5rhUTsHqQyZmvZePHrc3ZcAZ7vfWFn7FuGr2crf'
        , '8w3FgNEbAKvetyMx7qfsVqH6kEjiyCctjkfPcN54Rb1B'
        , '3FyHp9x8Hv8SzRqD8QkBh2sPeeXKykZo4VyBHhYSvkrT'
        , '7eU6sP7r635Ji3CkV8eZawBNuaESRm7tDkS82BTBdkci'
        , '95CvR67DtLiauSAqXhQJtVAZpNuSFjjeGiPAPZiC1TC1'
        , '2yK2ACpyRpoJDceNhHD7MnsQduzNbVsm71eqtg8f3T42'
        , '6XzW29uWXkUgyajE4g29E5dQgbs9MaZ9gBvkW2yxg72Q'
        , 'BBZ6ZPWuVHVf8uQdgRwWMmmbd5SpqUk2AAzZTMK7vcc3'
        , 'CoePTwkRYV5HXN2KkKrBFpNsVVk7GyQMr8RBnFVTMfu6'
        , '9NsE9x6eNAhucbawqRuNEBGnrhPw2tJnqAUQUEsoYc2B'
        , '9gGNgCDC6jwdiNBHEQG6i6PVGnMU6UN6DXnAU85tgRU8'
        , 'AfyeZ94zZQZiv29MN7MpBiSNqvjLagsRDeeuJ7LvDb5N'
        , 'JCe2HqZ3RgHa1oB9quw3YkdnitTKgVonXGpaSqTxfeJd'
    )
    group by 1, 2
), t1 as (
    select t.tx_id
    , t.block_timestamp
    , t.block_timestamp::date as date
    , t.mint
    , t.amount
    , t.tx_from
    , t.tx_to
    , case when tx_to in (
        '3xKcZ5rhUTsHqQyZmvZePHrc3ZcAZ7vfWFn7FuGr2crf'
        , '8w3FgNEbAKvetyMx7qfsVqH6kEjiyCctjkfPcN54Rb1B'
        , '3FyHp9x8Hv8SzRqD8QkBh2sPeeXKykZo4VyBHhYSvkrT'
        , '7eU6sP7r635Ji3CkV8eZawBNuaESRm7tDkS82BTBdkci'
        , '95CvR67DtLiauSAqXhQJtVAZpNuSFjjeGiPAPZiC1TC1'
        , '2yK2ACpyRpoJDceNhHD7MnsQduzNbVsm71eqtg8f3T42'
        , '6XzW29uWXkUgyajE4g29E5dQgbs9MaZ9gBvkW2yxg72Q'
        , 'BBZ6ZPWuVHVf8uQdgRwWMmmbd5SpqUk2AAzZTMK7vcc3'
        , 'CoePTwkRYV5HXN2KkKrBFpNsVVk7GyQMr8RBnFVTMfu6'
        , '9NsE9x6eNAhucbawqRuNEBGnrhPw2tJnqAUQUEsoYc2B'
        , '9gGNgCDC6jwdiNBHEQG6i6PVGnMU6UN6DXnAU85tgRU8'
        , 'AfyeZ94zZQZiv29MN7MpBiSNqvjLagsRDeeuJ7LvDb5N'
        , 'JCe2HqZ3RgHa1oB9quw3YkdnitTKgVonXGpaSqTxfeJd'
    ) and tx_from in (
        '3xKcZ5rhUTsHqQyZmvZePHrc3ZcAZ7vfWFn7FuGr2crf'
        , '8w3FgNEbAKvetyMx7qfsVqH6kEjiyCctjkfPcN54Rb1B'
        , '3FyHp9x8Hv8SzRqD8QkBh2sPeeXKykZo4VyBHhYSvkrT'
        , '7eU6sP7r635Ji3CkV8eZawBNuaESRm7tDkS82BTBdkci'
        , '95CvR67DtLiauSAqXhQJtVAZpNuSFjjeGiPAPZiC1TC1'
        , '2yK2ACpyRpoJDceNhHD7MnsQduzNbVsm71eqtg8f3T42'
        , '6XzW29uWXkUgyajE4g29E5dQgbs9MaZ9gBvkW2yxg72Q'
        , 'BBZ6ZPWuVHVf8uQdgRwWMmmbd5SpqUk2AAzZTMK7vcc3'
        , 'CoePTwkRYV5HXN2KkKrBFpNsVVk7GyQMr8RBnFVTMfu6'
        , '9NsE9x6eNAhucbawqRuNEBGnrhPw2tJnqAUQUEsoYc2B'
        , '9gGNgCDC6jwdiNBHEQG6i6PVGnMU6UN6DXnAU85tgRU8'
        , 'AfyeZ94zZQZiv29MN7MpBiSNqvjLagsRDeeuJ7LvDb5N'
        , 'JCe2HqZ3RgHa1oB9quw3YkdnitTKgVonXGpaSqTxfeJd'
    ) then 'inter'
    when tx_to in (
        '3xKcZ5rhUTsHqQyZmvZePHrc3ZcAZ7vfWFn7FuGr2crf'
        , '8w3FgNEbAKvetyMx7qfsVqH6kEjiyCctjkfPcN54Rb1B'
        , '3FyHp9x8Hv8SzRqD8QkBh2sPeeXKykZo4VyBHhYSvkrT'
        , '7eU6sP7r635Ji3CkV8eZawBNuaESRm7tDkS82BTBdkci'
        , '95CvR67DtLiauSAqXhQJtVAZpNuSFjjeGiPAPZiC1TC1'
        , '2yK2ACpyRpoJDceNhHD7MnsQduzNbVsm71eqtg8f3T42'
        , '6XzW29uWXkUgyajE4g29E5dQgbs9MaZ9gBvkW2yxg72Q'
        , 'BBZ6ZPWuVHVf8uQdgRwWMmmbd5SpqUk2AAzZTMK7vcc3'
        , 'CoePTwkRYV5HXN2KkKrBFpNsVVk7GyQMr8RBnFVTMfu6'
        , '9NsE9x6eNAhucbawqRuNEBGnrhPw2tJnqAUQUEsoYc2B'
        , '9gGNgCDC6jwdiNBHEQG6i6PVGnMU6UN6DXnAU85tgRU8'
        , 'AfyeZ94zZQZiv29MN7MpBiSNqvjLagsRDeeuJ7LvDb5N'
        , 'JCe2HqZ3RgHa1oB9quw3YkdnitTKgVonXGpaSqTxfeJd'
    ) then 'to' else 'from' end as direction
    , case when direction = 'inter' then 0 when direction = 'to' then amount else -amount end as net_amount
    from t0
    join solana.core.fact_transfers t
        on t.block_timestamp = t0.block_timestamp
        and t.tx_id = t0.tx_id
)
select * from t1


select *
from solana.core.fact_events
where program_id = 'GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY'
limit 10



select program_id
, min(block_timestamp) as start_timestamp
, count(distinct tx_id) as n_tx
from solana.core.fact_events i
where block_timestamp >= current_date - 10
    and program_id in ('GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY','voTpe3tHQ7AjQHMapgSue2HJFAh2cGsdokqN3XqmVSj')
    and succeeded
group by 1

select *
from solana.core.fact_transfers
where (
    tx_from = '41Y3beTKaCc5DACUhFVDtH9fLFENN4Xq3TM5nPEeVp8p'
    or tx_to = '41Y3beTKaCc5DACUhFVDtH9fLFENN4Xq3TM5nPEeVp8p'
)

with t0 as (
    select distinct tx_id
    , block_timestamp
    from solana.core.fact_events
    where block_timestamp >= '2022-08-01'
        and block_timestamp <= '2022-12-01'
        and signers[0] = '8w3FgNEbAKvetyMx7qfsVqH6kEjiyCctjkfPcN54Rb1B'
        and program_id = 'GrcZwT9hSByY1QrUTaRPp6zs5KxAA5QYuqEhjT1wihbm'
        and succeeded
)
select t.*
, case when tx_to = '8w3FgNEbAKvetyMx7qfsVqH6kEjiyCctjkfPcN54Rb1B' then amount
    when tx_from = '8w3FgNEbAKvetyMx7qfsVqH6kEjiyCctjkfPcN54Rb1B' then -amount
    else 0 end as net_amount
from t0
join solana.core.fact_transfers t
    on t.block_timestamp = t0.block_timestamp
    and t.tx_id = t0.tx_id
where t.block_timestamp >= '2022-08-01'
    and t.block_timestamp <= '2022-12-01'
    and amount > 0



select *
from solana.defi.fact_swaps
where swapper in (
	'3xKcZ5rhUTsHqQyZmvZePHrc3ZcAZ7vfWFn7FuGr2crf'
	, '8w3FgNEbAKvetyMx7qfsVqH6kEjiyCctjkfPcN54Rb1B'
	, '3FyHp9x8Hv8SzRqD8QkBh2sPeeXKykZo4VyBHhYSvkrT'
	, '7eU6sP7r635Ji3CkV8eZawBNuaESRm7tDkS82BTBdkci'
	, '95CvR67DtLiauSAqXhQJtVAZpNuSFjjeGiPAPZiC1TC1'
	, '2yK2ACpyRpoJDceNhHD7MnsQduzNbVsm71eqtg8f3T42'
	, '6XzW29uWXkUgyajE4g29E5dQgbs9MaZ9gBvkW2yxg72Q'
	, 'BBZ6ZPWuVHVf8uQdgRwWMmmbd5SpqUk2AAzZTMK7vcc3'
	, 'CoePTwkRYV5HXN2KkKrBFpNsVVk7GyQMr8RBnFVTMfu6'
	, '9NsE9x6eNAhucbawqRuNEBGnrhPw2tJnqAUQUEsoYc2B'
	, '9gGNgCDC6jwdiNBHEQG6i6PVGnMU6UN6DXnAU85tgRU8'
	, 'AfyeZ94zZQZiv29MN7MpBiSNqvjLagsRDeeuJ7LvDb5N'
	, 'JCe2HqZ3RgHa1oB9quw3YkdnitTKgVonXGpaSqTxfeJd'
)


select instructions[0]:parsed:info:source::string as source
, instructions[0]:parsed:info:destination::string as destination
, ARRAY_SIZE(inner_instructions)
, ARRAY_SIZE(inner_instructions) = 0
, ARRAY_SIZE(instructions) = 1
, instructions[0]:parsed:info:source
, instructions[0]:parsed:info:destination
, left(instructions[0]:parsed:info:source, 5)
, left(instructions[0]:parsed:info:destination, 5)
, succeeded
from solana.core.fact_transactions t
where t.block_timestamp::date = '2024-01-29'::date
    and tx_id = '4tgJxzNMNMDNk23K3kmkGKpnxh19UVii63GZ46CuJe2Sx448CxePXgUfAHfqdTUzkvQstnuQSY7Kt4ffZDAGHd52'


with t0 as (
    select e.program_id
    , e.tx_id
    , e.block_timestamp
    , max(case when i.tx_id is null then 0 else 1 end) as is_decoded
    from solana.core.fact_events e
    left join solana.core.fact_decoded_instructions i
        on i.block_timestamp = e.block_timestamp
        and i.tx_id = e.tx_id
        and i.program_id in (
            'voTpe3tHQ7AjQHMapgSue2HJFAh2cGsdokqN3XqmVSj'
            , 'GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY'
        )
    where e.block_timestamp >= '2024-02-01'
        and e.block_timestamp < dateadd('hours', -3, current_timestamp)
        and e.program_id in (
            'voTpe3tHQ7AjQHMapgSue2HJFAh2cGsdokqN3XqmVSj'
            , 'GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY'
        )
    group by 1, 2, 3
)
select block_timestamp::date as date
, program_id
, count(1) as n_tx
, avg(is_decoded) as pct_decoded
from t0
group by 1, 2
order by 2, 1


with t0 as (
    select signers[0]::string as address
    , decoded_instruction:args:amount::int * pow(10, -6) as lock_amount
    from solana.core.fact_decoded_instructions i
    where block_timestamp >= '2024-02-01'
        and i.program_id ='voTpe3tHQ7AjQHMapgSue2HJFAh2cGsdokqN3XqmVSj'
        and event_type = 'increaseLockedAmount'
    group by 1
    order by 2 desc
)
select *
, sum(lock_amount) over () as tot_lock_amount
from t0


select decoded_instruction:args:side::string as side
, decoded_instruction:args:weight::int * power(10, -6) as weight
, value:pubkey::string as pubkey
, value:name::string as name
, *
from solana.core.fact_decoded_instructions i
, lateral flatten(input_data:accounts)
where block_timestamp >= current_date - 1
    and i.program_id in (
        'voTpe3tHQ7AjQHMapgSue2HJFAh2cGsdokqN3XqmVSj'
        -- 'GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY'
    )
    and i.signers[0] = 'FSjicwUa1dmWRv8RSff52VjDb4rZDkegQp4nmyrxqJan'
limit 100


with t0 as (
    select decoded_instruction:args:side::string as side
    , decoded_instruction:args:weight::int * power(10, -6) as weight
    , signers[0]::string as voter
    , value:pubkey::string as proposal
    , value:name::string as name
    from solana.core.fact_decoded_instructions i
    , lateral flatten(input => decoded_instruction:accounts)
    where block_timestamp >= current_date - 1
        and i.program_id in (
            'GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY'
        )
        and name = 'proposal'
        and proposal = '6txWyf3guJrhnNJXcAHxnV2oVxBcvebuSbfYsgB3yUKc'
        qualify(
            row_number() over (partition by voter order by block_timestamp desc) = 1
        )
)
select proposal
, side
, case when side = '1' then 'Sharky'
    when side = '2' then 'Uprock'
    when side = '3' then 'Banx'
    when side = '4' then 'Zeus Network'
    when side = '5' then 'Monkey Dex'
    when side = '6' then 'Srcful'
    else 'Other' end as protocol
, sum(weight) as votes
, count(1) as n_voters
from t0
group by 1, 2, 3
order by votes desc



select program_id
, max(block_timestamp)
from solana.core.fact_decoded_instructions i
    where i.block_timestamp = current_date - 1
group by 1
order by 2 desc




select
coalesce(m.user_id, d.created_by_id) as user_id,
d.profile_id as profile_id,
coalesce(tu.username, u.username) as username,
d.id as dashboard_id,
d.title,
row_number() over (
    order by
    dr.ranking_trending
) as current_rank
from
bi_analytics.content_rankings.dashboard_rankings dr
join bi_analytics.velocity_app_prod.dashboards d on d.id = dr.dashboard_id
left join bi_analytics.velocity_app_prod.profiles p
    on p.id = d.profile_id
left join bi_analytics.velocity_app_prod.teams t
    on t.profile_id = p.id
left join bi_analytics.velocity_app_prod.members m
    on t.id = m.team_id
    and m.role = 'owner'
left join bi_analytics.velocity_app_prod.users tu
    -- kellen changed this line
    on tu.id = m.user_id
join bi_analytics.velocity_app_prod.users u on u.id = d.created_by_id
order by current_rank

select *
from bi_analytics.twitter.tweet
where conversation_id = '1766022851986182437'



select d.id as dashboard_id
, t.id as tweet_id
, t.conversation_id
, d.title
, d.latest_slug
, coalesce(tm.slug, u.username) as username
, u.id as user_id
, t.impression_count
, case when u.user_name is null then t.tweet_url else CONCAT('https://twitter.com/',u.user_name,'/status/',t.id) end as tweet_url
, t.created_at::date as tweet_date
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.twitter.tweet t
    on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
where t.conversation_id = '1766022851986182437'


select *
from bi_analytics.velocity_app_prod.users
where username = 'marqu'

select d.title as dash_title
, d.created_at::date as date
, d.latest_slug as slug
, d.*
from bi_analytics.velocity_app_prod.dashboards
where created_by_id = 'be58bfd3-ea79-42c1-8daa-7af18cde0676'
order by d.created_at desc
limit 50


select *
from bi_analytics.velocity_app_prod.dashboards
where id = 'fce42520-3950-4c29-967e-9ea11636ad31'




WITH labels as (
    select c.value:dashboard_id::string as dashboard_id
    , c.value:tag::string as dashboard_tag
    from crosschain.bronze.data_science_uploads
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'dashboard-tags%'
),  t0 as (
    select d.id as dashboard_id
    , t.id as tweet_id
    , t.conversation_id
    , d.title
    , d.latest_slug
    , coalesce(tm.slug, u.username) as username
    , u.id as user_id
    , t.impression_count
    , case when u.user_name is null then t.tweet_url else CONCAT('https://twitter.com/',u.user_name,'/status/',t.id) end as tweet_url
    , t.created_at::date as tweet_date
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    left join bi_analytics.twitter.user tu
        on tu.id = t.user_id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    left join bi_analytics.velocity_app_prod.profiles p
        on p.id = d.profile_id
    left join bi_analytics.velocity_app_prod.teams tm
        on tm.profile_id = p.id
    where not d.id in (
        select dashboard_id from labels where dashboard_tag = 'bot'
    )
    qualify(
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
)
select * from t0
where conversation_id = '1766138851041435712'
, t0d as (
    select DISTINCT dashboard_id
    from t0
), t1 as (
    select d.id as dashboard_id
    , case when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
        or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
        or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
        or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%'
        then 'Axelar' else INITCAP(t.name) end as chain
    , d.title
    , d.latest_slug
    , u.username
    , u.id as user_id
    , COUNT(DISTINCT q.id) as n_queries
    from bi_analytics.velocity_app_prod.dashboards d
    join t0d
        on t0d.dashboard_id = d.id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    -- join bi_analytics.velocity_app_prod._dashboards_to_queries dtq
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    -- join bi_analytics.velocity_app_prod._queries_to_tags qtt
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
        on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and t.type = 'project'
    group by 1, 2, 3, 4, 5, 6
), t2 as (
    select *
    , row_number() over (
        partition by dashboard_id
        order by
        n_queries desc
        , case when chain in (
            'Avalanche'
            , 'Axelar'
            , 'Flow'
            , 'Near'
            , 'Osmosis'
            , 'Sei'
            , 'Solana'
            , 'Thorchain'
        ) then 1 when chain = 'Ethereum' then 2 else 3 end
        , chain
    ) as rn
    , SUM(n_queries) over (partition by dashboard_id) as tot_queries
    , n_queries / tot_queries as pct
    from t1
), tc0 as (
    select t2.user_id
    , t2.username
    , t2.chain
    , SUM(pct) as tot_pct
    from t2
    join t0d
        on t0d.dashboard_id = t2.dashboard_id
    group by 1, 2, 3
), tc1 as (
    select *
    , row_number() over (
        partition by user_id
        order by
        tot_pct desc
        , case when chain in (
            'Avalanche'
            , 'Axelar'
            , 'Flow'
            , 'Near'
            , 'Osmosis'
            , 'Sei'
            , 'Solana'
            , 'Thorchain'
        ) then 1 when chain = 'Ethereum' then 2 else 3 end
        , chain
    ) as rn
    , SUM(tot_pct) over (partition by user_id) as tot_pct_2
    , tot_pct / tot_pct_2 as pct
    from tc0
), tc as (
    select tc1a.user_id
    , tc1a.username
    , CONCAT(
        tc1a.chain
        , case when tc1b.chain is null then '' else CONCAT(' + ', tc1b.chain) end
        , case when tc1c.chain is null then '' else CONCAT(' + ', tc1c.chain) end
    ) as user_chain
    from tc1 tc1a
    left join tc1 tc1b
        on tc1b.user_id = tc1a.user_id
        and tc1b.rn = 2
        and tc1b.pct > 0.25
    left join tc1 tc1c
        on tc1c.user_id = tc1a.user_id
        and tc1c.rn = 3
        and tc1c.pct > 0.25
    where tc1a.rn = 1
), t3 as (
    select t0.tweet_id
    , t0.conversation_id
    , t0.impression_count
    , t0.tweet_url
    , t0.tweet_date
    , t0.title
    , t0.latest_slug
    , t0.user_id
    , t0.username
    , COALESCE(tc.user_chain, 'Ethereum') as user_chain
    , CONCAT(
        case when t2a.chain is null then '' else t2a.chain end
        -- , ''
        , case when t2b.chain is null then '' else CONCAT(' + ', t2b.chain) end
        , case when t2c.chain is null then '' else CONCAT(' + ', t2c.chain) end
    ) as chain
    , case 
        when (
            (tweet_date >= '2023-08-27' and tweet_date <= '2023-09-04') 
        ) and t2a.user_id in ('",paste0(fpl_users$user_id, collapse="','"),"')
        then 'FPL S2' 
        when (
            (tweet_date >= '2023-09-10' and tweet_date <= '2023-09-18') 
            or (tweet_date >= '2023-09-24' and tweet_date <= '2023-10-02') 
            or (tweet_date >= '2023-10-08' and tweet_date <= '2023-10-16')
            or (tweet_date >= '2023-09-10' and tweet_date <= '2023-10-16')
        ) and t2a.user_id in ('",paste0(fpl_users[qualified == 1]$user_id, collapse="','"),"')
        then 'FPL S2'
        when (
            (tweet_date >= '2023-06-11' and tweet_date <= '2023-06-19') 
        ) and t2a.user_id in ('",paste0(fpl_users_s1$user_id, collapse="','"),"')
        then 'FPL S1'
        when (
            (tweet_date >= '2023-06-18' and tweet_date <= '2023-07-10')
        ) and t2a.user_id in ('",paste0(fpl_users_s1[qualified == 1]$user_id, collapse="','"),"')
        then 'FPL S1' else 'Other' end as segment
    from t0
    left join t2 t2a
        on t2a.dashboard_id = t0.dashboard_id
        and t2a.rn = 1
    left join tc
        on tc.user_id = t2a.user_id
    left join t2 t2b
        on t2b.dashboard_id = t0.dashboard_id
        and t2b.rn = 2
        and t2b.pct > 0.25
    left join t2 t2c
        on t2c.dashboard_id = t0.dashboard_id
        and t2c.rn = 3
        and t2c.pct > 0.25
), t4 as (
    select tweet_id as conversation_id
    , COUNT(DISTINCT user_id) as n_likes
    from bi_analytics.twitter.likes
    group by 1
)
select t3.*
, DATEDIFF('days', tweet_date, CURRENT_DATE) as days_ago
, COALESCE(t4.n_likes, 0) as n_likes
from t3
left join t4
    on t4.conversation_id = t3.conversation_id


select *
from solana.core.fact_transfers
where tx_from = 'hjMcayizkqnAQ3mmYWJJYVSjyCzcJmcwwkq3knFUYCh'
    or tx_to = 'hjMcayizkqnAQ3mmYWJJYVSjyCzcJmcwwkq3knFUYCh'

with t0 as (
    select distinct program_id
    , tx_id
    , block_timestamp
    , signers[0]::string as signer
    from solana.core.fact_events
    where (
            signers[0] in ()
        )
), t1 as (
    select t.tx_id
    , t.block_timestamp
    , t.tx_from
    , t.tx_to
    , t.mint
    , t.amount
    , t0.program_id
    , t0.signer
    from t0
    join solana.core.fact_transfers t
        on t.block_timestamp = t0.block_timestamp
        and t.tx_id = t0.tx_id
)
select *
from t1

select *
from solana.defi.fact_swaps
where swapper = '9gGNgCDC6jwdiNBHEQG6i6PVGnMU6UN6DXnAU85tgRU8'

select * from solana.core.dim_labels
where address in (
    'AYx2AJhqNKXfaZeqUaSWgptZWCXWCf9Z8N4vKZFDs97Q'
    -- , '41Y3beTKaCc5DACUhFVDtH9fLFENN4Xq3TM5nPEeVp8p'
)

with labels as (
    select c.value:dashboard_id::string as dashboard_id
    , c.value:tag::string as dashboard_tag
    from crosschain.bronze.data_science_uploads
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'dashboard-tags%'
), t0 as (
    select u.username
    , coalesce(up.url, '') as url
    , sum(t.impression_count) as impression_count
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    left join bi_analytics.velocity_app_prod.uploads up
        on up.id = u.avatar_id
    where not d.id in (
        select dashboard_id from labels where dashboard_tag = 'bot'
    )
    and t.created_at >= current_date - 30
    qualify(
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
)
select *
from t0


select *
from bi_analytics.velocity_app_prod.users u
where username = 'kellen'
limit 10



with pc0 as (
    select token_address
    , recorded_hour
    , close
    , lag(close, 1) over (
        partition by token_address
        order by recorded_hour
    ) as prv_price
    , close / prv_price as ratio
    from solana.price.ez_token_prices_hourly p
    where recorded_hour >= '2022-12-25'
        and is_imputed = false
), pc1 as (
    select recorded_hour::date as date
    , token_address
    from pc0
    where ratio >= 10
    or ratio <= 0.1
), p0 as (
    select p.token_address as mint
    , DATE_TRUNC('hour', p.recorded_hour) as hour
    , avg(close) as price
    , MIN(close) as min_price
    from solana.price.ez_token_prices_hourly p
    left join pc1
        on pc1.token_address = p.token_address
        and pc1.date = p.recorded_hour::date
    where recorded_hour >= '2022-12-25'
        and pc1.date is null
        and is_imputed = FALSE
        and close < 1000000
    group by 1, 2
), p1 as (
    select p.token_address as mint
    , DATE_TRUNC('day', recorded_hour) as date
    , avg(close) as price
    , MIN(close) as min_price
    from solana.price.ez_token_prices_hourly p
    left join pc1
        on pc1.token_address = p.token_address
        and pc1.date = p.recorded_hour::date
    where recorded_hour >= '2022-12-25'
        and pc1.date is null
        and is_imputed = FALSE
        and close < 1000000
    group by 1, 2
), p2 as (
    select p.token_address as mint
    , DATE_TRUNC('week', recorded_hour) as week
    , avg(close) as price
    , MIN(close) as min_price
    from solana.price.ez_token_prices_hourly p
    left join pc1
        on pc1.token_address = p.token_address
        and pc1.date = p.recorded_hour::date
    where recorded_hour >= '2022-12-25'
        and pc1.date is null
        and is_imputed = FALSE
        and close < 1000000
    group by 1, 2
), t0 as (
    SELECT s.block_timestamp::date as date
    , swapper
    , 'From' as direction
    , swap_from_mint as mint
    -- , COALESCE(tok.symbol, swap_from_mint) as symbol
    , sum(swap_from_amount) as amount
    from solana.defi.fact_swaps s
    -- left join solana.core.dim_tokens tok
    --     on tok.token_address = s.swap_from_mint
    -- left join p0 pf
    --     on pf.token_address = s.swap_from_mint
    --     and pf.date = s.block_timestamp::date
    -- left join p0 pt
    --     on pt.token_address = s.swap_to_mint
    --     and pt.date = s.block_timestamp::date
    WHERE s.block_timestamp >= '2022-12-25'
        and s.succeeded
    group by 1, 2, 3, 4
    UNION
    SELECT s.block_timestamp::date as date
    -- swapper, to, from, amount, amount usd
    , swapper
    , 'To' as direction
    , swap_to_mint as mint
    -- , COALESCE(tok.symbol, swap_to_mint) as symbol
    , sum(swap_to_amount) as amount
    -- , LEAST(swap_to_amount * COALESCE(pt.token_price, 0), swap_from_amount * COALESCE(pf.token_price, 0)) as amount_usd
    from solana.defi.fact_swaps s
    -- left join solana.core.dim_tokens tok
    --     on tok.token_address = s.swap_to_mint
    -- left join p0 pf
    --     on pf.token_address = s.swap_from_mint
    --     and pf.date = s.block_timestamp::date
    -- left join p0 pt
    --     on pt.token_address = s.swap_to_mint
    --     and pt.date = s.block_timestamp::date
    group by 1, 2, 3, 4
    WHERE s.block_timestamp >= '2022-12-25'
        and s.succeeded
), t0a as (
    select t0.*
    , COALESCE(tok.symbol, t0.mint) as symbol
    , amount * COALESCE(p1.price, p2.price, 0) as amount_usd
    from t0
    left join solana.core.dim_tokens tok
        on tok.token_address = t0.mint
    left join p1
        on p1.date = DATE_TRUNC('day', t0.date)
        and p1.mint = t0.mint
    left join p2
        on p2.week = date_trunc('week', t0.date)
        and p2.mint = t0.mint
), t1 as (
    SELECT t0a.mint
    , UPPER(SPLIT(t0a.symbol, '-')[0]) as symbol
    , COUNT(DISTINCT swapper) as n_unique_swappers
    , SUM(t0a.amount_usd) as tot_amount_usd
    , SUM(t0a.amount_usd) / SUM(t0a.amount) as avg_price
    from t0a
    WHERE amount > 0
      and date >= '2022-12-25'
    group by 1, 2
), t2 as (
    SELECT *
    , case when mint in (
        'So11111111111111111111111111111111111111112'
        , 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
        , '7i5KKsX2weiTkry7jA4ZwSuXGhs5eJBEjY8vVxR4pfRx'
        , 'EchesyfXePKdLtoiZSL8pBe8Myagyy8ZRqsACNCFGnvp'
        , 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
        , '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R'
        , 'SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt'
        , 'AFbX8oGjGpmVFywbVouvhQSRmiW2aR1mohfahi4Y2AdB'
        , 'MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey'
        , 'ETAtLmCmsoiEEKfNrHKJ2kYy3MoABhU6NQvpSfij5tDs'
        , 'orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE'
    ) then 'Yes' else 'No' end as is_on_cb_or_bin
    , case when mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' then 'BONK' else 'Other Tokens' end as color
    , row_number() over (order by tot_amount_usd desc) as rk
    from t1
    
)
SELECT *
, CONCAT(LPAD(to_varchar(rk), 2, '0'), '. ', symbol) as label
from t2
WHERE rk <= 10




WITH t0 as (
    SELECT distinct swapper
    , swap_to_mint as mint
    from solana.defi.fact_swaps s
    WHERE s.block_timestamp >= '2022-12-25'
        and s.succeeded
    union
    SELECT distinct swapper
    , swap_from_mint as mint
    from solana.defi.fact_swaps s
    WHERE s.block_timestamp >= '2022-12-25'
        and s.succeeded
), t1 as (
    select mint
    , count(distinct swapper) as n_swappers
    from t0
    group by 1
), t2 as (
    select distinct swapper as tot_swappers
    from t0
), t3 as (
    select t1.*
    , t2.tot_swappers
    , COALESCE(tok.symbol, t1.mint) as symbol
    from t1
    left join solana.core.dim_tokens tok
        on tok.token_address = t1.mint
    join t2 on true
), t4 as (
    SELECT t2.mint
    , UPPER(symbol) as symbol
    , n_swappers
    , ROUND(100 * n_swappers / tot_swappers, 2) as pct_swappers_have_swapped_with_token
    , row_number() over (order by n_swappers desc) as rk
    from t3
)
SELECT *
, CONCAT(LPAD(to_varchar(rk), 2, '0'), '. ', symbol) as label
, case when symbol = 'BONK' then 'BONK' else 'Other Tokens' end as color
from t4
WHERE rk <= 10


select *
from bi_analytics.gumby.pages
where context_campaign_source is not null
order by timestamp desc
limit 1000



WITH labels as (
    select c.value:dashboard_id::string as dashboard_id
    , c.value:tag::string as dashboard_tag
    from crosschain.bronze.data_science_uploads
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'dashboard-tags%'
),  t0 as (
    select d.id as dashboard_id
    , t.id as tweet_id
    , t.conversation_id
    , d.title
    , d.latest_slug
    , coalesce(tm.slug, u.username) as username
    , u.id as user_id
    , t.impression_count
    , case when u.user_name is null then t.tweet_url else CONCAT('https://twitter.com/',u.user_name,'/status/',t.id) end as tweet_url
    , t.created_at::date as tweet_date
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    left join bi_analytics.twitter.user tu
        on tu.id = t.user_id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    left join bi_analytics.velocity_app_prod.profiles p
        on p.id = d.profile_id
    left join bi_analytics.velocity_app_prod.teams tm
        on tm.profile_id = p.id
    where not d.id in (
        select dashboard_id from labels where dashboard_tag = 'bot'
    )
    qualify(
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
)
select date_trunc('month', tweet_date) as month
, case when impression_count < 2000 then 'A. <2k'
    when impression_count < 5000 then 'B. 2-10k'
    -- when impression_count < 10000 then 'C. 5-10k'
    -- when impression_count < 25000 then 'D. 10-25k'
    else 'C: 10k+' end as grp
, count(1) as n_tweets
, avg(impression_count) as avg_impression_count
, median(impression_count) as med_impression_count
, sum(impression_count) as impression_count
from t0
group by 1, 2



select slug_id
from bi_analytics.velocity_app_prod.queries
where name = 'total amount and unique senders'

select q.id as query_id
, split(split(p.context_page_tab_url, '/q/')[1], '/')[0]::string as page_slug
, p.anonymous_id
, q.name
, coalesce(p.user_id, p.anonymous_id) as user
, DATEDIFF('hours', p.timestamp, current_timestamp) as hours_ago
, (case when hours_ago > 24 * 2 then 0 else 1 end) * POWER(0.95, hours_ago) as wt_q_views_0
, (case when hours_ago > 24 * 14 then 0 else 1 end) * POWER(0.993, hours_ago) as wt_q_views_1
, (case when hours_ago > 24 * 30 then 0 else 1 end) * POWER(0.9985, hours_ago) as wt_q_views_2
, 1 as wt_q_views_3
, row_number() over (partition by user order by wt_q_views_0 desc) as rn_0
, row_number() over (partition by user order by wt_q_views_1 desc) as rn_1
, row_number() over (partition by user order by wt_q_views_2 desc) as rn_2
, row_number() over (partition by user order by wt_q_views_3 desc) as rn_3
from bi_analytics.gumby.pages p
join bi_analytics.velocity_app_prod.queries q
    on q.slug_id = page_slug
where timestamp >= current_date - 30
    and context_page_tab_url like '%/q/%'


select *
from solana.defi.fact_swaps
where block_timestamp >= '2024-01-01'
    and (
        swap_from_mint = 'CJYcKVEmQhohCpYSgJzYipitxTazkNvmThws7jPcfkwU'
        or swap_from_mint = 'CJYcKVEmQhohCpYSgJzYipitxTazkNvmThws7jPcfkwU'
    )


select tx_from
, sum(amount) as amount
, max(tx_id) as tx_id
from solana.core.fact_transfers
where block_timestamp >= current_date - 90
    and tx_to = 'rRxnTT9B164ohUhzNuLgEBzcp4Pkz72pX7etoQDzEfY'
    and mint = 'So11111111111111111111111111111111111111112'
    and amount > 0
group by 1
order by 2 desc

select *
from solana.core.fact_transfers
where block_timestamp >= '2023-11-01'
    and block_timestamp <= '2023-12-31'
    and mint = '7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr'
order by block_timestamp
limit 10




select *
from solana.defi.fact_swaps
where block_timestamp >= '2023-12-11'
    and (
        swap_from_mint = '7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr'
        or swap_to_mint = '7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr'
    )
    and succeeded
    and swapper in (
        'Gda3vwfRN62ambyyvRN7uSwpUVPzsAVJy7Zk5ue3BuuD'
        , '5qXCd6ug2wVwRkdVXYG9MogXbcE9fvY8zYpqbRFrog4W'
        , 'BVFckp45wpEZQt5wA8KiCcHDXPcZNa8kn5gvMEsNes3F'
        , '3Zq8ZdsXtHmG1prmbiYHQ7a9uc6CH9hLeptuKedyz4hj'
        , 'HCdVej9nG2pAj7VK9KDyPdTHYPsNPUtVpHJktfY3TvS7'
        , 'AqtFk4nyx351EeCwWausFTjhFAZfFeLEjefFvQm4E7WW'
        , 'Fn5Lp8XTzmyEmsbWYDDvrcMuEhssme93Wh5pnLBZCRoY'
        , '77D64C2WCYqZthWn1TMHhU8DK5gNr2EytpmZC76f7ksC'
        , 'FfVcPTaFQmhisCQtZrgjBmea3mbiwQGJySwj7fns17ov'
        , 'CXWFSQPCvJzr3mjk2frqkZiHHu5LceRkKnS13ACqAw1K'
        , '31Pv4HBN61PjNoXntqEbujybU86WfTXCrU9fVRP5imdd'
        , 'EAs5zb3jX3yJb17uvFDvCPuC15CnyndUA9M6t7iibkLU'
    )

select *
from bi_analytics.velocity_app_prod.tags
where name ilike '%vertex%'


select u.id
, u.username
, count(distinct q.id) as n_queries
from bi_analytics.velocity_app_prod.users u
join bi_analytics.velocity_app_prod.queries q
    on q.created_by_id = u.id
where q.created_at >= current_date - 30
group by 1, 2
order by 3 desc




with labels as (
    select c.value:dashboard_id::string as dashboard_id
    , c.value:tag::string as dashboard_tag
    from crosschain.bronze.data_science_uploads
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'dashboard-tags%'
), t0 as (
    select c.value:user_id::string as user_id
    , c.value:mentee_user_id::string as mentee_user_id
    , c.value:status::string as status
    , c.value:ecosystem::string as ecosystem
    , c.value:currency::string as currency
    , c.value:base_comp::int as base_comp
    , u.record_metadata:CreateTime::int as start_time
    , row_number() over (partition by user_id order by u.record_metadata:CreateTime::int desc) as rn
    from crosschain.bronze.data_science_uploads u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'ambassador%'
), t1 as (
    select u.username
    , t0.*
    from t0
    left join bi_analytics.velocity_app_prod.users u
        on u.id = t0.user_id
    where rn = 1
), imp0 as (
    select d.id as dashboard_id
    , d.created_by_id as user_id
    , impression_count
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    where not d.id in (
        select dashboard_id from labels where dashboard_tag = 'bot'
    )
    and t.created_at::date >= current_date - 8
        and t.created_at::date <= current_date - 1
    qualify(
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
), imp as (
    select user_id
    , sum(impression_count) as impression_count
    from imp0
    group by 1
), analyst as (
    select t1.user_id
    , t1.username
    , t1.base_comp
    , floor(least(250000, coalesce(i.impression_count, 0))/500) as incentive_comp
    , t1.base_comp + incentive_comp as amount_usd
    from t1
    left join imp i
        on i.user_id = t1.user_id
    where status = 'analyst'
), mentor as (
    select t1.user_id
    , t1.username
    , t1.base_comp
    , floor(least(250000, coalesce(i.impression_count, 0))/1000) as incentive_comp
    , t1.base_comp + incentive_comp as amount_usd
    from t1
    left join imp i
        on i.user_id = t1.mentee_user_id
    where status = 'mentor'
)
select *
from mentor
union select *
from analyst





-- Vertex (March 14 - 17)
-- JUP Boost (March 7 - 8)
-- Blast Boost (March 1 - 3)
-- Near Boost (Feb 24 - 27)

with t0 as (
    select d.id as dashboard_id
    , d.latest_slug
    from bi_analytics.velocity_app_prod.queries q
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
        on dtq.B = q.id
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = dtq.A
    where q.statement like '%VoTpe3tHQ7AjQHMapgSue2HJFAh2cGsdokqN3XqmVSj%'
        or q.statement like '%GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY%'

), t1 as (
    select d.dashboard_id
    , impression_count
    , t.created_at::date as tweet_date
    from t0 d
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    and t.created_at::date >= '2024-01-01'
    qualify(
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
)
select *
from t1


select date_trunc('month', q.created_at)::date as month
, count(distinct q.id) as queries_written
, count(distinct q.created_by_id) as n_users
from bi_analytics.velocity_app_prod.users u
join bi_analytics.velocity_app_prod.queries q
    on q.created_by_id = u.id
join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
    on q.id = qtt.A
join bi_analytics.velocity_app_prod.tags t
    on qtt.B = t.id
    and t.type = 'project'
    and t.name = 'Thorchain'
group by 1
order by 1 desc







greatest(0, least(1, 3 * ((COALESCE(avg_small_acct, 0) - 0.4)))) as avg_small_mult
least(1, COALESCE(tot_small_acct, 0) / 30) as tot_small_mult
LEAST(
    1
    , GREATEST(
        0
        , (1 - greatest(0, least(1, 3 * ((COALESCE(avg_small_acct, 0) - 0.4)))) * LEAST(1, COALESCE(tot_small_acct, 0) / 30))
    )
)

with rk_hist0 as (
    select d.id as dashboard_id
    , date_trunc('hour', dbt_updated_at) as hour
    , avg(hr.ranking_trending) as rk0
    from bi_analytics.snapshots.hourly_dashboard_rankings hr
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where dbt_updated_at >= '2024-04-10'
        and dbt_updated_at <= '2024-04-11'
        and coalesce(u.role, '') <> 'internal'
        and not u.username in (
            'Polaris_9R','dsaber','flipsidecrypto','metricsdao','drethereum','Orion_9R','sam','forgash','nftchance__','danner'
        )
        and hr.ranking_trending <= 100
    group by 1, 2
)
, rk_hist1 as (
    select *
    , row_number() over (partition by hour order by rk0) as rk
    from rk_hist0
)
, t0 as (
    select r.dashboard_id
    , r.hour
    , r.rk
    , case when r.rk <= 10 then 1.5 else 1 end as base_amount
    , l.title
    , l.username
    , l.project
    , b.name
    , b.start_hour::date as date
    , b.n_hours
    , b.mult
    , b.mult * base_amount as boost_amount
    from rk_hist1 r
    join labels l
        on l.dashboard_id = r.dashboard_id
    -- join boosts b
    --     on b.project = l.project
    --     and r.hour >= b.start_hour
    --     and r.hour < dateadd('hours', b.n_hours, b.start_hour)
    qualify(
        row_number() over (partition by r.dashboard_id, r.hour order by b.mult desc) = 1
    )
)
select * from t0 where username = 'Diamond'






, t0 as (
    select r.dashboard_id
    , r.hour
    , r.rk
    , l.title
    , l.username
    , l.project
    from rk_hist1 r
    join labels l
        on l.dashboard_id = r.dashboard_id
)
, t1 as (
    select *
    , case when rk <= 10 then 1.5 else 1.0 end as base
    , case
    when project = 'near' and hour >= '2024-02-23 19:00:00' and hour < '2024-02-26 19:00:00' and rk <= 30 then base * 2
    when project = 'blast' and hour >= '2024-05-01 16:00:00' and hour < '2024-03-04 16:00:00' and rk <= 40 then base * 2
    when project = 'Jupiter LFG Token Launchpad' and hour >= '2024-03-07 21:00:00' and hour < '2024-03-08 21:00:00' and rk <= 40 then base * 9
    when project = 'vertex' and hour >= '2024-03-14 16:00:00' and hour < '2024-03-17 16:00:00' and rk <= 40 then base * 2
    when project = 'near' and hour >= '2024-03-18 15:00:00' and hour < '2024-03-21 15:00:00' and rk <= 40 then base * 2
    when project = 'aptos' and hour >= '2024-03-19 22:00:00' and hour < '2024-03-20 22:00:00' and rk <= 40 then base * 2
    when project = 'near' and hour >= '2024-03-24 21:00:00' and hour < '2024-03-29 21:00:00' and rk <= 40 then base * 4
    else 0 end as additional_payment
    , case
    when project = 'near' and hour >= '2024-02-23 19:00:00' and hour < '2024-02-26 19:00:00' and rk <= 30 then 'Near Feb 23'
    when project = 'blast' and hour >= '2024-05-01 16:00:00' and hour < '2024-03-04 16:00:00' and rk <= 40 then 'Blast Mar 1'
    when project = 'Jupiter LFG Token Launchpad' and hour >= '2024-03-07 21:00:00' and hour < '2024-03-08 21:00:00' and rk <= 40 then 'Jup Mar 7'
    when project = 'vertex' and hour >= '2024-03-14 16:00:00' and hour < '2024-03-17 16:00:00' and rk <= 40 then 'Vertex Mar 14'
    when project = 'near' and hour >= '2024-03-18 15:00:00' and hour < '2024-03-21 15:00:00' and rk <= 40 then 'Near Mar 18'
    when project = 'aptos' and hour >= '2024-03-19 22:00:00' and hour < '2024-03-20 22:00:00' and rk <= 40 then 'Aptos Mar 19'
    when project = 'near' and hour >= '2024-03-24 21:00:00' and hour < '2024-03-29 21:00:00' and rk <= 40 then 'Near Mar 24'
    else 'None' end as boost
    from t0
), t2 as (
    select boost
    , title
    , username
    , sum(additional_payment) as additional_payment
    from t1
    where additional_payment > 0
    -- where project = 'Jupiter LFG Token Launchpad'
    group by 1, 2, 3
)
select *
from t2
order by 1

with accounts as (
    select twitter_id
    , twitter_handle
    , account_type
    , ecosystems[0]::string as ecosystem
    , n_followers
    from crosschain.bronze.twitter_accounts
    where not twitter_id in (
        '1314075720'
        , '925712018937712640'
        , '59119959'
        , '1445056111753584644'
        , '791390945656856577'
    )
    qualify(
        row_number() over (partition by twitter_id order by updated_at desc) = 1
    )
)
select t.conversation_id
, count(distinct tar.twitter_id) as n_retweets
, count(distinct taq.twitter_id) as n_quotes
, count(distinct tal.twitter_id) as n_likes
from bi_analytics.twitter.tweet t
left join bi_analytics.twitter.likes l
    on l.tweet_id = t.id
left join accounts tal
    on tal.twitter_id = l.user_id
left join bi_analytics.twitter.retweets r
    on r.tweet_id = t.id
left join accounts tar
    on tar.twitter_id = r.user_id
left join bi_analytics.twitter.quotes q
    on q.tweet_id = t.conversation_id
left join accounts taq
    on taq.twitter_id = q.user_id
where t.created_at >= current_date - 1
    and coalesce(taq.twitter_id, tar.twitter_id, tal.twitter_id) is not null
group by 1
order by 4 desc


with accounts as (
    select twitter_id
    , twitter_handle
    , account_type
    , ecosystems[0]::string as ecosystem
    , n_followers
    from crosschain.bronze.twitter_accounts
    where not twitter_id in (
        '1314075720'
        , '925712018937712640'
        , '59119959'
        , '1445056111753584644'
        , '791390945656856577'
    )
    qualify(
        row_number() over (partition by twitter_id order by updated_at desc) = 1
    )
)
select tal.twitter_id::string as twitter_id
, tal.twitter_handle
, count(1) as n
from bi_analytics.twitter.tweet t
join bi_analytics.twitter.likes l
    on l.tweet_id = t.id
join accounts tal
    on tal.twitter_id = l.user_id
where t.created_at >= current_date - 60
group by 1
order by 2 desc



with twitter_accounts as (
    select twitter_id
    , twitter_handle
    , account_type
    , ecosystems[0]::string as ecosystem
    , n_followers
    from crosschain.bronze.twitter_accounts
    where not twitter_id in (
        '1314075720'
        , '925712018937712640'
        , '59119959'
        , '1445056111753584644'
        , '791390945656856577'
    )
    qualify(
        row_number() over (partition by twitter_id order by updated_at desc) = 1
    )
), influencer_impact as (
    select t.conversation_id
    , coalesce(tar.twitter_id, taq.twitter_id, tal.twitter_id) as twitter_id
    , coalesce(tar.twitter_handle, taq.twitter_handle, tal.twitter_handle) as twitter_handle
    , coalesce(tar.account_type, taq.account_type, tal.account_type) as account_type
    , coalesce(tar.ecosystem, taq.ecosystem, tal.ecosystem) as ecosystem
    , max(case when tar.twitter_id is null then 0 else 1 end) as has_retweet
    , max(case when taq.twitter_id is null then 0 else 1 end) as has_quote
    , max(case when tal.twitter_id is null then 0 else 1 end) as has_like
    from bi_analytics.twitter.tweet t
    left join bi_analytics.twitter.likes l
        on l.tweet_id = t.conversation_id
    left join twitter_accounts tal
        on tal.twitter_id = l.user_id
    left join bi_analytics.twitter.retweets r
        on r.tweet_id = t.conversation_id
    left join twitter_accounts tar
        on tar.twitter_id = r.user_id
    left join bi_analytics.twitter.quotes q
        on q.tweet_id = t.conversation_id
    left join twitter_accounts taq
        on taq.twitter_id = q.user_id
    where t.created_at >= current_date - 1
        and coalesce(taq.twitter_id, tar.twitter_id, tal.twitter_id) is not null
    group by 1, 2, 3, 4, 5
)
select *
from influencer_impact

with twitter_accounts as (
    select twitter_id
    , twitter_handle
    , account_type
    , ecosystems[0]::string as ecosystem
    , n_followers
    from crosschain.bronze.twitter_accounts
    where not twitter_id in (
        '1314075720'
        , '925712018937712640'
        , '59119959'
        , '1445056111753584644'
        , '791390945656856577'
    )
    qualify(
        row_number() over (partition by twitter_id order by updated_at desc) = 1
    )
)
select *
from bi_analytics.twitter.retweets r
left join twitter_accounts a
    on a.twitter_id = r.user_id
where tweet_id = '1770858695838044646'


with t0 as (
    select distinct program_id
    , tx_id
    from solana.core.fact_events
    where block_timestamp >= CURRENT_DATE - 3
        and succeeded
    union
    select distinct c.value:program_id::string as program_id
    , t.tx_id
    from solana.core.fact_transactions t
    , lateral flatten(
        input => inner_instructions
    ) b
    , lateral flatten(
        input => b.value:instructions
    ) c
    where t.block_timestamp >= current_date - 3
), t1 as (
    select program_id
    , count(distinct tx_id) as n_tx
)
select t1.*
l.label
from t1
left join solana.core.dim_labels l
    on l.address = t1.program_id
order by n_tx desc
limit 10000



with t0 as (
    select distinct program_id
    , tx_id
    from solana.core.fact_events
    where block_timestamp >= CURRENT_DATE - 3
        and succeeded
    union
    select distinct c.value:program_id::string as program_id
    , t.tx_id
    from solana.core.fact_transactions t
    , lateral flatten(
        input => inner_instructions
    ) b
    , lateral flatten(
        input => b.value:instructions
    ) c
    where t.block_timestamp >= current_date - 3
        and t.succeeded
), t1 as (
    select program_id
    , count(distinct tx_id) as n_tx
)
select t1.*
l.label
from t1
left join solana.core.dim_labels l
    on l.address = t1.program_id
order by n_tx desc
limit 10000



with labels as (
  select
    c.value:dashboard_id :: string as dashboard_id,
    c.value:tag :: string as dashboard_tag
  from
    crosschain.bronze.data_science_uploads,
    lateral flatten(input => record_content) c
  where
    record_metadata:key like 'dashboard-tags%'
),
t0 as (
  select
    c.value:user_id :: string as user_id,
    c.value:mentee_user_id :: string as mentee_user_id,
    c.value:status :: string as status,
    LOWER(c.value:ecosystem :: string) as ecosystem,
    c.value:currency :: string as currency,
    c.value:base_comp :: int as base_comp,
    row_number() over (
      partition by user_id
      order by
        u.record_metadata:CreateTime :: int desc
    ) as rn
  from
    crosschain.bronze.data_science_uploads u,
    lateral flatten(input => record_content) c
  where
    record_metadata:key like 'ambassador%'
),
t1 as (
  select d.title
    , d.id as dashboard_id,
    u.username,
    u.profile_id,
    p.type
  from
    bi_analytics.velocity_app_prod.dashboards d
    left join bi_analytics.velocity_app_prod.users u on u.id = d.created_by_id
    left join bi_analytics.velocity_app_prod.profiles p on p.id = u.profile_id
  where
    p.type = 'user'
    and rn = 1
),
imp0 as (
  select
    d.id as dashboard_id,
    d.created_by_id as user_id,
    impression_count
  from
    bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.twitter.tweet t on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?') [0] :: string, 6)
  where
    not d.id in (
      select
        dashboard_id
      from
        labels
      where
        dashboard_tag = 'bot'
    )
    and t.created_at :: date >= current_date - 8
    and t.created_at :: date <= current_date - 1 qualify(
      row_number() over (
        partition by t.conversation_id
        order by
          impression_count desc
      ) = 1
      and row_number() over (
        partition by t.tweet_url
        order by
          impression_count desc
      ) = 1
    )
),
imp as (
  select
    user_id,
    sum(impression_count) as impression_count
  from
    imp0
  group by
    1
),
analyst as (
  select
    t1.user_id,
    t1.profile_id,
    t1.username,
    t1.ecosystem,
    t1.currency,
    t1.base_comp,
    floor(
      least(250000, coalesce(i.impression_count, 0)) / 500
    ) as incentive_comp,
    t1.base_comp + incentive_comp as amount
  from
    t1
    left join imp i on i.user_id = t1.user_id
  where
    status = 'analyst'
),
mentor as (
  select
    t1.user_id,
    t1.profile_id,
    t1.username,
    t1.ecosystem,
    t1.currency,
    t1.base_comp,
    floor(
      least(250000, coalesce(i.impression_count, 0)) / 1000
    ) as incentive_comp,
    t1.base_comp + incentive_comp as amount_usd
  from
    t1
    left join imp i on i.user_id = t1.mentee_user_id
  where
    status = 'mentor'
)
select
  *
from
  mentor
union
select
  *
from
  analyst


select block_timestamp::date as date
, sum(coalesce(sales_amount, 0)) as volume
, count(1) as n_sales
from solana.nft.fact_nft_sales
where succeeded
group by 1
order by 1


select *
from solana.core.fact_transfers
where block_timestamp >= '2023-09-01' 
    and block_timestamp <= '2024-01-01' 
    and tx_to in (
        '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj'
        , 'AoNVE2rKCE2YNA44V7NQt8N73JdPM7b6acZ2vzSpyPyi'
        , 'GFCYkPusxYWJ1yEoHNrFwjgLUmuCYiwC4nhwrV2UpMBk'
        , '7y4b6aJRaHZE4SedGxmHBza7W7vhihfacCjhnrsPCH7E'
        , 'FSjicwUa1dmWRv8RSff52VjDb4rZDkegQp4nmyrxqJan'
        , 'DwoMs2cjnPYqZEuU7erayAKVf5nSkjgEEWCcrzKDSiCU'
        , 'GUAvkrcND1KtReAhd39fu9JG7ku48WjVegoZpEngTkbF'
        , 'FDYSaNMFChoumP1FBBKuMdkXf8Tp4ENJrQP2NvWSNesD'
        , '2NSHjHeKA7fFMPUzQJjRy8zAWoxu5vtd3EZrY3vBEr53'
        , 'EYiZsYKcwrqUGShCyb2SjcR2aJYRr3a6bTkL6ieQBAqH'
        , '4vydyYdvSGTsHmx6CqAznKeApo8uj7372K6HwwLpSaJ8'
        , '64oTH9vE9zKG29ku2oSmMRVxi8EubJTJ4F2kKv5UfhMj'
        , '9LibpFooJaz4ENfCMRaDvjA5G48cNwYSYtKffaWc5Nog'
        , 'CsxtRUhtGk2eZE2amMoxJu9KW3xHscyMrsmRSYuumVx1'
        , '4Y7LZuuNZHsM5bW8W4o4YSJEsxvMP5LSkWY6C6aphTGb'
        , 'B849rQbJzzoH7LTfqZLxhDknuU3kmYJ43pCPM7SmWyoY'
        , '9zky78suDwh8t8mPbBCP4nr2Ew9pH2e1kLJMvMGDFT6L'
        , '3EL9aNsScuD3fhtoKKhcv9Y9eNxkkmazmZ1QGzPSqaeV'
        , '5HnuGGsBYMxRAjLPwTgEU4GH3omNHEihA3Jj1tajH56c'
        , '6YUm9FiU8Ub9Lf4PHPocyu7RojrpRRvW8eq8XDiHHjM9'
        , 'SdvB4HGbkqeXqoT1jW5wzbwV3vEYM1DA7Y3vXGavpwg'
        , 'GfiW9QxFanKghiaQFuCSP7gjvXnMhqzsFxQxLEaN9agG'
        , 'DSRHzvThxHFa2mFNrYZjsVLF6e2Yxqpe6dmU4Pti8dg3'
        , '6Ch8KyKpkZHKHNzEioLYg6b3w1WM8NMYg4pYnVsBU3pk'
        , 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC'
        , 'tLockgx2xHtyp8mtxKbWCuXhEFMoTTpAoFYBH5PRwAE'
    )
    and mint = 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL'


with wif as (
    select swapper
    , sum(case when swap_to_mint = 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm' then swap_to_amount else - swap_from_amount end) as net_wif
    from solana.defi.fact_swaps
    where block_timestamp >= '2023-10-01'
        and (swap_from_mint = 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm' or swap_to_mint = 'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm')
    group by 1
), popcat as (
    select swapper
    , sum(case when swap_to_mint = '7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr' then swap_to_amount else - swap_from_amount end) as net_popcat
    from solana.defi.fact_swaps
    where block_timestamp >= '2023-10-01'
        and (swap_from_mint = '7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr' or swap_to_mint = '7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr')
    group by 1
), bonk as (
    select swapper
    , sum(case when swap_to_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' then swap_to_amount else - swap_from_amount end) as net_bonk
    from solana.defi.fact_swaps
    where block_timestamp >= '2023-10-01'
        and (swap_from_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' or swap_to_mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263')
    group by 1
), t0 as (
    select coalesce(w.swapper, p.swapper, b.swapper) as address
    , coalesce(w.net_wif, 0) as net_wif
    , coalesce(p.net_popcat, 0) as net_popcat
    , coalesce(b.net_bonk, 0) as net_bonk
    from wif w
    full outer join popcat p
        on p.swapper = w.swapper
    full outer join bonk b
        on b.swapper = coalesce(w.swapper, p.swapper)
)
select *
, case when net_wif > 0 or net_popcat > 0 or net_bonk > 0 then 1 else 0 end as qualifies
from t0


select *
from solana.core.fact_transfers
where block_timestamp >= '2024-03-22'
    and mint = 'Av6qVigkb7USQyPXJkUvAEm4f599WTRvd75PUWBA9eNm'
    and (
        tx_from in ('rbz9r1AcFJYAat1XkGWG4vxyqb5Q3qP8Y6pVHeXV6ra', 'HpMhtZN2dyvgrucaRbeNGs4W4qpBae5YrJgHVuAMD2Nr', 'DxAUBJ97euJZYVishoV6fRA1EYwqWcaZfVZKwEn14Pw4', '5wnfsoAJTPJixK7jwen9r5ixVKLwQjwDAfpFMwFqwbyM', '3Q1z1YSGC441EicV1aKq6kqLSuHZovNsZfza3vwWuyAh')
        or tx_to in ('rbz9r1AcFJYAat1XkGWG4vxyqb5Q3qP8Y6pVHeXV6ra', 'HpMhtZN2dyvgrucaRbeNGs4W4qpBae5YrJgHVuAMD2Nr', 'DxAUBJ97euJZYVishoV6fRA1EYwqWcaZfVZKwEn14Pw4', '5wnfsoAJTPJixK7jwen9r5ixVKLwQjwDAfpFMwFqwbyM', '3Q1z1YSGC441EicV1aKq6kqLSuHZovNsZfza3vwWuyAh')
    )
order by block_timestamp

select swapper
, sum(case when swap_from_mint = 'Av6qVigkb7USQyPXJkUvAEm4f599WTRvd75PUWBA9eNm' then -swap_from_amount else swap_to_amount end) as net_amount
from solana.defi.fact_swaps
where block_timestamp >= '2024-03-22'
    and succeeded
    and swapper in (
'rbz9r1AcFJYAat1XkGWG4vxyqb5Q3qP8Y6pVHeXV6ra'
, 'HpMhtZN2dyvgrucaRbeNGs4W4qpBae5YrJgHVuAMD2Nr'
, 'DxAUBJ97euJZYVishoV6fRA1EYwqWcaZfVZKwEn14Pw4'
, '5wnfsoAJTPJixK7jwen9r5ixVKLwQjwDAfpFMwFqwbyM'
, '3Q1z1YSGC441EicV1aKq6kqLSuHZovNsZfza3vwWuyAh'
, 'CmYBQktaJDxpL5Y1K6Y58bvuBx38ynEUzbYfkMEfMQbE'
, '7YpaJTbn7nLHQ42gKzvE5RKNNDP3t7A7WaDbygqNgPc9'
, 'HpMhtZN2dyvgrucaRbeNGs4W4qpBae5YrJgHVuAMD2Nr'
, '5Vqb6yAnM7gC1UzCEmLugVheuVa9Mfs4VfoowTf2JToa'
, 'BQ72nSv9f3PRyRKCBnHLVrerrv37CYTHm5h3s9VSGQDV'
, 'GGztQqQ6pCPaJQnNpXBgELr5cs3WwDakRbh1iEMzjgSJ'
, '9nnLbotNTcUhvbrsA6Mdkx45Sm82G35zo28AqUvjExn8'
, '2MFoS3MPtvyQ4Wh4M9pdfPjz6UhVoNbFbGJAskCPCj3h'
, 'DxAUBJ97euJZYVishoV6fRA1EYwqWcaZfVZKwEn14Pw4'
, '4xDsmeTWPNjgSVSS1VTfzFq3iHZhp77ffPkAmkZkdu71'
, '6LXutJvKUw8Q5ue2gCgKHQdAN4suWW8awzFVC6XCguFx'
, 'Ehkzzin5ZT8nV6Rj7CP368YMbiYcz7PirrN5QdJP3iBa'
, '61ns7zTk2aaKPVruMgmBcGjhKAcmXfS3vKD3WXXtDiKj'
, 'AqFUtBcR94okzJLkatCQxfdLm4j837GfmcQLey628m8K'
, 'CapuXNQoDviLvU1PxFiizLgPNQCxrsag1uMeyk6zLVps'
, 'GdU9PgbLiwqfsDpyX8RYmmPko4MnJW5gos1NVtMYsYQ9'
, '34oieA1972b1CNopVXP9JMqZZYSxzhM92AwpdcDHETtV'
, '6U91aKa8pmMxkJwBCfPTmUEfZi6dHe7DcFq2ALvB2tbB'
, 'BgZSfgYP5XH8NCvxo9CqjV9yVkFkVrVKrsumWe81oDQU'
, 'Gdo2hHdf7rHdYngP4Bb1JBJLf1xHx3PfU8PTJf3CN33a'
, 'YP3ieya1v2dt43paiY63YcpRbUKp3R7YdFhvYNP8t2H'
, '5qcnDRq4oyWHMDghgDFU8pGmyUmgSXAvHeGiFGzP63tB'
, 'DFKmYw7MUZ54sLVHUqVV6rgaJ9ntsjJFbBVuHVsgeSfU'
, 'DCF7mg1fqhnnc65mqT5spjVwBs2actRoGNn1EsraG58j'
, '5deDWCuF78FkTZXaCMKw3iPJrpGJyeM1qHtRpMksASpR'
    )
    and (
        swap_from_mint = 'Av6qVigkb7USQyPXJkUvAEm4f599WTRvd75PUWBA9eNm'
        or swap_to_mint = 'Av6qVigkb7USQyPXJkUvAEm4f599WTRvd75PUWBA9eNm'
    )
group by 1


select *
from solana.core.fact_transfers
where block_timestamp >= '2024-01-01'
    and tx_from = '9gGNgCDC6jwdiNBHEQG6i6PVGnMU6UN6DXnAU85tgRU8'
    and tx_to = '3U7rsSeFmzDdpRVKaLx5gXgtpn64yZ7LHseQx6Nytx6q'
order by amount desc


with t0 as (
    select tx_from
    , tx_to
    , mint
    , sum(amount) as amount
    from solana.core.fact_transfers
    where block_timestamp >= '2023-10-01'
        and mint in (
            'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm'
            , '7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr'
            , 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
        )
    group by 1, 2, 3
), t1 as (
    select tx_from as address
    , mint
    , -amount as net_amount
    from t0
    union
    select tx_to as address
    , mint
    , amount as net_amount
    from t0
), t2 as (
    select address
    , mint
    , sum(net_amount) as net_amount
    from t1
    group by 1, 2
)
select *
from t2

select u.username
, u.id
, count(distinct q.id) as n_queries
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.users u
    on u.id = q.created_by_id
where q.created_at >= current_date - 14
group by 1, 2
order by 3 desc


select *
from bi_analytics_dev.silver.user_boost
limit 100



select u.username
, a.*
from bi_analytics.silver.ambassador a
left join bi_analytics.velocity_app_prod.users u
    on u.id = a.user_id



select *
-- decoded_instruction:args:side::string as side
-- , decoded_instruction:args:weight::int * power(10, -6) as weight
-- , signers[0]::string as voter
-- , value:pubkey::string as proposal
-- , value:name::string as name
from solana.core.fact_decoded_instructions i
-- , lateral flatten(input => decoded_instruction:accounts)
where block_timestamp >= '2024-02-26'
    and block_timestamp <= '2024-03-09'
    and i.program_id = 'TLoCKic2wGJm7VhZKumih4Lc35fUhYqVMgA4j389Buk'
    and tx_id in (
        '2PoG5Ghi4eLusft3HzNgoiGjN6uQDdM1PNzus8TYxHKA2qtyKQkyygE44E9dSEZcQftTAaQwe32gAYDPaBMxi9oV'
        , '2WBTEsLszbjcsE52grdPpbTSuZvWGyFFDNNmbRgMGN9ufKdGRNTJyE6LTuMZ2utQ9ahcyoySw78s7XKgPvRLGRXK'
        , '3nFLaPC8UfcAfGbMbV8mFNMyPqjLJ9pxogBBJuKiAXnvPQnTDt8nuKSz9HH7tZu8JjSrEyS6Xjvqhpw5QCfGyH1X'
    )
limit 100000

with t0 as (
    select distinct
    i.tx_id
    , i.block_timestamp
    , i.block_timestamp::date as date
    , i.event_type
    , a.value:name::string as name
    , a.value:pubkey::string as pubkey
    , a.value:orderVault::string as orderVault
    , decoded_instruction:args:aprBps::int / 100 as apr
    , decoded_instruction:args:price::int * pow(10, -9) as price
    , decoded_instruction:args:durationSec::int / (60 * 60 * 24) as n_days
    , i.signers[0]::string as signer
    -- decoded_instruction:args:side::string as side
    -- , decoded_instruction:args:weight::int * power(10, -6) as weight
    -- , signers[0]::string as voter
    -- , value:pubkey::string as proposal
    -- , value:name::string as name
    from solana.core.fact_decoded_instructions i
    , lateral flatten(input => decoded_instruction:accounts) a
    where block_timestamp >= '2024-02-26'
        -- and block_timestamp <= '2024-03-09'
        and i.program_id = 'TLoCKic2wGJm7VhZKumih4Lc35fUhYqVMgA4j389Buk'
        -- and tx_id in (
        --     '2PoG5Ghi4eLusft3HzNgoiGjN6uQDdM1PNzus8TYxHKA2qtyKQkyygE44E9dSEZcQftTAaQwe32gAYDPaBMxi9oV'
        --     , '2WBTEsLszbjcsE52grdPpbTSuZvWGyFFDNNmbRgMGN9ufKdGRNTJyE6LTuMZ2utQ9ahcyoySw78s7XKgPvRLGRXK'
        --     , '3nFLaPC8UfcAfGbMbV8mFNMyPqjLJ9pxogBBJuKiAXnvPQnTDt8nuKSz9HH7tZu8JjSrEyS6Xjvqhpw5QCfGyH1X'
        -- )
    order by block_timestamp, tx_id, event_type
    limit 100000
)
select *
from t0
, offers as (
    select o.*
    , t.signer as taker
    , t.tx_id as lock_tx
    , t.block_timestamp as lock_timestamp
    , c.tx_id as close_tx
    , c.block_timestamp as close_timestamp
    , w.pubkey as whitelist
    from t0 o
    join t0 w
        on w.name = 'whitelist'
        and w.tx_id = o.tx_id
    join t0 t
        on t.name = 'orderState'
        and t.event_type = 'lockOrder'
        and t.pubkey = o.pubkey
        and t.block_timestamp > o.block_timestamp
    left join t0 c
        on c.name = 'orderState'
        and c.event_type = 'closeOrder'
        and c.pubkey = o.pubkey
    where o.event_type = 'upsertOrder'
        and o.name = 'orderState'
), t1 as (
    select *
    , case 
        when whitelist = '3PJCoXPcswZEx8ZimR2xiQvr6sJGJWTp3cpsLhhc7pmP' then 'mad lads'
        when whitelist = '5SmBrw3z7wqXTZXxxZXRLDoiqY6yRzN57hiLULnRGihA' then 'sols spl20'
        when whitelist = '5ceCcEQ2PjYMXbsh956GZZPC5R1sZGnw4UBaJpVdDPE4' then 'froganas'
        when whitelist = 'AgnGG7RtqQncFcdJAT27GhnRWchk3wBDfsESkVXhmejq' then 'tensorians'
        when whitelist = 'EGdryU8HzwqhT8NpPno6CVT8D2Nr2CL4gJTF57NvX1UV' then 'cryptoundeads'
        when whitelist = 'EKPqEMbgMuf6qD69FMGnQamW2f5vCssDrqmoXJAiyjW1' then 'flash trade'
        when whitelist = 'CtXrSG5dzVbRoBpDyAhM69p7tjNuL3oTy18PfoKNQHyy' then 'claynosaurz'
        when whitelist = '2EywrATVadqRPPa1FoxtXV8QCkQgBrtNrcjWkw3g4HKV' then 'lifinity flares'
        when whitelist = '8uoZmfyBYmSumPzDwABz7gN7u6FAY4ziKLAWw7LYTYa7' then 'okay bears'
        when whitelist = 'HXESogjkv3jfw1qc82VZBtBQAan1h4pZ4trBjVaT4R4b' then 'sujiko warrior'
        when whitelist = 'WudU7ovgexnZLQoGhXcRYCtuzMwyFUftnkysZwCCZTW' then 'elixir: ovols'
        when whitelist = '5wnPJvyhgTMRU94MPxNLSLeYkfzCJeXMtvEjPrwPiB6A' then 'bodoggos'
        when whitelist = 'CvRbqkLj7cVztw8PRqyUncFQBrHWLaSMMXoSZ2kYiBPJ' then 'chads'
        when whitelist = 'Ddk7szjyjEtzwqajNY6ospWZjrQEYTYAg9GK65zUDc2J' then 'banx'
        when whitelist = 'CzSoeDrGdHVQFATmdx2LmarDNxvLn41xFxfJfaijfedS' then 'smb gen3'
        when whitelist = 'B9tkGi9aRz9YsxfLH5PTWf32PsWSQqccHv1DqZqj4khh' then 'marms'
        when whitelist = 'b8YmUZ2Z7dd3M7vXGcuK3t71KTrtbSBFHxFmxwP2WjP' then 'homeowners association (parcl)'
        when whitelist = 'F3nUN87ta1MXWHQVxtx9RSV95MMJoL173i9JveGd73Qs' then 'quekz'
        when whitelist = '8z2oktNUCRKJNCw6D5Hy8Fj8Rs5HQmKANa9KL2yWz86J' then 'famous fox federation'
        when whitelist = 'GFqNEdvnY3T2iX2NQenGNuXnkmhRfPgrnCKFJ51J6TDC' then 'sharx'
        when whitelist = 'DjLrH2MxHEUj79Jb9oB7KpJ8u85JT681Do1jboVP9okq' then 'smb gen2'
        -- when whitelist = '5Q3Ns8m8xpucPafax4B563pehUWqx1WKd6Fc9KYsWN8r' then '_'
        -- when whitelist = '5XpDVyL5QtmEPkef4gTzwbrNA7rggYAH5ezLZM2SykxG' then '_'
        -- when whitelist = '8d3WpJ4is6SWYE5qm9odqkw8jHArt1vtT1udu7MNKGo' then '_'
        -- when whitelist = 'Fvc7jhYKtorvW6KBKe3H2GZYc9ne9AEQwyTBhaNeCvwq' then '_'
        else 'other' end as collection
    from offers
    order by block_timestamp, tx_id
)
select *
from t1


with t0 as (
    select *
    -- decoded_instruction:args:side::string as side
    -- , decoded_instruction:args:weight::int * power(10, -6) as weight
    -- , signers[0]::string as voter
    -- , value:pubkey::string as proposal
    -- , value:name::string as name
    from solana.core.fact_decoded_instructions i
    -- , lateral flatten(input => decoded_instruction:accounts)
    where block_timestamp >= dateadd('hours', -5, current_timestamp)
        and i.program_id = 'TLoCKic2wGJm7VhZKumih4Lc35fUhYqVMgA4j389Buk'
    limit 100000
), t1 as (
    select t0.tx_id
    , t.mint
    , sum(amount) as volume
    from t0
    join solana.core.fact_transfers t
        on t.block_timestamp = t0.block_timestamp
        and t.tx_id = t0.tx_id
    where t.block_timestamp >= dateadd('hours', -5, current_timestamp)
        and mint <> 'So11111111111111111111111111111111111111112'
        and amount = 1
    group by 1, 2
), t2 as (
    select t0.tx_id
    , sum(amount) as volume
    from t0
    join solana.core.fact_transfers t
        on t.block_timestamp = t0.block_timestamp
        and t.tx_id = t0.tx_id
    where t.block_timestamp >= dateadd('hours', -5, current_timestamp)
        and mint = 'So11111111111111111111111111111111111111112'
    group by 1
)
select t1.tx_id
, t1.mint
, l.label
, t2.volume
-- , t0.tx_id
-- , t0.event_type
, t0.decoded_instruction:args:event:upsert:tupleData:"0":order:aprBps::int / 100 as aprBps
, t0.decoded_instruction:args:event:upsert:tupleData:"0":order:price::int * pow(10, -9) as price
, t0.decoded_instruction:args:event:upsert:tupleData:"0":order:maker::string as maker
, t0.block_timestamp::date as date
, t0.signers[0]::string as taker
, t0.*
from t0
join t1
    on t1.tx_id = t0.tx_id
join t2
    on t2.tx_id = t0.tx_id
join solana.core.dim_labels l
    on l.address = t1.mint
-- where t0.tx_id = '21VnsF9A5hNrUqBKr4tgWMQkmTpYXA7U9JMnKM2KdcwaLa4UnhBNc4YzY6zcQXDjEuRqiMWKTpNRSswcGD3wKyKg'
where price is not null
order by label, price

select *
from bi_analytics.silver.user_boost

select u.username
, q.created_at::date as date
, q.statement
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.users u
    on u.id = q.created_by_id
where statement like '%9gVndQ5SdugdFfGzyuKmePLRJZkCreKZ2iUTEg4agR5g%'


with t0 as (
    select 
    i.tx_id
    , i.block_timestamp
    , i.block_timestamp::date as date
    , i.event_type
    , a.value:name::string as name
    , a.value:pubkey::string as pubkey
    , a.value:orderVault::string as orderVault
    , decoded_instruction:args:aprBps::int / 100 as apr
    , decoded_instruction:args:price::int * pow(10, -9) as price
    , decoded_instruction:args:durationSec::int / (60 * 60 * 24) as n_days
    , case when decoded_instruction:args:orderType::string like '%token%' then 'short' else 'long' end as orderType
    , i.signers[0]::string as signer
    , i.decoded_instruction
    -- decoded_instruction:args:side::string as side
    -- , decoded_instruction:args:weight::int * power(10, -6) as weight
    -- , signers[0]::string as voter
    -- , value:pubkey::string as proposal
    -- , value:name::string as name
    from solana.core.fact_decoded_instructions i
    , lateral flatten(input => decoded_instruction:accounts) a
    where block_timestamp >= '2024-02-28'
        and block_timestamp <= '2024-04-09'
        and i.program_id = 'TLoCKic2wGJm7VhZKumih4Lc35fUhYqVMgA4j389Buk'
        -- and tx_id in (
        --     '2PoG5Ghi4eLusft3HzNgoiGjN6uQDdM1PNzus8TYxHKA2qtyKQkyygE44E9dSEZcQftTAaQwe32gAYDPaBMxi9oV'
        --     , '2WBTEsLszbjcsE52grdPpbTSuZvWGyFFDNNmbRgMGN9ufKdGRNTJyE6LTuMZ2utQ9ahcyoySw78s7XKgPvRLGRXK'
        --     , '3nFLaPC8UfcAfGbMbV8mFNMyPqjLJ9pxogBBJuKiAXnvPQnTDt8nuKSz9HH7tZu8JjSrEyS6Xjvqhpw5QCfGyH1X'
        -- )
    order by block_timestamp, tx_id, event_type
    -- limit 1000000
), offers as (
    select o.*
    , t.tx_id as lock_tx
    , t.block_timestamp as lock_timestamp
    , t.decoded_instruction as lock_instruction
    , c.tx_id as close_tx
    , c.block_timestamp as close_timestamp
    , w.pubkey as whitelist
    from t0 o
    join t0 w
        on w.name = 'whitelist'
        and w.tx_id = o.tx_id
    join t0 t
        on t.name = 'orderState'
        and t.event_type = 'lockOrder'
        and t.pubkey = o.pubkey
    left join t0 c
        on c.name = 'orderState'
        and c.event_type = 'closeOrder'
        and c.pubkey = o.pubkey
    where o.event_type = 'upsertOrder'
        and o.name = 'orderState'
    qualify(
        row_number() over (partition by lock_tx order by lock_timestamp desc) = 1
    )
), t1 as (
    select 
    case 
        when whitelist = '3PJCoXPcswZEx8ZimR2xiQvr6sJGJWTp3cpsLhhc7pmP' then 'mad lads'
        when whitelist = '5SmBrw3z7wqXTZXxxZXRLDoiqY6yRzN57hiLULnRGihA' then 'sols spl20'
        when whitelist = '5ceCcEQ2PjYMXbsh956GZZPC5R1sZGnw4UBaJpVdDPE4' then 'froganas'
        when whitelist = 'AgnGG7RtqQncFcdJAT27GhnRWchk3wBDfsESkVXhmejq' then 'tensorians'
        when whitelist = 'EGdryU8HzwqhT8NpPno6CVT8D2Nr2CL4gJTF57NvX1UV' then 'cryptoundeads'
        when whitelist = 'EKPqEMbgMuf6qD69FMGnQamW2f5vCssDrqmoXJAiyjW1' then 'flash trade'
        when whitelist = 'CtXrSG5dzVbRoBpDyAhM69p7tjNuL3oTy18PfoKNQHyy' then 'claynosaurz'
        when whitelist = '2EywrATVadqRPPa1FoxtXV8QCkQgBrtNrcjWkw3g4HKV' then 'lifinity flares'
        when whitelist = '8uoZmfyBYmSumPzDwABz7gN7u6FAY4ziKLAWw7LYTYa7' then 'okay bears'
        when whitelist = 'HXESogjkv3jfw1qc82VZBtBQAan1h4pZ4trBjVaT4R4b' then 'sujiko warrior'
        when whitelist = 'WudU7ovgexnZLQoGhXcRYCtuzMwyFUftnkysZwCCZTW' then 'elixir: ovols'
        when whitelist = '5wnPJvyhgTMRU94MPxNLSLeYkfzCJeXMtvEjPrwPiB6A' then 'bodoggos'
        when whitelist = 'CvRbqkLj7cVztw8PRqyUncFQBrHWLaSMMXoSZ2kYiBPJ' then 'chads'
        when whitelist = 'Ddk7szjyjEtzwqajNY6ospWZjrQEYTYAg9GK65zUDc2J' then 'banx'
        when whitelist = 'CzSoeDrGdHVQFATmdx2LmarDNxvLn41xFxfJfaijfedS' then 'smb gen3'
        when whitelist = 'B9tkGi9aRz9YsxfLH5PTWf32PsWSQqccHv1DqZqj4khh' then 'marms'
        when whitelist = 'b8YmUZ2Z7dd3M7vXGcuK3t71KTrtbSBFHxFmxwP2WjP' then 'homeowners association (parcl)'
        when whitelist = 'F3nUN87ta1MXWHQVxtx9RSV95MMJoL173i9JveGd73Qs' then 'quekz'
        when whitelist = '8z2oktNUCRKJNCw6D5Hy8Fj8Rs5HQmKANa9KL2yWz86J' then 'famous fox federation'
        when whitelist = 'GFqNEdvnY3T2iX2NQenGNuXnkmhRfPgrnCKFJ51J6TDC' then 'sharx'
        when whitelist = 'DjLrH2MxHEUj79Jb9oB7KpJ8u85JT681Do1jboVP9okq' then 'smb gen2'
        else 'other' end as collection
        , *
    from offers
    order by collection, price
)
select lock_timestamp::date as lock_date
, orderType
, count(1) as n_locks
, sum(price) as volume
from t1
where lock_date >= '2024-05-01'
group by 1, 2

select t0.*
from t0 where name = 'orderState'
    and tx_id in (select tx_id from offers union select lock_tx as tx_id from offers)
order by pubkey, block_timestamp, name
limit 1000
, case 
    when whitelist = '3PJCoXPcswZEx8ZimR2xiQvr6sJGJWTp3cpsLhhc7pmP' then 'mad lads'
    when whitelist = '5SmBrw3z7wqXTZXxxZXRLDoiqY6yRzN57hiLULnRGihA' then 'sols spl20'
    when whitelist = '5ceCcEQ2PjYMXbsh956GZZPC5R1sZGnw4UBaJpVdDPE4' then 'froganas'
    when whitelist = 'AgnGG7RtqQncFcdJAT27GhnRWchk3wBDfsESkVXhmejq' then 'tensorians'
    when whitelist = 'EGdryU8HzwqhT8NpPno6CVT8D2Nr2CL4gJTF57NvX1UV' then 'cryptoundeads'
    when whitelist = 'EKPqEMbgMuf6qD69FMGnQamW2f5vCssDrqmoXJAiyjW1' then 'flash trade'
    when whitelist = 'CtXrSG5dzVbRoBpDyAhM69p7tjNuL3oTy18PfoKNQHyy' then 'claynosaurz'
    when whitelist = '2EywrATVadqRPPa1FoxtXV8QCkQgBrtNrcjWkw3g4HKV' then 'lifinity flares'
    when whitelist = '8uoZmfyBYmSumPzDwABz7gN7u6FAY4ziKLAWw7LYTYa7' then 'okay bears'
    when whitelist = 'HXESogjkv3jfw1qc82VZBtBQAan1h4pZ4trBjVaT4R4b' then 'sujiko warrior'
    when whitelist = 'WudU7ovgexnZLQoGhXcRYCtuzMwyFUftnkysZwCCZTW' then 'elixir: ovols'
    when whitelist = '5wnPJvyhgTMRU94MPxNLSLeYkfzCJeXMtvEjPrwPiB6A' then 'bodoggos'
    when whitelist = 'CvRbqkLj7cVztw8PRqyUncFQBrHWLaSMMXoSZ2kYiBPJ' then 'chads'
    when whitelist = 'Ddk7szjyjEtzwqajNY6ospWZjrQEYTYAg9GK65zUDc2J' then 'banx'
    when whitelist = 'CzSoeDrGdHVQFATmdx2LmarDNxvLn41xFxfJfaijfedS' then 'smb gen3'
    when whitelist = 'B9tkGi9aRz9YsxfLH5PTWf32PsWSQqccHv1DqZqj4khh' then 'marms'
    when whitelist = 'b8YmUZ2Z7dd3M7vXGcuK3t71KTrtbSBFHxFmxwP2WjP' then 'homeowners association (parcl)'
    when whitelist = 'F3nUN87ta1MXWHQVxtx9RSV95MMJoL173i9JveGd73Qs' then 'quekz'
    when whitelist = '8z2oktNUCRKJNCw6D5Hy8Fj8Rs5HQmKANa9KL2yWz86J' then 'famous fox federation'
    when whitelist = 'GFqNEdvnY3T2iX2NQenGNuXnkmhRfPgrnCKFJ51J6TDC' then 'sharx'
    when whitelist = 'DjLrH2MxHEUj79Jb9oB7KpJ8u85JT681Do1jboVP9okq' then 'smb gen2'
    else whitelist end as collection
from offers
order by block_timestamp, tx_id


select distinct tx_id
    from
      solana.core.fact_transfers
    where
      BLOCK_TIMESTAMP >= CURRENT_DATE - 1
      and TX_TO = '7e7qhwnJuLVDGBiLAGjB9FxfpizsPhQjoBxkNA5wVCCc'
      and MINT = 'So11111111111111111111111111111111111111112'
limit 100


with t0 as (
    select distinct block_timestamp
    , tx_id
    , signers[0]::string as signer
    from solana.core.fact_events
    where block_timestamp >= current_date - 1
        and program_id = 'JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp'
        and succeeded
), t1 as (
    select distinct t0.*
    , case
        when log_messages::string like '%TakeLoan%' then 'Take'
        else 'Borrow'
        end as tx_type
    from t0
    join solana.core.fact_transactions t
        on t.block_timestamp = t0.block_timestamp
        and t.tx_id = t0.tx_id
    where t.block_timestamp >= current_date - 1
        and (
            log_messages::string like '%TakeLoan%'
            or log_messages::string like '%Borrow%'
        )
), t2 as (
    select t1.block_timestamp
    , t1.tx_id
    , t1.tx_type
    , t.tx_from
    , t1.signer
    , max(amount) as amount
    from t1
    join solana.core.fact_transfers t
        on t.block_timestamp = t1.block_timestamp
        and t.tx_id = t1.tx_id
        and mint = 'So11111111111111111111111111111111111111112'
    group by 1, 2, 3, 4
)
select *
from t2

with t0 as (
    select distinct block_timestamp
    , tx_id
    , signers[0]::string as signer
    from solana.core.fact_events e
    where block_timestamp >= current_date - 7
), t1 as (
    select t.block_timestamp::date as date
    , t.tx_id
    , t.tx_from
    , t.tx_to
    , t0.signer
    , sum(amount) as amount
    from solana.core.fact_transfers t
    join t0
        on t0.block_timestamp = t.block_timestamp
        and t0.tx_id = t.tx_id
    where t.block_timestamp >= current_date - 7
    group by 1, 2, 3, 4, 5
)
select * from t1

with t0 as (
    select distinct block_timestamp
    , tx_id
    , signers[0]::string as signer
    from solana.core.fact_events e
    where block_timestamp >= current_date - 7
), t1 as (
    select t.block_timestamp::date as date
    , t.tx_id
    , t.tx_from
    , t.tx_to
    , t0.signer
    , sum(amount) as amount
    from solana.core.fact_transfers t
    join t0
        on t0.block_timestamp = t.block_timestamp
        and t0.tx_id = t.tx_id
    where t.block_timestamp >= current_date - 7
    group by 1, 2, 3, 4, 5
)
select * from t1


with t0 as (
    select e.inner_instruction:instructions[2]:accounts[3]::string as mint
    , e.inner_instruction:instructions[0]:parsed:info:source::string as source
    , e.inner_instruction:instructions[0]:parsed:info:destination::string as destination
    , e.inner_instruction:instructions[0]:parsed:info:lamports::int * power(10, -9) as amount
    , e.tx_id
    , case when t.log_messages::string like '%Repay%' then 'repay' else 'other' end as tx_type
    from solana.core.fact_events e
    join solana.core.fact_transactions t
        on t.block_timestamp = e.block_timestamp
        and t.tx_id = e.tx_id
    where e.block_timestamp >= current_date - 5
        and t.block_timestamp >= current_date - 5
        and e.program_id = 'JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp'
)
select *
from t0
where mint is not null
    and source is not null
    and destination is not null
    and tx_type = 'repay'


select *
from bi_analytics.velocity_app_prod.dashboards
where slug_id like '%%'


select d.id as dashboard_id
, t.conversation_id
, t.impression_count
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.twitter.tweet t
    on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?') [0] :: string, 6)
qualify(
    row_number() over (partition by t.conversation_id order by impression_count desc) = 1
    and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
)



select *
from bi_analytics_dev.silver.ambassador
limit 100


select *
from solana.core.fact_decoded_instructions i
where block_timestamp >= current_date - 1
    and program_id = 'JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp'


with t0 as (
    select distinct tx_id
    from solana.core.fact_events
    where block_timestamp >= '2024-04-15'
        and program_id = 'DiSLRwcSFvtwvMWSs7ubBMvYRaYNYupa76ZSuYLe6D7j'
)
select tx_from, sum(amount)
from solana.core.fact_transfers t
join t0
    on t0.tx_id = t.tx_id
where t.block_timestamp >= '2024-04-15'
    and t.mint = 'SHARKSYJjqaNyxVfrpnBN9pjgkhwDhatnMyicWPnr1s'
group by 1



with t0 as (
    select distinct tx_id
    from solana.core.fact_transactions
    where block_timestamp >= '2024-03-28'
        and block_timestamp <= '2024-04-28'
        and log_messages::string like '%Program log: Instruction: NewClaim%'
)
select tx_from, tx_to, sum(amount), min(block_timestamp)
from solana.core.fact_transfers t
join t0
    on t0.tx_id = t.tx_id
where t.block_timestamp >= '2024-03-28'
    and t.block_timestamp <= '2024-04-28'
    and t.mint = 'ZEUS1aR7aX8DFFJf5QjWj2ftDDdNTroMNGo8YoQm3Gq'
group by 1, 2
order by 3 desc



select t.conversation_id
, t.dashboard_id
, concat('https://flipsidecrypto.xyz/',u. username, '/', d.latest_slug) as clean_url
, concat('https://twitter.com/NA/status/', t.conversation_id) as tweet_url
from bi_analytics.twitter.missing_tweets t
join bi_analytics.velocity_app_prod.dashboards d
    on d.id = t.dashboard_id
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id

select d.created_at::date as date
, d.title
, d.id
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
where u.username = 'alitaslimi'
order by d.created_at desc
limit 1000



select distinct d.title
, d.id as dashboard_id
, u.username
, u.id as user_id
, t.tweet_url
, t.created_at
from bi_analytics.twitter.tweet_bans b
join bi_analytics.twitter.tweet t
    on (
        t.id = b.conversation_id
        or t.conversation_id = b.conversation_id
    )
join bi_analytics.velocity_app_prod.dashboards d
    on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
order by t.created_at desc

select distinct c.value:dashboard_id::string as dashboard_id
, d.title
, u.username
, c.value:tag::string as dashboard_tag
from crosschain.bronze.data_science_uploads
, lateral flatten(
    input => record_content
) c
join bi_analytics.velocity_app_prod.dashboards d
    on d.id = dashboard_id
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
where record_metadata:key like 'dashboard-tags%'
    and dashboard_tag = 'bot'

'7Wgz6LB4gkd7hr1hjyTur8tZSXC5sx1QYbYGQ7N2w5z7'
, 'ySoLLxJfRkecrD4wNL6NXmSw6P6fSmeR7tt1fn4Lqvw'
, 'Hkg99Cz41FkvGKK2RSWfFgFbZUi8xhPjJyDMFZkNVVx8'
, 'B2VdH9Xqfu2EPjmYfYMuNU4i4e5Q7PBFBUn8htSGqgd'
, '6QvvZKGEHxyTKgkHQpfpJXPAgWUAzefXVHYPBFrJGPYP'
, 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC'
, '5WdaiasPs86NgtJQkffiW7FqBXg1KgNd3e3zmXUg5DRk'
, 'AHLQSeswjbL7jgF6u8VKcM7NB1VTK3N4TLvFR7GRLRMS'
, '7woFekwSQ4txRTegnTN57NvnBLLdjLjDBFbwHdd6YnDK'
, 'DHVZfycL8WYxjKkXjFoe2YAMZAdVHDSTNmW4WhSD4YhK'
, 'DVtPTMGQVnxoBgqMmh94aF76VDYMGuzrmXYTAaLjnqbt'
, 'Dyrmiif4SasibiunSAE2zc4TEgZQ2gfURW1TaeBgP69t'
, '5eYMVxUqtnwBtbQVbWpWEwZJPw1BpM4U3V3Z81igRWUv'
, 'A3ECFC6kN8Jd6pY4YVW2wbSX6E1QCi3nyF2ao1JVcAKB'
, 'CfdsYYnM1eqS1Ei2ppH1LA5q8yfRTE4KAmJJUU4kYeEz'
, 'znFEBgiGLQdvuSU5sNH6XS5bqQJViPUX4bofgFaWQam'
, 'BK1W2fphfja4TcZE7D58mzh9vuRUVbxNcFQXS7vzgWih'
, 'CmbwG6X9t49TbkpRbPsHzKEcNdVSjMMYE3jjoRV8JErP'
, 'Dc5VNFo8TVTPiAXNoFfDhRozV4irJpmhVX3WmMNmUWMQ'
, 'H9ko65q5RzfVCPLoti1FE1EP5cjh83UN6gBAqXXHkP3M'



with t0 as (
    select case when decoded_instruction:name::string = 'takeLoanV3' then decoded_instruction:accounts[0]:pubkey::string else decoded_instruction:accounts[5]:pubkey::string end as lender
    , case when block_timestamp <= '2024-04-18' then 'During Airdrop' else 'After Airdrop' end as time_period
    , count(1) as n_loans
    , sum(split(decoded_instruction:args:expectedLoan::string, ',')[1]::int) * power(10, -9) as volume
    from solana.core.fact_decoded_instructions
    where block_timestamp >= '2024-01-01'
        and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
        and decoded_instruction:name::string in ('takeLoanV3','extendLoanV3')
    group by 1, 2
    -- order by 3 desc
), t1 as (
    select coalesce(t0a.lender, t0b.lender) as lender
    , coalesce(t0a.n_loans, 0) as n_loans_0
    , coalesce(t0b.n_loans, 0) as n_loans_1
    , coalesce(t0a.volume, 0) as volume_0
    , coalesce(t0b.volume, 0) as volume_1
    from t0 t0a
    full outer join t0 t0b
        on t0b.lender = t0a.lender
    where coalesce(t0a.time_period, 'During Airdrop') = 'During Airdrop'
        and coalesce(t0b.time_period, 'After Airdrop') = 'After Airdrop'
)
select *
, sum(n_loans_0) over () as tot_loans_0
, sum(n_loans_1) over () as tot_loans_1
, sum(volume_0) over () as tot_volume_0
, sum(volume_1) over () as tot_volume_1
, n_loans_0 / tot_loans_0 as pct_loans_0
, n_loans_1 / tot_loans_1 as pct_loans_1
, volume_0 / tot_volume_0 as pct_volume_0
, volume_1 / tot_volume_1 as pct_volume_1
, pct_volume_1 - pct_volume_0 as dff
from t1
order by pct_volume_0 desc


with t0 as (
    select twitter_id
    , twitter_handle
    , account_type
    , ecosystems[0]::string as ecosystem
    , n_followers
    from crosschain.bronze.twitter_accounts
    left join 
        lateral flatten (input => ecosystems) f
    where not twitter_id in (
        '1314075720'
        , '925712018937712640'
        , '59119959'
        , '1445056111753584644'
        , '791390945656856577'
    )
    qualify(
        row_number() over (partition by twitter_id order by updated_at desc) = 1
    )
    group by 1, 2, 3, 4, 5
) 
select twitter_id
, twitter_handle
, account_type
, n_followers
, LISTAGG(f.value, ',') as ecosystem_list
from crosschain.bronze.twitter_accounts
left join 
    lateral flatten (input => ecosystems) f
where not twitter_id in (
    '1314075720'
    , '925712018937712640'
    , '59119959'
    , '1445056111753584644'
    , '791390945656856577'
)
group by 1, 2, 3, 4



select distinct name
from bi_analytics.velocity_app_prod.tags t
where t.type = 'project'
order by 1


select *
from crosschain.bronze.twitter_accounts
order by created_at desc



select *
, ecosystems[0]::string as eco_0
, ecosystems[1]::string as eco_1
, ecosystems[2]::string as eco_2
, ecosystems[3]::string as eco_3
from crosschain.bronze.twitter_accounts
order by updated_at desc


select *
from bi_analytics.content_rankings.dashboard_rankings
order by ranking_trending
limit 100     

select dbt_updated_at, ranking_trending
from bi_analytics.snapshots.hourly_dashboard_rankings hr
where dashboard_id = 'dd969aab-a0cc-433b-bcff-1daa2cfc72d1'
order by 1


with league as (
  -- this CTE updated by kb
  select c.value:user_id::string as user_id
  , c.value:tag_type::string as tag_type
  , c.value:tag_name::string as tag_name
  , u.record_metadata:CreateTime::int as updated_at
  from crosschain.bronze.data_science_uploads u
  , lateral flatten(
      input => record_content
  ) c
  where record_metadata:key like 'analyst-tag%'
      and tag_type = 'League'
  qualify (
      row_number() over (
          partition by user_id
          order by updated_at desc
      ) = 1
  )
), gold_league as (
    -- this CTE updated by kb
    select *
    from league
    where tag_name = 'Gold League'
), rk_hist0 as (
    select d.id as dashboard_id
    , date_trunc('hour', dbt_updated_at) as hour
    , case when dbt_updated_at <= '2024-05-01 10:00:00' or gl.user_id is not null then 'Gold' else 'Silver' end as league
    , avg(hr.ranking_trending) as rk0
    from bi_analytics.snapshots.hourly_dashboard_rankings hr
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    left join gold_league gl
        on gl.user_id = u.id
    where dbt_updated_at >= '2024-02-01 00:00:00'
        and coalesce(u.role, '') <> 'internal'
        and not u.username in (
            'Polaris_9R','dsaber','flipsidecrypto','metricsdao','drethereum','Orion_9R','sam','forgash','nftchance__','danner'
        )
        and hr.ranking_trending <= 200
    group by 1, 2, 3
), labels as (
    select distinct d.id as dashboard_id
    , d.title
    , d.latest_slug
    , concat('https://flipsidecrypto.xyz/', u.username, '/', d.latest_slug) as dashboard_url
    , u.username
    , case when (
        q.statement like '%VoTpe3tHQ7AjQHMapgSue2HJFAh2cGsdokqN3XqmVSj%'
        or q.statement like '%GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY%'
    ) then 'jupiter' else t.name end as project
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    left join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
        on q.id = qtt.A
    left join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and (t.name = 'vertex' or t.type = 'project')
    where t.name is not null
        or q.statement like '%VoTpe3tHQ7AjQHMapgSue2HJFAh2cGsdokqN3XqmVSj%'
        or q.statement like '%GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY%'
)
, boosts as (
    select 'Near Feb 23' as name, 'near' as project, '2024-02-23 19:00:00' as start_hour, 24 * 3 as n_hours, 30 as top_n, 2 as mult
    union select 'Blast Mar 1' as name, 'blast' as project, '2024-05-01 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Jup Mar 7' as name, 'jupiter' as project, '2024-03-07 21:00:00' as start_hour, 24 * 1 as n_hours, 40 as top_n, 9 as mult
    union select 'Vertex Mar 14' as name, 'vertex' as project, '2024-03-14 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Near Mar 18' as name, 'near' as project, '2024-03-18 15:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Aptos Mar 19' as name, 'aptos' as project, '2024-03-19 21:00:00' as start_hour, 24 * 1 as n_hours, 40 as top_n, 2 as mult
    union select 'Near Mar 24' as name, 'near' as project, '2024-03-24 21:00:00' as start_hour, 24 * 5 as n_hours, 40 as top_n, 4 as mult
    union select 'Near Mar 24' as name, 'near' as project, '2024-03-24 21:00:00' as start_hour, 24 * 5 as n_hours, 40 as top_n, 4 as mult
    union select 'Aptos Apr 10' as name, 'aptos' as project, '2024-04-10 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 2 as mult
    union select 'Vertex Apr 12' as name, 'vertex' as project, '2024-04-12 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Sei Apr 15' as name, 'sei' as project, '2024-04-15 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 2 as mult
    union select 'Axelar Apr 17' as name, 'axelar' as project, '2024-04-17 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 4 as mult
    union select 'Aptos Apr 24' as name, 'aptos' as project, '2024-04-24 17:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Axelar Apr 27' as name, 'axelar' as project, '2024-04-27 17:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Blast May 3' as name, 'blast' as project, '2024-05-03 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Avalanche May 6' as name, 'avalanche' as project, '2024-05-06 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Flow May 9' as name, 'flow' as project, '2024-05-09 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 4 as mult
    union select 'Near May 12' as name, 'near' as project, '2024-05-12 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 2 as mult
    union select 'Near May 14' as name, 'near' as project, '2024-05-14 16:00:00' as start_hour, 24 * 1 as n_hours, 40 as top_n, 4 as mult
    union select 'Aptos May 15' as name, 'aptos' as project, '2024-05-15 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
)
-- , twitter as (
--     select d.dashboard_id
--     , d.title
--     , d.dashboard_url
--     , d.username
--     , t.created_at::date as tweet_date
--     , t.tweet_url
--     , t.impression_count
--     , b.name
--     , b.project
--     from labels d
--     join bi_analytics.twitter.tweet t
--         on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
--     join boosts b
--         on b.project = d.project
--     where t.created_at >= dateadd('hours', -12, b.start_hour)
--         and t.created_at <= dateadd('hours', 2 + b.n_hours, b.start_hour)
--     qualify(
--         row_number() over (partition by t.conversation_id order by impression_count desc) = 1
--         and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
--     )
-- )
-- select project
-- , sum(1) 
-- select title, dashboard_url, username, count(1) as n_tweets, sum(impression_count) as impression_count from twitter
-- select *
-- where name = 'Avalanche May 6'
-- group by 1, 2, 3
, rk_hist1 as (
    select *
    , row_number() over (partition by hour, league order by rk0) as rk
    from rk_hist0
)
, t0 as (
    select r.dashboard_id
    , r.hour
    , r.hour::date as topn_date
    , r.rk
    , case
        when league = 'Gold' and r.rk <= 10 then 1.5
        when league = 'Gold' and r.rk <= 40 then 1
        when league = 'Silver' and r.rk <= 10 then 0.75
        when league = 'Silver' and r.rk <= 30 then 0.5
        else 0 end as base_amount
    , l.title
    , l.username
    , l.project
    , b.name
    , b.start_hour::date as date
    , b.n_hours
    , b.mult
    , casecoalesce(b.mult, 0) * base_amount as boost_amount
    from rk_hist1 r
    join labels l
        on l.dashboard_id = r.dashboard_id
    left join boosts b
        on b.project = l.project
        and r.hour >= b.start_hour
        and r.hour < dateadd('hours', b.n_hours, b.start_hour)
        and r.league = 'Gold'
    where r.rk <= 40
    qualify(
        row_number() over (partition by r.dashboard_id, r.hour order by coalesce(b.mult, 0) desc) = 1
    )
)
select *
from t0
where project = 'vertex'


-- select * from t0 where name = 'Blast May 3'
, cost as (
    select name
    , project
    , date
    , n_hours / 24 as n_days
    , mult + 1 as boost
    , sum(base_amount) as base_amount
    , sum(boost_amount) as boost_amount
    from t0
    group by 1, 2, 3, 4, 5
), impressions as (
    select name
    , sum(impression_count) as impression_count
    , count(1) as n_tweets
    , count(distinct dashboard_id) as n_dashboards
    from twitter
    group by 1
)
select c.*
, c.base_amount + c.boost_amount as total_paid
, i.impression_count
, i.n_tweets
, i.n_dashboards
from cost c
left join impressions i
    on i.name = c.name


select
coalesce(tu.username, u.username) as username,
d.id as dashboard_id,
t.title,
row_number() over (
    order by
    dr.ranking_trending
) as current_rank
from
bi_analytics.content_rankings.dashboard_rankings dr
join bi_analytics.velocity_app_prod.dashboards d on d.id = dr.dashboard_id
join bi_analytics.velocity_app_prod.users u on u.id = d.created_by_id
order by dr.ranking_trending



with t0 as (
    select *
    from crosschain.bronze.twitter_accounts
    qualify(
        row_number() over (partition by twitter_id order by updated_at desc) = 1
    )
), t1 as (
    select t0.*
    , e.value::string as ecosystem
    from t0
    , lateral flatten(
        input => ecosystems
    ) e
)
select *
from t1
order by updated_at desc, twitter_id, ecosystem




with t0a as (
    select u.id as user_id
    , u.username
    , case when u.created_at >= '2024-02-01' then 1 else 0 end as is_new
    , d.id as dashboard_id
    , coalesce(dr.ecosystem, '') as ecosystem
    , coalesce(dr.currency, '') as currency
    , d.title
    , coalesce(dr.dashboard_url, '') as dashboard_url
    , coalesce(dr.start_date_days_ago, 0) as start_date_days_ago
    , coalesce(dr.pct_twitter, 0) as pct_twitter
    , date_trunc('hour', dbt_updated_at) as hour
    , avg(hr.ranking_trending) as rk
    from bi_analytics.snapshots.hourly_dashboard_rankings hr
    left join bi_analytics.content_rankings.dashboard_rankings dr
        on dr.dashboard_id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where 1=1
        and dbt_updated_at >= '2023-01-24 15:00:00'
        and coalesce(u.role, '') <> 'internal'
        and not u.username in (
            'Polaris_9R','dsaber','flipsidecrypto','metricsdao','drethereum','Orion_9R','sam','forgash','nftchance__','danner'
        )
        and hr.ranking_trending <= 50
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
), imp as (
    select
    -- d.id as dashboard_id
    d.created_by_id as user_id
    , sum(impression_count) as impression_count
    , count(distinct t.conversation_id) as n_tweets
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    group by 1
), t0 as (
    select user_id
    , username
    , is_new
    , dashboard_id
    , ecosystem
    , currency
    , title
    , dashboard_url
    , start_date_days_ago
    , pct_twitter
    , hour
    , row_number() over (partition by hour order by rk, dashboard_id) as rk
    from t0a
    -- group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
), q as (
    select created_by_id as user_id
    , count(1) as n_queries
    , max(created_at)::date as most_recent_query_date
    from bi_analytics.velocity_app_prod.queries
    group by 1
), t1 as (
    select username
    , t0.user_id
    , is_new
    , coalesce(impression_count, 0) as impression_count
    , coalesce(n_tweets, 0) as n_tweets
    , coalesce(n_queries, 0) as n_queries
    , coalesce(most_recent_query_date, '2021-01-01') as recent_query_date
    , count(1) as n_hours_in_top_30
    from t0
    left join q
        on q.user_id = t0.user_id
    left join imp
        on imp.user_id = t0.user_id
    where rk <= 30
    group by 1, 2, 3, 4, 5, 6, 7
)
select *
, case when is_new = 1 or n_hours_in_top_30 < 50 then 1 else 0 end as is_in_silver_league
from t1


with t0 as (
    -- this CTE updated by kb
    select c.value:user_id::string as user_id
    , c.value:tag_type::string as tag_type
    , c.value:tag_name::string as tag_name
    , u.record_metadata:CreateTime::int as updated_at
    from crosschain.bronze.data_science_uploads u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'analyst-tag%'
        and tag_type = 'League'
    qualify (
        row_number() over (
            partition by user_id
            order by updated_at desc
        ) = 1
    )
)
select u.username
, t0.*
from t0
join bi_analytics.velocity_app_prod.users u
    on u.id = t0.user_id


select *
from solana.core.fact_events
where block_timestamp between '2023-12-14' and '2023-12-15'
    and tx_id = '3ago268gTdbjCrADZuhsu8xnnFM6dt9RuMmz6ugoA9eYWabqYk8FG8s5aAetbJ4sKkKzrkxWB8Dg3G5ryEb8mpWC'



with rk_hist0 as (
    select d.id as dashboard_id
    , date_trunc('hour', dbt_updated_at) as hour
    , avg(hr.ranking_trending) as rk0
    from bi_analytics.snapshots.hourly_dashboard_rankings hr
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where dbt_updated_at >= '2024-05-14 10:00:00'
        and dbt_updated_at <= '2024-05-15 01:00:00'
        and coalesce(u.role, '') <> 'internal'
        and not u.username in (
            'Polaris_9R','dsaber','flipsidecrypto','metricsdao','drethereum','Orion_9R','sam','forgash','nftchance__','danner'
        )
        and hr.ranking_trending <= 100
    group by 1, 2
), rk_hist1 as (
    select *
    , row_number() over (partition by hour order by rk0 desc) as rk
    from rk_hist0
), rk_hist2 as (
    select dashboard_id
    , min(rk) as top_ranking
    , sum(case when rk <= 40 then 1 else 0 end) as n_hours_in_top_40
    from rk_hist1
    group by 1
), t0 as (
    SELECT d.id as dashboard_id
    , t.id as tweet_id
    , t.impression_count
    , t.tweet_url
    , t.created_at::date as tweet_date
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
), t0d as (
    SELECT DISTINCT dashboard_id
    from t0
), t1 as (
    SELECT d.id as dashboard_id
    , case when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
        or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
        or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
        or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%'
        then 'Axelar' else INITCAP(t.name) end as chain
    , d.title
    , d.latest_slug
    , u.username
    , u.id as user_id
    , COUNT(DISTINCT q.id) as n_queries
    from bi_analytics.velocity_app_prod.dashboards d
    join t0d
        on t0d.dashboard_id = d.id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    join bi_analytics.velocity_app_prod._dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    join bi_analytics.velocity_app_prod._queries_to_tags qtt
        on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and t.type = 'project'
    group by 1, 2, 3, 4, 5, 6
), t2 as (
    SELECT *
    , row_number() over (
        partition by dashboard_id
        order by
        n_queries desc
        , case when chain in (
            'Avalanche'
            , 'Axelar'
            , 'Flow'
            , 'Near'
            , 'Osmosis'
            , 'Sei'
            , 'Solana'
            , 'Thorchain'
        ) then 1 when chain = 'Ethereum' then 2 else 3 end
        , chain
    ) as rn
    , SUM(n_queries) over (partition by dashboard_id) as tot_queries
    , n_queries / tot_queries as pct
    from t1
), tc0 as (
    SELECT t2.user_id
    , t2.username
    , t2.chain
    , SUM(pct) as tot_pct
    from t2
    join t0d
        on t0d.dashboard_id = t2.dashboard_id
    group by 1, 2, 3
), tc1 as (
    SELECT *
    , row_number() over (
        partition by user_id
        order by
        tot_pct desc
        , case when chain in (
            'Avalanche'
            , 'Axelar'
            , 'Flow'
            , 'Near'
            , 'Osmosis'
            , 'Sei'
            , 'Solana'
            , 'Thorchain'
        ) then 1 when chain = 'Ethereum' then 2 else 3 end
        , chain
    ) as rn
    , SUM(tot_pct) over (partition by user_id) as tot_pct_2
    , tot_pct / tot_pct_2 as pct
    from tc0
), tc as (
    SELECT tc1a.user_id
    , tc1a.username
    , CONCAT(
        tc1a.chain
        , case when tc1b.chain is null then '' else CONCAT(' + ', tc1b.chain) end
        , case when tc1c.chain is null then '' else CONCAT(' + ', tc1c.chain) end
    ) as user_chain
    from tc1 tc1a
    left join tc1 tc1b
        on tc1b.user_id = tc1a.user_id
        and tc1b.rn = 2
        and tc1b.pct > 0.25
    left join tc1 tc1c
        on tc1c.user_id = tc1a.user_id
        and tc1c.rn = 3
        and tc1c.pct > 0.25
    WHERE tc1a.rn = 1
), t3 as (
    SELECT t0.tweet_id
    , t0.impression_count
    , t0.tweet_url
    , t0.tweet_date
    , t2a.title
    , t2a.latest_slug
    , t2a.user_id
    , t2a.username
    , tc.user_chain
    , rh.top_ranking
    , rh.n_hours_in_top_40
    , dr.ranking_trending
    , CONCAT(
        t2a.chain
        -- , ''
        , case when t2b.chain is null then '' else CONCAT(' + ', t2b.chain) end
        , case when t2c.chain is null then '' else CONCAT(' + ', t2c.chain) end
    ) as chain
    from t0
    join t2 t2a
        on t2a.dashboard_id = t0.dashboard_id
        and t2a.rn = 1
    join tc
        on tc.user_id = t2a.user_id
    left join t2 t2b
        on t2b.dashboard_id = t0.dashboard_id
        and t2b.rn = 2
        and t2b.pct > 0.25
    left join t2 t2c
        on t2c.dashboard_id = t0.dashboard_id
        and t2c.rn = 3
        and t2c.pct > 0.25
    left join rk_hist2 rh
        on rh.dashboard_id = t0.dashboard_id
    left join bi_analytics.content_rankings.dashboard_rankings dr
        on dr.dashboard_id = t0.dashboard_id
)
SELECT *
, DATEDIFF('days', tweet_date, CURRENT_DATE) as days_ago
from t3


WITH league as (
  -- this CTE updated by kb
  select c.value:user_id::string as user_id
  , c.value:tag_type::string as tag_type
  , c.value:tag_name::string as tag_name
  , u.record_metadata:CreateTime::int as updated_at
  from crosschain.bronze.data_science_uploads u
  , lateral flatten(
      input => record_content
  ) c
  where record_metadata:key like 'analyst-tag%'
      and tag_type = 'League'
  qualify (
      row_number() over (
          partition by user_id
          order by updated_at desc
      ) = 1
  )
), gold_league as (
    -- this CTE updated by kb
    select *
    from league
    where tag_name = 'Gold League'
), chain0 as (
  SELECT
    d.id as dashboard_id,
    case
      when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
      or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
      or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
      or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%' then 'Axelar'
      else INITCAP(t.name)
    end as chain,
    COUNT(DISTINCT q.id) as n_queries
  from
    bi_analytics.velocity_app_prod.users u
    join bi_analytics.velocity_app_prod.dashboards d on d.created_by_id = u.id
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q on dtq.B = q.id
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t on qtt.B = t.id
    and t.type = 'project'
  group by
    1,
    2
),
chain as (
  SELECT
    *,
    row_number() over (
      partition by dashboard_id
      order by
        case
          when chain in (
            'Aptos',
            'Avalanche',
            'Axelar',
            'Flow',
            'Near',
            'Sei',
            'Solana'
          ) then 1
          else 2
        end,
        n_queries desc,
        chain
    ) as rn
  from
    chain0
),
t0 as (
  SELECT
    COALESCE(m.user_id, d.created_by_id) as user_id,
    d.profile_id as profile_id,
    COALESCE(tu.username, u.username) as username,
    dr.currency,
    d.title, -- updated by kb
    d.id as dashboard_id,
    d.created_at::date as dashboard_date, -- updated by kb
    p.type,
    COALESCE(c.chain, 'Polygon') as chain,
    row_number() over (
      order by
        dr.ranking_trending
    ) as current_rank,
    COALESCE(u.role, '') = 'internal'
    or u.username in (
      'Polaris_9R',
      'dsaber',
      'flipsidecrypto',
      'metricsdao',
      'drethereum',
      'Orion_9R',
      'sam',
      'forgash',
      'danner',
      'charliemarketplace',
      'theericstone',
      'sunslinger' -- updated by kb
    ) as internal_user
  from
    bi_analytics.content_rankings.dashboard_rankings dr
    join bi_analytics.velocity_app_prod.dashboards d on d.id = dr.dashboard_id
    left join bi_analytics.velocity_app_prod.profiles p on p.id = d.profile_id
    left join bi_analytics.velocity_app_prod.teams t on t.profile_id = p.id
    left join bi_analytics.velocity_app_prod.members m on t.id = m.team_id
    and m.role = 'owner'
    left join bi_analytics.velocity_app_prod.users tu
    on tu.id = m.user_id
    join bi_analytics.velocity_app_prod.users u on u.id = d.created_by_id
    left join chain c on c.dashboard_id = dr.dashboard_id
    and c.rn = 1
    left join gold_league l on l.user_id = COALESCE(m.user_id, d.created_by_id) -- updated by kb
  WHERE l.user_id is null -- updated by kb
)
SELECT
  t0.user_id,
  username,
  current_rank,-- updated by kb
  dashboard_id,
  title,-- updated by kb
  dashboard_date,-- updated by kb
  profile_id,
  type,
  case
    when chain in (
        -- we pay out aptos in usdc
        -- 'Aptos',
        'Avalanche',
        'Axelar',
        'Flow',
        'Near',
        'Sei',
        'Solana'
    ) then chain
    else 'Polygon'
  end as ecosystem,
  case
    when chain = 'Solana' then 'SOL'
    when chain = 'Avalanche' then 'AVAX'
    when chain = 'Axelar' then 'AXL'
    when chain = 'Flow' then 'FLOW'
    when chain = 'Near' then 'NEAR'
    when chain = 'Sei' then 'SEI'
    else 'USDC'
  end as currency,
  case
    when current_rank <= 30 then 1 -- updated by kb
    else 0 -- updated by kb
  end as base_amount,
  coalesce(ub.boost, 1) as user_boost,
  case
    when internal_user = false then base_amount * user_boost
    else 0
  end as amount
from
  t0
left join bi_analytics.silver.user_boost ub
    on ub.user_id = t0.user_id
    and ub.start_date <= current_timestamp
    and ub.end_date >= current_timestamp
WHERE
  current_rank <= 30
order by
  current_rank

select *
from solana.defi.fact_token_burn_actions
where block_timestamp >= '2022-12-20'
limit 10


SELECT block_id
, block_timestamp
, tx_id
, event_type
, instruction :parsed :info :mint :: STRING as mint,
COALESCE(
    instruction :parsed :info :amount :: INTEGER,
    instruction :parsed :info :tokenAmount: amount :: INTEGER
) as burn_amount,
COALESCE(
    instruction :parsed :info :authority :: string,
    instruction :parsed :info :multisigAuthority :: string
) as burn_authority,
COALESCE(
    instruction :parsed :info :signers :: string,
    signers :: string
) as signers
from solana.core.fact_events
where block_timestamp >= '2022-12-20'
    and succeeded
    and event_type in (
       'burn',
        'burnChecked'
    )

with base as (
    select max(block_timestamp) as recent_block
    from solana.core.fact_transactions
    where block_timestamp >= current_date - 7
), t0 as (
    select block_timestamp::date as date
    , sum(burn_amount) * pow(10, -5) as daily_burn_amount
    , sum(case when block_timestamp >= dateadd('minutes', -60 * 24, recent_block) then burn_amount else 0 end) as burn_amount_24h
    , sum(case when block_timestamp >= dateadd('minutes', -60 * 24 * 7, recent_block) then burn_amount else 0 end) as burn_amount_7d
    from solana.defi.fact_token_burn_actions
    join base
        on true
    where block_timestamp >= '2022-12-20'
        and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
    group by 1
), t1 as (
    select *
    , sum(daily_burn_amount) over (order by date) as cumu_burn_amount
    , pow(10, 14) as starting_supply
    , starting_supply - cumu_burn_amount as remaining_supply
    from t0
)
select *
from t1



select *
from solana.defi.fact_token_burn_actions
where block_timestamp >= '2022-12-20'
    and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
    and succeeded
order by burn_amount desc
limit 100


with t0 as (
    select recorded_hour::date as date
    , median(close) as sol_price
    from solana.price.ez_token_prices_hourly p
    where token_address = 'So11111111111111111111111111111111111111112'
        and close > 1
        and date >= '2023-09-01'
    group by 1
), t1 as (
    select dateadd('days', -3, date) as date
    , 3 as n_days
    , sol_price as next_sol_price
    from t0
    union
    select dateadd('days', -7, date) as date
    , 7 as n_days
    , sol_price as next_sol_price
    from t0
    union
    select dateadd('days', -14, date) as date
    , 14 as n_days
    , sol_price as next_sol_price
    from t0
    where date >= '2023-10-01'
), t2 as (
    select t0.*
    , t1.n_days
    , t1.next_sol_price
    from t0
    join t1
        on t1.date = t0.date
), t3 as (
    select *
    , (next_sol_price / sol_price) - 1 as pct_change
    , row_number() over (order by pct_change) as rn
    from t2
)
select *
from t3

with t0 as (
    select case when decoded_instruction:name::string = 'takeLoanV3' then decoded_instruction:accounts[0]:pubkey::string else decoded_instruction:accounts[5]:pubkey::string end as lender
    , count(1) as n_loans
    , sum(split(decoded_instruction:args:expectedLoan::string, ',')[1]::int) * power(10, -9) as volume
    from solana.core.fact_decoded_instructions
    where week >= current_date - 7
        and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
        and decoded_instruction:name::string in ('takeLoanV3','extendLoanV3')
    group by 1, 2
    -- order by 3 desc
), t1 as (
    -- select case when lender in ('7Wgz6LB4gkd7hr1hjyTur8tZSXC5sx1QYbYGQ7N2w5z7','ySoLLxJfRkecrD4wNL6NXmSw6P6fSmeR7tt1fn4Lqvw','runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC') then left(lender, 4) else 'other' end as lender
    select lender
    , left(lender, 4) as abbr
    , volume
    , row_number() over (order by volume desc) as rk
    from t0
    qualify(
        row_number() over (order by volume desc) <= 10
    )
)
select *
from t1
order by rk


with t0 as (
    select case when decoded_instruction:name::string = 'takeLoanV3' then decoded_instruction:accounts[0]:pubkey::string else decoded_instruction:accounts[5]:pubkey::string end as lender
    , date_trunc('week', block_timestamp) as week
    , count(1) as n_loans
    , sum(split(decoded_instruction:args:expectedLoan::string, ',')[1]::int) * power(10, -9) as volume
    from solana.core.fact_decoded_instructions
    where week >= '2023-01-01'
        and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
        and decoded_instruction:name::string in ('takeLoanV3','extendLoanV3')
    group by 1, 2
), t1 as (
    -- select case when lender in ('7Wgz6LB4gkd7hr1hjyTur8tZSXC5sx1QYbYGQ7N2w5z7','ySoLLxJfRkecrD4wNL6NXmSw6P6fSmeR7tt1fn4Lqvw','runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC') then left(lender, 4) else 'other' end as lender
    select case when n_loans >= 50 then 'bot' else 'human' end as account_type
    , week
    , sum(volume) as volume
    , sum(n_loans) as n_loans
    from t0
    group by 1
), t2 as (
    select *
    , sum(volume) over (partition by week) as tot_volume
    , sum(n_loans) over (partition by week) as tot_n_loans
    , round(volume * 100 / tot_volume, 1) as pct_volume
    , round(n_loans * 100 / tot_n_loans, 1) as pct_n_loans
    from t1
)
select *
from t2
order by week
 

select *
from solana.defi.mint_actions
limit 10

with likes as (
    select tweet_id
    , count(1) as n_likes
    from bi_analytics.twitter.likes
    group by 1
), tweets as (
    select t.*
    , coalesce(n_likes, 0) as n_likes
    from bi_analytics.twitter.tweet t
    left join likes l
        on (l.tweet_id = t.conversation_id or l.tweet_id = t.conversation_id)
)
select *
from tweets
where n_likes = 0
    and t.created_at >= current_date - 30
order by impression_count desc
limit 1000




select created_by_id as user_id
, u.username
, count(distinct q.id) as n_queries
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.users u
    on u.id = q.created_by_id
where q.created_at >= current_date - 30
group by 1, 2
order by 3 desc



select u.username
, a.user_id
from bi_analytics.silver.ambassador a
join bi_analytics.silver.users u
    on u.id = a.user_id



select event_type
, count(1)
from solana.core.fact_decoded_instructions i
where block_timestamp >= current_date - 7
    and program_id = 'JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp'
group by 1
order by 2 desc



with
  loans as (
    SELECT
      BLOCK_TIMESTAMP,
      tx_id,
      abs(post_balances[0] - pre_balances[0]) / 1e9 as amount,
        case when ARRAY_CONTAINS(
          PARSE_JSON(
            '{"pubkey":"JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp","signer":false,"source":"transaction","writable":false}'
          ),
          ACCOUNT_KEYS
        ) then 'CITRUS'
      end as PLATFORM
    from
      solana.core.fact_transactions
    WHERE
      BLOCK_TIMESTAMP >= current_date - 5
      and (
          ARRAY_CONTAINS(
          PARSE_JSON(
            '{"pubkey":"JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp","signer":false,"source":"transaction","writable":false}'
          ),
          ACCOUNT_KEYS
        )
      )
      and (
        array_contains(
          to_variant('Program log: Instruction: TakeLoan'),
          log_messages
        )
        or array_contains(
          to_variant('Program log: Instruction: Borrow'),
          log_messages
        )
        or array_contains(
          to_variant('Program log: Instruction: MigrateV2Loan'),
          log_messages
        )
        or array_contains(
          to_variant('Program log: Instruction: TakeLoanV3'),
          log_messages
        )
      )
      and succeeded = True
  ),
  reborrows_citrus_1 as (
    SELECT
      block_timestamp as loan_date,
      tx_id,
      inner_instructions[0]:instructions[1]:parsed:info:source as tx_to
    from
      solana.core.fact_transactions
    WHERE
      BLOCK_TIMESTAMP >= current_date - 5
      -- and BLOCK_TIMESTAMP < CURRENT_DATE
      and ARRAY_CONTAINS(
        PARSE_JSON(
          '{"pubkey":"JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp","signer":false,"source":"transaction","writable":false}'
        ),
        ACCOUNT_KEYS
      )
      and array_contains(
        to_variant('Program log: Instruction: Reborrow'),
        log_messages
      )
      and succeeded = True
  ),
  reborrows_citrus_2 as (
    SELECT
      loan_date,
      tx_id,
      TX_TO,
      amount
    from
      solana.core.fact_transfers
      join reborrows_citrus_1 using (tx_to)
    WHERE
      BLOCK_TIMESTAMP >= (
        CURRENT_DATE - 15
      )
      and mint = 'So11111111111111111111111111111111111111112'
  ),
  reborrows_citrus_3 as (
    SELECT
      tx_to,
      max(amount) as amount
    from
      reborrows_citrus_2
    group by
      1
  ),
  reborrows_citrus_4 as (
    select
      loan_date,
      tx_id,
      amount,
      'CITRUS' as platform
    from
      reborrows_citrus_2
      join reborrows_citrus_3 using (tx_to, amount)
  ),
  total as (
    SELECT
      *
    from
      loans
    UNION ALL
    SELECT
      *
    from
      reborrows_citrus_4
  )
SELECT
*
from
  total


-- 7MyTjmRygJoCuDBUtAuSugiYZFULD2SWaoUTmtjtRDzD bern
-- 7MyTjmRygJoCuDBUtAuSugiYZFULD2SWaoUTmtjtRDzD bern

with t0 as (
    select burn_authority
    , sum(burn_amount) * pow(10, -5) as amt
    , count(distinct tx_id) as n_tx
    from solana.defi.fact_token_burn_actions
    where block_timestamp >= '2022-12-20'
        and mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
        and succeeded
    group by 1
    order by 2 desc
), t1 as (
    select t0.*
    , l.label
    from t0
    left join solana.core.dim_labels l
        on l.address = t0.burn_authority
)
select *
from t1
order by amt desc



with t0 as (
    select 
    i.*
    , c.*
    , i.signers[0]::string as signer_0
    , i.signers[1]::string as signer_1
    , i.signers[2]::string as signer_2
    from solana.core.fact_decoded_instructions i
    , lateral flatten (
        input => decoded_instruction:accounts
    ) c
    where block_timestamp >= current_date - 1
        and program_id = 'JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp'
    limit 10000
), t1 as (
    select *
    from t0
    where value:name = 'loanAccount'
)
select *
from t1


with loans as (
    select distinct tx_id
    , c.value:pubkey as loanAccount
    from solana.core.fact_decoded_instructions i
    , lateral flatten (
        input => decoded_instruction:accounts
    ) c
    where block_timestamp >= current_date - 5
        and program_id = 'JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp'
), t0 as (
    select 
    i.*
    , c.*
    , l.loanAccount
    , i.signers[0]::string as signer_0
    , i.signers[1]::string as signer_1
    , i.signers[2]::string as signer_2
    -- distinct tx_id, event_type
    -- case when decoded_instruction:name::string = 'takeLoanV3' then decoded_instruction:accounts[0]:pubkey::string else decoded_instruction:accounts[5]:pubkey::string end as lender
    -- , date_trunc('week', block_timestamp) as week
    -- , count(1) as n_loans
    -- , sum(split(decoded_instruction:args:expectedLoan::string, ',')[1]::int) * power(10, -9) as volume
    from solana.core.fact_decoded_instructions i
    join loans l
        on l.tx_id = i.tx_id
    , lateral flatten (
        input => decoded_instruction
    ) c
    where block_timestamp >= current_date - 5
        and program_id = 'JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp'
        -- and decoded_instruction:name::string in ('takeLoanV3','extendLoanV3')
    -- group by 1, 2
        -- and key = 'name'
        -- and event_type not in ('offerLoan','cancelOffer','delistCollateral','requestLoan','repay','repay2','resetPool','cancelRequest')
        -- and event_type in ('borrow','reborrow','renew')
    -- order by i.block_timestamp, i.tx_id, c.path, c.key
    limit 10000
), terms as (
    select tx_id
    , event_type as t_event_type
    , signer_0
    , signer_1
    , signer_2
    , decoded_instruction
    , value as args
    , loanAccount
    , value:terms:apyBps::int as apyBps
    , value:terms:duration::int as duration
    , value:terms:principal::int * power(10, -9) as principal
    from t0
    where key = 'args'
        and event_type = 'offerLoan'
        and signer_0 = 'kcitfabbdz4KFNPn8RJjUEg1rD36SJdFwJg8imo2Rrg'
), mints as (
    select tx_id
    , loanAccount
    , event_type as m_event_type
    , value[0]:pubkey::string as loanAccount
    , value[1]:pubkey::string as lendAuthority
    , value[2]:pubkey::string as borrowAuthority
    , value[3]:pubkey::string as collectionConfig
    , value[4]:pubkey::string as borrower
    , value[5]:pubkey::string as lender
    , value[6]:pubkey::string as mint
    from t0
    where key = 'accounts'
)
select t.*
, m.*
, coalesce(l.label, 'other') as collection
from terms t
join mints m
    on m.tx_id = t.tx_id
left join solana.core.dim_labels l
    on l.address = m.mint


select * 
from solana.core.fact_decoded_instructions i
-- , lateral flatten (
--     input => decoded_instruction
-- ) c
where block_timestamp >= current_date - 10
    and program_id = 'JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp'
    and tx_id = 'FgWGa4FMK7AM8VQCWtHKEjgYjRc1XEyYbwUFj3vjJ7x5CzLRrbdnYYrnUveDYBT6WAtZDgFfo4uk8mz7yb7Fi7Y'

with t0 as (
    select i.tx_id
    , i.block_timestamp
    , i.decoded_instruction
    , i.decoded_instruction:name::string as name
    , di.value:pubkey::string as borrower
    , i.decoded_instruction:args:floor::int as floor
    , i.decoded_instruction:args:terms:apyBps::int as apyBps
    , i.decoded_instruction:args:terms:duration::int as duration
    , i.decoded_instruction:args:terms:principal::int * pow(10, -9) as principal
    from solana.core.fact_decoded_instructions i
    , lateral flatten(input => decoded_instruction:accounts) as di
    where i.block_timestamp >= current_date - 3
        and di.value:name::string = 'borrower'
        and i.program_id = 'JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp'
        and borrower is not null
), t1 as (
    select t0.*
    , di.value:pubkey::string as lender
    from t0
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = 'lender'
        and lender is not null
), t2 as (
    select t1.*
    , di.value:pubkey::string as lendAuthority
    from t1
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = 'lendAuthority'
        and lendAuthority is not null
), t3 as (
    select distinct tx_id
    , borrower
    , lendAuthority
    from t2
), t4 as (
    select t3.tx_id
    , sum(amount) as xfer_amt
    , count(amount) as n_xfers
    from t3
    join solana.core.fact_transfers t
        on t.tx_id = t3.tx_id
    where t.block_timestamp >= current_date - 3
        and t.tx_to = t3.borrower
        and t.tx_from = t3.lendAuthority
        and t.mint = 'So11111111111111111111111111111111111111112'
    group by 1
)
select t2.*
, t4.xfer_amt
, t4.n_xfers
from t2
left join t4
    on t4.tx_id = t2.tx_id
order by t4.xfer_amt desc



, t2 as (
    select t1.*
    , di.value:accounts:pubkey::string as lender
    from t1
    , lateral flatten(input => parsed_json:decoded_instruction:accounts) as di
    where di.value:accounts:name::string = 'lender'
)
  "args": {
    "floor": "102900000000",
    "merkleData": null,
    "terms": {
      "apyBps": "8000",
      "duration": "604800",
      "principal": "0"
    }


select 
    di1.value:accounts:pubkey::string as borrower_pubkey,
    di2.value:accounts:pubkey::string as lender_pubkey,
    di3.value:args:terms:apyBps::string as apyBps,
    di3.value:args:terms:duration::string as duration
from 
    solana.core.fact_decoded_instructions i
    , lateral flatten(input => parsed_json:decoded_instruction:accounts) as di1
    , lateral flatten(input => parsed_json:decoded_instruction:accounts) as di2
    , lateral flatten(input => parsed_json:decoded_instruction:args:terms) as di3
WHERE
    block_timestamp >= current_date - 30
    and di1.value:accounts:name::string = 'borrower'
    and di2.value:accounts:name::string = 'lender'
    and di2.value:accounts:pubkey::string = 'kcitfabbdz4KFNPn8RJjUEg1rD36SJdFwJg8imo2Rrg'

{
  "accounts": [
    {
      "name": "loanAccount",
      "pubkey": "2Un7oS4FYFtpgLgmnP9svmZzodVsifem11Rwd2DZWBk8"
    },
    {
      "name": "lendAuthority",
      "pubkey": "E2Tu2YzLw8ESPCZrmwGz9oSoQpMJvjGcVqdft82zYLrR"
    },
    {
      "name": "borrowAuthority",
      "pubkey": "4Pg519pUtpnvMHEYMCzo81eLbpUVukbk41H95YES36Bs"
    },
    {
      "name": "collectionConfig",
      "pubkey": "FEfa1MYtVdaVbPpCwfGxovvjdrp92fRPeWgxzWc19ZQA"
    },
    {
      "name": "borrower",
      "pubkey": "GbciBossuDsyWw6D8SYUg6WEZzuYzvLNgPPWRoqRqRPA"
    },
    {
      "name": "lender",
      "pubkey": "kcitfabbdz4KFNPn8RJjUEg1rD36SJdFwJg8imo2Rrg"
    },
    {
      "name": "mint",
      "pubkey": "91zRUUVNwfpHF6d3USPCPB6drVNNqeMGsfAeEh5gSPd1"
    },
    {
      "name": "tokenAccount",
      "pubkey": "5wt4CB574uvEMDUJ8jJSnDft4ZNTT9sKfZ5QZbL164Yt"
    },
    {
      "name": "masterEdition",
      "pubkey": "CzfNDQBNyPgNJ7mXUxkVEPP3NhzKQvvemRa38Q37pRao"
    },
    {
      "name": "metadata",
      "pubkey": "D4gc7q62gXUEx67iMsvek8LSxTMendnDnnCqbXRSVHrc"
    },
    {
      "name": "tokenRecord",
      "pubkey": "8mFDghYsFbdwVfxSZe5rhc4Y7GvuG4zeHQbP7cdDh9R3"
    },
    {
      "name": "tokenProgram",
      "pubkey": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    },
    {
      "name": "systemProgram",
      "pubkey": "11111111111111111111111111111111"
    },
    {
      "name": "tokenMetadataProgram",
      "pubkey": "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"
    },
    {
      "name": "sysvarInstructions",
      "pubkey": "Sysvar1nstructions1111111111111111111111111"
    },
    {
      "name": "authorizationRulesProgram",
      "pubkey": "auth9SigNpDKz4sJJ1DfCTuZrZNSAgh9sFD3rboVmgg"
    },
    {
      "name": "rules",
      "pubkey": "eBJLFYPxJmMGKuFwpDWkzxZeUrad92kZRC5BJLpzyT9"
    },
    {
      "name": "fpAuthority",
      "pubkey": "FiyCTJ4hY7NxfDbyZxmSLdFXpXg4bTsM1k21bJFDjhj4"
    }
  ],
  "args": {
    "floor": "102900000000",
    "merkleData": null,
    "terms": {
      "apyBps": "8000",
      "duration": "604800",
      "principal": "0"
    }
  },
  "name": "borrow",
  "program": "JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp"
}


select close as bonk_price
from solana.price.ez_token_prices_hourly p
where token_address = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
qualify(
    row_number() over (order by recorded_hour desc) = 1
)


with t0 as (
    select date_trunc('month', t.created_at)::date as tweet_month
    , d.id as dashboard_id
    , t.impression_count
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    qualify(
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
)
select tweet_month\
, count(distinct dashboard_id) as n_dashboards
, sum(impression_count) as impression_count
from t0
group by 1
order by 1



with t0 as (
    select date_trunc('month', t.created_at)::date as tweet_month
    , d.id as dashboard_id
    , t.impression_count
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    qualify(
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
), t1 as (
    select tweet_month
    , count(distinct dashboard_id) as n_dashboards
    , sum(impression_count) as impression_count
    from t0
    group by 1
), t2 as (
    select *
    , avg(n_dashboards) over (order by tweet_month rows between 2 preceding and current row) as rolling_avg_n_dashboards
    , avg(impression_count) over (order by tweet_month rows between 2 preceding and current row) as rolling_avg_impression_count
    from t1
)
select *
from t2
order by month


with t0 as (
    select l.user_id as liker_id
    , t.id as tweet_id
    , date_trunc('week', t.created_at)::date as week
    , row_number() over (partition by l.user_id order by t.created_at) as rn
    , t.impression_count
    , count(1) over (partition by tweet_id) as tot_likes
    from bi_analytics.twitter.likes l
    join bi_analytics.twitter.tweet t
        on t.id = l.tweet_id
    where l.user_followers >= 200
    qualify(
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
), t1 as (
    select week
    , case when rn <= 4 then 'New Audience'
        -- when rn <= 9 then 'Medium'
        else 'Existing Audience' end as audience
    , count(1) as n_likes
    , sum(impression_count / tot_likes) as n_impressions
    from t0
    group by 1, 2
), t2 as (
    select *
    , sum(n_likes) over (partition by week) as tot_likes
    , n_likes / tot_likes as pct_audience
    from t1
)
select *
from t2
order by week, audience


select *
from bi_analytics.silver.user_bans
limit 100

select d.title
, d.id
, dr.ranking_trending
, u.username
from bi_analytics.silver.dashboard_bans db
join bi_analytics.velocity_app_prod.dashboards d
    on db.dashboard_id = d.id
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
left join bi_analytics.content_rankings.dashboard_rankings dr
    on dr.dashboard_id = d.id
limit 1000


select d.title
, d.id as dashboard_id
, d.latest_slug
, u.username
from bi_analytics.silver.dashboard_bans db
join bi_analytics.velocity_app_prod.dashboards d
    on db.dashboard_id = d.id
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
limit 100

select *
from bi_analytics.velocity_app_prod.users u
limit 10


with t0 as (
    select i.tx_id
    , i.block_timestamp::date as date
    , i.decoded_instruction
    , i.decoded_instruction:name::string as name
    , di.value:pubkey::string as borrower
    , i.decoded_instruction:args:floor::int as floor
    , i.decoded_instruction:args:terms:apyBps::int as apyBps
    , i.decoded_instruction:args:terms:duration::int as duration
    , dateadd('seconds', duration, i.block_timestamp) as due_date
    , case when due_date < current_timestamp then 1 else 0 end as is_due
    , i.decoded_instruction:args:terms:principal::int * pow(10, -9) as principal
    from solana.core.fact_decoded_instructions i
    , lateral flatten(input => decoded_instruction:accounts) as di
    -- where i.block_timestamp >= current_date - 30
    where i.block_timestamp >= '2024-05-01'
        -- and i.block_timestamp <= '2024-04-19'
        and i.program_id = 'JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp'
        -- and tx_id = '3SjtWGFx27DY1ZcJL1VpbRGVDqTkAPPfwSuE68LabpvGGSYXFCAyHvt2UYM3gbyy6LL2Mn3gw2mpecsvUYVtr3vc'
        and di.value:name::string = 'borrower'
        and borrower is not null
)
-- select *
-- from t0
-- where borrower = 'zPgpXRyW4VtDgv5SCwNMXF8UKP1DwkXuhk6tPFKXNkS'

, t1 as (
    select t0.*
    , di.value:pubkey::string as loanAccount
    from t0
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = case when name = 'reborrow' then 'newLoanAccount' else 'loanAccount' end
        and loanAccount is not null
)
-- select * from t1
, repaid as (
    select distinct loanAccount
    from t1
    where name in ('repay','repay2','sellRepay')
)
, t1b as (
    select t1.*
    , di.value:pubkey::string as lender
    from t1
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = case when name = 'reborrow' then 'newLender' else 'lender' end
        and lender is not null
)
-- select *
-- from t1b

, t2 as (
    select t1b.*
    , di.value:pubkey::string as lendAuthority
    from t1b
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = case when name = 'reborrow' then 'newLendAuthority' else 'lendAuthority' end
        and lendAuthority is not null
)
, t3 as (
    select distinct tx_id
    , borrower
    , lendAuthority
    from t2
), t4 as (
    select t3.tx_id
    , sum(amount) as xfer_amt
    , count(amount) as n_xfers
    from t3
    join solana.core.fact_transfers t
        on t.tx_id = t3.tx_id
    where t.block_timestamp >= '2024-04-01'
    -- where t.block_timestamp >= current_date - 30
        and t.tx_to = t3.borrower
        and t.tx_from = t3.lendAuthority
        and t.mint = 'So11111111111111111111111111111111111111112'
    group by 1
), t5 as (
    select t2.*
    , t4.xfer_amt as loan_amt
    , t4.n_xfers
    , r.loanAccount as repaidLoanAccount
    , case when r.loanAccount is null then 0 else 1 end as is_repaid
    , greatest(is_repaid, is_due) * loan_amt as loan_due_amount
    , is_repaid * loan_amt as loan_repaid_amount
    from t2
    join t4
        on t4.tx_id = t2.tx_id
    left join repaid r
        on r.loanAccount = t2.loanAccount
)
-- select *
-- from t5
-- where lender = 'kcitfabbdz4KFNPn8RJjUEg1rD36SJdFwJg8imo2Rrg'
select lender
, count(1) as n_loans
, sum(loan_amt) as loan_amt
, sum(loan_due_amount) as loan_due_amount
, sum(loan_repaid_amount) as loan_repaid_amount
from t5
group by 1
order by 3 desc




select *
from bi_analytics.bronze.data_science_uploads
where record_metadata:key like 'twitter-missing%'
limit 100


select *
from bi_analytics.twitter.tweet t
where conversation_id == '1788629341787627893'



SELECT d.id as dashboard_id
, t.id as tweet_id
, t.conversation_id::string as conversation_id
, coalesce(d.title, t.tweet_type) as title
, coalesce(d.latest_slug, t.clean_url) as latest_slug
, coalesce(tm.slug, u.username, t.tweet_type) as username
, u.id as user_id
, t.impression_count
, case when u.user_name is null then t.tweet_url else CONCAT('https://twitter.com/',u.user_name,'/status/',t.id) end as tweet_url
, t.created_at::date as tweet_date
from bi_analytics.twitter.tweet t
left join bi_analytics.velocity_app_prod.dashboards d
    on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
left join bi_analytics.twitter.user tu
    on tu.id = t.user_id
left join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
left join bi_analytics.velocity_app_prod.profiles p
    on p.id = d.profile_id
left join bi_analytics.velocity_app_prod.teams tm
    on tm.profile_id = p.id
WHERE conversation_id == '1788629341787627893' and NOT coalesce(d.id, '') in (
    SELECT dashboard_id from labels WHERE dashboard_tag = 'bot'
) and (d.id is not null or t.tweet_type = 'Flipside Science Dashboard')
QUALIFY(
    row_number() over (partition by t.conversation_id order by impression_count desc) = 1
    and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
)

with t0 as (
    select *
    from flipside_prod_db.bronze.prod_address_label_sink_291098491
    where record_metadata:topic::string = 'twitter-tweet'
)
, t1 as (
    SELECT t0._inserted_timestamp
    , c.value:id::string as id
    , c.value:user_id::string as user_id
    , COALESCE(c.value:user_followers::int, 0) as user_followers
    , c.value:conversation_id::int as conversation_id
    , coalesce(c.value:created_at::datetime, t0._inserted_timestamp) as created_at
    , COALESCE(c.value:like_count::int, 0) as like_count
    , COALESCE(c.value:impression_count::int, 0) as impression_count
    , COALESCE(c.value:rewteet_count::int, 0) as rewteet_count
    , COALESCE(c.value:quote_count::int, 0) as quote_count
    , c.value:tweet_type::string as tweet_type
    , c.value:clean_url::string as clean_url
    , c.value:platform::string as platform
    , c.value:tweet_url::string as tweet_url
    from t0
    , LATERAL FLATTEN(
        input => record_content
    ) c
    WHERE user_id is NOT null
)
select *
from t1
where conversation_id = '1788629341787627893'

select s.tx_id
, s.purchaser
, s.sales_amount
, l.label as collection
from solana.nft.fact_nft_sales s
left join solana.core.dim_labels l
    on l.address = s.mint
order by sales_amount desc
limit 1000

with t0 as (
    select date_trunc('month', BLOCK_TIMESTAMP) as month
    , count (distinct tx_id) as n_sales
    , count (distinct purchaser) as n_buyers
    , count (distinct seller) as n_sellers
    , count (distinct mint) as n_mints
    , sum(sales_amount) as volume
    , avg(sales_amount) as avg_price
    , median(sales_amount) as median_price
    , count(distinct block_timestamp::date) as n_days
    from solana.nft.fact_nft_sales s
    where block_timestamp::date < current_date
        and succeeded = 'TRUE'
    group by 1
)
select *
, volume / n_days as avg_daily_volume
from t0


with t0 as (
    select date_trunc('day', BLOCK_TIMESTAMP) as date
    , count (distinct tx_id) as n_sales
    , count (distinct purchaser) as n_buyers
    , count (distinct seller) as n_sellers
    , count (distinct mint) as n_mints
    , sum(sales_amount) as volume
    , avg(sales_amount) as avg_price
    , median(sales_amount) as median_price
    , count(distinct block_timestamp::date) as n_days
    from solana.nft.fact_nft_sales s
    where block_timestamp::date < current_date
        and succeeded = 'TRUE'
        and sales_amount < 1000
    group by 1
), t1 as (
    select *
    , avg(volume) over (
        order by date
        rows between 29 preceding and current row
    ) as rolling_avg_30_days
    , avg(volume) over (
        order by date
        rows between 7 preceding and current row
    ) as rolling_avg_7_days
    from t0
)
select *
from t1




with t0 as (
	SELECT conversation_id
	, MIN(start_timestamp) as start_timestamp
    from bi_analytics.twitter.tweet
    where created_at >= '2024-06-10'
        and created_at <= '2024-06-12'
	group by 1
), t1 as (
	SELECT dashboard_id
    , count(distinct date_trunc('hour', dbt_updated_at)) as n_hours_in_top_40
	from bi_analytics.snapshots.hourly_dashboard_rankings hr
    where dbt_updated_at >= '2024-06-11'
        and dbt_updated_at <= '2024-06-12'
        and ranking_trending <= 40
	group by 1
)
select d.id
, u.username
, d.title
, d.created_at
, case when t0.start_timestamp >= '2024-05-15'  
    and t0.start_timestamp <= '2024-05-21' then 1 else 0 end as is_missing
, dr.ranking_trending
, t.conversation_id as missing_conversation_id
, t0.start_timestamp
, coalesce(t1.n_hours_in_top_40, 0) as n_hours_in_top_40
, t.*
from bi_analytics.content_rankings.dashboard_rankings dr
join bi_analytics.velocity_app_prod.dashboards d
    on d.id = dr.dashboard_id
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
-- left join bi_analytics.twitter.missing_tweets mt
--     on mt.dashboard_id = dr.dashboard_id
join bi_analytics.twitter.tweet t
    on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
join t0
    on t0.conversation_id = t.conversation_id
left join t1
    on t1.dashboard_id = d.id
where t.created_at >= '2024-06-10'
    and t.created_at <= '2024-06-12'
qualify(
    row_number() over (partition by d.id order by t.created_at desc) <= 5
)
order by dr.ranking_trending
limit 1000


select u.username
, d.title
, d.created_at::date as date
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
where d.created_at between '2024-05-15' and '2024-05-20'

select *
from solana.nft.fact_nft_sales
where block_timestamp >= current_date and marketplace ilike '%tensor%'
limit 10




with labels as (
    select distinct d.id as dashboard_id
    , d.title
    , d.latest_slug
    , concat('https://flipsidecrypto.xyz/', u.username, '/', d.latest_slug) as dashboard_url
    , u.username
    , case when t.name = 'aurora' then 'near' when t.name in (
        'aptos'
        , 'avalanche'
        , 'axelar'
        , 'base'
        , 'blast'
        , 'flow'
        , 'near'
        , 'sei'
        , 'solana'
        , 'thorchain'
        , 'vertex'
    ) then t.name else 'other' end as project
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    left join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
        on q.id = qtt.A
    left join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and (t.name = 'vertex' or t.type = 'project')
    where t.name is not null
), t0 as (
    select date_trunc('week', t.created_at)::date as week
    , l.project
    , t.conversation_id
    , d.id as dashboard_id
    , t.impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    join labels l
        on l.dashboard_id = d.id
    where week >= dateadd('month', -6, current_timestamp)
    qualify(
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
), t1 as (
    select week
    , project
    , sum(impression_count) as impression_count
    from t0
    group by 1, 2
)
select *
, sum(impression_count) over (partition by project order by week) as cumu_impression_count
, sum(1) over (partition by project order by week) as cumu_dashboards
from t1

select *
from bi_analytics.twitter.user u
limit 100






with league as (
  -- this CTE updated by kb
  select c.value:user_id::string as user_id
  , c.value:tag_type::string as tag_type
  , c.value:tag_name::string as tag_name
  , u.record_metadata:CreateTime::int as updated_at
  from crosschain.bronze.data_science_uploads u
  , lateral flatten(
      input => record_content
  ) c
  where record_metadata:key like 'analyst-tag%'
      and tag_type = 'League'
  qualify (
      row_number() over (
          partition by user_id
          order by updated_at desc
      ) = 1
  )
), gold_league as (
    -- this CTE updated by kb
    select *
    from league
    where tag_name = 'Gold League'
), rk_hist0 as (
    select d.id as dashboard_id
    , date_trunc('hour', dbt_updated_at) as hour
    , case when dbt_updated_at <= '2024-05-01 10:00:00' or gl.user_id is not null then 'Gold' else 'Silver' end as league
    , avg(hr.ranking_trending) as rk0
    from bi_analytics.snapshots.hourly_dashboard_rankings hr
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    left join gold_league gl
        on gl.user_id = u.id
    where dbt_updated_at >= '2024-02-20 10:00:00'
        -- and dbt_updated_at <= '2024-05-15 01:00:00'
        and coalesce(u.role, '') <> 'internal'
        and not u.username in (
            'Polaris_9R','dsaber','flipsidecrypto','metricsdao','drethereum','Orion_9R','sam','forgash','nftchance__','danner'
        )
        and hr.ranking_trending <= 500
    group by 1, 2, 3
), labels as (
    select distinct d.id as dashboard_id
    , d.title
    , d.latest_slug
    , concat('https://flipsidecrypto.xyz/', u.username, '/', d.latest_slug) as dashboard_url
    , u.username
    , u.id as user_id
    , case when (
        q.statement like '%VoTpe3tHQ7AjQHMapgSue2HJFAh2cGsdokqN3XqmVSj%'
        or q.statement like '%GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY%'
    ) then 'jupiter' else t.name end as project
    , case when project in ('near'
        , 'solana'
        , 'aptos'
        , 'flow'
        , 'blast'
        , 'sei'
        , 'axelar'
        , 'avalanche'
        , 'vertex'
        , 'thorchain'
        , 'jupiter'
    ) then project else 'non-partner' end as partner_name
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    left join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
        on q.id = qtt.A
    left join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and (t.name = 'vertex' or t.type = 'project')
    where t.name is not null
        or q.statement like '%VoTpe3tHQ7AjQHMapgSue2HJFAh2cGsdokqN3XqmVSj%'
        or q.statement like '%GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY%'
)
, boosts as (
    select 'Near Feb 23' as name, 'near' as project, '2024-02-23 19:00:00' as start_hour, 24 * 3 as n_hours, 30 as top_n, 2 as mult
    union select 'Blast Mar 1' as name, 'blast' as project, '2024-05-01 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    -- union select 'Jup Mar 7' as name, 'jupiter' as project, '2024-03-07 21:00:00' as start_hour, 24 * 1 as n_hours, 40 as top_n, 9 as mult
    union select 'Vertex Mar 14' as name, 'vertex' as project, '2024-03-14 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Near Mar 18' as name, 'near' as project, '2024-03-18 15:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Aptos Mar 19' as name, 'aptos' as project, '2024-03-19 21:00:00' as start_hour, 24 * 1 as n_hours, 40 as top_n, 2 as mult
    union select 'Near Mar 24' as name, 'near' as project, '2024-03-24 21:00:00' as start_hour, 24 * 5 as n_hours, 40 as top_n, 4 as mult
    union select 'Near Mar 24' as name, 'near' as project, '2024-03-24 21:00:00' as start_hour, 24 * 5 as n_hours, 40 as top_n, 4 as mult
    union select 'Aptos Apr 10' as name, 'aptos' as project, '2024-04-10 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 2 as mult
    union select 'Vertex Apr 12' as name, 'vertex' as project, '2024-04-12 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Sei Apr 15' as name, 'sei' as project, '2024-04-15 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 2 as mult
    union select 'Axelar Apr 17' as name, 'axelar' as project, '2024-04-17 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 4 as mult
    union select 'Aptos Apr 24' as name, 'aptos' as project, '2024-04-24 17:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Axelar Apr 27' as name, 'axelar' as project, '2024-04-27 17:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Blast May 3' as name, 'blast' as project, '2024-05-03 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Avalanche May 6' as name, 'avalanche' as project, '2024-05-06 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Flow May 9' as name, 'flow' as project, '2024-05-09 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 4 as mult
    union select 'Near May 12' as name, 'near' as project, '2024-05-12 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 2 as mult
    union select 'Near May 14' as name, 'near' as project, '2024-05-14 16:00:00' as start_hour, 24 * 1 as n_hours, 40 as top_n, 4 as mult
    union select 'Aptos May 15' as name, 'aptos' as project, '2024-05-15 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Aptos May 20' as name, 'aptos' as project, '2024-05-20 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
)
, twitter as (
    select d.dashboard_id
    , d.title
    , d.dashboard_url
    , d.username
    , d.project
    , d.partner_name
    , t.created_at::date as tweet_date
    , t.tweet_url
    , t.conversation_id
    , t.impression_count
    , least(10000, t.impression_count) as impression_count_10k_cap
    , b.name
    , b.n_hours
    , b.mult
    from labels d
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    left join boosts b
        on b.project = d.project
        and t.created_at >= dateadd('hours', -12, b.start_hour)
        and t.created_at <= dateadd('hours', 2 + b.n_hours, b.start_hour)
    qualify(
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
)
, calendar as (
    select distinct tweet_date
    from twitter
)
, project as (
    select distinct project
    from twitter
)
, calendar_partner_boost as (
    select distinct tweet_date
    , project
    , 1 as active_boost
    , mult
    , n_hours
    from twitter
    where name is not null
)
, calendar_partner as (
    select c.tweet_date
    , p.project
    , coalesce(b.active_boost, 0) as active_boost
    , coalesce(b.mult, 0) as mult
    , coalesce(b.n_hours, 0) as n_hours
    , sum(coalesce(t.impression_count, 0)) as impression_count
    , sum(coalesce(t.impression_count_10k_cap, 0)) as impression_count_10k_cap
    , count(distinct t.conversation_id) as n_tweets
    , count(distinct t.dashboard_id) as n_dashboards
    from calendar c
    join project p
        on true
    left join calendar_partner_boost b
        on b.tweet_date = c.tweet_date
        and b.project = p.project
    left join twitter t
        on t.tweet_date = c.tweet_date
        and t.project = p.project
    group by 1, 2, 3, 4, 5
)
, baseline as (
    select project
    , avg(impression_count) as impressions_baseline
    , avg(impression_count_10k_cap) as impression_count_10k_cap
    , avg(n_tweets) as n_tweets_baseline
    , avg(n_dashboards) as n_dashboards_baseline
    from calendar_partner
    where tweet_date >= '2024-02-01'
    group by 1
)
-- select *
-- from calendar_partner
-- where tweet_date >= '2024-02-01'
-- order by tweet_date desc, partner_name
-- select project
-- , sum(1) 
-- select title, dashboard_url, username, count(1) as n_tweets, sum(impression_count) as impression_count from twitter
-- select *
-- where name = 'Avalanche May 6'
-- group by 1, 2, 3
, rk_hist1 as (
    select *
    , row_number() over (partition by hour, league order by rk0) as rk
    from rk_hist0
)
, t0 as (
    select r.dashboard_id
    , r.hour
    , r.hour::date as topn_date
    , r.rk
    , r.league
    , l.user_id
    , case
        when league = 'Gold' and r.rk <= 10 then 1.5
        when league = 'Gold' and r.rk <= 40 then 1
        when coalesce(league, 'Silver') = 'Silver' and r.rk <= 30 then 0.5
        else 0 end as base_amount
    , l.title
    , l.username
    , l.project
    , b.name
    , b.start_hour::date as date
    , b.n_hours
    , b.mult
    , case when coalesce(league, 'Silver') = 'Silver' then 0 else coalesce(b.mult, 0) * base_amount end as boost_amount
    from rk_hist1 r
    join labels l
        on l.dashboard_id = r.dashboard_id
    join boosts b
        on b.project = l.project
        and r.hour >= b.start_hour
        and r.hour < dateadd('hours', b.n_hours, b.start_hour)
    where r.rk <= 40
    qualify(
        row_number() over (partition by r.dashboard_id, r.league, r.hour order by coalesce(b.mult, 0) desc) = 1
    )
)
-- select user_id, sum(base_amount + boost_amount) as total_amount
-- from t0
-- group by 1
-- order by 2 desc


-- select * from t0 where name = 'Blast May 3'
, cost as (
    select name
    , project
    , date
    , (n_hours / 24)::int as n_days
    , mult + 1 as boost
    , sum(base_amount) as base_amount
    , sum(boost_amount) as boost_amount
    from t0
    group by 1, 2, 3, 4, 5
), impressions as (
    select name
    , sum(impression_count) as impression_count
    , sum(impression_count_10k_cap) as impression_count_10k_cap
    , count(1) as n_tweets
    , count(distinct dashboard_id) as n_dashboards
    from twitter
    group by 1
)
-- select project
-- , league
-- , sum(base_amount)
-- from cost
-- group by 1
-- order by 2 desc
, t1 as (
    select c.*
    , c.base_amount + c.boost_amount as total_paid
    , i.impression_count
    , i.n_tweets
    , i.n_dashboards
    , b.impressions_baseline
    , b.impression_count_10k_cap
    , b.n_tweets_baseline
    , b.n_dashboards_baseline
    , i.impression_count - (b.impressions_baseline * n_days) as incremental_impressions
    , i.impression_count_10k_cap - (b.impression_count_10k_cap * n_days) as incremental_impressions_10k_cap
    , i.n_tweets - (b.n_tweets_baseline * n_days) as incremental_tweets
    , i.n_dashboards - (b.n_dashboards_baseline * n_days) as incremental_dashboards
    from cost c
    left join impressions i
        on i.name = c.name
    left join baseline b
        on b.project = c.project
    order by date desc
)
select *
from t1
, t2 as (
    select n_days::string as val
    , 'n_days' as variable
    , avg(boost_amount) as avg_cost
    , sum(boost_amount) as incremental_cost
    , avg(incremental_impressions) as avg_incremental_impressions
    , avg(incremental_impressions_10k_cap) as avg_incremental_impressions_10k_cap
    , avg(incremental_tweets) as avg_incremental_tweets
    , avg(incremental_dashboards) as avg_incremental_dashboards
    , sum(incremental_impressions) as incremental_impressions
    , sum(incremental_impressions_10k_cap) as incremental_impressions_10k_cap
    , sum(incremental_tweets) as incremental_tweets
    , sum(incremental_dashboards) as incremental_dashboards
    from t1
    group by 1, 2
    union
    select boost::string as val
    , 'boost' as variable
    , avg(boost_amount) as avg_cost
    , sum(boost_amount) as incremental_cost
    , avg(incremental_impressions) as avg_incremental_impressions
    , avg(incremental_impressions_10k_cap) as avg_incremental_impressions_10k_cap
    , avg(incremental_tweets) as avg_incremental_tweets
    , avg(incremental_dashboards) as avg_incremental_dashboards
    , sum(incremental_impressions) as incremental_impressions
    , sum(incremental_impressions_10k_cap) as incremental_impressions_10k_cap
    , sum(incremental_tweets) as incremental_tweets
    , sum(incremental_dashboards) as incremental_dashboards
    from t1
    group by 1, 2
    union
    select concat(n_days::string, ' x ', boost::string)::string as val
    , 'days x boost' as variable
    , avg(boost_amount) as avg_cost
    , sum(boost_amount) as incremental_cost
    , avg(incremental_impressions) as avg_incremental_impressions
    , avg(incremental_impressions_10k_cap) as avg_incremental_impressions_10k_cap
    , avg(incremental_tweets) as avg_incremental_tweets
    , avg(incremental_dashboards) as avg_incremental_dashboards
    , sum(incremental_impressions) as incremental_impressions
    , sum(incremental_impressions_10k_cap) as incremental_impressions_10k_cap
    , sum(incremental_tweets) as incremental_tweets
    , sum(incremental_dashboards) as incremental_dashboards
    from t1
    group by 1, 2
), t3 as (
    select *
    , round(incremental_impressions / incremental_cost, 1) as incremental_impressions_per_dollar
    , round(incremental_impressions_10k_cap / incremental_cost, 1) as incremental_impressions_10k_cap_per_dollar
    , round(incremental_tweets * 1000 / incremental_cost, 1) as incremental_tweets_per_1k_usd
    , round(incremental_dashboards * 1000 / incremental_cost, 1) as incremental_dashboards_per_1k_usd
    from t2
)
select val
, variable
, avg_cost
, incremental_impressions_per_dollar
from t3
order by variable
, incremental_impressions_per_dollar desc


select *
from thorchain


select date_trunc('month', t.created_at)::date as tweet_month
, d.id as dashboard_id
, t.impression_count
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.twitter.tweet t
    on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
qualify(
    row_number() over (partition by t.conversation_id order by impression_count desc) = 1
    and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
)


with t0 as (
SELECT distinct    
    t.tx_hash as tx_id,
    t.block_timestamp as block_timestamp,
    t.origin_from_address as address,
    1 as action_count,
    1 as quest_step,
    'AVAX' as currency,
    l.decoded_log:memo::string,
    case when l.decoded_log:memo::string like '+:AVAX.AVAX:thor%' then t.amount * 2
      else t.amount 
      end as token_amount,
    case when token_amount > 0.4 then TRUE else FALSE end as valid,
    tx.tx_fee as fee_amount

from avalanche.core.ez_native_transfers t
left join avalanche.core.fact_decoded_event_logs l
      on t.tx_hash = l.tx_hash
left join avalanche.core.fact_transactions tx 
      on t.tx_hash = tx.tx_hash

  WHERE
    l.event_name = 'Deposit'
    and l.decoded_log:memo::string like '+:AVAX.AVAX:%'
    and t.to_address = '0x8f66c4ae756bebc49ec8b81966dd8bba9f127549'
    and t.origin_function_signature = '0x44bc937b'
    and lower(t.origin_from_address) in (:userAddresses)
    and t.block_timestamp :: date >= current_date - 5
    and l.block_timestamp :: date >= current_date - 5
    and tx.block_timestamp :: date >= current_date - 5

    -- and t.block_timestamp :: date >= current_date - 5
    -- and l.block_timestamp :: date >= current_date - 5
    -- and tx.block_timestamp :: date >= current_date - 5
)
SELECT
  ':questId' as quest_id,
  block_timestamp,
  tx_id,
  action_count,
  address,
  valid,
  quest_step,
  token_amount,
  fee_amount,
  currency
from
  t0 

QUALIFY row_number() over (
    partition by address
    order by
      block_timestamp ASC,
      valid desc
  ) = 1;

select *
from avalanche.core.dim_labels
where label_type ilike '%bridge%'
limit 1000


select l.address_name
, count(distinct t.tx_hash) as n
from avalanche.core.ez_token_transfers t
join avalanche.core.dim_labels l
    on l.address = t.origin_from_address
    or l.address = t.origin_to_address
    or l.address = t.contract_address
    or l.address = t.from_address
    or l.address = t.to_address
where block_timestamp >= current_date - 1
    and l.label_type = 'bridge'




with league as (
  -- this CTE updated by kb
  select c.value:user_id::string as user_id
  , c.value:tag_type::string as tag_type
  , c.value:tag_name::string as tag_name
  , u.record_metadata:CreateTime::int as updated_at
  from crosschain.bronze.data_science_uploads u
  , lateral flatten(
      input => record_content
  ) c
  where record_metadata:key like 'analyst-tag%'
      and tag_type = 'League'
  qualify (
      row_number() over (
          partition by user_id
          order by updated_at desc
      ) = 1
  )
), gold_league as (
    -- this CTE updated by kb
    select *
    from league
    where tag_name = 'Gold League'
), imp as (
    select
    d.created_by_id as user_id
    , sum(impression_count) as impression_count
    , count(distinct t.conversation_id) as n_tweets
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    where t.created_at >= '2024-05-01'
        and t.created_at <= '2024-06-02'
    group by 1
), t0 as (
    select u.id as user_id
    , u.username
    , case when gl.user_id is null then 'Silver' else 'Gold' end as league
    , count(distinct q.id) as n_queries
    from bi_analytics.velocity_app_prod.users u
    join bi_analytics.velocity_app_prod.queries q
        on q.created_by_id = u.id
    left join gold_league gl
        on gl.user_id = u.id
    where q.created_at >= '2024-05-01'
    group by 1, 2, 3
)
select t0.*
, i.impression_count
from t0
join imp i
    on i.user_id = t0.user_id
order by n_queries desc




with likes as (
    select tweet_id as conversation_id
    , count(distinct user_id) as n_likes
    from bi_analytics.twitter.likes l
    group by 1
)
select concat('https://twitter.com/runkellen/status/', t.conversation_id) as twitter_url
, u.username
, d.title
, concat('https://flipsidecrypto.xyz/', u.username, '/', d.latest_slug) as dashboard_url
, t.like_count
, t.impression_count
, t.conversation_id::string as conversation_id
from bi_analytics.twitter.tweet t
join bi_analytics.velocity_app_prod.dashboards d
    on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
left join likes l
    on l.conversation_id = t.conversation_id
where t.platform = 'Flipside'
    and t.tweet_type in ('Dashboard', 'Flipside Science Dashboard')
    and t.created_at >= dateadd('hours', -24, current_timestamp)
    and t.created_at <= dateadd('hours', -3, current_timestamp)
    and l.n_likes is null
    and t.impression_count >= 100
order by impression_count desc



select *
from hevo.bronze_earn_quests_treasury_2024.quests
limit 10

with t0 as (
    select distinct q.agent_address as address
    , q.user_id
    , u.username
    , u.email
    , p.created_at::date as user_created_at
    , coalesce(s.score, 0) as total_score
    , q.status as quest_status
    , left(date_trunc('minute', q.created_at)::string, 16) as quest_start
    , q.created_at as quest_start_timestamp
    , quests.slug
    -- , *
    -- , q.*
    -- select *
    from hevo.bronze_earn_quests_treasury_2024.user_quest_status q
    -- limit 100
    join hevo.bronze_earn_quests_treasury_2024.quests
        on q.quest_id = quests.id
    -- left join hevo.bronze_earn_quests_treasury_2024.external_address_scores s
    left join hevo.bronze_earn_quests_treasury_2024.user_score s
        on s.id = q.user_score_id
        -- on lower(s.address) = lower(q.agent_address)
        -- and s.chain = 'avalanche'
    left join bi_analytics.velocity_app_prod.profiles p
        on p.id = q.profile_id
    left join bi_analytics.velocity_app_prod.users u
        on u.profile_id = p.id
    left join hevo.bronze_earn_quests_treasury_2024.user_payments up
        on q.id = up.reward_source_id
    -- where q.quest_id = '2db9f785-4aa9-43d2-8250-efef69c5b43c'
    where quests.slug like '%thorchain-%'
    order by total_score desc
), t1 as (
    select t0.*
    , l.block_timestamp::date as date
    , case when l.block_timestamp::date >= dateadd('days', 1, quest_start) then 1 else 0 end as is_after_quest_start
    , l.from_address
    , l.to_address
    , l.asset_address
    , asset_amount_usd + rune_amount_usd as amount_usd
    , case when lp_action = 'add_liquidity' then 1 else -1 end as mult
    , case when mult > 0 then amount_usd else 0 end as add_amount_usd
    , mult * rune_amount as net_rune_amount
    , mult * rune_amount_usd as net_rune_amount_usd
    , mult * asset_amount as net_asset_amount
    , mult * asset_amount_usd as net_asset_amount_usd
    , case when block_timestamp < quest_start_timestamp then 1 else 0 end as is_before_quest_start
    from thorchain.defi.fact_liquidity_actions l
    join t0
        on lower(t0.address) = lower(l.from_address)
        or lower(t0.address) = lower(l.to_address)
        or lower(t0.address) = lower(l.asset_address)
    where block_timestamp >= '2024-06-04'
        and t0.slug ilike '%provide%liquidity%'
)
-- select * from t1
, t2 as (
    select address
    , coalesce(username, 'none') as username
    , coalesce(email, 'none') as email
    , slug
    , sum(net_rune_amount) as net_rune_amount
    , sum(net_asset_amount) as net_asset_amount
    , sum(add_amount_usd) as add_amount_usd
    , sum(net_asset_amount_usd) as net_asset_amount_usd
    , max(is_before_quest_start) as is_before_quest_start
    from t1
    group by 1, 2, 3, 4
)
select *
, case
    when email ilike '%lyl%' or is_before_quest_start = 1 then 1 else 0 end as is_bot
from t2

, wallets as (
    select distinct address
    from t0
)
, swap as (
    select s.from_address as address
    -- , t0.username
    -- , t0.email
    -- , t0.user_created_at
    -- , t0.total_score
    -- , t0.quest_status
    -- , t0.quest_start
    -- , t0.slug
    , sum(from_amount_usd) as swap_volume
    from thorchain.defi.fact_swaps s
    join wallets w
        on lower(t0.address) = lower(s.from_address)
        -- and t0.quest_start <= dateadd('hours', 12, s.block_timestamp)
    where s.block_timestamp >= '2024-06-04'
    group by 1, 2, 3, 4, 5, 6, 7, 8
)
select *
from swap



select s.from_address as address
, pool_name
, sum(from_amount_usd) as swap_volume
, sum(distinct tx_id) as n_swaps
from thorchain.defi.fact_swaps s
where lower(from_address) in 
(
    '0x042d21b306d6274c6a12f27b6a94c899d0d0b167'
    , '0x04be6616bb87f0a7bda77c19bb761a7da8d195da'
    , '0x0770457567feaae5dd8ad4d6e441ccd9dc34b44c'
    , '0x085720fd7fcacb6a59340070373ae1660d992769'
    , '0x09715f3a4f72032546fb5a9c0500cb701699763f'
    , '0x09bee2e57d1c0edca7a45b7d8db4a1c5e806a27e'
    , '0x0c3fc56b2b9eb5dc3623a73c1b3787d0f521128c'
    , '0x106c62f52907a8348ce93fe7b265cd36c6c64c47'
    , '0x110c7971aad9188be4bdf7ac28a96b56f7c52df4'
    , '0x11866fe915ad2ed16a1cd45e0c6c648799093f08'
    , '0x19c5899132680d07f87ca9186040d7496a4a8e1d'
    , '0x1aad6f9793bb2ed50f7dc362faf2a71afdc50876'
    , '0x20dc31fe377de1dc5ee837360f3073eee4d3a31e'
    , '0x22c6284526fa0f6b3ec00807517c1f5e67417d48'
    , '0x23d3397345fd4f74d07c748f8a19a68668cf47d5'
    , '0x288d7c57e6a18549d2290f5a1a0eb988ac813418'
    , '0x2f2a2f8e1da4b340c47b4373fdb34799f765804d'
    , '0x2f6eb9d4e5844135543610d6d3fa67ad799ba30b'
    , '0x2f94c5ebd682b14a60d9b8717b073d5434313765'
    , '0x30de50679bc2d653d3af62db835416d250deb578'
    , '0x35abe1ff5a549659bbef0c9c20cbebbf6bc99a33'
    , '0x3c9a9b541db5189b4872a64f1f98d644d1c9604a'
    , '0x3ecbe1eefb004cb694a9c0278a9a1e5613ab3357'
    , '0x3fc931b0671a912eb6551daf99651aac6e0dd7de'
    , '0x455a92a4523034adceaf55ccdb85f379912e3e2e'
    , '0x5126672144bb5771674129e2430e28c7871ba388'
    , '0x51b940b0d49da59f67921bb64c1d81002db82a24'
    , '0x5df2fdfc32b6039cb560367cfcdad6d747afd8d2'
    , '0x66ee56fa8f48a73bfd8d9704922d9c361d53e83e'
    , '0x6982aaabfc2577cd38d40d73b709351ca60ab394'
    , '0x69bc8de1fc166fcb390362273de03b6289daae9c'
    , '0x6b67d955bc233a20811ec062ef7fa38c3e9b69ff'
    , '0x6bd9c35109020c5a4b8168df46f7c0059338e626'
    , '0x6dd8181210681ad3a754c7600a9fce7301b516d3'
    , '0x6f78c2de60a96d793129570dc01f1e95434e0b07'
    , '0x7063335518914b613441aa0a14b99510b033afd7'
    , '0x723b8a5aa12c4296f49b32085911286de1c2291b'
    , '0x738e104613ac0e99fa6509ddfe0e80ee5eeca4e0'
    , '0x7a627f0af774e21c8ff7361f880914e682641268'
    , '0x7c3e170006fcb567b222be49963f357ebeb33b2b'
    , '0x7d58c469d142c1d599894da959a96bdfe8794daf'
    , '0x7e7bb527701e9169c363e63bd0ac2a0e42fca18d'
    , '0x81fb5bae2c1ded10b1319249568971925afbc270'
    , '0x87a768534b4c49803376d8aca003665bd29de111'
    , '0x8f5246e3182df2844d4b284f5c9a5b63877f1e43'
    , '0x935ef0c100073c49701622ab9363762889984cda'
    , '0x944379600c74bd554784fd97c4d915e0849646fd'
    , '0x955f012280aa067fb8b54eddf01d2a1ebc7a2ee0'
    , '0x95e592ad47f84c418cfa0091d090c00c3f5f6399'
    , '0x989f5d20d47940f0a3b221284586ea577a66a7f1'
    , '0x9a055dbcb1abac856a0e70329293948181b00a0d'
    , '0x9a73b06c0cc052300ace833984499a7fb667ae32'
    , '0x9e581ab6f04120721ccd5dfc1a9c671a3c82701b'
    , '0xa0afd8d9327c5a9c88ee620a9931181a416cbffc'
    , '0xa0dcbf00ee6ff8b807620ef1b987ee041e701a32'
    , '0xa5f00ea7dbadd7a00e2eab4b840fc9c76e209a9a'
    , '0xadac862542cf65528b0c53b9b2768525df6d69ba'
    , '0xb4358538d62a2d9d3364f909cfff7f7e13e76397'
    , '0xb4ee18c7c9b0954761a1d8c9486111eac8856f12'
    , '0xb725cb3400439d81d4dcac9a5dd77b6e017ac961'
    , '0xb9783a4465d463cd910317501bbdcfd999ec34d4'
    , '0xc37c49030cc5ee9c93d07002c5885d71577178f1'
    , '0xd1b52c421d66d931bec8dfb6b1f9fb7027e85d04'
    , '0xd25c46f5045751002abdaed1c88708d085bccecb'
    , '0xd4d4558c1f6a5f57b0d892bd2eb875f68069f746'
    , '0xd9d6123dcf266a35163364bb71094beaf94ffec6'
    , '0xda1423bc8592d0a1bdc200f71e41334a6ca6ba25'
    , '0xe30b0b467e3b0a2559298040fa65658f6a4c9e7e'
    , '0xea32e867d8d734f6c963f2d50c879dd737ad0284'
    , '0xeaff4034e949d91d53a813845291d3395a23613e'
    , '0xecf0d3e4aa21a9b6c4c38aa3a5e9074d3e86e442'
    , '0xed920fefde58c428d2882d75da56370ceec191a7'
    , '0xef8546456710b815b71720b7b02d9ead06efe70b'
    , '0xf0f3f6cedd5e9f4a911e150d49ac14fea68ee413'
    , '0xf8b965ea9d88e4c56d0aec2a2ddb6bd0a1db4b7b'
    , '0xfb18e1d1f0ccdc1f2278f965176f483d61618016'
)
group by 1, 2


, t2 as (
    select address
    , username
    , email
    , user_created_at
    , total_score
    , quest_status
    , quest_start
    , slug
    , sum(add_amount_usd) as add_amount_usd
    , sum(net_rune_amount) as net_rune_amount
    , sum(net_asset_amount) as net_asset_amount
    from t1
    group by 1, 2, 3, 4, 5, 6, 7, 8
)
select 
coalesce(t2.address, s.address) as address
, coalesce(t2.username, s.username) as username
, coalesce(t2.email, s.email) as email
, coalesce(t2.user_created_at, s.user_created_at) as user_created_at
, coalesce(t2.total_score, s.total_score) as total_score
, coalesce(t2.quest_status, s.quest_status) as quest_status
, coalesce(t2.quest_start, s.quest_start) as quest_start
, coalesce(t2.slug, s.slug) as slug
, coalesce(t2.add_amount_usd, 0) as add_amount_usd
, coalesce(t2.net_rune_amount, 0) as net_rune_amount
, coalesce(t2.net_asset_amount, 0) as net_asset_amount
, coalesce(t2.net_asset_amount, 0) as net_asset_amount
, case when net_asset_amount < 0.1 then 0 else 1 end as is_retention
, coalesce(s.swap_volume, 0) as swap_volume
from t2
full outer join swap s
    on s.address = t2.address
    and s.username = t2.username
    and s.email = t2.email
    and s.user_created_at = t2.user_created_at
    and s.total_score = t2.total_score
    and s.quest_status = t2.quest_status
    and s.quest_start = t2.quest_start
    and s.slug = t2.slug
order by add_amount_usd desc


SELECT distinct t.tx_hash as tx_id
, t.block_timestamp as block_timestamp
, t.origin_from_address as address
, 1 as action_count
, 1 as quest_step
, 'USDC' as currency
, l.decoded_log:memo::string
, case
    when l.decoded_log:memo::string like '+:ETH.ETH:thor%' then t.amount * 2
    else t.amount end as amount
, case when amount >= 0.009 then TRUE else FALSE end as valid
, amount_usd as token_amount
, tx.tx_fee as fee_amount
, t.to_address
, t.origin_function_signature
, case when la.asset_tx_id is null then 0 else 1 end as is_lp
from ethereum.core.ez_native_transfers t
left join ethereum.core.fact_decoded_event_logs l
    on t.tx_hash = l.tx_hash
left join ethereum.core.fact_transactions tx
    on t.tx_hash = tx.tx_hash
left join thorchain.defi.fact_liquidity_actions la
    on lower(right(la.asset_tx_id, 64)) = lower(right(t.tx_hash, 64))
WHERE l.event_name = 'Deposit'
    and l.decoded_log:memo::string like '+:ETH.ETH:%'
    -- and l.decoded_log:memo::string like '%+%:%ETH.ETH%%'
    -- and t.to_address = '0x8f66c4ae756bebc49ec8b81966dd8bba9f127549'
    and t.origin_function_signature = '0x44bc937b'
    -- and lower(t.origin_from_address) in (:userAddresses)
    -- and t.block_timestamp :: date >= :startsAt
    -- and l.block_timestamp :: date >= :startsAt
    -- and tx.block_timestamp :: date >= :startsAt
    and t.block_timestamp :: date >= current_date - 50
    and l.block_timestamp :: date >= current_date - 50
    and tx.block_timestamp :: date >= current_date - 50


select *
from thorchain.defi.fact_liquidity_actions l
where block_timestamp >= current_date - 50
    and lp_action = 'add_liquidity'
    and pool_name ilike '%eth.eth%'
limit 100


select distinct t.conversation_id::string as conversation_id
from bi_analytics.twitter.tweet t
where t.platform = 'Thorchain'



-- what % of thorchain content comes from flipside
with labels as (
    select distinct d.id as dashboard_id
    , d.title
    , d.latest_slug
    , concat('https://flipsidecrypto.xyz/', u.username, '/', d.latest_slug) as dashboard_url
    , u.username
    , u.id as user_id
    , case when (
        q.statement like '%VoTpe3tHQ7AjQHMapgSue2HJFAh2cGsdokqN3XqmVSj%'
        or q.statement like '%GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY%'
    ) then 'jupiter' else t.name end as project
    , case when project in ('near'
        , 'solana'
        , 'aptos'
        , 'flow'
        , 'blast'
        , 'sei'
        , 'axelar'
        , 'avalanche'
        , 'vertex'
        , 'thorchain'
        , 'jupiter'
    ) then project else 'non-partner' end as partner_name
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    left join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
        on d.id = dtq.A
    left join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    left join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
        on q.id = qtt.A
    left join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and t.type = 'project'
    where (
        coalesce(t.name, '') = 'thorchain' or d.title ilike '%thorchain%'
    )
), t0 as (
    select t.*
    , u.user_name
    , case when l.latest_slug is null then 0 else 1 end as is_flipside
    from bi_analytics.twitter.tweet t
    left join bi_analytics.twitter.user u
        on u.id = t.user_id
    left join labels l
        on right(l.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)
    where t.created_at >= '2024-06-01'
        and (platform = 'Thorchain' or l.project = 'thorchain')
    order by impression_count desc
), likes as (
    select t0.conversation_id
    , t0.is_flipside
    , l.user_id as liker_id
    from bi_analytics.twitter.likes l
    join t0
        on t0.conversation_id = l.tweet_id
)
select sum(impression_count) as tot_impression_count
, sum(impression_count * is_flipside) as flipside_impression_count
, round(flipside_impression_count * 100 / tot_impression_count, 1) as pct_flipside_impression
from t0



with t0 as (
SELECT distinct    
    t.tx_hash as tx_id,
    t.block_timestamp as block_timestamp,
    t.origin_from_address as address,
    1 as action_count,
    1 as quest_step,
    'AVAX' as currency,
    l.decoded_log:memo::string,
    case when l.decoded_log:memo::string like '+:AVAX.AVAX:thor%' then t.amount * 2
      else t.amount 
      end as token_amount,
    case when token_amount > 0.4 then TRUE else FALSE end as valid,
    tx.tx_fee as fee_amount

from avalanche.core.ez_native_transfers t
left join avalanche.core.fact_decoded_event_logs l
      on t.tx_hash = l.tx_hash
left join avalanche.core.fact_transactions tx 
      on t.tx_hash = tx.tx_hash

  WHERE
    l.event_name = 'Deposit'
    and l.decoded_log:memo::string like '+:AVAX.AVAX:%'
    and t.to_address = '0x8f66c4ae756bebc49ec8b81966dd8bba9f127549'
    and t.origin_function_signature = '0x44bc937b'
    and lower(t.origin_from_address) in (:userAddresses)
    and t.block_timestamp :: date >= :startsAt
    and l.block_timestamp :: date >= :startsAt
    and tx.block_timestamp :: date >= :startsAt

    -- and t.block_timestamp :: date >= current_date - 5
    -- and l.block_timestamp :: date >= current_date - 5
    -- and tx.block_timestamp :: date >= current_date - 5
)
SELECT
  ':questId' as quest_id,
  block_timestamp,
  tx_id,
  action_count,
  address,
  valid,
  quest_step,
  token_amount,
  fee_amount,
  currency
from
  t0 

QUALIFY row_number() over (
    partition by address
    order by
      block_timestamp ASC,
      valid desc
  ) = 1;

with likes as (

)
, dashboards as (

)
, tweets as (
    select conversation_id::string as conversation_id
    , impression_count
)

select *
from thorchain.

[
    {
    "asset": "LTC.LTC",
    "amount": "322732401529"
    },
    {
    "asset": "BSC.BNB",
    "amount": "203750537631"
    },
    {
    "asset": "ETH.USDC-0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48",
    "amount": "88065596702300",
    "decimals": 6
    },
    {
    "asset": "ETH.ETH",
    "amount": "303276370260"
    },
    {
    "asset": "AVAX.AVAX",
    "amount": "5772647373441"
    },
    {
    "asset": "GAIA.ATOM",
    "amount": "3960771886900",
    "decimals": 6
    },
    {
    "asset": "DOGE.DOGE",
    "amount": "732227719856865"
    },
    {
    "asset": "BTC.BTC",
    "amount": "13399079408"
    },
    {
    "asset": "BCH.BCH",
    "amount": "637133293819"
    },
    {
    "asset": "BSC.USDT-0X55D398326F99059FF775485246999027B3197955",
    "amount": "23205998632701"
    },
    {
    "asset": "ETH.VTHOR-0X815C23ECA83261B6EC689B60CC4A58B54BC24D8D",
    "amount": "6691149258699"
    },
    {
    "asset": "ETH.SNX-0XC011A73EE8576FB46F5E1C5751CA3B9FE0AF2A6F",
    "amount": "610164799143"
    },
    {
    "asset": "ETH.LUSD-0X5F98805A4E8BE255A32880FDEC7F6728C6568BA0",
    "amount": "2302347182005"
    },
    {
    "asset": "ETH.DAI-0X6B175474E89094C44DA98B954EEDEAC495271D0F",
    "amount": "9011821419798"
    },
    {
    "asset": "ETH.THOR-0XA5F2211B9B8170F694421F2046281775E8468044",
    "amount": "305761983152084"
    },
    {
    "asset": "ETH.TGT-0X108A850856DB3F85D0269A2693D896B394C80325",
    "amount": "44069642931576"
    },
    {
    "asset": "ETH.LINK-0X514910771AF9CA656AF840DFF83E8264ECF986CA",
    "amount": "149653904780"
    },
    {
    "asset": "AVAX.USDC-0XB97EF9EF8734C71904D8002F8B6BC66DD9C48A6E",
    "amount": "31492189266100",
    "decimals": 6
    },
    {
    "asset": "ETH.KYL-0X67B6D479C7BB412C54E03DCA8E1BC6740CE6B99C",
    "amount": "21713335444309"
    },
    {
    "asset": "ETH.WSTETH-0X7F39C581F595B53C5CB19BD0B3F8DA6C935E2CA0",
    "amount": "619238324"
    },
    {
    "asset": "ETH.WBTC-0X2260FAC5E5542A773AA44FBCFEDF7C193BC2C599",
    "amount": "876346945"
    },
    {
    "asset": "AVAX.SOL-0XFE6B19286885A4F7F55ADAD09C3CD1F906D2478F",
    "amount": "36222800804"
    },
    {
    "asset": "ETH.HOT-0X6C6EE5E31D828DE241282B9606C8E98EA48526E2",
    "amount": "26304398911492"
    },
    {
    "asset": "BSC.USDC-0X8AC76A51CC950D9822D68B83FE1AD97B32CD580D",
    "amount": "6476352955792"
    },
    {
    "asset": "ETH.AAVE-0X7FC66500C84A76AD7E9C93437BFC5AC33E2DDAE9",
    "amount": "6386549550"
    },
    {
    "asset": "AVAX.USDT-0X9702230A8EA53601F5CD2DC00FDBC13D4DF4A8C7",
    "amount": "3520452996700",
    "decimals": 6
    },
    {
    "asset": "ETH.USDT-0XDAC17F958D2EE523A2206206994597C13D831EC7",
    "amount": "223513706955300",
    "decimals": 6
    },
    {
    "asset": "ETH.USDP-0X8E870D67F660D95D5BE530380D0EC0BD388289E1",
    "amount": "2554139408517"
    },
    {
    "asset": "ETH.RAZE-0X5EAA69B29F99C84FE5DE8200340B4E9B4AB38EAC",
    "amount": "89281708479690"
    },
    {
    "asset": "ETH.XRUNE-0X69FA0FEE221AD11012BAB0FDB45D444D3D2CE71C",
    "amount": "322580060316351"
    },
    {
    "asset": "ETH.DPI-0X1494CA1F11D487C2BBE4543E90080AEBA4BA3C2B",
    "amount": "9842533676"
    },
    {
    "asset": "ETH.VIU-0X519475B31653E46D20CD09F9FDCF3B12BDACB4F5",
    "amount": "541705"
    },
    {
    "asset": "ETH.GRT-0XC944E90C64B2C07662A292BE6244BDF05CDA44A7",
    "amount": "8984899780"
    },
    {
    "asset": "ETH.GUSD-0X056FD409E1D7A124BD7017459DFEA2F387B6D5CD",
    "amount": "2579487000000",
    "decimals": 2
    },
    {
    "asset": "ETH.LENDS-0X2C06BA9E7F0DACCBC1F6A33EA67E85BB68FBEE3A",
    "amount": "42060000000000"
    },
    {
    "asset": "ETH.XDEFI-0X72B886D09C117654AB7DA13A14D603001DE0B777",
    "amount": "2642279189040"
    },
    {
    "asset": "ETH.FOX-0XC770EEFAD204B5180DF6A14EE197D99D808EE52D",
    "amount": "28689047392879"
    },
    {
    "asset": "ETH.YFI-0X0BC529C00C6401AEF6D220BE8C6EA1667F6AD93E",
    "amount": "81180101"
    },
    {
    "asset": "ETH.FLIP-0X826180541412D574CF1336D22C0C0A287822678A",
    "amount": "48102281732"
    },
    {
    "asset": "ETH.UOS-0XD13C7342E1EF687C5AD21B27C2B65D772CAB5C8C",
    "amount": "123064900000",
    "decimals": 4
    }
]

con.host <- "prod-us-east-1-treasury-public.cluster-cleyy72dxyqt.us-east-1.rds.amazonaws.com"
con <- dbConnect(drv = RPostgres::Postgres(), host = con.host, 
                 dbname="Treasury", port = 5432, user = "external_scores_writer", password = readLines("~/data_science/analysis/ar_scores/userinfo.txt"))
-- score.summaries <- as.data.table(dbGetQuery(con, "select blockchain, total_score, count(1) as n_addresses from external_address_scores group by blockchain, total_score"))
scores <- as.data.table(dbGetQuery(con, "select blockchain, total_score, address from external_address_scores where blockchain = 'Avalanche'"))

select *
from ethereum.


SELECT distinct    
    t.tx_hash as tx_id,
    t.block_timestamp as block_timestamp,
    t.origin_from_address as address,
    1 as action_count,
    1 as quest_step,
    'USDC' as currency,
    case when amount > 0.008 then TRUE else FALSE end as valid,
    amount_usd as token_amount,
    tx.tx_fee as fee_amount,
    case when s.asset_tx is null then 0 else 1 end as is_lp,
    case when l.decoded_log:memo::string ilike '+:' then 1 else 0 end as is_lp_memo,
    l.event_name

from ethereum.core.ez_native_transfers t
left join ethereum.core.fact_decoded_event_logs l
      on t.tx_hash = l.tx_hash
left join ethereum.core.fact_transactions tx 
      on t.tx_hash = tx.tx_hash
left join thorchain.bronze.stake_events s
    on lower(s.asset_tx) = lower(t.tx_hash)

WHERE 1=1
-- l.event_name = 'Deposit'
-- and l.decoded_log:memo::string like '=:a:%'
and t.to_address = '0xe8ae26cb4353b8f09e025c769ed6635d6bf4f017'
-- and t.origin_function_signature = '0x44bc937b'

and t.block_timestamp :: date >= current_date - 5
and l.block_timestamp :: date >= current_date - 5
and tx.block_timestamp :: date >= current_date - 5


select *
from thorchain.bronze.stake_events
limit 100




select *
from thorchain.bronze.swap_events sw
where tx ilike 'aabf07a4ed147b13ec366240947e57b52820ab2382688c0955b9272df73391b8'



SELECT 
    t.tx_hash as tx_id,
    t.block_timestamp as block_timestamp,
    tx.to_address as tx_to_address,
    t.origin_from_address as address,
    -- l.decoded_log:memo::string as memo,
    t.origin_function_signature,
    -- s.memo as s_memo,
    -- sw.memo as sw_memo,
    t.to_address,
    1 as action_count,
    1 as quest_step,
    'USDC' as currency,
    max(case when amount > 0.008 then TRUE else FALSE end) as valid,
    -- amount_usd as token_amount,
    -- tx.tx_fee as fee_amount,
    max(case when s.asset_tx is null then 0 else 1 end) as is_lp,
    max(case when sw.tx is null then 0 else 1 end) as is_swap,
    max(case when l.decoded_log:memo::string ilike '%+:%' then 1 else 0 end) as is_lp_memo,
    -- l.event_name
    max(case when l.decoded_log:memo::string ilike any ('s:%', 'swap:%', '=:%') then 1 else 0 end) as is_swap_memo

from ethereum.core.ez_native_transfers t
join ethereum.core.fact_decoded_event_logs l
      on t.tx_hash = l.tx_hash
join ethereum.core.fact_transactions tx 
      on t.tx_hash = tx.tx_hash
left join thorchain.bronze.stake_events s
    on right(lower(s.asset_tx), 64) = right(lower(t.tx_hash), 64)
left join thorchain.bronze.swap_events sw
    on right(lower(sw.tx), 64) = right(lower(t.tx_hash), 64)
WHERE 1=1
    -- and (s.asset_tx is not null or sw.tx is not null)
    -- l.event_name = 'Deposit'
    -- and l.decoded_log:memo::string like '=:a:%'
    -- and t.to_address = '0xe8ae26cb4353b8f09e025c769ed6635d6bf4f017'
    and t.origin_function_signature in (
        '0x44bc937b'
      , '0xe4d0c7f0'
      , '0x08a018aa'
      , '0xdf759fce'
      , '0x972250fe'
      , '0x2541ec57'
      , '0x3d21e25a'
      , '0x1fece7b4'
    )
    and t.block_timestamp :: date >= current_date - 10
    and l.block_timestamp :: date >= current_date - 10
    and tx.block_timestamp :: date >= current_date - 10
group by 1, 2, 3, 4, 5, 6, 7, 8, 9


SELECT count(distinct t.tx_hash)
from ethereum.core.ez_native_transfers t
join ethereum.core.fact_decoded_event_logs l
      on t.tx_hash = l.tx_hash
join ethereum.core.fact_transactions tx 
      on t.tx_hash = tx.tx_hash
WHERE t.block_timestamp :: date >= current_date - 10
    and l.block_timestamp :: date >= current_date - 10
    and tx.block_timestamp :: date >= current_date - 10
    and t.origin_function_signature in (
        '0x44bc937b'
      , '0xe4d0c7f0'
      , '0x08a018aa'
      , '0xdf759fce'
      , '0x972250fe'
      , '0x2541ec57'
      , '0x3d21e25a'
      , '0x1fece7b4'
    )
    and l.decoded_log:memo::string ilike any ('s:%', 'swap:%', '=:%')

SELECT count(distinct t.tx_hash)
from ethereum.core.ez_native_transfers t
join ethereum.core.fact_decoded_event_logs l
      on t.tx_hash = l.tx_hash
join ethereum.core.fact_transactions tx 
      on t.tx_hash = tx.tx_hash
join thorchain.bronze.swap_events sw on right(lower(sw.tx), 64) = right(lower(t.tx_hash), 64)
WHERE t.block_timestamp :: date >= current_date - 10
    and l.block_timestamp :: date >= current_date - 10
    and tx.block_timestamp :: date >= current_date - 10


-- thorchain eth swap check
with t0 as (
  select distinct t.tx_hash
  , t.block_timestamp::date as date
  , 1 as check_1
  from avalanche.core.ez_native_transfers t
  join avalanche.core.fact_decoded_event_logs l
        on t.tx_hash = l.tx_hash
  join avalanche.core.fact_transactions tx 
        on t.tx_hash = tx.tx_hash
  WHERE t.block_timestamp :: date >= current_date - 10
      and l.block_timestamp :: date >= current_date - 10
      and tx.block_timestamp :: date >= current_date - 10
      and t.origin_function_signature in (
          '0x44bc937b'
        , '0xe4d0c7f0'
        , '0x08a018aa'
        , '0xdf759fce'
        , '0x972250fe'
        , '0x2541ec57'
        , '0x3d21e25a'
        , '0x1fece7b4'
      )
      and l.decoded_log:memo::string ilike any ('s:%', 'swap:%', '=:%')
      and t.amount >= 0.1
), t1 as (
  select distinct t.tx_hash
  , t.block_timestamp::date as date
  , 1 as check_2
  from avalanche.core.ez_native_transfers t
  join avalanche.core.fact_decoded_event_logs l
        on t.tx_hash = l.tx_hash
  join avalanche.core.fact_transactions tx 
        on t.tx_hash = tx.tx_hash
  join thorchain.bronze.swap_events sw on right(lower(sw.tx), 64) = right(lower(t.tx_hash), 64)
  WHERE t.block_timestamp :: date >= current_date - 10
      and l.block_timestamp :: date >= current_date - 10
      and tx.block_timestamp :: date >= current_date - 10
      and t.amount >= 0.1
)
select coalesce(t0.tx_hash, t1.tx_hash) as tx_hash
, coalesce(t0.date, t1.date) as date
, coalesce(t0.check_1, 0) as check_1
, coalesce(t1.check_2, 0) as check_2
from t0
full outer join t1
  on t1.tx_hash = t0.tx_hash
  and t1.date = t0.date



SELECT hr.dashboard_id
, d.title
, u.username
, u.id as user_id
, count(distinct date_trunc('hour', dbt_updated_at)) as n_hours_in_top_40
from bi_analytics.snapshots.hourly_dashboard_rankings hr
join bi_analytics.velocity_app_prod.dashboards d
    on d.id = hr.dashboard_id
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
    on d.id = dtq.A
join bi_analytics.velocity_app_prod.queries q
    on dtq.B = q.id
join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
    on q.id = qtt.A
join bi_analytics.velocity_app_prod.tags t
    on qtt.B = t.id
where dbt_updated_at >= '2024-06-12'
    and ranking_trending <= 40
    and t.name = 'kaia'
group by 1, 2, 3, 4


SELECT block_timestamp::date as date 
, split_part(pool_name, '-', 1) as token
, max(asset_usd) as price
from thorchain.price.fact_prices
where block_timestamp >= current_date - 10
group by 1, 2
order by 1, 2

SELECT block_timestamp::date as date 
, max(rune_usd) as price
from thorchain.price.fact_prices
where block_timestamp >= current_date - 10
group by 1
order by 1

SELECT split_part(pool_name, '-', 1) as token
, *
from thorchain.price.fact_prices
where block_timestamp >= current_date - 10
    and token = 'THOR.ATOM'
order by block_timestamp

SELECT hr.dashboard_id
, d.title
, u.username
, u.id as user_id
, dbt_updated_at
, ranking_trending
from bi_analytics.snapshots.hourly_dashboard_rankings hr
join bi_analytics.velocity_app_prod.dashboards d
    on d.id = hr.dashboard_id
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
where dbt_updated_at >= '2024-06-12'
    -- and ranking_trending <= 40
    -- and t.name = 'kaia'
    and d.title = 'Raydium Megadash'
order by dbt_updated_at


select *
from bi_analytics.twitter.missing_tweets
where dashboard_id = '7dd31327-457f-40e9-b6aa-959f0898879c'



SELECT *
from flipside_prod_db.bronze.prod_address_label_sink_291098491 dbc 
WHERE _inserted_timestamp >= '2023-06-05'
    and record_metadata:topic::string = 'twitter-tweet'
limit 10

SELECT *
from bi_analytics.twitter.tweet
WHERE conversation_id = '1801320508350415191'
limit 10


with twitter_accounts as (
    select twitter_id
    , twitter_handle
    , account_type
    , ecosystems[0]::string as ecosystem
    , n_followers
    from crosschain.bronze.twitter_accounts
    where not twitter_id in (
        '1314075720'
        , '925712018937712640'
        , '59119959'
        , '1445056111753584644'
        , '791390945656856577'
    )
    qualify(
        row_number() over (partition by twitter_id order by updated_at desc) = 1
    )
)
select taq.twitter_handle
, u.username
, count(distinct t.conversation_id) as n_quotes
-- distinct t.conversation_id
-- , taq.twitter_handle
-- , q.quote_tweet_id
-- , t.conversation_id
-- , u.username
-- , d.title
-- , concat('https://x.com/jpthor/status/',q.quote_tweet_id) as tweet_url
-- , taq.*
from bi_analytics.twitter.tweet t
join bi_analytics.velocity_app_prod.dashboards d
    on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
join bi_analytics.twitter.quotes q
    on q.tweet_id = t.conversation_id
join twitter_accounts taq
    on taq.twitter_id = q.user_id
where t.created_at >= '2024-06-01'
    and taq.twitter_handle ilike '%jpthor%'
    and taq.ecosystem ilike '%thorchain%'
    and (not taq.account_type ilike '%flipside%')
group by 1, 2
order by 3 desc



select *
from bi_analytics.twitter.retweets r
join bi_analytics.twitter.tweet t
    on (
        t.conversation_id = r.tweet_id
        or t.id = r.tweet_id
    )
order by t.created_at desc
limit 10


select *
from bi_analytics.velocity_app_prod.dashboards d
limit 10




select distinct id
, conversation_id
, clean_url
from bi_analytics.twitter.tweet t
join bi_analytics.velocity_app_prod.dashboards d
    on right(d.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)

with t0 as (
    select pool_name
    , lower(s.from_address) as address
    , min(block_timestamp::date) as first_date
    from thorchain.defi.fact_swaps s
    where block_timestamp >= '2024-06-04'
        and pool_name = 'AVAX.AVAX'
    group by 1, 2
), t1 as (
    select *
    , case when address in 
    (
        '0x042d21b306d6274c6a12f27b6a94c899d0d0b167'
        , '0x04be6616bb87f0a7bda77c19bb761a7da8d195da'
        , '0x0770457567feaae5dd8ad4d6e441ccd9dc34b44c'
        , '0x085720fd7fcacb6a59340070373ae1660d992769'
        , '0x09715f3a4f72032546fb5a9c0500cb701699763f'
        , '0x09bee2e57d1c0edca7a45b7d8db4a1c5e806a27e'
        , '0x0c3fc56b2b9eb5dc3623a73c1b3787d0f521128c'
        , '0x106c62f52907a8348ce93fe7b265cd36c6c64c47'
        , '0x110c7971aad9188be4bdf7ac28a96b56f7c52df4'
        , '0x11866fe915ad2ed16a1cd45e0c6c648799093f08'
        , '0x19c5899132680d07f87ca9186040d7496a4a8e1d'
        , '0x1aad6f9793bb2ed50f7dc362faf2a71afdc50876'
        , '0x20dc31fe377de1dc5ee837360f3073eee4d3a31e'
        , '0x22c6284526fa0f6b3ec00807517c1f5e67417d48'
        , '0x23d3397345fd4f74d07c748f8a19a68668cf47d5'
        , '0x288d7c57e6a18549d2290f5a1a0eb988ac813418'
        , '0x2f2a2f8e1da4b340c47b4373fdb34799f765804d'
        , '0x2f6eb9d4e5844135543610d6d3fa67ad799ba30b'
        , '0x2f94c5ebd682b14a60d9b8717b073d5434313765'
        , '0x30de50679bc2d653d3af62db835416d250deb578'
        , '0x35abe1ff5a549659bbef0c9c20cbebbf6bc99a33'
        , '0x3c9a9b541db5189b4872a64f1f98d644d1c9604a'
        , '0x3ecbe1eefb004cb694a9c0278a9a1e5613ab3357'
        , '0x3fc931b0671a912eb6551daf99651aac6e0dd7de'
        , '0x455a92a4523034adceaf55ccdb85f379912e3e2e'
        , '0x5126672144bb5771674129e2430e28c7871ba388'
        , '0x51b940b0d49da59f67921bb64c1d81002db82a24'
        , '0x5df2fdfc32b6039cb560367cfcdad6d747afd8d2'
        , '0x66ee56fa8f48a73bfd8d9704922d9c361d53e83e'
        , '0x6982aaabfc2577cd38d40d73b709351ca60ab394'
        , '0x69bc8de1fc166fcb390362273de03b6289daae9c'
        , '0x6b67d955bc233a20811ec062ef7fa38c3e9b69ff'
        , '0x6bd9c35109020c5a4b8168df46f7c0059338e626'
        , '0x6dd8181210681ad3a754c7600a9fce7301b516d3'
        , '0x6f78c2de60a96d793129570dc01f1e95434e0b07'
        , '0x7063335518914b613441aa0a14b99510b033afd7'
        , '0x723b8a5aa12c4296f49b32085911286de1c2291b'
        , '0x738e104613ac0e99fa6509ddfe0e80ee5eeca4e0'
        , '0x7a627f0af774e21c8ff7361f880914e682641268'
        , '0x7c3e170006fcb567b222be49963f357ebeb33b2b'
        , '0x7d58c469d142c1d599894da959a96bdfe8794daf'
        , '0x7e7bb527701e9169c363e63bd0ac2a0e42fca18d'
        , '0x81fb5bae2c1ded10b1319249568971925afbc270'
        , '0x87a768534b4c49803376d8aca003665bd29de111'
        , '0x8f5246e3182df2844d4b284f5c9a5b63877f1e43'
        , '0x935ef0c100073c49701622ab9363762889984cda'
        , '0x944379600c74bd554784fd97c4d915e0849646fd'
        , '0x955f012280aa067fb8b54eddf01d2a1ebc7a2ee0'
        , '0x95e592ad47f84c418cfa0091d090c00c3f5f6399'
        , '0x989f5d20d47940f0a3b221284586ea577a66a7f1'
        , '0x9a055dbcb1abac856a0e70329293948181b00a0d'
        , '0x9a73b06c0cc052300ace833984499a7fb667ae32'
        , '0x9e581ab6f04120721ccd5dfc1a9c671a3c82701b'
        , '0xa0afd8d9327c5a9c88ee620a9931181a416cbffc'
        , '0xa0dcbf00ee6ff8b807620ef1b987ee041e701a32'
        , '0xa5f00ea7dbadd7a00e2eab4b840fc9c76e209a9a'
        , '0xadac862542cf65528b0c53b9b2768525df6d69ba'
        , '0xb4358538d62a2d9d3364f909cfff7f7e13e76397'
        , '0xb4ee18c7c9b0954761a1d8c9486111eac8856f12'
        , '0xb725cb3400439d81d4dcac9a5dd77b6e017ac961'
        , '0xb9783a4465d463cd910317501bbdcfd999ec34d4'
        , '0xc37c49030cc5ee9c93d07002c5885d71577178f1'
        , '0xd1b52c421d66d931bec8dfb6b1f9fb7027e85d04'
        , '0xd25c46f5045751002abdaed1c88708d085bccecb'
        , '0xd4d4558c1f6a5f57b0d892bd2eb875f68069f746'
        , '0xd9d6123dcf266a35163364bb71094beaf94ffec6'
        , '0xda1423bc8592d0a1bdc200f71e41334a6ca6ba25'
        , '0xe30b0b467e3b0a2559298040fa65658f6a4c9e7e'
        , '0xea32e867d8d734f6c963f2d50c879dd737ad0284'
        , '0xeaff4034e949d91d53a813845291d3395a23613e'
        , '0xecf0d3e4aa21a9b6c4c38aa3a5e9074d3e86e442'
        , '0xed920fefde58c428d2882d75da56370ceec191a7'
        , '0xef8546456710b815b71720b7b02d9ead06efe70b'
        , '0xf0f3f6cedd5e9f4a911e150d49ac14fea68ee413'
        , '0xf8b965ea9d88e4c56d0aec2a2ddb6bd0a1db4b7b'
        , '0xfb18e1d1f0ccdc1f2278f965176f483d61618016'
    ) then 1 else 0 end as is_from_flipside_quest
)
, t2 as (
    select first_date
    , is_from_flipside_quest
    , count(1) as n_wallets
    from t1
    group by 1, 2
)
select *
from t2
where first_date >= '2024-05-01'
order by first_date desc
, is_from_flipside_quest desc




select created_by_id as user_id
, u.username
, count(distinct q.id) as n_queries
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.users u
    on u.id = q.created_by_id
where q.created_at >= current_date - 30
group by 1, 2
order by 3 desc


select created_at::date as date
, count(1) as n
from bi_analytics.twitter.tweet
where created_at >= '2024-05-01'
group by 1
order by 1



-- popular thorchain tweeters
select user_id
, username
, sum(impression_count) as impression_count
, count(1) as n
from bi_analytics.twitter.tweet t
left join bi_analytics.twitter.user u
    on u.id = t.user_id
where created_at >= '2024-05-01'
    and platform = 'Thorchain'
    and impression_count >= 200
group by 1, 2
order by 3 desc

-- daily tweets by platform
select platform
, created_at::date as date
, sum(impression_count) as impression_count
, count(1) as n
from bi_analytics.twitter.tweet
where created_at >= '2024-05-01'
group by 1, 2
order by 1, 2


-- likes with no tweet
with t0 as (
    select tweet_id
    , count(1) as n
    from bi_analytics.twitter.likes
    group by 1
), t1 as (
    select distinct conversation_id as id
    from bi_analytics.twitter.tweet
    union
    select distinct id
    from bi_analytics.twitter.tweet
)
select t0.*
from t0
where not tweet_id in (
    select id from t1
)





select 
q.created_at::date as quest_date
, uqs.created_at::date as quest_start_date
, up.usd_amount as reward
, q.slug as quest
, uqs.status as user_quest_status
, u.username
, u.email
-- q.slug
-- , up.usd_amount
-- , up.*
, *
from hevo.bronze_earn_quests_treasury_2024.user_payments up
join hevo.bronze_earn_quests_treasury_2024.user_quest_status uqs
    on uqs.id = up.reward_source_id
join hevo.bronze_earn_quests_treasury_2024.quests q
    on q.id = uqs.quest_id
join bi_analytics.velocity_app_prod.users u
    on u.id = up.user_id
where q.slug ilike '%thorchain%'
-- limit 100


select *
from thorchain.defi.fact_block_pool_depths
qualify(
    row_number() over (
        partition by pool_name
        order by block_timestamp desc
        ) = 1
)
-- order by block_timestamp desc, pool_name
limit 100

select *
from thorchain.defi.fact_daily_tvl
limit 10


select *
from bi_analytics.twitter.tweet
where conversation_id = '1804137915108266172'

with t0 as (
    select *
    from bi_analytics.velocity_app_prod.dashboards d
    where d.created_at >= current_date - 1
    QUALIFY(
        row_number() over (partition by id order by updated_at desc) = 1
    )
)
, t1 as (
    select DISTINCT c.value:component:type::string as type
    , c.value:formula:visId::string as visId
    , c.value:formula:queryId::string as queryId
    , d.id as dashboard_id
    , c.value:formula:text::string as text
    , c.*
    from t0 d
    , LATERAL FLATTEN(
        input => published:cells
    ) c
), t2 as (
    select t1.*
    , v.title
    from t1
    left join bi_analytics.velocity_app_prod.visualizations v
        on v.id = t1.visId
)
select * 
from t2
where (
    title ilike '%nft%'
    or title ilike '%Echelon%'
    or title ilike '%bridge%'
) or (
    text ilike '%nft%'
    or text ilike '%Echelon%'
    or text ilike '%bridge%'
)

select *
from bi_analytics.velocity_app_prod.visualizations
where created_at >= current_date - 7
and (
    title ilike '%nft%'
    or title ilike '%Echelon%'
    or title ilike '%bridge%'
)
qualify(
    row_number() over (partition by type order by created_at desc) <= 3
)
limit 100



-- dashboard title, username, poster, likes, RTs, QTs
select right(
    split(regexp_replace(split(split(c.text, 'flipsidecrypto.xyz')[1], '?')[0], '\\s+', ' '), ' ')[0]::string, 6
) as dashboard_slug
, d.id as dashboard_id
, reaction_type * POWER(
    0.9500, (DATEDIFF('minutes', l.created_at, m.mx_timestamp) / 60)
) as warpcast_rt_like_wt_0
, reaction_type * POWER(
    0.9900, (DATEDIFF('minutes', l.created_at, m.mx_timestamp) / 60)
) as warpcast_rt_like_wt_1
, reaction_type * POWER(
    0.9985, (DATEDIFF('minutes', l.created_at, m.mx_timestamp) / 60)
) as warpcast_rt_like_wt_2
, reaction_type as warpcast_rt_like_wt_3
select d.title
, u.username
from external.bronze.farcaster_casts c
join external.bronze.farcaster_reactions r
    on r.target_hash = c.hash
join external.bronze.farcaster_fnames f
    on f.id = c.fid
join bi_analytics.velocity_app_prod.dashboards d
    on right(d.latest_slug, 6) = right(dashboard_slug, 6)
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
where c.created_at >= current_date - 30
    and c.text ilike '%flipsidecrypto.xyz%'




with t0 as (
    select left(hour::date::string, 10) as date
    , symbol
    , avg(price) as avg_price
    from crosschain.price.ez_prices_hourly
    where hour >= current_date - 365
        and token_address in (
            'So11111111111111111111111111111111111111112'
            , 'So11111111111111111111111111111111111111112'
        )
        or (
            symbol = 'BTC'
            and blockchain = 'bitcoin'
        )
        or (
            symbol = 'ETH'
            and blockchain = 'ethereum'
        )
    group by 1, 2
), t1 as (
    select b.date
    , b.avg_price as btc_price
    , s.avg_price as sol_price
    , e.avg_price as eth_price
    , row_number() over (order by b.date) as rn
    from t0 b
    join t0 s
        on s.date = b.date
        and s.symbol = 'SOL'
    join t0 e
        on e.date = b.date
        and e.symbol = 'ETH'
    where b.symbol = 'BTC'
), t2 as (
    select *
    , sol_price / btc_price as sol_btc_price
    , eth_price / btc_price as eth_btc_price
    from t1
), t3 as (
    select sol_btc_price as starting_sol_btc_price
    , eth_btc_price as starting_eth_btc_price
    from t2
    where rn = 1
), t4 as (
    select t2.*
    , t3.*
    , t2.sol_btc_price / t3.starting_sol_btc_price as sol_ratio
    , t2.eth_btc_price / t3.starting_eth_btc_price as eth_ratio
    from t2
    join t3 on true
)
select *
from t4



with league as (
  -- this CTE updated by kb
  select c.value:user_id::string as user_id
  , c.value:tag_type::string as tag_type
  , c.value:tag_name::string as tag_name
  , u.record_metadata:CreateTime::int as updated_at
  from crosschain.bronze.data_science_uploads u
  , lateral flatten(
      input => record_content
  ) c
  where record_metadata:key like 'analyst-tag%'
      and tag_type = 'League'
  qualify (
      row_number() over (
          partition by user_id
          order by updated_at desc
      ) = 1
  )
), gold_league as (
    -- this CTE updated by kb
    select *
    from league
    where tag_name = 'Gold League'
)
select d.title
, d.created_at::date as date
, u.username
, coalesce(tag_name, 'Silver Leage') as league
, r.*
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
join bi_analytics.content_rankings.dashboard_rankings r
    on r.dashboard_id = d.id
left join gold_league gl
    on gl.user_id = u.id
order by r.ranking_trending
limit 500000




with p as (
    select hour::date as date
    , avg(price) as avg_price
    from crosschain.price.ez_prices_hourly
    where hour >= current_date - 365
        and token_address in (
            'So11111111111111111111111111111111111111112'
        )
    group by 1
), t0 as (
    select date_trunc('day', BLOCK_TIMESTAMP) as date
    , count (distinct tx_id) as n_sales
    , count (distinct purchaser) as n_buyers
    , count (distinct seller) as n_sellers
    , count (distinct mint) as n_mints
    , sum(sales_amount) as volume
    , avg(sales_amount) as avg_price
    , median(sales_amount) as median_price
    , count(distinct block_timestamp::date) as n_days
    , sum(sales_amount * avg_price) as volume_usd
    from solana.nft.fact_nft_sales s
    join p
        on p.date = s.block_timestamp::date
    where block_timestamp::date < current_date
        and succeeded = 'TRUE'
        and sales_amount < 1000
    group by 1
), t1 as (
    select *
    , avg(volume) over (
        order by date
        rows between 29 preceding and current row
    ) as rolling_avg_30_days
    , avg(volume) over (
        order by date
        rows between 7 preceding and current row
    ) as rolling_avg_7_days
    , avg(volume_usd) over (
        order by date
        rows between 29 preceding and current row
    ) as rolling_avg_30_days_usd
    , avg(volume_usd) over (
        order by date
        rows between 7 preceding and current row
    ) as rolling_avg_7_days_usd
    from t0
)
select *
from t1



with t0 as (
    select i.tx_id
    , i.block_timestamp::date as date
    , i.decoded_instruction
    , i.decoded_instruction:name::string as name
    , di.value:pubkey::string as borrower
    , i.decoded_instruction:args:floor::int as floor
    , i.decoded_instruction:args:terms:apyBps::int as apyBps
    , i.decoded_instruction:args:terms:duration::int as duration
    , i.decoded_instruction:args:terms:principal::int * pow(10, -9) as principal
    from solana.core.fact_decoded_instructions i
    , lateral flatten(input => decoded_instruction:accounts) as di
    -- where i.block_timestamp >= current_date - 7
    where i.block_timestamp >= '2024-06-01'
        and di.value:name::string = 'borrower'
        and i.program_id = 'JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp'
        and borrower is not null
)
-- select * from t0
-- where loanAccount
-- where tx_id = 'BiSPxxhk1auTNQj6n8MBDzeSHfZXXUN18CwH6F4cZikyjyAuEBd8GyftxPHohMHLkL6TgHv4usA3DsVw6ktF8W2'
, t1 as (
    select t0.*
    , di.value:pubkey::string as mint
    from t0
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = 'mint'
        and mint is not null
)
select *
from t1
where mint = 'AqNVntPc61ffAt6KovHYs8CtoKFCKsUrJsby2fxR4i9s'
, t1 as (
    select t0.*
    , di.value:pubkey::string as lender
    from t0
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = 'lender'
        and lender is not null
)
-- select * from t1
-- where tx_id = 'BiSPxxhk1auTNQj6n8MBDzeSHfZXXUN18CwH6F4cZikyjyAuEBd8GyftxPHohMHLkL6TgHv4usA3DsVw6ktF8W2'
, t2 as (
    select t1.*
    , di.value:pubkey::string as lendAuthority
    from t1
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = 'lendAuthority'
        and lendAuthority is not null
)
-- select * from t2
-- where tx_id = 'BiSPxxhk1auTNQj6n8MBDzeSHfZXXUN18CwH6F4cZikyjyAuEBd8GyftxPHohMHLkL6TgHv4usA3DsVw6ktF8W2'
, t3 as (
    select distinct tx_id
    , borrower
    , lendAuthority
    from t2
)
-- select * from t3
-- where tx_id = 'BiSPxxhk1auTNQj6n8MBDzeSHfZXXUN18CwH6F4cZikyjyAuEBd8GyftxPHohMHLkL6TgHv4usA3DsVw6ktF8W2'
, t4 as (
    select t3.tx_id
    , max(amount) as xfer_amt
    , count(amount) as n_xfers
    from t3
    left join solana.core.fact_transfers t
        on t.tx_id = t3.tx_id
        and t.mint = 'So11111111111111111111111111111111111111112'
    where t.block_timestamp >= current_date - 30
        -- and t.tx_to = t3.borrower
        -- and t.tx_from = t3.lendAuthority
    group by 1
)
-- select * from t4
-- where tx_id = 'BiSPxxhk1auTNQj6n8MBDzeSHfZXXUN18CwH6F4cZikyjyAuEBd8GyftxPHohMHLkL6TgHv4usA3DsVw6ktF8W2'
, t5 as (
    select t2.*
    , t4.xfer_amt
    , t4.n_xfers
    from t2
    join t4
        on t4.tx_id = t2.tx_id
)
select *
from t5
where tx_id = 'BiSPxxhk1auTNQj6n8MBDzeSHfZXXUN18CwH6F4cZikyjyAuEBd8GyftxPHohMHLkL6TgHv4usA3DsVw6ktF8W2'
-- where lender = 'kcitfabbdz4KFNPn8RJjUEg1rD36SJdFwJg8imo2Rrg'

select lender
, count(1) as n_loans
, sum(xfer_amt) as loan_amt
from t5
group by 1
order by 3 desc

select *
from solana.core.fact_decoded_instructions
where block_timestamp >= current_date - 7
    and tx_id = 'BiSPxxhk1auTNQj6n8MBDzeSHfZXXUN18CwH6F4cZikyjyAuEBd8GyftxPHohMHLkL6TgHv4usA3DsVw6ktF8W2'


select *
from solana.nft.fact_nft_sales
where block_timestamp >= current_date - 90
    and tx_id = '32mVeNJ3L9SeT7HYnWtK2EEwuuiVuQbNcYCxXq3pJAkdMUwjaWNWRJranizNXpwZqQtE8RpNqQDwc96Urw2ZVchH'

with popularity as (
    select block_timestamp::date as date
    , swap_to_mint as mint
    , count(distinct swapper) as n_swappers
    from solana.defi.fact_swaps
    where block_timestamp >= current_date - 300
        and succeeded
    group by 1, 2
)
, prices0 as (
    select date_trunc('hour', hour) as hour
    , token_address as mint
    , avg(price) as avg_price
    from crosschain.price.ez_prices_hourly
    where hour >= current_date - 300
        and blockchain = 'solana'
    group by 1, 2
)
, prices as (
    select p.hour
    , p.mint
    , p.avg_price
    , pop.n_swappers
    from prices0 p
    join popularity pop
        on pop.date = p.hour::date
        and pop.mint = p.mint
)
, cur_price as (
    select mint
    , avg_price as cur_price
    from prices
    qualify(
        row_number() over (partition by mint order by hour desc) = 1
    )
)
, t0 as (
    select swapper
    , block_timestamp
    , swap_from_mint
    , swap_to_mint
    , swap_from_amount
    , swap_to_amount
    , case
        when coalesce(pf.n_swappers, 0) > coalesce(pt.n_swappers, 0) then pf.avg_price * swap_from_amount
        else pt.avg_price * swap_to_amount
        end as usd_value
    from solana.defi.fact_swaps s
    left join prices pf
        on pf.mint = s.swap_from_mint
        and pf.hour = date_trunc('hour', s.block_timestamp)
    left join prices pt
        on pt.mint = s.swap_to_mint
        and pt.hour = date_trunc('hour', s.block_timestamp)
    where s.block_timestamp >= current_date - 300
        and succeeded
        and swapper = '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj'
)
-- amount bought, avg purchase price, amount sold, avg sale price, amount held, current price, net profits
, t1 as (
    select swapper
    , block_timestamp
    , swap_from_mint as mint
    , swap_from_amount as amount
    , 0 as n_buys
    , 1 as n_sales
    , 0 as amount_bought
    , swap_from_amount as amount_sold
    , -swap_from_amount as net_amount
    , usd_value
    from t0
    union
    select swapper
    , block_timestamp
    , swap_to_mint as mint
    , swap_to_amount as amount
    , 1 as n_buys
    , 0 as n_sales
    , swap_to_amount as amount_bought
    , 0 as amount_sold
    , -swap_to_amount as net_amount
    , usd_value
    from t0
)
select swapper
, mint
, sum(amount_bought) as amount_bought
, sum(amount_sold) as amount_sold
, sum(net_amount) as cur_amount
, sum(n_buys) as n_buys
, sum(n_sales) as n_sales
from t1
group by 1, 2


with t0 as (
    select swap_to_mint as token_address
    , count(distinct swapper) as n_swappers
    from solana.defi.fact_swaps
    where block_timestamp >= current_date - 365
    group by 1
), t1 as (
    select t0.*
    , m.symbol
    , m.name
    from t0
    left join solana.price.ez_asset_metadata m
        on m.token_address = t0.token_address
)
select *
from t1
order by n_swappers desc



    select right(
        split(regexp_replace(split(split(concat(c.text, ' '), 'flipsidecrypto.xyz')[1]::string, '?')[0]::string, '\n', ' '), ' ')[0]::string, 6
    ) as dashboard_slug
    -- , split(regexp_replace(
    --     split(
    --         split(
    --             concat(c.text, ' '), 'flipsidecrypto.xyz'
    --         )[1]
    --         , '?'
    --     )[0], '\\s+', ' '), ' ')
    , coalesce(d.title, 'none') as title
    , coalesce(u.username, 'none') as username
    , f.fname as caster_name
    , left(c.created_at::string, 16) as cast_time
    , sum(case when r.reaction_type = 2 then 1 else 0 end) as n_recasts
    , sum(case when r.reaction_type = 1 then 1 else 0 end) as n_likes
    from external.bronze.farcaster_casts c
    join external.bronze.farcaster_fnames f
        on f.fid = c.fid
    left join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(dashboard_slug, 6)
    left join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    left join external.bronze.farcaster_reactions r
        on r.target_hash = c.hash
    left join external.bronze.farcaster_fnames fr
        on fr.fid = r.fid
    where c.created_at >= '2024-05-01'
        and c.text ilike '%flipsidecrypto.xyz'
    group by 1, 2, 3, 4, 5
    order by cast_time desc


with league as (
  -- this CTE updated by kb
  select c.value:user_id::string as user_id
  , c.value:tag_type::string as tag_type
  , c.value:tag_name::string as tag_name
  , u.record_metadata:CreateTime::int as updated_at
  from crosschain.bronze.data_science_uploads u
  , lateral flatten(
      input => record_content
  ) c
  where record_metadata:key like 'analyst-tag%'
      and tag_type = 'League'
  qualify (
      row_number() over (
          partition by user_id
          order by updated_at desc
      ) = 1
  )
), gold_league as (
    -- this CTE updated by kb
    select *
    from league
    where tag_name = 'Gold League'
), rk_hist0 as (
    select d.id as dashboard_id
    , date_trunc('hour', dbt_updated_at) as hour
    , case when dbt_updated_at <= '2024-05-01 10:00:00' or gl.user_id is not null then 'Gold' else 'Silver' end as league
    , avg(hr.ranking_trending) as rk0
    from bi_analytics.snapshots.hourly_dashboard_rankings hr
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    left join gold_league gl
        on gl.user_id = u.id
    where dbt_updated_at >= '2024-02-20 10:00:00'
        -- and dbt_updated_at <= '2024-05-15 01:00:00'
        and coalesce(u.role, '') <> 'internal'
        and not u.username in (
            'Polaris_9R','dsaber','flipsidecrypto','metricsdao','drethereum','Orion_9R','sam','forgash','nftchance__','danner'
        )
        and hr.ranking_trending <= 500
    group by 1, 2, 3
), labels as (
    select distinct d.id as dashboard_id
    , d.title
    , d.latest_slug
    , concat('https://flipsidecrypto.xyz/', u.username, '/', d.latest_slug) as dashboard_url
    , u.username
    , u.id as user_id
    , case when (
        q.statement like '%VoTpe3tHQ7AjQHMapgSue2HJFAh2cGsdokqN3XqmVSj%'
        or q.statement like '%GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY%'
    ) then 'jupiter' else t.name end as project
    , case when project in ('near'
        , 'solana'
        , 'aptos'
        , 'flow'
        , 'blast'
        , 'sei'
        , 'axelar'
        , 'avalanche'
        , 'vertex'
        , 'thorchain'
        , 'jupiter'
    ) then project else 'non-partner' end as partner_name
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    left join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
        on q.id = qtt.A
    left join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and (t.name = 'vertex' or t.type = 'project')
    where t.name is not null
        or q.statement like '%VoTpe3tHQ7AjQHMapgSue2HJFAh2cGsdokqN3XqmVSj%'
        or q.statement like '%GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY%'
)
, boosts as (
    select 'Near Feb 23' as name, 'near' as project, '2024-02-23 19:00:00' as start_hour, 24 * 3 as n_hours, 30 as top_n, 2 as mult
    union select 'Blast Mar 1' as name, 'blast' as project, '2024-05-01 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    -- union select 'Jup Mar 7' as name, 'jupiter' as project, '2024-03-07 21:00:00' as start_hour, 24 * 1 as n_hours, 40 as top_n, 9 as mult
    union select 'Vertex Mar 14' as name, 'vertex' as project, '2024-03-14 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Near Mar 18' as name, 'near' as project, '2024-03-18 15:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Aptos Mar 19' as name, 'aptos' as project, '2024-03-19 21:00:00' as start_hour, 24 * 1 as n_hours, 40 as top_n, 2 as mult
    union select 'Near Mar 24' as name, 'near' as project, '2024-03-24 21:00:00' as start_hour, 24 * 5 as n_hours, 40 as top_n, 4 as mult
    union select 'Near Mar 24' as name, 'near' as project, '2024-03-24 21:00:00' as start_hour, 24 * 5 as n_hours, 40 as top_n, 4 as mult
    union select 'Aptos Apr 10' as name, 'aptos' as project, '2024-04-10 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 2 as mult
    union select 'Vertex Apr 12' as name, 'vertex' as project, '2024-04-12 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Sei Apr 15' as name, 'sei' as project, '2024-04-15 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 2 as mult
    union select 'Axelar Apr 17' as name, 'axelar' as project, '2024-04-17 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 4 as mult
    union select 'Aptos Apr 24' as name, 'aptos' as project, '2024-04-24 17:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Axelar Apr 27' as name, 'axelar' as project, '2024-04-27 17:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Blast May 3' as name, 'blast' as project, '2024-05-03 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Avalanche May 6' as name, 'avalanche' as project, '2024-05-06 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Flow May 9' as name, 'flow' as project, '2024-05-09 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 4 as mult
    union select 'Near May 12' as name, 'near' as project, '2024-05-12 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 2 as mult
    union select 'Near May 14' as name, 'near' as project, '2024-05-14 16:00:00' as start_hour, 24 * 1 as n_hours, 40 as top_n, 4 as mult
    union select 'Aptos May 15' as name, 'aptos' as project, '2024-05-15 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Aptos May 20' as name, 'aptos' as project, '2024-05-20 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Near May 23' as name, 'near' as project, '2024-05-23 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Aptos May 26' as name, 'aptos' as project, '2024-05-26 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Sei May 30' as name, 'sei' as project, '2024-05-30 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Axelar June 3' as name, 'axelar' as project, '2024-06-03 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Thorchain June 6' as name, 'thorchain' as project, '2024-06-06 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Near June 9' as name, 'near' as project, '2024-06-09 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Kaia June 12' as name, 'kaia' as project, '2024-06-12 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mult
    union select 'Thorchain June 15' as name, 'thorchain' as project, '2024-06-15 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 2 as mult
    union select 'Avalanche June 17' as name, 'avalanche' as project, '2024-06-17 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Thorchain June 23' as name, 'thorchain' as project, '2024-06-23 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 2 as mult
    union select 'Blast June 27' as name, 'blast' as project, '2024-06-27 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mult
    union select 'Axelar Jul 3' as name, 'axelar' as project, '2024-07-03 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mul
    union select 'Thorchain Jul 6' as name, 'thorchain' as project, '2024-07-06 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mul
    union select 'Near Jul 9' as name, 'near' as project, '2024-07-09 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mul
    union select 'Aptos Jul 9' as name, 'aptos' as project, '2024-07-09 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 4 as mul
    union select 'Flow Jul 12' as name, 'flow' as project, '2024-07-12 16:00:00' as start_hour, 24 * 2 as n_hours, 40 as top_n, 4 as mul
    union select 'Lava Jul 12' as name, 'lava' as project, '2024-07-15 16:00:00' as start_hour, 24 * 3 as n_hours, 40 as top_n, 2 as mul
)
, twitter as (
    select d.dashboard_id
    , d.title
    , d.dashboard_url
    , d.username
    , d.project
    , d.partner_name
    , t.created_at::date as tweet_date
    , t.tweet_url
    , t.conversation_id
    , t.impression_count
    , least(10000, t.impression_count) as impression_count_10k_cap
    , b.name
    , b.n_hours
    , b.mult
    from labels d
    join bi_analytics.twitter.tweet t
        on RIGHT(d.latest_slug, 6) = RIGHT(SPLIT(t.clean_url, '?')[0]::string, 6)
    left join boosts b
        on b.project = d.project
        and t.created_at >= dateadd('hours', -12, b.start_hour)
        and t.created_at <= dateadd('hours', 2 + b.n_hours, b.start_hour)
    qualify(
        row_number() over (partition by t.conversation_id order by impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by impression_count desc) = 1
    )
)
, calendar as (
    select distinct tweet_date
    from twitter
)
, project as (
    select distinct project
    from twitter
)
, calendar_partner_boost as (
    select distinct tweet_date
    , project
    , 1 as active_boost
    , mult
    , n_hours
    from twitter
    where name is not null
)
, calendar_partner as (
    select c.tweet_date
    , p.project
    , coalesce(b.active_boost, 0) as active_boost
    , coalesce(b.mult, 0) as mult
    , coalesce(b.n_hours, 0) as n_hours
    , sum(coalesce(t.impression_count, 0)) as impression_count
    , sum(coalesce(t.impression_count_10k_cap, 0)) as impression_count_10k_cap
    , count(distinct t.conversation_id) as n_tweets
    , count(distinct t.dashboard_id) as n_dashboards
    from calendar c
    join project p
        on true
    left join calendar_partner_boost b
        on b.tweet_date = c.tweet_date
        and b.project = p.project
    left join twitter t
        on t.tweet_date = c.tweet_date
        and t.project = p.project
    group by 1, 2, 3, 4, 5
)
, baseline as (
    select project
    , avg(impression_count) as impressions_baseline
    , avg(impression_count_10k_cap) as impression_count_10k_cap
    , avg(n_tweets) as n_tweets_baseline
    , avg(n_dashboards) as n_dashboards_baseline
    from calendar_partner
    where tweet_date >= '2024-02-01'
    group by 1
)
-- select *
-- from calendar_partner
-- where tweet_date >= '2024-02-01'
-- order by tweet_date desc, partner_name
-- select project
-- , sum(1) 
-- select title, dashboard_url, username, count(1) as n_tweets, sum(impression_count) as impression_count from twitter
-- select *
-- where name = 'Avalanche May 6'
-- group by 1, 2, 3
, rk_hist1 as (
    select *
    , row_number() over (partition by hour, league order by rk0) as rk
    from rk_hist0
)
, t0 as (
    select r.dashboard_id
    , r.hour
    , r.hour::date as topn_date
    , r.rk
    , r.league
    , l.user_id
    , case
        when league = 'Gold' and r.rk <= 10 then 1.5
        when league = 'Gold' and r.rk <= 40 then 1
        when coalesce(league, 'Silver') = 'Silver' and r.rk <= 30 then 0.5
        else 0 end as base_amount
    , l.title
    , l.username
    , l.project
    , b.name
    , b.start_hour::date as date
    , b.n_hours
    , b.mult
    , case when coalesce(league, 'Silver') = 'Silver' then 0 else coalesce(b.mult, 0) * base_amount end as boost_amount
    from rk_hist1 r
    join labels l
        on l.dashboard_id = r.dashboard_id
    join boosts b
        on b.project = l.project
        and r.hour >= b.start_hour
        and r.hour < dateadd('hours', b.n_hours, b.start_hour)
    where r.rk <= 40
    qualify(
        row_number() over (partition by r.dashboard_id, r.league, r.hour order by coalesce(b.mult, 0) desc) = 1
    )
)
-- select user_id, sum(base_amount + boost_amount) as total_amount
-- from t0
-- group by 1
-- order by 2 desc


-- select * from t0 where name = 'Blast May 3'
, cost as (
    select name
    , project
    , date
    , (n_hours / 24)::int as n_days
    , mult + 1 as boost
    , sum(base_amount) as base_amount
    , sum(boost_amount) as boost_amount
    from t0
    group by 1, 2, 3, 4, 5
), impressions as (
    select name
    , sum(impression_count) as impression_count
    , sum(impression_count_10k_cap) as impression_count_10k_cap
    , count(1) as n_tweets
    , count(distinct dashboard_id) as n_dashboards
    from twitter
    group by 1
)
-- select project
-- , league
-- , sum(base_amount)
-- from cost
-- group by 1
-- order by 2 desc
, t1 as (
    select c.*
    , c.base_amount + c.boost_amount as total_paid
    , i.impression_count
    , i.n_tweets
    , i.n_dashboards
    , b.impressions_baseline
    , b.impression_count_10k_cap
    , b.n_tweets_baseline
    , b.n_dashboards_baseline
    , round(i.impression_count - (b.impressions_baseline * n_days), 0) as incremental_impressions
    , round(i.impression_count_10k_cap - (b.impression_count_10k_cap * n_days), 0) as incremental_impressions_10k_cap
    , round(i.n_tweets - (b.n_tweets_baseline * n_days), 1) as incremental_tweets
    , round(i.n_dashboards - (b.n_dashboards_baseline * n_days), 1) as incremental_dashboards
    from cost c
    left join impressions i
        on i.name = c.name
    left join baseline b
        on b.project = c.project
    order by date desc
)
select *
from t1


select *
from bi_analytics.twitter.tweet
where conversation_id = '1810260829385924655'




select t.*
, d.title
, tw.impression_count
from bi_analytics.twitter.missing_tweets t
join bi_analytics.twitter.tweet tw 
    on tw.conversation_id = t.conversation_id
join bi_analytics.velocity_app_prod.dashboards d
    on d.id = t.dashboard_id
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
where u.username = 'crypto_guy'



with t0 as (
    select c.value:dashboard_id::string as dashboard_id
    , c.value:conversation_id::string as conversation_id
    , u.record_metadata:CreateTime as created_at
    from crosschain.bronze.data_science_uploads u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'twitter-missing%'
)
select t0.*
, u.username
, d.title
from t0
join bi_analytics.velocity_app_prod.dashboards d
    on d.id = t0.dashboard_id
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
order by t0.created_at desc






with twitter_accounts as (
    select twitter_id
    , twitter_handle
    , account_type
    , ecosystems[0]::string as ecosystem
    , n_followers
    from crosschain.bronze.twitter_accounts
    where not twitter_id in (
        '1314075720'
        , '925712018937712640'
        , '59119959'
        , '1445056111753584644'
        , '791390945656856577'
    )
    qualify(
        row_number() over (partition by twitter_id order by updated_at desc) = 1
    )
)
select date_trunc('week', t.created_at) as week
, count(distinct t.conversation_id) as n
-- distinct t.conversation_id
-- , q.quote_tweet_id
-- , t.conversation_id
-- , concat('https://x.com/jpthor/status/',q.quote_tweet_id) as tweet_url
-- , taq.*
from bi_analytics.twitter.tweet t
join bi_analytics.twitter.retweets q
    on q.tweet_id = t.conversation_id
join twitter_accounts taq
    on taq.twitter_id = q.user_id
where t.created_at >= '2024-06-01'
    and twitter_handle ilike '%jpthor%'
group by 1
order by 1


with t0 as (
    select c.value:dashboard_id::string as dashboard_id
    , *
    from crosschain.bronze.data_science_uploads u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'dashboard-bans%'
), t1 as (
    select t0.*
    from t0
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = t0.dashboard_id
    where d.latest_slug ilike '%i9M4jq%'
        or d.latest_slug ilike '%i9M4jq%'
)
select *
from t1


select d.latest_slug
, p.*
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.velocity_app_prod.profiles p
    on p.id = d.profile_id
where latest_slug ilike '%MrQx1x'
    and p.type = 'team'


select * from bi_analytics_dev.silver.team_bans


with t0 as (
    select d.id as dashboard_id
    , t.created_at
    , t.conversation_id
    , impression_count
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    QUALIFY(
        row_number() over (partition by t.conversation_id order by i.impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by i.impression_count desc) = 1
    )
)
select left(date_trunc('month', created_at)::string, 10) as month
, sum(impression_count) as x_impressions
from t0
group by 1
order by 1 desc


select *
from solana.defi.fact_swaps
where block_timestamp >= '2024-05-01'
    and swapper = 'JjjJQQNFTXyfeszqPn7T8iEihfjbgXefnRHvkDs7VtD'
    and mint = '5mbK36SZ7J19An8jFochhQS4of8g6BwUjbeCSxBSoWdp'

select *
from solana.defi.fact_swaps
where block_timestamp >= '2024-05-01'
    and tx_id = '4FhxnpX6f7JfxM9Z8sUZZUHpi6NanJRoAdQvavXnRAx444JPsA7xvJm1btPGF4dQ3fRPUZSerqcjfDb5fWVwaNX1'



select *
from solana.core.fact_transfers
where block_timestamp >= '2024-05-01'
    and (
        tx_from = 'JjjJQQNFTXyfeszqPn7T8iEihfjbgXefnRHvkDs7VtD'
        or tx_to = 'JjjJQQNFTXyfeszqPn7T8iEihfjbgXefnRHvkDs7VtD'
    )
    and mint = '5mbK36SZ7J19An8jFochhQS4of8g6BwUjbeCSxBSoWdp'
limit 10000


select decoded_instruction:name::string
, count(1)
from solana.core.fact_decoded_instructions i
left join solana.core.dim_labels l
    on l.address = case when decoded_instruction:name::string = 'takeLoanV3' then decoded_instruction:accounts[3]:pubkey::string else decoded_instruction:accounts[11]:pubkey::string end
where date >= current_date - 4
    and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
group by 1
order by 2 desc


with t0 as (
    select i.tx_id
    , i.block_timestamp
    , decoded_instruction:name::string as name
    , i.decoded_instruction:args:principalLamports::int * pow(10, -9) as amount
    , di.value:name as acct_name
    , di.value:pubkey as acct_pubkey
    , di.seq
    , i.decoded_instruction
    from solana.core.fact_decoded_instructions i
    , lateral flatten(input => decoded_instruction:accounts) as di
    where block_timestamp >= current_date - 1
        and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
    -- order by block_timestamp, tx_id
    -- limit 100
)
, t1 as (
    select o.tx_id
    , o.block_timestamp
    , o.seq
    , o.amount
    , o.acct_pubkey as escrow
    , l.acct_pubkey as lender
    from t0 o
    join t0 l
        on l.tx_id = o.tx_id
        and l.seq = o.seq
        and l.acct_name = 'lender'
    where o.name = 'offerLoan'
        and o.acct_name = 'escrow'
)
select *
from t1


with t0 as (
    select i.tx_id
    , i.block_timestamp
    , i.block_timestamp::date as date
    , i.decoded_instruction
    , i.decoded_instruction:name::string as name
    , di.value:pubkey::string as borrower
    , i.decoded_instruction:args:floor::int as floor
    , i.decoded_instruction:args:terms:apyBps::int as apyBps
    , i.decoded_instruction:args:terms:duration::int as duration
    , dateadd('seconds', duration, i.block_timestamp) as due_date
    , case when due_date < current_timestamp then 1 else 0 end as is_due
    , i.decoded_instruction:args:terms:principal::int * pow(10, -9) as principal
    from solana.core.fact_decoded_instructions i
    , lateral flatten(input => decoded_instruction:accounts) as di
    -- where i.block_timestamp >= current_date - 30
    where i.block_timestamp >= '2022-01-01'
        -- and i.block_timestamp <= '2024-04-19'
        and i.program_id = 'JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp'
        -- and tx_id = '3SjtWGFx27DY1ZcJL1VpbRGVDqTkAPPfwSuE68LabpvGGSYXFCAyHvt2UYM3gbyy6LL2Mn3gw2mpecsvUYVtr3vc'
        and di.value:name::string = 'borrower'
        and borrower is not null
)
-- select *
-- from t0
-- where borrower = 'zPgpXRyW4VtDgv5SCwNMXF8UKP1DwkXuhk6tPFKXNkS'

, t1 as (
    select t0.*
    , di.value:pubkey::string as loanAccount
    from t0
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = case when name = 'reborrow' then 'newLoanAccount' else 'loanAccount' end
        and loanAccount is not null
)
-- select * from t1
, repaid as (
    select loanAccount
    , max(block_timestamp) as time_repaid
    from t1
    where name in ('repay','repay2','sellRepay','listCollateral')
    group by 1
)
, t1b as (
    select t1.*
    , di.value:pubkey::string as lender
    from t1
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = case when name = 'reborrow' then 'newLender' else 'lender' end
        and lender is not null
)
, t2 as (
    select t1b.*
    , di.value:pubkey::string as lendAuthority
    from t1b
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = case when name = 'reborrow' then 'newLendAuthority' else 'lendAuthority' end
        and lendAuthority is not null
)
select date_trunc('week', date) as week
, sum(principal) as tot_principal
, sum(case when lender = 'kcitfabbdz4KFNPn8RJjUEg1rD36SJdFwJg8imo2Rrg' then principal else 0 end) as my_principal
, sum(case when lender = 'kcitfabbdz4KFNPn8RJjUEg1rD36SJdFwJg8imo2Rrg' then 0 else principal end) as other_principal
, round(100 * my_principal / tot_principal, 2) as pct_me
from t2
group by 1





with rk_hist0 as (
    select d.id as dashboard_id
    , date_trunc('hour', dbt_updated_at) as hour
    , avg(hr.ranking_trending) as rk0
    from bi_analytics.snapshots.hourly_dashboard_rankings hr
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where dbt_updated_at >= '2023-10-24 15:00:00'
        and coalesce(u.role, '') <> 'internal'
        and not u.username in (
            'Polaris_9R','dsaber','flipsidecrypto','metricsdao','drethereum','Orion_9R','sam','forgash','nftchance__','danner'
        )
        and hr.ranking_trending <= 100
    group by 1, 2
), t0 as (
    select d.title
    , d.latest_slug
    , r.*
    , row_number() over (partition by hour order by rk0, dashboard_id) as rk
    from rk_hist0 r
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = r.dashboard_id
    where (d.latest_slug ilike '%Mr10hz%' or d.latest_slug ilike '%Tsz9HX%')
)
select *
from t0
order by dashboard_id, hour

with t0 as (
    select swap_to_mint as mint
    , count(distinct swapper) as n
    from solana.defi.fact_swaps
    where block_timestamp >= current_date - 30
    group by 1
)
select t0.*
, m.*
from t0
left join solana.price.ez_asset_metadata m
    on m.token_address = t0.mint
order by n desc


select *
from solana.price.ez_prices_hourly
limit 10



WITH p0 as (
    SELECT p.token_address
    , hour::date as date
    , AVG(price) as token_price
    from solana.price.ez_prices_hourly p
    WHERE hour >= current_date - 180
        and is_imputed = FALSE
    group by 1, 2
)
, t0 as (
    SELECT s.block_timestamp::date as date
    , swap_from_mint
    , swap_to_mint
    , sum(swap_from_amount) as swap_from_amount
    , sum(swap_to_amount) as swap_to_amount
    from solana.defi.fact_swaps s
    where s.block_timestamp >= current_date - 180
        and s.succeeded
    group by 1, 2, 3
), t1 as (
    select date
    , swap_from_mint as mint
    , sum(swap_from_amount) as amount
    from t0
    group by 1, 2
    union
    select date
    , swap_to_mint as mint
    , sum(swap_to_amount) as amount
    from t0
    group by 1, 2
), t2 as (
    select t1.mint
    , case 
        when t1.mint = '3B5wuUrMEi5yATD7on46hKfej3pfmd7t1RKgrsN3pump' then 'BILLY'
        when tok.symbol is null then mint
        else UPPER(SPLIT(tok.symbol, '-')[0]) end as symbol
    , sum(amount * token_price) as amount_usd
    from t1
    left join solana.core.dim_tokens tok
        on tok.token_address = t1.mint
    left join p0
        on p0.token_address = mint
        and p0.date = t1.date
    group by 1, 2
), t3 as (
    select t2.*
    , case when mint = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263' then 'BONK' else 'Other Tokens' end as color
    , row_number() over (order by amount_usd desc) as rk
    from t2
)
select *
from t3
where rk <= 100
order by rk


select * from 

with t0 as (
    select twitter_id
    , twitter_handle
    , account_type
    , ecosystems[0]::string as ecosystem
    , n_followers
    from crosschain.bronze.twitter_accounts
    where not twitter_id in (
        '1314075720'
        , '925712018937712640'
        , '59119959'
        , '1445056111753584644'
        , '791390945656856577'
    )
    -- and ecosystem[0]::string = 'Avalanche'
    qualify(
        row_number() over (partition by twitter_id order by updated_at desc) = 1
    )
)
select * from t0
select ecosystem
, count(1) as n
from t0
group by 1
order by 2 desc




SELECT p.token_address
, hour::date as date
, AVG(price) as token_price
from solana.price.ez_prices_hourly p
WHERE hour >= current_date - 30
    and is_imputed = FALSE
    and token_address = 'DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263'
group by 1, 2
order by date desc





select * from solana.nft.fact_nft_mints
where block_timestamp >= '2024-01-01'
    and tx_id = 'YpdqsHHSo25jF87cRbH6A44b5kb63yBggAhe1kHvUNhbby52KBamhxxtq5iZir3Ycs1k2AwLnXEAo1oybZyTfon'
    and mint = '5J1LABhJPTMqwyhrRutPpUeo6CqsWEDLYQh8KKbjrGS4'

-- dashboard id | current rank | title | username | league | # of queries | age of user | 
with t0 as (
    select created_by_id as user_id
    , count(distinct case when parent_query_id is null then id else null end) as n_queries
    , count(distinct id) as n_own_queries
    from bi_analytics.velocity_app_prod.queries
    group by 1
), t1 as (
    select d.id as dashboard_id
    , d.title
    , dr.ranking_trending
    , u.username
    , u.id as user_id
    , least(100, datediff('hours', u.created_at, d.published_at) / 24.0) as days_to_publish
    , least(100, datediff('hours', u.created_at, current_timestamp) / 24.0) as account_age_days
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    left join bi_analytics.content_rankings.dashboard_rankings dr
        on dr.dashboard_id = d.id
), t2 as (
    select dashboard_id
    , min(ranking_trending) as top_ranking
    , sum(case when ranking_trending <= 40 then 1 else 0 end) as n_hours_in_top_40
    , sum(case when ranking_trending <= 100 then 1 else 0 end) as n_hours_in_top_100
    from bi_analytics.snapshots.hourly_dashboard_rankings
    where dbt_updated_at >= current_date - 60
    group by 1
)
select t1.*
, t2.top_ranking
, t2.n_hours_in_top_40
, t2.n_hours_in_top_100
, t0.n_queries
, t0.n_own_queries
from t1
left join t2
    on t2.dashboard_id = t1.dashboard_id
left join t0
    on t0.user_id = t1.user_id
where n_hours_in_top_100 >= 3





with t0 as (
    select c.value:twitter_id::string as twitter_id
    , c.value:twitter_handle::string as twitter_handle
    , c.value:ecosystem::string as ecosystem
    , c.value:month::string as month
    , c.value:score::int as score
    , coalesce(c.value:deleted::boolean, false) as deleted
    , u.record_metadata:CreateTime::int as CreateTime
    from crosschain.bronze.data_science_uploads u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'user-community-score%'
        and CreateTime >= 1722264540000
), t1 as (
    select t0.*
    from t0
    where twitter_id is not null
    qualify(
        row_number() over (partition by twitter_id order by CreateTime desc) = 1
    )
)
select twitter_id
, twitter_handle
, ecosystem
, month
, score
from t1
where not deleted

from crosschain.bronze.twitter_accounts

with t0 as (
    select distinct t.twitter_id
    , t.twitter_handle
    , t.account_type
    , t.n_followers
    , t.updated_at
    , lower(c.value::string) as ecosystem
    from crosschain.bronze.twitter_accounts t
    , lateral flatten(
        input => ecosystems
    ) c
), t1 as (
    select distinct twitter_id
    , ecosystem
    from t0
), t2 as (
    select twitter_id
    , listagg(ecosystem, ', ') within group (order by ecosystem) as ecosystem_list
    from t1
    group by 1
)
, t3 as (
    select twitter_id
    , parse_json(
        concat(
            '["', 
            replace(ecosystem_list, ',', '", "'), 
            '"]'
        )
    ) 
    as ecosystems
    from t2
)
select *
, ecosystems[0]::string as eco 
, ecosystems[1]::string as eco1 from t3
order by 1


select *
from bi_analytics_dev.twitter.twitter_accounts ta
from bi_analytics_dev.silver.user_community_scores ucs
    on ucs.twitter_id = ta.twitter_id


select created_at::date as date
, platform
, sum(impression_count) as impression_count
from bi_analytics.twitter.tweet
where created_at >= current_date - 30
    and tweet_type = 'Dashboard'
group by 1, 2

with t0 as (
    select date_trunc('month', created_at) as month
    , conversation_id
    , platform
    , max(impression_count) as impression_count
    from bi_analytics.twitter.tweet
    where month >= '2023-06-01'
        and tweet_type = 'Dashboard'
    group by 1, 2, 3
), t1 as (
    select platform
    , month
    , sum(impression_count) as tot_impressions
    , count(1) as n_tweets
    , sum(case when impression_count >= 3000 then 1 else 0 end) as tweets_3k_impressions
    , avg(case when impression_count >= 3000 then 1 else 0 end) as avg_tweets_3k_impressions
    , sum(case when impression_count >= 10000 then 1 else 0 end) as tweets_10k_impressions
    , avg(case when impression_count >= 10000 then 1 else 0 end) as avg_tweets_10k_impressions
    , sum(case when impression_count >= 25000 then 1 else 0 end) as tweets_25k_impressions
    , avg(case when impression_count >= 25000 then 1 else 0 end) as avg_tweets_25k_impressions
    from t0
    group by 1, 2
), t2 as (
    select f.month
    , f.tot_impressions as flip_impressions
    , d.tot_impressions as dune_impressions
    , round(flip_impressions * 100 / dune_impressions) as ratio_impressions
    , f.n_tweets as flip_n_tweets
    , d.n_tweets as dune_n_tweets
    , round(flip_n_tweets * 100 / dune_n_tweets) as ratio_n_tweets
    , f.tweets_3k_impressions as flip_tweets_3k_impressions
    , d.tweets_3k_impressions as dune_tweets_3k_impressions
    , round(flip_tweets_3k_impressions * 100 / dune_tweets_3k_impressions) as ratio_tweets_3k_impressions
    , f.avg_tweets_3k_impressions as flip_avg_3k_impressions
    , d.avg_tweets_3k_impressions as dune_avg_3k_impressions
    , round(flip_avg_3k_impressions * 100 / dune_avg_3k_impressions) as ratio_avg_3k_impressions
    , f.tweets_25k_impressions as flip_tweets_25k_impressions
    , d.tweets_25k_impressions as dune_tweets_25k_impressions
    , round(flip_tweets_25k_impressions * 100 / dune_tweets_25k_impressions) as ratio_tweets_25k_impressions
    , f.avg_tweets_25k_impressions as flip_avg_25k_impressions
    , d.avg_tweets_25k_impressions as dune_avg_25k_impressions
    , round(flip_avg_25k_impressions * 100 / dune_avg_25k_impressions) as ratio_avg_25k_impressions
    from t1 f
    join t1 d
        on d.month = f.month
    where f.platform = 'Flipside'
        and d.platform = 'Dune'
)
select *
from t2

with t0 as (
select *
, ecosystems[0]::string as ecosystem
, row_number() over (
    partition by ecosystem
    order by score desc
    , twitter_id
) as rn
from bi_analytics.silver.user_community_scores ucs
join bi_analytics.twitter.twitter_accounts ta
    on ucs.twitter_id::string = ta.twitter_id::string
), t1 as (
    select ecosystem
    , sum(score * 2 / 15) as score
    from t0
    where rn <= 50
    group by 1
)
select *
from t1
order by score desc

select *
from bi_analytics.velocity_app_prod.dashboards
where latest_slug ilike '%y35UYf%'


select ecosystem
, count(1) as n
from bi_analytics.twitter.core_audience
group by 1
order by 2 desc


select *
from bi_analytics.twitter.tweet
limit 10


with mx as (
    select max(created_at) as created_at
    from bi_analytics.twitter.tweet
) , ambassador as (
    select *
    from bi_analytics_dev.silver.ambassador
), rk_hist0 as (
    select d.id as dashboard_id
    , date_trunc('hour', dbt_updated_at) as hour
    , avg(hr.ranking_trending) as rk0
    from bi_analytics.snapshots.hourly_dashboard_rankings hr
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where dbt_updated_at >= '2023-10-24 15:00:00'
        and coalesce(u.role, '') <> 'internal'
        and not u.username in (
            'Polaris_9R','dsaber','flipsidecrypto','metricsdao','drethereum','Orion_9R','sam','forgash','nftchance__','danner'
        )
        and hr.ranking_trending <= 100
    group by 1, 2
), rk_hist1 as (
    select *
    , row_number() over (partition by hour order by rk0 asc) as rk
    from rk_hist0
), rk_hist2 as (
    select dashboard_id
    , min(rk) as top_ranking
    , sum(case when rk <= 40 then 1 else 0 end) as n_hours_in_top_40
    from rk_hist1
    group by 1
), labels as (
    SELECT c.value:dashboard_id::string as dashboard_id
    , c.value:tag::string as dashboard_tag
    from crosschain.bronze.data_science_uploads
    , LATERAL FLATTEN(
        input => record_content
    ) c
    WHERE record_metadata:key LIKE 'dashboard-tags%'
), imp as (
    select conversation_id
    , max(impression_count) as impression_count
    from bi_analytics.twitter.tweet t
    group by 1
), t0 as (
    SELECT d.id as dashboard_id
    , t.id as tweet_id
    , t.conversation_id::string as conversation_id
    , coalesce(d.title, t.tweet_type) as title
    , coalesce(d.latest_slug, t.clean_url) as latest_slug
    , coalesce(tm.slug, u.username, t.tweet_type) as username
    , u.id as user_id
    , i.impression_count
    , t.start_timestamp
    , case when u.user_name is null then t.tweet_url else CONCAT('https://twitter.com/',u.user_name,'/status/',t.id) end as tweet_url
    , t.created_at::date as tweet_date
    from bi_analytics.twitter.tweet t
    left join bi_analytics.velocity_app_prod.dashboards d
        on RIGHT(d.latest_slug, 6) = RIGHT(SPLIT(t.clean_url, '?')[0]::string, 6)
    join imp i
        on i.conversation_id = t.conversation_id
    left join bi_analytics.twitter.user tu
        on tu.id = t.user_id
    left join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    left join bi_analytics.velocity_app_prod.profiles p
        on p.id = d.profile_id
    left join bi_analytics.velocity_app_prod.teams tm
        on tm.profile_id = p.id
    WHERE NOT coalesce(d.id, '') in (
        SELECT dashboard_id from labels WHERE dashboard_tag = 'bot'
    ) and (d.id is not null or t.tweet_type = 'Flipside Science Dashboard')
    QUALIFY(
        row_number() over (partition by t.conversation_id order by i.impression_count desc) = 1
        and row_number() over (partition by t.tweet_url order by i.impression_count desc) = 1
    )
), t0d as (
    SELECT DISTINCT dashboard_id
    from t0
), t1 as (
    SELECT d.id as dashboard_id
    , case when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
        or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
        or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
        or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%'
        then 'Axelar' else INITCAP(t.name) end as chain
    , d.title
    , d.latest_slug
    , u.username
    , u.id as user_id
    , COUNT(DISTINCT q.id) as n_queries
    from bi_analytics.velocity_app_prod.dashboards d
    join t0d
        on t0d.dashboard_id = d.id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    -- join bi_analytics.velocity_app_prod._dashboards_to_queries dtq
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    -- join bi_analytics.velocity_app_prod._queries_to_tags qtt
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
        on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and (t.type = 'project' or t.name = 'vertex' or t.name ilike 'kaia' or t.name ilike 'Klatyn')
    group by 1, 2, 3, 4, 5, 6
)
, t2 as (
    SELECT *
    , row_number() over (
        partition by dashboard_id
        order by
        n_queries desc
        , case when chain in (
            'Aptos'
            , 'Avalanche'
            , 'Axelar'
            , 'Base'
            , 'Blast'
            , 'Flow'
            , 'Kaia'
            , 'Near'
            , 'Sei'
            , 'Solana'
            , 'Thorchain'
            , 'Vertex'
        ) then 1 when chain = 'Ethereum' then 2 else 3 end
        , chain
    ) as rn
    , SUM(n_queries) over (partition by dashboard_id) as tot_queries
    , n_queries / tot_queries as pct
    from t1
), tc0 as (
    SELECT t2.user_id
    , t2.username
    , t2.chain
    , SUM(pct) as tot_pct
    from t2
    join t0d
        on t0d.dashboard_id = t2.dashboard_id
    group by 1, 2, 3
), tc1 as (
    SELECT *
    , row_number() over (
        partition by user_id
        order by
        tot_pct desc
        , case when chain in (
            'Aptos'
            , 'Avalanche'
            , 'Axelar'
            , 'Base'
            , 'Blast'
            , 'Flow'
            , 'Kaia'
            , 'Near'
            , 'Sei'
            , 'Solana'
            , 'Thorchain'
            , 'Vertex'
        ) then 1 when chain = 'Ethereum' then 2 else 3 end
        , chain
    ) as rn
    , SUM(tot_pct) over (partition by user_id) as tot_pct_2
    , tot_pct / tot_pct_2 as pct
    from tc0
), tc as (
    SELECT tc1a.user_id
    , tc1a.username
    , CONCAT(
        tc1a.chain
        , case when tc1b.chain is null then '' else CONCAT(' + ', tc1b.chain) end
        , case when tc1c.chain is null then '' else CONCAT(' + ', tc1c.chain) end
    ) as user_chain
    from tc1 tc1a
    left join tc1 tc1b
        on tc1b.user_id = tc1a.user_id
        and tc1b.rn = 2
        and tc1b.pct > 0.25
    left join tc1 tc1c
        on tc1c.user_id = tc1a.user_id
        and tc1c.rn = 3
        and tc1c.pct > 0.25
    WHERE tc1a.rn = 1
), t3 as (
    SELECT t0.conversation_id as tweet_id
    , t0.conversation_id
    , t0.impression_count
    , t0.tweet_url
    , t0.tweet_date
    , t0.start_timestamp
    , t0.title
    , t0.latest_slug
    , t0.user_id
    , t0.username
    , rh.top_ranking
    , rh.n_hours_in_top_40
    , dr.ranking_trending
    , coalesce(dr.sybil_penalty_mult, 1) as sybil_penalty_mult
    , COALESCE(tc.user_chain, 'Ethereum') as user_chain
    , CONCAT(
        case when t2a.chain is null then '' else t2a.chain end
        -- , ''
        , case when t2b.chain is null then '' else CONCAT(' + ', t2b.chain) end
        , case when t2c.chain is null then '' else CONCAT(' + ', t2c.chain) end
    ) as chain
    , case 
        when (
            (tweet_date >= '2023-08-27' and tweet_date <= '2023-09-04') 
        ) and t2a.user_id in ('",paste0(fpl_users$user_id, collapse="','"),"')
        then 'FPL S2' 
        when (
            (tweet_date >= '2023-09-10' and tweet_date <= '2023-09-18') 
            or (tweet_date >= '2023-09-24' and tweet_date <= '2023-10-02') 
            or (tweet_date >= '2023-10-08' and tweet_date <= '2023-10-16')
            or (tweet_date >= '2023-09-10' and tweet_date <= '2023-10-16')
        ) and t2a.user_id in ('",paste0(fpl_users[qualified == 1]$user_id, collapse="','"),"')
        then 'FPL S2'
        when (
            (tweet_date >= '2023-06-11' and tweet_date <= '2023-06-19') 
        ) and t2a.user_id in ('",paste0(fpl_users_s1$user_id, collapse="','"),"')
        then 'FPL S1'
        when (
            (tweet_date >= '2023-06-18' and tweet_date <= '2023-07-10')
        ) and t2a.user_id in ('",paste0(fpl_users_s1[qualified == 1]$user_id, collapse="','"),"')
        then 'FPL S1' else 'Other' end as segment
    from t0
    left join t2 t2a
        on t2a.dashboard_id = t0.dashboard_id
        and t2a.rn = 1
    left join tc
        on tc.user_id = t2a.user_id
    left join t2 t2b
        on t2b.dashboard_id = t0.dashboard_id
        and t2b.rn = 2
        and t2b.pct > 0.25
    left join t2 t2c
        on t2c.dashboard_id = t0.dashboard_id
        and t2c.rn = 3
        and t2c.pct > 0.25
    left join rk_hist2 rh
        on rh.dashboard_id = t0.dashboard_id
    left join bi_analytics.content_rankings.dashboard_rankings dr
        on dr.dashboard_id = t0.dashboard_id
), t4 as (
    SELECT tweet_id as conversation_id
    , COUNT(DISTINCT user_id) as n_likes
    , avg(case when user_followers <= 150 then 1 else 0 end) as avg_small_acct
    , sum(case when user_followers <= 150 then 1 else 0 end) as tot_small_acct
    from bi_analytics.twitter.likes
    group by 1
)
SELECT t3.*
, greatest(0, least(1, 3 * (avg_small_acct - 0.4))) as avg_small_mult
, greatest(0, least(1, (tot_small_acct - 5) / 30)) as tot_small_mult
, case when a.user_id is not null then 1 else 1 - (avg_small_mult * tot_small_mult) end as twitter_sybil_mult
, round(1 - COALESCE(twitter_sybil_mult, 1), 2) as twitter_sybil_flag
, DATEDIFF('days', tweet_date, CURRENT_DATE) as days_ago
, COALESCE(t4.n_likes, 0) as n_likes
, mx.created_at
from t3
left join ambassador a
    on a.user_id = t3.user_id
left join t4
    on t4.conversation_id = t3.conversation_id
join mx on true


with t0 as (
    select program_id
    , count(1) as n_tx
    from solana.core.fact_events
    where block_timestamp >= current_date - 1
        and program_id ilike 'jup%'
    group by 1
    order by 2 desc
)
, t1 as (
    select program_id
    , count(1) as n_decoded_tx
    from solana.core.ez_events_decoded
    where block_timestamp >= current_date - 1
        and program_id ilike 'jup%'
    group by 1
    order by 2 desc
)
, t2 as (
    select t0.*
    , coalesce(t1.n_decoded_tx, 0) as n_decoded_tx
    from t0
    left join t1
        on t1.program_id = t0.program_id
)
select *
from t2
left join solana.core.dim_labels l
    on l.address = t2.program_id


select u.username
, a.*
from bi_analytics.silver.ambassador a
join bi_analytics.velocity_app_prod.users u
    on u.id = a.user_id
where a.status = 'analyst'
-- group by 1
order by ecosystem


with t0 as (
    select t.conversation_id
    , t.user_id as twitter_id
    , d.id as dashboard_id
    , d.created_by_id as flipside_id
    , u.username as flipside_username
    , coalesce(tu.user_name, '') as twitter_handle
    , max(impression_count) as impression_count
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    left join bi_analytics.twitter.user tu
        on tu.id = t.user_id
    group by 1, 2, 3, 4, 5, 6
), t1 as (
    select flipside_id
    , twitter_id
    , flipside_username
    , twitter_handle
    , count(distinct conversation_id) as n_tweets
    , count(distinct dashboard_id) as n_dashboards
    , sum(impression_count) as impression_count
    from t0
    group by 1, 2, 3, 4
), chains as (
    select q.created_by_id as flipside_id
    , t.name
    , count(distinct q.id) as n_queries
    from bi_analytics.velocity_app_prod.queries q
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
        on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and (t.type = 'project' or t.name = 'vertex' or t.name ilike 'kaia' or t.name ilike 'klatyn')
    where q.created_at >= '2024-01-01'
    group by 1, 2
), tot as (
    select q.created_by_id as flipside_id
    , count(distinct q.id) as tot_queries
    from bi_analytics.velocity_app_prod.queries q
    where q.created_at >= '2024-01-01'
    group by 1
), c0 as (
    select c.*
    , t.tot_queries
    , c.n_queries / t.tot_queries as pct_queries
    , row_number() over (partition by t.flipside_id order by n_queries desc) as rk
    from chains c
    join tot t 
        on t.flipside_id = c.flipside_id
), c1 as (
    select c0a.flipside_id
    , concat(c0a.name, case when c0b.name is null then '' else concat(',', c0b.name) end, case when c0c.name is null then '' else concat(',', c0c.name) end) as ecosystems
    from c0 c0a
    left join c0 c0b
        on c0b.flipside_id = c0a.flipside_id
        and c0b.rk = 2
        and c0b.pct_queries >= 0.25
    left join c0 c0c
        on c0c.flipside_id = c0a.flipside_id
        and c0c.rk = 3
        and c0c.pct_queries >= 0.25
    where c0a.rk = 1
)
select t1.*
, s.score
, a.*
, c1.ecosystems
from t1
left join bi_analytics.silver.user_community_scores s
    on s.twitter_id = t1.twitter_id
left join bi_analytics.twitter.twitter_accounts a
    on a.twitter_id = s.twitter_id
left join c1
    on c1.flipside_id = t1.flipside_id
order by n_dashboards desc




with t0 as (
    select twitter_id
    , sum(score) as score
    from bi_analytics.silver.user_community_scores_monthly
    where month >= '2024-03'
    group by 1
)
-- tweets
, t1 as (
    select platform
    , a.ecosystems[0]::string as ecosystem
    , a.twitter_handle
    , coalesce(t0.score, 0) as score
    , count(distinct t.conversation_id) as n_tweets
    , sum(impression_count) as impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.twitter.twitter_accounts a
        on a.twitter_id = t.user_id
        and a.account_type != 'flipside'
    left join t0
        on t0.twitter_id = t.user_id
    where t.created_at >= '2023-01-01'
        and tweet_type = 'Dashboard'
        and platform = 'Flipside'
    group by 1, 2, 3, 4
)
-- retweets
, t2 as (
    select platform
    , a.ecosystems[0]::string as ecosystem
    , a.twitter_handle
    , coalesce(t0.score, 0) as score
    , count(distinct t.conversation_id) as n_rts
    , sum(t.impression_count) as impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.twitter.retweets r
        on r.tweet_id = t.conversation_id
    join bi_analytics.twitter.twitter_accounts a
        on a.twitter_id = r.user_id
        and a.account_type != 'flipside'
    left join t0
        on t0.twitter_id = r.user_id
    where t.created_at >= '2023-01-01'
        and tweet_type = 'Dashboard'
        and platform = 'Flipside'
    group by 1, 2, 3, 4
)
-- quotes
, t3 as (
    select platform
    , a.ecosystems[0]::string as ecosystem
    , a.twitter_handle
    , coalesce(t0.score, 0) as score
    , count(distinct t.conversation_id) as n_qts
    , sum(q.impression_count) as impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.twitter.quotes q
        on q.tweet_id = t.conversation_id
    join bi_analytics.twitter.twitter_accounts a
        on a.twitter_id = q.user_id
        and a.account_type != 'flipside'
    left join t0
        on t0.twitter_id = q.user_id
    where t.created_at >= '2023-01-01'
        and tweet_type = 'Dashboard'
        and platform = 'Flipside'
    group by 1, 2, 3, 4
)
, t4 as (
    select a.twitter_handle
    , a.ecosystems[0]::string as ecosystem
    , coalesce(t0.score, 0) as score
    , coalesce(t1.n_tweets, 0) as n_tweets
    , coalesce(t2.n_rts, 0) as n_rts
    , coalesce(t3.n_qts, 0) as n_qts
    , n_tweets + n_rts + n_qts as tot_engagements
    from bi_analytics.twitter.twitter_accounts a
    left join t0
        on t0.twitter_id = a.twitter_id
    left join t1
        on t1.ecosystem = a.ecosystem
        and t1.twitter_handle = a.twitter_handle
    left join t2
        on t2.ecosystem = a.ecosystem
        and t2.twitter_handle = a.twitter_handle
    left join t3
        on t3.ecosystem = a.ecosystem
        and t3.twitter_handle = a.twitter_handle
)
select t4.*
from t4
order by tot_engagements desc

select case when tx_to = 'DYK1gdufQPMvQ878zZ64v4mMJL24ReS9CXrn1Hhz6vxh' then amount else -amount end as net_amount
, t.*
from solana.core.fact_transfers t
left join solana.defi.fact_swaps s
    on s.block_timestamp >= '2024-05-01'
    and s.block_timestamp = t.block_timestamp
    and s.tx_id = t.tx_id
where t.block_timestamp >= '2024-05-01'
    and mint = '5mbK36SZ7J19An8jFochhQS4of8g6BwUjbeCSxBSoWdp'
    and (
        tx_to = 'DYK1gdufQPMvQ878zZ64v4mMJL24ReS9CXrn1Hhz6vxh'
        or tx_from = 'DYK1gdufQPMvQ878zZ64v4mMJL24ReS9CXrn1Hhz6vxh'
    )
    and s.tx_id is null
    and net_amount < 0
order by block_timestamp


with t0 as (
    select twitter_id
    , sum(score) as score
    from bi_analytics.silver.user_community_scores_monthly
    where month >= '2024-01'
    group by 1
)
, nt as (
    select u.username
    , count(distinct t.conversation_id) as n_tweets
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where t.created_at >= current_date - 90
        and tweet_type = 'Dashboard'
    group by 1
)
-- tweets
, t1 as (
    select u.username
    , a.ecosystems[0]::string as ecosystem
    , a.twitter_handle
    , coalesce(t0.score, 0) as score
    , count(distinct t.conversation_id) as n_tweets
    , sum(impression_count) as impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.twitter.twitter_accounts a
        on a.twitter_id = t.user_id
        and a.account_type != 'flipside'
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    left join t0
        on t0.twitter_id = t.user_id
    where t.created_at >= current_date - 90
        and tweet_type = 'Dashboard'
    group by 1, 2, 3, 4
)
-- retweets
, t2 as (
    select u.username
    , a.ecosystems[0]::string as ecosystem
    , a.twitter_handle
    , coalesce(t0.score, 0) as score
    , count(distinct t.conversation_id) as n_rts
    , sum(t.impression_count) as impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.twitter.retweets r
        on r.tweet_id = t.conversation_id
    join bi_analytics.twitter.twitter_accounts a
        on a.twitter_id = r.user_id
        and a.account_type != 'flipside'
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    left join t0
        on t0.twitter_id = r.user_id
    where t.created_at >= current_date - 90
        and tweet_type = 'Dashboard'
    group by 1, 2, 3, 4
)
-- quotes
, t3 as (
    select u.username
    , a.ecosystems[0]::string as ecosystem
    , a.twitter_handle
    , coalesce(t0.score, 0) as score
    , count(distinct t.conversation_id) as n_qts
    , sum(q.impression_count) as impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.twitter.quotes q
        on q.tweet_id = t.conversation_id
    join bi_analytics.twitter.twitter_accounts a
        on a.twitter_id = q.user_id
        and a.account_type != 'flipside'
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    left join t0
        on t0.twitter_id = q.user_id
    where t.created_at >= current_date - 90
        and tweet_type = 'Dashboard'
    group by 1, 2, 3, 4
)
, t4 as (
    select coalesce(t1.username, t2.username, t3.username) as username
    , coalesce(t1.ecosystem, t2.ecosystem, t3.ecosystem) as ecosystem
    , coalesce(t1.twitter_handle, t2.twitter_handle, t3.twitter_handle) as twitter_handle
    , coalesce(t1.score, 0) * power(coalesce(t1.n_tweets, 0), 0.5) as points_tweets
    , coalesce(t2.score, 0) * power(coalesce(t2.n_rts, 0), 0.5) as points_retweets
    , coalesce(t3.score, 0) * power(coalesce(t3.n_qts, 0), 0.5) as points_quotes
    , power((points_tweets * 4) + (points_quotes * 2) + points_retweets, 0.9) as tot_points
    from t1
    full outer join t2
        on t2.username = t1.username
        and t2.ecosystem = t1.ecosystem
        and t2.twitter_handle = t1.twitter_handle
    full outer join t3
        on t3.username = coalesce(t1.username, t2.username)
        and t3.ecosystem = coalesce(t1.ecosystem, t2.ecosystem)
        and t3.twitter_handle = coalesce(t1.twitter_handle, t2.twitter_handle)
)
select * from t4
, t5 as (
    select username
    , sum(tot_points) as tot_points
    from t4
    group by 1
)
select t5.*
, nt.n_tweets
from t4
join nt
    on nt.username = t4.username
order by tot_points desc


select u.username, b.*
from bi_analytics.silver.user_bans b
join bi_analytics.velocity_app_prod.users u
    on u.id = b.user_id

with t0a as (
    select distinct t.twitter_id
    , t.twitter_handle
    , t.account_type
    , t.n_followers
    , t.updated_at
    , lower(c.value::string) as ecosystem
    from crosschain.bronze.twitter_accounts t
    , lateral flatten(
        input => ecosystems
    ) c
)
, t0 as (
    select *
    from t0a
    qualify(
        row_number() over (partition by twitter_id order by updated_at desc) = 1
    )
)
, t1 as (
    select s.twitter_id
    , s.score
    , s.month
    , t0.ecosystem
    , row_number() over (partition by t0.ecosystem, s.month order by s.score desc, s.twitter_id) as rn
    from bi_analytics.silver.user_community_scores_monthly s
    join t0
        on t0.twitter_id = s.twitter_id
    where month = '2024-12'
)
, t2 as (
    select ecosystem
    , month
    , sum(score) / 15 as ecosystem_score
    from t1
    where rn <= 100
    group by 1, 2
)
select *
from t2
order by month desc, ecosystem_score desc


select u.username, q.created_at::date as date, q.name, q.statement
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.users u
    on u.id = q.created_by_id
where q.created_at >= current_date - 10
    and q.statement ilike '%live%'
    and u.username = 'kellen'
limit 10

-- impressions + KOL RT + KOL QT for all posts with dashboard
-- take the top 3 each month
-- take their top 3 scores from the past 6 months
with t0 as (
    select twitter_id
    , sum(score) as score
    from bi_analytics.silver.user_community_scores_monthly
    where month >= '2024-01'
    group by 1
)
, t1 as (
    select conversation_id
    , u.username
    , date_trunc('month', created_at) as month
    , max(impression_count) as n_impressions
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    group by 1, 2, 3
)
-- quotes
, t3 as (
    select u.username
    , a.ecosystems[0]::string as ecosystem
    , a.twitter_handle
    , coalesce(t0.score, 0) as score
    , count(distinct t.conversation_id) as n_qts
    , sum(q.impression_count) as impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.twitter.quotes q
        on q.tweet_id = t.conversation_id
    join bi_analytics.twitter.twitter_accounts a
        on a.twitter_id = q.user_id
        and a.account_type != 'flipside'
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    left join t0
        on t0.twitter_id = q.user_id
    where t.created_at >= current_date - 90
        and tweet_type = 'Dashboard'
    group by 1, 2, 3, 4
)
select * from bi_analytics.content_rankings.dashboard_rankings
limit 10



select u.username
, max(date_trunc('month', t.created_at)) as month


from bi_analytics.content_rankings.dashboard_rankings r
order by wt_score_4_1 desc
limit 10



select u.username, b.*
from bi_analytics.silver.user_bans b
join bi_analytics.velocity_app_prod.users u
    on u.id = b.user_id

select u.username
, d.title
, dr.ranking_trending
, dr.*
from bi_analytics.content_rankings.dashboard_rankings dr
join bi_analytics.velocity_app_prod.dashboards d
    on d.id = dr.dashboard_id
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id


select *
from bi_analytics.silver.user_bans b


-- score
-- 1 point per 1k impressions (max of 20)
-- 1 point per KOL tweet
-- impressions
-- quotes
-- retweets
with t0 as (
    select t.conversation_id
    , date_trunc('month', t.created_at) as month
    , u.id as user_id
    , u.username
    , case when ta.twitter_id is null then 0 else 1 end as is_kol_tweet
    , max(t.impression_count) as impression_count
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    left join bi_analytics.twitter.twitter_accounts ta
        on ta.twitter_id = t.user_id
        and ta.account_type != 'flipside'
    where t.created_at >= '2023-06-01'
        and t.user_id != '925712018937712640'
    group by 1, 2, 3, 4, 5
)
, t1 as (
    select t.conversation_id
    , count(distinct taq.twitter_id) as n_kol_qts
    , count(distinct tar.twitter_id) as n_kol_rts
    , count(distinct caq.user_id) as n_ca_qts
    , count(distinct car.user_id) as n_ca_rts
    from bi_analytics.twitter.tweet t
    left join bi_analytics.twitter.quotes q
        on q.tweet_id = t.conversation_id
    left join bi_analytics.twitter.twitter_accounts taq
        on taq.twitter_id = q.user_id
        and taq.account_type != 'flipside'
    left join bi_analytics.twitter.retweets r
        on r.tweet_id = t.conversation_id
    left join bi_analytics.twitter.twitter_accounts tar
        on tar.twitter_id = r.user_id
        and tar.account_type != 'flipside'
    left join bi_analytics.twitter.core_audience car
        on car.user_id = r.user_id
        and tar.twitter_id is null
    left join bi_analytics.twitter.core_audience caq
        on caq.user_id = q.user_id
        and taq.twitter_id is null
    group by 1
)
, t2 as (
    select t0.*
    , coalesce(t1.n_kol_qts, 0) as n_kol_qts
    , coalesce(t1.n_kol_rts, 0) as n_kol_rts
    , coalesce(t1.n_ca_qts, 0) as n_ca_qts
    , coalesce(t1.n_ca_rts, 0) as n_ca_rts
    from t0
    left join t1
        on t1.conversation_id = t0.conversation_id
)
, f as (
    select t2.*
    , least(100, least(40, impression_count / 500) + (is_kol_tweet * 10) + (n_kol_qts * 5) + (n_kol_rts * 3.0) + (n_ca_qts * 0.0) + (n_ca_rts * 0.0)) as score
    , row_number() over (partition by month, username, user_id order by score desc) as rn
    from t2
)
, t3 as (
    select left(month::string, 10) as month
    , username
    , user_id
    , sum(score) as tot_score
    from f
    where rn <= 3
    group by 1, 2, 3
    order by 1 desc, 4 desc
)
select t3.*
, case when a.user_id is null then 0 else 1 end as is_ambassador
-- , sum(tot_score) over (partition by username order by month )
from t3
left join bi_analytics.silver.ambassador a
    on a.user_id = t3.user_id

-- select month
-- , sum(score) as tot_score
-- from f
-- group by 1
-- order by 1 desc


-- select *
-- from f
-- order by score desc

-- select username
-- , user_id
-- , sum(score) as tot_score
-- from f
-- where rn <= 3
-- group by 1, 2
-- order by 3 desc


-- select *
-- from bi_analytics.velocity_app_prod.query_runs r
-- where r.created_at >= current_date - 90
-- limit 10


-- select *
-- from bi_analytics.compass_prod.query_runs r
-- where r.created_at >= current_date - 90
-- order by r.created_at desc
-- limit 10



select *
from bi_analytics.twitter.tweet t
left join bi_analytics.twitter.user tu
    on tu.id = t.user_id
where t.created_at >= current_date - 11
limit 10

-- get the amount they made
-- KOL RTs / KOL QTs / impressions
-- in the last 90d

with rk_hist0 as (
    select d.id as dashboard_id
    , date_trunc('hour', dbt_updated_at) as hour
    , avg(hr.ranking_trending) as rk0
    from bi_analytics.snapshots.hourly_dashboard_rankings hr
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where dbt_updated_at >= current_date - 90
        and coalesce(u.role, '') <> 'internal'
        and not u.username in (
            'Polaris_9R','dsaber','flipsidecrypto','metricsdao','drethereum','Orion_9R','sam','forgash','nftchance__','danner'
        )
        and hr.ranking_trending <= 100
    group by 1, 2
), rk_hist1 as (
    select *
    , row_number() over (partition by hour order by rk0 asc) as rk
    from rk_hist0
), rk_hist2 as (
    select dashboard_id
    , min(rk) as top_ranking
    , sum(case when rk <= 10 then 1.5 when rk <= 40 then 1 else 0 end) as n_hours_in_top_40
    from rk_hist1
    group by 1
), imp as (
    select conversation_id
    , max(impression_count) as impression_count
    from bi_analytics.twitter.tweet t
    where t.created_at >= current_date - 90
    group by 1
)
, qts as (
    select distinct t.conversation_id
    , taq.twitter_id
    from bi_analytics.twitter.tweet t
    join bi_analytics.twitter.quotes q
        on q.tweet_id = t.conversation_id
    join bi_analytics.twitter.twitter_accounts taq
        on taq.twitter_id = q.user_id
        and taq.account_type != 'flipside'
)
, rts as (
    select distinct t.conversation_id
    , tar.twitter_id
    from bi_analytics.twitter.tweet t
    join bi_analytics.twitter.retweets r
        on r.tweet_id = t.conversation_id
    join bi_analytics.twitter.twitter_accounts tar
        on tar.twitter_id = r.user_id
        and tar.account_type != 'flipside'
)
, kols0 as (
    select coalesce(q.conversation_id, r.conversation_id) as conversation_id
    , coalesce(q.twitter_id, r.twitter_id) as twitter_id
    , case when q.twitter_id is null then 0 else 1 end as is_qt
    from qts q
    full outer join rts r
        on r.conversation_id = q.conversation_id
        and r.twitter_id = q.twitter_id
)
, kols as (
    select conversation_id
    , sum(is_qt) as n_qts
    , sum(1 - is_qt) as n_rts
    from kols0
    group by 1
)
, t0 as (
    select i.conversation_id
    , i.impression_count
    , coalesce(k.n_qts, 0) as n_qts
    , coalesce(k.n_rts, 0) as n_rts
    from imp i
    left join kols k
        on k.conversation_id = i.conversation_id
)
, t1 as (
    select distinct d.id as dashboard_id
    , d.title
    , u.username
    , t.conversation_id
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    join bi_analytics.twitter.tweet t
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    where t.created_at >= current_date - 90
)
, t2 as (
    select dashboard_id
    , title
    , username
    , sum(impression_count) as n_impressions
    , sum(n_qts) as n_qts
    , sum(n_rts) as n_rts
    from t1
    join t0
        on t0.conversation_id = t1.conversation_id
    group by 1, 2, 3
)
, t3 as (
    select t2.*
    , coalesce(r.n_hours_in_top_40, 0) as n_usd
    from t2
    left join rk_hist2 r
        on r.dashboard_id = t2.dashboard_id
)
select *
from t3
order by n_usd

with t0a as (
    select c.value:user_id::string as user_id
    , c.value:mentee_user_id::string as mentee_user_id
    , c.value:status::string as status
    , c.value:partner::string as partner
    , c.value:ecosystem::string as ecosystem
    , c.value:currency::string as currency
    , c.value:base_comp::int as base_comp
    , c.value:impression_incentive::float as impression_incentive
    , c.value:deleted::boolean as deleted
    , row_number() over (partition by user_id, lower(partner), status order by u.record_metadata:CreateTime::int desc, deleted asc) as rn
    from crosschain.bronze.data_science_uploads u
    , lateral flatten(
        input => record_content
    ) c
    where record_metadata:key like 'ambassador%'
), t1a as (
    select t0a.*
    from t0a
    where rn = 1
        and user_id is not null
        and status is not null
        and partner is not null
        and ecosystem is not null
        and currency is not null
        and base_comp is not null
        and impression_incentive is not null
        and deleted = false
), ambassador as (
  select user_id
  , mentee_user_id
  , status
  , partner
  , ecosystem
  , currency
  , base_comp as base_comp
  , impression_incentive
  from t1a
)
, labels as (
  select
    c.value:dashboard_id :: string as dashboard_id,
    c.value:tag :: string as dashboard_tag
  from
    crosschain.bronze.data_science_uploads,
    LATERAL FLATTEN(input => record_content) c
  where
    record_metadata:key like 'dashboard-tags%'
),
t1 as (
  select
    u.username,
    u.profile_id,
    a.*
  from
    ambassador a
    left join bi_analytics_dev.velocity_app_prod.users u on u.id = a.user_id
    left join bi_analytics_dev.velocity_app_prod.profiles p on p.id = u.profile_id
  where
    p.type = 'user'
)
-- select * from t1
, impr as (
  select conversation_id
  , max(impression_count) as impression_count
  from bi_analytics.twitter.tweet
  group by 1
)
,
imp0 as (
  select
    d.id as dashboard_id,
    d.created_by_id as user_id,
    i.impression_count
  from
    bi_analytics_dev.velocity_app_prod.dashboards d
    join bi_analytics_dev.twitter.tweet t on RIGHT(d.latest_slug, 6) = RIGHT(SPLIT(t.clean_url, '?') [0] :: string, 6)
    join impr i
      on i.conversation_id = t.conversation_id
  where
    not d.id in (
      select
        dashboard_id
      from
        labels
      where
        dashboard_tag = 'bot'
    )
    and t.created_at :: date >= current_date - 8
    and t.created_at :: date <= current_date - 1 qualify(
      row_number() over (
        partition by t.conversation_id
        order by
          i.impression_count desc
      ) = 1
      and row_number() over (
        partition by t.tweet_url
        order by
          i.impression_count desc
      ) = 1
    )
),
imp as (
  select
    user_id,
    sum(impression_count) as impression_count
  from
    imp0
  group by
    1
),
analyst as (
  select
    t1.user_id as datastudio_id,
    t1.profile_id,
    t1.username,
    initcap(t1.partner) as partner,
    t1.ecosystem as chain,
    t1.currency,
    t1.base_comp as base_comp,
    coalesce(i.impression_count, 0) as impression_count,
    t1.impression_incentive, -- kb update on 6/4/24
    floor(
      least(250000, coalesce(i.impression_count, 0) * t1.impression_incentive) -- kb update on 6/4/24
    ) as incentive_comp,
    t1.base_comp + incentive_comp as amount
  from
    t1
    left join imp i on i.user_id = t1.user_id
  where
    status = 'analyst'
),
mentor as (
  select
    t1.user_id as datastudio_id,
    t1.profile_id,
    t1.username,
    initcap(t1.partner) as partner,
    t1.ecosystem as chain,
    t1.currency,
    t1.base_comp,
    coalesce(i.impression_count, 0) as impression_count,
    t1.impression_incentive, -- kb update on 6/4/24
    floor(
      least(250000, coalesce(i.impression_count, 0) * t1.impression_incentive) -- kb update on 6/4/24
    ) as incentive_comp,
    t1.base_comp + incentive_comp as usd_amount
  from
    t1
    left join imp i on i.user_id = t1.mentee_user_id
  where
    status = 'mentor'
),
combined as (
    select
      *
    from
      mentor
    union
    select
      *
    from
      analyst
),
final as (
    select 
        case 
            when impression_incentive > 0 then ' and impressions bonus.'
            when impression_incentive = 0 then '.' end
            as has_bonus,
        concat ('Ambassador weekly payment for ', partner, ' ecosystem from ', (current_date - 8), ' to ', (current_date - 1), ' including base pay', has_bonus) as name,
        'AMBASSADOR' as type,
        (current_date - 1) as price_scrape_time,
        combined.*
    from combined
)
select 
    name,
    type,
    datastudio_id,
    chain, 
    currency,
    round(usd_amount) as usd_amount,
    null as token_amount,
    price_scrape_time,
    -- ^ required columns
    
    null as note,
    username,
    profile_id,
    round(base_comp) as base_comp,
    impression_incentive,
    incentive_comp
    -- ,dashboard_id
    -- ^ extra columns
    
from final
order by name, username
;



SELECT *
from bi_analytics.velocity_app_prod.dashboards d
, LATERAL FLATTEN(
    input => OBJECT_KEYS(publishedConfig:cells)
) cell_key
, LATERAL FLATTEN(
    input => TO_VARIANT(publishedConfig:cells[cell_key])
) cell
WHERE
and cell.value:variant::string = 'visualization'
group by 1



select distinct t.conversation_id
, r.*
, tar.*
from bi_analytics.twitter.tweet t
join bi_analytics.twitter.retweets r
    on r.tweet_id = t.conversation_id
left join bi_analytics.twitter.twitter_accounts tar
    on tar.twitter_id = r.user_id
where conversation_id = '1833587147435348020'


select t.conversation_id as t_c
, e.conversation_id as e_c
, concat('https://x.com/thorchain/status/', coalesce(t.conversation_id, e.conversation_id))
, *
, case when t.conversation_id is null then 1 else 0 end as missing_0
, case when t.conversation_id is null then 0 else 1 end as missing_1
from bi_analytics.twitter.tweet t
full outer join datascience.twitter.ez_tweets e
    on e.conversation_id = t.conversation_id
where t.conversation_id is null or e.conversation_id is null

select t.tweet_id as t_id
, e.id as e_id
, concat('https://x.com/thorchain/status/', coalesce(t.tweet_id, e.id)) as tweet_url
, concat('https://x.com/thorchain/status/', coalesce(t.quote_tweet_id, e.quote_tweet_id)) as tweet_url_2
, *
, case when t.tweet_id is null then 1 else 0 end as missing_0
, case when t.tweet_id is null then 0 else 1 end as missing_1
, case when missing_0 + missing_1 > 0 then 1 else 0 end as has_diff
from bi_analytics.twitter.quotes t
full outer join datascience.twitter.fact_quote_tweets e
    on e.id::int::string = t.tweet_id::int::string

select t.tweet_id as t_id
, e.id as e_id
, concat('https://x.com/thorchain/status/', coalesce(t.tweet_id, e.id)) as tweet_url
, concat('https://x.com/thorchain/status/', coalesce(t.quote_tweet_id, e.quote_tweet_id)) as tweet_url_2
, *
, case when t.tweet_id is null then 1 else 0 end as missing_0
, case when t.tweet_id is null then 0 else 1 end as missing_1
, case when missing_0 + missing_1 > 0 then 1 else 0 end as has_diff
from bi_analytics.twitter.quotes t
full outer join datascience.twitter.fact_quote_tweets e
    on e.id::int::string = t.tweet_id::int::string
-- where t_id is null or e_id is null


select count(1)
from bi_analytics.twitter.retweets r
join bi_analytics.twitter.tweets t
    on r.tweet_id = t.id
where t.created_at >= current_date - 1

select count(1)
from datascience.twitter.ez_tweets t
join datascience.twitter.fact_retweets r
    on r.tweet_id = t.id
where t.created_at >= current_date - 1


select t.conversation_id
, coalesce(r.user_id, r2.user_id) as user_id
, case when r.user_id is null then 1 else 0 as missing_new
, case when r2.user_id is null then 1 else 0 as missing_old
from datascience.twitter.ez_tweets t
left join datascience.twitter.fact_retweets r
    on r.id = t.id
where t.created_at >= '2024-08-29'

select *
from bi_analytics.twitter.quotes
limit 10

select *
from datascience.twitter.fact_quote_tweets
limit 10

select t.conversation_id
, coalesce(r.user_id, r2.user_id) as user_id
, case when r.user_id is null then 1 else 0 as missing_new
, case when r2.user_id is null then 1 else 0 as missing_old
from datascience.twitter.ez_tweets t
left join datascience.twitter.fact_retweets r
    on r.id = t.id
where t.created_at >= '2024-08-29'



with a as (
    select distinct qtt.A as query_id
    from bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
    join bi_analytics.velocity_app_prod.tags t on qtt.B = t.id
        and t.type = 'project'
        and t.name = 'solana'
), t0 as (
    select r.*
    , tags:queryId::string as query_id
    , timestampdiff(seconds,started_at,ended_at) as query_runtime
    from bi_analytics.compass_prod.QUERY_RUNS r
    where started_at > current_date - 365
        and tags:queryId::string in (select query_id from a)
)
select median(query_runs)
from t0

select 
created_by_id as gumby_id,
username,
count(distinct(query_id)) as n_queries,
sum(coalesce(query_runtime, 0)) as studio_queryseconds,
studio_queryseconds * 0.02 as cost
from t0
join bi_analytics.velocity_app_prod.queries q
    on q.id = query_id
join bi_analytics.velocity_app_prod.users u
    on u.id = created_by_id
where started_at >= current_date - 365
group by 1,2
order by 4 desc



create a QT of this post from @berajibears: "Mint details • 🧵 Supply: 500 • ⛓️ Chain: Arbitrum. • 💰 Public Price: 0.0228 ETH | WL Price: 0.02 ETH. • 🕓 Mint Date: November 28th, 4:20 PM UTC."
create a comment under this post from @berajibears: "Mint details • 🧵 Supply: 500 • ⛓️ Chain: Arbitrum. • 💰 Public Price: 0.0228 ETH | WL Price: 0.02 ETH. • 🕓 Mint Date: November 28th, 4:20 PM UTC."

create a QT of this post from @0xoogabooga: "Introducing Ooga Bucks. X posts that mention or talk about Ooga Booga in any capacity, have the possibility to be rewarded.  Let’s walk through an example:  1. Kevin posts a tweet: “I love Ooga Booga”.  2. Ooga Booga's account replies to Kevin’s tweet with a code such as: “3fn10n”.  3. Kevin goes to http://app.oogabooga.io/oogabucks, inputs the code, and receives his verification code.  4. Kevin replies to Ooga Booga’s tweet with the verification code which looks like: “ooga booga jar sea keys ceiling”. This allows us to associate poster, and wallet address.  5. Kevin, after waiting up to 10 minutes, will be able to claim the rewarded amount of Ooga Bucks on the Ooga Booga UI!  Here's a real-life example walkthrough between Kevin and Ooga Booga:  https://x.com/0xoogabooga/status/1859327038164959736  Ooga Bucks are represented as a non-transferable ERC20 on the Arbitrum network. They can be redeemed 1:1 for USDC, or they can be held until $OOGA TGE, where the equivalent amount will be airdropped at 10m FDV in $OOGA on Berachain mainnet.  FAQ:   1. What type of content is eligible for Ooga Bucks?  Any post that mentions or discusses Ooga Booga in any capacity. This could range from memes or engaging in creative or meaningful ways with the Ooga Booga brand or the words “Ooga Booga”.  2. How does the verification process prevent bots or fraudulent claims from abusing the system?  Only the original poster's verification code is taken into account. The verification process uses unique codes to tie a post to a wallet address, creating a two-step verification: A code (e.g., “3fn10n”) is issued to the tweet by the Ooga Booga's official account. Users must return with a second verification code (e.g., “ooga booga jar sea keys ceiling”) to complete the process. These steps eliminate the chance of bots or fake accounts abusing the system since it requires a verified, repeat interaction from the original poster.  3. What kind of metrics or thresholds determine how much Ooga Bucks are rewarded for each tweet?  This is subject to change.  4. Are there caps or limitations per user?  No cap or limitation on tweets/posts. We encourage users to post as much as they want, and previous rewarded amounts will never be taken into account with whether more rewards can be distributed.  Ooga Booga."

create a QT of this post from @0xoogabooga: "my therapist said that Berachain's best form of self-care and healing is to rip faces in Q5 Berachain growth arc"
create a comment under this post from @SmokeyTheBera: "my therapist said that Berachain's best form of self-care and healing is to rip faces in Q5 Berachain growth arc"


select *
from solana.defi.ez_dex_swaps
where block_timestamp::date >= '2024-01-01'
    and swapper = '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj'
order by block_timestamp, tx_id
limit 500;


select distinct conversation_id
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.twitter.tweet t
    on right(d.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)
where t.created_at >= current_date - 30


select count(distinct tx_id)
from solana.core.fact_transactions


with a as (
    select distinct qtt.A as query_id
    , q.statement
    from bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
    join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and t.type = 'project'
        and t.name = 'solana'
    join bi_analytics.velocity_app_prod.queries q
        on q.id = qtt.a
    where (not q.statement ilike '%join%')
        and (not q.statement ilike '%array_contains%')
        and (not q.statement ilike '%like%')
        and q.statement ilike '%where%block_timestamp%limit%'
), t0 as (
    select r.*
    , a.statement
    , tags:queryId::string as query_id
    , tags:executionType::string as execution_type
    , timestampdiff(seconds,started_at,query_running_ended_at)::float as query_runtime
    from bi_analytics.compass_prod.QUERY_RUNS r
    join a
        on a.query_id = tags:queryId::string
    where r.state = 'QUERY_STATE_SUCCESS'
        and started_at is not null and query_running_ended_at is not null
        and execution_type = 'REALTIME'
        and error_name is null
)
select median(query_runtime) as median_runtime
, avg(query_runtime) as avg_runtime
, count(1) as n_query_runs
, min(started_at) as started_at
from t0




WITH pages as (
    SELECT id
    , user_id
    , timestamp
    , context_page_url
    , context_page_tab_url
    , context_page_referrer
    , context_page_referring_domain
    , anonymous_id
    , context_ip
    , row_number() over (partition by COALESCE(user_id, anonymous_id), context_session_id order by timestamp) as page_in_session
    from bi_analytics.gumby.pages
    UNION
    SELECT id
    , user_id
    , timestamp
    , context_page_url
    , context_page_tab_url
    , context_page_referrer
    , context_page_referring_domain
    , anonymous_id
    , context_ip
    , row_number() over (partition by COALESCE(user_id, anonymous_id), context_session_id order by timestamp) as page_in_session
    from bi_analytics.flipside_app_prod.pages
), id_map0 as (
    SELECT user_id
    , anonymous_id
    , COUNT(1) as n
    , MIN(timestamp) as timestamp
    from pages
    WHERE user_id is NOT null
    group by 1, 2
), id_map as (
    SELECT user_id
    , anonymous_id
    from id_map0
    QUALIFY(
        row_number() over (partition by user_id, anonymous_id order by timestamp, n desc) = 1
    )
), id_map1 as (
    SELECT user_id
    , context_ip
    , COUNT(1) as n
    , MIN(timestamp) as timestamp
    from pages
    WHERE user_id is NOT null
    group by 1, 2
), id_map_ip as (
    SELECT user_id
    , context_ip
    from id_map1
    QUALIFY(
        row_number() over (partition by user_id, context_ip order by timestamp, n desc) = 1
    )
), earliest_page_load as (
    SELECT COALESCE(p.user_id, i.user_id, ip.user_id) as user_id
    , SPLIT(context_page_url, '?')[0]::string as context_page_url
    , timestamp
    , p.id
    , p.context_page_referrer
    , p.context_page_referring_domain
    , p.page_in_session
    , case when d.id is null then 0 else 1 end as is_dash
    , row_number() over (
        partition by COALESCE(p.user_id, i.user_id, ip.user_id)
        order by
            -- if they viewed a dashboard in the first page in the session when their account was created, use that
            -- (take the most recent dash they viewed)
            case when p.timestamp < u.created_at then 0 else 1 end
            , case when p.page_in_session = 1 then 0 else 1 end
            , case when d.id is null then 1 else 0 end
            , timestamp desc
    ) as rn
    from pages p
    left join id_map i
        on i.anonymous_id = p.anonymous_id
    left join id_map_ip ip
        on ip.context_ip = p.context_ip
    join bi_analytics.velocity_app_prod.users u
        on u.id = COALESCE(p.user_id, i.user_id, ip.user_id)
    left join bi_analytics.velocity_app_prod.dashboards d
        on RIGHT(d.latest_slug, 6) = RIGHT(SPLIT(context_page_url, '?')[0]::string, 6)
    left join bi_analytics.velocity_app_prod.users du
        on du.id = d.created_by_id
    WHERE left(context_page_url, 16) != 'http://localhost'
        and p.timestamp < u.created_at
        and p.page_in_session = 1
    QUALIFY(
        row_number() over (
            -- get the most recent dashboard they viewed that was before they created their account, if it was the first page in their session
            partition by COALESCE(p.user_id, i.user_id, ip.user_id)
            order by
                case when d.id is null then 1 else 0 end
                , timestamp desc
        ) = 1
    )
)
, queries as (
    SELECT q.created_by_id as user_id
    , COUNT(DISTINCT q.id) as n_queries
    , COUNT(DISTINCT coalesce(r.query_id, r2.tags:queryId::string)) as n_queries_run
    from bi_analytics.velocity_app_prod.queries q
    left join bi_analytics.velocity_app_prod.query_runs r
        on r.query_id = q.id
        and r.status = 'finished'
    left join bi_analytics.compass_prod.query_runs r2
        on q.id = r2.tags:queryId::string
    WHERE q.name <> 'Getting Started'
    group by 1
)
, dashboards as (
    SELECT d.created_by_id as user_id
    , COUNT(DISTINCT d.id) as n_dashboards
    , COUNT(DISTINCT case when dbc.dashboard_id is null and d.published_at is null then null else d.id end) as n_dashboards_published
    , SUM(COALESCE(impression_count, 0)) as impression_count
    from bi_analytics.velocity_app_prod.dashboards d
    left join bi_analytics.twitter.tweet t
        on RIGHT(d.latest_slug, 6) = RIGHT(SPLIT(t.clean_url, '?')[0]::string, 6)
    left join bi_analytics.flipside_app_prod.dashboard_publish dbc
        on dbc.dashboard_id = d.id
    group by 1
)
, f as (
    SELECT COALESCE(u.id, 'None') as user_id
    , COALESCE(u.username, f.fs_snowflake_username) as username
    , COALESCE(f.hs_email, u.email) as email
    , u.twitter_handle
    , u.discord_handle
    , f.fs_snowflake_trial_start_date::date as fs_snowflake_trial_start_date
    , COALESCE(u.created_at, f.fs_snowflake_trial_start_date)::date as date
    , COALESCE(e.context_page_url, 'None') as initial_source
    , COALESCE(db.title, 'None') as initial_source_dashboard_title
    , du.username as onboarding_user
    , COALESCE(q.n_queries, 0) as n_queries
    , COALESCE(q.n_queries_run, 0) as n_queries_run
    -- , COALESCE(d.n_dashboards, 0) as n_dashboards
    -- , COALESCE(d.n_dashboards_published, 0) as n_dashboards_published
    -- , COALESCE(d.impression_count, 0) as impression_count
    -- , COALESCE(c.csv_downloads, 0) as csv_downloads
    -- , COALESCE(j.json_downloads, 0) as json_downloads
    -- , COALESCE(qru.total_studio_runtime, 0) as total_studio_runtime
    -- , COALESCE(api.api_requests, 0) as api_requests
    -- , COALESCE(api.api_query_seconds_total, 0) as api_query_seconds_total
    -- , (COALESCE(d.impression_count, 0) / 10) + (COALESCE(q.n_queries, 0) * 5) as user_value
    -- , LEAST(500, user_value) as capped_value
    -- , CONCAT(
    --     COALESCE(uc1.chain, '')
    --     , case when uc2.chain is null then '' else CONCAT(' + ', uc2.chain) end
    --     , case when uc3.chain is null then '' else CONCAT(' + ', uc3.chain) end
    -- ) as chain
    , COALESCE(e.context_page_referrer, 'None') as context_page_referrer
    , COALESCE(e.context_page_referring_domain, 'None') as context_page_referring_domain
    , COALESCE(du.username, e.context_page_referring_domain, 'Unknown') as onboarding_source
    , COALESCE(case when du.username is null then null else 'Community' end, e.context_page_referring_domain, 'Unknown') as onboarding_source_group
    , case when f.hs_contact_id is null then 0 else 1 end as has_free_trial
    from bi_analytics.velocity_app_prod.users u
    FULL OUTER join bi_analytics.sales_intel.free_trial_formfills f
        on f.hs_gumby_id = u.id
        or f.hs_email = u.email
    -- left join gumby_users_qr_usage qru
    --     on qru.user_id = u.id
    -- left join compass_qs api
    --     on api.user_id = u.id
    -- left join json_downloads j
    --     on j.user_id = u.id
    -- left join csv_downloads c
    --     on c.user_id = u.id
    left join queries q
        on q.user_id = u.id
    left join dashboards d
        on d.user_id = u.id
    left join earliest_page_load e
        on e.user_id = u.id
    -- left join user_chain uc1
    --     on uc1.id = u.id
    --     and uc1.rn = 1
    -- left join user_chain uc2
    --     on uc2.id = u.id
    --     and uc2.rn = 2
    --     and (uc2.pct >= uc1.pct or uc2.pct > 0.25)
    -- left join user_chain uc3
    --     on uc3.id = u.id
    --     and uc3.rn = 3
    --     and (uc3.pct >= uc1.pct or uc3.pct > 0.25)
    left join bi_analytics.velocity_app_prod.dashboards db
        on RIGHT(db.latest_slug, 6) = RIGHT(SPLIT(e.context_page_url, '?')[0]::string, 6)
    left join bi_analytics.velocity_app_prod.users du
        on du.id = db.created_by_id
    where u.created_at >= current_date - 30
    QUALIFY(
        row_number() over (
            partition by COALESCE(u.id, f.fs_snowflake_username)
            order by
            case when du.username is null then 1 else 0 end
            , case when e.context_page_referring_domain is null then 1 else 0 end
        ) = 1
    )
)
SELECT *
-- , case when (
--     n_queries_run >= 100
--     or csv_downloads >= 100
--     or json_downloads >= 100
--     or total_studio_runtime >= 100000
--     or api_requests >= 500
--     or api_query_seconds_total >= 10000
-- ) and n_dashboards_published <= 1 then 1 else 0 end as is_pro_candidate
-- , case
--     when has_free_trial = 1 then 'PLG-Trial'
--     when is_pro_candidate = 1 then 'PLG-Qualified'
--     when n_queries_run >= 3 then 'Serious User'
--     else 'User' end as user_group
, case
    when onboarding_source_group in ('Community','Unknown') then onboarding_source_group
    when onboarding_source_group = 'www.google.com' then 'Google'
    when onboarding_source_group = 't.co' then 'Twitter'
    when onboarding_source_group = 'www.youtube.com' then 'YouTube'
    when onboarding_source_group = 'flipsidecrypto.xyz' then 'Flipside App'
    when onboarding_source_group = 'docs.flipsidecrypto.com' then 'Flipside Docs'
    when onboarding_source_group = 'app.flipsidecrypto.com' then 'Flipside App'
    -- when onboarding_source_group = 'data.flipsidecrypto.xyz' then 'data.flipsidecrypto.xyz'
    when onboarding_source_group = 'www.trails.fm' then 'Trails'
    when onboarding_source_group = 'dashboard.quicknode.com' then 'Quicknode'
    when onboarding_source_group = 'science.flipsidecrypto.xyz' then 'Data Science App'
    when onboarding_source_group = 'web.telegram.org' then 'Telegram'
    when onboarding_source_group = 'org.telegram.messenger' then 'Telegram'
    -- when onboarding_source_group = 'luabase.com' then 'Luabase'
    when onboarding_source_group = 'duckduckgo.com' then 'Other Search Browsers'
    when onboarding_source_group = 'search.brave.com' then 'Other Search Browsers'
    when onboarding_source_group = 'www.bing.com' then 'Other Search Browsers'
    else 'Other' end as onboarding_source_group_2
from f
order by date desc


with t0 as (
    select conversation_id
    , min(date_trunc('month', t.created_at)) as month
    , max(impression_count) as n_impressions
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(SPLIT(t.clean_url, '?')[0]::string, 6)
    group by 1
)
select month
, sum(n_impressions) as n_impressions
from t0
group by 1
order by 1


select *
from solana.defi.fact_swaps
where block_timestamp >= current_date - 30
    and swapper = 'GLi418odqLgLETe6eWmoD3tGYrbjRBekPEWjUV2HQNr'
order by swap_to_mint, swap_from_mint, block_timestamp


with ambas as (
    select distinct a.user_id
    , u.username
    from bi_analytics.silver.ambassador a
    join bi_analytics.velocity_app_prod.users u
        on u.id = a.user_id
    where status = 'analyst'
)
, imp as (
    select conversation_id
    , d.created_by_id as user_id
    , max(impression_count) as impression_count
    , max(user_followers) as n_followers
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)
    where date_trunc('month', t.created_at)::date = '2024-04-01'
    group by 1, 2
)
, kols as (
    select a.*
    , c.value::string as ecosystem
    from bi_analytics.twitter.twitter_accounts a
    , lateral flatten(
        input => ecosystems
    ) c
)
, rts as (
    select t.conversation_id
    , coalesce(tar.twitter_id, taq.twitter_id) as twitter_id
    , count(distinct coalesce(tar.twitter_id, taq.twitter_id)) as n_kol_rts
    , count(distinct case when coalesce(kq.ecosystem, kr.ecosystem) = 'near' then coalesce(tar.twitter_id, taq.twitter_id) else null end) as n_kol_rts_near
    , count(distinct case when coalesce(kq.ecosystem, kr.ecosystem) = 'base' then coalesce(tar.twitter_id, taq.twitter_id) else null end) as n_kol_rts_base
    , count(distinct case when coalesce(kq.ecosystem, kr.ecosystem) = 'blast' then coalesce(tar.twitter_id, taq.twitter_id) else null end) as n_kol_rts_blast
    , sum(distinct case when coalesce(kq.ecosystem, kr.ecosystem) = 'near' then coalesce(tar.twitter_id, taq.twitter_id) else null end) as n_kol_rts_near
    , count(distinct case when coalesce(kq.ecosystem, kr.ecosystem) = 'base' then coalesce(tar.twitter_id, taq.twitter_id) else null end) as n_kol_rts_base
    , count(distinct case when coalesce(kq.ecosystem, kr.ecosystem) = 'blast' then coalesce(tar.twitter_id, taq.twitter_id) else null end) as n_kol_rts_blast
    from bi_analytics.twitter.tweet t
    left join bi_analytics.twitter.quotes q
        on q.tweet_id = t.conversation_id
    left join bi_analytics.twitter.twitter_accounts taq
        on taq.twitter_id = q.user_id
        and taq.account_type != 'flipside'
    left join kols kq
        on kq.twitter_id = taq.twitter_id
    left join bi_analytics.twitter.retweets r
        on r.tweet_id = t.conversation_id
    left join bi_analytics.twitter.twitter_accounts tar
        on tar.twitter_id = r.user_id
        and tar.account_type != 'flipside'
    left join kols kr
        on kr.twitter_id = tar.twitter_id
    where date_trunc('month', t.created_at)::date >= '2024-04-01'
        and coalesce(taq.twitter_handle, tar.twitter_handle) is not null
    group by 1
)
, rts as (
    select distinct t.conversation_id
    , coalesce(tar.twitter_id, taq.twitter_id) as twitter_id
    , count(distinct coalesce(tar.twitter_id, taq.twitter_id)) as n_kol_rts
    , count(distinct case when coalesce(kq.ecosystem, kr.ecosystem) = 'near' then coalesce(tar.twitter_id, taq.twitter_id) else null end) as n_kol_rts_near
    , count(distinct case when coalesce(kq.ecosystem, kr.ecosystem) = 'base' then coalesce(tar.twitter_id, taq.twitter_id) else null end) as n_kol_rts_base
    , count(distinct case when coalesce(kq.ecosystem, kr.ecosystem) = 'blast' then coalesce(tar.twitter_id, taq.twitter_id) else null end) as n_kol_rts_blast
    , sum(distinct case when coalesce(kq.ecosystem, kr.ecosystem) = 'near' then coalesce(tar.twitter_id, taq.twitter_id) else null end) as n_kol_rts_near
    , count(distinct case when coalesce(kq.ecosystem, kr.ecosystem) = 'base' then coalesce(tar.twitter_id, taq.twitter_id) else null end) as n_kol_rts_base
    , count(distinct case when coalesce(kq.ecosystem, kr.ecosystem) = 'blast' then coalesce(tar.twitter_id, taq.twitter_id) else null end) as n_kol_rts_blast
    from bi_analytics.twitter.tweet t
    left join bi_analytics.twitter.quotes q
        on q.tweet_id = t.conversation_id
    left join bi_analytics.twitter.twitter_accounts taq
        on taq.twitter_id = q.user_id
        and taq.account_type != 'flipside'
    left join kols kq
        on kq.twitter_id = taq.twitter_id
    left join bi_analytics.twitter.retweets r
        on r.tweet_id = t.conversation_id
    left join bi_analytics.twitter.twitter_accounts tar
        on tar.twitter_id = r.user_id
        and tar.account_type != 'flipside'
    left join kols kr
        on kr.twitter_id = tar.twitter_id
    where date_trunc('month', t.created_at)::date >= '2024-04-01'
        and coalesce(taq.twitter_handle, tar.twitter_handle) is not null
    group by 1
)
, tweets as (
    select i.*
    , coalesce(r.n_kol_rts, 0) as n_kol_rts
    from imp i
    join rts r
        on r.conversation_id = i.conversation_id
)
select a.user_id
, a.username
, sum(impression_count) as impression_count
, sum(n_kol_rts) as n_kol_rts
, max(n_followers) as n_followers
from ambas a
left join tweets t
    on t.user_id = a.user_id
group by 1, 2

with t0 as (
    select distinct user_id
    from bi_analytics.twitter.tweet
    union
    select distinct user_id
    from bi_analytics.twitter.retweet
    union
    select distinct user_id
    from bi_analytics.twitter.quotes
)
select distinct user_id
from t0

with t0 as (
    select distinct u.username
    , coalesce(tar.twitter_id, taq.twitter_id) as twitter_id
    from bi_analytics.twitter.tweet t
    join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    join bi_analytics.silver.ambassador a
        on a.user_id = u.id
    left join bi_analytics.twitter.quotes q
        on q.tweet_id = t.conversation_id
    left join bi_analytics.twitter.twitter_accounts taq
        on taq.twitter_id = q.user_id
        and taq.account_type != 'flipside'
    left join bi_analytics.twitter.retweets r
        on r.tweet_id = t.conversation_id
    left join bi_analytics.twitter.twitter_accounts tar
        on tar.twitter_id = r.user_id
        and tar.account_type != 'flipside'
    where date_trunc('month', t.created_at) = '2024-09-01'
        and coalesce(tar.twitter_id, taq.twitter_id) is not null
)
, kols as (
    select a.*
    , c.value::string as ecosystem
    from bi_analytics.twitter.twitter_accounts a
    , lateral flatten(
        input => ecosystems
    ) c
)
, t1 as (
    select username
    , count(distinct t0.twitter_id) as n_kol_rts
    , count(distinct case when k.ecosystem = 'near' then t0.twitter_id else null end) as n_kol_rts_near
    , count(distinct case when k.ecosystem = 'base' then t0.twitter_id else null end) as n_kol_rts_base
    , count(distinct case when k.ecosystem = 'blast' then t0.twitter_id else null end) as n_kol_rts_blast
    , sum(case when k.ecosystem = 'near' then coalesce(u.user_followers, 0) else 0 end) as kol_rts_reach_near
    , sum(case when k.ecosystem = 'base' then coalesce(u.user_followers, 0) else 0 end) as kol_rts_reach_base
    , sum(case when k.ecosystem = 'blast' then coalesce(u.user_followers, 0) else 0 end) as kol_rts_reach_blast
    from t0
    join kols k
        on k.twitter_id = t0.twitter_id
    left join bi_analytics.twitter.user u
        on u.id = t0.twitter_id
    group by 1
)
select *
from t1



with cur_price as (
    select token_address as mint
    , price as cur_price
    from solana.price.ez_prices_hourly p
    qualify(
        row_number() over (partition by token_address order by hour desc) = 1
    )
)
, cur_swap_price as (
    select swap_to_mint
    , swap_to_amount
    , swap_from_mint
    , swap_from_amount
    , p.cur_price * s.swap_from_amount / s.swap_to_amount as cur_calc_price
    from solana.defi.fact_swaps s
    join cur_price p
        on p.mint = s.swap_from_mint
    where block_timestamp >= current_date - 1
        and succeeded
        and swap_to_amount > 0
        and swap_from_amount > 0
    qualify(
        row_number() over (partition by swap_to_mint order by block_timestamp desc) = 1
    )
)
select swap_to_mint as mint
, cur_calc_price as price
from cur_swap_price


WITH t0 as (
    SELECT *
	from crosschain.bronze.data_science_uploads dbc 
    WHERE record_metadata:key::string ilike 'twitter-users%'
), t1 as (
    SELECT c.value:id::string as id
    , c.value:profile_image_url::string as profile_image_url
    , c.value:user_name::string as user_name
    , COALESCE(c.value:user_followers::int, 0) as user_followers
    , c.value:flipside_id::string as flipside_id
    from t0
    , LATERAL FLATTEN(
        input => record_content
    ) c
    where user_name is NOT null
), t2 as (
    SELECT *
    from t1
    QUALIFY (
        row_number() over (partition by id order by user_followers desc) = 1
    )
)
SELECT id
, profile_image_url
, user_name
, user_followers
, flipside_id
from t2
where id = '441544044'

SELECT c.value:id::string as twitter_id
, c.value:user_name::string as user_name
, c.value:user_followers::int as user_followers
, c.value:tweet_id::string as tweet_id
, c.value:user_id::string as user_id
, COALESCE(c.value:impression_count::int, 0) as impression_count
, COALESCE(c.value:like_count::int, 0) as like_count
, COALESCE(c.value:retweet_count::int, 0) as retweet_count
, COALESCE(c.value:quote_count::int, 0) as quote_count
from {{ source('crosschain_bronze','data_science_uploads') }} dbc 
, LATERAL FLATTEN(
    input => record_content
) c
where record_metadata:key::string like 'twitter-user%'

-- check missing program ids in decoded tables
with t0 as (
    select distinct program_id
    from solana.core.ez_events_decoded
    where block_timestamp >= current_date - 3
), t1 as (
    select program_id
    , count(1) as n_tx
    , count(distinct signers[0]::string) as n_signers
    from solana.core.fact_events
    where block_timestamp >= '2024-10-01'
        and succeeded
    group by 1
)
select t1.*
, case when t0.program_id is null then 0 else 1 end as is_decoded
from t1 
left join t0 
    on t0.program_id = t1.program_id
order by n_signers desc


select *
from solana.core.dim_labels
limit 10

select distinct signers[0]::string as address
from solana.core.fact_events
where block_timestamp >= '2024-09-01'
    and program_id = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'



select signers[0]::string as address
, inner_instructions:instructions[0]:parsed:info:newAccount::string as mint
from solana.core.fact_events
where block_timestamp >= dateadd('minutes', -130, current_timestamp)
    and succeeded
    and program_id = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
    -- and instruction::string like '%GLGb2BtpJDCZP1sZiqmZzi6C5c71BXh8jzBNnvcVpump%'
    and inner_instruction like '%mintTo%'
order by block_timestamp desc, tx_id
limit 5000


with t0 as (
    select distinct user_id
    from bi_analytics.twitter.retweets
    union
    select distinct user_id
    from bi_analytics.twitter.quotes
)
select t0.*
, a.*
from t0
left join bi_analytics.twitter.twitter_accounts a
    on a.twitter_id = t0.user_id
order by coalesce(a.user_followers, 0) desc


select *
from bi_analytics.twitter.retweet
limit 10

select t.*
, d.title
from bi_analytics.twitter.missing_tweets m
join bi_analytics.twitter.tweet t
    on t.conversation_id = m.conversation_id
join bi_analytics.velocity_app_prod.dashboards d
    on right(d.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
where u.username ilike '%diamond%'
    and t.created_at >= current_date - 30
order by t.impression_count desc
limit 100



select block_timestamp::date
, l.label as collection
, median(sales_amount) as price
from solana.nft.fact_nft_sales s
join solana.core.dim_labels l
    on l.address = s.mint
where date >= current_date - 90
    and s.succeeded
group by 1, 2



select *
from solana.core.fact_transactions
where block_timestamp >= '2024-10-15 10:00:00'
    and signers[0]::string = 'runkuDMGfyVapyBXKJQyAGoMgQxwKU2Ba2qZy9BFj1n'
order by block_timestamp desc



select conversation_id
, user_id as author_id
, start_timestamp as created_at
, impression_count
, d.id as dashboard_id
, d.title
from BI_ANALYTICS.twitter.tweet t
join BI_ANALYTICS.velocity_app_prod.dashboards d
    on RIGHT(d.latest_slug, 6) = RIGHT(split(t.clean_url, '?')[0]::string, 6)
where t.created_at >= current_date - 3
    and d.latest_slug ilike '%U-jGWz%'

select program_id
, count(1) as n
, count(distinct tx_id) as n_tx
, count(distinct signers[0]::string) as n_signers
from solana.core.fact_events
where block_timestamp >= current_date - 30
    and program_id in (
        'LUCK57mxzZiRGF2PdHAY79P6tZ8Apsi381tKvBrTdqk'
        , 'hadeK9DLv9eA7ya5KCTqSvSvRZeJC3JgD5a9Y3CNbvu'
        , 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K'
        , 'TB1Dqt8JeKQh7RLDzfYDJsq8KS4fS2yt87avRjyRxMv'
    )
group by 1
order by 2 desc



select *
from solana.nft.fact_nft_sales
where block_timestamp >= current_date - 365
    and ix_id in (
        '2YmD4bK7HTHAbck6Z3jnDp5UqTfW9kwCWTgASnRtacdhtaz37hFsoTJ93m3Rpd6a7PpueztvFpnWnPwY9QLS41BW'
        , 'V1bniRAMrXGeSeVyqaiGWt7FedPQ5JHzydgW6kNNRdyxwfKR2n1uggb6gyQCoFNB4bQgSeoiDrXRZi9uGssnfCA'
        , 'Q9yTDgtFBWRBYru3PpyiTjbCbZkGaGsSyUtdxQ8PccAgJcTUu6Exos1XeNDLE62KPyqsmNaZpkzrwP4yEFYfpRX'
        , '3jReZu2uNcugC21xefcFzfNaBVFtgfErnW9EDgfddheXFLa1gADUYmhJCgJ9qsmgTavXdVmMr8YitXjyrswkV9cd'
    )
limit 100








select concat('https://x.com/', u.user_name, '/status/', t.conversation_id) as tweet_url
, t.created_at::date as tweet_date
, t.impression_count
, t.tweet_type
, u.user_followers
, a.ecosystems[0]::string as kol_ecosystem
, d.title as dashboard_title
, tu.username as analyst_username
-- , *
from bi_analytics.twitter.tweet t
left join bi_analytics.twitter.user u
    on u.id = t.user_id
left join bi_analytics.twitter.twitter_accounts a
    on a.twitter_id = t.user_id
left join bi_analytics.velocity_app_prod.dashboards d
    on right(d.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)
left join bi_analytics.velocity_app_prod.users tu
    on tu.id = d.created_by_id
where t.platform = 'Flipside'
order by t.impression_count desc
limit 1000


select u.username
, q.title
, q.created_at
, row_number() over (order by q.created_at) as rank
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.users u
    on u.id = q.created_by_id



with t0 as (
    select coalesce(m.nft_collection_name, l.label, 'Other') as collection
    , block_timestamp
    , sales_amount
    , tx_id
    , row_number() over (partition by collection order by block_timestamp desc) as rank
    from solana.nft.fact_nft_sales s
    left join solana.core.dim_labels l
        on l.address = s.mint
    left join solana.nft.dim_nft_metadata m
        on m.mint = s.mint
    where block_timestamp >= current_date - 3
        and collection ilike '%famous fox fed%'
)
select *
from t0




with pc0 as (
    select token_address
    , hour
    , price
    , lag(price, 1) over (
        partition by token_address
        order by hour
    ) as prv_price
    , price / prv_price as ratio
    from solana.price.ez_prices_hourly p
    where hour >= '2024-10-15 10:00:00'
        and is_imputed = false
), pc1 as (
    select hour::date as date
    , token_address
    from pc0
    where ratio >= 10
    or ratio <= 0.1
), p0 as (
    select p.token_address as mint
    , DATE_TRUNC('hour', p.hour) as hour
    , avg(price) as price
    , MIN(price) as min_price
    from solana.price.ez_prices_hourly p
    left join pc1
        on pc1.token_address = p.token_address
        and pc1.date = p.hour::date
    where hour >= '2024-10-15 10:00:00'
        and pc1.date is null
        and is_imputed = FALSE
        and price < 1000000
    group by 1, 2
), p1 as (
    select p.token_address as mint
    , DATE_TRUNC('day', hour) as date
    , avg(price) as price
    , MIN(price) as min_price
    from solana.price.ez_prices_hourly p
    left join pc1
        on pc1.token_address = p.token_address
        and pc1.date = p.hour::date
    where hour >= '{{start_date}}'
        and pc1.date is null
        and is_imputed = FALSE
        and price < 1000000
    group by 1, 2
), p2 as (
    select p.token_address as mint
    , DATE_TRUNC('week', hour) as week
    , avg(price) as price
    , MIN(price) as min_price
    from solana.price.ez_prices_hourly p
    left join pc1
        on pc1.token_address = p.token_address
        and pc1.date = p.hour::date
    where hour >= '{{start_date}}'
        and pc1.date is null
        and is_imputed = FALSE
        and price < 1000000
    group by 1, 2
)
, cur_price as (
    select mint
    , price as cur_price
    from p0
    qualify(
        row_number() over (partition by mint order by hour desc) = 1
    )
)
, cur_swap_price as (
    select swap_to_mint
    , swap_to_amount
    , swap_from_mint
    , swap_from_amount
    , p.cur_price * s.swap_from_amount / s.swap_to_amount as cur_calc_price
    from solana.defi.fact_swaps s
    join cur_price p
        on p.mint = s.swap_from_mint
    where block_timestamp >= current_date - 1
        and succeeded
        and swap_to_amount > 0
        and swap_from_amount > 0
    qualify(
        row_number() over (partition by swap_to_mint order by block_timestamp desc) = 1
    )
)
, swap_price as (
    select swap_to_mint
    , swap_to_amount
    , swap_from_mint
    , swap_from_amount
    , p.price * s.swap_from_amount / s.swap_to_amount as calc_price
    , p.hour
    from solana.defi.fact_swaps s
    join p0 p
        on p.mint = s.swap_from_mint
        and p.hour = date_trunc('hour', s.block_timestamp)
    where block_timestamp >= '2024-10-15 10:00:00'
        and succeeded
        and swap_to_amount > 0
        and swap_from_amount > 0
    qualify(
        row_number() over (partition by swap_to_mint, hour order by block_timestamp desc) = 1
    )
)
, exclude as (
    select distinct tx_id
    , block_timestamp
    from solana.defi.fact_swaps_jupiter_summary s
    where s.block_timestamp >= '2024-10-15 10:00:00'
        and swap_to_amount > 0
        and swap_from_amount > 0
        -- and swap_program ilike 'jup%'
)
, t0 as (
    select s.tx_id
    , s.swapper
    , s.block_timestamp
    , s.swap_from_mint
    , s.swap_to_mint
    , s.swap_from_amount
    , s.swap_to_amount
    from solana.defi.fact_swaps s
    left join exclude e
        on e.block_timestamp = s.block_timestamp
        and e.tx_id = s.tx_id
    where s.block_timestamp >= '2024-10-15 10:00:00'
        and succeeded
        and (not swap_program ilike 'jup%')
        and swap_to_amount > 0
        and swap_from_amount > 0
        -- and swapper = 'DYK1gdufQPMvQ878zZ64v4mMJL24ReS9CXrn1Hhz6vxh'
        and swapper = 'runkuDMGfyVapyBXKJQyAGoMgQxwKU2Ba2qZy9BFj1n'
        and e.tx_id is null
    union
    select s.tx_id
    , s.swapper
    , s.block_timestamp
    , s.swap_from_mint
    , s.swap_to_mint
    , s.swap_from_amount
    , s.swap_to_amount
    from solana.defi.fact_swaps_jupiter_summary s
    where s.block_timestamp >= '2024-10-15 10:00:00'
        and succeeded
        and swap_to_amount > 0
        and swap_from_amount > 0
        -- and swapper = 'DYK1gdufQPMvQ878zZ64v4mMJL24ReS9CXrn1Hhz6vxh'
        -- and swapper = '9VhsSZ6ni7dZtmKRHE81yAd3UQW1oKu9LNEWRGFMA5wj'
        and swapper = '{{wallet}}'
)

-- select swap_to_mint
-- , count(1)
-- from solana.defi.fact_swaps
-- where block_timestamp >= current_date - 1
-- group by 1 order by 2 desc
, t0b as (
    select distinct t0.*
    , coalesce(p0f.price, p0f2.calc_price, p1f.price, p2f.price) as f_price
    , coalesce(p0t.price, p0t2.calc_price, p1t.price, p2t.price) as t_price
    , t0.swap_from_amount * f_price as f_usd
    , t0.swap_to_amount * t_price as t_usd
    , case when t0.swap_to_mint in (
        'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
        , 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
        , 'So11111111111111111111111111111111111111112'
    ) then t_usd when t0.swap_from_mint in (
        'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
        , 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
        , 'So11111111111111111111111111111111111111112'
    ) then f_usd else least(f_usd, t_usd) end as usd_value
    from t0
    left join p0 p0f
        on left(p0f.mint, 16) = left(t0.swap_from_mint, 16)
        and p0f.hour = date_trunc('hour', t0.block_timestamp)
    left join swap_price p0f2
        on left(p0f2.swap_to_mint, 16) = left(t0.swap_from_mint, 16)
        and p0f2.hour = date_trunc('hour', t0.block_timestamp)
    left join p1 p1f
        on left(p1f.mint, 16) = left(t0.swap_from_mint, 16)
        and p1f.date = date_trunc('day', t0.block_timestamp)
    left join p2 p2f
        on left(p2f.mint, 16) = left(t0.swap_from_mint, 16)
        and p2f.week = date_trunc('week', t0.block_timestamp)
    left join p0 p0t
        on left(p0t.mint, 16) = left(t0.swap_to_mint, 16)
        and p0t.hour = date_trunc('hour', t0.block_timestamp)
    left join p1 p1t
        on left(p1t.mint, 16) = left(t0.swap_to_mint, 16)
        and p1t.date = date_trunc('day', t0.block_timestamp)
    left join p2 p2t
        on left(p2t.mint, 16) = left(t0.swap_to_mint, 16)
        and p2t.week = date_trunc('week', t0.block_timestamp)
    left join swap_price p0t2
        on left(p0t2.swap_to_mint, 16) = left(t0.swap_to_mint, 16)
        and p0t2.hour = date_trunc('hour', t0.block_timestamp)
)
-- select * from t0b
-- amount bought, avg purchase price, amount sold, avg sale price, amount held, current price, net profits
, t1 as (
    select swapper
    , tx_id
    , block_timestamp
    , swap_from_mint as mint
    , swap_from_amount as amount
    , 0 as n_buys
    , 1 as n_sales
    , 0 as amount_bought
    , swap_from_amount as amount_sold
    , -swap_from_amount as net_amount
    , 0 as usd_bought
    , usd_value as usd_sold
    from t0b
    union
    select swapper
    , tx_id
    , block_timestamp
    , swap_to_mint as mint
    , swap_to_amount as amount
    , 1 as n_buys
    , 0 as n_sales
    , swap_to_amount as amount_bought
    , 0 as amount_sold
    , swap_to_amount as net_amount
    , usd_value as usd_bought
    , 0 as usd_sold
    from t0b
)
, labels as (
    select token_address as mint
    , symbol
    , name
    from solana.price.ez_asset_metadata
    qualify(
        row_number() over (partition by token_address order by modified_timestamp desc) = 1
    )
)
-- select t1.*
-- , m.name
-- , upper(m.symbol) as symbol
-- , coalesce(c.cur_price, cs.cur_calc_price, 0) as usd_price
-- from t1
-- left join cur_price c
--     on c.mint = t1.mint
-- left join cur_swap_price cs
--     on cs.swap_to_mint = t1.mint
-- left join labels m
--     on m.mint = t1.mint
-- order by block_timestamp desc


, t2 as (
    select swapper
    , mint
    , round(sum(usd_bought), 2) as usd_bought
    , round(sum(usd_sold), 2) as usd_sold
    , round(sum(amount_bought), 2) as amount_bought
    , round(sum(amount_sold), 2) as amount_sold
    , greatest(round(sum(net_amount), 2), 0) as cur_amount
    , sum(n_buys) as n_buys
    , sum(n_sales) as n_sales
    from t1
    group by 1, 2
)
, t3 as (
    select t2.*
    , coalesce(c.cur_price, cs.cur_calc_price, 0) as cur_price
    , round(t2.cur_amount * coalesce(c.cur_price, cs.cur_calc_price, 0), 2) as usd_remaining
    , round(usd_sold - usd_bought + usd_remaining, 2) as net_profit_usd
    , m.name
    , upper(m.symbol) as symbol
    from t2
    left join cur_price c
        on c.mint = t2.mint
    left join cur_swap_price cs
        on cs.swap_to_mint = t2.mint
    left join labels m
        on m.mint = t2.mint
)
select *
, sum(usd_bought + usd_sold) over () as tot_volume_usd
, sum(net_profit_usd) over () as tot_profit
from t3
where not mint in (
    'So11111111111111111111111111111111111111112'
    , 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
    , 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
    , 'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn'
    , '2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo'
    , 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So'
    , 'LSTxxxnJzKDFSLr4dUkPcmCf5VyryEqzPLz5j4bpxFp'
    , 'bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1'
    , 'jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v'
    , '5oVNBeEEQvYi1cX3ir8Dx5n1P7pdxydbGF2X4TxVusJm'
    , '3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh'
    , '7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs'
)

select *
from bi_analytics.twitter.twitter_accounts
where ecosystems[0]::string = 'sei'
limit 10

select sum(fee) from
solana.core.fact_transactions
where block_timestamp >= '2024-10-01 10:00:00'
and signers[0]::string = 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC'

with league as (
    select
        c.value:user_id::string as user_id,
        c.value:tag_type::string as tag_type,
        c.value:tag_name::string as tag_name,
        u.record_metadata:CreateTime::int as updated_at
    from
        crosschain.bronze.data_science_uploads u,
        lateral flatten(input => record_content) c
    where
        record_metadata:key like 'analyst-tag%'
        and tag_type = 'League' qualify (
            row_number() over (
                partition by user_id
                order by
                    updated_at desc
            ) = 1
        )
),
rankings as (
    select
        hdr.dashboard_id,
        COALESCE(tu.username, u.username) as username,
        COALESCE(tu.username, u.username) in (
            'Polaris_9R',
            'dsaber',
            'flipsidecrypto',
            'metricsdao',
            'drethereum',
            'Orion_9R',
            'sam',
            'forgash',
            'danner',
            'charliemarketplace',
            'theericstone',
            'sunslinger'
        ) as internal_user,
        d.title,
        dbt_updated_at,
        row_number() over (
            partition by dbt_updated_at
            order by
                ranking_trending
        ) as rk
    from
        bi_analytics.snapshots.hourly_dashboard_rankings hdr
        join bi_analytics.velocity_app_prod.dashboards d on d.id = hdr.dashboard_id
        left join bi_analytics.velocity_app_prod.profiles p on p.id = d.profile_id
        left join bi_analytics.velocity_app_prod.teams t on t.profile_id = p.id
        left join bi_analytics.velocity_app_prod.members m on t.id = m.team_id
        and m.role = 'owner'
        left join bi_analytics.velocity_app_prod.users tu on tu.id = m.user_id
        join bi_analytics.velocity_app_prod.users u on u.id = d.created_by_id
    where
        hdr.dbt_updated_at > '2024-11-01T17:00:00.000Z'
),
current_earners as (
    select
        hdr.dashboard_id,
        hdr.username,
        hdr.title,
        sum(iff(rk < 11, 1, 0)) as hours_in_top_10,
        sum(
            iff(
                rk < 50,
                1,
                0
            )
        ) as hours_in_top_N,
        array_agg(rk) within group (
            order by
                dbt_updated_at
        ) rank_trend,
        array_agg(dbt_updated_at) within group (
            order by
                dbt_updated_at
        ) rank_trend_date
    from
        rankings hdr
    group by
        1,
        2,
        3
    HAVING
        hours_in_top_N > 0
        and hdr.username NOT in (
            'Polaris_9R',
            'dsaber',
            'flipsidecrypto',
            'metricsdao',
            'drethereum',
            'Orion_9R',
            'sam',
            'forgash',
            'danner',
            'charliemarketplace',
            'theericstone',
            'sunslinger'
        )
),
current_stats as (
    select
        hdr.dashboard_id,
        hdr.ranking_trending,
        hdr.dbt_updated_at
    from
        bi_analytics.snapshots.hourly_dashboard_rankings hdr
        left join current_earners USING (dashboard_id)
    WHERE
        hdr.dbt_updated_at > DATEADD('hours', -1, current_timestamp)
)
select
    ce.dashboard_id,
    ce.title,
    ce.username,
    cs.ranking_trending,
    ce.hours_in_top_10,
    ce.hours_in_top_N,
    ce.rank_trend,
    ce.rank_trend_date
from
    current_earners ce
    left join current_stats cs using (dashboard_id) QUALIFY (
        row_number() over (
            partition by cs.dashboard_id
            order by
                cs.dbt_updated_at desc
        ) = 1
    )
order by
    5 desc


select marketplace
, program_id
, case when s.purchaser = t.signers[0]::string then 'buyer' else 'seller' end as role
, count(1)
from solana.nft.fact_nft_sales s
join solana.core.fact_transactions t
    on t.block_timestamp = s.block_timestamp
    and t.tx_id = s.tx_id
where s.block_timestamp >= current_date - 1
and t.block_timestamp >= current_date - 1
and s.succeeded
group by 1, 2, 3
order by 4 desc
limit 1000

select case when s.purchaser = t.signers[0]::string then 'buyer' else 'seller' end as role
, s.*
, t.signers
from solana.nft.fact_nft_sales s
join solana.core.fact_transactions t
    on t.block_timestamp = s.block_timestamp
    and t.tx_id = s.tx_id
where s.block_timestamp >= current_date - 1
    and t.block_timestamp >= current_date - 1
    and s.succeeded
order by s.block_timestamp desc, s.tx_id
limit 1000

select count(1) from solana.defi.fact_swaps
where block_timestamp >= '2023-06-01'



select *
from bi_analytics.twitter.twitter_accounts

with t0 as (
    select a.*
    , c.value::string as ecosystem
    from bi_analytics.twitter.twitter_accounts a
    , lateral flatten(
        input => ecosystems
    ) c
)
select *
from t0
where ecosystem = 'olas'
and account_type != 'flipside'
limit 1000

with t0 as (
    select convert_timezone('UTC', 'America/Los_Angeles', block_timestamp)::date as date
    , sum(fee) * power(10, -9) as daily_fees
    from solana.core.fact_transactions 
    where block_timestamp >= '2024-10-01'
    and signers[0]::string = 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC'
)
select *
, sum(daily_fees) over () as total_fees
, sum(daily_fees) over (order by date) as cumu_fees
from t0


select *
from solana.core.fact_token_balances
limit 10
where mint = 'So11111111111111111111111111111111111111112'



WITH dashboard_bans as (
	select distinct dashboard_id
	from bi_analytics.silver.dashboard_bans db
)
, user_bans as (
	select distinct user_id
	from bi_analytics.silver.user_bans
	where coalesce(end_time, current_timestamp) >= current_timestamp
)
, team_bans as (
	select distinct profile_id
	from bi_analytics.silver.team_bans
	where coalesce(end_time, current_timestamp) >= current_timestamp
)
, tweet_bans as (
	select distinct conversation_id
	from bi_analytics.twitter.tweet_bans
)
, ambassador as (
	select distinct user_id
	from bi_analytics.silver.ambassador
)
, twitter_accounts as (
	select distinct twitter_id
	from crosschain.bronze.twitter_accounts
), db_ids as (
	select *
	from bi_analytics.silver.dashboard_urls 
), most_recent_db_map as (
	select *
	from db_ids
	qualify( row_number() over (partition by dashboard_id order by timestamp desc) = 1 )
), pages as (
	select *
	from bi_analytics.silver.pages
	where timestamp >= current_date - 90
)
, id_map as (
	select distinct user_id
	, anonymous_id
	from pages
), dashboard_url_map0 as (
	select i.dashboard_id
	, right(split(p.context_page_tab_url, '?')[0]::string, 6) as context_page_tab_url
	, count(1) as n
	from pages p
	join most_recent_db_map i on i.slug_id = p.slug_id
	group by 1, 2
), dashboard_url_map as (
	select *
	from dashboard_url_map0
	qualify(
		row_number() over (partition by dashboard_id order by n desc) = 1
	)
), pages_viewed_by_user as (
	select coalesce(i.user_id, p.use_id) as use_id
	, count(distinct p.slug_id ) as n_dashboards_viewed
	, count(distinct m.created_by_id ) as n_creators_viewed
	from pages p
	join most_recent_db_map m
		on m.slug_id = p.slug_id
	left join id_map i
		on i.anonymous_id = p.anonymous_id
	group by 1
), dashboard_avgs as (
	select i.dashboard_id
	, AVG(case when id_map.user_id is NOT null then null when u.n_creators_viewed <= 1 then 1 else 0 end) as pct_views_only_this
	, AVG(case when id_map.user_id is NOT null then 1 else 0 end) as pct_views_are_creator_anonymous
	, AVG(case when p.user_id = i.created_by_id then 1 else 0 end) as pct_views_are_creator
	, SUM(case when id_map.user_id is NOT null then null when u.n_creators_viewed <= 1 then 1 else 0 end) as n_views_only_this
	, SUM(case when id_map.user_id is NOT null then 1 else 0 end) as n_views_are_creator_anonymous
	, SUM(case when p.user_id = i.created_by_id then 1 else 0 end) as n_views_are_creator
	, count(1) as n_views
	from pages p
	join most_recent_db_map i on i.slug_id = p.slug_id
	join pages_viewed_by_user u
		on u.use_id = p.use_id
	left join id_map
		on id_map.user_id = i.created_by_id
		and id_map.anonymous_id = p.anonymous_id
	group by 1
), mx_date as (
	select MAX(timestamp) as mx_timestamp
	from pages
	where timestamp <= current_timestamp
), dashboard_id_map as (
	select dashboard_id
	, use_ip
	, use_id
	from pages p
	join most_recent_db_map i on i.slug_id = p.slug_id
	qualify(
		row_number() over (partition by dashboard_id, use_ip order by p.timestamp) = 1
	)
), n_ips0 as (
	select dashboard_id
	, network_ip
	, date
	, count(distinct use_ip) as n_ip_groups_from_network
	, count(distinct context_ip) as n_ips_from_network
	from pages p
	join most_recent_db_map i on i.slug_id = p.slug_id
	group by 1, 2, 3
),  n_ips as (
	select dashboard_id
	, use_ip
	, date_trunc('day', p.timestamp)
	, count(distinct anonymous_id) as n_anon_with_same_ip
	from pages p
	join most_recent_db_map i on i.slug_id = p.slug_id
	group by 1, 2, 3
), mx_ips as (
	select dashboard_id
	, MAX(n_anon_with_same_ip) as mx_n_anon_with_same_ip
	from n_ips
	group by 1
), mx_views_10_mins0 as (
	select i.dashboard_id
	, TIME_SLICE(p.timestamp, 10, 'minute') as minute_slice_10
	, count(1) as n_views
	from pages p
	join most_recent_db_map i on i.slug_id = p.slug_id
	group by 1, 2
), mx_views_10_mins as (
	select dashboard_id
	, MAX(n_views) as mx_views_10_mins
	from mx_views_10_mins0
	group by 1
), mx_views_30_mins0 as (
	select i.dashboard_id
	, TIME_SLICE(p.timestamp, 30, 'minute') as minute_slice_30
	, count(1) as n_views
	from pages p
	join most_recent_db_map i on i.slug_id = p.slug_id
	group by 1, 2
), mx_views_30_mins as (
	select dashboard_id
	, MAX(n_views) as mx_views_30_mins
	from mx_views_30_mins0
	group by 1
), group_user_ids0 as (
	select distinct context_ip
	, user_id
	from pages
	where user_id is NOT null
), group_user_ids1 as (
	select distinct g0.user_id
	, g1.user_id as converted_user_id
	from group_user_ids0 g0
	join group_user_ids0 g1
		on g0.context_ip = g1.context_ip
	where g0.user_id != g1.user_id
), v0a as (
	select i.dashboard_id
	, DATEDIFF('minutes', p.timestamp, m.mx_timestamp) / 60 as hours_ago
	, p.user_id
	, p.anonymous_id
	, p.use_ip
	, dim.use_id
	, p.context_page_referring_domain
	, p.initial_referrer
	, n_ips0.n_ip_groups_from_network
	, n_ips0.n_ips_from_network
	-- , case
	-- 	when context_page_referring_domain LIKE '%exchange24%'
	-- 	or context_page_referring_domain LIKE '%p30rank%' then 1 else 0
	-- end as invalid_referrer
	, coalesce(n_creators_viewed, 0) as n_creators_viewed
	, case when coalesce(n_creators_viewed, 0) <= 3 then 1 when coalesce(n_creators_viewed, 0) <= 5 then 0.5 else 0 end as is_few_viewed
	, case 
		when (coalesce(p.context_page_referring_domain, '') = ''
		and coalesce(p.initial_referrer, '') = '$direct')
		-- or invalid_referrer = 1
		then 1 else 0 end as is_direct
	, GREATEST(is_direct, is_few_viewed) as possibly_sybil
	, case when page_in_session > 1 and is_few_viewed = 0 and is_direct = 0 then 1 else 0 end as not_first_page_viewed_in_session
	, SUM(possibly_sybil) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN 5 preceding and CURRENT ROW) as cum_possibly_sybil_6
	, SUM(possibly_sybil) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN 10 preceding and CURRENT ROW) as cum_possibly_sybil_11
	, SUM(possibly_sybil) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN 20 preceding and CURRENT ROW) as cum_possibly_sybil_21

	, SUM(possibly_sybil) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN CURRENT ROW and 5 following) as cum_possibly_sybil_6f
	, SUM(possibly_sybil) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN CURRENT ROW and 10 following) as cum_possibly_sybil_11f
	, SUM(possibly_sybil) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN CURRENT ROW and 20 following) as cum_possibly_sybil_21f

	, SUM(is_few_viewed) over (partition by i.dashboard_id order by p.timestamp) as cum_is_few_viewed
	, SUM(1) over (partition by i.dashboard_id order by p.timestamp) as cum_views
	, row_number() over (partition by i.dashboard_id order by p.timestamp desc) as view_rn

	, SUM(is_direct) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN 10 preceding and CURRENT ROW) as cum_is_direct_11
	, SUM(is_few_viewed) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN 10 preceding and CURRENT ROW) as cum_is_few_viewed_11
	, SUM(is_few_viewed) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN CURRENT ROW and 10 FOLLOWING) as cum_is_few_viewed_11f
	, SUM(is_few_viewed) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN 5 preceding and CURRENT ROW) as cum_is_few_viewed_6
	, SUM(is_few_viewed) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN CURRENT ROW and 5 following) as cum_is_few_viewed_6f
	, SUM(is_direct) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN 20 preceding and CURRENT ROW) as cum_is_direct_21
	, SUM(is_few_viewed) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN 20 preceding and CURRENT ROW) as cum_is_few_viewed_21
	, SUM(is_few_viewed) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN CURRENT ROW and 20 following) as cum_is_few_viewed_21f
	, SUM(not_first_page_viewed_in_session) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN 10 preceding and CURRENT ROW) as cum_not_first_page_viewed_in_session_11
	, SUM(not_first_page_viewed_in_session) over (partition by i.dashboard_id order by p.timestamp ROWS BETWEEN 20 preceding and CURRENT ROW) as cum_not_first_page_viewed_in_session_21
	-- flag if most page views are from users that don't view anything else (possibly bots)
	, LEAST(1, GREATEST(
		cum_possibly_sybil_6 / 6
		, cum_possibly_sybil_11 / 10
		, cum_possibly_sybil_21 / 19
		, cum_possibly_sybil_6f / 6
		, cum_possibly_sybil_11f / 10
		, cum_possibly_sybil_21f / 19
	)) as possibly_sybil_ratio
	, case when is_few_viewed > 0 and  (
		cum_is_few_viewed >= cum_views
		or cum_is_few_viewed_11 >= 10
		or cum_is_few_viewed_11f >= 10
		or cum_is_few_viewed_21 >= 20
		or cum_is_few_viewed_21f >= 20
		-- or (cum_is_few_viewed_11 >= 6 and (cum_is_few_viewed_21 + cum_not_first_page_viewed_in_session_21) >= 20)
	) then 1 else 0 end as is_few_viewed_flag
	-- flag if most page views are direct (possibly bots)
	, case when is_direct = 1 and cum_is_direct_11 >= 5 and (
		 (cum_is_direct_11 + cum_not_first_page_viewed_in_session_11) >= 9
		or cum_is_direct_11 >= 8
		or cum_is_direct_21 >= 17
		or (cum_is_direct_21 + cum_not_first_page_viewed_in_session_21) >= 20
	) then 1 else 0 end as is_direct_flag
	, case 
		when hours_ago > 24 * 2 then 0
		when page_in_session > 100 then 0
		when is_direct_flag = 1 then 0
		-- when invalid_referrer = 1 then 0
		when is_few_viewed_flag = 1 then 0
		when page_in_session > 1 then 0.05
		when p.context_page_referring_domain = 't.co' then 1 
		when is_direct = 1 then 0.25
		else 0.75 end * 
		POWER(0.95, hours_ago)
	as wt_views_0
	, case 
		when hours_ago > 24 * 14 then 0
		when page_in_session > 100 then 0
		when is_direct_flag = 1 then 0
		-- when invalid_referrer = 1 then 0
		when is_few_viewed_flag = 1 then 0
		when page_in_session > 1 then 0.05
		when p.context_page_referring_domain = 't.co' then 1 
		when is_direct = 1 then 0.25
		else 0.75 end * 
		POWER(0.979, hours_ago)
	as wt_views_1
	, case 
		when hours_ago > 24 * 30 then 0
		when page_in_session > 100 then 0
		when is_direct_flag = 1 then 0
		-- when invalid_referrer = 1 then 0
		when is_few_viewed_flag = 1 then 0
		when page_in_session > 1 then 0.05
		when p.context_page_referring_domain = 't.co' then 1 
		when is_direct = 1 then 0.25
		else 0.75 end * 
		POWER(0.9985, hours_ago)
	as wt_views_2
	, case 
		when page_in_session > 100 then 0
		when is_direct_flag = 1 then 0
		-- when invalid_referrer = 1 then 0
		when is_few_viewed_flag = 1 then 0
		when page_in_session > 1 then 0.05
		when p.context_page_referring_domain = 't.co' then 1 
		when is_direct = 1 then 0.25
		else 0.75 end
	as wt_views_3
	, case 
		when hours_ago > 24 * 45 then 0
		when is_direct_flag = 1 then 0
		-- when invalid_referrer = 1 then 0
		when is_few_viewed_flag = 1 then 0
		when page_in_session > 100 then 0
		when page_in_session > 1 then 0.05
		when p.context_page_referring_domain = 't.co' then 1 
		when is_direct = 1 then 0.25
		else 0.75 end
	as wt_views_4
	from pages p
	left join mx_date m on TRUE
	left join most_recent_db_map i on i.slug_id = p.slug_id
	-- if the dashboard creator and the viewer are the same, remove
	left join id_map
		on id_map.user_id = i.created_by_id
		and id_map.anonymous_id = p.anonymous_id
	left join dashboard_id_map dim
		on dim.dashboard_id = i.dashboard_id
		and dim.use_ip = p.use_ip
	left join pages_viewed_by_user pvbu
		on pvbu.use_id = p.use_id
	left join n_ips0
		on n_ips0.dashboard_id = i.dashboard_id
		and n_ips0.date = p.date
		and n_ips0.network_ip = p.network_ip
	where 
		p.is_dashboard = 1
		and coalesce(context_page_referring_domain, '') <> 'flipsidecrypto.xyz'
		and id_map.user_id is null
		and NOT coalesce(context_page_referring_domain, '') LIKE '%exchange24%'
		and NOT coalesce(context_page_referring_domain, '') LIKE '%p30rank%'
)
, warpcast as (
	select right(
		split(regexp_replace(split(split(c.text, 'flipsidecrypto.xyz')[1], '?')[0], '\\s+', ' '), ' ')[0]::string, 6
	) as dashboard_slug
	, d.id as dashboard_id
	, sum(reaction_type * POWER(
		0.9500, (DATEDIFF('minutes', r.created_at, m.mx_timestamp) / 60)
	)) as warpcast_rt_like_wt_0
	, sum(reaction_type * POWER(
		0.979, (DATEDIFF('minutes', r.created_at, m.mx_timestamp) / 60)
	)) as warpcast_rt_like_wt_1
	, sum(reaction_type * POWER(
		0.9985, (DATEDIFF('minutes', r.created_at, m.mx_timestamp) / 60)
	)) as warpcast_rt_like_wt_2
	, sum(reaction_type) as warpcast_rt_like_wt_3
	from external.bronze.farcaster_casts c
	join external.bronze.farcaster_reactions r
		on r.target_hash = c.hash
	join bi_analytics.velocity_app_prod.dashboards d
		on right(d.latest_slug, 6) = right(dashboard_slug, 6)
	join mx_date m on TRUE
	where c.created_at >= current_date - 30
		and c.text ilike '%flipsidecrypto.xyz%'
	group by 1, 2
)
, sybil_flags as (
	select dashboard_id
	, cum_is_few_viewed_21 as cur_is_few_viewed_21
	-- , MAX(cum_is_few_viewed_11) as max_cum_is_few_viewed_11
	-- , MAX(cum_is_few_viewed_21) as max_cum_is_few_viewed_21
	from v0a
	-- where hours_ago < 24 * 7
	where view_rn = 1
	-- group by 1
), v0 as (
	select v0a.*
	, 1 - GREATEST(0, ((f.cur_is_few_viewed_21 - 19) / 4)) as few_viewed_ratio
	, few_viewed_ratio * wt_views_0 as wt_views_0b
	, few_viewed_ratio * wt_views_1 as wt_views_1b
	, few_viewed_ratio * wt_views_2 as wt_views_2b
	, few_viewed_ratio * wt_views_3 as wt_views_3b
	, few_viewed_ratio * wt_views_4 as wt_views_4b
	-- , case when coalesce(f.max_cum_is_few_viewed_11, 0) >= 10 or coalesce(f.max_cum_is_few_viewed_21, 0) >= 19 and is_few_viewed_flag = 1 then wt_views_1 else wt_views_1 end as wt_views_1b
	-- , case when coalesce(f.max_cum_is_few_viewed_11, 0) >= 10 or coalesce(f.max_cum_is_few_viewed_21, 0) >= 19 and is_few_viewed_flag = 1 then wt_views_2 else wt_views_2 end as wt_views_2b
	-- , case when coalesce(f.max_cum_is_few_viewed_11, 0) >= 10 or coalesce(f.max_cum_is_few_viewed_21, 0) >= 19 and is_few_viewed_flag = 1 then wt_views_3 else wt_views_3 end as wt_views_3b
	-- , case when coalesce(f.max_cum_is_few_viewed_11, 0) >= 10 or coalesce(f.max_cum_is_few_viewed_21, 0) >= 19 and is_few_viewed_flag = 1 then wt_views_4 else wt_views_4 end as wt_views_4b
	, row_number() over (partition by use_id, v0a.dashboard_id order by wt_views_0b desc) as rn_0
	, row_number() over (partition by use_id, v0a.dashboard_id order by wt_views_1b desc) as rn_1
	, row_number() over (partition by use_id, v0a.dashboard_id order by wt_views_2b desc) as rn_2
	, row_number() over (partition by use_id, v0a.dashboard_id order by wt_views_3b desc) as rn_3
	, row_number() over (partition by use_id, v0a.dashboard_id order by wt_views_4b desc) as rn_4
	from v0a
	join sybil_flags f
		on f.dashboard_id = v0a.dashboard_id
), vw_date0 as (
	select i.dashboard_id
	, use_id
	, MIN(p.timestamp) as mn_view
	from pages p
	join most_recent_db_map i on i.slug_id = p.slug_id
	group by 1, 2
), vw_date1 as (
	select *
	, DATEDIFF('seconds', mn_view, CURRENT_TIMESTAMP) / (24 * 60 * 60) as days_ago
	, row_number() over (partition by dashboard_id order by mn_view) as rn
	from vw_date0
), vw_date2 as (
	-- start date is when the 5th person views it
	select dashboard_id
	, AVG(days_ago) as dashboard_age
	from vw_date1
	where rn <= 5
	group by 1
), views as (
	select dashboard_id
	, SUM(case when rn_0 <= 5 then wt_views_0b / rn_0 else 0 end) as wt_views_0
	, SUM(case when rn_1 <= 5 then wt_views_1b / rn_1 else 0 end) as wt_views_1
	, SUM(case when rn_2 <= 5 then wt_views_2b / rn_2 else 0 end) as wt_views_2
	, SUM(case when rn_3 <= 5 then wt_views_3b / rn_3 else 0 end) as wt_views_3
	, SUM(case when rn_4 <= 5 then wt_views_4b / rn_3 else 0 end) as wt_views_4
	, SUM(case when hours_ago < (24 * 1) then 1 else 0 end) as n_views_1d
	, SUM(case when hours_ago < (24 * 7) then 1 else 0 end) as n_views_7d
	, SUM(case when hours_ago < (24 * 30) then 1 else 0 end) as n_views_30d
	, count(1) as n_views
	, count(distinct user_id) as n_user_views
	, AVG(is_direct_flag) as pct_direct_flag
	, AVG(is_few_viewed_flag) as pct_is_few_viewed_flag
	, AVG(case when wt_views_4 = 1 then 1 else 0 end) as pct_twitter
	from v0
	where LEAST(rn_0, rn_1, rn_2, rn_3, rn_4) <= 5
	group by 1
)
select u.username
, v.*
from views v
join bi_analytics.velocity_app_prod.dashboards d
    on d.id = v.dashboard_id
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id

, vote_pct0 as (
	select d.created_by_id as creator_id
    , l.created_by_id as liker_id
	, count(1) as n_likes
	from bi_analytics.velocity_app_prod.likes l
	join bi_analytics.velocity_app_prod.dashboards d
		on d.id = l.resource_id
	group by 1, 2
), vote_pct1 as (
	select *
	, n_likes / (SUM(n_likes) over (partition by liker_id)) as pct_likes
	, 1 - pct_likes as liker_wt
	from vote_pct0
), votes0 as (
	select d.id as dashboard_id
    , d.created_by_id as creator_id
    , l.created_by_id as liker_id
	, coalesce(vu.username, u.username) as username
	, coalesce(vu.created_at, u.created_at)::date as user_date
    , GREATEST(0, DATEDIFF('hours', coalesce(vu.created_at, u.created_at), l.created_at)) / 24 as account_age
	-- , coalesce(pv.n_creators_viewed, 0) as n_creators_viewed
    , 3 * MIN(case when di.user_id is NOT null then 0 else POWER(LEAST(1, (account_age / 100)), 0.7) * POWER(LEAST(1, (n_creators_viewed / 100)), 0.7) end) as wt
    , MAX(l.created_at) as created_at
	from bi_analytics.velocity_app_prod.likes l
	join bi_analytics.velocity_app_prod.dashboards d on d.id = l.resource_id
	left join bi_analytics.flipside_app_prod.users u
		on u.id = l.created_by_id
	left join group_user_ids1 g
		on g.user_id = d.created_by_id
		and g.converted_user_id = l.created_by_id
	left join bi_analytics.velocity_app_prod.users vu
		on vu.id = l.created_by_id
	left join pages_viewed_by_user pv
		on pv.use_id = l.created_by_id
	-- get the anonymous id of the person that liked
	left join id_map li
		on li.user_id = l.created_by_id
	left join id_map di
		on di.user_id = d.created_by_id
		-- dashboard creator anonymous_id same as liker
		and di.anonymous_id = li.anonymous_id
	where g.user_id is null
	group by 1, 2, 3, 4, 5, 6
), votes as (
	select l.dashboard_id
	, SUM( case when DATEDIFF('minutes', l.created_at, m.mx_timestamp) > 24 * 2 * 60  then 0 else wt * liker_wt * POWER(0.9500, (DATEDIFF('minutes', l.created_at, m.mx_timestamp) / 60)) end) as wt_votes_0
	, SUM( case when DATEDIFF('minutes', l.created_at, m.mx_timestamp) > 24 * 7 * 60  then 0 else wt * liker_wt * POWER(0.9790, (DATEDIFF('minutes', l.created_at, m.mx_timestamp) / 60)) end) as wt_votes_1
	, SUM( case when DATEDIFF('minutes', l.created_at, m.mx_timestamp) > 24 * 30 * 60 then 0 else wt * liker_wt * POWER(0.9985, (DATEDIFF('minutes', l.created_at, m.mx_timestamp) / 60)) end) as wt_votes_2
	, SUM( wt * liker_wt ) as wt_votes_3
	, SUM( case when DATEDIFF('minutes', l.created_at, m.mx_timestamp) > 24 * 45 * 60 then 0 else wt * liker_wt end) as wt_votes_4
	, count(1) as n_votes
	from votes0 l
	join mx_date m on TRUE
	join vote_pct1 vp
		on vp.creator_id = l.creator_id
		and vp.liker_id = l.liker_id
	group by 1
), vote_avg as (
	select dashboard_id
	, AVG(wt) as avg_vote_wt
	, count(1) as n_votes
	from votes0
	group by 1
), has_chain as (
	select distinct dashboard_id
	from bi_analytics.silver.dashboards_queries_tags
		where tag_type = 'project'
)
, payment as (
	select dashboard_id
	, sum(
		case when ranking_trending <= 5 then 3
		when ranking_trending <= 10 then 2
		when ranking_trending <= 15 then 1.5
		when ranking_trending <= 20 then 1
		when ranking_trending <= 30 then 0.75
		when ranking_trending <= 40 then 0.5
		when ranking_trending <= 50 then 0.25
		else 0 end
	) as payment
	from bi_analytics.snapshots.hourly_dashboard_rankings
	where dbt_updated_at >= CURRENT_DATE - 90
		and ranking_trending <= 50
	group by 1
)
, 
with tweets0 as (
	select conversation_id
	, user_id as author_id
	, start_timestamp as created_at
	, impression_count
	, d.id as dashboard_id
    from bi_analytics.twitter.tweet t
	join bi_analytics.velocity_app_prod.dashboards d
        on RIGHT(d.latest_slug, 6) = RIGHT(split(t.clean_url, '?')[0]::string, 6)
	where t.created_at >= '2024-01-01'
	union
	select conversation_id
	, user_id as author_id
	, _inserted_timestamp as created_at
	, impression_count
	, d.id as dashboard_id
    from datascience.twitter.ez_tweets t
	join bi_analytics.velocity_app_prod.dashboards d
        on RIGHT(d.latest_slug, 6) = RIGHT(split(t.clean_url, '?')[0]::string, 6)
	where t.created_at >= '2024-01-01'
)
, overall_score as (
    select twitter_id
    , avg(score) as score
    from bi_analytics.silver.user_community_scores_monthly
    where concat(month, '-01')::date >= current_date - 90
    group by 1
)
, core_audience as (
	select distinct user_id
	, 1 as is_core_audience
	from bi_analytics.twitter.core_audience
)
, kols as (
	select distinct ta.twitter_id
	, 1 as is_kol
	, coalesce(score, 0) as score
	from bi_analytics.twitter.twitter_accounts ta
	left join overall_score o
		on o.twitter_id = ta.twitter_id
	where account_type != 'flipside'
)
, twitter_users0 as (
	select id
	, user_name
	, user_followers
    from bi_analytics.twitter.user
	union
	select user_id as id
	, username as user_name
	, followers_count as user_followers
    from datascience.twitter.dim_users
)
, twitter_users as (
	select id
	, max(user_name) as user_name
	, max(coalesce(user_followers, 0)) as user_followers
	from twitter_users0
	group by 1
)
, tweets1 as (
	select t.conversation_id
	, author_id
	, dashboard_id
	, min(created_at) as created_at
	, max(impression_count) as impression_count
	, max(coalesce(k.score, 0)) as kol_score
	, max(case when k.twitter_id is null then 0 else 1 end) as is_kol_tweet
	, max(case when ca.user_id is null then 0 else 1 end) as is_ca_tweet
	from tweets0 t
	left join kols k
		on k.twitter_id = t.author_id
	left join core_audience ca
		on ca.user_id = t.author_id
	left join tweet_bans tb
		on tb.conversation_id = t.conversation_id
	where tb.conversation_id is null
	group by 1, 2, 3
)
, tweets as (
	select *
    , (kol_score * 1.75) + greatest((is_kol_tweet * 2.5), is_ca_tweet) as author_score
	, concat('https://x.com/', u.user_name, '/status/', t.conversation_id::int) as tweet_url
	from tweets1 t
	left join twitter_users u
		on u.id = t.author_id
)
-- select * from tweets where conversation_id = '1846614310606590269'
, retweets as (
	select r.tweet_id
	, r.user_id
	, min(t.created_at) as created_at
    from bi_analytics.twitter.retweets r
	join tweets t
		on t.conversation_id = r.tweet_id
		and t.author_id != r.user_id
	group by 1, 2
	union
	select r.id as tweet_id
	, r.user_id
	, min(t.created_at) as created_at
    from datascience.twitter.fact_retweets r
	join tweets t
		on t.conversation_id = r.id
		and t.author_id != r.user_id
	group by 1, 2
)
, quotes as (
	select q.tweet_id
	, q.user_id
	, min(t.created_at) as created_at
    from bi_analytics.twitter.quotes q
	join tweets t
		on t.conversation_id = q.tweet_id
		and t.author_id != q.user_id
	group by 1, 2
	union
	select q.conversation_id as tweet_id
	, q.user_id
	, min(t.created_at) as created_at
    from datascience.twitter.fact_quote_tweets q
	join tweets t
		on t.conversation_id = q.conversation_id
		and t.author_id != q.user_id
	group by 1, 2
)
, rts as (
	select tweet_id
	, user_id
	, min(created_at) as created_at
	from retweets r
	group by 1, 2
)
, qts as (
	select tweet_id
	, user_id
	, min(created_at) as created_at
	from quotes q
	group by 1, 2
)
, qrts as (
	select coalesce(r.tweet_id, q.tweet_id) as tweet_id
	, coalesce(r.user_id, q.user_id) as user_id
	, max(case when q.tweet_id is null then 0 else 1 end) as is_qt
	, min(coalesce(r.created_at, q.created_at)) as created_at
	, max(coalesce(k.score, 0)) as kol_score
	, max(coalesce(k.is_kol, 0)) as is_kol
	, max(case when k.is_kol is null then coalesce(ca.is_core_audience, 0) else 0 end) as is_core_audience
	from rts r
	full outer join qts q
		on q.tweet_id = r.tweet_id
		and q.user_id = r.user_id
	left join kols k
		on k.twitter_id = coalesce(r.user_id, q.user_id)
	left join core_audience ca
		on ca.user_id = coalesce(r.user_id, q.user_id)
	group by 1, 2
)
, n_qrts as (
	select cur.tweet_id
	, cur.user_id
	, count(1) as n_prev_rts
	from qrts cur
	join qrts prv
		on prv.user_id = cur.user_id
		and prv.tweet_id != cur.tweet_id
		and prv.created_at < cur.created_at
		and prv.created_at >= dateadd('days', -30, cur.created_at)
	group by 1, 2
)
, qrts2 as (
	select q.*
	, tu.user_name
	, t.impression_count
	, t.conversation_id
	, t.author_score
	, t.dashboard_id
	, t.tweet_url
	-- , t.created_at
	, coalesce(tu.user_followers, 0) as user_followers
	, coalesce(n.n_prev_rts, 0) as n_prev_rts
	from tweets t
	left join qrts q
		on t.conversation_id = q.tweet_id
	left join twitter_users tu0
		on tu0.id = t.author_id
	left join n_qrts n
		on n.tweet_id = q.tweet_id
		and n.user_id = q.user_id
	left join twitter_users tu
		on tu.id = q.user_id
)
, t0 as (
	-- singular qrts
	select *
	, case when is_qt = 1 then 2 else 1 end as qt_mult
	, greatest(kol_score * 1.75, # log(10, greatest(10, user_followers - 250))) + (is_kol * 2.5) + is_core_audience as acct_score
	, power(0.9, greatest(n_prev_rts - 3, 0)) as n_prev_rts_mult
	, case when is_kol = 1 then greatest(0.33, n_prev_rts_mult) else n_prev_rts_mult end as rts_mult
	, qt_mult * acct_score * rts_mult as score
	from qrts2
	-- TODO: change to 30 days
	where created_at >= '2024-01-01'
	order by score desc
)
-- select * from t0
, t1 as (
	select conversation_id
	, tweet_url
	, dashboard_id
    , min(created_at) as created_at
	, sum(score) as score
	, sum(0) as author_score
	, sum(is_kol) as n_kol_rts
	, max(impression_count) as impression_count
	from t0
	group by 1, 2, 3
    union
	select conversation_id
	, tweet_url
	, dashboard_id
    , min(created_at) as created_at
	, max(0) as score
	, max(author_score) as author_score
	, sum(0) as n_kol_rts
	, max(impression_count) as impression_count
	from tweets
	group by 1, 2, 3
)
-- select * from t1 where conversation_id = '1846614310606590269'
, t2 as (
	select conversation_id
	, tweet_url
	, dashboard_id
    , min(created_at) as created_at
	, sum(score) as score
	, sum(n_kol_rts) as n_kol_rts
	, max(author_score) as author_score
	, max(impression_count) as impression_count
	from t1
	group by 1, 2, 3
)
select u.username
, t2.*
from t2
join bi_analytics.velocity_app_prod.dashboards d
    on d.id = t2.dashboard_id
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
, t3 as (
	select *
	, 850 + (score * 39) + (power(score, 0.5) * 270) + power(author_score, 0.5) * 200 as exp_impression_count
	, least(exp_impression_count, impression_count) as twitter_score
	, datediff('hours', created_at, current_timestamp) as hours_ago
	, twitter_score * power(0.979, hours_ago) as twitter_score_1
	from t2
	qualify(
		row_number() over (partition by conversation_id order by impression_count desc) = 1
	)
)
, id_map0 as (
	select user_id
	, anonymous_id
	, count(1) as n
	, MIN(timestamp) as timestamp
	from pages
	where user_id is NOT null
	group by 1, 2
)
, id_map_a as (
	select user_id
	, anonymous_id
	from id_map0
	qualify(
		row_number() over (partition by user_id, anonymous_id order by timestamp, n desc) = 1
	)
)
-- get main user_id <> context_ip mapping
, id_map_ip_0 as (
	select user_id
	, context_ip
	, count(1) as n
	, MIN(timestamp) as timestamp
	from pages
	where user_id is NOT null
	group by 1, 2
), id_map_ip as (
	select user_id
	, context_ip
	from id_map_ip_0
	qualify(
		row_number() over (partition by user_id, context_ip order by timestamp, n desc) = 1
	)
)
-- get their earliest page load
, earliest_page_load as (
	select coalesce(p.user_id, i.user_id, ip.user_id) as user_id
	, split(context_page_tab_url, '?')[0]::string as context_page_tab_url
	, timestamp
	, p.id
	, p.context_page_referrer
	, p.context_page_referring_domain
	, p.page_in_session
	, case when d.id is null then 0 else 1 end as is_dash
	, row_number() over (
		partition by coalesce(p.user_id, i.user_id, ip.user_id)
		order by
			case when p.timestamp < u.created_at then 0 else 1 end
			, case when p.page_in_session = 1 then 0 else 1 end
			, case when d.id is null then 1 else 0 end
			, timestamp desc
	) as rn
	from pages p
	left join id_map_a i
		on i.anonymous_id = p.anonymous_id
	left join id_map_ip ip
		on ip.context_ip = p.context_ip
	join bi_analytics.velocity_app_prod.users u
		on u.id = coalesce(p.user_id, i.user_id, ip.user_id)
	left join bi_analytics.velocity_app_prod.dashboards d
		on RIGHT(d.latest_slug, 6) = RIGHT(split(context_page_tab_url, '?')[0]::string, 6)
	left join bi_analytics.velocity_app_prod.users du
		on du.id = d.created_by_id
	where left(context_page_tab_url, 16) != 'http://localhost'
		and p.timestamp < u.created_at
		and p.page_in_session = 1
	qualify(
		row_number() over (
			-- get the most recent dashboard they viewed that was before they created their account, if it was the first page in their session
			partition by coalesce(p.user_id, i.user_id, ip.user_id)
			order by
				case when d.id is null then 1 else 0 end
				, timestamp desc
		) = 1
	)
)

, queries as (
	select q.created_by_id as user_id
	, count(distinct q.id) as n_queries
	, count(distinct r.tags:queryId::string) as n_queries_run
	from bi_analytics.velocity_app_prod.queries q
	left join bi_analytics.compass_prod.query_runs r
		on r.tags:queryId::string = q.id
		and r.state = 'QUERY_STATE_SUCCESS'
	where q.name <> 'Getting Started'
	group by 1
)
, f as (
	select u.id as user_id
	, db.id as dashboard_id
	, u.username
	, u.email
	, u.twitter_handle
	, u.discord_handle
	, u.created_at as user_created_timestamp
	, u.created_at::date as user_created_date
	, coalesce(e.context_page_tab_url, 'None') as initial_source
	, coalesce(db.title, 'None') as initial_source_dashboard_title
	, du.username as onboarding_user
	, coalesce(q.n_queries, 0) as n_queries
	, coalesce(q.n_queries_run, 0) as n_queries_run
	, coalesce(e.context_page_referrer, 'None') as context_page_referrer
	, coalesce(e.context_page_referring_domain, 'None') as context_page_referring_domain
	, coalesce(du.username, e.context_page_referring_domain, 'Unknown') as onboarding_source
	, coalesce(case when du.username is null then null else 'Community' end, e.context_page_referring_domain, 'Unknown') as onboarding_source_group
	from bi_analytics.velocity_app_prod.users u
	left join earliest_page_load e
		on e.user_id = u.id
	left join queries q
		on q.user_id = u.id
	left join bi_analytics.velocity_app_prod.dashboards db
		on RIGHT(db.latest_slug, 6) = RIGHT(split(e.context_page_tab_url, '?')[0]::string, 6)
	left join bi_analytics.velocity_app_prod.users du
		on du.id = db.created_by_id
	qualify(
		row_number() over (
			partition by u.id
			order by
			case when du.username is null then 1 else 0 end
			, case when e.context_page_referring_domain is null then 1 else 0 end
		) = 1
	)
)
, new_users_created as (
	select *
	, datediff('hours', user_created_timestamp, current_timestamp) as hours_ago
	, power(0.979, hours_ago) as wt
	, case when n_queries = 0 then 1 else 0 end as is_new_viewer
	, case when n_queries > 0 then 1 else 0 end as is_new_analyst
	, case
		when n_queries = 0 then 0 
		when n_queries_run = 0 then 1
		when n_queries_run < 3 then 2
		else 3 end as user_value
	, case
		when n_queries_run >= 3 then 'Serious User'
		else 'User' end as user_group
	, case
		when onboarding_source_group in ('Community','Unknown') then onboarding_source_group
		when onboarding_source_group = 'www.google.com' then 'Google'
		else 'Other' end as onboarding_source_group_2
	from f
	order by user_created_date desc
)
select * from new_users_created
where user_created_date >= current_date - 90



WITH t0 as (
    s
)
, dashboard_data as (
    SELECT 
        d.id as dashboard_id,
        d.title as dashboard_title,
        tw.conversation_id,
        DATE_TRUNC('month', tw.created_at) as month,
        max(tw.impression_count) as impression_count
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
        on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
    join bi_analytics.twitter.tweet tw
        on RIGHT(d.latest_slug, 6) = RIGHT(SPLIT(tw.clean_url, '?')[0]::string, 6)
    where t.type = 'project'
    and t.name = 'near'
    group by 1, 2, 3, 4
)
select month,
    COUNT(DISTINCT dashboard_id) as n_dashboards,
    SUM(impression_count) as total_impressions,
    AVG(impression_count) as avg_impressions_per_dashboard
from dashboard_data
group by month
order by month desc

with rk_hist0 as (
    select d.id as dashboard_id
    , date_trunc('hour', dbt_updated_at) as hour
    , avg(hr.ranking_trending) as rk0
    from bi_analytics.snapshots.hourly_dashboard_rankings hr
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where dbt_updated_at >= '2024-01-01'
        and coalesce(u.role, '') <> 'internal'
        and not u.username in (
            'Polaris_9R','dsaber','flipsidecrypto','metricsdao','drethereum','Orion_9R','sam','forgash','nftchance__','danner'
        )
        and hr.ranking_trending <= 100
    group by 1, 2
), rk_hist1 as (
    select *
    , row_number() over (partition by hour order by rk0 asc) as rk
    from rk_hist0
), rk_hist2 as (
    select dashboard_id
    , min(rk) as top_ranking
    , sum(case when rk <= 10 then 1.5 when rk <= 40 then 1 else 0 end) as n_hours_in_top_40
    from rk_hist1
    group by 1
)
, chain as (
    SELECT 
        distinct d.id as dashboard_id,
        t.name as chain,
        d.created_by_id as user_id
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
        on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
    join rk_hist2 r
        on r.dashboard_id = d.id
        and r.top_ranking <= 40
    where (t.type = 'project' or t.name = 'vertex')
)
select chain
, count(distinct user_id) as n_users_earned_top_n
from chain
group by 1


select t.name
, count(distinct q.created_by_id) as n_users
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
    on q.id = qtt.A
join bi_analytics.velocity_app_prod.tags t
    on qtt.B = t.id
where q.created_at >= '2024-01-01'
    and q.name != 'Getting Started'
    and (t.type = 'project' or t.name = 'vertex')
group by 1
order by 2 desc
limit 1000

select * from bi_analytics.twitter.tweet limit 10



SELECT d.id as dashboard_id
, case when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
    or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
    or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
    or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%'
    then 'Axelar' else INITCAP(coalesce(t.name, t2.name)) end as chain
, d.title
, d.latest_slug
, u.username
, u.id as user_id
, COUNT(DISTINCT q.id) as n_queries
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.velocity_app_prod.users u
    on u.id = d.created_by_id
-- join bi_analytics.velocity_app_prod._dashboards_to_queries dtq
join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
    on d.id = dtq.A
join bi_analytics.velocity_app_prod.queries q
    on dtq.B = q.id
-- join bi_analytics.velocity_app_prod._queries_to_tags qtt
left join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
    on q.id = qtt.A
left join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_tags dtt
    on d.id = dtq.A
left join bi_analytics.velocity_app_prod.tags t
    on qtt.B = t.id
    and (t.type = 'project' or t.name = 'aleo' or t.name = 'lava' or t.name = 'vertex' or t.name ilike 'kaia' or t.name ilike 'Klatyn')
left join bi_analytics.velocity_app_prod.tags t2
    on dtt.B = t2.id
    and (t2.type = 'project' or t2.name = 'aleo' or t2.name = 'lava' or t2.name = 'vertex' or t2.name ilike 'kaia' or t2.name ilike 'Klatyn')
    and t2.name = t.name
    and t2.name = t.name
where chain ilike '%mantle%'
group by 1, 2, 3, 4, 5, 6

select *
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
    on q.id = qtt.A
join bi_analytics.velocity_app_prod.tags t
    on qtt.B = t.id
where t.name ilike '%mantle%';

select *
from bi_analytics.velocity_app_prod.dashboards d
left join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_tags dtt
    on d.id = dtt.A
join bi_analytics.velocity_app_prod.tags t
    on dtt.B = t.id
where t.name ilike '%mantle%';


select seller
, count(1) as n
, sum(sales_amount) as volume
from solana.nft.fact_nft_sales s
join solana.core.dim_labels l
    on l.address = s.mint
where s.block_timestamp >= current_date - 14
    and l.label ilike '%degod%'
group by 1
order by 3 desc

select signers[0]:string as signer
, *
from solana.core.fact_events
where block_timestamp >= current_date - 1
    and program_id = 'CAsR78ednjQvTFTsUDQqzA9b7d5pnfUeqnytTmhGKEwU'
order by block_timestamp desc

select program_id
, swap_program
, count(1) as n
from solana.defi.fact_swaps
where block_timestamp >= current_date - 1
    and succeeded
group by 1, 2
order by 3 desc
limit 1000

with t0 as (
    select program_id
    , count(1) as n
    from solana.core.fact_events
    where block_timestamp >= current_date - 1
        and succeeded
    group by 1
)
, t1 as (
    select t0.*
    , l.label
    from t0
    left join solana.core.dim_labels l
        on l.address = t0.program_id
)
select *
from t1
order by n desc



select t.conversation_id
, date_trunc('month', t.created_at) as month
, case when tags.name = 'thorchain' then 'Flipside' when t.platform = 'Thorchain' then 'Thorchain' else 'Other' end
, sum(impression_count) as impression_count
, count(1) as n
from datascience.twitter.ez_tweets t
left join bi_analytics.velocity_app_prod.dashboards d
    on right(d.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)
left join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_tags dtt
    on d.id = dtt.A
left join bi_analytics.velocity_app_prod.tags
    on dtt.B = tags.id
where t.created_at >= '2024-05-01'
    and (t.platform = 'Thorchain' or tags.name = 'thorchain')
group by 1, 2, 3
order by 4 desc

select count(distinct conversation_id)
from datascience.twitter.ez_tweets
where created_at >= '2024-06-01'
    and platform = 'Thorchain'


select distinct t.conversation_id
from datascience.twitter.ez_tweets t
left join bi_analytics.velocity_app_prod.dashboards d
    on right(d.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)
left join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_tags dtt
    on d.id = dtt.A
left join bi_analytics.velocity_app_prod.tags
    on dtt.B = tags.id
where t.created_at >= '2024-06-01'
    and (t.platform = 'Thorchain' or tags.name = 'thorchain')


with t0 as (
    select t.conversation_id
	, concat('https://x.com/', coalesce(u.username, 'other'), '/status/', t.conversation_id::int) as tweet_url
    , date_trunc('month', t.created_at) as month
    , case when tags.name = 'thorchain' then 'Flipside' when t.platform = 'Thorchain' then 'Thorchain' else 'Other' end as source
    , case when source = 'Flipside' then 0 else 1 end as ord
    , max(impression_count) as impression_count
    from datascience.twitter.ez_tweets t
    left join datascience.twitter.dim_users u
        on u.user_id = t.user_id
    left join bi_analytics.velocity_app_prod.dashboards d
        on right(d.latest_slug, 6) = right(split(t.clean_url, '?')[0]::string, 6)
    left join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_tags dtt
        on d.id = dtt.A
    left join bi_analytics.velocity_app_prod.tags
        on dtt.B = tags.id
    where t.created_at >= '2024-12-01'
        and (t.platform = 'Thorchain' or tags.name = 'thorchain')
    group by 1, 2, 3, 4, 5
)
, t1 as (
    select *
    from t0
    qualify(
        row_number() over (partition by conversation_id order by ord, impression_count desc) = 1
    )
)
select *
from t1
order by impression_count desc


select *
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_tags dtt
    on d.id = dtt.A
join bi_analytics.velocity_app_prod.tags t
    on dtt.B = t.id
where t.name = 'bonk'



-- forked from Weekly Whale Performance @ https://flipsidecrypto.xyz/studio/queries/c070c3d8-88e2-40ad-9ec3-618f1ecbdcea

with p0 as (
    select price as cur_sol_price
    from solana.price.ez_prices_hourly p
    where hour >= current_date - 7
        and token_address like 'So1111111%'
        and is_imputed = false
    qualify(
        row_number() over (partition by token_address order by hour desc) = 1
    )
)
, manual_repaids as (
    select '3iL3wzxfEpg8g4LVEyf7x6MpWoSnxeeNhJ3ZcZbTm68ZRjcw2sfW7CUDYTRN4GahBWZbYxjvcsWHKqtqgX8VSSik' as loan_take_tx
)
, manual_sales as (
    select '5XeezttFqMZgZ5T8x7MANYwcyF1jMBzsG4MVideCSkruK5bwK8aCrfa6gcZXi7z1g48heHFfjp2SMj8mwWaQvJvJ' as loan_take_tx
    , 2.0 as manual_sales_amount
)
, t0 as (
    select i.tx_id
    , convert_timezone('UTC', 'America/Los_Angeles', i.block_timestamp) as block_timestamp
    , decoded_instruction:name::string as name
    , i.decoded_instruction:args:principalLamports::int * pow(10, -9) as amount
    , di.value:name::string as acct_name
    , di.value:pubkey::string as acct_pubkey
    , di.seq
    , i.decoded_instruction
    from solana.core.fact_decoded_instructions i
    , lateral flatten(input => decoded_instruction:accounts) as di
    where convert_timezone('UTC', 'America/Los_Angeles', i.block_timestamp) >= current_date - 11
        and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
    order by convert_timezone('UTC', 'America/Los_Angeles', i.block_timestamp), i.tx_id
)
, offerloans as (
    select o.tx_id as offer_tx
    , o.block_timestamp
    , o.seq
    , o.amount
    , o.acct_pubkey as escrow
    , l.acct_pubkey as lender
    , ob.acct_pubkey as orderbook
    from t0 o
    join t0 l
        on l.tx_id = o.tx_id
        and l.seq = o.seq
        and l.acct_name = 'lender'
    join t0 ob
        on ob.tx_id = o.tx_id
        and ob.seq = o.seq
        and ob.acct_name ilike 'orderbook'
    where o.name = 'offerLoan'
        and o.acct_name = 'escrow'
)
, takeloans as (
    select t.tx_id as loan_take_tx
    , t.block_timestamp as loan_take_timestamp
    , t.block_timestamp::date as loan_take_date
    , date_trunc('day', t.block_timestamp)::date as loan_take_week
    , t.seq
    , t.name as borrow_name
    , coalesce(ne.acct_pubkey, t.acct_pubkey) as escrow
    , case when ne.acct_pubkey is null then null else t.acct_pubkey end as old_escrow
    , b.acct_pubkey as borrower
    , m.acct_pubkey as mint
    from t0 t
    join t0 b
        on b.tx_id = t.tx_id
        and b.seq = t.seq
        and b.acct_name = 'borrower'
    join t0 m
        on m.tx_id = t.tx_id
        and m.seq = t.seq
        and m.acct_name = 'collateralMint'
    left join t0 ne
        on ne.tx_id = t.tx_id
        and ne.seq = t.seq
        and ne.acct_name = 'newEscrow'
    where t.name in (
            'extendLoanV3'
            , 'extendLoanV3Compressed'
            , 'takeLoanV3'
            , 'takeLoanV3Compressed'
        )
        and t.acct_name = 'escrow'
)
, repay as (
    select r.tx_id as repaid_tx
    , r.block_timestamp as repaid_timestamp
    , r.block_timestamp::date as repay_date
    , r.acct_pubkey as escrow
    from t0 r
    where r.name in (
            'repayLoanV3'
            , 'repayLoanV3Compressed'
        )
        and r.acct_name = 'escrow'
)
, foreclose as (
    select t0.tx_id as foreclose_tx
    , t0.block_timestamp as foreclose_timestamp
    , t0.block_timestamp::date as foreclose_date
    , t0.acct_pubkey as escrow
    from t0
    where t0.name in (
            'forecloseLoanV3'
            , 'forecloseLoanV3Compressed'
        )
        and t0.acct_name = 'escrow'
), t1 as (
    select o.*
    , t.borrower
    , t.loan_take_tx
    , repay_date
    , coalesce(r.repaid_tx, r2.loan_take_tx) as repaid_tx
    , coalesce(r.repaid_timestamp, r2.loan_take_timestamp) as repaid_timestamp
    , foreclose_date
    , foreclose_timestamp
    , t.loan_take_timestamp
    , t.loan_take_date
    , t.loan_take_week
    , t.mint
    , t.borrow_name
    , coalesce(m.nft_collection_name, l.label, 'Other') as collection
    , case when coalesce(r.escrow, r2.old_escrow) is null then 0 else 1 end as is_repaid
    , case when f.escrow is null then 0 else 1 end as is_foreclosed
    , f.foreclose_tx
    , case when orderbook = '2yGRv7B3TY9TM75LGUYy6p2UpMkUhMDh5HRBERKjnPj7' then 90
        when orderbook = 'b3eTZandg6wJGoV9auL9J8KCYTsCW7oeLixzLjKmvo1' then 90
        when orderbook = 'C22tLdLd6bPLnj9gC9JXYQMLyb94tusvGi8auVkQJdqj' then 90
        when orderbook = 'DmroJFsNv4Z3d1i1mxSCvubHA7WraDG8LfRdjHvtwrYV' then 120
        when orderbook = '9Qm5fJCgFBx31iqj69WdCFZ9sFJHBfwvd96w6HcVAH2a' then 120
        when orderbook = '5Qjuwat77TeBtcVg4weeieh9aBQ9GcBHSi3hQT8avwyt' then 120
        when orderbook = '2HZNcJm8R7XPCk8tWu7Y4oW7r5ZmVycrb1m5iZtFiEYn' then 120
        when orderbook = '8FYbwk3PbmgNpRuhf3cveXJZCWvDUso3ZdRodmGQohUR' then 120
        when orderbook = 'EQZU7u6hVy3bDVGMv16Q8tUDb9zmadEu9r2Qk2NmGfb7' then 120
        when orderbook = '6nTVvvJMP4eVnTqa2A8dhz2h9mcECagu6mHv4yMA642o' then 120
        when orderbook = '2LAL6ez8iUzQvAKeXuh78czmGjns5Kz45u4g36ipwYs8' then 120
        when orderbook = '5p13nk3YXgtdLrTAiEwd19SFaMkXdGfpvB8a6vxh9GXk' then 120
        when orderbook = '8mVzaaSNk8ijpvr7sHn6SAbnRySDpJKi5Zpbh2UYenzG' then 120
        when orderbook = 'GNsebpV1pRadbJ4KTK5jmQSMFssNTkDUxLbVEzwmnJ5Q' then 120
        when orderbook = '8srsRbC6o3ryLMzYVza9rGFFCfwhieKzCHXdRs3VSf7R' then 120
        when orderbook = 'DgPkzBd8JpC8W3ZQmxjyXEG2pSugrk8XLSxzg9qEWEAW' then 120
        when orderbook = '9VpbQvtDcqdUURxGsh4WzRL9V43yST6nZcYa7HX8ki2E' then 120
        when orderbook = 'APzKtY1eEfa9gvZKSiwmiLotBUQ7iwri8REtAocii5HW' then 120
        when orderbook = '55HyYqPiwahpJDU7WMDZiSJRjHCcqXNzejjVzc2KNuwc' then 120
        when orderbook = '5z5ZT9tvGxMxwDumBkSXo6GwWbt6J7qG2VQJCFooDq2d' then 120
        when orderbook = '9umYdARgjMPmSDU8jLk2nYjieaLKVdjMk3RLtCUUp826' then 120
        when orderbook = 'EHafnV1tDKFcqWatgN23Efboj3xdrUemyU7noWEpYZRh' then 120
        when orderbook = 'H4fv8sLz52uRcYCh7St7NEbGLCU7U4TuVnvDGQxkyMpM' then 120
        when orderbook = '4A3K1NPX7SLaJ6GUWdPoLHe5vbVz1ZVtGkH9EftHB6P8' then 120
        when orderbook = '6w2Gwht8Pb1D4x3eLDzr3DrRcP1FKF5hSAorFvzW9nGL' then 120
        when orderbook = '5NctcwUFGWkgs4t3oaHsGszzEppW86Bn7eLzGvFP1but' then 120
        when orderbook = 'CJXNmF8496hCE4eT1XRTjYKm2VNNzVduydJkt5JKQZiF' then 120
        when orderbook = 'H4BzEirDZQhU8NPMA8xRHj39JUUJbrZbUgPgfLXcmGor' then 120
        when orderbook = '27ReAHqW7WSJ4kqrU1o1eFpeg6QzQW4RsfSUKgeEXXSi' then 130
        when orderbook = '3r3H3qyu2vaPRawwRyguXH1YuUokhQzHStWcNDNyy7gw' then 140
        when orderbook = 'B3HXykcUXdMsmAzeZWa2bVvb6StwhsarsARJPcT74aRY' then 140
        when orderbook = '4unaSWSZUFjku5YhET2nn6BVDwfMpnBfXcoYnQ1bwvqC' then 140
        when orderbook = 'ALHXcLEzCVgA4GP9vc8xKVD2YgytzEYUsNzhPqjED11M' then 140
        when orderbook = '6akWWdsxQBL6f3DS1M37Ge64S35dKQwAkvbCRE5acrXa' then 140
        when orderbook = 'Au8EDW4HmPhBjzLYryZ5dPN5EeHrVoRjUyfAzhtgkEwC' then 140
        when orderbook = '4wKeU9Cbc3uRpjozJ3s5x1y67VgUa9T2SAG3g4r7958i' then 140
        when orderbook = 'B21JBKBptNueophVWWgkSr8zFwG17qmghy4koxEsY29Y' then 140
        when orderbook = '4Lmt2wLFLYbptyrSCQuSepmzMiGVeYta9FmRAnU3R12X' then 140
        when orderbook = 'HoaszYXduaKXrkmETPSETc8SDYCXztwAFvc7Q96uwW5E' then 140
        when orderbook = 'DeCQdHCSjEvXmYdekPaMRnJY1djo5sc4FgCCw7WpFqrx' then 160
        when orderbook = 'DZP2UfdE2bMXUWRjwmhfN9YSCxzRUEDqvjpmGXWtnT7x' then 160
        when orderbook = 'DMymDzcKcV2ymXzJJg4gXoX2Q5buzNNzDxn34WnoqDYb' then 160
        when orderbook = 'Cx1sPKmRz9H2SHgzayjafBRPfFaWzD8ocFq59w5ophkV' then 160
        when orderbook = 'CvRLWnb4Xn5dksNgi8t2vsdPtTE7JUhnWVnbdbGuyFNX' then 160
        when orderbook = '9AVp4GgALSkWhvbSiKJmddjiNKmkNdXtsPtU5Pkq1RBV' then 180
        when orderbook = 'AT7RtNRDjAVapMXVfms7W43sRFThV88bEvsp9HVeztnS' then 180
        when orderbook = 'B9dtGabrj4yGtXmz4w7ZHDMKEQPCHpkuJCzQX5dHRG3M' then 180
        when orderbook = 'HeCETNbqwBXUDtRf1NCihqzDRWKMjMD3P7NbmYdp5MCV' then 180
        when orderbook = '8k77HmE2uXzTPmgBzjovWPb2DfGZarm1VsmTLb6tzVaT' then 180
        when orderbook = 'Gf7wqmdJzvBwzaLURMGJT8Qt8r8pY1CSjMWQRTphiTCF' then 180
        when orderbook = '9hdY7aoMeA7ZAfKY2n3B7UoCuNvmjmnAhYMSn6jo8SoM' then 180
        when orderbook = 'EuVhyu4a2rk32Ndqtj3d8gEsy1RoUaiezrMG57mEjc9C' then 180
        when orderbook = '2tfe2gy1MtfX5dnnatbx1x1KK5f1EN9649LHkH3PZRk3' then 180
        when orderbook = '3B1NnYzd1R3iY2TNbtwkrEa3RPKPLeKdMLMJNYfYK1R4' then 180
        when orderbook = 'Gsjh6GEfzxCFsfpYM1cw7uNvbpd71aTsk57gjaxh1Ei3' then 180
        when orderbook = '4D8buxB4wBquP8Y18gY32jkPmjSpcC1MtQruEFGkdScA' then 180
        when orderbook = '6zP5FF55ZboKFBMKPFGUvgkKVsAsDVgTHpMKZxe1pZK4' then 180
        when orderbook = 'CASiRk1Bo5hL8uQTYUPYUgEaqMJtMmPtKaurhZiiuNKB' then 180
        when orderbook = 'BUVctfN6VeXj288s5iDbiNTC5SZTST1hcEAAhbEXTVyq' then 180
        when orderbook = 'EUQWL8zSRgaHTBJwxDBmLc2Uv7PwqUHVEGj3bmvDwmx' then 180
        when orderbook = 'CLqsLAUhvMBx81Br5QXLak3gYxQk7J9cQKGh3ToMDWKK' then 180
        when orderbook = '7N4MKNzWan4L7EwGA9poUt2HWLu7URRkQkfK9uQjkuVZ' then 180
        when orderbook = '3egbgXhFvEsmMBrw2XkZwy78L6d4q8xznhDvEphGUD8a' then 180
        when orderbook = 'A9kw9CxqoiqxANA2yEga4YCfv8r3QTA4t6UKrLnGuYAo' then 180
        when orderbook = 'GqifXjDe1BVP7rGGTB6gqVbtsD5p8kgBMiuwhgobxXcz' then 180
        when orderbook = '3dabeQGBbCJXSBD5g7Wn17yidQMJXUUf9S8C2jtYS3j4' then 180
        when orderbook = 'HNXH47hMbhVmNgBL1uahBvKomdJQVbdGuexnkxomazMz' then 180
        when orderbook = '9DzwzKKcYNkT5PaE2ajRqqxwotc8kAHHv1RomQoWpL4' then 180
        when orderbook = '7euLwMTdBMxWCsKmpkWpxCHChT8SZtdcPNBzcJGkm3kR' then 180
        when orderbook = 'DxBPJ9RttmB2ZrwMLZMymJKHWrkLx2M8DcFivcG6q6yy' then 180
        when orderbook = 'GVkHNC3RwCBSDWvXnoFHkwSgPeRHQvPi5iszGot2kZHi' then 180
        when orderbook = '9DtDt8VjUeLxmUrd7goboZey676sweoDBoPGfYXwE2wa' then 180
        when orderbook = '7y9inxTzv2M8qT2exD6gnMKtgmMZdLbymr9Z5nRDeARP' then 180
        when orderbook = 'FhyA9pdvEjPuNDfuw4c8kafj1sx7ksDNmByMLsexMojT' then 180
        when orderbook = 'BDzf4GfAvU7FUzn9HrcNJSdhumhiW6W2shKihnP8kx5s' then 180
        when orderbook = 'Htjia4NnhqSgz7zsn512MJSa6Q29swZqeiShuawUzkkn' then 180
        when orderbook = 'Don7pzRgGYmHWRSJrS6J3UfZfHTkdPzno1LNPWqCgBqZ' then 180
        when orderbook = '4wRei1pdjdahgboknVoNbB2BRWy218fEayUAKbSmR2QU' then 180
        when orderbook = 'J7D4848dsBWtAg98CJqgUbJCC83xXs1jVjyqthZS16du' then 180
        when orderbook = 'BawqUy9UGyhoxLHDikAyzmRCu84RpYRqKnH136NqjEvw' then 180
        when orderbook = '9P2hSoj1Bn1Wmw7WndT4QojPzdgAW3TUvw6vwpPi2CoQ' then 180
        when orderbook = 'GyAecDfySYfebR5FW2ccVhRtGFscT7VCxBs8f1c7Zkup' then 180
        when orderbook = '78j8mUBM14YVQoFZykukQiNumn5TqXdsR2xUSS45aoMj' then 180
        when orderbook = 'D9XgLBXv4BktVXZmzZXjs2e32BtL3ErPk5eWrmybvWpd' then 180
        when orderbook = '9Unt1XHZmN13S1VAso7qWzdXeV8iCm5xANHcUhx74fkV' then 180
        when orderbook = 'Dnanesbc8Nfo7B4QU5zQ8NBNHr5gaRSCWCNzokTD65gz' then 180
        when orderbook = '93jYxr7zpadU5LRWXy5A23GUYthBZbtw7UHQeoNLMe1J' then 180
        when orderbook = 'CXMxuGFd6gnybruPU7CRWNJ96zfddGaft4s9Z6znxeKg' then 180
        when orderbook = 'G3iVgPZYikEK6T9LLKZYex31GGA9SBkffuBfq3a2wDUF' then 180
        when orderbook = '2Hg7wSvZHNhqP7atAUjqQeZ3JQJnMf82VcGGTHjaLay9' then 180
        when orderbook = 'ERH4KWEFZDTHqyGY7qyKZ94jGy1SAF7H3TgUm6Zq3HGV' then 180
        when orderbook = '78rrWGz9HEnZYnGFUUN7fyJ8hPJPsuJaBqtP5MScnGgw' then 180
        when orderbook = '7nxjj1GEgZLC7Hf39ZYqStcahE1muAjv5hKsHfo8bouJ' then 180
        when orderbook = '9CxcERivY6qvaAuqfsqBLm4ncyeQk5mqYUD5BFkavT2V' then 180
        when orderbook = '8TFsdbjqYHDjj8yMPPwMCHX8NuXjaoJbmhqq8sJEg6LC' then 180
        when orderbook = 'H8w4jGYCyMFZnHiMNQS1bBaGKHGSmFWAHyEKjBtPmJYe' then 180
        when orderbook = 'AnDREtabYyRTcdpZJSCw8ZiM1cW5jJ5jZUuj3wqRcRSE' then 180
        when orderbook = 'FgygrXHEzDBTZYRzCA3QMSPe8CmdvFcgesBBqBW9r8hk' then 180
        when orderbook = 'LjA8vPRhcwiZY3o1YQdmQmu3Z7j3257U9bAPSJwGJW8' then 180
        when orderbook = '2fmf8fqYwf2qZ2BKgKHYJPwndCD9MUwXoNSB1tWezEzo' then 180
        when orderbook = 'EQ4C5ic9S5jJVxpYCv6mtPn2SUEAfE5f5BHqeMwwkuxZ' then 180
        when orderbook = 'BTMiWRvAR2HcccwbtxvSmhBMGyqpw36jFMjRhwcBMRo6' then 180
        when orderbook = 'BZbH5SdbvXA6BmEZie5RU3NdYHh8deUbPKCsotrwSRS5' then 180
        when orderbook = 'A6BdN8yuRMkee3aHbVWysQccHLHNAbDiW37PBSgp9EBQ' then 180
        when orderbook = 'An18C2FozPD42PnCW3Q1nXoEtcFnqbLALan3KEMwVnL3' then 180
        when orderbook = '2xnpNAtERkcaXfHbk9QgaFNMtUMsvS5pRueQG4BatwHk' then 180
        when orderbook = 'Er7bsT31HYoi54ZpuHiZN4n82NDGoAHtU7TzgJgZVoNH' then 180
        when orderbook = 'AvwJkrGhu7hsMjLdwzHYiPqLbKzKQsywkKrixZhK4Xy8' then 180
        when orderbook = '768FwUh1goEzvR3BvhFK2CjpU6RZC1hWqBQeh1Ykw9Hi' then 180
        when orderbook = '5HqzYzSYHy5TNEQn6c1ay7uo8GvanAcH9yLdeez5CG8a' then 180
        when orderbook = '4dFBenUZcHVKMtGnDh64jjYrKVkktjwPEvw587jL62PX' then 180
        when orderbook = 'HLxwZ5ZBPDDv8WaAJoAbB22yxZdGPvFvopTUnFGvLLvr' then 180
        when orderbook = 'DrgDSCJ5YCPoGms7RJYPf4tNxpP39eJyiRS5EznMS9Wx' then 180
        when orderbook = '4o5Gq3hpLr1CtGSJA8AY5yGkxksrCMA4KeT9MW2N82e3' then 180
        when orderbook = 'Eiups7qSZ394KE2u5YR6pSr2wdQNR1NgzWHLFdLP67r6' then 180
        when orderbook = '5ocFr92kMhbpzLiWGeFjbspWE7N4g3hxiK5NfKqKFZ9f' then 180
        when orderbook = '2tDWS8U2SVUtzDfRFYLaZeBVf6ELpEkefK8wPdCXWddF' then 180
        when orderbook = 'GxbDm6vqyHA6cKR9ECo7H42Hc2MHdZNwmLXaXzu4VvK4' then 180
        when orderbook = '82RNbMgyZz9HpGpMysBZ7CKh5MeVppLBPy2TYSU3dArc' then 180
        when orderbook = 'GTyFCDhH87cYSZkgwvGkEGd15JTXjyWRjEQ1LEiwWL3W' then 180
        when orderbook = '4P164Rc1S45zQktx4DU4GrTKKNtFnRS6jwkqiVqGYN2P' then 180
        when orderbook = 'AiYV1ZfNTNdcfyCsxQVGJUqdhHvfiMkkW1Dtif1RHf3o' then 180
        when orderbook = 'Bn4WWVkTJix7HYmH6cq86gZZobMT9mU8KWFMWHzJMcUH' then 180
        when orderbook = 'GF3o4ro4UYMobaknizyA69ZxUawfhxNNBF5uYrBwAtDz' then 180
        when orderbook = '21nMGd2vdgnPHUErL4uySzSk59cex9sHWZi2WiSDZguo' then 180
        when orderbook = 'AqjvMPHkgkKpdzDKbhcCsCyHjSsU9HnzV9t43vhAFMwj' then 180
        when orderbook = 'ES8e8REyNBcX9Fae2piF1ETcNwjn8ZbhsppcV5TMfvVQ' then 180
        when orderbook = '7mFNyhAV48FDLVEFneEDauP2KJ74zecRYhUCtzgtfWaL' then 180
        when orderbook = 'HLMxwGk3bhpHq4cfSv9AkcLDKzn976pHbDwKmSzaqeod' then 180
        when orderbook = '5vr2CkzTyQVZ2z17eYtCuysD6EfihKnZ8B48kLzu9gnw' then 180
        when orderbook = '4uzfpDhD38ptuDcXHc5xXxxmsejwrfLixXcS3nLiGEWU' then 180
        when orderbook = '9tsPpk7uFLBA6xusWS92jbx7ZrxvWAPhypDGSvDzqXXy' then 180
        when orderbook = '8ZoSrQC7S5DipsLfEs3fy9MaMyvBDMAnWSzJ9DWoMBRZ' then 180
        when orderbook = '2E1AuwUK7YV1kkdbruHAU4Y4f22gXPugGa8XoyA5pQJd' then 180
        when orderbook = 'ENg19HSdqvA6syxigLEcBRT85P4FrVD7zxeJQGM5pTUy' then 180
        when orderbook = '6ko3GGSac396pcsEGx7VkrDfKZPMdW9Fvf39MLpQ5vSq' then 180
        when orderbook = 'A2dhM8VWwexxhWnaSKA41sU4SJZjn77AZrxMfaAfTGUR' then 180
        when orderbook = '2VCQ4oPpXoDNsGf6U73dSnmpGdLYnsdZzv3Gb8SaUQxq' then 180
        when orderbook = 'FxVkepR7ueSt4KQnX8DLXC49m6iQsz38eHnNreSSPKM8' then 180
        when orderbook = '4M1LocPnpJuQQqn3QgjZRhBmmSUy2EA6X1VsYxyLxuTH' then 180
        when orderbook = 'AiogFPv38R5Y1HtWZQ2Lcint8JYjk9WjnzwuAc1DiD5k' then 180
        when orderbook = '8W9d5oBhyihLESonZPo3eyjQwSQc6hHH2D3CkibsxV5i' then 180
        when orderbook = '3T1CBuRBRbmD7R96sqCVrbLtJkDT5hpV18ojYkP8wsyt' then 180
        when orderbook = '2vxuqRDPgPATYzTiWS9YAgZijCv3kfRrpondmVm5s6th' then 180
        when orderbook = '48ztPvxAiNu76WDofscNN12w3B8xzGqtyNoW3m39nXSS' then 180
        when orderbook = 'Hq1sMcQVoEikLHimYtgFjTe5pCKQXEXxdMZ6fMkMnfbB' then 180
        when orderbook = 'GiazYRELcJoaES2iXfuarvY5eFy8gDsjRy6HcAkpAYjX' then 180
        when orderbook = '61MSjhovRLGm52dA7dJVpSYSM7emEn7EttRCDcVz9vTB' then 180
        when orderbook = '8dYPQnG6638fK822Q2rw25sPQ565SGvJpq54uZZSoowx' then 180
        when orderbook = 'FwQe7vJogwffMiEq1aosykZV2JrEXQK39BusjKx4CRgy' then 180
        when orderbook = '6NGKtLXrPJ3HqDduzM91VRWwRpRUXNWrQcpNKfeF92Cc' then 180
        when orderbook = '9Fcn3tFm7iNwNxhvJZmmLD3akdbSM991X5zSvVan2zZ9' then 180
        when orderbook = '3y2i1UJ9r2yezyoJtzpCjFSVDLG2EeQmi2kL7ueCnBXZ' then 180
        when orderbook = 'Lx38A6S6p1uA7PmkpM5sqfs2nsEVEHCcPJ2y3KkLXhu' then 180
        when orderbook = 'EvRMHrEpXdSGWL3Hkp6iW33swrQQXpKAEeTDcwKCSu9W' then 180
        when orderbook = 'GyEftrDrPHzJrGzDjA3avb4Cp4qrqZbnzaSdfDNUNMS2' then 180
        when orderbook = 'H4r3CpKRa94D16sSJLHdvSt6bb5tr7mZvFBWviQ2NByc' then 180
        when orderbook = 'mpqRBuDrc9z3SG4VCkVayXU1N2sDBFYXBvwmapF6P8b' then 180
        when orderbook = '6wEkzyWC6naFkHRRZosCoypxUT9XFqZXWLLqz69b3svr' then 180
        when orderbook = 'FoGRDfbbca7DuwYZfYYLCL1i4yB9Ui2RJvWQJNQ1Kygx' then 180
        when orderbook = '4QGzF6eDKzdFN8K1vRkZbcCm78m8PSD6sdcPSm6JT7W2' then 180
        when orderbook = 'EuKftNc8jQqvnXmJPuLFmxCYDBxoU9UXKdN4nWJa4igA' then 180
        when orderbook = '5u5nvQm4Pbxf9U5aNBZwDPaNqwtKzZyWSXVcdUE376vf' then 180
        when orderbook = 'q823vJW8QChuLM3KdYKJUjz81jqU1QL9jrMmBMjtRmQ' then 180
        when orderbook = 'CmiMEYEQk3hWVgBAzytcRrrAv5wkg48zcteTf3gEfF4F' then 180
        when orderbook = '5Y2KbbFvFxEeuCVjJmcY6ZVL89yGAPEhRwaWUnQURm4B' then 180
        when orderbook = 'E7XUUQt15uF5fsLhm9nBAxb4B5fvUzwK4ErVKbgvdzDB' then 180
        when orderbook = '7qeucfDbqkRtZ4L6U7ffbnsuv9fpCt2FKkkrv4mssAKU' then 180
        when orderbook = 'AcErr8rKpaeuTi2UohJRN8W4mTgi4qA1cU6ZvHWGWvgn' then 180
        when orderbook = '2DJdf5JbQM5V7awmGpmhHWiMdPZaU3KBWfX8nmoBj62X' then 180
        when orderbook = 'HnUg9gViy4eLTsxhjrZSg3uGB4giQuuBanK7ewTebD6z' then 180
        when orderbook = '6Rv7xZQdsgunArSS4RxwnTaqXRJTrXDKjaVqTdtXduMS' then 180
        when orderbook = 'ENVRWTkkBCA8HPyKLo17jVFPY5cb2DsEv27J6Kdbxmf7' then 180
        when orderbook = '4gHeHya2DVHjGwkosFvgsfVpGn8gWA6RSuxgmF1agPXL' then 180
        when orderbook = '8u7Q5Tz4BYjBK2STktrCZkvkMxCSFUB3mBkP6q2mFyq6' then 180
        when orderbook = '6dVkZhpPyZeUZ15wrC1kNK3gmG839BXLSW4QrVrCSSP9' then 180
        when orderbook = 'H3RrLuxoCpQCeoJai8yQEPfursKfBb6pc3UwedGKjUV9' then 180
        when orderbook = '8QWYCXWafc9KZd9UxDJbK5cq2MNyVo1REVFttEW76vSe' then 180
        when orderbook = '6XPvCp6dkpAjAnc3Lnt11njVpzXb1fMwAFx4p877VyU9' then 180
        when orderbook = 'CX7YdDitGaYgahm3AYCDtZ9hfNB5rdEBVBm1nW2Dk8vp' then 180
        when orderbook = 'DonD3ZiRTX5w83bKfDzuQjKff4vhoYnswC2AwFiuvWS4' then 180
        when orderbook = 'EHT9b3sUBhBZf5EpaRuzZ8q5WXfRxzkiEogy5AYiVXB1' then 180
        when orderbook = 'CkSsL9uLs1XUGXrcqzFzDVHzsToh92DBM1W5rKBxUA9D' then 180
        when orderbook = 'C46Dx2jVCCfce8ar3R6gHTC7kfWzWuWT4bomA8jNfYRJ' then 180
        when orderbook = 'CgGEp5KwEKgPNPoWzyw17X77J9pF3utyKGKxJSbSRkCF' then 180
        when orderbook = '6sdGWTwFtMpcKdRH4fBpDPNFfz3rT4ZSfzh1hnNTsDqD' then 180
        when orderbook = 'CMKh9Ww7x9Tz23fgjzLpZietXNEZVny3TzNjQcTEkDru' then 180
        when orderbook = '8jB7GDv4ZutSPuFHcu7yf4baG5ATFccKD2MU5PjyzDRA' then 180
        when orderbook = '9bondED2Z31sd349pM73Yp7wTkCbav9f1kj5EHHhmh9p' then 180
        when orderbook = 'DZojZiK2LbZeDG5Cvf8g7XFhVJrvYUZ7gH8wweNDTV7L' then 180
        when orderbook = 'J6nJ2PmphDLFc94kbKnihAQpzmr3uHmYW9wVtMR6WfFb' then 180
        when orderbook = '8YWPutChRuiBpXyDAX4nBKeZTm9DyCFqEhk6QmKC5Gge' then 180
        when orderbook = 'GZQ7DZt3YRr9gGfGSJGoxmXk1r6rkJxgG78Q5SZFiAa9' then 180
        when orderbook = 'EobeQZX1P2aSejXCdMg5qSjDAZCNLGJ8tYKdEyPs1s4w' then 180
        when orderbook = '4xYsBo1LfYLQVczFftN3fA1ZsGnwcKwghXPX5CMY1Kwc' then 180
        when orderbook = 'FFNbSaZb1DJyDapZks665QWzyCiLtVsGjoBuF1GLggaH' then 180
        when orderbook = 'EGQR5uwZ1XEwBmvdmgvHRAyrgMqMPQ3ckEZtjAYZ7ioa' then 180
        when orderbook = 'RU4Nj23JHbnonAskwpEjRrCPEADyiraZMu6rg2b3yrL' then 180
        when orderbook = '12bwZJRHR95Lwk1domF7B32tci5vGkHxQJRXbvJBYPqM' then 180
        when orderbook = '8dU3A9QPJ4QqrVcesdMV2wRvVtXRwetBLxeTmRbcM42w' then 180
        when orderbook = 'J2xZYp9YZj3eQM7QwDQQc9pWFjvGAe1YCmZL1crKVUse' then 180
        when orderbook = '2aLUgFVkqGRahZXUZHQjD89PCwaeZjpCPQ6BG4h3A9f7' then 180
        when orderbook = 'A8YhiSo4xE9poXBWnoSiJHzPg7fToX9TToB2rzfsEPUH' then 180
        when orderbook = '3LdoUZUs9iBxAH8Vqs55rDsMNmRFUAkewjuiL331eq3E' then 180
        when orderbook = '55uGhbsa2aqV7EDn8MMkFDMH6w9jFVizGhhFr52mCPMW' then 180
        when orderbook = 'HRSWHYrKNSwhRi2ktg72znV3gCZ5uBLbnbfL7TBzjY2q' then 180
        when orderbook = 'HLUqWiemgEPdfz9w5WeP4nxsW3JzHayQX6n3zbTVGnaT' then 180
        when orderbook = '3rmQkJCAMRGEeqmmh836FiUryKEarhvcHV87Cqgs4p3q' then 180
        when orderbook = 'HHwRQztu3CS24z3eMntzM4WFAPc8431stY4VerjarUk4' then 180
        when orderbook = '3pKudKtuNJNhPUFQcDVcMS8ZY7k1ZMdjMbHS1rx739f7' then 180
        when orderbook = 'AF1JtCWgPRUKk6S5znp9xhgcby6PoPhDkxDZUrCmKjSw' then 180
        when orderbook = 'BnJxJvfrL4fCuxwWJdgNQcEWWXFCcMAS37BnMUXVigZZ' then 180
        when orderbook = 'EJUU8Skh6dqGgQfQkL2i486rwPjo2ZsfJzKF5UsTfEnp' then 180
        when orderbook = 'BywAQ4p6d8zBgcJp2cZgdFUpmxioBSQoD42PZjA3JJdd' then 180
        when orderbook = 'CrdwR9sYXFt5YTSPUQfftRCeFehih7McvHG715nv4ty2' then 180
        when orderbook = 't9MMqVPCLJTH3sj6gYK2uRJm9oc9TnaNB2WGJthn8ur' then 180
        when orderbook = '5izELWAwe6LeCBufg7t8RuL7vUvnaQ17PvbQ18Bmwjwv' then 180
        when orderbook = '9TUKMjGJeu87bz3WkqEae7CFxaSZ9wTWjQ51M3CAWivN' then 180
        when orderbook = 'EzmTj4P8xJEQmbiUhi3ZDZ3fA3NdjaBWRd58R3Z6QMC5' then 180
        when orderbook = 'GkSxfdpjFu3vJUKDtzztuBEwf2VrTPmFmprp5asNR6t8' then 180
        when orderbook = '7J4yvUhLR6NYJjQewKydhyUgymn9SNWrpRRaLF6xoip5' then 180
        when orderbook = '7FbuhkNjvis2wk3vxu9jpeHjgxpr2Myyw9vmnbVWoPLT' then 180
        when orderbook = '4Zat4ZrTRMzXUPyYSB6DF39r4GZ6kaQb1yxZaeT9MybT' then 180
        when orderbook = '8KxSqLgVU1D57fUh2LaAKSrgeCY9L7L2jhgwNrByTPMw' then 180
        when orderbook = '5Pw1hC8xJ8NuJTJGCkscyyJMxBQRGosX11MWoFCZn223' then 180
        when orderbook = '4WakAKCjYT5NRuW4YBKQvXwnMATszCywzB9taBrcMVnK' then 180
        when orderbook = 'H34ZXKSqaiBzLEm3f2sBzsr91hovxvRpowM6fY6Lz84n' then 180
        when orderbook = '6hkg39J1kbQCfCnNBUybktooRJ1eDLpTfywrz3CU9N7o' then 180
        when orderbook = '491W1kai4qheuFZmwQ41gb2nZvo3kdMRgJmHhSgTxaYG' then 180
        when orderbook = 'EvUQv9xrWih4mp5A8HpaNubVWMKxhPEtJxNvkUtSH6o7' then 180
        when orderbook = 'H9boXKfdYugdRtNt323XBRiErxHgQAWM5DEbGjashf3T' then 180
        when orderbook = 'GZRQXy5239hYyKpyFtT2u9ZzFYB9iCXNK3vN9DSor2oY' then 180
        when orderbook = '9uscsVbP9VSk5qXSUzZfYmaoFKJVouFx2PZbTby3ME1P' then 180
        when orderbook = 'FCp7sa9rvhyjBSSh3e5G1jG22KP122ayr1nTwnZN11ys' then 180
        when orderbook = 'B3KewE2K26EVCJLzZphSLtM5RxU9zRqoDyhJnUo7i1Hb' then 180
        when orderbook = '9DLwMNE11swnwZRxUw8DaXtFkyr2uJ1QxmH3uceseudN' then 180
        when orderbook = 'CiVsiMMZ5jtRbUgQkbr1KXns2gQfzG2g2o1uDUasue42' then 180
        when orderbook = '9oSjfQuwEXpkTDZoy2ycRmzRu1oHoHeim88MRWbCa8m1' then 180
        when orderbook = 'FkfpuRrvwLFdL6cUfXXnyic2m87AFftZ89A1sBmK7gQz' then 180
        when orderbook = '51mumtNVARBwgcEJ63rbLwTxeg1WrScQ9BmB9RVmeyQq' then 180
        when orderbook = '7Xp6sTDvDYFuRxomVfQ2Jt5tuaMhLXu9tNyYpYg6UDQy' then 180
        when orderbook = '6J7jKmjtc8Drg8LB3pPm6rridrwkz94NyJR7A8Mr5BiV' then 180
        when orderbook = '7a4pjsUTS7TEjBuFwLCAPNMrmRKvTUTALNnWUztNxTQe' then 180
        when orderbook = '5A7mLirEu1YW6zzyke8yURcZYrUR49fysLh8tDeEv1s9' then 180
        when orderbook = 'GgvVjAcWyaaiQgBe9MEa49aexLiFtj5Dn38xaXfGwudg' then 180
        when orderbook = '9aXFDrqbijYjwLLDQdgJV71A91AYAgBpRUEjKQrij1Gh' then 180
        when orderbook = '2kew1EtHBmxGdXt6BsbwCmLp5L5pycNY3YdWtnPMQ9XB' then 180
        when orderbook = '2yaSK3v2gmnXv8WycKCx5gjenVtzLoA3o4g86No8zwhT' then 180
        when orderbook = '9YiGTok6R73gEzFsRrZ58NYM9xd7eopQA9o1VRvHH9nS' then 180
        when orderbook = '5BYEHf5K1LGZWG2RbBSbWKA9n9DhyvKxuBvb45SAcZEt' then 180
        when orderbook = '7jVrLg3hu45nynukLzERDUj4Dwefqbx9XdB1MHRV9vN4' then 180
        when orderbook = 'ARMmkLcKMu6aiS76Wjx4d2fmYPd9f4t3dR7Esjryeh4W' then 180
        when orderbook = '3b6XwiXNribYmbo4P9TSP7LHYAcbP3YUPGYWNpfwfBMp' then 180
        when orderbook = '77M8cSPgxQhabwRqcu5eqBwt4sqinrMXFtsfiSfxjuny' then 180
        when orderbook = '3jvbiRg6ijBEpaoNvKsyH4KJYPui3NfjsZcwoDDPuB9j' then 180
        when orderbook = '4MYqTKy8pzCtUXrr63zwn3hDw84TfeV8XtvdM2TYB7pq' then 180
        when orderbook = '8eSZEQULZ9aq8PQ8pu7nKaTZJzWX5tkqDT8HCRcLyXyz' then 180
        when orderbook = 'Gx7zWJSXfyk1yUci5CB156pMQuUMxzPWJsPSDmcX8hjC' then 180
        when orderbook = '5vSZcek5ciwAYUezevXf8Hd2BrFH4Vq9cgGf6yDWzVSL' then 180
        when orderbook = 'WnmSJ8UTXTuBq2PtERH6t63Ki4RnX9KT8aovxDVYSc3' then 180
        when orderbook = 'HDeoWDeLZuqdzzKAsALjKtBpFoJSCRu3T5aGDbxmM1sG' then 180
        when orderbook = '3JELLvYuiwdefb6WEDJGCayXR28ieJXGBE3t3peVQymA' then 180
        when orderbook = 'CD3SE2DZoEP4YhTz3jW8syWvC8tM9Ut2bT8JzRnmbcYT' then 180
        when orderbook = '5my9WYhnsdSLXUEqG5FDNjwZTDfvMUP3V2FE5CAoVMhM' then 180
        when orderbook = 'BcsrQeev9j4WhLzKzwAk2yrJzasGU8CnrmnGec5TtZUF' then 180
        when orderbook = 'AfsMEncggLE7XJsrkBXuVcSJAZQAfgJbaakDC8kMHkmy' then 180
        when orderbook = 'DZ8G7bqi8QNrsuoZoTEHBrBaLhyjocS6KvtQMBFa1eit' then 180
        when orderbook = 'CAAsGaktYdVWATM2ptoYHyxxz8ZouDM5rj68RDqbEdEg' then 180
        when orderbook = '5kQ4WnxUtU2JKTfHjyNbRi2XYXJCsb8sLcXX4abrUzqJ' then 180
        when orderbook = '2kBdGCy8NTszWLuB3ktdZZyus1xL3LWzSTR1wMoogvWb' then 180
        when orderbook = '73irCghxWGmQsVNyoRnUdknNqrWGYPDU5RCwk96X7mzJ' then 180
        when orderbook = '2xitAYkoG8S9vwFb45WSKzYe9nS6fTULjqffQcjDDnkv' then 180
        when orderbook = '6xgVU3rwwYAV1288795GYgsMSBBrozo9eNuVRi22CB7e' then 180
        when orderbook = '2dz33qzKLnSb6ibA6CZhDc7YZAw6EGz4aGoBp3DZ2AHb' then 180
        when orderbook = 'BJNwChqPBXJ2jbtYnBHXcAX9zvQQiYWaGyvv6pDcAjda' then 180
        when orderbook = 'HzDeqUuMgtaZweahb8tVi6JPCGH4XdTFspA6JZEa5neM' then 180
        when orderbook = '8AsZaDn1kCqjn2xvivfjLVXhKMZzz1zuaANhSE3mmVRZ' then 180
        when orderbook = 'EmMvAcxpea8JhsZnzTgkrYEQB9pb5DBRTrR4W3BeJpcP' then 180
        when orderbook = '7naVAd7cWNUMCiDVJbxFYTYgeGrPzj8TPek1V1Wjyy2u' then 180
        when orderbook = '87xXE1URecyTH9ywwNwPAsjTSxRkBXhS9M6aPvTyn92R' then 180
        when orderbook = 'eNMQBwZ23b5V3JEyjjSgGLuJWU34HLC8M7Pe4fkSFgq' then 180
        when orderbook = '7jmwXYq2BZWJTNNDoe3uzPjwaRSqdV2g1pj1D8ZXEX7f' then 180
        when orderbook = '7sS98YT4ap1ZcyUiEXSuwY1kPTADKFk9bto3L2jJud19' then 180
        when orderbook = 'H6ku1Kxf4SeL63deW3ZcZuMXr9b5jW83GHzX1ZaUU3Qj' then 180
        when orderbook = '9pSirXyVoxCmmemLAQZW2uxrYxtTJFCv9t2vQnieAmaJ' then 180
        when orderbook = 'Gwy2x8VHjVoH1wwKPg5Ttruue8gKUcZitfdsss9x63Qv' then 180
        when orderbook = '84Yd6ZEeoEXE4QiProVtg7o7rAcW3g1hX7CRRAyceMQa' then 180
        when orderbook = '2CkZh2mk6Pbw3hPgvFpnShiQ8maAnVB6supDNrWd5PwL' then 180
        when orderbook = '7gWrhoy4rJJXRkE7kH3Fio1nFxU3EtEUhKXS6R57uguk' then 180
        when orderbook = 'HETGR2S9mJhJDKteGQ8EevsmPG82jF3MSxWtdHkSfofB' then 180
        when orderbook = '7U2Gd4qY3iY7uYvk7XxbJ5o8ndNALvPD2Wh77shUuKvp' then 180
        when orderbook = 'FyfqopY6DSiHzJ7gfUwv6z9bQYctc1LjCrWffqeiEmqe' then 180
        when orderbook = '6KWFfy33UfQE5mXdSptLUQJwfeq4ZZGEb7Rhzf3KApCx' then 180
        when orderbook = '3dUqrnBDimZiXctwEMz7jeDJ9nBcwi6oRyBBP99JJfHN' then 180
        when orderbook = '6BMHJcR9wGB4V8Z9JAEqhxSZjmrsbqx253JPAX47Tgix' then 180
        when orderbook = '3CBNipV6cQ4xyxRZyzVYQPQEUvNuUEbqrUidjhjHSZND' then 180
        when orderbook = '6hthxTrRELgojgSrXbR8ruJZt9Y8hgwq2KQUN29qmGEp' then 180
        when orderbook = 'FWaLM18XF5yT1qmf5yMWkruqP3aW5PPG8LMtc2hjDiLF' then 180
        when orderbook = '2Nb9nmMzhpzXVA2s2ymYdhwsAw7EFa8bZ2qcdyPQgmF2' then 180
        when orderbook = '7yHXqKKutDyHneRZBnyKo36BXTp492q5pXMyJQiVVWMe' then 180
        when orderbook = 'GE6Untcqtp7JDq8unPQkM7YitmQ7hvKoWBRzxuP5X6Tb' then 180
        when orderbook = 'GCiSLBZn7MVj3oiidF5ZAhQ2JsUATcenLJaRc5s5y9ZS' then 180
        when orderbook = 'AFn3o1FcoM5mTdEBiL37GtfMXzFjmebevEKXwchF6CDo' then 180
        when orderbook = '2zEZpgCb9h4yU5kovPZrt5vjwdzy2heyVDfsMoVyWCFv' then 180
        when orderbook = '14tngHvcSm4NySKmeDrM2G1bb9QBcVXCtpD9Vt5nKZ1Z' then 180
        when orderbook = '5nKJLFG56jhvBZYzphSh3ouerZnPjfndK5fPpYpu7Mzx' then 180
        when orderbook = '2E5B3oALkYkUReZBBbo1N5UyoErNJQ9mo2MrZeLLTfTx' then 180
        when orderbook = '4F5NMeLzWJHt9ouBnU3qTtnq66U227SYvginFqhNdkE9' then 180
        when orderbook = 'GgAysMX483v3wCKf6RcTLCekgA1ctnoA6j7mbg8pM1PP' then 180
        when orderbook = '2hNG5Tz5J7CFMsKE5Yo9znYfva1QfK9DasFQ35aeF8g4' then 180
        when orderbook = 'EvzMEqN13U16439gEyY5aCW35Pn979EhHjbBjvMpr4uM' then 180
        when orderbook = '8CFnobPj4UL4rNcmRsQLpFAc8snBVk1fuTk6bqXnEp72' then 180
        when orderbook = '5h7moSgPmv9WVNh6NFCS6eGPuKunMdoZ3Ks5SoG6u9LS' then 180
        when orderbook = 'B3Myfb6TPV4BhiKCFd4rTE248GDrMwoWtRpuHzwNaZZW' then 180
        when orderbook = 'FBGjE2bKk3xtRxewd8zy6QU4kEMGq2EH8doe9mbKZygc' then 180
        when orderbook = '2r7xcKPEF5skRuemQ9AXFLtoZHwyJ8Unm9f1QbuXKA4U' then 180
        when orderbook = 'FFiEzNdho5e9B17csDPSRZhRHhzBSmhRK4yWRikR71Sd' then 180
        when orderbook = 'GmF3mRff7S19YxNiJty1GuWvh5p2Xk8Atkd8REZ47jP8' then 180
        when orderbook = '5YrayX7TQ97R35TjDqqJatSrA531RnvH2mGjHvqWPjnu' then 180
        when orderbook = 'F8GNSxu9nHCxJhcJoVEze4gNEHLmEqzmhgzToqfBk5hk' then 180
        when orderbook = 'GgZNocTDJjc56PX9kcYeuPcgxWTFKDa7P4p5JQmrHdqE' then 180
        when orderbook = 'BeTvizCfnp2y8FnxAvFfxNsyLGsgbc37mVu7ofVHrDnM' then 180
        when orderbook = 'FmHebeib8waNivr5k8NExe2ojUsvBp3KKnDL28jDMGFa' then 180
        when orderbook = 'CTDWHoLNnLGaXpYqvEmJcXxnZFhCc1jXKFVbSNF3M5NW' then 180
        when orderbook = 'F13rXMWu3rjLkSr7Q4LWZXivc2rXAAAoXLwni8hTevUU' then 180
        when orderbook = '7Xm87AFgGMnaceBFTBn5A23i6AnEmrmRcEFCHCcHarfp' then 180
        when orderbook = '5pm9gEkNaF5AcErgxR9VANPccNQC3tqM7zkkqTPwF5Tx' then 180
        when orderbook = '9cEjYpJYUtZUTiSHMebGBf4f1612mgbnsgReUeywCa7F' then 180
        when orderbook = 'AyzA1FiVCWUxMam5kGoLuxu2x3qSdHmTU47wFN5iEKD9' then 180
        when orderbook = '8dCv2Q2HCnvAThwvs8BXK8DDuTmLzEV4HvHY5Lw42pAS' then 180
        when orderbook = 'BVzyP9jk1D2BFpPXcP1rgDUk8KjR13EmjhRpG43v48Z6' then 180
        when orderbook = '3NBmGfuTevw6seHCmhzYNGE21c1kn4BKUH276dKdbfmc' then 180
        when orderbook = '8gmvWVj9TskuMQhRbx3aCxq1Q4iefp5mAuN2R9AEiEjr' then 180
        when orderbook = 'DMmME1tTt4ACzJ81AiiZFUE8mTKPyp41yQgEDpzu1WCx' then 180
        when orderbook = '4s4W5T5gLdzEaAU7oGtwRn2hDi2BxfHc141i2gKBJVKt' then 180
        when orderbook = 'CXMPcBjYt55vb9zWSj7pvVwaSSH6PfQkHgdnAKyHSR2i' then 180
        when orderbook = '9swCgtFatd64UwXNwaCf76hdMtXRWx6RDNpDiEgoYjNo' then 180
        when orderbook = '8QizjcBwoJHtBmkTqhkU4KhBoiVT1444hRorp99pU9yX' then 180
        when orderbook = 'BAo1pM3MedfXbTijsXv66Xwy5QHmTfQSaME7uiZnzo81' then 180
        when orderbook = '4JLyVxZdMPeubTgKhwt9MQqfox2rsQt3d2yrxFKWFMCE' then 180
        when orderbook = '9MZe9ctW23qJRDrGTHRqsPJosPc5TGtf9f5E9x224Uuh' then 180
        when orderbook = 'CLnxNnxEgxuNTfDqzh6Fqj6A87fNZa6WwvwTB7k5N1eR' then 180
        when orderbook = 'DuokZiiQ2wHxiiq9FXZa12NVSnK7SXmWFvSBVbCTSvpr' then 180
        when orderbook = '8wZVmmLLZAijuT6mc4WhN5d3ratWBfhDBmgt57qxrCEF' then 180
        when orderbook = 'HW7vRnebjArAmNXfqvbuGC54oNGPQsCYYcvCPL1EJhGj' then 180
        when orderbook = '2Zmi3wXns5d3FxDCJYL9oXWe31nEmx2JrzLiwCLakeJV' then 180
        when orderbook = '6Z5zgpYxrPMrgHo2EoFS8KJb4mT8NYjRwAfzSBMcguU4' then 180
        when orderbook = '2MCherDm9RsxDtDHPaBmoEPxvvvETwCuupbWaqESVxjF' then 180
        when orderbook = 'wSJoTu6Q5FzhkGrnkKf1eB1Bm2fgitiW3ot1sSXqTzu' then 180
        when orderbook = '77yFHWsBfz3xS889MvKHYjNDvYF5ZTsjSBRhBYhiqJ5T' then 180
        when orderbook = '9yXZtFz3bWfzxxNNmtXG41Ya3YfHTBHotCGnKtSnW6kz' then 180
        when orderbook = 'BfXazb5VsQfRbXuCvuWvVXoBdNHVjiWyt3Czc1U2Cm8V' then 180
        when orderbook = '26JGMcL8ephK51bcBPMQfKeVKSAksGR2bKyrwAxgDiRn' then 180
        when orderbook = '5Gt3XSJTmNp2Sem8BLHz9dh7vdVWN5smsKexRH46PX4V' then 180
        when orderbook = '2Cy5PohJpGJsXFN58GkyYqASakkgUyXrXZYuEFgpximk' then 180
        when orderbook = '3ePp2EMLdMP5v2LPKw79KVHpcsjEXwX1zXfWQvaexMvX' then 180
        when orderbook = 'J3ofbjZ6emFUDUZ96wkvkuN2iPVgN6eNenSdd8LprVgK' then 180
        when orderbook = '9ikJKFYENUaXekruZS1j65PJt8p5w8xSamfXPtTdbXE8' then 180
        when orderbook = 'X1MSpjdi3DBLEVFhzigydQXZhAmw3oAEYLv2mXMY3KN' then 180
        when orderbook = '3hPYjq7SoesjgvtEDYAKxYccS4U4o2yd6VAGHKaZWjDX' then 180
        when orderbook = 'BENrqx18n8tP2xYPBQ7vaDz88Y7wSmzB1xkr8h4XrhvM' then 180
        when orderbook = 'BrTPzW6faeSg9wNHNXA8DvQf5f99Swsc3oNG2QS2c9zH' then 180
        when orderbook = 'GznjibK8btWFAwZFtNSAzWUmNwHTX5XkDgwF6vGKWA31' then 180
        when orderbook = '74VnnrcZF4uSScQPN6W8Kcs2tmRJgepSYzKfd6hQQACx' then 180
        when orderbook = 'DFYu4tfjLeYMbbR1VW5hX52gPoauwfkNxzHbqE4aL2Hz' then 180
        when orderbook = '27Ds6LXyvWKQ2aKMmnVMZ8thMwshnuk9EZs3Lq7GX7vv' then 180
        when orderbook = 'DRutLdi5vPM7LmqyBKhKVA6x75eGQLvakPmbXtuiC18P' then 180
        when orderbook = 'F7rrmiBdwgkWFrzUew6Y9eDgGGJaEBGvC5X14SFnLn3i' then 180
        when orderbook = '6MZjgmUPtPDbvDFjaAEPTh3DXoTHYsyLTwygu62d4Ppu' then 180
        when orderbook = '3rgY8DEpRasLbU6QuYPAnwy86ph7uhXbDC6kxtBddJRe' then 180
        when orderbook = '5vqDcLfjm5o1Q3LVn3aLPvLxdwaJhmfZRZEE5iMkgCNo' then 180
        when orderbook = '7YZj9e8GxUmLKfReLRseRki4ByMRPFuhzQGQXa2bsbox' then 180
        when orderbook = 'piz44JVk5BqxxPpsdxum6ZJgn3vkctZscgCFd4VW7YH' then 180
        when orderbook = 'DbhMC3yyJT8Lbr6auhqjC9whpRKaxxzwKNYiM9TrKmoH' then 180
        when orderbook = '3FqYQVzoxYZiS352pw2YVUcqqXdDsgaNXGRsR5mWQQuk' then 180
        when orderbook = 'FsbW5qLnJrQezXfLEJYMjUxWhBAbhV7yFnLZRL2mxRB2' then 180
        when orderbook = '6F1icezrzjtrKFLsNSLqFPUp63xCDwxrBCdewrR2FAkZ' then 180
        when orderbook = '52DJeBNZmjksswtBEgbvqG224yNihtM2kkdLti2rf9jj' then 180
        when orderbook = '7yJpf4UzKkeWveDvW6KmHunbuXRAEZdcwgcv4zMyNU5y' then 180
        when orderbook = 'B9aYKEJYs2rqpNk6fXY9evf7iiMFh3Xri4eYvrnoUWEp' then 180
        when orderbook = '5SyJ7Qho6kbAJn1S24izEvNJDj27eo3acpQMtw6mTGhV' then 180
        when orderbook = '6GdB1D6iCb1VBTiSaKbxspvdfEir1aPaCNPefTSBL8yb' then 180
        when orderbook = 'HJAYTbNFssXSBCBVZa4TEvz6Bnj2G6GbXdDapXo2gf9' then 180
        when orderbook = '637LUNmd3FtAR9yb1Hi5CTvafLZChxDHUDyT5vyajH7P' then 180
        when orderbook = 'HM2DjhLp4ZR6qCDwC3uTvLZQJRK3LJcpptBQypLgPpw9' then 180
        when orderbook = 'AQ2co6WsYiFnThinp2cU2JoQZpkqgNV9A26yNZa7he8H' then 180
        when orderbook = '3s8iEwRehaKCwXbh13DP78Qw5JzaEpErqCfVuFzL5YCf' then 180
        when orderbook = 'Fwucg5wf1GGj5LiTBD7tvbGGpH22T39tFB4RL45rdr2w' then 180
        when orderbook = 'ESXztB1sfzuQFM7GCXU5FWD4EBeP99xYXg5qJhZBSHCK' then 180
        when orderbook = '7H2qZQiuNurUPhsRtreUuHR6V3xnxCKAmaoXuAAjhzNN' then 180
        when orderbook = 'ECQMNk8tC7hzzx63amB3Aa9MuUuf8TYHQF4PiDQe9DBF' then 180
        when orderbook = 'vQ3K79xoCmucDpkb4VB3bgzQXXE9y19MZuQafGNiH5U' then 180
        when orderbook = 'CW6oJwJCqvj9zwBtbEUwXS3nmhjowA1tgxzU6oHoVaqq' then 180
        when orderbook = 'BvgZL9ZrB8fVmAwgzQ5gsPFnKCzGjG9MjCADbdFv2Uey' then 180
        when orderbook = '8ZdHVG2qsWndCYWiKLrQsr1UnzXUxd8ReEhVDtbVmotU' then 180
        when orderbook = 'GZbwfxnyjHe1cHQDJwE14AMEBRPHDnE9tc78c4f5L9w1' then 180
        when orderbook = 'DUue7UMu3Q8pikLY8x8tgfjvN755ekNqooCDBSfembzU' then 180
        when orderbook = 'AYgQYyZ7ZiH1qVYG43dyjp2EAi64yDEiFWeMwECMSodr' then 180
        when orderbook = 'C7KH7qQMQ4QFPmAoFs8mktnSv6K4c5WMyvA7NvKXWMZv' then 180
        when orderbook = 'HsuEMCBsM7rgpLGuMWRWaLoQunTz23WDJx2zdAQjRLKc' then 180
        when orderbook = 'HPoy6XSE8YWgf3Auwhsv7yQsoPqzDtVuZSvTYKiBXpRr' then 180
        when orderbook = '3i12wSQTRj7trvap2eBLQKK5rqi6sUSJHY49K2H4e7Na' then 180
        when orderbook = 't5g52EvNo6YxJKgzX3s7iJycR7qiHxeJGzNb6QSbK3s' then 180
        when orderbook = '9eycVDHqummRe67o8fjwzUb68jp6PFPcB9WnPMQhD67U' then 180
        when orderbook = '6RNkpufPBopY1Q43NYQN3BbrQetwABvoQbQ9A4fsSchB' then 180
        when orderbook = 'RRS7pN2oTXDwhUURUVc9C829ovWfRrMRWYwNg9LBfE8' then 180
        when orderbook = '8x9DJgMiHumyGr2xXfgNv3wGWNv8P7BjAqz3J2biLp7x' then 180
        when orderbook = 'D8Y5rVyxsS63FHcCp32fMh3FgEgviMwM5fQUrhzuzz2M' then 180
        when orderbook = '12knhhvSRgWMWjc7knNXSvdxrD24FtcasAkJEfUDLnmn' then 180
        when orderbook = '9UTQ5h8bKQCN9vVc6bNMcBJkJ9QAD8hhgRA4jqyZUjKC' then 180
        when orderbook = 'DMhBB2W9EbHQTwrboEeb9aZ9CDNncbYr5JUA2T579i1D' then 180
        when orderbook = 'xE5jC5wwihdY43NwwyhiQgAaJ1AXxPEkDQ7NZMhQ3rw' then 180
        when orderbook = 'FFtBfTXnmsSjrj9yA65VSwtrcLkYVZGkYNTYsigKpQib' then 180
        when orderbook = '6cbkFpoWEFDqVp6FT7bRkyt9Rdhw6u4xyL8QgwJ4SgDZ' then 180
        when orderbook = '7YnHHA9Fzx3m7eUnUESLit85BGcYxi4UEF28sEsas4yp' then 180
        when orderbook = '9TXst811r3mDTCVMstLEXh61MgiFrUtBgE368Xer9fa3' then 180
        when orderbook = '4Mx5YV1tQPFibVn365cSaj2uLGKQHPRUtA6uQgbzukhn' then 180
        when orderbook = 'cZN9zsimprnu3DWip7Qa2tEVuwyCd1bd2utqnPt7YFC' then 180
        when orderbook = '2vFF1NkzJAdivyPdHWoJLuH1YFzpEkVxaadGHnhh6fdx' then 180
        when orderbook = 'FGz6DkTpyxdnfunpH1W6tFTdotFwrUeWLoi8pcrCe1c' then 180
        when orderbook = 'GLq65dWVdFusqRKFSJRdytd1bV69uBw4wRXyj9sMw9FJ' then 180
        when orderbook = '1vnUQPDbY9Dbn4k2mSdPcjD9pp8d3CPfUz9rn7rDG7t' then 180
        when orderbook = '4cJD2YMvgc6rCB64L4dMGwaEs9Wz5jet7mTSC79G32ng' then 180
        when orderbook = 'J97YukrzEvSKdcrqMRmhQnfqs5zfh7s1orVC9YUCLs2c' then 180
        when orderbook = '2SBafwVAQAFf1pFPwpG3DRSBEDWbTUG3qCdzXiJ7uDHP' then 180
        when orderbook = 'DmsPX4ubjnnvHSqbe683ZPafGadLYxQkfDoeXA1swq8m' then 180
        when orderbook = 'BwYhdm89QNNnfUcuzGWha7ghK8DkWWiXkJaGZLajviZ4' then 180
        when orderbook = 'FFz8sNRWmtU6g2YFkE7o93yebfYFyA4AWxXhNEyP7Gor' then 180
        when orderbook = 'GXB5TmTPPNdHWMEMGJndVgSmsynaH7uzZbQWmh35TTun' then 180
        when orderbook = 'GdNTchHXGucsoFrPSVUQPx6q56QvDQho3CrCJt4Ptfi7' then 180
        when orderbook = '4H6tR5zvCxtZ3uG5XiEbLcU1HFxW54hWxq5BBEhsyDwX' then 180
        when orderbook = 'CK3TwfGJptmvqX8Bd4sQroL14dgra88bFpB5h7ZxnCrz' then 180
        when orderbook = '7NFYCvbwxoKJ2PMTTuojGGoS2AxNvua5A4hB6wG25Qty' then 180
        when orderbook = '37k31eUjHyuVm5sr65AYNnhC6xLHQcQZ32YR35WPhD67' then 180
        when orderbook = 'A5wgHA1r9gtnCut8Snkxc5gRy8hKZKwBCZ5dxRrQhtFF' then 180
        when orderbook = 'DipwJSf5XVreDk2TWW8fq2uAaW8sz4m6NCkK14y36DfH' then 180
        when orderbook = '9TFEr6ikTvizV4Pih74RMqoo8jZyESmgyG5SwDedeeXz' then 180
        when orderbook = '7YyG4pmKBtY6H6VFtCYpDSqi87b1bWhCVmvycCA72qht' then 180
        when orderbook = '3TsT9uzbkjf9ma7T4VA767u19UT4VgPYAqidpRdHbU6u' then 180
        when orderbook = 'CXPaozx4ZtbaP8r62WoqkDKiHyF5S1hebbNsMyMTivbw' then 180
        when orderbook = '5V7LcdpxXCVSbHuSi2B1sPKWnQi2gSq9KKCFfn7xa2GN' then 180
        when orderbook = '3tuTPS3XRxvkhzkiEdP3EAqgNyemxUJP5v8gDn1B7DWF' then 180
        when orderbook = '2SQugE6XZwaeE1tu614nvChDbcnyCv9hYrFWBG5KgfhN' then 180
        when orderbook = '9DEsruE3FzBorj1BZYX358VCWU3AEs8D7vcjqcvym1Zu' then 180
        when orderbook = '8RGpeZRz6Rj6SKVJpwgcVwCd6rCoJj7Jv74oUa3V35uy' then 180
        when orderbook = 'FKMbn9wFEinQCwLysh3DnnZEaRXdcArtKLpLjL7K8ARz' then 180
        when orderbook = 'oKDsUv1MsMk6sp4iMjNFGxFaeaLzjRKrcdSwAZRF7ci' then 180
        when orderbook = '73LnqVswvoYderGHGEJgNU7FHAh68z3M6u7QahAesGMe' then 180
        when orderbook = 'GosNdfo9SpzBi8YbRbaVffjKudZPkYdQReNvRoipzyNN' then 180
        when orderbook = 'D9TUZg4kwjga3H9tF3SAvsCKMTXYyrk1g71Q3pXBBrFz' then 180
        when orderbook = '6Ppd7qHuJEX2hATNf4fd3vC3HszaaQhuBd3CTn4eXA8B' then 180
        when orderbook = 'F79KcbRecijxZ6sijJyKq2CiSfxpB5954omirqvgN7Ep' then 180
        when orderbook = '32Bz5m9XGDZ1vLX5CcpzFT3yYVkLXKmufTPQQxTcy1rT' then 180
        when orderbook = 'DVu7PKtQg1Ai1RoNNpZGciSVxAkYCzYGvfagB4gELU4' then 180
        when orderbook = '6G4bL3AjQJTAhXbR1YBGC3AxAdgUMf6Qnv11C25ZvvJp' then 180
        when orderbook = 'DvbqPovrUc95SK9evEW984wkqthUFFNKmt13swbL9UZk' then 180
        when orderbook = 'GhzAKbiZP1qpUj1qnE8tSTUoYYVcRj8j9cZ8XZAcBa8j' then 180
        when orderbook = '62M3qZzqP6YkHwutM8Xn36mrrusBey9f5vjCybZH2USh' then 180
        when orderbook = 'Es62wFv1Fg9mmTkjNq4FpbPH5SJxqBwe1eB6kaxQTBEk' then 180
        when orderbook = 'DLZVjjVKSR7numo3rRLoHrPiPnY4kDkwzTymjb8Ctj9T' then 180
        when orderbook = 'H9w35mGLtwKo1nUa7M44cD1UNLpxAgMCFhipf3sgyz3a' then 180
        when orderbook = '7XBQzM1Uk8uGXezJx3G9P5Yi9RJyU8iYqcTd7hQc6XHF' then 180
        when orderbook = '4y7P9cjEoL6TRLQLsDb1z9qRfnkoejcmVfuAEn2gsBJz' then 180
        when orderbook = 'CyMGmSF36WV1M1SiXBtfgDMv8Cjqrv28n1gFv3CbuJ2D' then 180
        when orderbook = '32hp7NgpbnQ59c2KzC6MzaQrTx7KqjT2JygjW9R13K4u' then 180
        when orderbook = '39gF7D4S3c8ccWGWBhkp5SriDLsevMqvcB6eajsjoHAv' then 180
        when orderbook = '2Av2hiL4KZ9bsNtBX6MLLPHiN6buGCd8AxDqH8XkhWPf' then 180
        when orderbook = '3sX6bcvKpiTeikjjPqUqBBvpEECQtYcy65vG6uYvV3PR' then 180
        when orderbook = 'JD3Lm1WjzUNYtZ7aBnnR2CFnZ1miHi3jEtWg9jCLZ9wH' then 180
        when orderbook = '3H6LSS3XJG8szTzgpNmFjXHhp1ycV4cJsHPwPvHuyZF2' then 180
        when orderbook = 'CUoY6AGuPcEs5RsrAF58wxugVcZKZW4zVujiQqzPaSe' then 180
        when orderbook = 'G9ff7WTpSE3T4oMB2REz9byKCeascTtBuCV3LjbUj52y' then 180
        when orderbook = 'E4aoQpR6Rh1Z2zuLn7NvH91QvUjYuDeUxgnxgTZkMfMi' then 180
        when orderbook = 'EfoFNRjZVLzqfdoMzuMsJ7hsRyEMX3JcVCPdBKaqMCc3' then 180
        when orderbook = 'FMkgsFB79ZnUNikjttHhfyVBfaLMM4eGRajCBAXVNKE' then 180
        when orderbook = 'Ba5FnKkACkvi9b3ULAqVDCD5qsBPXuyx2CQeiscqDfAq' then 180
        when orderbook = '7uzbiN5YoMPcX1m9f5jxdbHqTQ2cvtM1rFgTKCQG2M4M' then 180
        when orderbook = '6YnxQKM9etuf4kRE4EyzuPWVTgUkXSc8Q3DtcFnV6xG8' then 180
        when orderbook = 'Hessa61SpqWr9GxQ4qMNh6GvY7bn7LFKHdKBDXcaXRJq' then 180
        when orderbook = '6HodKRQND3VAvdVz6rjKqtwha4oaq3Qq3brJiDEUpfDF' then 180
        when orderbook = '4n36L1ViLKTDso3p323CQ7DacmEvY3Cisc7LSgJVQ32M' then 180
        when orderbook = 'GnJUAdKKDXBQ6b9eHpgMDvJV7h5Eh18eruuS1XxnU2UG' then 180
        when orderbook = 'BUS4ikKfs4LBKhipH7wmMimqCHPtyX6MtcUM1NuHDtxz' then 180
        when orderbook = '6KxpxjpP66gp6MsEytYBoqEVV1n9xphq64q3JtTihK7y' then 180
        when orderbook = 'DCSH84b2kLsbpq1LuWyqds4o1nh8Z5GU7g7dAb8nvT3r' then 180
        when orderbook = 'Evt5K5ZiCVeC6vR6yQdgqGXA24XbbWtQYiqdGWzgpKLZ' then 180
        when orderbook = '5o92NFAtE9ThSrjsLH6mm3D4G8c2SguGaMmGT51zVSg1' then 180
        when orderbook = '9ySuwXyWvFvWifVfbAYDGGMy7CdndEfX6efGt1yQb6dc' then 180
        when orderbook = 'BrXSwyhUJFkZr6hCE3GMt8mtvmKeuSStrkguhTvuoTtB' then 180
        when orderbook = 'ERWMSg8pYtZJscwkoeSt7JXiuAvWEsQ23U3NGh4PWEqw' then 180
        when orderbook = 'DwXVdkAADLGdorXWmcdTSheNUzMWbDP7wVhXsjD6Lxft' then 180
        when orderbook = 'DyJkskdV9QMevdhhsZncfei8rmUrSK3Z2RYr59Rfrs5D' then 180
        when orderbook = '6UQ7LQPX3ji8y6UrAfwPnwvDDqu5jSEfz4shFMLTanFu' then 180
        when orderbook = '72AAevoeMnpH17YHBKdN9TaBY8T9cbtrthiuNNtsJq56' then 180
        when orderbook = 'AdD9mFkNMn2fvhv4fRsVGeRBjb8yP9nz9krgPjyU7eVY' then 180
        when orderbook = 'A3cH6DSkfbJeGuHrUc4Dqef986xZeZndZ4wSW19ZcJwk' then 180
        when orderbook = '5LidgUiRuzeRkz2sLZ5MMdoxY2tEP7iRvSDNdSquiehb' then 180
        when orderbook = '2wZ5ZbSrgnY3XxQP2uT9UcAxwg9BP4GND14XMGxdhe1U' then 180
        when orderbook = '9yvBVmCDuvgAgkikd2cRoLjk3SiLEhh37KwX3UFi6VtD' then 180
        when orderbook = '4mfucNSKpSCZjfqwbh9eD4Lnda6A7QHRMufKDU7Tufm1' then 180
        when orderbook = '76DE8GPAvYveFfNxd2Ed3jLebAjYnJTbm1hrmPCCg1h9' then 180
        when orderbook = '97fYV3BQafu5Yh7fW7JFKRYKikeA7iaaV2DFpBcUR5q6' then 180
        when orderbook = 'EGA1EWrtmzBLfnRXaWXj9CXLyCjyR8quSgmRzmhV3cmD' then 180
        when orderbook = 'FpnCG7iQBYDnKx7MV7kNgfJ9gV12J4YvCcZTLT5wSwL3' then 180
        when orderbook = 'BGWt9t33tgLsm4yeQG38PFgXq4UKcPWmFQER3i78Cc1g' then 180
        when orderbook = '9xdDe5zKx1GzDMp6QTzq5FS6qHiVa9zXAsAPntq5FHSy' then 180
        when orderbook = '9W6u2xRH27DNU5D67fsy1kGoc86r1ywNThjGgsAPJE52' then 180
        when orderbook = 'C3KXCxNWvTPp3PiYjCMkxyyhaj4W7DYKN2UfH1qH8GVv' then 180
        when orderbook = 'CEfNg5oHAUcRfzmb9HVpNtSbrcZZuJcKtqEprySv7dT7' then 180
        when orderbook = '7wyEhJLksJfEcnQ6RniYM4WVdd4g5GzdZF9nFrQsQdpC' then 180
        when orderbook = '9B1qhF6H91bQy56Q8PyQrneFpvr59pQ44xKXAFE5ifJA' then 180
        when orderbook = 'HLGfjQPHnf7iXv5FhXFMSpDoB5FKqfhPhUd1kzPwgHbA' then 180
        when orderbook = 'DAEmasEQTiJv9AberakvwsBqZr8SpXJGViJcS6J7R2LT' then 180
        when orderbook = 'BYaBedcQhNyYm8cg26bNCNFhQVz1YF3phZYTzupLQJWH' then 180
        when orderbook = 'DDnNPZ5yRDkPomm29PTg5QsGBm3H1oBZqhNX5MEpzV8w' then 180
        when orderbook = 'Gm9pQkbsuLtXKP2z96bCeCBNT4QMXfgXqUT7ZUuvkZxj' then 180
        when orderbook = 'BDbbyDqGmkFPCGhgXhDZdsCfFMTeqdKQuFZY164cBBy4' then 180
        when orderbook = '9QsH7dhhDAWLdiG7wjXJ82Lc96ZPAvnaz7JoVYJwo9jD' then 180
        when orderbook = '53hL3jAQ6eaPLz1py91FffNCrmeD1mUf3fkB7Sk67EHd' then 180
        when orderbook = '2tKtpre8c9TVSUDJFdeTZqAAb1eK6iqWrVXC4fvqqXAD' then 180
        when orderbook = 'CvFve57kYaJbA2wddfEAGBfryGApT4mw42hPX24zndWy' then 180
        when orderbook = 'AYrHeXpjVCxmw5EnpjM6cYDkR2t8WfBGePNhjTYp6g1h' then 180
        when orderbook = 'GgxiN6ktmUSeHAnpBzrpBgGZd6CVNWQTHjXvKP8jsYaF' then 200
        when orderbook = '42FTJrtzUvLXa9Vkn8vufwQQmkiQcyH96YxMg9kERm5U' then 200
        when orderbook = '891ovdLaxVPSu94yxkqupT6zzJcTTctdw3btmmBw4ErT' then 200
        when orderbook = 'GjurAS1MLoFVrsDpew7w1L2aYkTNYovVaDEvfpe7Zodu' then 200
        when orderbook = '3VeLVuwTjpDZ931B8NkfqNa8SMZDXLECRq27oCmFNead' then 200
        when orderbook = 'DLerqrYPAJLetqQ1N65HbKR4eQXR7oBS6oPCGPJKkwxX' then 200
        when orderbook = '4dKe6Hp84f1sVTty7YcBKU3MjKgcR7ChD6QRsS4zUJVM' then 200
        when orderbook = 'FA3u6Lp1WXgLDkeAvheJBpUFpL8HhLLdzHVzmW3dBbgQ' then 200
        when orderbook = 'AaPmJLuH8ChdvNw2tJTe3PWGimt6NFfP5KCuixdqEXqd' then 200
        when orderbook = '22DkHqEN48KpfLu6fvDBYvidB1EtijXzWCzSbLiAYPvS' then 200
        when orderbook = '8kesJY8jweBxkjbqVXKzy9PqXnKtvW3XSVjiwkG6rbHj' then 200
        when orderbook = 'FJ1bTc5SJuahnpAD4F35mrgxpYPZME5cNKt7r5dWzqDp' then 200
        when orderbook = 'CHz5k2FbwAURWWoYv7bmkMBWSvfQg74BsYHFahF7PviR' then 200
        when orderbook = 'EEc88Zud5D9pKSPuMdV2inWV6bdeNWUJ2ZTbzaELsJkU' then 200
        when orderbook = '7XsLCBbmqfGKTX7UEfZiTUE9FVPXv3h3qHy66nT8KTGH' then 200
        when orderbook = 'G6vguQNqRfPAV4XhXkbVWwLjRtQc8BiUt3gbJ31Z7Q2R' then 200
        when orderbook = 'Gn4JnXMwSDjjmyh9HPh5tgJ8ErogudSd6V75LBZmgwUP' then 200
        when orderbook = 'AptPfZ9hNrKrF6BiBqZdbAKUCpyw7zSXa2ye8eaTf5F7' then 200
        when orderbook = '6v8SGwRNLkVsE4j7rG2TeYEKXk9gZDB6PvZ3LkGkDESj' then 200
        when orderbook = '2ZFmLTe1j9S9uDRvDpkU8XRmTpExfgznwnrCPfJWo1p2' then 200
        when orderbook = 'BvgdUKHiuenYQs4NCbM4RWFRHESjmc1wvSYvK1fifRAa' then 200
        when orderbook = '7cvpRJ8XrjV7m9wwsGEDdrMKY32BeiK8LccMoxvEFwhS' then 200
        when orderbook = 'CPL7gQ8JFV22CCdMawCAZocZn9NNe2xq7jeFs2Wy1iVG' then 200
        when orderbook = '3sTKwxkAhGNrdEoor2fXzfqhsnLpWKeqzcGrNzdoVaZT' then 200
        when orderbook = '3Qe7LBduUkHkk96HbMVQ4QSxMVdddVr6gJEoF9m4qB4v' then 200
        when orderbook = '2Gye3WVekwkgZb24FYsTRWDLyAaK6vdPVUVbCsdsFXib' then 200
        when orderbook = 'HFQ9ZJVDfm7f4W2rba8ywjuPStcsBHVnCwwnUCD4xsPL' then 200
        when orderbook = '2YrKeHF8kXce4mA49SaPXXqorq7KNQiVZsUAc9N89s9n' then 200
        when orderbook = '4DqsHhosyoZr5JNvC1fNRzADAhzNZ2yAd9mWgR6NXNZ2' then 200
        when orderbook = '2NX643mZUzpihRepymQ5fh82qSefwDFs6SLTUu15bGdA' then 200
        when orderbook = '41ebo74cm4Ch4r7tAPxit1FD91EDotH9kp63Go68QvPD' then 200
        when orderbook = 'DhxLXegmk3vBQur663PGhfFJFSy2ue3BoBPmpvfvynYm' then 200
        when orderbook = 'Gr1Nehyw7GEsbGcuopU8df61GnbxcVvF4PMx8HtVgTFB' then 200
        when orderbook = 'J5TPDoqb3Brn1QYkeY8nMKMj9rpcAEyEgjQsR1VxehiJ' then 200
        when orderbook = '3V3qgmVHckS8T2x3TU7rWcUGLwaNtmAkqwa5nwxMoUcw' then 200
        when orderbook = '2NHwDH4yTbWexuUugdkJ2yiHzYA5krh4n7zMBPBqQsa4' then 200
        when orderbook = '3AcVWSWhZAUeDdtdkrtix3BEBEGVbh9R5QqNdKTxqGZw' then 200
        when orderbook = 'E1Jv7gtzfYroecMgsEMPQ2Ux1xxx1NK9LhXV1FLyx74b' then 200
        when orderbook = '2D3cgTCUuZNpxUidB15YuPsh536FA1qQVENLTHPjm1TD' then 200
        when orderbook = '4cDZAVcHwc2B3tz8FQc4WBGGRuq5PNG1hS8CGuCTjaC1' then 200
        when orderbook = '6b97SnKasqE8RJHGEp1prG72d9F4p2Zo1wWR95YX3aSn' then 200
        when orderbook = '5tpNwLhfMQ6CDyuzMUzkPPN2ZK7K8SZeW9GgEvGDrgA7' then 200
        when orderbook = 'EiCb5hPLKs8piTjoLzzpug6yrD9QRZPwUUrhZv7kewrd' then 200
        when orderbook = 'HGvQWpznrn2BXrZB7dNjAw6zcAzWAnEkx6wy4gAZfqtE' then 200
        when orderbook = '7akdgq28vtaKrQpDfRPBpMTKoLSYqNGFZv97eFCARNxi' then 200
        when orderbook = '4FLvtHs9dvBawU4q1cvt3JeXHEQmvS6u9KLHzampm1qj' then 200
        when orderbook = '38J5P5LeEBEEGURqfXoH5vaJhR7qBSkH3wtWSEsRauLn' then 220
        when orderbook = '2ruxyAWodkVS4PwQBG4V7VD8MbWhrLuqC2zSeq2bZ1tN' then 220
        when orderbook = 'vsrcyReLwYftBpfLhwv3NrzwADg83ky8ZvopWvMG26Q' then 220
        when orderbook = '9WBLcHxjeZGL23xJuEFbA2ei6bA5MKY4zaa1kCv9i5YE' then 220
        when orderbook = 'HTobSeaciZHwkPHUMMJQrBLCyUfToQJRx8e7N13Giamc' then 220
        when orderbook = 'G1ib5Pghe6GTWvhjXWbnPbg6rFx9PnkxqmSNYwFFFduK' then 220
        when orderbook = '54NNZpbpMaeEj7T8MPFYeFkX8Wppz4gB1AX1poJ84siw' then 220
        when orderbook = 'FavTXAydmuSvZ4coAURiDLfy9vkf7aTvmX8jKNqKcM7c' then 220
        when orderbook = 'BVQVWc8xu4fug8qXkv6oGuyaKyJVbY7wY7bM5XpEtisq' then 220
        when orderbook = 'BqBdwgXjeutjvYyfSepJHaqCR2U9y6xfce9CTLB1HhAt' then 220
        when orderbook = '3G67sRmPpqYa7JaknaUJjz7UV9a1iA7vFUi27qnmKLe3' then 220
        when orderbook = 'BMsncmqTafEwFFTRb6RjrdPRLsuz9BBfosU5TSXod7Vc' then 220
        when orderbook = 'EiLtD9KyPK4ZDv4SEcimoCBrZtLjdQUN1SrJ6oj3p99i' then 220
        when orderbook = '5CgSZz8Zk55art11AeZzwxTA5J9gafRew45GGZtGSbvo' then 220
        when orderbook = '981F8LDt2BJ6RjnYxXPsh7enZaDCxPbS4sxTjgXVjLHW' then 220
        when orderbook = '3WKsXLUij6P5PDLGYabXNyRS11rk2VyQeVcNE4dXcRf3' then 220
        when orderbook = 'EqbvyNTS7yK78PFhcXXg8jPEGVuCLmJtpoSMrPs1S1b1' then 220
        when orderbook = '2u2QH3GiaiNrn8tWxSa3bhjW2oKcpB54C11zwpTe2pXo' then 220
        when orderbook = '4CRfCuKtiFCNpuPgzPW4y9Sk7Wndkqwdr5smGbgCazWd' then 220
        when orderbook = '2Mkk1EfgJHp6Zv2crDJaegut6qtGF6YRbc73GJe45Me1' then 220
        when orderbook = '21HyCXgA6bauXnF6KS5GRpudGqjhG3dT9LzYd4WScDM5' then 220
        when orderbook = 'pGoPxoWEuzTx7VCvdLh958TXuE1s3eGCJE2QfL4k4v1' then 220
        when orderbook = '2EQ5asbmnuageU6n5nJ4qoYgmBkdGs8SVQypBjYuU6rE' then 220
        when orderbook = 'GZSzKWQYk1AUdraRjLHU5rEMr2UP6poxmsh4fZRHb7Ck' then 220
        when orderbook = '9czz3JqWiXp3CrtUzCZzBg5g1FuWmt93mR4PfUbCKcxd' then 220
        when orderbook = '7NhCdk818FWZpofL74xt4YmYDEM1XgF4tWKoAmLkiAWp' then 220
        when orderbook = 'Tvh5FvqyGbP9YKFZXbAbG9BqPLfNpGAGqqwALTQ2nfU' then 220
        when orderbook = 'AqoUHZbtgaHVhuQuqmuPrzUBUZfdzhGoskcWRbjYT54' then 220
        when orderbook = '7kURCzfshNA941ytzD62WYACJjt5xcC4f8jXjzdYLHBT' then 220
        when orderbook = '9oG4WASVGNZQtn1URXKfeqRRDLjUKNMFrRgX561rL9pu' then 220
        when orderbook = 'E4dpdcSZ3Tith1tnVM3Dn7FKBC5AQ3fCSEqyrWW6mQsV' then 220
        when orderbook = '2DjnQGjXwGrrqz4V5vdskCo3uFf2Lw9GQWPh8xXSNiBr' then 220
        when orderbook = '7wRisucVGyTsXSeaY7QLQJmTnN8mhkfVURSPYhyuuHsN' then 220
        when orderbook = '2uciRT3xH9nWZn82x1fP6ggmAT19FjrZ26Cjh56SJeLe' then 220
        when orderbook = 'BnDifWP7t1Noog2XeUbXJx7tqinvSXkdYCXhodCf9T5x' then 220
        when orderbook = '9qtvbWiJ5f5YJXYxaGbTZZGL5uK8TShaQtvEU52XD32u' then 220
        when orderbook = 'AmorqeH5MTVBX5uDTqGY7vdundoXkdcipLnEUYXEtWe' then 220
        when orderbook = 'He8pssZ2hB3pX8ccomxX6eEFXZg76kt52Gr88tkA1AkV' then 220
        when orderbook = 'JWWz38jUYuhNAr7gUyNFv8j3dPGc2dsS5gJqsskwrxX' then 220
        when orderbook = '3fXAopLxFnYFEFaz21PsQXYK6mnqp9TMYb41gKo7f1ZB' then 220
        when orderbook = 'BjH4kSwKJRivpnLiBJBde57qsbAJCKT52gAqjoC8qHre' then 220
        when orderbook = '4LVZfbdA2hMhmks5hgrwsoydzaGgAUkEwQnwppCtPBhU' then 220
        when orderbook = 'Bq7VcRYdHtbGg7uXn79ui94foGJtARgBWr9cqMbD58tF' then 220
        when orderbook = '9nEyXQk5dGYehoV4HgKBB1U5dAJqYEPHjXiSnpiLAafW' then 220
        when orderbook = 'EdLsguh6QQ1uyXuApM6Y9JG98KCZnDBvEPHT4qpFLpuX' then 220
        when orderbook = 'E8xiZ2C6e9opFwiLGs4jcVpCkPqo1YC13CdDKPLLWJDz' then 220
        when orderbook = '13cYY72ucPuns1ed5NhNrMyLAL1vZ9XKVCQ2m4EP2jZd' then 220
        when orderbook = 'CXfVEwMeFBE5CRmjPNk8YDpbttAN6xZGosGbdHVdpzhz' then 220
        when orderbook = '5tVTqXnPfTCRzEQiJxjYWyNUXJnGe6ZXj8A8qVUPDRLn' then 220
        when orderbook = '2qGfdRuJ9Y5PCnriwBG35e9hTDpqrN2mMygJ6SiYMZtN' then 220
        when orderbook = 'Am4k8JdFfYbDZqisSrW1Uo25AXHyN9dyaQBigrqCrZJb' then 220
        when orderbook = 'CFvpMCxB8eL8Mmxsbsyt1Jwh5TbGqATNNWgs9kv66dN2' then 220
        when orderbook = '51sCrYhqWFJ3t5maqesFmoG2ii3QaFfynN6ENDVRxVH7' then 220
        when orderbook = '7aGJk4ASgg89ygToo1eXmuVF3yW2HaEjemUzdysEPDTg' then 220
        when orderbook = '2xH9tWoVqgkvXhMu1erAG3ye1oXi9Q4SWyu76mLjf9fv' then 220
        when orderbook = 'GgxyzwkoqBc2jQaxJmyswgsu8UBXP9QtHeFX3QZjB5vT' then 220
        when orderbook = 'CJumNLxJtcEr8B5tvUTQeKNYC7h7FRwsDSixdDdVQRCk' then 220
        when orderbook = '7fRp4yNsbwcCRdVvtFUrpWJ91LMk1rpDLngjtpEZtizJ' then 220
        when orderbook = '65yUWgvqzUZUeW4wVf83YfCWiwvTwx4awU8suiHU8rVN' then 220
        when orderbook = '8HGVaxkcoFqZHVtuXM4dAzbT8uYtTtdNztHgFP97ZhnW' then 220
        when orderbook = 'CZRBzvVzk3wW6aqc5he5rfqFEc4Zp5EJ7zuQHY7QS5JV' then 220
        when orderbook = '2ef7ghdBQX5cgaBzKFXjf6AhJBVBC29b5yWzp9pKwatq' then 220
        when orderbook = '7abreWiL8jQKiS6wjs48YckKa7Xp9jeTxa8DpUfUQZBq' then 220
        when orderbook = '53D9uvcyXAcMvYKfCeFALW2WaZGQzMqq3cz7gNr5LZ68' then 220
        when orderbook = '3n9JcfWMYNcstLjcYd8iTHz7fMYezAHv3MV3tqq2rfDq' then 220
        when orderbook = 'Fxfm6LYSKLxWUrwGicYqdAWeLFQ5Xfv3PU9SLYKi7FEL' then 220
        when orderbook = 'Fv7x4J61QqLVjFnsbuCKe87swnLiSmagjYV5QkbLdjoa' then 220
        when orderbook = 'FsZ7jZbkntMazYqLpQjbL5GuAf8NagmzsrPd8wTjHUEk' then 220
        when orderbook = '4qQgKNkfyo9H82pLtBqyRZvrnnMtjBkRsqLWL7HDjBfL' then 220
        when orderbook = 'EmVA4ehJ7qAA2WhiJkfwRDxx2efZt5SL5UW36uW4uKYm' then 220
        when orderbook = '69GDW13q3yse8kguLUiJ75u1A5UmGDgaqPAnZxt6XPHs' then 220
        when orderbook = '9FvkMgiKr93g3qo4jGiT2dC2nWSW55dpiDYutxqaiSN5' then 220
        when orderbook = 'Eze3tvw2NdeQPHqhkuQUNsVmRcbrqwDDmpdSbR5sZVmW' then 220
        when orderbook = '4TdKjF8yEQJN7ASKboWK1Pmkw5MUz7rcDL3TddPkTx7y' then 220
        when orderbook = '8HSRxgRHf9eLvzHfQRn4iHddhLVhHemzVnto6TKwCZme' then 220
        when orderbook = '8t6rmu58VHYYzL492b4wQCNBGy2uepPgtwJVYCT9mXxt' then 220
        when orderbook = 'ERFCBa9WFSJxcjPhJdy1pQVt49rN2X1TffHwxH7XmZtq' then 220
        when orderbook = '7y9rpdFbCPNQhCvCVDSjDz7UyjXgm7ApvfEnyWQ9z4Ge' then 220
        when orderbook = 'AyuFtg64HWZRDDMQXEdArr7Jf9orGcNo1L3JcaFP9uqc' then 240
        when orderbook = '59HFw6ftN94UbqQrkfnqeFBe7fJbvbmErsMgZaNeqBCe' then 240
        when orderbook = '5j3Xw1Ub7UWJZfd1NyMi5PDc95FYCrodogtA6f8pY1D3' then 240
        when orderbook = '6u8ub6gtCEc977YafThuqKXWmmbTs1gLids8MNJA5bTY' then 240
        when orderbook = '2PXkTTN1ovfpmP4hXA2mZcSV2vK7w7q7dNK49qEZqi8A' then 260
        when orderbook = 'BfgnP2ZVTJ4YtLvVxrBXL7bBpE73m6GQpcbEW3dZzSXV' then 260
        when orderbook = 'FrQ8Y71sk3n3bX6QzVpP1PawfS6GYfY99yGXXkZvS8zD' then 280
        when orderbook = 'DD9ggAByDwnBuLxVqNYKD1CeLpw8YZ4uuYXUCuL51mXE' then 280
        when orderbook = '6E33kBnFWMVouigBzawsBkyqcvbaqqrLBPzqjPZ5rrCG' then 320
        when orderbook = 'Eoao1bnbxhbopbNP3eXdkJ9BNibRk1eWp1LjxdoUNaKo' then 360
        when orderbook = '2QJT8NLrKj88Fyqi9nz8f2889x8uifTXQ3yChP1zaByP' then 360
        when orderbook = 'GFT8quNKDFT69TARyjecydATqpXgCbhNvYntAh9MCvMh' then 360 else 0 end as apy
    , case when orderbook = '2yGRv7B3TY9TM75LGUYy6p2UpMkUhMDh5HRBERKjnPj7' then 7
        when orderbook = 'b3eTZandg6wJGoV9auL9J8KCYTsCW7oeLixzLjKmvo1' then 14
        when orderbook = 'C22tLdLd6bPLnj9gC9JXYQMLyb94tusvGi8auVkQJdqj' then 14
        when orderbook = 'DmroJFsNv4Z3d1i1mxSCvubHA7WraDG8LfRdjHvtwrYV' then 7
        when orderbook = '9Qm5fJCgFBx31iqj69WdCFZ9sFJHBfwvd96w6HcVAH2a' then 7
        when orderbook = '5Qjuwat77TeBtcVg4weeieh9aBQ9GcBHSi3hQT8avwyt' then 7
        when orderbook = '2HZNcJm8R7XPCk8tWu7Y4oW7r5ZmVycrb1m5iZtFiEYn' then 7
        when orderbook = '8FYbwk3PbmgNpRuhf3cveXJZCWvDUso3ZdRodmGQohUR' then 7
        when orderbook = 'EQZU7u6hVy3bDVGMv16Q8tUDb9zmadEu9r2Qk2NmGfb7' then 7
        when orderbook = '6nTVvvJMP4eVnTqa2A8dhz2h9mcECagu6mHv4yMA642o' then 7
        when orderbook = '2LAL6ez8iUzQvAKeXuh78czmGjns5Kz45u4g36ipwYs8' then 7
        when orderbook = '5p13nk3YXgtdLrTAiEwd19SFaMkXdGfpvB8a6vxh9GXk' then 7
        when orderbook = '8mVzaaSNk8ijpvr7sHn6SAbnRySDpJKi5Zpbh2UYenzG' then 7
        when orderbook = 'GNsebpV1pRadbJ4KTK5jmQSMFssNTkDUxLbVEzwmnJ5Q' then 7
        when orderbook = '8srsRbC6o3ryLMzYVza9rGFFCfwhieKzCHXdRs3VSf7R' then 7
        when orderbook = 'DgPkzBd8JpC8W3ZQmxjyXEG2pSugrk8XLSxzg9qEWEAW' then 7
        when orderbook = '9VpbQvtDcqdUURxGsh4WzRL9V43yST6nZcYa7HX8ki2E' then 7
        when orderbook = 'APzKtY1eEfa9gvZKSiwmiLotBUQ7iwri8REtAocii5HW' then 7
        when orderbook = '55HyYqPiwahpJDU7WMDZiSJRjHCcqXNzejjVzc2KNuwc' then 7
        when orderbook = '5z5ZT9tvGxMxwDumBkSXo6GwWbt6J7qG2VQJCFooDq2d' then 14
        when orderbook = '9umYdARgjMPmSDU8jLk2nYjieaLKVdjMk3RLtCUUp826' then 14
        when orderbook = 'EHafnV1tDKFcqWatgN23Efboj3xdrUemyU7noWEpYZRh' then 14
        when orderbook = 'H4fv8sLz52uRcYCh7St7NEbGLCU7U4TuVnvDGQxkyMpM' then 14
        when orderbook = '4A3K1NPX7SLaJ6GUWdPoLHe5vbVz1ZVtGkH9EftHB6P8' then 14
        when orderbook = '6w2Gwht8Pb1D4x3eLDzr3DrRcP1FKF5hSAorFvzW9nGL' then 14
        when orderbook = '5NctcwUFGWkgs4t3oaHsGszzEppW86Bn7eLzGvFP1but' then 14
        when orderbook = 'CJXNmF8496hCE4eT1XRTjYKm2VNNzVduydJkt5JKQZiF' then 14
        when orderbook = 'H4BzEirDZQhU8NPMA8xRHj39JUUJbrZbUgPgfLXcmGor' then 14
        when orderbook = '27ReAHqW7WSJ4kqrU1o1eFpeg6QzQW4RsfSUKgeEXXSi' then 14
        when orderbook = '3r3H3qyu2vaPRawwRyguXH1YuUokhQzHStWcNDNyy7gw' then 7
        when orderbook = 'B3HXykcUXdMsmAzeZWa2bVvb6StwhsarsARJPcT74aRY' then 7
        when orderbook = '4unaSWSZUFjku5YhET2nn6BVDwfMpnBfXcoYnQ1bwvqC' then 7
        when orderbook = 'ALHXcLEzCVgA4GP9vc8xKVD2YgytzEYUsNzhPqjED11M' then 7
        when orderbook = '6akWWdsxQBL6f3DS1M37Ge64S35dKQwAkvbCRE5acrXa' then 14
        when orderbook = 'Au8EDW4HmPhBjzLYryZ5dPN5EeHrVoRjUyfAzhtgkEwC' then 14
        when orderbook = '4wKeU9Cbc3uRpjozJ3s5x1y67VgUa9T2SAG3g4r7958i' then 14
        when orderbook = 'B21JBKBptNueophVWWgkSr8zFwG17qmghy4koxEsY29Y' then 14
        when orderbook = '4Lmt2wLFLYbptyrSCQuSepmzMiGVeYta9FmRAnU3R12X' then 14
        when orderbook = 'HoaszYXduaKXrkmETPSETc8SDYCXztwAFvc7Q96uwW5E' then 14
        when orderbook = 'DeCQdHCSjEvXmYdekPaMRnJY1djo5sc4FgCCw7WpFqrx' then 7
        when orderbook = 'DZP2UfdE2bMXUWRjwmhfN9YSCxzRUEDqvjpmGXWtnT7x' then 7
        when orderbook = 'DMymDzcKcV2ymXzJJg4gXoX2Q5buzNNzDxn34WnoqDYb' then 14
        when orderbook = 'Cx1sPKmRz9H2SHgzayjafBRPfFaWzD8ocFq59w5ophkV' then 14
        when orderbook = 'CvRLWnb4Xn5dksNgi8t2vsdPtTE7JUhnWVnbdbGuyFNX' then 16
        when orderbook = '9AVp4GgALSkWhvbSiKJmddjiNKmkNdXtsPtU5Pkq1RBV' then 3
        when orderbook = 'AT7RtNRDjAVapMXVfms7W43sRFThV88bEvsp9HVeztnS' then 3
        when orderbook = 'B9dtGabrj4yGtXmz4w7ZHDMKEQPCHpkuJCzQX5dHRG3M' then 7
        when orderbook = 'HeCETNbqwBXUDtRf1NCihqzDRWKMjMD3P7NbmYdp5MCV' then 7
        when orderbook = '8k77HmE2uXzTPmgBzjovWPb2DfGZarm1VsmTLb6tzVaT' then 7
        when orderbook = 'Gf7wqmdJzvBwzaLURMGJT8Qt8r8pY1CSjMWQRTphiTCF' then 7
        when orderbook = '9hdY7aoMeA7ZAfKY2n3B7UoCuNvmjmnAhYMSn6jo8SoM' then 7
        when orderbook = 'EuVhyu4a2rk32Ndqtj3d8gEsy1RoUaiezrMG57mEjc9C' then 7
        when orderbook = '2tfe2gy1MtfX5dnnatbx1x1KK5f1EN9649LHkH3PZRk3' then 7
        when orderbook = '3B1NnYzd1R3iY2TNbtwkrEa3RPKPLeKdMLMJNYfYK1R4' then 7
        when orderbook = 'Gsjh6GEfzxCFsfpYM1cw7uNvbpd71aTsk57gjaxh1Ei3' then 7
        when orderbook = '4D8buxB4wBquP8Y18gY32jkPmjSpcC1MtQruEFGkdScA' then 7
        when orderbook = '6zP5FF55ZboKFBMKPFGUvgkKVsAsDVgTHpMKZxe1pZK4' then 7
        when orderbook = 'CASiRk1Bo5hL8uQTYUPYUgEaqMJtMmPtKaurhZiiuNKB' then 7
        when orderbook = 'BUVctfN6VeXj288s5iDbiNTC5SZTST1hcEAAhbEXTVyq' then 7
        when orderbook = 'EUQWL8zSRgaHTBJwxDBmLc2Uv7PwqUHVEGj3bmvDwmx' then 7
        when orderbook = 'CLqsLAUhvMBx81Br5QXLak3gYxQk7J9cQKGh3ToMDWKK' then 7
        when orderbook = '7N4MKNzWan4L7EwGA9poUt2HWLu7URRkQkfK9uQjkuVZ' then 7
        when orderbook = '3egbgXhFvEsmMBrw2XkZwy78L6d4q8xznhDvEphGUD8a' then 7
        when orderbook = 'A9kw9CxqoiqxANA2yEga4YCfv8r3QTA4t6UKrLnGuYAo' then 7
        when orderbook = 'GqifXjDe1BVP7rGGTB6gqVbtsD5p8kgBMiuwhgobxXcz' then 7
        when orderbook = '3dabeQGBbCJXSBD5g7Wn17yidQMJXUUf9S8C2jtYS3j4' then 7
        when orderbook = 'HNXH47hMbhVmNgBL1uahBvKomdJQVbdGuexnkxomazMz' then 7
        when orderbook = '9DzwzKKcYNkT5PaE2ajRqqxwotc8kAHHv1RomQoWpL4' then 7
        when orderbook = '7euLwMTdBMxWCsKmpkWpxCHChT8SZtdcPNBzcJGkm3kR' then 7
        when orderbook = 'DxBPJ9RttmB2ZrwMLZMymJKHWrkLx2M8DcFivcG6q6yy' then 7
        when orderbook = 'GVkHNC3RwCBSDWvXnoFHkwSgPeRHQvPi5iszGot2kZHi' then 7
        when orderbook = '9DtDt8VjUeLxmUrd7goboZey676sweoDBoPGfYXwE2wa' then 7
        when orderbook = '7y9inxTzv2M8qT2exD6gnMKtgmMZdLbymr9Z5nRDeARP' then 7
        when orderbook = 'FhyA9pdvEjPuNDfuw4c8kafj1sx7ksDNmByMLsexMojT' then 7
        when orderbook = 'BDzf4GfAvU7FUzn9HrcNJSdhumhiW6W2shKihnP8kx5s' then 7
        when orderbook = 'Htjia4NnhqSgz7zsn512MJSa6Q29swZqeiShuawUzkkn' then 7
        when orderbook = 'Don7pzRgGYmHWRSJrS6J3UfZfHTkdPzno1LNPWqCgBqZ' then 7
        when orderbook = '4wRei1pdjdahgboknVoNbB2BRWy218fEayUAKbSmR2QU' then 7
        when orderbook = 'J7D4848dsBWtAg98CJqgUbJCC83xXs1jVjyqthZS16du' then 7
        when orderbook = 'BawqUy9UGyhoxLHDikAyzmRCu84RpYRqKnH136NqjEvw' then 7
        when orderbook = '9P2hSoj1Bn1Wmw7WndT4QojPzdgAW3TUvw6vwpPi2CoQ' then 7
        when orderbook = 'GyAecDfySYfebR5FW2ccVhRtGFscT7VCxBs8f1c7Zkup' then 7
        when orderbook = '78j8mUBM14YVQoFZykukQiNumn5TqXdsR2xUSS45aoMj' then 7
        when orderbook = 'D9XgLBXv4BktVXZmzZXjs2e32BtL3ErPk5eWrmybvWpd' then 7
        when orderbook = '9Unt1XHZmN13S1VAso7qWzdXeV8iCm5xANHcUhx74fkV' then 7
        when orderbook = 'Dnanesbc8Nfo7B4QU5zQ8NBNHr5gaRSCWCNzokTD65gz' then 7
        when orderbook = '93jYxr7zpadU5LRWXy5A23GUYthBZbtw7UHQeoNLMe1J' then 7
        when orderbook = 'CXMxuGFd6gnybruPU7CRWNJ96zfddGaft4s9Z6znxeKg' then 7
        when orderbook = 'G3iVgPZYikEK6T9LLKZYex31GGA9SBkffuBfq3a2wDUF' then 7
        when orderbook = '2Hg7wSvZHNhqP7atAUjqQeZ3JQJnMf82VcGGTHjaLay9' then 7
        when orderbook = 'ERH4KWEFZDTHqyGY7qyKZ94jGy1SAF7H3TgUm6Zq3HGV' then 7
        when orderbook = '78rrWGz9HEnZYnGFUUN7fyJ8hPJPsuJaBqtP5MScnGgw' then 7
        when orderbook = '7nxjj1GEgZLC7Hf39ZYqStcahE1muAjv5hKsHfo8bouJ' then 7
        when orderbook = '9CxcERivY6qvaAuqfsqBLm4ncyeQk5mqYUD5BFkavT2V' then 7
        when orderbook = '8TFsdbjqYHDjj8yMPPwMCHX8NuXjaoJbmhqq8sJEg6LC' then 7
        when orderbook = 'H8w4jGYCyMFZnHiMNQS1bBaGKHGSmFWAHyEKjBtPmJYe' then 7
        when orderbook = 'AnDREtabYyRTcdpZJSCw8ZiM1cW5jJ5jZUuj3wqRcRSE' then 7
        when orderbook = 'FgygrXHEzDBTZYRzCA3QMSPe8CmdvFcgesBBqBW9r8hk' then 7
        when orderbook = 'LjA8vPRhcwiZY3o1YQdmQmu3Z7j3257U9bAPSJwGJW8' then 7
        when orderbook = '2fmf8fqYwf2qZ2BKgKHYJPwndCD9MUwXoNSB1tWezEzo' then 7
        when orderbook = 'EQ4C5ic9S5jJVxpYCv6mtPn2SUEAfE5f5BHqeMwwkuxZ' then 7
        when orderbook = 'BTMiWRvAR2HcccwbtxvSmhBMGyqpw36jFMjRhwcBMRo6' then 7
        when orderbook = 'BZbH5SdbvXA6BmEZie5RU3NdYHh8deUbPKCsotrwSRS5' then 7
        when orderbook = 'A6BdN8yuRMkee3aHbVWysQccHLHNAbDiW37PBSgp9EBQ' then 7
        when orderbook = 'An18C2FozPD42PnCW3Q1nXoEtcFnqbLALan3KEMwVnL3' then 7
        when orderbook = '2xnpNAtERkcaXfHbk9QgaFNMtUMsvS5pRueQG4BatwHk' then 7
        when orderbook = 'Er7bsT31HYoi54ZpuHiZN4n82NDGoAHtU7TzgJgZVoNH' then 7
        when orderbook = 'AvwJkrGhu7hsMjLdwzHYiPqLbKzKQsywkKrixZhK4Xy8' then 7
        when orderbook = '768FwUh1goEzvR3BvhFK2CjpU6RZC1hWqBQeh1Ykw9Hi' then 7
        when orderbook = '5HqzYzSYHy5TNEQn6c1ay7uo8GvanAcH9yLdeez5CG8a' then 7
        when orderbook = '4dFBenUZcHVKMtGnDh64jjYrKVkktjwPEvw587jL62PX' then 7
        when orderbook = 'HLxwZ5ZBPDDv8WaAJoAbB22yxZdGPvFvopTUnFGvLLvr' then 7
        when orderbook = 'DrgDSCJ5YCPoGms7RJYPf4tNxpP39eJyiRS5EznMS9Wx' then 7
        when orderbook = '4o5Gq3hpLr1CtGSJA8AY5yGkxksrCMA4KeT9MW2N82e3' then 7
        when orderbook = 'Eiups7qSZ394KE2u5YR6pSr2wdQNR1NgzWHLFdLP67r6' then 7
        when orderbook = '5ocFr92kMhbpzLiWGeFjbspWE7N4g3hxiK5NfKqKFZ9f' then 7
        when orderbook = '2tDWS8U2SVUtzDfRFYLaZeBVf6ELpEkefK8wPdCXWddF' then 7
        when orderbook = 'GxbDm6vqyHA6cKR9ECo7H42Hc2MHdZNwmLXaXzu4VvK4' then 7
        when orderbook = '82RNbMgyZz9HpGpMysBZ7CKh5MeVppLBPy2TYSU3dArc' then 7
        when orderbook = 'GTyFCDhH87cYSZkgwvGkEGd15JTXjyWRjEQ1LEiwWL3W' then 7
        when orderbook = '4P164Rc1S45zQktx4DU4GrTKKNtFnRS6jwkqiVqGYN2P' then 7
        when orderbook = 'AiYV1ZfNTNdcfyCsxQVGJUqdhHvfiMkkW1Dtif1RHf3o' then 7
        when orderbook = 'Bn4WWVkTJix7HYmH6cq86gZZobMT9mU8KWFMWHzJMcUH' then 7
        when orderbook = 'GF3o4ro4UYMobaknizyA69ZxUawfhxNNBF5uYrBwAtDz' then 7
        when orderbook = '21nMGd2vdgnPHUErL4uySzSk59cex9sHWZi2WiSDZguo' then 7
        when orderbook = 'AqjvMPHkgkKpdzDKbhcCsCyHjSsU9HnzV9t43vhAFMwj' then 7
        when orderbook = 'ES8e8REyNBcX9Fae2piF1ETcNwjn8ZbhsppcV5TMfvVQ' then 7
        when orderbook = '7mFNyhAV48FDLVEFneEDauP2KJ74zecRYhUCtzgtfWaL' then 7
        when orderbook = 'HLMxwGk3bhpHq4cfSv9AkcLDKzn976pHbDwKmSzaqeod' then 7
        when orderbook = '5vr2CkzTyQVZ2z17eYtCuysD6EfihKnZ8B48kLzu9gnw' then 7
        when orderbook = '4uzfpDhD38ptuDcXHc5xXxxmsejwrfLixXcS3nLiGEWU' then 7
        when orderbook = '9tsPpk7uFLBA6xusWS92jbx7ZrxvWAPhypDGSvDzqXXy' then 7
        when orderbook = '8ZoSrQC7S5DipsLfEs3fy9MaMyvBDMAnWSzJ9DWoMBRZ' then 7
        when orderbook = '2E1AuwUK7YV1kkdbruHAU4Y4f22gXPugGa8XoyA5pQJd' then 7
        when orderbook = 'ENg19HSdqvA6syxigLEcBRT85P4FrVD7zxeJQGM5pTUy' then 7
        when orderbook = '6ko3GGSac396pcsEGx7VkrDfKZPMdW9Fvf39MLpQ5vSq' then 7
        when orderbook = 'A2dhM8VWwexxhWnaSKA41sU4SJZjn77AZrxMfaAfTGUR' then 7
        when orderbook = '2VCQ4oPpXoDNsGf6U73dSnmpGdLYnsdZzv3Gb8SaUQxq' then 7
        when orderbook = 'FxVkepR7ueSt4KQnX8DLXC49m6iQsz38eHnNreSSPKM8' then 7
        when orderbook = '4M1LocPnpJuQQqn3QgjZRhBmmSUy2EA6X1VsYxyLxuTH' then 7
        when orderbook = 'AiogFPv38R5Y1HtWZQ2Lcint8JYjk9WjnzwuAc1DiD5k' then 7
        when orderbook = '8W9d5oBhyihLESonZPo3eyjQwSQc6hHH2D3CkibsxV5i' then 7
        when orderbook = '3T1CBuRBRbmD7R96sqCVrbLtJkDT5hpV18ojYkP8wsyt' then 7
        when orderbook = '2vxuqRDPgPATYzTiWS9YAgZijCv3kfRrpondmVm5s6th' then 7
        when orderbook = '48ztPvxAiNu76WDofscNN12w3B8xzGqtyNoW3m39nXSS' then 7
        when orderbook = 'Hq1sMcQVoEikLHimYtgFjTe5pCKQXEXxdMZ6fMkMnfbB' then 7
        when orderbook = 'GiazYRELcJoaES2iXfuarvY5eFy8gDsjRy6HcAkpAYjX' then 7
        when orderbook = '61MSjhovRLGm52dA7dJVpSYSM7emEn7EttRCDcVz9vTB' then 7
        when orderbook = '8dYPQnG6638fK822Q2rw25sPQ565SGvJpq54uZZSoowx' then 7
        when orderbook = 'FwQe7vJogwffMiEq1aosykZV2JrEXQK39BusjKx4CRgy' then 7
        when orderbook = '6NGKtLXrPJ3HqDduzM91VRWwRpRUXNWrQcpNKfeF92Cc' then 7
        when orderbook = '9Fcn3tFm7iNwNxhvJZmmLD3akdbSM991X5zSvVan2zZ9' then 7
        when orderbook = '3y2i1UJ9r2yezyoJtzpCjFSVDLG2EeQmi2kL7ueCnBXZ' then 7
        when orderbook = 'Lx38A6S6p1uA7PmkpM5sqfs2nsEVEHCcPJ2y3KkLXhu' then 7
        when orderbook = 'EvRMHrEpXdSGWL3Hkp6iW33swrQQXpKAEeTDcwKCSu9W' then 7
        when orderbook = 'GyEftrDrPHzJrGzDjA3avb4Cp4qrqZbnzaSdfDNUNMS2' then 7
        when orderbook = 'H4r3CpKRa94D16sSJLHdvSt6bb5tr7mZvFBWviQ2NByc' then 7
        when orderbook = 'mpqRBuDrc9z3SG4VCkVayXU1N2sDBFYXBvwmapF6P8b' then 7
        when orderbook = '6wEkzyWC6naFkHRRZosCoypxUT9XFqZXWLLqz69b3svr' then 7
        when orderbook = 'FoGRDfbbca7DuwYZfYYLCL1i4yB9Ui2RJvWQJNQ1Kygx' then 7
        when orderbook = '4QGzF6eDKzdFN8K1vRkZbcCm78m8PSD6sdcPSm6JT7W2' then 7
        when orderbook = 'EuKftNc8jQqvnXmJPuLFmxCYDBxoU9UXKdN4nWJa4igA' then 7
        when orderbook = '5u5nvQm4Pbxf9U5aNBZwDPaNqwtKzZyWSXVcdUE376vf' then 7
        when orderbook = 'q823vJW8QChuLM3KdYKJUjz81jqU1QL9jrMmBMjtRmQ' then 7
        when orderbook = 'CmiMEYEQk3hWVgBAzytcRrrAv5wkg48zcteTf3gEfF4F' then 7
        when orderbook = '5Y2KbbFvFxEeuCVjJmcY6ZVL89yGAPEhRwaWUnQURm4B' then 7
        when orderbook = 'E7XUUQt15uF5fsLhm9nBAxb4B5fvUzwK4ErVKbgvdzDB' then 7
        when orderbook = '7qeucfDbqkRtZ4L6U7ffbnsuv9fpCt2FKkkrv4mssAKU' then 7
        when orderbook = 'AcErr8rKpaeuTi2UohJRN8W4mTgi4qA1cU6ZvHWGWvgn' then 7
        when orderbook = '2DJdf5JbQM5V7awmGpmhHWiMdPZaU3KBWfX8nmoBj62X' then 7
        when orderbook = 'HnUg9gViy4eLTsxhjrZSg3uGB4giQuuBanK7ewTebD6z' then 7
        when orderbook = '6Rv7xZQdsgunArSS4RxwnTaqXRJTrXDKjaVqTdtXduMS' then 7
        when orderbook = 'ENVRWTkkBCA8HPyKLo17jVFPY5cb2DsEv27J6Kdbxmf7' then 7
        when orderbook = '4gHeHya2DVHjGwkosFvgsfVpGn8gWA6RSuxgmF1agPXL' then 7
        when orderbook = '8u7Q5Tz4BYjBK2STktrCZkvkMxCSFUB3mBkP6q2mFyq6' then 7
        when orderbook = '6dVkZhpPyZeUZ15wrC1kNK3gmG839BXLSW4QrVrCSSP9' then 7
        when orderbook = 'H3RrLuxoCpQCeoJai8yQEPfursKfBb6pc3UwedGKjUV9' then 7
        when orderbook = '8QWYCXWafc9KZd9UxDJbK5cq2MNyVo1REVFttEW76vSe' then 7
        when orderbook = '6XPvCp6dkpAjAnc3Lnt11njVpzXb1fMwAFx4p877VyU9' then 7
        when orderbook = 'CX7YdDitGaYgahm3AYCDtZ9hfNB5rdEBVBm1nW2Dk8vp' then 7
        when orderbook = 'DonD3ZiRTX5w83bKfDzuQjKff4vhoYnswC2AwFiuvWS4' then 7
        when orderbook = 'EHT9b3sUBhBZf5EpaRuzZ8q5WXfRxzkiEogy5AYiVXB1' then 7
        when orderbook = 'CkSsL9uLs1XUGXrcqzFzDVHzsToh92DBM1W5rKBxUA9D' then 7
        when orderbook = 'C46Dx2jVCCfce8ar3R6gHTC7kfWzWuWT4bomA8jNfYRJ' then 7
        when orderbook = 'CgGEp5KwEKgPNPoWzyw17X77J9pF3utyKGKxJSbSRkCF' then 7
        when orderbook = '6sdGWTwFtMpcKdRH4fBpDPNFfz3rT4ZSfzh1hnNTsDqD' then 7
        when orderbook = 'CMKh9Ww7x9Tz23fgjzLpZietXNEZVny3TzNjQcTEkDru' then 7
        when orderbook = '8jB7GDv4ZutSPuFHcu7yf4baG5ATFccKD2MU5PjyzDRA' then 7
        when orderbook = '9bondED2Z31sd349pM73Yp7wTkCbav9f1kj5EHHhmh9p' then 7
        when orderbook = 'DZojZiK2LbZeDG5Cvf8g7XFhVJrvYUZ7gH8wweNDTV7L' then 7
        when orderbook = 'J6nJ2PmphDLFc94kbKnihAQpzmr3uHmYW9wVtMR6WfFb' then 7
        when orderbook = '8YWPutChRuiBpXyDAX4nBKeZTm9DyCFqEhk6QmKC5Gge' then 7
        when orderbook = 'GZQ7DZt3YRr9gGfGSJGoxmXk1r6rkJxgG78Q5SZFiAa9' then 7
        when orderbook = 'EobeQZX1P2aSejXCdMg5qSjDAZCNLGJ8tYKdEyPs1s4w' then 7
        when orderbook = '4xYsBo1LfYLQVczFftN3fA1ZsGnwcKwghXPX5CMY1Kwc' then 7
        when orderbook = 'FFNbSaZb1DJyDapZks665QWzyCiLtVsGjoBuF1GLggaH' then 7
        when orderbook = 'EGQR5uwZ1XEwBmvdmgvHRAyrgMqMPQ3ckEZtjAYZ7ioa' then 7
        when orderbook = 'RU4Nj23JHbnonAskwpEjRrCPEADyiraZMu6rg2b3yrL' then 7
        when orderbook = '12bwZJRHR95Lwk1domF7B32tci5vGkHxQJRXbvJBYPqM' then 7
        when orderbook = '8dU3A9QPJ4QqrVcesdMV2wRvVtXRwetBLxeTmRbcM42w' then 7
        when orderbook = 'J2xZYp9YZj3eQM7QwDQQc9pWFjvGAe1YCmZL1crKVUse' then 7
        when orderbook = '2aLUgFVkqGRahZXUZHQjD89PCwaeZjpCPQ6BG4h3A9f7' then 7
        when orderbook = 'A8YhiSo4xE9poXBWnoSiJHzPg7fToX9TToB2rzfsEPUH' then 7
        when orderbook = '3LdoUZUs9iBxAH8Vqs55rDsMNmRFUAkewjuiL331eq3E' then 7
        when orderbook = '55uGhbsa2aqV7EDn8MMkFDMH6w9jFVizGhhFr52mCPMW' then 7
        when orderbook = 'HRSWHYrKNSwhRi2ktg72znV3gCZ5uBLbnbfL7TBzjY2q' then 7
        when orderbook = 'HLUqWiemgEPdfz9w5WeP4nxsW3JzHayQX6n3zbTVGnaT' then 7
        when orderbook = '3rmQkJCAMRGEeqmmh836FiUryKEarhvcHV87Cqgs4p3q' then 7
        when orderbook = 'HHwRQztu3CS24z3eMntzM4WFAPc8431stY4VerjarUk4' then 7
        when orderbook = '3pKudKtuNJNhPUFQcDVcMS8ZY7k1ZMdjMbHS1rx739f7' then 7
        when orderbook = 'AF1JtCWgPRUKk6S5znp9xhgcby6PoPhDkxDZUrCmKjSw' then 7
        when orderbook = 'BnJxJvfrL4fCuxwWJdgNQcEWWXFCcMAS37BnMUXVigZZ' then 7
        when orderbook = 'EJUU8Skh6dqGgQfQkL2i486rwPjo2ZsfJzKF5UsTfEnp' then 7
        when orderbook = 'BywAQ4p6d8zBgcJp2cZgdFUpmxioBSQoD42PZjA3JJdd' then 7
        when orderbook = 'CrdwR9sYXFt5YTSPUQfftRCeFehih7McvHG715nv4ty2' then 7
        when orderbook = 't9MMqVPCLJTH3sj6gYK2uRJm9oc9TnaNB2WGJthn8ur' then 7
        when orderbook = '5izELWAwe6LeCBufg7t8RuL7vUvnaQ17PvbQ18Bmwjwv' then 7
        when orderbook = '9TUKMjGJeu87bz3WkqEae7CFxaSZ9wTWjQ51M3CAWivN' then 7
        when orderbook = 'EzmTj4P8xJEQmbiUhi3ZDZ3fA3NdjaBWRd58R3Z6QMC5' then 7
        when orderbook = 'GkSxfdpjFu3vJUKDtzztuBEwf2VrTPmFmprp5asNR6t8' then 7
        when orderbook = '7J4yvUhLR6NYJjQewKydhyUgymn9SNWrpRRaLF6xoip5' then 7
        when orderbook = '7FbuhkNjvis2wk3vxu9jpeHjgxpr2Myyw9vmnbVWoPLT' then 7
        when orderbook = '4Zat4ZrTRMzXUPyYSB6DF39r4GZ6kaQb1yxZaeT9MybT' then 7
        when orderbook = '8KxSqLgVU1D57fUh2LaAKSrgeCY9L7L2jhgwNrByTPMw' then 7
        when orderbook = '5Pw1hC8xJ8NuJTJGCkscyyJMxBQRGosX11MWoFCZn223' then 7
        when orderbook = '4WakAKCjYT5NRuW4YBKQvXwnMATszCywzB9taBrcMVnK' then 7
        when orderbook = 'H34ZXKSqaiBzLEm3f2sBzsr91hovxvRpowM6fY6Lz84n' then 7
        when orderbook = '6hkg39J1kbQCfCnNBUybktooRJ1eDLpTfywrz3CU9N7o' then 7
        when orderbook = '491W1kai4qheuFZmwQ41gb2nZvo3kdMRgJmHhSgTxaYG' then 7
        when orderbook = 'EvUQv9xrWih4mp5A8HpaNubVWMKxhPEtJxNvkUtSH6o7' then 7
        when orderbook = 'H9boXKfdYugdRtNt323XBRiErxHgQAWM5DEbGjashf3T' then 7
        when orderbook = 'GZRQXy5239hYyKpyFtT2u9ZzFYB9iCXNK3vN9DSor2oY' then 7
        when orderbook = '9uscsVbP9VSk5qXSUzZfYmaoFKJVouFx2PZbTby3ME1P' then 7
        when orderbook = 'FCp7sa9rvhyjBSSh3e5G1jG22KP122ayr1nTwnZN11ys' then 7
        when orderbook = 'B3KewE2K26EVCJLzZphSLtM5RxU9zRqoDyhJnUo7i1Hb' then 7
        when orderbook = '9DLwMNE11swnwZRxUw8DaXtFkyr2uJ1QxmH3uceseudN' then 7
        when orderbook = 'CiVsiMMZ5jtRbUgQkbr1KXns2gQfzG2g2o1uDUasue42' then 7
        when orderbook = '9oSjfQuwEXpkTDZoy2ycRmzRu1oHoHeim88MRWbCa8m1' then 7
        when orderbook = 'FkfpuRrvwLFdL6cUfXXnyic2m87AFftZ89A1sBmK7gQz' then 7
        when orderbook = '51mumtNVARBwgcEJ63rbLwTxeg1WrScQ9BmB9RVmeyQq' then 7
        when orderbook = '7Xp6sTDvDYFuRxomVfQ2Jt5tuaMhLXu9tNyYpYg6UDQy' then 7
        when orderbook = '6J7jKmjtc8Drg8LB3pPm6rridrwkz94NyJR7A8Mr5BiV' then 7
        when orderbook = '7a4pjsUTS7TEjBuFwLCAPNMrmRKvTUTALNnWUztNxTQe' then 7
        when orderbook = '5A7mLirEu1YW6zzyke8yURcZYrUR49fysLh8tDeEv1s9' then 7
        when orderbook = 'GgvVjAcWyaaiQgBe9MEa49aexLiFtj5Dn38xaXfGwudg' then 7
        when orderbook = '9aXFDrqbijYjwLLDQdgJV71A91AYAgBpRUEjKQrij1Gh' then 7
        when orderbook = '2kew1EtHBmxGdXt6BsbwCmLp5L5pycNY3YdWtnPMQ9XB' then 7
        when orderbook = '2yaSK3v2gmnXv8WycKCx5gjenVtzLoA3o4g86No8zwhT' then 7
        when orderbook = '9YiGTok6R73gEzFsRrZ58NYM9xd7eopQA9o1VRvHH9nS' then 7
        when orderbook = '5BYEHf5K1LGZWG2RbBSbWKA9n9DhyvKxuBvb45SAcZEt' then 7
        when orderbook = '7jVrLg3hu45nynukLzERDUj4Dwefqbx9XdB1MHRV9vN4' then 7
        when orderbook = 'ARMmkLcKMu6aiS76Wjx4d2fmYPd9f4t3dR7Esjryeh4W' then 7
        when orderbook = '3b6XwiXNribYmbo4P9TSP7LHYAcbP3YUPGYWNpfwfBMp' then 7
        when orderbook = '77M8cSPgxQhabwRqcu5eqBwt4sqinrMXFtsfiSfxjuny' then 7
        when orderbook = '3jvbiRg6ijBEpaoNvKsyH4KJYPui3NfjsZcwoDDPuB9j' then 7
        when orderbook = '4MYqTKy8pzCtUXrr63zwn3hDw84TfeV8XtvdM2TYB7pq' then 7
        when orderbook = '8eSZEQULZ9aq8PQ8pu7nKaTZJzWX5tkqDT8HCRcLyXyz' then 7
        when orderbook = 'Gx7zWJSXfyk1yUci5CB156pMQuUMxzPWJsPSDmcX8hjC' then 7
        when orderbook = '5vSZcek5ciwAYUezevXf8Hd2BrFH4Vq9cgGf6yDWzVSL' then 7
        when orderbook = 'WnmSJ8UTXTuBq2PtERH6t63Ki4RnX9KT8aovxDVYSc3' then 7
        when orderbook = 'HDeoWDeLZuqdzzKAsALjKtBpFoJSCRu3T5aGDbxmM1sG' then 7
        when orderbook = '3JELLvYuiwdefb6WEDJGCayXR28ieJXGBE3t3peVQymA' then 7
        when orderbook = 'CD3SE2DZoEP4YhTz3jW8syWvC8tM9Ut2bT8JzRnmbcYT' then 7
        when orderbook = '5my9WYhnsdSLXUEqG5FDNjwZTDfvMUP3V2FE5CAoVMhM' then 7
        when orderbook = 'BcsrQeev9j4WhLzKzwAk2yrJzasGU8CnrmnGec5TtZUF' then 7
        when orderbook = 'AfsMEncggLE7XJsrkBXuVcSJAZQAfgJbaakDC8kMHkmy' then 7
        when orderbook = 'DZ8G7bqi8QNrsuoZoTEHBrBaLhyjocS6KvtQMBFa1eit' then 7
        when orderbook = 'CAAsGaktYdVWATM2ptoYHyxxz8ZouDM5rj68RDqbEdEg' then 7
        when orderbook = '5kQ4WnxUtU2JKTfHjyNbRi2XYXJCsb8sLcXX4abrUzqJ' then 7
        when orderbook = '2kBdGCy8NTszWLuB3ktdZZyus1xL3LWzSTR1wMoogvWb' then 7
        when orderbook = '73irCghxWGmQsVNyoRnUdknNqrWGYPDU5RCwk96X7mzJ' then 7
        when orderbook = '2xitAYkoG8S9vwFb45WSKzYe9nS6fTULjqffQcjDDnkv' then 7
        when orderbook = '6xgVU3rwwYAV1288795GYgsMSBBrozo9eNuVRi22CB7e' then 7
        when orderbook = '2dz33qzKLnSb6ibA6CZhDc7YZAw6EGz4aGoBp3DZ2AHb' then 7
        when orderbook = 'BJNwChqPBXJ2jbtYnBHXcAX9zvQQiYWaGyvv6pDcAjda' then 7
        when orderbook = 'HzDeqUuMgtaZweahb8tVi6JPCGH4XdTFspA6JZEa5neM' then 7
        when orderbook = '8AsZaDn1kCqjn2xvivfjLVXhKMZzz1zuaANhSE3mmVRZ' then 7
        when orderbook = 'EmMvAcxpea8JhsZnzTgkrYEQB9pb5DBRTrR4W3BeJpcP' then 7
        when orderbook = '7naVAd7cWNUMCiDVJbxFYTYgeGrPzj8TPek1V1Wjyy2u' then 7
        when orderbook = '87xXE1URecyTH9ywwNwPAsjTSxRkBXhS9M6aPvTyn92R' then 7
        when orderbook = 'eNMQBwZ23b5V3JEyjjSgGLuJWU34HLC8M7Pe4fkSFgq' then 7
        when orderbook = '7jmwXYq2BZWJTNNDoe3uzPjwaRSqdV2g1pj1D8ZXEX7f' then 7
        when orderbook = '7sS98YT4ap1ZcyUiEXSuwY1kPTADKFk9bto3L2jJud19' then 7
        when orderbook = 'H6ku1Kxf4SeL63deW3ZcZuMXr9b5jW83GHzX1ZaUU3Qj' then 7
        when orderbook = '9pSirXyVoxCmmemLAQZW2uxrYxtTJFCv9t2vQnieAmaJ' then 7
        when orderbook = 'Gwy2x8VHjVoH1wwKPg5Ttruue8gKUcZitfdsss9x63Qv' then 7
        when orderbook = '84Yd6ZEeoEXE4QiProVtg7o7rAcW3g1hX7CRRAyceMQa' then 7
        when orderbook = '2CkZh2mk6Pbw3hPgvFpnShiQ8maAnVB6supDNrWd5PwL' then 7
        when orderbook = '7gWrhoy4rJJXRkE7kH3Fio1nFxU3EtEUhKXS6R57uguk' then 7
        when orderbook = 'HETGR2S9mJhJDKteGQ8EevsmPG82jF3MSxWtdHkSfofB' then 7
        when orderbook = '7U2Gd4qY3iY7uYvk7XxbJ5o8ndNALvPD2Wh77shUuKvp' then 7
        when orderbook = 'FyfqopY6DSiHzJ7gfUwv6z9bQYctc1LjCrWffqeiEmqe' then 7
        when orderbook = '6KWFfy33UfQE5mXdSptLUQJwfeq4ZZGEb7Rhzf3KApCx' then 7
        when orderbook = '3dUqrnBDimZiXctwEMz7jeDJ9nBcwi6oRyBBP99JJfHN' then 7
        when orderbook = '6BMHJcR9wGB4V8Z9JAEqhxSZjmrsbqx253JPAX47Tgix' then 7
        when orderbook = '3CBNipV6cQ4xyxRZyzVYQPQEUvNuUEbqrUidjhjHSZND' then 7
        when orderbook = '6hthxTrRELgojgSrXbR8ruJZt9Y8hgwq2KQUN29qmGEp' then 7
        when orderbook = 'FWaLM18XF5yT1qmf5yMWkruqP3aW5PPG8LMtc2hjDiLF' then 7
        when orderbook = '2Nb9nmMzhpzXVA2s2ymYdhwsAw7EFa8bZ2qcdyPQgmF2' then 7
        when orderbook = '7yHXqKKutDyHneRZBnyKo36BXTp492q5pXMyJQiVVWMe' then 7
        when orderbook = 'GE6Untcqtp7JDq8unPQkM7YitmQ7hvKoWBRzxuP5X6Tb' then 7
        when orderbook = 'GCiSLBZn7MVj3oiidF5ZAhQ2JsUATcenLJaRc5s5y9ZS' then 7
        when orderbook = 'AFn3o1FcoM5mTdEBiL37GtfMXzFjmebevEKXwchF6CDo' then 7
        when orderbook = '2zEZpgCb9h4yU5kovPZrt5vjwdzy2heyVDfsMoVyWCFv' then 7
        when orderbook = '14tngHvcSm4NySKmeDrM2G1bb9QBcVXCtpD9Vt5nKZ1Z' then 7
        when orderbook = '5nKJLFG56jhvBZYzphSh3ouerZnPjfndK5fPpYpu7Mzx' then 7
        when orderbook = '2E5B3oALkYkUReZBBbo1N5UyoErNJQ9mo2MrZeLLTfTx' then 7
        when orderbook = '4F5NMeLzWJHt9ouBnU3qTtnq66U227SYvginFqhNdkE9' then 7
        when orderbook = 'GgAysMX483v3wCKf6RcTLCekgA1ctnoA6j7mbg8pM1PP' then 7
        when orderbook = '2hNG5Tz5J7CFMsKE5Yo9znYfva1QfK9DasFQ35aeF8g4' then 7
        when orderbook = 'EvzMEqN13U16439gEyY5aCW35Pn979EhHjbBjvMpr4uM' then 7
        when orderbook = '8CFnobPj4UL4rNcmRsQLpFAc8snBVk1fuTk6bqXnEp72' then 7
        when orderbook = '5h7moSgPmv9WVNh6NFCS6eGPuKunMdoZ3Ks5SoG6u9LS' then 7
        when orderbook = 'B3Myfb6TPV4BhiKCFd4rTE248GDrMwoWtRpuHzwNaZZW' then 7
        when orderbook = 'FBGjE2bKk3xtRxewd8zy6QU4kEMGq2EH8doe9mbKZygc' then 7
        when orderbook = '2r7xcKPEF5skRuemQ9AXFLtoZHwyJ8Unm9f1QbuXKA4U' then 7
        when orderbook = 'FFiEzNdho5e9B17csDPSRZhRHhzBSmhRK4yWRikR71Sd' then 7
        when orderbook = 'GmF3mRff7S19YxNiJty1GuWvh5p2Xk8Atkd8REZ47jP8' then 7
        when orderbook = '5YrayX7TQ97R35TjDqqJatSrA531RnvH2mGjHvqWPjnu' then 7
        when orderbook = 'F8GNSxu9nHCxJhcJoVEze4gNEHLmEqzmhgzToqfBk5hk' then 7
        when orderbook = 'GgZNocTDJjc56PX9kcYeuPcgxWTFKDa7P4p5JQmrHdqE' then 7
        when orderbook = 'BeTvizCfnp2y8FnxAvFfxNsyLGsgbc37mVu7ofVHrDnM' then 7
        when orderbook = 'FmHebeib8waNivr5k8NExe2ojUsvBp3KKnDL28jDMGFa' then 7
        when orderbook = 'CTDWHoLNnLGaXpYqvEmJcXxnZFhCc1jXKFVbSNF3M5NW' then 7
        when orderbook = 'F13rXMWu3rjLkSr7Q4LWZXivc2rXAAAoXLwni8hTevUU' then 7
        when orderbook = '7Xm87AFgGMnaceBFTBn5A23i6AnEmrmRcEFCHCcHarfp' then 7
        when orderbook = '5pm9gEkNaF5AcErgxR9VANPccNQC3tqM7zkkqTPwF5Tx' then 7
        when orderbook = '9cEjYpJYUtZUTiSHMebGBf4f1612mgbnsgReUeywCa7F' then 7
        when orderbook = 'AyzA1FiVCWUxMam5kGoLuxu2x3qSdHmTU47wFN5iEKD9' then 7
        when orderbook = '8dCv2Q2HCnvAThwvs8BXK8DDuTmLzEV4HvHY5Lw42pAS' then 7
        when orderbook = 'BVzyP9jk1D2BFpPXcP1rgDUk8KjR13EmjhRpG43v48Z6' then 7
        when orderbook = '3NBmGfuTevw6seHCmhzYNGE21c1kn4BKUH276dKdbfmc' then 7
        when orderbook = '8gmvWVj9TskuMQhRbx3aCxq1Q4iefp5mAuN2R9AEiEjr' then 7
        when orderbook = 'DMmME1tTt4ACzJ81AiiZFUE8mTKPyp41yQgEDpzu1WCx' then 7
        when orderbook = '4s4W5T5gLdzEaAU7oGtwRn2hDi2BxfHc141i2gKBJVKt' then 7
        when orderbook = 'CXMPcBjYt55vb9zWSj7pvVwaSSH6PfQkHgdnAKyHSR2i' then 7
        when orderbook = '9swCgtFatd64UwXNwaCf76hdMtXRWx6RDNpDiEgoYjNo' then 7
        when orderbook = '8QizjcBwoJHtBmkTqhkU4KhBoiVT1444hRorp99pU9yX' then 7
        when orderbook = 'BAo1pM3MedfXbTijsXv66Xwy5QHmTfQSaME7uiZnzo81' then 7
        when orderbook = '4JLyVxZdMPeubTgKhwt9MQqfox2rsQt3d2yrxFKWFMCE' then 7
        when orderbook = '9MZe9ctW23qJRDrGTHRqsPJosPc5TGtf9f5E9x224Uuh' then 7
        when orderbook = 'CLnxNnxEgxuNTfDqzh6Fqj6A87fNZa6WwvwTB7k5N1eR' then 7
        when orderbook = 'DuokZiiQ2wHxiiq9FXZa12NVSnK7SXmWFvSBVbCTSvpr' then 7
        when orderbook = '8wZVmmLLZAijuT6mc4WhN5d3ratWBfhDBmgt57qxrCEF' then 7
        when orderbook = 'HW7vRnebjArAmNXfqvbuGC54oNGPQsCYYcvCPL1EJhGj' then 7
        when orderbook = '2Zmi3wXns5d3FxDCJYL9oXWe31nEmx2JrzLiwCLakeJV' then 7
        when orderbook = '6Z5zgpYxrPMrgHo2EoFS8KJb4mT8NYjRwAfzSBMcguU4' then 7
        when orderbook = '2MCherDm9RsxDtDHPaBmoEPxvvvETwCuupbWaqESVxjF' then 7
        when orderbook = 'wSJoTu6Q5FzhkGrnkKf1eB1Bm2fgitiW3ot1sSXqTzu' then 7
        when orderbook = '77yFHWsBfz3xS889MvKHYjNDvYF5ZTsjSBRhBYhiqJ5T' then 7
        when orderbook = '9yXZtFz3bWfzxxNNmtXG41Ya3YfHTBHotCGnKtSnW6kz' then 7
        when orderbook = 'BfXazb5VsQfRbXuCvuWvVXoBdNHVjiWyt3Czc1U2Cm8V' then 7
        when orderbook = '26JGMcL8ephK51bcBPMQfKeVKSAksGR2bKyrwAxgDiRn' then 7
        when orderbook = '5Gt3XSJTmNp2Sem8BLHz9dh7vdVWN5smsKexRH46PX4V' then 7
        when orderbook = '2Cy5PohJpGJsXFN58GkyYqASakkgUyXrXZYuEFgpximk' then 7
        when orderbook = '3ePp2EMLdMP5v2LPKw79KVHpcsjEXwX1zXfWQvaexMvX' then 7
        when orderbook = 'J3ofbjZ6emFUDUZ96wkvkuN2iPVgN6eNenSdd8LprVgK' then 7
        when orderbook = '9ikJKFYENUaXekruZS1j65PJt8p5w8xSamfXPtTdbXE8' then 7
        when orderbook = 'X1MSpjdi3DBLEVFhzigydQXZhAmw3oAEYLv2mXMY3KN' then 7
        when orderbook = '3hPYjq7SoesjgvtEDYAKxYccS4U4o2yd6VAGHKaZWjDX' then 7
        when orderbook = 'BENrqx18n8tP2xYPBQ7vaDz88Y7wSmzB1xkr8h4XrhvM' then 7
        when orderbook = 'BrTPzW6faeSg9wNHNXA8DvQf5f99Swsc3oNG2QS2c9zH' then 7
        when orderbook = 'GznjibK8btWFAwZFtNSAzWUmNwHTX5XkDgwF6vGKWA31' then 7
        when orderbook = '74VnnrcZF4uSScQPN6W8Kcs2tmRJgepSYzKfd6hQQACx' then 7
        when orderbook = 'DFYu4tfjLeYMbbR1VW5hX52gPoauwfkNxzHbqE4aL2Hz' then 7
        when orderbook = '27Ds6LXyvWKQ2aKMmnVMZ8thMwshnuk9EZs3Lq7GX7vv' then 7
        when orderbook = 'DRutLdi5vPM7LmqyBKhKVA6x75eGQLvakPmbXtuiC18P' then 7
        when orderbook = 'F7rrmiBdwgkWFrzUew6Y9eDgGGJaEBGvC5X14SFnLn3i' then 7
        when orderbook = '6MZjgmUPtPDbvDFjaAEPTh3DXoTHYsyLTwygu62d4Ppu' then 7
        when orderbook = '3rgY8DEpRasLbU6QuYPAnwy86ph7uhXbDC6kxtBddJRe' then 7
        when orderbook = '5vqDcLfjm5o1Q3LVn3aLPvLxdwaJhmfZRZEE5iMkgCNo' then 7
        when orderbook = '7YZj9e8GxUmLKfReLRseRki4ByMRPFuhzQGQXa2bsbox' then 7
        when orderbook = 'piz44JVk5BqxxPpsdxum6ZJgn3vkctZscgCFd4VW7YH' then 7
        when orderbook = 'DbhMC3yyJT8Lbr6auhqjC9whpRKaxxzwKNYiM9TrKmoH' then 7
        when orderbook = '3FqYQVzoxYZiS352pw2YVUcqqXdDsgaNXGRsR5mWQQuk' then 7
        when orderbook = 'FsbW5qLnJrQezXfLEJYMjUxWhBAbhV7yFnLZRL2mxRB2' then 7
        when orderbook = '6F1icezrzjtrKFLsNSLqFPUp63xCDwxrBCdewrR2FAkZ' then 7
        when orderbook = '52DJeBNZmjksswtBEgbvqG224yNihtM2kkdLti2rf9jj' then 7
        when orderbook = '7yJpf4UzKkeWveDvW6KmHunbuXRAEZdcwgcv4zMyNU5y' then 7
        when orderbook = 'B9aYKEJYs2rqpNk6fXY9evf7iiMFh3Xri4eYvrnoUWEp' then 7
        when orderbook = '5SyJ7Qho6kbAJn1S24izEvNJDj27eo3acpQMtw6mTGhV' then 7
        when orderbook = '6GdB1D6iCb1VBTiSaKbxspvdfEir1aPaCNPefTSBL8yb' then 7
        when orderbook = 'HJAYTbNFssXSBCBVZa4TEvz6Bnj2G6GbXdDapXo2gf9' then 7
        when orderbook = '637LUNmd3FtAR9yb1Hi5CTvafLZChxDHUDyT5vyajH7P' then 7
        when orderbook = 'HM2DjhLp4ZR6qCDwC3uTvLZQJRK3LJcpptBQypLgPpw9' then 7
        when orderbook = 'AQ2co6WsYiFnThinp2cU2JoQZpkqgNV9A26yNZa7he8H' then 7
        when orderbook = '3s8iEwRehaKCwXbh13DP78Qw5JzaEpErqCfVuFzL5YCf' then 7
        when orderbook = 'Fwucg5wf1GGj5LiTBD7tvbGGpH22T39tFB4RL45rdr2w' then 7
        when orderbook = 'ESXztB1sfzuQFM7GCXU5FWD4EBeP99xYXg5qJhZBSHCK' then 7
        when orderbook = '7H2qZQiuNurUPhsRtreUuHR6V3xnxCKAmaoXuAAjhzNN' then 7
        when orderbook = 'ECQMNk8tC7hzzx63amB3Aa9MuUuf8TYHQF4PiDQe9DBF' then 7
        when orderbook = 'vQ3K79xoCmucDpkb4VB3bgzQXXE9y19MZuQafGNiH5U' then 7
        when orderbook = 'CW6oJwJCqvj9zwBtbEUwXS3nmhjowA1tgxzU6oHoVaqq' then 7
        when orderbook = 'BvgZL9ZrB8fVmAwgzQ5gsPFnKCzGjG9MjCADbdFv2Uey' then 7
        when orderbook = '8ZdHVG2qsWndCYWiKLrQsr1UnzXUxd8ReEhVDtbVmotU' then 7
        when orderbook = 'GZbwfxnyjHe1cHQDJwE14AMEBRPHDnE9tc78c4f5L9w1' then 7
        when orderbook = 'DUue7UMu3Q8pikLY8x8tgfjvN755ekNqooCDBSfembzU' then 7
        when orderbook = 'AYgQYyZ7ZiH1qVYG43dyjp2EAi64yDEiFWeMwECMSodr' then 7
        when orderbook = 'C7KH7qQMQ4QFPmAoFs8mktnSv6K4c5WMyvA7NvKXWMZv' then 7
        when orderbook = 'HsuEMCBsM7rgpLGuMWRWaLoQunTz23WDJx2zdAQjRLKc' then 7
        when orderbook = 'HPoy6XSE8YWgf3Auwhsv7yQsoPqzDtVuZSvTYKiBXpRr' then 7
        when orderbook = '3i12wSQTRj7trvap2eBLQKK5rqi6sUSJHY49K2H4e7Na' then 7
        when orderbook = 't5g52EvNo6YxJKgzX3s7iJycR7qiHxeJGzNb6QSbK3s' then 7
        when orderbook = '9eycVDHqummRe67o8fjwzUb68jp6PFPcB9WnPMQhD67U' then 7
        when orderbook = '6RNkpufPBopY1Q43NYQN3BbrQetwABvoQbQ9A4fsSchB' then 7
        when orderbook = 'RRS7pN2oTXDwhUURUVc9C829ovWfRrMRWYwNg9LBfE8' then 7
        when orderbook = '8x9DJgMiHumyGr2xXfgNv3wGWNv8P7BjAqz3J2biLp7x' then 7
        when orderbook = 'D8Y5rVyxsS63FHcCp32fMh3FgEgviMwM5fQUrhzuzz2M' then 7
        when orderbook = '12knhhvSRgWMWjc7knNXSvdxrD24FtcasAkJEfUDLnmn' then 7
        when orderbook = '9UTQ5h8bKQCN9vVc6bNMcBJkJ9QAD8hhgRA4jqyZUjKC' then 7
        when orderbook = 'DMhBB2W9EbHQTwrboEeb9aZ9CDNncbYr5JUA2T579i1D' then 7
        when orderbook = 'xE5jC5wwihdY43NwwyhiQgAaJ1AXxPEkDQ7NZMhQ3rw' then 7
        when orderbook = 'FFtBfTXnmsSjrj9yA65VSwtrcLkYVZGkYNTYsigKpQib' then 7
        when orderbook = '6cbkFpoWEFDqVp6FT7bRkyt9Rdhw6u4xyL8QgwJ4SgDZ' then 7
        when orderbook = '7YnHHA9Fzx3m7eUnUESLit85BGcYxi4UEF28sEsas4yp' then 7
        when orderbook = '9TXst811r3mDTCVMstLEXh61MgiFrUtBgE368Xer9fa3' then 7
        when orderbook = '4Mx5YV1tQPFibVn365cSaj2uLGKQHPRUtA6uQgbzukhn' then 7
        when orderbook = 'cZN9zsimprnu3DWip7Qa2tEVuwyCd1bd2utqnPt7YFC' then 7
        when orderbook = '2vFF1NkzJAdivyPdHWoJLuH1YFzpEkVxaadGHnhh6fdx' then 7
        when orderbook = 'FGz6DkTpyxdnfunpH1W6tFTdotFwrUeWLoi8pcrCe1c' then 7
        when orderbook = 'GLq65dWVdFusqRKFSJRdytd1bV69uBw4wRXyj9sMw9FJ' then 7
        when orderbook = '1vnUQPDbY9Dbn4k2mSdPcjD9pp8d3CPfUz9rn7rDG7t' then 7
        when orderbook = '4cJD2YMvgc6rCB64L4dMGwaEs9Wz5jet7mTSC79G32ng' then 7
        when orderbook = 'J97YukrzEvSKdcrqMRmhQnfqs5zfh7s1orVC9YUCLs2c' then 7
        when orderbook = '2SBafwVAQAFf1pFPwpG3DRSBEDWbTUG3qCdzXiJ7uDHP' then 7
        when orderbook = 'DmsPX4ubjnnvHSqbe683ZPafGadLYxQkfDoeXA1swq8m' then 7
        when orderbook = 'BwYhdm89QNNnfUcuzGWha7ghK8DkWWiXkJaGZLajviZ4' then 7
        when orderbook = 'FFz8sNRWmtU6g2YFkE7o93yebfYFyA4AWxXhNEyP7Gor' then 7
        when orderbook = 'GXB5TmTPPNdHWMEMGJndVgSmsynaH7uzZbQWmh35TTun' then 7
        when orderbook = 'GdNTchHXGucsoFrPSVUQPx6q56QvDQho3CrCJt4Ptfi7' then 7
        when orderbook = '4H6tR5zvCxtZ3uG5XiEbLcU1HFxW54hWxq5BBEhsyDwX' then 7
        when orderbook = 'CK3TwfGJptmvqX8Bd4sQroL14dgra88bFpB5h7ZxnCrz' then 7
        when orderbook = '7NFYCvbwxoKJ2PMTTuojGGoS2AxNvua5A4hB6wG25Qty' then 7
        when orderbook = '37k31eUjHyuVm5sr65AYNnhC6xLHQcQZ32YR35WPhD67' then 7
        when orderbook = 'A5wgHA1r9gtnCut8Snkxc5gRy8hKZKwBCZ5dxRrQhtFF' then 7
        when orderbook = 'DipwJSf5XVreDk2TWW8fq2uAaW8sz4m6NCkK14y36DfH' then 7
        when orderbook = '9TFEr6ikTvizV4Pih74RMqoo8jZyESmgyG5SwDedeeXz' then 7
        when orderbook = '7YyG4pmKBtY6H6VFtCYpDSqi87b1bWhCVmvycCA72qht' then 7
        when orderbook = '3TsT9uzbkjf9ma7T4VA767u19UT4VgPYAqidpRdHbU6u' then 7
        when orderbook = 'CXPaozx4ZtbaP8r62WoqkDKiHyF5S1hebbNsMyMTivbw' then 7
        when orderbook = '5V7LcdpxXCVSbHuSi2B1sPKWnQi2gSq9KKCFfn7xa2GN' then 7
        when orderbook = '3tuTPS3XRxvkhzkiEdP3EAqgNyemxUJP5v8gDn1B7DWF' then 7
        when orderbook = '2SQugE6XZwaeE1tu614nvChDbcnyCv9hYrFWBG5KgfhN' then 7
        when orderbook = '9DEsruE3FzBorj1BZYX358VCWU3AEs8D7vcjqcvym1Zu' then 7
        when orderbook = '8RGpeZRz6Rj6SKVJpwgcVwCd6rCoJj7Jv74oUa3V35uy' then 7
        when orderbook = 'FKMbn9wFEinQCwLysh3DnnZEaRXdcArtKLpLjL7K8ARz' then 7
        when orderbook = 'oKDsUv1MsMk6sp4iMjNFGxFaeaLzjRKrcdSwAZRF7ci' then 7
        when orderbook = '73LnqVswvoYderGHGEJgNU7FHAh68z3M6u7QahAesGMe' then 7
        when orderbook = 'GosNdfo9SpzBi8YbRbaVffjKudZPkYdQReNvRoipzyNN' then 7
        when orderbook = 'D9TUZg4kwjga3H9tF3SAvsCKMTXYyrk1g71Q3pXBBrFz' then 7
        when orderbook = '6Ppd7qHuJEX2hATNf4fd3vC3HszaaQhuBd3CTn4eXA8B' then 7
        when orderbook = 'F79KcbRecijxZ6sijJyKq2CiSfxpB5954omirqvgN7Ep' then 7
        when orderbook = '32Bz5m9XGDZ1vLX5CcpzFT3yYVkLXKmufTPQQxTcy1rT' then 7
        when orderbook = 'DVu7PKtQg1Ai1RoNNpZGciSVxAkYCzYGvfagB4gELU4' then 7
        when orderbook = '6G4bL3AjQJTAhXbR1YBGC3AxAdgUMf6Qnv11C25ZvvJp' then 7
        when orderbook = 'DvbqPovrUc95SK9evEW984wkqthUFFNKmt13swbL9UZk' then 7
        when orderbook = 'GhzAKbiZP1qpUj1qnE8tSTUoYYVcRj8j9cZ8XZAcBa8j' then 7
        when orderbook = '62M3qZzqP6YkHwutM8Xn36mrrusBey9f5vjCybZH2USh' then 7
        when orderbook = 'Es62wFv1Fg9mmTkjNq4FpbPH5SJxqBwe1eB6kaxQTBEk' then 7
        when orderbook = 'DLZVjjVKSR7numo3rRLoHrPiPnY4kDkwzTymjb8Ctj9T' then 7
        when orderbook = 'H9w35mGLtwKo1nUa7M44cD1UNLpxAgMCFhipf3sgyz3a' then 7
        when orderbook = '7XBQzM1Uk8uGXezJx3G9P5Yi9RJyU8iYqcTd7hQc6XHF' then 7
        when orderbook = '4y7P9cjEoL6TRLQLsDb1z9qRfnkoejcmVfuAEn2gsBJz' then 7
        when orderbook = 'CyMGmSF36WV1M1SiXBtfgDMv8Cjqrv28n1gFv3CbuJ2D' then 7
        when orderbook = '32hp7NgpbnQ59c2KzC6MzaQrTx7KqjT2JygjW9R13K4u' then 7
        when orderbook = '39gF7D4S3c8ccWGWBhkp5SriDLsevMqvcB6eajsjoHAv' then 7
        when orderbook = '2Av2hiL4KZ9bsNtBX6MLLPHiN6buGCd8AxDqH8XkhWPf' then 7
        when orderbook = '3sX6bcvKpiTeikjjPqUqBBvpEECQtYcy65vG6uYvV3PR' then 7
        when orderbook = 'JD3Lm1WjzUNYtZ7aBnnR2CFnZ1miHi3jEtWg9jCLZ9wH' then 7
        when orderbook = '3H6LSS3XJG8szTzgpNmFjXHhp1ycV4cJsHPwPvHuyZF2' then 7
        when orderbook = 'CUoY6AGuPcEs5RsrAF58wxugVcZKZW4zVujiQqzPaSe' then 7
        when orderbook = 'G9ff7WTpSE3T4oMB2REz9byKCeascTtBuCV3LjbUj52y' then 7
        when orderbook = 'E4aoQpR6Rh1Z2zuLn7NvH91QvUjYuDeUxgnxgTZkMfMi' then 7
        when orderbook = 'EfoFNRjZVLzqfdoMzuMsJ7hsRyEMX3JcVCPdBKaqMCc3' then 7
        when orderbook = 'FMkgsFB79ZnUNikjttHhfyVBfaLMM4eGRajCBAXVNKE' then 7
        when orderbook = 'Ba5FnKkACkvi9b3ULAqVDCD5qsBPXuyx2CQeiscqDfAq' then 7
        when orderbook = '7uzbiN5YoMPcX1m9f5jxdbHqTQ2cvtM1rFgTKCQG2M4M' then 7
        when orderbook = '6YnxQKM9etuf4kRE4EyzuPWVTgUkXSc8Q3DtcFnV6xG8' then 7
        when orderbook = 'Hessa61SpqWr9GxQ4qMNh6GvY7bn7LFKHdKBDXcaXRJq' then 14
        when orderbook = '6HodKRQND3VAvdVz6rjKqtwha4oaq3Qq3brJiDEUpfDF' then 14
        when orderbook = '4n36L1ViLKTDso3p323CQ7DacmEvY3Cisc7LSgJVQ32M' then 14
        when orderbook = 'GnJUAdKKDXBQ6b9eHpgMDvJV7h5Eh18eruuS1XxnU2UG' then 14
        when orderbook = 'BUS4ikKfs4LBKhipH7wmMimqCHPtyX6MtcUM1NuHDtxz' then 14
        when orderbook = '6KxpxjpP66gp6MsEytYBoqEVV1n9xphq64q3JtTihK7y' then 14
        when orderbook = 'DCSH84b2kLsbpq1LuWyqds4o1nh8Z5GU7g7dAb8nvT3r' then 14
        when orderbook = 'Evt5K5ZiCVeC6vR6yQdgqGXA24XbbWtQYiqdGWzgpKLZ' then 14
        when orderbook = '5o92NFAtE9ThSrjsLH6mm3D4G8c2SguGaMmGT51zVSg1' then 14
        when orderbook = '9ySuwXyWvFvWifVfbAYDGGMy7CdndEfX6efGt1yQb6dc' then 14
        when orderbook = 'BrXSwyhUJFkZr6hCE3GMt8mtvmKeuSStrkguhTvuoTtB' then 14
        when orderbook = 'ERWMSg8pYtZJscwkoeSt7JXiuAvWEsQ23U3NGh4PWEqw' then 14
        when orderbook = 'DwXVdkAADLGdorXWmcdTSheNUzMWbDP7wVhXsjD6Lxft' then 14
        when orderbook = 'DyJkskdV9QMevdhhsZncfei8rmUrSK3Z2RYr59Rfrs5D' then 14
        when orderbook = '6UQ7LQPX3ji8y6UrAfwPnwvDDqu5jSEfz4shFMLTanFu' then 14
        when orderbook = '72AAevoeMnpH17YHBKdN9TaBY8T9cbtrthiuNNtsJq56' then 14
        when orderbook = 'AdD9mFkNMn2fvhv4fRsVGeRBjb8yP9nz9krgPjyU7eVY' then 14
        when orderbook = 'A3cH6DSkfbJeGuHrUc4Dqef986xZeZndZ4wSW19ZcJwk' then 14
        when orderbook = '5LidgUiRuzeRkz2sLZ5MMdoxY2tEP7iRvSDNdSquiehb' then 14
        when orderbook = '2wZ5ZbSrgnY3XxQP2uT9UcAxwg9BP4GND14XMGxdhe1U' then 14
        when orderbook = '9yvBVmCDuvgAgkikd2cRoLjk3SiLEhh37KwX3UFi6VtD' then 14
        when orderbook = '4mfucNSKpSCZjfqwbh9eD4Lnda6A7QHRMufKDU7Tufm1' then 14
        when orderbook = '76DE8GPAvYveFfNxd2Ed3jLebAjYnJTbm1hrmPCCg1h9' then 14
        when orderbook = '97fYV3BQafu5Yh7fW7JFKRYKikeA7iaaV2DFpBcUR5q6' then 14
        when orderbook = 'EGA1EWrtmzBLfnRXaWXj9CXLyCjyR8quSgmRzmhV3cmD' then 14
        when orderbook = 'FpnCG7iQBYDnKx7MV7kNgfJ9gV12J4YvCcZTLT5wSwL3' then 14
        when orderbook = 'BGWt9t33tgLsm4yeQG38PFgXq4UKcPWmFQER3i78Cc1g' then 14
        when orderbook = '9xdDe5zKx1GzDMp6QTzq5FS6qHiVa9zXAsAPntq5FHSy' then 14
        when orderbook = '9W6u2xRH27DNU5D67fsy1kGoc86r1ywNThjGgsAPJE52' then 14
        when orderbook = 'C3KXCxNWvTPp3PiYjCMkxyyhaj4W7DYKN2UfH1qH8GVv' then 14
        when orderbook = 'CEfNg5oHAUcRfzmb9HVpNtSbrcZZuJcKtqEprySv7dT7' then 14
        when orderbook = '7wyEhJLksJfEcnQ6RniYM4WVdd4g5GzdZF9nFrQsQdpC' then 14
        when orderbook = '9B1qhF6H91bQy56Q8PyQrneFpvr59pQ44xKXAFE5ifJA' then 14
        when orderbook = 'HLGfjQPHnf7iXv5FhXFMSpDoB5FKqfhPhUd1kzPwgHbA' then 14
        when orderbook = 'DAEmasEQTiJv9AberakvwsBqZr8SpXJGViJcS6J7R2LT' then 14
        when orderbook = 'BYaBedcQhNyYm8cg26bNCNFhQVz1YF3phZYTzupLQJWH' then 14
        when orderbook = 'DDnNPZ5yRDkPomm29PTg5QsGBm3H1oBZqhNX5MEpzV8w' then 14
        when orderbook = 'Gm9pQkbsuLtXKP2z96bCeCBNT4QMXfgXqUT7ZUuvkZxj' then 14
        when orderbook = 'BDbbyDqGmkFPCGhgXhDZdsCfFMTeqdKQuFZY164cBBy4' then 14
        when orderbook = '9QsH7dhhDAWLdiG7wjXJ82Lc96ZPAvnaz7JoVYJwo9jD' then 14
        when orderbook = '53hL3jAQ6eaPLz1py91FffNCrmeD1mUf3fkB7Sk67EHd' then 14
        when orderbook = '2tKtpre8c9TVSUDJFdeTZqAAb1eK6iqWrVXC4fvqqXAD' then 14
        when orderbook = 'CvFve57kYaJbA2wddfEAGBfryGApT4mw42hPX24zndWy' then 14
        when orderbook = 'AYrHeXpjVCxmw5EnpjM6cYDkR2t8WfBGePNhjTYp6g1h' then 14
        when orderbook = 'GgxiN6ktmUSeHAnpBzrpBgGZd6CVNWQTHjXvKP8jsYaF' then 7
        when orderbook = '42FTJrtzUvLXa9Vkn8vufwQQmkiQcyH96YxMg9kERm5U' then 7
        when orderbook = '891ovdLaxVPSu94yxkqupT6zzJcTTctdw3btmmBw4ErT' then 7
        when orderbook = 'GjurAS1MLoFVrsDpew7w1L2aYkTNYovVaDEvfpe7Zodu' then 7
        when orderbook = '3VeLVuwTjpDZ931B8NkfqNa8SMZDXLECRq27oCmFNead' then 7
        when orderbook = 'DLerqrYPAJLetqQ1N65HbKR4eQXR7oBS6oPCGPJKkwxX' then 7
        when orderbook = '4dKe6Hp84f1sVTty7YcBKU3MjKgcR7ChD6QRsS4zUJVM' then 7
        when orderbook = 'FA3u6Lp1WXgLDkeAvheJBpUFpL8HhLLdzHVzmW3dBbgQ' then 7
        when orderbook = 'AaPmJLuH8ChdvNw2tJTe3PWGimt6NFfP5KCuixdqEXqd' then 7
        when orderbook = '22DkHqEN48KpfLu6fvDBYvidB1EtijXzWCzSbLiAYPvS' then 7
        when orderbook = '8kesJY8jweBxkjbqVXKzy9PqXnKtvW3XSVjiwkG6rbHj' then 7
        when orderbook = 'FJ1bTc5SJuahnpAD4F35mrgxpYPZME5cNKt7r5dWzqDp' then 7
        when orderbook = 'CHz5k2FbwAURWWoYv7bmkMBWSvfQg74BsYHFahF7PviR' then 7
        when orderbook = 'EEc88Zud5D9pKSPuMdV2inWV6bdeNWUJ2ZTbzaELsJkU' then 7
        when orderbook = '7XsLCBbmqfGKTX7UEfZiTUE9FVPXv3h3qHy66nT8KTGH' then 7
        when orderbook = 'G6vguQNqRfPAV4XhXkbVWwLjRtQc8BiUt3gbJ31Z7Q2R' then 7
        when orderbook = 'Gn4JnXMwSDjjmyh9HPh5tgJ8ErogudSd6V75LBZmgwUP' then 7
        when orderbook = 'AptPfZ9hNrKrF6BiBqZdbAKUCpyw7zSXa2ye8eaTf5F7' then 7
        when orderbook = '6v8SGwRNLkVsE4j7rG2TeYEKXk9gZDB6PvZ3LkGkDESj' then 7
        when orderbook = '2ZFmLTe1j9S9uDRvDpkU8XRmTpExfgznwnrCPfJWo1p2' then 7
        when orderbook = 'BvgdUKHiuenYQs4NCbM4RWFRHESjmc1wvSYvK1fifRAa' then 7
        when orderbook = '7cvpRJ8XrjV7m9wwsGEDdrMKY32BeiK8LccMoxvEFwhS' then 7
        when orderbook = 'CPL7gQ8JFV22CCdMawCAZocZn9NNe2xq7jeFs2Wy1iVG' then 7
        when orderbook = '3sTKwxkAhGNrdEoor2fXzfqhsnLpWKeqzcGrNzdoVaZT' then 7
        when orderbook = '3Qe7LBduUkHkk96HbMVQ4QSxMVdddVr6gJEoF9m4qB4v' then 7
        when orderbook = '2Gye3WVekwkgZb24FYsTRWDLyAaK6vdPVUVbCsdsFXib' then 7
        when orderbook = 'HFQ9ZJVDfm7f4W2rba8ywjuPStcsBHVnCwwnUCD4xsPL' then 7
        when orderbook = '2YrKeHF8kXce4mA49SaPXXqorq7KNQiVZsUAc9N89s9n' then 7
        when orderbook = '4DqsHhosyoZr5JNvC1fNRzADAhzNZ2yAd9mWgR6NXNZ2' then 7
        when orderbook = '2NX643mZUzpihRepymQ5fh82qSefwDFs6SLTUu15bGdA' then 7
        when orderbook = '41ebo74cm4Ch4r7tAPxit1FD91EDotH9kp63Go68QvPD' then 7
        when orderbook = 'DhxLXegmk3vBQur663PGhfFJFSy2ue3BoBPmpvfvynYm' then 7
        when orderbook = 'Gr1Nehyw7GEsbGcuopU8df61GnbxcVvF4PMx8HtVgTFB' then 7
        when orderbook = 'J5TPDoqb3Brn1QYkeY8nMKMj9rpcAEyEgjQsR1VxehiJ' then 7
        when orderbook = '3V3qgmVHckS8T2x3TU7rWcUGLwaNtmAkqwa5nwxMoUcw' then 7
        when orderbook = '2NHwDH4yTbWexuUugdkJ2yiHzYA5krh4n7zMBPBqQsa4' then 7
        when orderbook = '3AcVWSWhZAUeDdtdkrtix3BEBEGVbh9R5QqNdKTxqGZw' then 7
        when orderbook = 'E1Jv7gtzfYroecMgsEMPQ2Ux1xxx1NK9LhXV1FLyx74b' then 7
        when orderbook = '2D3cgTCUuZNpxUidB15YuPsh536FA1qQVENLTHPjm1TD' then 7
        when orderbook = '4cDZAVcHwc2B3tz8FQc4WBGGRuq5PNG1hS8CGuCTjaC1' then 7
        when orderbook = '6b97SnKasqE8RJHGEp1prG72d9F4p2Zo1wWR95YX3aSn' then 7
        when orderbook = '5tpNwLhfMQ6CDyuzMUzkPPN2ZK7K8SZeW9GgEvGDrgA7' then 7
        when orderbook = 'EiCb5hPLKs8piTjoLzzpug6yrD9QRZPwUUrhZv7kewrd' then 7
        when orderbook = 'HGvQWpznrn2BXrZB7dNjAw6zcAzWAnEkx6wy4gAZfqtE' then 7
        when orderbook = '7akdgq28vtaKrQpDfRPBpMTKoLSYqNGFZv97eFCARNxi' then 7
        when orderbook = '4FLvtHs9dvBawU4q1cvt3JeXHEQmvS6u9KLHzampm1qj' then 7
        when orderbook = '38J5P5LeEBEEGURqfXoH5vaJhR7qBSkH3wtWSEsRauLn' then 3
        when orderbook = '2ruxyAWodkVS4PwQBG4V7VD8MbWhrLuqC2zSeq2bZ1tN' then 7
        when orderbook = 'vsrcyReLwYftBpfLhwv3NrzwADg83ky8ZvopWvMG26Q' then 7
        when orderbook = '9WBLcHxjeZGL23xJuEFbA2ei6bA5MKY4zaa1kCv9i5YE' then 7
        when orderbook = 'HTobSeaciZHwkPHUMMJQrBLCyUfToQJRx8e7N13Giamc' then 7
        when orderbook = 'G1ib5Pghe6GTWvhjXWbnPbg6rFx9PnkxqmSNYwFFFduK' then 7
        when orderbook = '54NNZpbpMaeEj7T8MPFYeFkX8Wppz4gB1AX1poJ84siw' then 7
        when orderbook = 'FavTXAydmuSvZ4coAURiDLfy9vkf7aTvmX8jKNqKcM7c' then 7
        when orderbook = 'BVQVWc8xu4fug8qXkv6oGuyaKyJVbY7wY7bM5XpEtisq' then 7
        when orderbook = 'BqBdwgXjeutjvYyfSepJHaqCR2U9y6xfce9CTLB1HhAt' then 7
        when orderbook = '3G67sRmPpqYa7JaknaUJjz7UV9a1iA7vFUi27qnmKLe3' then 7
        when orderbook = 'BMsncmqTafEwFFTRb6RjrdPRLsuz9BBfosU5TSXod7Vc' then 7
        when orderbook = 'EiLtD9KyPK4ZDv4SEcimoCBrZtLjdQUN1SrJ6oj3p99i' then 7
        when orderbook = '5CgSZz8Zk55art11AeZzwxTA5J9gafRew45GGZtGSbvo' then 7
        when orderbook = '981F8LDt2BJ6RjnYxXPsh7enZaDCxPbS4sxTjgXVjLHW' then 7
        when orderbook = '3WKsXLUij6P5PDLGYabXNyRS11rk2VyQeVcNE4dXcRf3' then 7
        when orderbook = 'EqbvyNTS7yK78PFhcXXg8jPEGVuCLmJtpoSMrPs1S1b1' then 7
        when orderbook = '2u2QH3GiaiNrn8tWxSa3bhjW2oKcpB54C11zwpTe2pXo' then 7
        when orderbook = '4CRfCuKtiFCNpuPgzPW4y9Sk7Wndkqwdr5smGbgCazWd' then 7
        when orderbook = '2Mkk1EfgJHp6Zv2crDJaegut6qtGF6YRbc73GJe45Me1' then 7
        when orderbook = '21HyCXgA6bauXnF6KS5GRpudGqjhG3dT9LzYd4WScDM5' then 7
        when orderbook = 'pGoPxoWEuzTx7VCvdLh958TXuE1s3eGCJE2QfL4k4v1' then 7
        when orderbook = '2EQ5asbmnuageU6n5nJ4qoYgmBkdGs8SVQypBjYuU6rE' then 7
        when orderbook = 'GZSzKWQYk1AUdraRjLHU5rEMr2UP6poxmsh4fZRHb7Ck' then 7
        when orderbook = '9czz3JqWiXp3CrtUzCZzBg5g1FuWmt93mR4PfUbCKcxd' then 7
        when orderbook = '7NhCdk818FWZpofL74xt4YmYDEM1XgF4tWKoAmLkiAWp' then 7
        when orderbook = 'Tvh5FvqyGbP9YKFZXbAbG9BqPLfNpGAGqqwALTQ2nfU' then 7
        when orderbook = 'AqoUHZbtgaHVhuQuqmuPrzUBUZfdzhGoskcWRbjYT54' then 7
        when orderbook = '7kURCzfshNA941ytzD62WYACJjt5xcC4f8jXjzdYLHBT' then 7
        when orderbook = '9oG4WASVGNZQtn1URXKfeqRRDLjUKNMFrRgX561rL9pu' then 7
        when orderbook = 'E4dpdcSZ3Tith1tnVM3Dn7FKBC5AQ3fCSEqyrWW6mQsV' then 7
        when orderbook = '2DjnQGjXwGrrqz4V5vdskCo3uFf2Lw9GQWPh8xXSNiBr' then 7
        when orderbook = '7wRisucVGyTsXSeaY7QLQJmTnN8mhkfVURSPYhyuuHsN' then 7
        when orderbook = '2uciRT3xH9nWZn82x1fP6ggmAT19FjrZ26Cjh56SJeLe' then 7
        when orderbook = 'BnDifWP7t1Noog2XeUbXJx7tqinvSXkdYCXhodCf9T5x' then 7
        when orderbook = '9qtvbWiJ5f5YJXYxaGbTZZGL5uK8TShaQtvEU52XD32u' then 7
        when orderbook = 'AmorqeH5MTVBX5uDTqGY7vdundoXkdcipLnEUYXEtWe' then 7
        when orderbook = 'He8pssZ2hB3pX8ccomxX6eEFXZg76kt52Gr88tkA1AkV' then 7
        when orderbook = 'JWWz38jUYuhNAr7gUyNFv8j3dPGc2dsS5gJqsskwrxX' then 7
        when orderbook = '3fXAopLxFnYFEFaz21PsQXYK6mnqp9TMYb41gKo7f1ZB' then 7
        when orderbook = 'BjH4kSwKJRivpnLiBJBde57qsbAJCKT52gAqjoC8qHre' then 7
        when orderbook = '4LVZfbdA2hMhmks5hgrwsoydzaGgAUkEwQnwppCtPBhU' then 7
        when orderbook = 'Bq7VcRYdHtbGg7uXn79ui94foGJtARgBWr9cqMbD58tF' then 7
        when orderbook = '9nEyXQk5dGYehoV4HgKBB1U5dAJqYEPHjXiSnpiLAafW' then 7
        when orderbook = 'EdLsguh6QQ1uyXuApM6Y9JG98KCZnDBvEPHT4qpFLpuX' then 7
        when orderbook = 'E8xiZ2C6e9opFwiLGs4jcVpCkPqo1YC13CdDKPLLWJDz' then 7
        when orderbook = '13cYY72ucPuns1ed5NhNrMyLAL1vZ9XKVCQ2m4EP2jZd' then 7
        when orderbook = 'CXfVEwMeFBE5CRmjPNk8YDpbttAN6xZGosGbdHVdpzhz' then 7
        when orderbook = '5tVTqXnPfTCRzEQiJxjYWyNUXJnGe6ZXj8A8qVUPDRLn' then 7
        when orderbook = '2qGfdRuJ9Y5PCnriwBG35e9hTDpqrN2mMygJ6SiYMZtN' then 7
        when orderbook = 'Am4k8JdFfYbDZqisSrW1Uo25AXHyN9dyaQBigrqCrZJb' then 7
        when orderbook = 'CFvpMCxB8eL8Mmxsbsyt1Jwh5TbGqATNNWgs9kv66dN2' then 7
        when orderbook = '51sCrYhqWFJ3t5maqesFmoG2ii3QaFfynN6ENDVRxVH7' then 7
        when orderbook = '7aGJk4ASgg89ygToo1eXmuVF3yW2HaEjemUzdysEPDTg' then 7
        when orderbook = '2xH9tWoVqgkvXhMu1erAG3ye1oXi9Q4SWyu76mLjf9fv' then 7
        when orderbook = 'GgxyzwkoqBc2jQaxJmyswgsu8UBXP9QtHeFX3QZjB5vT' then 7
        when orderbook = 'CJumNLxJtcEr8B5tvUTQeKNYC7h7FRwsDSixdDdVQRCk' then 7
        when orderbook = '7fRp4yNsbwcCRdVvtFUrpWJ91LMk1rpDLngjtpEZtizJ' then 7
        when orderbook = '65yUWgvqzUZUeW4wVf83YfCWiwvTwx4awU8suiHU8rVN' then 7
        when orderbook = '8HGVaxkcoFqZHVtuXM4dAzbT8uYtTtdNztHgFP97ZhnW' then 7
        when orderbook = 'CZRBzvVzk3wW6aqc5he5rfqFEc4Zp5EJ7zuQHY7QS5JV' then 7
        when orderbook = '2ef7ghdBQX5cgaBzKFXjf6AhJBVBC29b5yWzp9pKwatq' then 7
        when orderbook = '7abreWiL8jQKiS6wjs48YckKa7Xp9jeTxa8DpUfUQZBq' then 7
        when orderbook = '53D9uvcyXAcMvYKfCeFALW2WaZGQzMqq3cz7gNr5LZ68' then 7
        when orderbook = '3n9JcfWMYNcstLjcYd8iTHz7fMYezAHv3MV3tqq2rfDq' then 7
        when orderbook = 'Fxfm6LYSKLxWUrwGicYqdAWeLFQ5Xfv3PU9SLYKi7FEL' then 7
        when orderbook = 'Fv7x4J61QqLVjFnsbuCKe87swnLiSmagjYV5QkbLdjoa' then 7
        when orderbook = 'FsZ7jZbkntMazYqLpQjbL5GuAf8NagmzsrPd8wTjHUEk' then 7
        when orderbook = '4qQgKNkfyo9H82pLtBqyRZvrnnMtjBkRsqLWL7HDjBfL' then 7
        when orderbook = 'EmVA4ehJ7qAA2WhiJkfwRDxx2efZt5SL5UW36uW4uKYm' then 7
        when orderbook = '69GDW13q3yse8kguLUiJ75u1A5UmGDgaqPAnZxt6XPHs' then 7
        when orderbook = '9FvkMgiKr93g3qo4jGiT2dC2nWSW55dpiDYutxqaiSN5' then 7
        when orderbook = 'Eze3tvw2NdeQPHqhkuQUNsVmRcbrqwDDmpdSbR5sZVmW' then 7
        when orderbook = '4TdKjF8yEQJN7ASKboWK1Pmkw5MUz7rcDL3TddPkTx7y' then 7
        when orderbook = '8HSRxgRHf9eLvzHfQRn4iHddhLVhHemzVnto6TKwCZme' then 7
        when orderbook = '8t6rmu58VHYYzL492b4wQCNBGy2uepPgtwJVYCT9mXxt' then 7
        when orderbook = 'ERFCBa9WFSJxcjPhJdy1pQVt49rN2X1TffHwxH7XmZtq' then 7
        when orderbook = '7y9rpdFbCPNQhCvCVDSjDz7UyjXgm7ApvfEnyWQ9z4Ge' then 14
        when orderbook = 'AyuFtg64HWZRDDMQXEdArr7Jf9orGcNo1L3JcaFP9uqc' then 7
        when orderbook = '59HFw6ftN94UbqQrkfnqeFBe7fJbvbmErsMgZaNeqBCe' then 7
        when orderbook = '5j3Xw1Ub7UWJZfd1NyMi5PDc95FYCrodogtA6f8pY1D3' then 7
        when orderbook = '6u8ub6gtCEc977YafThuqKXWmmbTs1gLids8MNJA5bTY' then 7
        when orderbook = '2PXkTTN1ovfpmP4hXA2mZcSV2vK7w7q7dNK49qEZqi8A' then 7
        when orderbook = 'BfgnP2ZVTJ4YtLvVxrBXL7bBpE73m6GQpcbEW3dZzSXV' then 7
        when orderbook = 'FrQ8Y71sk3n3bX6QzVpP1PawfS6GYfY99yGXXkZvS8zD' then 7
        when orderbook = 'DD9ggAByDwnBuLxVqNYKD1CeLpw8YZ4uuYXUCuL51mXE' then 7
        when orderbook = '6E33kBnFWMVouigBzawsBkyqcvbaqqrLBPzqjPZ5rrCG' then 7
        when orderbook = 'Eoao1bnbxhbopbNP3eXdkJ9BNibRk1eWp1LjxdoUNaKo' then 7
        when orderbook = '2QJT8NLrKj88Fyqi9nz8f2889x8uifTXQ3yChP1zaByP' then 7
        when orderbook = 'GFT8quNKDFT69TARyjecydATqpXgCbhNvYntAh9MCvMh' then 7 else 7 end as n_days
    , ((power(1 + (apy / 100), n_days / 365)) - 1) * amount as interest
    , dateadd('days', n_days, t.loan_take_timestamp) as loan_due_timestamp
    , case when current_timestamp > loan_due_timestamp then 1 else 0 end as is_due
    from offerloans o
    join takeloans t
        on t.escrow = o.escrow
    left join repay r
        on r.escrow = o.escrow
    left join takeloans r2
        on r2.old_escrow = o.escrow
    left join foreclose f
        on f.escrow = o.escrow
    left join solana.core.dim_labels l
        on l.address = t.mint
    left join solana.nft.dim_nft_metadata m
        on m.mint = t.mint
)
-- select count(distinct lender) from t1
, floors as (
    select coalesce(m.nft_collection_name, l.label, 'Other') as collection
    , convert_timezone('UTC', 'America/Los_Angeles', s.block_timestamp) block_timestamp
    , sales_amount
    , row_number() over (partition by collection order by convert_timezone('UTC', 'America/Los_Angeles', s.block_timestamp) desc)
    from solana.nft.fact_nft_sales s
    left join solana.core.dim_labels l
        on l.address = s.mint
    left join solana.nft.dim_nft_metadata m
        on m.mint = s.mint
    where convert_timezone('UTC', 'America/Los_Angeles', s.block_timestamp) >= current_date - 3
    qualify(
        row_number() over (partition by collection order by s.block_timestamp desc) <= 9
    )
)
, median_sale as (
    select collection
    , round(median(sales_amount * 0.95), 2) as cur_floor
    from floors
    where collection != 'Other'
    group by 1
)
, c0 as (
    select i.tx_id
    , i.signers[0]::string as signer
    , convert_timezone('UTC', 'America/Los_Angeles', i.block_timestamp) as block_timestamp
    , convert_timezone('UTC', 'America/Los_Angeles', i.block_timestamp)::date as date
    , i.decoded_instruction
    , i.decoded_instruction:name::string as name
    , di.value:pubkey::string as borrower
    , di.value:name::string as di_name
    , i.decoded_instruction:args:floor::int as floor
    , i.decoded_instruction:args:ltvTerms:ltvBps::int as ltvBps
    , i.decoded_instruction:args:ltvTerms:maxOffer::int * pow(10, -9) as maxOffer
    , i.decoded_instruction:args:terms:apyBps::int as apyBps
    , i.decoded_instruction:args:terms:duration::int as duration
    , i.decoded_instruction:args:terms:principal::int * pow(10, -9) as principal
    , case when i.decoded_instruction:args:borrower is null then 1 else 0 end as is_pool
    from solana.core.fact_decoded_instructions i
    , lateral flatten(input => decoded_instruction:accounts) as di
    where convert_timezone('UTC', 'America/Los_Angeles', i.block_timestamp) >= current_date - 42
        and i.program_id = 'JCFRaPv7852ESRwJJGRy2mysUMydXZgVVhrMLmExvmVp'
)
, private_loans as (
    select distinct di.value:pubkey::string as loanAccount
    , true as is_private_loan
    from c0
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = case when name = 'reborrow' then 'newLoanAccount' else 'loanAccount' end
        and loanAccount is not null
        and is_pool = false
        and name = 'offerLoan'
)
, terms as (
    select di.value:pubkey::string as loanAccount
    , max(coalesce(floor, 0)) as terms_floor
    , max(coalesce(ltvBps, 0)) as terms_ltvBps
    , max(coalesce(maxOffer, 0)) as terms_maxOffer
    , max(coalesce(apyBps, 0)) as terms_apyBps
    , max(coalesce(duration, 0)) as terms_duration
    , max(coalesce(principal, 0)) as terms_principal
    from c0
    , lateral flatten(input => decoded_instruction:accounts) as di
    where coalesce(di.value:name::string, '') = case when name = 'reborrow' then 'newLoanAccount' else 'loanAccount' end
    group by 1
)
, c1 as (
    select c0.*
    , c0.block_timestamp::date as loan_take_date
    , date_trunc('day', loan_take_date) as loan_take_week
    , c0.block_timestamp as loan_take_timestamp
    , c0.tx_id as loan_take_tx
    , di.value:pubkey::string as loanAccount
    from c0
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = case when name = 'reborrow' then 'newLoanAccount' else 'loanAccount' end
        and loanAccount is not null
        and c0.di_name = 'borrower'
        and borrower is not null
)
, repaid as (
    select loanAccount
    , max(tx_id) as repaid_tx
    , max(block_timestamp) as repaid_timestamp
    , max(block_timestamp)::date as repaid_date
    from c1
    where name in ('repay','repay2','sellRepay','listCollateral')
    group by 1
)
, foreclosed as (
    select loanAccount
    , block_timestamp as foreclose_timestamp
    , tx_id as foreclose_tx
    from c1
    where name in ('claim')
)
, c1b as (
    select c1.*
    , di.value:pubkey::string as lender
    from c1
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = case when name = 'reborrow' then 'newLender' else 'lender' end
        and lender is not null
)
, c2 as (
    select c1b.*
    , di.value:pubkey::string as lendAuthority
    from c1b
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = case when name = 'reborrow' then 'newLendAuthority' else 'lendAuthority' end
        and lendAuthority is not null
)
, m0 as (
    select distinct c1.loanAccount
    , di.value:pubkey::string as mint
    from c1
    , lateral flatten(input => decoded_instruction:accounts) as di
    where di.value:name::string = 'mint'
        and mint is not null
)
, c5 as (
    select c2.*
    , case
        when c2.principal > 0 then c2.principal
        when t.terms_principal > 0 then t.terms_principal
        else t.terms_ltvBps * pow(10, -4) * t.terms_floor * pow(10, -9) end as loan_amt
    , m0.mint
    , r.repaid_date
    , r.loanAccount as repaidLoanAccount
    , case when r.loanAccount is null then 0 else 1 end as is_repaid
    , case when f.loanAccount is null then 0 else 1 end as is_foreclosed
    , f.foreclose_tx
    , f.foreclose_timestamp
    -- , loan_take_date
    , is_repaid * loan_amt as loan_repaid_amount
    , (apyBps / 10000) as apy_decimal
    , terms_duration / (60*60*24) as duration_days
    , ((power(1 + apy_decimal, duration_days / 365)) - 1) * loan_amt as interest
    , dateadd('days', duration_days, loan_take_timestamp) as loan_due_timestamp
    , r.repaid_timestamp
    , r.repaid_tx
    , coalesce(r.repaid_timestamp, f.foreclose_timestamp, loan_due_timestamp) as loan_end_timestamp
    , loan_end_timestamp::date as loan_end_date
    , case when loan_due_timestamp < current_timestamp then 1 else 0 end as is_due
    , greatest(is_repaid, is_due) * loan_amt as loan_due_amount
    from c2
    left join terms t
        on t.loanAccount = c2.loanAccount
    left join repaid r
        on r.loanAccount = c2.loanAccount
    left join foreclosed f
        on f.loanAccount = c2.loanAccount
    left join m0
        on m0.loanAccount = c2.loanAccount
)
, forecloses as (
    select c5.foreclose_tx
    , c5.mint
    , min(c5.foreclose_timestamp) as foreclose_timestamp
    from c5
    where c5.foreclose_timestamp is not null
    group by 1, 2
    union
    select t1.foreclose_tx
    , t1.mint
    , min(t1.foreclose_timestamp) as foreclose_timestamp
    from t1
    where t1.foreclose_timestamp is not null
    group by 1, 2
)
, s0 as (
    select f.foreclose_tx
    , s.sales_amount
    , s.tx_id as sales_tx
    , s.seller
    , s.mint
    , convert_timezone('UTC', 'America/Los_Angeles', s.block_timestamp) as block_timestamp
    from forecloses f
    join solana.nft.fact_nft_sales s
        on s.mint = f.mint
        and convert_timezone('UTC', 'America/Los_Angeles', s.block_timestamp) > f.foreclose_timestamp
    where convert_timezone('UTC', 'America/Los_Angeles', s.block_timestamp) >= current_date - 42
        and s.seller in (
            'kcitfabbdz4KFNPn8RJjUEg1rD36SJdFwJg8imo2Rrg'
            , 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC'
        )
    qualify(
        row_number() over (
            partition by f.foreclose_tx
            , f.mint
            order by s.block_timestamp
        ) = 1
    )
)
, sales as (
    select s0.foreclose_tx
    , s0.sales_amount
    , s0.sales_tx
    , s0.seller
    , s0.mint
    , s0.block_timestamp
    , max(case when t.tx_to = s0.seller then coalesce(t.amount, 0) else 0 end) as seller_sales_amt
    from s0
    left join solana.core.fact_transfers t
        on t.block_timestamp >= current_date - 42
        and convert_timezone('UTC', 'America/Los_Angeles', t.block_timestamp) = s0.block_timestamp
        and t.tx_id = s0.sales_tx
        and t.mint like 'So111%'
    group by 1, 2, 3, 4, 5, 6
)
, c6 as (
    select 
    left(date_trunc('minute', c5.block_timestamp)::string, 16) as cur_time
    , loan_take_date
    , loan_take_week
    , loan_take_timestamp
    , repaid_timestamp
    , repaid_tx
    , loan_end_date
    , c5.loan_end_timestamp
    , c5.loan_take_tx
    , lender
    , tx_id
    , s.sales_amount
    , case
        when coalesce(seller_sales_amt, 0) > 0 then seller_sales_amt
        else coalesce(sales_amount * 0.95, ms.manual_sales_amount, 0) end as clean_sales_amount
    , med.cur_floor
    , name
    , principal
    , loan_amt
    , m.mint
    , coalesce(m.nft_collection_name, l.label, 'Other') as collection
    , case
        when is_repaid = 1 then 'Repaid'
        when clean_sales_amount > 0 then 'Sold'
        when is_foreclosed = 1 then 'Foreclosed'
        when loan_due_amount = 0 and cur_floor >= loan_amt then 'Healthy'
        when loan_due_amount = 0 and cur_floor < loan_amt then 'Unhealthy'
        else 'Other' end as status
    , case
        when status in ('Repaid', 'Healthy') then interest
        when status in ('Unhealthy', 'Foreclosed') then cur_floor - loan_amt
        when status = 'Sold' then clean_sales_amount - loan_amt
        else 0 end as profit
    , duration_days
    , interest
    , apyBps / 1000 as apy
    , is_due
    from c5
    left join solana.core.dim_labels l
        on l.address = c5.mint
    left join solana.nft.dim_nft_metadata m
        on m.mint = c5.mint
    left join sales s
        on s.foreclose_tx = c5.foreclose_tx
    left join manual_sales ms
        on ms.loan_take_tx = c5.loan_take_tx
    left join median_sale med
        on med.collection = coalesce(m.nft_collection_name, l.label, 'Other')     
)
, t2 as (
    select t1.*
    , s.sales_amount
    , case
        when coalesce(ms.manual_sales_amount, 0) > 0 then ms.manual_sales_amount
        when coalesce(seller_sales_amt, 0) > 0 then seller_sales_amt
        else coalesce(sales_amount * 0.95, 0) end as clean_sales_amount
    , s.sales_tx
    , s.seller
    , s.seller_sales_amt
    , m.cur_floor
    , p0.cur_sol_price
    , coalesce(repaid_timestamp, foreclose_timestamp, t1.loan_due_timestamp) as loan_end_timestamp
    , loan_end_timestamp::date as loan_end_date
    , date_trunc('day', loan_end_date) as loan_end_week
    -- , date_trunc('week', loan_take_date) as loan_take_week
    , case
        when is_repaid = 1 or r.loan_take_tx is not null then 'Repaid'
        when clean_sales_amount > 0 then 'Sold'
        when greatest(is_due, is_foreclosed) = 1 then 'Foreclosed'
        when is_due = 0 and cur_floor >= amount then 'Healthy'
        when is_due = 0 and cur_floor < amount then 'Unhealthy'
        else 'Other' end as status
    , case
        when status in ('Repaid', 'Healthy') then interest
        when status in ('Unhealthy', 'Foreclosed') then cur_floor - amount
        when status = 'Sold' then clean_sales_amount - amount
        else 0 end as profit
    -- , case
    --     when seller_sales_amt > 0 then seller_sales_amt - amount
    --     when sales_amount > 0 then (sales_amount * 0.95) - amount
    --     when t1.is_foreclosed = 1 then coalesce((cur_floor * 0.95) - amount, 0)
    --     when is_repaid = 0 and amount + interest > m.cur_floor then m.cur_floor - amount
    --     else interest end as profit
    , sum(profit) over () as tot_profit
    , sum(1) over () as n_loans_taken
    , avg(case when profit > 0 then 1 else 0 end) over () as pct_profitable
    , avg(case when t1.is_foreclosed = 1 then 1 else 0 end) over () as pct_default
    , avg(case when t1.is_foreclosed = 1 then profit else 0 end) over () as default_profit
    , avg(case when t1.is_foreclosed = 1 then 0 else profit end) over () as repaid_profit
    , sum(amount) over () as tot_loan_volume
    , profit * cur_sol_price as profit_usd
    , tot_profit * cur_sol_price as tot_profit_usd
    , amount * cur_sol_price as loan_volume_usd
    , tot_loan_volume * cur_sol_price as tot_loan_volume_usd
    from t1
    left join manual_repaids r
        on r.loan_take_tx = t1.loan_take_tx
    left join sales s
        on s.foreclose_tx = t1.foreclose_tx
        and t1.is_foreclosed = 1
    left join manual_sales ms
        on ms.loan_take_tx = t1.loan_take_tx
    left join median_sale m
        on m.collection = t1.collection
    join p0
        on true
)
, combined as (
    select lender
    , loan_take_date
    , loan_take_week
    , loan_take_timestamp
    , repaid_timestamp
    , repaid_tx
    , collection
    , mint
    , round(cur_floor, 2) as cur_floor
    , loan_take_tx
    , loan_end_date
    , loan_end_timestamp
    , round(amount, 4) as principal
    , round(interest, 4) as interest
    , round(profit, 4) as profit
    , status
    , round(clean_sales_amount, 2) as clean_sales_amount
    , 'sharky' as platform
    from t2 
    union all
    select lender
    , loan_take_date
    , loan_take_week
    , loan_take_timestamp
    , repaid_timestamp
    , repaid_tx
    , collection
    , mint
    , round(cur_floor, 2) as cur_floor
    , loan_take_tx
    , loan_end_date
    , loan_end_timestamp
    , round(principal, 4) as principal
    , round(interest, 4) as interest
    , round(profit, 4) as profit
    , status
    , round(clean_sales_amount, 2) as clean_sales_amount
    , 'citrus' as platform
    from c6
    where name in (
        'borrow'
        , 'reborrow'
        , 'mortgage'
    )
)
, f0 as (
    select platform
    , lender
    , loan_take_week
    , sum(principal) as tot_principal
    , sum(case when platform = 'sharky' then principal else 0 end) as sharky_principal
    , sum(case when platform = 'citrus' then principal else 0 end) as citrus_principal
    from combined
    group by 1, 2, 3
)
-- select * from f0
, labels as (
    select lender
    , sum(sharky_principal) as sharky_principal
    , sum(citrus_principal) as citrus_principal
    from f0
    group by 1
)
, labels_ranked as (
    select *
    , row_number() over (order by sharky_principal desc) as rk_sharky
    , row_number() over (order by citrus_principal desc) as rk_citrus
    from labels
)
, f1 as (
    select f0.*
    , lr.rk_sharky
    , lr.rk_citrus
    , case when lr.rk_sharky <= 4 or f0.lender in ('runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC') then left(f0.lender, 6) else 'Other' end as lender_group_sharky
    , case when lr.rk_citrus <= 4 or f0.lender in ('kcitfabbdz4KFNPn8RJjUEg1rD36SJdFwJg8imo2Rrg') then left(f0.lender, 6) else 'Other' end as lender_group_citrus
    from f0
    join labels_ranked lr
        on f0.lender = lr.lender
)
, f2 as (
    select lender_group_sharky as lender_group
    , loan_take_week
    , sum(sharky_principal) as sharky_principal
    , sum(0) as citrus_principal
    from f1
    group by 1, 2
    union all
    select lender_group_citrus as lender_group
    , loan_take_week
    , sum(0) as sharky_principal
    , sum(citrus_principal) as citrus_principal
    from f1
    group by 1, 2
)
, f3 as (
    select lender_group
    , loan_take_week
    , sum(sharky_principal) as sharky_principal
    , sum(citrus_principal) as citrus_principal
    from f2
    where loan_take_week >= current_date - 7
    group by 1, 2
    order by 1, 2
)
select lender_group
, loan_take_week
, case when f3.sharky_principal > 0 then sharky_principal else null end as sharky_principal
, case when f3.citrus_principal > 0 then citrus_principal else null end as citrus_principal
, sum(case when lender_group like 'runpZT%' then sharky_principal else 0 end) over (partition by loan_take_week, lender_group) as my_sharky_principal
, sum(case when lender_group like 'kcitfa%' then f3.citrus_principal else 0 end) over (partition by loan_take_week, lender_group) as my_citrus_principal
, sum(f3.sharky_principal) over (partition by loan_take_week) as weekly_sharky_principal
, sum(f3.citrus_principal) over (partition by loan_take_week) as weekly_citrus_principal
, round(100 * my_sharky_principal / greatest(1, weekly_sharky_principal)) as weekly_pct_sharky_principal
, round(100 * my_citrus_principal / greatest(1, weekly_citrus_principal)) as weekly_pct_citrus_principal
from f3
 

 
-- rank each query by # of forks, # of views, and time in top 40
select *
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.users u
    on u.id = q.created_by_id


-- get dashboards with only solana queries
with dash_queries as (
    select d.id as dashboard_id
    , d.title
    , u.username
    , q.name
    , q.id as query_id
    , case when parent_query_id is null then 0 else 1 end as is_fork
    , count(distinct case when t.name = 'solana' then q.id else null end) as n_solana_queries
    , count(distinct case when t.name = 'solana' then null else q.id end) as n_non_solana_queries
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
        on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and t.type = 'project'
    where q.created_at >= current_date - 365
    group by 1, 2, 3, 4, 5, 6
)
, invalid_dashboards as (
    select distinct dashboard_id
    from dash_queries
    where n_non_solana_queries > 0
)
, valid_queries as (
    select d.* 
    from dash_queries d
    left join invalid_dashboards i
        on i.dashboard_id = d.dashboard_id
    where i.dashboard_id is null
)
, valid_dashboards as (
    select distinct dashboard_id
    from valid_queries
)
, dashboard_score as (
    select v.dashboard_id
    , min(ranking_trending) as top_ranking
    , sum(case when ranking_trending <= 10 then 1 else 0 end) as n_hours_in_top_10
    , sum(case when ranking_trending <= 40 then 1 else 0 end) as n_hours_in_top_40
    , sum(case when ranking_trending <= 100 then 1 else 0 end) as n_hours_in_top_100
    from valid_dashboards v
    join bi_analytics.snapshots.hourly_dashboard_rankings r
        on r.dashboard_id = v.dashboard_id
    where dbt_updated_at >= current_date - 365
    group by 1
)
, query_score as (
    select q1.created_by_id as user_id
    , q1.id as query_id
    , count(distinct q2.created_by_id) as n_forks
    from bi_analytics.velocity_app_prod.queries q1
    left join bi_analytics.velocity_app_prod.queries q2
        on q2.parent_query_id = q1.id
        and q2.created_by_id != q1.created_by_id
    group by 1, 2
)
, view_score as (
    select q.id as query_id
    , count(distinct p.anonymous_id) as n_viewers
    from bi_analytics.velocity_app_prod.queries q
    join bi_analytics.gumby.pages p
        on right(p.context_page_tab_url, 36) = q.id
    where p.timestamp >= current_date - 365
    group by 1
    order by 2 desc
)
, t0 as (
    select v.*
    , coalesce(q.n_forks, 0) as n_forks
    , coalesce(d.top_ranking, 100) as top_ranking
    , coalesce(d.n_hours_in_top_10, 100) as n_hours_in_top_10
    , coalesce(d.n_hours_in_top_40, 100) as n_hours_in_top_40
    , coalesce(d.n_hours_in_top_100, 100) as n_hours_in_top_100
    , coalesce(vs.n_viewers, 0) as n_viewers
    from valid_queries v
    left join dashboard_score d
        on d.dashboard_id = v.dashboard_id
    left join query_score q
        on q.query_id = v.query_id
    left join view_score vs
        on vs.query_id = v.query_id
)
, t1 as (
    select *
    , case when n_forks > 10 then 10 + pow(n_forks - 10, 0.5) else n_forks end * 2 as n_forks_score
    , greatest(0, 10 - top_ranking) as top_ranking_score
    , least(10, n_viewers / 20) as n_viewers_score
    , least(10, n_hours_in_top_10 / 20) as n_hours_in_top_10_score
    , least(10, n_hours_in_top_40 / 40) as n_hours_in_top_40_score
    , n_forks_score + n_viewers_score + top_ranking_score + n_hours_in_top_10_score + n_hours_in_top_40_score as total_score
    , row_number() over (partition by dashboard_id order by total_score desc) as dash_rk
    from t0
    having row_number() over (partition by dashboard_id order by is_fork, total_score desc) <= 2
)
, t2 as (
    select *
    from t1
    having row_number() over (partition by username order bytotal_score desc) <= 250
)
select *
from t2
order by total_score desc
limit 250


select p.*
, u.username
from bi_analytics.gumby.pages p
left join bi_analytics.gumby.users u
    on u.id = p.user_id
where context_page_tab_url ilike '%8cd0bfd8-e5dc-4b3b-ba54-89ded55ba6d6%'
order by timestamp desc
limit 100000



SELECT t.name
, t.type
, count(1) as n_queries
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
    on q.id = qtt.A
join bi_analytics.velocity_app_prod.tags t
    on qtt.B = t.id
where q.created_at >= current_date - 30
group by 1, 2
order by 3 desc

SELECT t.name
, t.type
, count(1) as n_queries
from bi_analytics.velocity_app_prod.dashboards d
join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_tags dtt
    on d.id = dtt.A
join bi_analytics.velocity_app_prod.tags t
    on qtt.B = t.id
where q.created_at >= current_date - 30
    and (
        t.name ilike '%bonk%'
    )
group by 1, 2
order by 3 desc




select coalesce(d.id, q.id) as dashboard_id
, coalesce(d.title, q.name) as title
, u.id as user_id
, u.username
, q.name
, q.id as query_id
, q.statement
, q.created_at
, case when parent_query_id is null then 0 else 1 end as is_fork
, count(distinct case when t.name = 'solana' then q.id else null end) as n_solana_queries
, count(distinct case when t.name = 'solana' then null else q.id end) as n_non_solana_queries
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.users u
    on u.id = q.created_by_id
left join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
    on dtq.B = q.id
left join bi_analytics.velocity_app_prod.dashboards d
    on d.id = dtq.A
left join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
    on q.id = qtt.A
left join bi_analytics.velocity_app_prod.tags t
    on qtt.B = t.id
    and t.type = 'project'
where q.created_at >= current_date - 365
group by 1, 2, 3, 4, 5, 6, 7, 8, 9



-- wallets who bought hot tokens at under 50% their current value
WITH token_list as (
  SELECT
    ARRAY_CONSTRUCT(
      'HeLp6NuQkmYB4pYWo2zYs22mESHXPQYzXbB8n4V98jwC',
      '9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump',
      'KENJSUYLASHUMfHyy5o4Hp2FdNqZg1AsUPhfH2kYvEP',
      'CzLSujWBLFsSjncfkh59rUFqvafWcY5tzedWJSuypump',
      'Dfh5DzRgSvvCFDoYc2ciTkMrbDfRKybA4SoFbPmApump',
      '8x5VqbHA8D7NkD52uNuS5nnt3PwA8pLD34ymskeSo2Wn',
      '61V8vBaqAGMpgDQi4JcAwo1dmBGHsyhzodcPqnEVpump',
      'GJAFwWjJ3vnTsrQVabjBVK2TYB1YtRCQXRDfDgUnpump',
      'DKu9kykSfbN5LBfFXtNNDPaX35o4Fv6vJ9FKk7pZpump',
      'HNg5PYJmtqcmzXrv6S9zP1CDKk5BgDuyFBxbvNApump',
      'AxGAbdFtdbj2oNXa4dKqFvwHzgFtW9mFHWmd7vQfpump',
      '9DHe3pycTuymFk4H4bbPoAJ4hQrr2kaLDF6J6aAKpump',
      'FQ1tyso61AH1tzodyJfSwmzsD3GToybbRNoZxUBz21p8',
      'Hjw6bEcHtbHGpQr8onG3izfJY5DJiWdt7uk2BfdSpump',
      '5voS9evDjxF589WuEub5i4ti7FWQmZCsAsyD5ucbuRqM',
      '92cRC6kV5D7TiHX1j56AbkPbffo9jwcXxSDQZ8Mopump',
      '63LfDmNb3MQ8mw9MtZ2To9bEA2M71kZUUGq5tiJxcqj9',
      '2qEHjDLDLbuBgRYvsxhc5D6uDWAivNFZGan56P1tpump',
      '74SBV4zDXxTRgv1pEMoECskKBkZHc2yGPnc7GYVepump',
      'eL5fUxj2J4CiQsmW85k5FG9DvuQjjUoBHoQBi2Kpump',
      'A8C3xuqscfmyLrte3VmTqrAq8kgMASius9AFNANwpump',
      'GekTNfm84QfyP2GdAHZ5AgACBRd69aNmgA5FDhZupump',
      '7XJiwLDrjzxDYdZipnJXzpr1iDTmK55XixSFAa7JgNEL',
      'HNg5PYJmtqcmzXrv6S9zP1CDKk5BgDuyFBxbvNApump',
      'oraim8c9d1nkfuQk9EzGYEUGxqL3MHQYndRw1huVo5h',
      '7D1iYWfhw2cr9yBZBFE6nZaaSUvXHqG5FizFFEZwpump',
      '5QS7RcHfGUa2ZtrovPvEJMB9coqroiT7H48dPSwFpump',
      'Hjw6bEcHtbHGpQr8onG3izfJY5DJiWdt7uk2BfdSpump',
      '9DHe3pycTuymFk4H4bbPoAJ4hQrr2kaLDF6J6aAKpump',
      '5voS9evDjxF589WuEub5i4ti7FWQmZCsAsyD5ucbuRqM',
      '9doRRAik5gvhbEwjbZDbZR6GxXSAfdoomyJR57xKpump',
      '3N2ETvNpPNAxhcaXgkhKoY1yDnQfs41Wnxsx5qNJpump',
      'CboMcTUYUcy9E6B3yGdFn6aEsGUnYV6yWeoeukw6pump',
      'WEmjxPMGXEW1Nvc4rCgRKiWHj1H1tvhPsKMw2yvpump',
      '6d5zHW5B8RkGKd51Lpb9RqFQSqDudr9GJgZ1SgQZpump',
      '8i51XNNpGaKaj4G4nDdmQh95v4FKAxw8mhtaRoKd9tE8',
      '98mb39tPFKQJ4Bif8iVg9mYb9wsfPZgpgN1sxoVTpump',
      'AuLFCTA8V8katsgpkFsezQtkHodJby5M4KB2VryTpump',
      '89q6aHpZ1fXhuwpnrBgqmCvuAX4GaCrRPQNp5xVHpump',
      'B8hCuoikV9gLeuwmTyhNdLbPnb5k3P77Q7WTtEM7pump',
      '866Sh46xjH7cW7aW18tBUmGm3xh6EzGTk1Li7YbbmqJr',
      'D2dzg6bw6BmniXZAXjR3z7bEVdsKzgu5XvkabnYCZhqw',
      'AxGAbdFtdbj2oNXa4dKqFvwHzgFtW9mFHWmd7vQfpump',
      '9DHe3pycTuymFk4H4bbPoAJ4hQrr2kaLDF6J6aAKpump',
      '79yTpy8uwmAkrdgZdq6ZSBTvxKsgPrNqTLvYQBh1pump',
      '9doRRAik5gvhbEwjbZDbZR6GxXSAfdoomyJR57xKpump',
      'FVdo7CDJarhYoH6McyTFqx71EtzCPViinvdd1v86Qmy5'
    ) as tokens
),
current_prices as (
  SELECT
    token_address,
    price as current_price
  from
    solana.price.ez_prices_hourly
  WHERE
    hour >= CURRENT_TIMESTAMP - INTERVAL '6 hour'
    and token_address in (
      SELECT
        VALUE
      from
        TABLE(
          FLATTEN(
            input =>
            SELECT
              tokens
            from
              token_list
          )
        )
    )
qualify(row_number() over (partition by token_address order by hour desc) = 1)
),
trades as (
  SELECT
    swapper,
    swap_to_mint as token,
    MIN(swap_from_amount_usd / swap_to_amount) as min_price,
    COUNT(*) as trades_per_token
  from
    solana.defi.ez_dex_swaps
  WHERE
    block_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
    and greatest(swap_from_amount_usd, swap_to_amount_usd) > 1000
    and swap_to_amount > 0
    and swap_from_amount > 0
    and swap_to_mint in (
      SELECT
        VALUE
      from
        TABLE(
          FLATTEN(
            input =>
            SELECT
              tokens
            from
              token_list
          )
        )
    )
  group by
    1,
    2
),
wallet_stats as (
  SELECT
    swapper,
    COUNT(*) as total_trades,
    COUNT(DISTINCT swap_to_mint) as unique_tokens_bought
  from
    solana.defi.ez_dex_swaps
  WHERE
    block_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
  group by
    1
  HAVING
    total_trades < 1500
    and unique_tokens_bought <= 250
)
, t0 as (
    SELECT
    t.swapper as wallet,
    ws.total_trades,
    ws.unique_tokens_bought,
    COUNT(
        DISTINCT case
        when t.min_price <= cp.current_price * 0.5 then t.token
        end
    ) as tokens_bought_below_50pct
    , sum(case when t.min_price <= cp.current_price * 0.1 then 4  when t.min_price <= cp.current_price * 0.2 then 3 when t.min_price <= cp.current_price * 0.35 then 2  when t.min_price <= cp.current_price * 0.5 then 1 else 0 end) as pts
    , pts / ((greatest(10, unique_tokens_bought) + (greatest(total_trades, 20) / 5)) + 10) as score
    from
    trades t
    join wallet_stats ws on t.swapper = ws.swapper
    join current_prices cp on t.token = cp.token_address
    group by
    1,
    2,
    3
    HAVING
    tokens_bought_below_50pct > 0
    order by
    score desc limit 1000;
)
, latest_balances as (
  SELECT
    owner as wallet,
    mint,
    balance,
    block_timestamp
  from
    solana.core.fact_token_balances
  WHERE
    block_timestamp >= current_date - 30
    and owner in (select wallet from t0) qualify(
      row_number() over (
        partition by mint, owner
        order by
          block_timestamp desc
      )
    ) = 1
),
latest_sol as (
  SELECT
    account_address as wallet,
    balance as sol_balance
  from
    solana.core.fact_sol_balances
  WHERE
    block_timestamp >= current_date - 30
    and account_address in (select wallet from t0)
     qualify(
      row_number() over (
        partition by account_address
        order by
          block_timestamp desc
      )
    ) = 1
), latest_prices as (
  SELECT
    token_address,
    price
  from
    solana.price.ez_prices_hourly
  WHERE
    hour >= current_date - 1 qualify(
      row_number() over (
        partition by token_address
        order by
          hour desc
      )
    ) = 1
)
, wallet_value as (
    SELECT
        wallet,
        sum(balance_usd) as total_usd_value
    from
    (
        SELECT
        lb.wallet,
        lb.mint,
        lb.balance * lp.price as balance_usd
        from
        latest_balances lb
        join latest_prices lp on lb.mint = lp.token_address
        UNION
        SELECT
        wallet,
        'So11111111111111111111111111111111111111111' as mint,
        sol_balance * price as balance_usd
        from
        latest_sol ls
        join latest_prices lp on lp.token_address = 'So11111111111111111111111111111111111111112'
    )
    group by 1
)
select t0.*
, v.total_usd_value
from t0
join wallet_value v
    on v.wallet = t0.wallet


select *
from solana.defi.ez_dex_swaps
where block_timestamp >= current_date - 1
limit 10





with pc0 as (
    select token_address
    , hour
    , price
    , lag(price, 1) over (
        partition by token_address
        order by hour
    ) as prv_price
    , price / prv_price as ratio
    from solana.price.ez_prices_hourly p
    where hour >= current_date - 1
        and is_imputed = false
), pc1 as (
    select hour::date as date
    , token_address
    from pc0
    where ratio >= 10
    or ratio <= 0.1
), p0 as (
    select p.token_address as mint
    , DATE_TRUNC('hour', p.hour) as hour
    , avg(price) as price
    , MIN(price) as min_price
    from solana.price.ez_prices_hourly p
    left join pc1
        on pc1.token_address = p.token_address
        and pc1.date = p.hour::date
    where hour >= current_date - 1
        and pc1.date is null
        and is_imputed = FALSE
        and price < 1000000
    group by 1, 2
), p1 as (
    select p.token_address as mint
    , DATE_TRUNC('day', hour) as date
    , avg(price) as price
    , MIN(price) as min_price
    from solana.price.ez_prices_hourly p
    left join pc1
        on pc1.token_address = p.token_address
        and pc1.date = p.hour::date
    where hour >= current_date - 1
        and pc1.date is null
        and is_imputed = FALSE
        and price < 1000000
    group by 1, 2
), p2 as (
    select p.token_address as mint
    , DATE_TRUNC('week', hour) as week
    , avg(price) as price
    , MIN(price) as min_price
    from solana.price.ez_prices_hourly p
    left join pc1
        on pc1.token_address = p.token_address
        and pc1.date = p.hour::date
    where hour >= current_date - 1
        and pc1.date is null
        and is_imputed = FALSE
        and price < 1000000
    group by 1, 2
)
, cur_price as (
    select mint
    , price as cur_price
    from p0
    qualify(
        row_number() over (partition by mint order by hour desc) = 1
    )
)
, cur_swap_price as (
    select swap_to_mint
    , swap_to_amount
    , swap_from_mint
    , swap_from_amount
    , p.cur_price * s.swap_from_amount / s.swap_to_amount as cur_calc_price
    from solana.defi.fact_swaps s
    join cur_price p
        on p.mint = s.swap_from_mint
    where block_timestamp >= current_date - 1
        and succeeded
        and swap_to_amount > 0
        and swap_from_amount > 0
    qualify(
        row_number() over (partition by swap_to_mint order by block_timestamp desc) = 1
    )
)
, swap_price as (
    select swap_to_mint
    , swap_to_amount
    , swap_from_mint
    , swap_from_amount
    , p.price * s.swap_from_amount / s.swap_to_amount as calc_price
    , p.hour
    from solana.defi.fact_swaps s
    join p0 p
        on p.mint = s.swap_from_mint
        and p.hour = date_trunc('hour', s.block_timestamp)
    where block_timestamp >= current_date - 1
        and succeeded
        and swap_to_amount > 0
        and swap_from_amount > 0
    qualify(
        row_number() over (partition by swap_to_mint, hour order by block_timestamp desc) = 1
    )
)
, t0 as (
    select s.tx_id
    , s.swapper
    , s.block_timestamp
    , s.swap_from_mint
    , s.swap_to_mint
    , s.swap_from_amount
    , s.swap_to_amount
    from solana.defi.fact_swaps s
    where s.block_timestamp >= current_date - 1
        and succeeded
        and swap_to_amount > 0
        and swap_from_amount > 0
        and swapper = 'runkuDMGfyVapyBXKJQyAGoMgQxwKU2Ba2qZy9BFj1n'
)
, t0b as (
    select distinct t0.*
    , coalesce(p0f.price, p0f2.calc_price, p1f.price, p2f.price) as f_price
    , coalesce(p0t.price, p0t2.calc_price, p1t.price, p2t.price) as t_price
    , t0.swap_from_amount * f_price as f_usd
    , t0.swap_to_amount * t_price as t_usd
    , case when t0.swap_to_mint in (
        'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
        , 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
        , 'So11111111111111111111111111111111111111112'
    ) then t_usd when t0.swap_from_mint in (
        'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
        , 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
        , 'So11111111111111111111111111111111111111112'
    ) then f_usd else least(f_usd, t_usd) end as usd_value
    from t0
    left join p0 p0f
        on left(p0f.mint, 16) = left(t0.swap_from_mint, 16)
        and p0f.hour = date_trunc('hour', t0.block_timestamp)
    left join swap_price p0f2
        on left(p0f2.swap_to_mint, 16) = left(t0.swap_from_mint, 16)
        and p0f2.hour = date_trunc('hour', t0.block_timestamp)
    left join p1 p1f
        on left(p1f.mint, 16) = left(t0.swap_from_mint, 16)
        and p1f.date = date_trunc('day', t0.block_timestamp)
    left join p2 p2f
        on left(p2f.mint, 16) = left(t0.swap_from_mint, 16)
        and p2f.week = date_trunc('week', t0.block_timestamp)
    left join p0 p0t
        on left(p0t.mint, 16) = left(t0.swap_to_mint, 16)
        and p0t.hour = date_trunc('hour', t0.block_timestamp)
    left join p1 p1t
        on left(p1t.mint, 16) = left(t0.swap_to_mint, 16)
        and p1t.date = date_trunc('day', t0.block_timestamp)
    left join p2 p2t
        on left(p2t.mint, 16) = left(t0.swap_to_mint, 16)
        and p2t.week = date_trunc('week', t0.block_timestamp)
    left join swap_price p0t2
        on left(p0t2.swap_to_mint, 16) = left(t0.swap_to_mint, 16)
        and p0t2.hour = date_trunc('hour', t0.block_timestamp)
)
, labels as (
    select token_address as mint
    , symbol
    , name
    from solana.price.ez_asset_metadata
    qualify(
        row_number() over (partition by token_address order by modified_timestamp desc) = 1
    )
)
, t1 as (
    select datediff('minutes', block_timestamp, current_timestamp) / 60.0 as hours_ago
    , case when swap_from_mint = 'So11111111111111111111111111111111111111112' then 'buy' else 'sell' end as trade_type
    , case when trade_type = 'buy' then swap_to_mint else swap_from_mint end as trade_mint
    , case when trade_type = 'buy' then swap_to_amount / swap_from_amount else swap_from_amount / swap_to_amount end as price
    , m.name as token
    , upper(m.symbol) as symbol
    , t0b.*
    from t0b
    left join labels m
        on m.mint = trade_mint
    order by block_timestamp desc
)
, buy_prices as (
    select trade_mint
    , price as buy_price
    , block_timestamp
    from t1
    where trade_type = 'buy'
)
, t2 as (
    select t1.*
    , b.buy_price
    , b.buy_price / t1.price as ratio
    , case when ratio > 1 then 1 else 0 end as is_profitable
    from t1
    left join buy_prices b
        on b.trade_mint = t1.trade_mint
        and b.block_timestamp < t1.block_timestamp
        and t1.trade_type = 'sell'
    qualify(
        row_number() over (partition by t1.tx_id order by b.block_timestamp desc) = 1
    )
)
select *
from t2
order by block_timestamp desc

select *
from solana.nft.fact_nft_sales
where block_timestamp >= '2024-01-01'
    and mint = '6YEkab95jB9GP64q5xRFT2uZXUz1UkJJaJJ9Yr8Bsbhq'

with t0 as (
    select *
    , case when swap_from_mint like 'So1111%' then 'buy' else 'sell' end as action
    , case when action = 'buy' then swap_to_mint else swap_from_mint end as mint
    , case when action = 'buy' then swap_from_amount else swap_to_amount end as sol_amount
    , min(block_timestamp) over (partition by mint) as start_timestamp
    from solana.defi.fact_swaps
    where block_timestamp >= current_date - 7
        and (
            swap_from_mint like 'So1111%'
            or swap_to_mint like 'So1111%'
        )
)
, t1 as (
    select mint
    , count(distinct swapper) as n_swappers
    , sum(sol_amount) as sol_amount
    from t0
    where start_timestamp >= current_date - 6
        and block_timestamp <= dateadd('hours', 24, start_timestamp)
    group by 1
)
, t2 as (
    select t1.*
    , m.symbol
    , m.name
    from t1
    left join solana.price.ez_asset_metadata m
        on m.token_address = t1.mint
)
select *
from t2
order by sol_amount desc
limit 10000



with rk_hist0 as (
    select d.id as dashboard_id
    , d.title
    , u.username
    , date_trunc('hour', dbt_updated_at) as hour
    , avg(hr.ranking_trending) as rk0
    from bi_analytics.snapshots.hourly_dashboard_rankings hr
    join bi_analytics.velocity_app_prod.dashboards d
        on d.id = hr.dashboard_id
    join bi_analytics.velocity_app_prod.users u
        on u.id = d.created_by_id
    where dbt_updated_at >= '2024-09-01'
        and dbt_updated_at < '2025-01-01'
        and coalesce(u.role, '') <> 'internal'
        and not u.username in (
            'Polaris_9R','dsaber','flipsidecrypto','metricsdao','drethereum','Orion_9R','sam','forgash','nftchance__','danner'
        )
        and hr.ranking_trending <= 100
    group by 1, 2, 3, 4
), rk_hist1 as (
    select *
    , row_number() over (partition by hour order by rk0 asc) as rk
    from rk_hist0
), rk_hist2 as (
    select dashboard_id
    , title
    , username
    , sum(41 - rk) as score
    , sum(1) as n_hours_in_top_40
    , sum(case when rk <= 10 then 1.5 when rk <= 40 then 1 else 0 end) as n_hours_in_top_40
    from rk_hist1 r
    where rk <= 40
    group by 1, 2, 3
)
select *
from rk_hist2
order by score desc


select *
from solana.nft.fact_nft_sales
where block_timestamp::date = '2024-06-05'::date
    and tx_id = '2YmD4bK7HTHAbck6Z3jnDp5UqTfW9kwCWTgASnRtacdhtaz37hFsoTJ93m3Rpd6a7PpueztvFpnWnPwY9QLS41BW'

select concat(day_, '-', replace(replace(lower(subject), ' ', '-'), '\'', '')) as id
, date_part('epoch', to_timestamp(day_)) as createdAt
, concat(subject, ': ', summary) as text
from datascience_dev.trending_topics.ai_summary 
where day_ >= current_date - 7
limit 10000
'



select m.twitter_id
, ta.twitter_handle
, ta.account_type
, sum(coalesce(score, 0)) as score
from bi_analytics.twitter.twitter_accounts ta
left join bi_analytics.silver.user_community_scores_monthly m
    on m.twitter_id = ta.twitter_id
    and month >= '2024-10'
where ecosystems::string ilike 'solana%'
group by 1, 2, 3
order by 4 desc

select * 
from bi_analytics.velocity_app_prod.visualizations
where id = 'f619e0f5-1691-4f54-95da-884852d53aaa'

select id
, count(1) as n
from bi_analytics.velocity_app_prod.dashboards
group by 1
having count(1) > 1


select date_trunc('month', block_timestamp) as month
, sum(amount) as amount
, max(amount) as amount
from solana.core.fact_transfers
where block_timestamp >= '2023-10-01'
    and tx_to = 'feegKBq3GAfqs9G6muPjdn8xEEZhALLTr2xsigDyxnV'
    and mint like 'So111%'
group by 1


with t0 as (
    select program_id
    , count(distinct tx_id) as n_tx
    from solana.core.fact_events
    where block_timestamp >= current_date - 1
    group by 1
)
, t1 as (
    select t0.*
    , l.label
    from t0
    join solana.core.dim_labels l
        on l.address = t0.program_id
)
select *
from t1
order by n_tx desc
limit 100

select q.created_at::date as date
, u.username
, q.statement
from bi_analytics.velocity_app_prod.queries q
join bi_analytics.velocity_app_prod.users u
    on u.id = q.created_by_id
where statement ilike '%graphql%'
order by date desc



with t0 as (
    SELECT
    livequery.live.udf_api(
        'POST',
        'https://beta.node.thegrid.id/graphql',
        {'Content-Type': 'application/json'},
        {
        'query': 'query Assets {\n  products(\n    where: {productDeployments: {smartContractDeployment: {smartContracts: {address: {_in: [\"JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4\", \"PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY\", \"6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P\", \"cjg3oHmg9uuPsP8D6g29NWvhySJkdYdAo9D25PRbKXJ\", \"ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXpkf3aD\", \"dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH\", \"DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M\", \"4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7\", \"whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc\", \"LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo\", \"CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK\", \"BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY\", \"KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD\", \"FLASH6Lo6h3iasJKWDs2F8TkW2UKf3s15C8PMGuVfgBn\", \"routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS\", \"675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8\"]}}}}}\n  ) {\n    name\n    isMainProduct\n    description\n    productStatus {\n      name\n    }\n  }\n}',
        'variables':{}
        }
    ):data:data:products as rawoutput
    , p.*
    , lateral flatten(input => rawoutput) as p
)
, t1 as (
    select *
    , lateral flatten(input => rawoutput) as p
    from t0
)

select *
from t1
limit 10000




with t0 as (
    select *
    from crosschain.bronze.twitter_accounts u
    where ecosystems ilike '%solana%'
    qualify (
        row_number() over (partition by twitter_id order by created_at desc) = 1
    )
)
select ta.twitter_handle
, ta.account_type
, ta.twitter_id
, avg(u.score) as score
from t0 ta
join bi_analytics.silver.user_community_scores_monthly u
    on u.twitter_id = ta.twitter_id
where month >= '2024-11-01'
    and ta.ecosystems ilike '%solana%'
group by 1, 2, 3
order by 4 desc


select *
from solana.core.fact_decoded_instructions i
where block_timestamp >= current_date - 1
    and program_id = 'veTbq5fF2HWYpgmkwjGKTYLVpY6miWYYmakML7R7LRf'

WITH staking_p as (
    SELECT
        TRUNC(block_timestamp, 'day') as day, -- Staking date
        block_timestamp,
        tx_id,
        signers[0] as owner,
        utils.udf_hex_to_int(
            TO_CHAR(REVERSE(TO_BINARY(SUBSTR(utils.udf_base58_to_hex(instruction:data), 19, 16))))
        ) / POW(10, 6) as amount_staked,
        TO_TIMESTAMP(
            utils.udf_hex_to_int(
                TO_CHAR(REVERSE(TO_BINARY(SUBSTR(utils.udf_base58_to_hex(instruction:data), 35, 8))))
            )
        ) as lockup_end,
        DATEDIFF(
            'day',
            block_timestamp,
            lockup_end
        ) as lockup_days,
        ((lockup_days * 0.01302225)+ 0.9875) * amount_staked as staking_power,
        'stake' as claim_program
    from 
        solana.core.fact_events
    WHERE 
        succeeded
        and fact_events.program_id = 'veTbq5fF2HWYpgmkwjGKTYLVpY6miWYYmakML7R7LRf'
        and SUBSTR(utils.udf_base58_to_hex(fact_events.instruction:data), 3, 16) = 'ceb0ca12c8d1b36c' -- staking
        and block_timestamp >= '2024-12-09'
        and staking_power > 0
)
SELECT owner,
     sum(Staking_power) as Staking_power,
    SUM(amount_staked) as total_$ME_Staked -- Total amount in this range
from 
    staking_p
group by 
    1
order by 2 desc
LIMIT 10000;



with t0 as (
    select decoded_instruction:args:args:endTs::int as endTs
    , decoded_instruction:args:args:amount::int * pow(10, -6) as lockupAmt
    , decoded_instruction:name::string as i_name
    , datediff('day', block_timestamp, to_timestamp(endTs)) as days
    , signers[0]::string as wallet
    , *
    from solana.core.fact_decoded_instructions i
    where block_timestamp >= '2024-12-09'
        and program_id = 'veTbq5fF2HWYpgmkwjGKTYLVpY6miWYYmakML7R7LRf'
        -- and signers[0]::string in ('GcXFgTPypQPqTBGTgZdZHHweZAaDJ3Suwugj52kyAcov','2S5JDnseFzM3r5ZQ9LyGYZGuduQXQ4SkXXtvRbuD9qyC')
)
, t1 as (
    select t0.*
    , p.value:name::string as name
    , p.value:pubkey::string as pubkey
    from t0
    , lateral flatten(input => decoded_instruction:accounts) as p
), t2 as (
    select *
    , max(days) over (partition by pubkey) as duration
    , 2000 as lockupTargetVotingPct
    , 1 as lockupMinDuration
    , 1460 as lockupMaxSaturation
    , (lockupAmt * lockupTargetVotingPct) / 100 as maxVotingPower
    , lockupMaxSaturation - lockupMinDuration as lockupDurationRange
    , case
        when duration < lockupMinDuration then lockupAmt
        when duration >= lockupMaxSaturation then maxVotingPower
        else lockupAmt +
        ((maxVotingPower - lockupAmt) * (duration - lockupMinDuration)) /
        (lockupMaxSaturation - lockupMinDuration)
        end as votingPower
    from t1
    where name = 'lockup'
    order by wallet, block_timestamp
)
select wallet
, pubkey as lockup_address
, sum(votingPower) as votingPower
, sum(lockupAmt) as lockupAmt
, max(duration) as max_duration
, min(duration) as min_duration
, count(1) as n_lockups
from t2
where votingPower > 0
group by 1, 2
order by 3 desc
-- limit 10000



with t0 as (
    select livequery.live.udf_api(
        'https://api.flipsidecrypto.com/api/v2/queries/dd888a2a-7dd9-4226-8332-560441f214c8/data/latest'
    ):data as data
)
select data[0]:wallet::string as wallet
, sum(data[0]:ACTIVELOCKUPAMT::int) as lockup_amt
, sum(data[0]:ACTIVEVOTINGPOWER::int) as voting_power
, max(data[0]:MAXENDTS) as end_ts
from t0
group by 1
order by 3 desc


select *
from datascience.twitter.ez_tweets
where conversation_id = '1892569388575846568' 


with t0 as (
    select d.id as dashboard_id
    -- special case for squid router -> Axelar
    , case when statement ilike '%0xbe54BaFC56B468d4D20D609F0Cf17fFc56b99913%'
        or statement ilike '%0x0cD070285380cabfc3be55176928dc8A55e6d2A7%'
        or statement ilike '%0xfb3330531E3f98671296f905cd82CC407d90CE97%'
        or statement ilike '%0xce16f69375520ab01377ce7b88f5ba8c48f8d666%'
        then 'Axelar' else INITCAP(t.name) end as chain
    , COUNT(DISTINCT q.id) as n_queries
    from bi_analytics.velocity_app_prod.dashboards d
    join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
        on d.id = dtq.A
    join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    join bi_analytics.bronze.velocity_app_prod_mtm__queries_to_tags qtt
        on q.id = qtt.A
    join bi_analytics.velocity_app_prod.tags t
        on qtt.B = t.id
        and (t.type = 'project' or t.name = 'vertex')
    group by 1, 2
)
, t1 as (
    -- special case for bonk
    select rt.resource_id as dashboard_id
    , 'Bonk' as chain
    , count(distinct q.id) as n_queries
    from hevo.bronze_datastudio_db_2024.resource_tags rt
    left join hevo.bronze_datastudio_db_2024.tags t
        on t.id = rt.tag_id
    left join hevo.bronze_datastudio_db_2024.dashboards d
        on d.id = rt.resource_id
    left join bi_analytics.bronze.velocity_app_prod_mtm__dashboards_to_queries dtq
        on d.id = dtq.A
    left join bi_analytics.velocity_app_prod.queries q
        on dtq.B = q.id
    where t.name ilike '%bonk%' or (d.title ilike '%bonk%' or q.name ilike '%bonk%')
    group by 1, 2
)
, t2 as (
    select *
    from t0
    union all
    select *
    from t1
)
, t3 as (
    SELECT t2.*
    , row_number() over (
        partition by dashboard_id
        order by
        n_queries desc
        , case when chain in (
            'Aleo'
            , 'Aptos'
            , 'Avalanche'
            , 'Axelar'
            , 'Blast'
            , 'Bonk'
            , 'Flow'
            , 'Kaia'
            , 'Lava'
            , 'Near'
            , 'Olas'
            , 'Solana'
            , 'Sei'
            , 'Thorchain'
            , 'Vertex'
            , 'Swell'
        ) then 1 when chain = 'Ethereum' then 2 else 3 end
        , chain
    ) as rn
    , SUM(n_queries) over (partition by dashboard_id) as tot_queries
    , n_queries / greatest(tot_queries, 1) as pct
    from t2
)
SELECT t3a.dashboard_id
, CONCAT(
    case when t3a.chain is null then 'Other' else t3a.chain end
    , case when t3b.chain is null then '' else CONCAT(' + ', t3b.chain) end
    , case when t3c.chain is null then '' else CONCAT(' + ', t3c.chain) end
) as chain
from t3 t3a
left join t3 t3b
    on t3a.dashboard_id = t3b.dashboard_id
    and t3b.rn = 2
    and t3b.pct > 0.25
left join t3 t3c
    on t3a.dashboard_id = t3c.dashboard_id
    and t3c.rn = 3
    and t3c.pct > 0.25
where t3a.rn = 1




with ambassador as (
    select '3e4c5f08-a622-4c0d-84aa-3f82d684879f' as user_id, '' as mentee_user_id, 'analyst' as status, 'Aptos' as partner, 'aptos' as ecosystem, 'APT' as currency, 230 as base_comp, 0.002 as impression_incentive
    union select 'e57d70cd-45e2-4d5b-9c65-80445176c2a0' as user_id, '' as mentee_user_id, 'analyst' as status, 'Flipside' as partner, 'flow' as ecosystem, 'FLOW' as currency, 285 as base_comp, 0.002 as impression_incentive
    union select 'e57d70cd-45e2-4d5b-9c65-80445176c2a0' as user_id, 'f156f8cc-752d-4f74-81f9-727e96f9f2a2' as mentee_user_id, 'mentor' as status, 'Lava' as partner, 'polygon' as ecosystem, 'USDC' as currency, 115 as base_comp, 0.002 as impression_incentive
    union select 'b469ac38-f43f-4004-a281-d83417202f90' as user_id, '' as mentee_user_id, 'analyst' as status, 'Near' as partner, 'near' as ecosystem, 'NEAR' as currency, 125 as base_comp, 0 as impression_incentive
    union select '2cc4fa71-7f73-4ccf-b993-17e46607dd27' as user_id, '' as mentee_user_id, 'analyst' as status, 'Avalanche' as partner, 'avalanche' as ecosystem, 'AVAX' as currency, 175 as base_comp, 0.002 as impression_incentive
    union select '188d5fb7-5d6a-4f7d-8126-7724d1d9672f' as user_id, '' as mentee_user_id, 'analyst' as status, 'BONK' as partner, 'polygon' as ecosystem, 'USDC' as currency, 125 as base_comp, 0.002 as impression_incentive
    union select '0ae867bf-4464-4a5d-a431-d08939c5dd9f' as user_id, '' as mentee_user_id, 'analyst' as status, 'Thorchain' as partner, 'polygon' as ecosystem, 'USDC' as currency, 115 as base_comp, 0.002 as impression_incentive
    union select '1d096292-b747-405c-b472-e1e638a06931' as user_id, '' as mentee_user_id, 'analyst' as status, 'NEAR / Vertex' as partner, 'near' as ecosystem, 'NEAR' as currency, 575 as base_comp, 0.002 as impression_incentive
    union select 'ccedc530-8617-419f-90f0-3a30be4e0520' as user_id, '' as mentee_user_id, 'analyst' as status, 'Aptos' as partner, 'aptos' as ecosystem, 'APT' as currency, 115 as base_comp, 0.002 as impression_incentive
    union select '4f1d8ac0-9107-40ed-bc44-c07b3c8ebcbe' as user_id, '' as mentee_user_id, 'analyst' as status, 'Flow' as partner, 'flow' as ecosystem, 'FLOW' as currency, 285 as base_comp, 0.002 as impression_incentive
    union select '762199f7-63de-4a0c-b854-dd7d66cddf14' as user_id, '' as mentee_user_id, 'analyst' as status, 'Axelar' as partner, 'axelar' as ecosystem, 'AXL' as currency, 115 as base_comp, 0.002 as impression_incentive
    union select '8f66ec68-5da1-498e-bc32-70bcdfe9d1ae' as user_id, '' as mentee_user_id, 'analyst' as status, 'Solana' as partner, 'solana' as ecosystem, 'SOL' as currency, 115 as base_comp, 0.002 as impression_incentive
    union select '001dcab6-54ae-448f-80fb-b4d98303bd1e' as user_id, '188d5fb7-5d6a-4f7d-8126-7724d1d9672f' as mentee_user_id, 'mentor' as status, 'BONK' as partner, 'polygon' as ecosystem, 'USDC' as currency, 55 as base_comp, 0 as impression_incentive
    union select '001dcab6-54ae-448f-80fb-b4d98303bd1e' as user_id, '' as mentee_user_id, 'analyst' as status, 'Solana' as partner, 'solana' as ecosystem, 'SOL' as currency, 175 as base_comp, 0.002 as impression_incentive
    union select '5cf41adf-8579-467f-9106-9260d1916f75' as user_id, '' as mentee_user_id, 'analyst' as status, 'Other' as partner, 'polygon' as ecosystem, 'USDC' as currency, 230 as base_comp, 0.002 as impression_incentive
    union select '1abe2020-72ce-4291-ac8f-c098a28c5877' as user_id, '' as mentee_user_id, 'analyst' as status, 'Marinade' as partner, 'solana' as ecosystem, 'SOL' as currency, 175 as base_comp, 0.002 as impression_incentive
    union select 'be58bfd3-ea79-42c1-8daa-7af18cde0676' as user_id, '' as mentee_user_id, 'analyst' as status, 'Solana' as partner, 'solana' as ecosystem, 'SOL' as currency, 345 as base_comp, 0.002 as impression_incentive
    union select '7d1eba10-7543-4a0a-aadd-c59ea717d688' as user_id, '' as mentee_user_id, 'analyst' as status, 'Near' as partner, 'near' as ecosystem, 'NEAR' as currency, 230 as base_comp, 0.002 as impression_incentive
    union select 'f156f8cc-752d-4f74-81f9-727e96f9f2a2' as user_id, '' as mentee_user_id, 'analyst' as status, 'Olas' as partner, 'polygon' as ecosystem, 'USDC' as currency, 175 as base_comp, 0.002 as impression_incentive
    union select '7404ed93-13d1-4aaf-96ba-7ec4243257ac' as user_id, '' as mentee_user_id, 'analyst' as status, 'Near' as partner, 'near' as ecosystem, 'NEAR' as currency, 115 as base_comp, 0.002 as impression_incentive
    union select '64f62962-3fc8-4c1d-b017-4c565fc43a4d' as user_id, '' as mentee_user_id, 'analyst' as status, 'Axelar' as partner, 'polygon' as ecosystem, 'USDC' as currency, 125 as base_comp, 0.004 as impression_incentive
    union select '21d3e917-d4a7-48a9-a100-88b0782ef888' as user_id, '' as mentee_user_id, 'analyst' as status, 'Thorchain' as partner, 'polygon' as ecosystem, 'USDC' as currency, 201 as base_comp, 0.004 as impression_incentive
    union select '08b215df-7b48-4e5c-9814-95435c9dc289' as user_id, '' as mentee_user_id, 'analyst' as status, 'Blast' as partner, 'blast' as ecosystem, 'BLAST' as currency, 115 as base_comp, 0.002 as impression_incentive
    union select 'd286b975-feb9-4650-b27c-159759b9ef81' as user_id, '' as mentee_user_id, 'analyst' as status, 'Kaia' as partner, 'kaia' as ecosystem, 'KLAY' as currency, 125 as base_comp, 0.002 as impression_incentive
    union select 'ed1eebf3-3eef-4fa4-abd6-a7226420045a' as user_id, '' as mentee_user_id, 'analyst' as status, 'Near' as partner, 'near' as ecosystem, 'NEAR' as currency, 115 as base_comp, 0.002 as impression_incentive
    union select '92a13cf3-d112-43a2-8f44-e71355868b63' as user_id, '' as mentee_user_id, 'analyst' as status, 'Other' as partner, 'polygon' as ecosystem, 'USDC' as currency, 175 as base_comp, 0.002 as impression_incentive
    union select '9f513eee-0aeb-4887-86f2-f878404accd2' as user_id, '' as mentee_user_id, 'analyst' as status, 'Vertex' as partner, 'polygon' as ecosystem, 'USDC' as currency, 115 as base_comp, 0.002 as impression_incentive
    union select '79a72c69-c253-4f5e-997d-680e75022a57' as user_id, '' as mentee_user_id, 'analyst' as status, 'Aptos' as partner, 'aptos' as ecosystem, 'APT' as currency, 115 as base_comp, 0.002 as impression_incentive
    union select '1355167c-d56b-4afe-9e32-7983c2e3aa30' as user_id, '' as mentee_user_id, 'analyst' as status, 'Avalanche' as partner, 'avalanche' as ecosystem, 'AVAX' as currency, 125 as base_comp, 0.002 as impression_incentive
    union select '098ebead-a420-42d8-9fca-4ef045d2c65e' as user_id, '' as mentee_user_id, 'analyst' as status, 'Aleo' as partner, 'polygon' as ecosystem, 'USDC' as currency, 115 as base_comp, 0.002 as impression_incentive
)
, labels as (
  select
    c.value:dashboard_id :: string as dashboard_id,
    c.value:tag :: string as dashboard_tag
  from
    crosschain.bronze.data_science_uploads,
    LATERAL FLATTEN(input => record_content) c
  where
    record_metadata:key like 'dashboard-tags%'
),
t1 as (
  select
    u.username,
    u.profile_id,
    a.*
  from
    ambassador a
    left join bi_analytics_dev.velocity_app_prod.users u on u.id = a.user_id
    left join bi_analytics_dev.velocity_app_prod.profiles p on p.id = u.profile_id
  where
    p.type = 'user'
)
-- select * from t1
, impr as (
  select conversation_id
  , max(impression_count) as impression_count
  from datascience.twitter.ez_tweets
  group by 1
)
,
imp0 as (
  select
    d.id as dashboard_id,
    d.created_by_id as user_id,
    i.impression_count
  from
    bi_analytics_dev.velocity_app_prod.dashboards d
    join datascience.twitter.ez_tweets t on RIGHT(d.latest_slug, 6) = RIGHT(SPLIT(t.clean_url, '?') [0] :: string, 6)
    join impr i
      on i.conversation_id = t.conversation_id
  where
    not d.id in (
      select
        dashboard_id
      from
        labels
      where
        dashboard_tag = 'bot'
    )
    and t.created_at :: date >= current_date - 10
    and t.created_at :: date <= current_date - 4 qualify(
      row_number() over (
        partition by t.conversation_id
        order by
          i.impression_count desc
      ) = 1
      and row_number() over (
        partition by t.tweet_url
        order by
          i.impression_count desc
      ) = 1
    )
),
imp as (
  select
    user_id,
    sum(impression_count) as impression_count
  from
    imp0
  group by
    1
),
analyst as (
  select
    t1.user_id as datastudio_id,
    t1.profile_id,
    t1.username,
    initcap(t1.partner) as partner,
    t1.ecosystem as chain,
    t1.currency,
    t1.base_comp as base_comp,
    coalesce(i.impression_count, 0) as impression_count,
    t1.impression_incentive, -- kb update on 6/4/24
    floor(
      least(250000, coalesce(i.impression_count, 0) * t1.impression_incentive) -- kb update on 6/4/24
    ) as incentive_comp,
    t1.base_comp + incentive_comp as amount
  from
    t1
    left join imp i on i.user_id = t1.user_id
  where
    status = 'analyst'
),
mentor as (
  select
    t1.user_id as datastudio_id,
    t1.profile_id,
    t1.username,
    initcap(t1.partner) as partner,
    t1.ecosystem as chain,
    t1.currency,
    t1.base_comp,
    coalesce(i.impression_count, 0) as impression_count,
    t1.impression_incentive, -- kb update on 6/4/24
    floor(
      least(250000, coalesce(i.impression_count, 0) * t1.impression_incentive) -- kb update on 6/4/24
    ) as incentive_comp,
    t1.base_comp + incentive_comp as usd_amount
  from
    t1
    left join imp i on i.user_id = t1.mentee_user_id
  where
    status = 'mentor'
),
combined as (
    select
      *
    from
      mentor
    union
    select
      *
    from
      analyst
),
final as (
    select 
        case 
            when impression_incentive > 0 then ' and impressions bonus.'
            when impression_incentive = 0 then '.' end
            as has_bonus,
        concat ('Ambassador weekly payment for ', partner, ' ecosystem from ', (current_date - 10), ' to ', (current_date - 4), ' including base pay', has_bonus) as name,
        'AMBASSADOR' as type,
        (current_date - 4) as price_scrape_time,
        combined.*
    from combined
)
select 
    name,
    type,
    datastudio_id,
    chain, 
    currency,
    case 
        when partner = 'Bitcoin' then 'flipside-self-funded'
        when partner = 'Other' then 'flipside-self-funded'
        when username = 'jackguy' then 'marinade'
        when username = 'brian-terra' then 'near'
        else lower(partner) 
        end as partner,
    round(usd_amount) as usd_amount,
    null as token_amount,
    price_scrape_time,
    -- ^ required columns
    
    null as note,
    username,
    profile_id,
    round(base_comp) as base_comp,
    impression_incentive,
    incentive_comp
    -- ,dashboard_id
    -- ^ extra columns
    
from final
order by name, username
;



	select coalesce(r.tweet_id, q.tweet_id) as tweet_id
	, coalesce(r.user_id, q.user_id) as user_id
	, case when q.tweet_id is null then 0 else 1 end as is_qt
	, coalesce(r.created_at, q.created_at) as created_at
	, coalesce(k.score, 0) as kol_score
	, coalesce(k.is_kol, 0) as is_kol
	, case when k.is_kol is null then coalesce(ca.is_core_audience, 0) else 0 end as is_core_audience

with t0 as (
    select tweet_id
    , sum(1 - is_qt) as n_retweet
    , sum(is_qt) as n_quote
    from bi_analytics.twitter.qrt_view
    group by 1
)
select dt.conversation_id
, dt.impression_count
, concat('https://x.com/',coalesce(u.username, 'unknown'),'/status/', dt.conversation_id) as tweet_url
, dt.created_at::date as tweet_date
, dt.created_at as start_timestamp
, d.title
, d.latest_slug
, d.created_by_id as user_id
, du.username
, dcm.chain
, dt.is_banned_tweet
, coalesce(t0.n_retweet, 0) as n_retweet
, coalesce(t0.n_quote, 0) as n_quote
from bi_analytics.twitter.dashboard_tweets dt
join bi_analytics.velocity_app_prod.dashboards d
    on d.id = dt.dashboard_id
join bi_analytics.velocity_app_prod.users du
    on du.id = d.created_by_id
left join datascience.twitter.dim_users u
    on u.user_id = dt.author_id
left join bi_analytics.silver.dashboard_chain_map dcm
    on dcm.dashboard_id = d.id
left join t0
    on t0.tweet_id = dt.conversation_id



select v.tweet_id as conversation_id
, v.user_id as twitter_id
, coalesce(u.username, ta.twitter_handle) as twitter_handle
, coalesce(ta.account_type, 'Other') as account_type
, coalesce(ta.ecosystems[0]::string, 'Other') as ecosystem
, v.is_qt as has_quote
, 1 - v.is_qt as has_retweet
from bi_analytics.twitter.qrt_view v
left join datascience.twitter.dim_users u
    on u.user_id = v.user_id
left join bi_analytics.twitter.twitter_accounts ta
    on ta.twitter_id = u.user_id
limit 10



select *
from solana.core.fact_events
where block_timestamp >= current_date - 1
    and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
limit 10




with t0 as (
    select distinct tx_id
    , block_timestamp
    from solana.core.fact_events
    where
        block_timestamp <= current_date - 3
        and block_timestamp >= current_date - 14
        and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
        and succeeded = true
)
, tr as (
    select t0.tx_id
    , max(coalesce(amount, 0)) as amount
    from t0
    left join solana.core.fact_transfers t
        on t.block_timestamp = t0.block_timestamp
        and t.tx_id = t0.tx_id
    where
        t.block_timestamp <= current_date - 3
        and t.block_timestamp >= current_date - 14
        and t.mint like 'So111%'
    group by 1
)
, t1 as (
    select distinct tx_id
    from solana.core.fact_decoded_instructions
    where
        block_timestamp <= current_date - 3
        and block_timestamp >= current_date - 14
        and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
        and not decoded_instruction::string ilike '%Unknown instruction%'
)
, t2 as (
    select tr.tx_id as e_tx_id
    , t1.tx_id as d_tx_id
    , coalesce(tr.amount, 0) as amount
    from tr
    full outer join t1
        on tr.tx_id = t1.tx_id
)
-- select *
-- from t2
-- where d_tx_id is null
-- order by amount desc

select 
    count(distinct e_tx_id) as e_tx_count
    , count(distinct d_tx_id) as d_tx_count
    , count(1) as n_tx
    , round(d_tx_count * 100 / n_tx, 1) as pct_decoded
from t2
where amount >= 0.2


