
with popular_mints as (
    select swap_to_mint as mint
    , count(distinct swapper) as num_swappers
    from solana.defi.ez_dex_swaps
    where block_timestamp >= dateadd(hour, -12, current_timestamp)
    group by 1
    order by 2 desc
)
, t0 as (
    select 
    s.block_timestamp,
    s.tx_id,
    s.swap_to_mint,
    s.swap_from_mint,
    s.swap_to_amount_usd,
    s.swap_from_amount_usd,
    t.mint,
    t.amount
    from solana.defi.ez_dex_swaps s
    join solana.core.fact_transfers t
        on t.block_timestamp = s.block_timestamp
        and t.tx_id = s.tx_id
        and t.tx_to = 'ZG98FUCjb8mJ824Gbs6RsgVmr1FhXb2oNiJHa2dwmPd'
    where s.block_timestamp >= current_date - 1
        and t.block_timestamp >= current_date - 1
)
, t1 as (
    select t0.*
    , coalesce(pf.num_swappers, 0) as num_swappers_f
    , coalesce(pt.num_swappers, 0) as num_swappers_t
    , case when num_swappers_f > num_swappers_t then coalesce(swap_from_amount_usd, swap_to_amount_usd) else coalesce(swap_to_amount_usd, swap_from_amount_usd) end as volume
    from t0
    left join popular_mints pf
        on pf.mint = t0.swap_from_mint
    left join popular_mints pt
        on pt.mint = t0.swap_to_mint
)
select block_timestamp::date as date
, sum(volume) as volume
from t1
group by 1
order by 1 desc