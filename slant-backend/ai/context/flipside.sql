This query analyzes Sharky lending protocol data on Solana over the past 7 days, tracking loan volumes and counts per lender, ranking the top 30 lenders by volume while calculating their percentage contributions to total lending activity.

Sharky | Largest Sharky Whales (7d)

with t0 as (
    select case when decoded_instruction:name::string = 'takeLoanV3' then decoded_instruction:accounts[0]:pubkey::string else decoded_instruction:accounts[5]:pubkey::string end as lender
    , count(1) as n_loans
    , sum(split(decoded_instruction:args:expectedLoan::string, ',')[1]::int) * power(10, -9) as volume
    from solana.core.fact_decoded_instructions
    where block_timestamp >= current_date - 7
        and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
        and decoded_instruction:name::string in ('takeLoanV3','extendLoanV3')
    group by 1
), t1 as (
    -- select case when lender in ('7Wgz6LB4gkd7hr1hjyTur8tZSXC5sx1QYbYGQ7N2w5z7','ySoLLxJfRkecrD4wNL6NXmSw6P6fSmeR7tt1fn4Lqvw','runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC') then left(lender, 4) else 'other' end as lender
    select lender
    , left(lender, 4) as abbr
    , n_loans
    , volume
    , sum(volume) over () as tot_volume
    , sum(n_loans) over () as tot_n_loans
    , round(volume * 100 / tot_volume, 1) as pct_volume
    , round(n_loans * 100 / tot_n_loans, 1) as pct_n_loans
    , row_number() over (order by volume desc) as rk
    from t0
    -- qualify(
    --     row_number() over (order by volume desc) <= 10
    -- )
)
select *
from t1
where rk <= 30
order by rk