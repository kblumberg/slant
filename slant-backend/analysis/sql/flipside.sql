select *
from solana.core.fact_events
where block_timestamp > current_date - 1
    and program_id = 'SHARKobtfF1bHhxD2eqftjHBdVSCbKo9JtgK71FhELP'
limit 10