import os
import time
import pandas as pd
from utils.utils import log
from datetime import datetime, timedelta
from utils.db import pg_upload_data
from utils.db import pc_execute_query
from classes.JobState import JobState
from utils.flipside import extract_project_tags_from_user_prompt
from ai.tools.utils.prompt_refiner_for_flipside_sql import prompt_refiner_for_flipside_sql
from constants.keys import OPENAI_API_KEY, ANTHROPIC_API_KEY
from langchain_openai import ChatOpenAI
from ai.tools.utils.utils import state_to_reference_materials, log_llm_call
from constants.constant import MAX_FLIPSIDE_SQL_ATTEMPTS
from ai.tools.utils.parse_json_from_llm import parse_json_from_llm
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic

prompt = f"""

        You are an expert in analyzing blockchain analytics and providing feedback on SQL queries and results.

        You will be given:
        - A **description of the analysis objective**
        - Possibly **previous SQL queries** that failed (e.g., returned no results or incorrect results)
        - A reference **schema**
        - A set of **example queries** to show good patterns and table usage
        - Other **reference materials** to enrich your understanding of the analysis objective
        - The **latest SQL query**, which has returned results
        - The **latest SQL query results** (first and last 10 rows)

        ---

        ## 🔍 Your Task

        Analyze the latest SQL query results and determine if the query successfully fulfills the analysis objective and return a valid JSON object with the following fields:
        - `change_type`: int: 0 = no change, 1 = almost there, just a minor change to query, 2 = more substantial change to query, possibly a new table or new filters
        - `change_summary`: a single block of text that explains why the query / results are wrong and some potential ways to fix it. If there are no issues with the query and the results are correct, just an empty string.

        Only suggest a change if you are confident that the query and results are materially incorrect. If the labeling or ordering of the results is incorrect, that is not a problem.

        ---

        ## 📚 Reference Materials


**RELATED FLIPSIDE QUERIES**:
Here are some example queries written by other analysts. They may not be respresent the best or most optimized way to approach your analysis, but feel free to use them for inspiration and to understand available schema and patterns, incorporating them into your query if you think they are helpful. If you know how to write the query just using the schemas and other reference materials, feel free to ignore these queries:



### Example Query #1:
This query analyzes NFT sales statistics on Solana for October 2024, breaking down sales volume (in both SOL and USD), buyer counts, and seller counts by NFT collection and marketplace, with a focus on the top 10 collections by volume while grouping the rest as "Others."

NFT Sales Stats on Solana: October 2024 | Daily NFT Stats by Collection & Marketplace



WITH solana_transactions AS 
(
SELECT 
  block_timestamp,
  marketplace,
  tx_id,
  purchaser,
  seller,
  mint,
  sales_amount as sol_amount
FROM 
  solana.nft.fact_nft_sales
WHERE 
  succeeded AND block_timestamp >= '2024-10-01' AND block_timestamp < '2024-11-01'
),

solana_usd AS 
(
SELECT 
  date_trunc('day', hour) as timestamp,
  AVG(PRICE) as USD_PRICE
FROM
  solana.price.ez_prices_hourly
WHERE 
  token_address = 'So11111111111111111111111111111111111111112'
AND 
  timestamp::date >= '2024-10-01' AND timestamp::date < '2024-11-01'
GROUP BY 1
),

most_famous_collections AS 
(
SELECT 
  nft_collection_name,
  SUM(sol_amount) as total_sol_volume,
  COUNT(distinct tx_id) as sale_count
FROM
  solana_transactions s JOIN solana_usd su ON (s.block_timestamp::date = su.timestamp::date) JOIN solana.nft.dim_nft_metadata labels ON (s.mint = labels.mint)
WHERE 
  nft_collection_name is not null
GROUP BY 1 ORDER BY 2 DESC, 3 DESC LIMIT 10

)

SELECT 
  CASE WHEN nft_collection_name IN (SELECT distinct nft_collection_name FROM most_famous_collections) then nft_collection_name
  ELSE 'Others' END as "Collection",
  CASE WHEN marketplace ilike '%eden%' then 'Magic Eden'
  WHEN marketplace ilike '%tensor%' then 'Tensor'
  WHEN marketplace ilike '%solanart%' then 'Solanart'
  WHEN marketplace ilike '%hadeswap%' then 'Hadeswap'
  WHEN marketplace ilike '%sniper%' then 'Sol Sniper'
  WHEN marketplace ilike '%exchange art%' then 'Exchange.Art'
  ELSE marketplace END as "NFT Marketplace",
  SUM(sol_amount) as "Sales $SOL Volume",
  "Sales $SOL Volume" * AVG(USD_PRICE) as "Sales $SOL Volume (USD)",
  COUNT(distinct purchaser) as "Buyer Count",
  COUNT(distinct seller) as "Seller Count"
FROM
  solana_transactions s JOIN solana_usd su ON (s.block_timestamp::date = su.timestamp::date) JOIN solana.nft.dim_nft_metadata labels ON (s.mint = labels.mint)
WHERE 
  nft_collection_name is not null
GROUP BY 1, 2


**FLIPSIDE DATA SCHEMA**:


# Schema: nft
## Table: solana.nft.dim_nft_metadata
**Description**: Contains NFT metadata sourced from Solscan and Helius API.
**Columns**:
- mint 
- nft_collection_name 
- collection_id 
- creators 
- authority 
- metadata 
- image_url 
- metadata_uri 
- nft_name 

## Table: solana.nft.ez_nft_sales
**Description**: A convenience table containing NFT sales across multiple marketplaces, included information on metadata, USD prices and marketplace. Note that USD prices are not available prior to 2021-12-16.
**Columns**:
- marketplace  (Examples: tensorswap, magic eden v3, magic eden v2, Magic Eden, solsniper, tensor, hadeswap, hyperspace, exchange art, solanart) (Usage Tips: group similar values together - e.g. `case when marketplace ilike '%magic eden%' then 'Magic Eden' when marketplace ilike '%tensor%' then 'Tensor'...` (use this technique unless there are explicit instructions to the contrary))
- marketplace_version  (Examples: v1, v2, v3)
- block_timestamp 
- block_id 
- tx_id 
- succeeded 
- index 
- inner_index 
- program_id 
- buyer_address 
- seller_address 
- mint 
- nft_name 
- price  (Usage Tips: if you are using this column to get volume, it is usually best to filter for currency_address = "So11111111111111111111111111111111111111111" as well to ensure you are getting just SOL volume)
- currency_address  (Examples: `So11111111111111111111111111111111111111111`, `3dgCCb15HMQSA4Pn3Tfii5vRk7aRqTH95LJjxzsG2Mug`, `EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v`, `MEFNBXixkEbait3xn9bkm8WsJzXtVsaJEn4c8Sam21u`)
- currency_symbol  (Examples: `SOL`, `HXD`, `USDC`, `ME`)
- price_usd 
- tree_authority 
- merkle_tree 
- leaf_index 
- is_compressed 
- nft_collection_name 
- collection_id 
- creators 
- authority 
- metadata
- image_url 
- metadata_uri 

# Schema: price
## Table: solana.price.ez_prices_hourly
**Description**: A convenience table for determining token prices by address and blockchain, and native asset prices by symbol and blockchain. This data set is highly curated and contains metadata for one price per hour per unique asset and blockchain.
**Notes**:
 - Does NOT contain data for all tokens; if you are looking for a token that is not in this table, you can use the `solana.defi.ez_dex_swaps` table to get the price
 - If you are looking for a token that is not in this table, you can use the `solana.defi.ez_dex_swaps` table to get the price
 - The `solana.price.ez_token_prices_hourly` table is deprecated. Use `solana.price.ez_prices_hourly` instead
**Columns**:
- hour 
- token_address  (Usage Tips: When joining or filtering for analysis, ALWAYS use `token_address` - DO NOT USE `symbol` or `name` to filter because multiple tokens can have the same symbol or name)
- symbol  (Usage Tips: When displaying data to the user, though, use `symbol` or `name` to make it more readable)
- name 
- decimals 
- price  (Usage Tips: If joining onto a swaps table where there is a `swap_from_amount` and `swap_to_amount`, there is a special logic you should apply to get the correct USD amounts, like so:

```sql
select
    s.tx_id,
    s.block_timestamp,
    s.swap_from_amount,
    s.swap_to_amount,
    case
        -- if the swap is from $SOL, $USDC, or $USDT, then use the price of the from mint
        when s.swap_from_mint IN ('So11111111111111111111111111111111111111112','EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v','Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB') then s.swap_from_amount * from_mint.price
        -- if the swap is to $SOL, $USDC, or $USDT, then use the price of the to mint
        when s.swap_to_mint IN ('So11111111111111111111111111111111111111112','EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v','Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB') then s.swap_to_amount * to_mint.price
        -- otherwise, default to the from mint price, but coalesce to the to mint price if the from mint price is null
        else coalesce(s.swap_from_amount * from_mint.price, s.swap_to_amount * to_mint.price)
    end as swap_amount_usd
from solana.defi.<swap_table> s
left join solana.price.ez_prices_hourly from_mint
    on date_trunc('hour', s.block_timestamp) = from_mint.hour
    and s.swap_from_mint = from_mint.token_address
left join solana.price.ez_prices_hourly to_mint
    on date_trunc('hour', s.block_timestamp) = to_mint.hour
    and s.swap_to_mint = to_mint.token_address
```

This logic defaults to using prices for SOL, USDC, and USDT, since they are the most reliable prices, but can fall back to the prices of the from or to mints depending on price availability.)
- blockchain  (Usage Tips: `blockchain` is always 'solana' so you can ignore this column)
- is_native 
- is_imputed 
- is_deprecated 

# Schema: core
## Table: crosschain.core.dim_dates
**Notes**:
 - This table goes far into the future, so you MUST filter through the current date (e.g. `and date_day <= CURRENT_DATE()`) to make sure there are no extra rows into the future.
**Columns**:
- date_day 
- prior_date_day 
- next_date_day 
- day_of_week_name  (Examples: `Monday`, `Tuesday`)
- day_of_week_name_short  (Examples: `Mon`, `Tue`)

## Performance Tips
 - If you have a `block_timestamp` filter or join, ALWAYS put that first in the WHERE clause or JOIN clause to optimize performance.
 - ALWAYS filter and join on `block_timestamp` when possible and appropriate to optimize performance.
 - Even when you are filtering one table for `block_timestamp`, filter the other table for `block_timestamp` as well to improve performance. e.g. `FROM table1 JOIN table2 ON table1.block_timestamp = table2.block_timestamp AND table1.tx_id = table2.tx_id WHERE table1.block_timestamp >= DATEADD(day, -30, CURRENT_DATE()) AND table2.block_timestamp >= DATEADD(day, -30, CURRENT_DATE())`
 - If you are joining on `tx_id`, ALWAYS also join on `block_timestamp` as well to improve performance and put the join on `block_timestamp` first. e.g. `FROM table1 t1 JOIN table2 t2 ON t1.block_timestamp = t2.block_timestamp AND t1.tx_id = t2.tx_id`
 - Avoid `SELECT *` and limit columns to what is needed
 - Use `GROUP BY` when using aggregate functions
 - Use example queries for structure and patterns, but tailor them to the user’s question
 - Make sure token addresses and program IDs are correct (cross-reference with examples)
 - Any time-based column should be aliased as `date_time`
 - The primary categorical column should be aliased as `category`
 - If you are doing some kind of running total, make sure there are no gaps in the data by using the `crosschain.core.dim_dates` table
 - If the analysis is "to now", "to present", etc., you don't need to do a <= `block_timestamp` filter
 - Do NOT use any placeholders. Only use real mints, addresses, program ids, dates, etc
 - Make sure to use LIMIT if you are only looking for a specific number of results (e.g. top 10, top 100, etc.)
 - Project program_ids can change over time; you may need to include multiple program ids in the WHERE clause to get the correct results
 - All timestamps are in UTC; default to that unless the user explicitly mentions a different timezone
 - To do time comparisons, use `dateadd` and `current_timestamp()` or `current_date()` like so: `and block_timestamp >= dateadd(hour, -12, current_timestamp())`
 - If there is a "launch date" you are referencing, its usually a good idea to give a 2 day buffer to make sure you get all the data. (e.g. `and block_timestamp >= dateadd(day, -2, <launch_date>)`)
 - market cap = price * circulating supply
 - ONLY use the columns that are listed in the schema
 - if you are parsing amounts from the instructions (e.g. `instruction:parsed:info:amount`) then you may need to adjust for decimals, but if you are using an `amount` column directly from the tables, it is already decimal-adjusted
 - use `current_date` or `current_timestamp` to get today's date / timestamp
 - To get the "% of XXX that is YYY" (where YYY is a subset of XXX), one method to consider is to select all the distinct `tx_id`s from YYY and LEFT JOIN it to XXX and calculate it that way

For example, to get the "% of swaps that are Jupiter swaps", you can do the following:
```sql
with subset as (
    select distinct tx_id
    , block_timestamp
    from solana.___.YYY
)
, total as (
    select tx_id
    , block_timestamp
    , case when subset.tx_id is not null then 1 else 0 end as is_included
    , value
    from solana.___.XXX
    left join subset
        on subset.block_timestamp = XXX.block_timestamp
        and subset.tx_id = XXX.tx_id
)
select sum(is_included * value) / sum(value) over () as pct
from total;
**RECOMMENDED SQL APPROACH**:
Here is the approach we recommend to write the Flipside SQL query to analyze the user's analysis goal using raw tables. Generally try to adhere to this approach, but feel free to deviate if you think it is more appropriate:



## Analysis Approach Summary

### Primary Table
- **solana.nft.ez_nft_sales** - Contains all NFT sales data with marketplace information and wallet addresses (buyer_address, seller_address)

### Query Structure

1. **Date Filtering CTE**
   - Filter for the past 12 months using `block_timestamp >= CURRENT_DATE - INTERVAL '12 months'`
   - Extract month from block_timestamp for grouping

2. **Marketplace Categorization CTE**
   - Standardize marketplace names using CASE statements:
     - `WHEN marketplace ILIKE '%magic eden%' THEN 'Magic Eden'`
     - `WHEN marketplace ILIKE '%tensor%' THEN 'Tensor'`
     - `ELSE 'Other'`
   - This handles variations like "magic eden v2", "magic eden v3", "tensorswap"

3. **Unique Wallets Aggregation CTE**
   - Use UNION to combine buyer_address and seller_address into a single wallet column
   - Group by month and marketplace category
   - Count DISTINCT wallets to get unique participants

4. **Monthly Totals CTE**
   - Calculate total unique wallets per month across all marketplaces
   - This will be used for percentage calculations

5. **Final Query**
   - Join wallet counts with monthly totals
   - Calculate percentage share: `(marketplace_wallets / total_monthly_wallets) * 100`
   - Pivot data to create columns for each marketplace category
   - Order by month chronologically

### Key Considerations
- **DISTINCT is critical** when counting wallets since the same wallet can appear multiple times
- **UNION (not UNION ALL)** when combining buyers and sellers to avoid double-counting wallets that both buy and sell
- **Filter succeeded = TRUE** to only include successful transactions
- **Handle NULL marketplaces** by filtering them out or categorizing as 'Other'
- **Ensure complete month coverage** by potentially using a date dimension table if there are months with no activity

### Optimization Notes
- Index usage on block_timestamp for date filtering
- Minimize data scanned by filtering early in CTEs
- Consider using DATE_TRUNC('month', block_timestamp) for cleaner month grouping

        ---

        ## 🧠 Inputs

        **Analysis Objective:** 
        Provide a normalized stacked bar chart showing the monthly distribution of unique wallets (buyers or sellers) over the past 12 months, grouped into three categories: Magic Eden, Tensor, and Other marketplaces. Each month's total should sum to 100%, reflecting the relative share of unique wallet activity per group.

        **Previous SQL Attempts:** 
        ```sql
        
        ```

        **Latest SQL Query:**
        ```sql
        WITH sales_filtered AS (
  SELECT
    DATE_TRUNC('month', block_timestamp) AS month,
    CASE 
      WHEN marketplace ILIKE '%magic eden%' THEN 'Magic Eden'
      WHEN marketplace ILIKE '%tensor%' THEN 'Tensor'
      ELSE 'Other'
    END AS marketplace_group,
    buyer_address,
    seller_address
  FROM solana.nft.ez_nft_sales
  WHERE block_timestamp >= DATEADD('month', -12, CURRENT_DATE())
    AND block_timestamp <= CURRENT_DATE()
    AND succeeded = TRUE
),
wallets_union AS (
  SELECT month, marketplace_group, buyer_address AS wallet_address
  FROM sales_filtered
  WHERE buyer_address IS NOT NULL
  UNION
  SELECT month, marketplace_group, seller_address AS wallet_address
  FROM sales_filtered
  WHERE seller_address IS NOT NULL
),
wallet_counts AS (
  SELECT
    month,
    marketplace_group,
    COUNT(DISTINCT wallet_address) AS unique_wallets
  FROM wallets_union
  GROUP BY month, marketplace_group
),
monthly_totals AS (
  SELECT
    month,
    SUM(unique_wallets) AS total_wallets
  FROM wallet_counts
  GROUP BY month
)
SELECT
  wc.month AS date_time,
  wc.marketplace_group AS category,
  ROUND((wc.unique_wallets * 100.0 / mt.total_wallets), 2) AS percentage_share
FROM wallet_counts wc
JOIN monthly_totals mt
  ON wc.month = mt.month
WHERE mt.total_wallets > 0
ORDER BY wc.month, wc.marketplace_group
        ```

        **Latest SQL Query Results:** First and last 10 rows
        |    | date_time                | category   |   percentage_share |     timestamp |
|---:|:-------------------------|:-----------|-------------------:|--------------:|
|  0 | 2024-06-01T00:00:00.000Z | Magic Eden |              56.02 | 1717200000000 |
|  1 | 2024-06-01T00:00:00.000Z | Other      |               3.77 | 1717200000000 |
|  2 | 2024-06-01T00:00:00.000Z | Tensor     |              40.22 | 1717200000000 |
|  3 | 2024-07-01T00:00:00.000Z | Magic Eden |              46.69 | 1719792000000 |
|  4 | 2024-07-01T00:00:00.000Z | Other      |               2.59 | 1719792000000 |
|  5 | 2024-07-01T00:00:00.000Z | Tensor     |              50.72 | 1719792000000 |
|  6 | 2024-08-01T00:00:00.000Z | Magic Eden |              54.98 | 1722470400000 |
|  7 | 2024-08-01T00:00:00.000Z | Other      |               2.5  | 1722470400000 |
|  8 | 2024-08-01T00:00:00.000Z | Tensor     |              42.52 | 1722470400000 |
|  9 | 2024-09-01T00:00:00.000Z | Magic Eden |              53.2  | 1725148800000 |
| 29 | 2025-03-01T00:00:00.000Z | Tensor     |              24.75 | 1740787200000 |
| 30 | 2025-04-01T00:00:00.000Z | Magic Eden |              77.45 | 1743465600000 |
| 31 | 2025-04-01T00:00:00.000Z | Other      |               0.97 | 1743465600000 |
| 32 | 2025-04-01T00:00:00.000Z | Tensor     |              21.58 | 1743465600000 |
| 33 | 2025-05-01T00:00:00.000Z | Magic Eden |              80.5  | 1746057600000 |
| 34 | 2025-05-01T00:00:00.000Z | Other      |               0.38 | 1746057600000 |
| 35 | 2025-05-01T00:00:00.000Z | Tensor     |              19.12 | 1746057600000 |
| 36 | 2025-06-01T00:00:00.000Z | Magic Eden |              83.74 | 1748736000000 |
| 37 | 2025-06-01T00:00:00.000Z | Other      |               0.33 | 1748736000000 |
| 38 | 2025-06-01T00:00:00.000Z | Tensor     |              15.92 | 1748736000000 |

        ⚠️ Red Flags to Watch For
        When analyzing whether the results seem correct, look for these issues:
        - Mostly zero values or NULL values in key columns where non-zero values are expected (counts, volumes, fees)
        - Extremely low row count when the objective suggests an active dataset
        - Duplicate rows where unique rows are expected
        - Values that seems very erratic or not consistent with other values within the same column
        - Data is cut off in a way that prevents a complete analysis
        - Values that do not match the intended metric

        🤔 Reasoning Process
        1️⃣ Analyze the query results and check whether they are correct and aligned with the analysis objective, using the red flags above. Web an twitter data is often noisy and may not be correct, so if the results are inconsistent with the web or twitter data, it still may be correct. Rely primarily on the red flags to determine if the results are correct.
        2️⃣ If the results are correct and sufficient, return a JSON object with the following fields:
        - `change_type`: 0
        - `change_summary`: an empty string
        3️⃣ If the results are incorrect or insufficient:
        - Identify what is wrong with the current query and results.
        - Decide how to rewrite it: what tables, filters, or logic to change. If the change is minor, set `change_type` to 1. If the change is more substantial, set `change_type` to 3.
        4️⃣ Output a JSON object with the following fields:
        - `change_type`: 1 or 2
        - `change_summary`: a single block of text that explains why the query / results are wrong and some potential ways to fix it.

        📝 Output
        Return only the JSON object.

        
        
"""

llm = ChatOpenAI(
    model="gpt-4.1-mini",
    openai_api_key=OPENAI_API_KEY,
    temperature=0.00,
)
complex_llm = ChatOpenAI(
    model="gpt-4.1",
    openai_api_key=OPENAI_API_KEY,
    temperature=0.00,
)
reasoning_llm_openai = ChatOpenAI(
    model="o4-mini",
    openai_api_key=OPENAI_API_KEY,
    # temperature=0.00,
)
reasoning_llm_anthropic = ChatAnthropic(
    # model="gpt-4.1",
    model="claude-opus-4-20250514",
    # model="o1",
    anthropic_api_key=ANTHROPIC_API_KEY
)
print(format(len(prompt), ','))
feedback = log_llm_call(prompt, reasoning_llm_openai, 'test', 'Test')
feedback = parse_json_from_llm(feedback, llm, True)
print(feedback)