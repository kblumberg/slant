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
from state_snapshots
order by timestamp desc
limit 1

The user requests an analysis of the market cap of stablecoins on Solana from 2021 to the present, including both aggregated and individual token data. The analysis should be conducted on a monthly basis, using the circulating supply multiplied by the price to determine market cap in USD. All available on-chain stablecoins should be included in the analysis.

{
  "analyses": [
    "\n        Metric: market cap\n        Project: solana\n        Activity: stablecoin analysis\n        Tokens: []\n        Start Time: 1609459200\n        End Time: 0\n        ",
    "\n        Metric: market cap\n        Project: solana\n        Activity: stablecoin analysis\n        Tokens: ['USDC', 'USDT', 'others']\n        Start Time: 1609459200\n        End Time: 0\n        "
  ],
  "response": "### Key Points\n\n- **Rapid Early Growth:**  \n  Between January and December 2021, the stablecoin market cap on Solana grew significantly—from around tens of millions USD for individual tokens (e.g., EPjFWdd5Aufq... around $20M in January 2021) to aggregate values exceeding $6.6B by year‐end, indicating strong early adoption.\n\n- **Consistent Dominance of Key Tokens:**  \n  Over the years, the EPjFWdd5Aufq... token consistently represented the largest share of the market cap, typically hovering around $4.98–$5.00B, while the Es9vMFrzaCER... token maintained a stable presence at approximately $1.88–$1.89B, with minor fluctuations across months.\n\n- **Emergence of New Stablecoins and Increased Aggregation:**  \n  Starting from mid-2023 into 2024 and early 2025, additional on-chain stablecoins (e.g., tokens like A1KLoBrKBde8Ty9... and HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr) were introduced. These contributed to notable increases in the aggregated market cap, which peaked above $12B in early 2025, reflecting both new entrants and an overall scaling of supply.\n\n### Summary\n\nSince 2021, the Solana on-chain stablecoin market cap has experienced substantial growth and stabilization. Early rapid increases were driven primarily by core tokens such as EPjFWdd5Aufq... and Es9vMFrzaCER..., whose consistent market shares provided a stable foundation. In recent years, the introduction of additional stablecoins has further diversified the ecosystem and driven the aggregate market cap to new highs, now exceeding $12B as of early 2025. The chart below summarizes these trends on a monthly basis, highlighting the growth trajectory and relative contributions of individual tokens over time.",
  "user_prompt": "1. can you show both aggregated and individual?\n2. monthly\n3. circulating supply multiplied by price (so in $USD terms). \n4. include all you can find",
  "flipside_sql_query": "WITH stablecoins AS (\n  SELECT \n    token_address,\n    symbol,\n    COALESCE(decimals, 6) AS decimals\n  FROM solana.price.ez_asset_metadata\n  WHERE token_address IN (\n    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',  -- USDC\n    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',  -- USDT\n    '2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo',  -- PYUSD\n    'USDSwr9ApdHk5bvJKMjzff41FfuX8bSxdKcR81vTwcA',  -- USDS\n    'A1KLoBrKBde8Ty9qtNQUtq3C2ortoC3u7twggz7sEto6',  -- USDY\n    '9zNQRsGLjNKwCUU5Gq5LR8beUCPzQMVMqKAi3SSZh54u',  -- FDUSD\n    'susdabGDNbhrnCa6ncrYo81u4s9GM8ecK2UwMyZiq4X',  -- sUSD\n    'FrBfWJ4qE5sCzKm3k3JaAtqZcXUh4LvJygDeketsrsH4',  -- ZUSD\n    'HzwqbKZw8HxMN6bF2yFZNrht3c2iXXzpKcFu7uBEDKtr'   -- EURC\n  )\n),\nmints AS (\n  SELECT \n    DATE_TRUNC('month', block_timestamp) AS date_time,\n    mint,\n    SUM(mint_amount / POWER(10, s.decimals)) AS minted\n  FROM solana.defi.fact_token_mint_actions a\n  JOIN stablecoins s ON a.mint = s.token_address\n  WHERE block_timestamp >= '2021-01-01'\n    AND succeeded = TRUE\n  GROUP BY 1, 2\n),\nburns AS (\n  SELECT \n    DATE_TRUNC('month', block_timestamp) AS date_time,\n    mint,\n    SUM(-burn_amount / POWER(10, s.decimals)) AS burned\n  FROM solana.defi.fact_token_burn_actions a\n  JOIN stablecoins s ON a.mint = s.token_address\n  WHERE block_timestamp >= '2021-01-01'\n    AND succeeded = TRUE\n  GROUP BY 1, 2\n),\ntxns AS (\n  SELECT \n    COALESCE(m.date_time, b.date_time) AS date_time,\n    COALESCE(m.mint, b.mint) AS token,\n    COALESCE(m.minted, 0) + COALESCE(b.burned, 0) AS net_amount\n  FROM mints m\n  FULL OUTER JOIN burns b\n    ON m.date_time = b.date_time\n   AND m.mint = b.mint\n),\nsupply AS (\n  SELECT \n    date_time,\n    token,\n    SUM(net_amount) OVER (PARTITION BY token ORDER BY date_time) AS circulating_supply\n  FROM txns\n),\nprice_monthly AS (\n  SELECT \n    DATE_TRUNC('month', hour) AS date_time,\n    token_address,\n    AVG(price) AS avg_price\n  FROM solana.price.ez_prices_hourly\n  WHERE hour >= '2021-01-01'\n    AND token_address IN (SELECT token_address FROM stablecoins)\n  GROUP BY 1, token_address\n),\nsupply_price AS (\n  SELECT \n    s.date_time,\n    s.token,\n    s.circulating_supply,\n    p.avg_price,\n    s.circulating_supply * p.avg_price AS market_cap\n  FROM supply s\n  JOIN price_monthly p \n    ON s.token = p.token_address\n   AND s.date_time = p.date_time\n),\naggregated AS (\n  SELECT \n    date_time,\n    'ALL' AS token,\n    SUM(market_cap) AS market_cap\n  FROM supply_price\n  GROUP BY date_time\n)\nSELECT \n  date_time,\n  token AS category,\n  circulating_supply,\n  avg_price,\n  market_cap\nFROM supply_price\nUNION ALL\nSELECT \n  date_time,\n  token AS category,\n  NULL AS circulating_supply,\n  NULL AS avg_price,\n  market_cap\nFROM aggregated\nORDER BY date_time, category;",
  "web_search_results": "USDC and USDT dominate Solana's stablecoin market, with USDC leading in supply and usage. New alternatives like EURC and PYUSD are emerging. USDC remains the most used stablecoin on Solana.",
  "analysis_description": "The user requests an analysis of the market cap of stablecoins on Solana from 2021 to the present, including both aggregated and individual token data. The analysis should be conducted on a monthly basis, using the circulating supply multiplied by the price to determine market cap in USD. All available on-chain stablecoins should be included in the analysis.",
  "flipside_example_queries": [
    "df5f6dcb-f1d4-4a06-804f-3e24aed97271",
    "e971087f-b988-42a0-8f86-9b5fcadb9b3b",
    "a927812a-0b3e-4790-ba64-81a74273b3ac",
    "3d792250-7a9c-4a41-9261-760a0a1dc33e",
    "282fa52d-4be4-421f-b6f6-bf40ce884011",
    "8114454c-e3f6-4f14-8b39-f8e6e0e2af04",
    "8366c1ba-3c6e-4720-9aea-7951a5057cfe",
    "447f46d2-0834-4a18-a387-d3b1811dcb6a",
    "04655e9a-3d5d-47e2-95d4-5f3589fda708",
    "1e98ba4a-c8c6-4b55-82a5-c53c7e74b86c",
    "f1eac887-787b-48fc-abcf-17c846a63512",
    "54cbcf0e-e58e-42c6-9cbe-42f1b0d93724",
    "dbd35208-fc06-43a9-832c-1dd11c119fca",
    "605550a7-e046-477b-9473-ccee034b0c2a",
    "b2d73c30-516d-4560-afe8-b4c2a0934e65"
  ]
}