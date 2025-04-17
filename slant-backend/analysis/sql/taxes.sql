
with prices as (
    select hour::date as date
    , token_address
    , symbol
    , name
    , avg(price) as price
    from solana.price.ez_prices_hourly
    where hour::date between '2024-01-01' and '2025-01-01'
    group by 1, 2, 3, 4
), t0 as (
    select swap_from_mint as mint
    , symbol
    , name
    , sum(swap_from_amount * price) as swap_from_amount_usd
    from solana.defi.fact_swaps_jupiter_summary j
    left join prices p
        on j.swap_from_mint = p.token_address
        and j.hour::date = p.date
    where block_timestamp >= '2024-01-01'
        and block_timestamp < '2025-01-01'
        and swapper in (
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
            , 'kcitfabbdz4KFNPn8RJjUEg1rD36SJdFwJg8imo2Rrg'
            , 'runkuDMGfyVapyBXKJQyAGoMgQxwKU2Ba2qZy9BFj1n'
            , 'runHLSv8jWNaqUrXgxp4L9gMUC7rBm19ZAXdXNHa4CX'
            , 'runpZTPawgvyKcUkbZrgGj9uPjBkzL5VGMzpS9L58TC'
        )
    group by 1, 2, 3
)
select *
from t0
