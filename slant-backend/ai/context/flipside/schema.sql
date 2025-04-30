
Here is the schema, tables, and columns for the Flipside database. Use this to understand the available tables and columns.

================================================================================

Performance Notes:
- If you have a `block_timestamp` filter or join, ALWAYS put that first in the WHERE clause to optimize performance.
- ALWAYS filter and join on `block_timestamp` when possible and appropriate to optimize performance.
- Even when you are filtering one table for `block_timestamp`, filter the other table for `block_timestamp` as well to improve performance. e.g. `FROM table1 JOIN table2 ON table1.block_timestamp = table2.block_timestamp AND table1.tx_id = table2.tx_id WHERE table1.block_timestamp >= DATEADD(day, -30, CURRENT_DATE()) AND table2.block_timestamp >= DATEADD(day, -30, CURRENT_DATE())`

================================================================================

Schema: core
Table: solana.core.dim_labels
Purpose: Provides labels for Solana addresses (programs, accounts, etc.).
Key Columns:
- address
- address_name (e.g. binance deposit_wallet, coinbase deposit_wallet, bybit deposit_wallet, stepn: token contract, gate.io deposit_wallet, kucoin deposit_wallet, okx deposit_wallet, backpack exchange deposit_wallet, kraken deposit_wallet, mexc deposit_wallet)
- label_type (e.g. nft, cex, dex, games, token, operator, dapp, defi, bridge, chadmin)
- label_subtype (e.g. nf_token_contract, deposit_wallet, token_contract, pool, aggregator_contract, validator, governance, general_contract, router, bridge)
- label (e.g. binance, coinbase, bybit, raydium, stepn, gate.io, kucoin, okx, backpack exchange, kraken, mexc)

Table: solana.core.ez_events_decoded
Purpose: Provides decoded event data from program instructions. Use this table to extract data from events emitted by specific program ids.
Key Columns:
- block_timestamp
- tx_id
- signers
- succeeded
- index
- inner_index
- event_type (e.g. swapV2, swapBaseIn, buy, swap, sell, route, placeOrderPegged, flashBorrowReserveLiquidity, flashRepayReserveLiquidity, CancelUpToWithFreeFunds)
- program_id
- decoded_instruction
- decoded_accounts
- decoded_args
- decoding_error


Table: solana.core.fact_transactions
Purpose: Provides detailed transaction information.
Key Columns:
- block_timestamp
- block_id
- tx_id
- signers
- fee: Transaction fee (in lamports)
- succeeded
- account_keys: List of accounts that are referenced by pre/post sol/token balances objects
- pre_balances
- post_balances
- pre_token_balances
- post_token_balances
- instructions
- inner_instructions
- log_messages
- units_consumed: The number of compute units consumed by the program.
- units_limit: The max number of compute units that can be consumed by the program.
- tx_size: The size of the transaction in bytes.
- tx_index: The index of the transaction in the block. Index of 0 is the first transaction executed in the block.


Table: solana.core.fact_transfers
Purpose: Tracks SOL and SPL token transfers.
Key Columns:
- block_timestamp
- block_id
- tx_id
- index
- tx_from
- tx_to
- amount
- mint


Table: solana.core.fact_decoded_instructions
Purpose: Provides decoded instruction data based on program IDLs.
Key Columns:
- tx_id
- block_id
- block_timestamp
- program_id
- signers
- index
- inner_index
- event_type (e.g. swapV2, swapBaseIn, buy, swap, sell, route, placeOrderPegged, flashRepayReserveLiquidity, flashBorrowReserveLiquidity, CancelUpToWithFreeFunds)
- decoded_instruction


Table: solana.core.fact_events
Purpose: Records inner instructions generated during transaction execution.
Key Columns:
- block_timestamp
- block_id
- tx_id
- signers
- succeeded
- index
- event_type (e.g. transfer, closeAccount, initializeAccount, createAccountWithSeed, createIdempotent, advanceNonce, create, syncNative, transferChecked, burn)
- program_id
- instruction
- inner_instruction

Table: solana.core.fact_events_inner
Purpose: Records inner instructions generated during transaction execution.
Key Columns:
- block_timestamp
- tx_id
- signers
- succeeded
- instruction_index: Location of the instruction (event) within a transaction
- inner_index: Location of the instruction within an instructions (event) inner instruction
- instruction_program_id: An address that identifies the program that is being interacted with. I.E. which DEX for a swap or marketplace for an NFT sale.. For the instruction calling this inner instruction.
- program_id: An address that identifies the program that is being interacted with. I.E. which DEX for a swap or marketplace for an NFT sale.
- event_type (e.g. transferChecked, transfer, createAccount, initializeAccount3, getAccountDataSize, initializeImmutableOwner, mintTo, approve, closeAccount, burn)
- instruction: Specifies which program it is calling, which accounts it wants to read or modify, and additional data that serves as auxiliary input to the program


Table: solana.core.fact_sol_balances
Purpose: Records the balance of SOL in an account before and after a transaction.
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- account_address: The account address which holds a specific token.
- mint
- owner: The wallet holding the native SOL. This is represented as the same value as the 'account_address'.
- pre_balance: The initial decimal-adjusted amount in an account.
- balance: The final decimal-adjusted amount in an account.


Table: solana.core.fact_token_account_owners
Purpose: Records the owner of a token account.
Key Columns:
- account_address: Address of token account
- owner: The wallet holding the native SOL. This is represented as the same value as the 'account_address'.
- start_block_id: Block where this ownership begins
- end_block_id: Block where this ownership ends, null value represents current ownership


Table: solana.core.fact_token_balances
Purpose: Records the balance of a token account.
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- account_address: The account address which holds a specific token.
- mint
- owner: The final owner of the token account within the block.
- pre_balance: The initial decimal-adjusted amount in an account.
- balance: The final decimal-adjusted amount in an account.



Table: solana.core.ez_signers
Purpose: 1 row per signer. Summarizes the signers activity across all days.
Key Columns:
- signer
- first_tx_date: The first date that the wallet performed a transaction on.
- first_program_id: The ID of the first program this signer interacted with, excluding chain admin programs.
- last_tx_date: The date of the most recent transaction the signer has performed.
- last_program_id: The ID of the most recent program this signer interacted with, excluding chain admin programs.
- num_days_active: A count of the total number of unique days that this signer has performed a transaction.
- num_txs: The total number of distinct transactions initiated by this signer.
- total_fees: The total amount of fees (in lamports) that the signer has paid on a given day. This field can be null, as only the first signer pays fees in a transaction.
- programs_used: An array containing all program IDs a user interacted with on a given day.


================================================================================


Schema: nft


Table: solana.nft.ez_nft_sales (default to using this table for NFT sales)

Purpose: Records NFT sales

Notes:
- Use this table instead of `solana.nft.fact_nft_sales`
- For the `marketplace` column, group similar values together - e.g. `case when marketplace ilike '%magic eden%' then 'Magic Eden' when marketplace ilike '%tensor%' then 'Tensor'...` (use this technique unless there are explicit instructions to the contrary)

Key Columns:
- marketplace (e.g. tensorswap, magic eden v3, magic eden v2, Magic Eden, solsniper, tensor, hadeswap, hyperspace, exchange art, solanart)
- marketplace_version (e.g. v1, v2, v3)
- block_timestamp
- block_id
- tx_id
- succeeded
- index
- inner_index
- program_id
- buyer_address
- seller_address
- mint: address that uniquely identifies the NFT
- nft_name
- price: The amount of Solana the NFT was purchased for (if you are using this column, best to filter for currency_address = "So11111111111111111111111111111111111111111" as well to ensure you are getting SOL volume)
- currency_address (typically "So11111111111111111111111111111111111111111", others include "3dgCCb15HMQSA4Pn3Tfii5vRk7aRqTH95LJjxzsG2Mug" honeland, "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" usdc, and "MEFNBXixkEbait3xn9bkm8WsJzXtVsaJEn4c8Sam21u" $ME magic eden)
- currency_symbol (typically "SOL", others include "HXD", "USDC", and "ME")
- price_usd
- is_compressed
- nft_collection_name
- creators, VARIANT: Creators of the NFT and what percentage of royalties they receive
- authority: Authority address for the mint. When editions are minted, the authority remains the one from the master NFT
- metadata, ARRAY: a block of json that describes the traits of an NFT
- image_url
- metadata_uri: URL that links to the token metadata on the ipfs service


Table: solana.nft.dim_nft_metadata
Purpose: Stores metadata and attributes for NFTs on Solana, including collection information, creator details, and NFT characteristics
Key Columns:
- mint
- nft_collection_name
- collection_id
- creators
- authority
- metadata, ARRAY
- image_url
- metadata_uri
- nft_name


Table: solana.nft.fact_nft_mints
Purpose: Tracks NFT minting events
Key Columns:
- block_id
- block_timestamp
- tx_id
- succeeded
- program_id
- purchaser
- mint_price
- mint_currency
- mint
- is_compressed


Table: solana.nft.fact_nft_mint_actions
Purpose: Tracks NFT minting events and actions
Key Columns:
- block_id
- block_timestamp
- tx_id
- succeeded
- index
- inner_index
- event_type (e.g. mintTo, initializeMint2, initializeMint, mintToChecked, initializeNonTransferableMint)
- mint
- mint_amount
- mint_authority
- signers
- mint_standard_type


Table: solana.nft.fact_nft_burn_actions
Purpose: Records NFT burns
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- index
- inner_index
- event_type (e.g. burn, burnChecked)
- mint
- burn_amount
- burn_authority
- signers
- mint_standard_type



Table: solana.nft.fact_nft_sales (**this table is deprecated, use `solana.nft.ez_nft_sales` instead**)
Purpose: Records NFT sales
Key Columns:
- marketplace (e.g. tensorswap, magic eden v3, magic eden v2, Magic Eden, solsniper, tensor, hadeswap, hyperspace, exchange art, solanart)
- block_timestamp
- block_id
- tx_id
- succeeded
- index
- inner_index
- program_id
- purchaser
- seller
- mint
- sales_amount
- is_compressed
- currency_address


================================================================================


Schema: price

Schema Notes for `solana.price.ez_prices_hourly` and `solana.price.ez_asset_metadata`:
- Does NOT contain data for all tokens; if you are looking for a token that is not in this table, you can use the `solana.defi.ez_dex_swaps` table to get the price
- When joining or filtering for analysis, ALWAYS use `token_address` - DO NOT USE `symbol` or `name` to filter because multiple tokens can have the same symbol or name
- When displaying data to the user, though, use `symbol` or `name` to make it more readable
- If doing exploratory analysis, you can use `symbol` or `name` to filter to then use `token_address` for final analysis


Table: solana.price.ez_prices_hourly
Purpose: Provides price data for tokens on Solana
Notes:
- If the token is not in `solana.price.ez_prices_hourly`, you can calculate the price using the `solana.defi.ez_dex_swaps` table instead
Key Columns:
- hour
- token_address (SOL is "So11111111111111111111111111111111111111112" in this table)
- symbol
- name
- price
- decimals
- is_native: True for SOL
- is_imputed: If price was estimated/carried forward


Table: solana.price.ez_asset_metadata
Purpose: Provides metadata for tokens on Solana
Key Columns:
- asset_id
- token_address
- symbol
- name
- decimals
- is_native (True for SOL)


================================================================================


Schema: defi

Table: solana.defi.ez_dex_swaps
Purpose: Records DEX swaps

Key Columns:
- swap_program: Name of DEX program (e.g., 'Raydium Liquidity Pool V4', 'phoenix')
- block_id
- block_timestamp
- tx_id
- program_id
- swapper
- swap_from_mint
- swap_from_symbol (can be null; best to use `coalesce(swap_from_symbol, swap_from_mint)` to get the symbol)
- swap_from_amount
- swap_from_amount_usd (can be null if flipside doesnt have price data; therefore, always use `coalesce(swap_from_amount_usd, swap_to_amount_usd)` to get the usd value)
- swap_to_mint
- swap_to_symbol (can be null; best to use `coalesce(swap_to_symbol, swap_to_mint)` to get the symbol)
- swap_to_amount
- swap_to_amount_usd (can be null if flipside doesnt have price data; therefore, always use `coalesce(swap_to_amount_usd, swap_from_amount_usd)` to get the usd value)
- _log_id: Combination of TX_ID and event index
- ez_swaps_id: Unique identifier for each row

Notes:
- Use this table instead of `fact_swaps`
- Only shows swaps on the underlying DEX program (e.g., 'Raydium Liquidity Pool V4', 'phoenix'); to get Jupiter aggregator swaps use `fact_swaps_jupiter_inner` or `fact_swaps_jupiter_summary`
- `swap_from_amount_usd` and `swap_to_amount_usd` will generally be roughly the same values (except for fees)
- Because we dont have price data for all tokens, it is best practice to use coalescing functions to get the price (e.g., `COALESCE(swap_from_amount_usd, swap_to_amount_usd)` or `COALESCE(swap_to_amount_usd, swap_from_amount_usd)`)
- If we dont have price data for `swap_from_mint` then `swap_from_amount_usd` will be null (in this case, you can use `swap_to_amount_usd` to get usd value)
- If we dont have price data for `swap_to_mint` then `swap_to_amount_usd` will be null (in this case, you can use `swap_from_amount_usd` to get usd value)
- `solana.defi.ez_dex_swaps` only includes successful swaps by default. There is NO `succeeded` column. If you need to include failed swaps, use `fact_swaps` instead.




Table: solana.defi.fact_swaps
Purpose: Records DEX swaps
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- swapper
- swap_from_amount
- swap_from_mint
- swap_to_amount
- swap_to_mint
- program_id
- swap_program

Table: solana.defi.ez_liquidity_pool_actions
Purpose: Records liquidity pool actions
Key Columns:
- block_timestamp
- tx_id
- index: Location of the instruction (event) within a transaction
- inner_index: Location of the instruction within an instructions (event) inner instruction
- action_type: Type of LP action (`deposit` or `withdraw`)
- provider_address: Wallet address initiating the deposit/withdraw
- token_a_mint: Address of the mint representing the first token in a liquidity pool pair
- token_a_symbol
- token_a_amount
- token_a_amount_usd
- token_b_mint: Address of the mint representing the second token in a liquidity pool pair
- token_b_symbol
- token_b_amount
- token_b_amount_usd
- token_c_mint: Address of the mint representing the third token in a liquidity pool pair
- token_c_symbol
- token_c_amount
- token_c_amount_usd
- token_d_mint: Address of the mint representing the fourth token in a liquidity pool pair
- token_d_symbol
- token_d_amount
- token_d_amount_usd
- pool_address
- pool_name
- program_id
- platform (e.g. orca, raydium, meteora)


Table: solana.defi.fact_liquidity_pool_actions
Purpose: Records liquidity pool actions
Key Columns:
- block_id
- block_timestamp
- tx_id
- succeeded
- program_id
- action: Type of LP action (e.g., withdraw, deposit, addLiquidityByStrategy, removeLiquidityByRange, addLiquidityByWeight, mintTo, burn, removeBalanceLiquidity, addBalanceLiquidity, removeLiquidity)
- liquidity_provider
- liquidity_pool_address
- amount
- mint


Table: solana.defi.fact_bridge_activity
Purpose: Records bridge activity
Key Columns:
- block_id
- block_timestamp
- tx_id
- succeeded
- index
- program_id
- platform (e.g. `mayan finance`, `deBridge`, `wormhole`)
- direction (e.g. `inbound` or `outbound`)
- user_address
- amount
- mint


Table: solana.defi.fact_token_burn_actions
Purpose: Records token burns
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- index
- inner_index
- event_type (e.g. burn, burnChecked)
- mint
- burn_amount
- burn_authority
- token_account
- signers
- decimal
- mint_standard_type: The type of mint following Metaplex mint standards

Table: solana.defi.fact_token_mint_actions
Purpose: Records token mints
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- index
- inner_index
- event_type (e.g. mintTo, initializeMint2, initializeMint, mintToChecked, initializeNonTransferableMint)
- mint
- mint_authority
- mint_amount
- token_account
- signers
- decimal
- mint_standard_type: The type of mint following Metaplex mint standards


Table: solana.defi.fact_swaps_jupiter_inner
Purpose: Records Jupiter DEX swaps
Key Columns:
- block_timestamp
- block_id
- tx_id
- index
- inner_index
- swap_index: Order in which the intermediate swap was executed as it relates to the top level Jupiter swap instruction
- succeeded
- swapper
- swap_from_mint
- swap_from_amount
- swap_to_mint
- swap_to_amount
- swap_program_id: This is the AMM performing the swap
- aggregator_program_id: This is the aggregator calling the different AMMs


Table: solana.defi.fact_swaps_jupiter_summary
IMPORTANT: Use this table to get Jupiter DEX swaps, the solana.defi.ez_dex_swaps table only shows the underlying swaps, but not the fact that Jupiter router was used
Purpose: Records Jupiter DEX swaps
Key Columns:
- block_timestamp
- block_id
- tx_id
- index
- inner_index
- swap_index: Order in which the intermediate swap was executed as it relates to the top level Jupiter swap instruction. This value is 0 for Jupiter v4 swaps prior to 2023-10-31
- succeeded
- swapper
- swap_from_mint
- swap_from_amount
- swap_to_mint
- swap_to_amount
- program_id
- is_dca_swap: Whether the swap was initiated by a Jupiter DCA
- dca_requester: Original address that requested the DCA swap
- is_limit_swap: Whether the swap was initiated by a Jupiter limit order
- limit_requester: Original address that requested the limit order


Table: solana.defi.fact_stake_pool_actions
Purpose: Records stake pool actions (ONLY for the $SOL native token)
Key Columns:
- stake_pool_name (e.g. `jito`, `jpool`, `blazestake`, `daopool`, `marinade`)
- tx_id
- block_timestamp
- block_id
- index
- succeeded
- action (e.g. `deposit`, `withdraw`, `deposit_stake`, `withdraw_stake`, `order_unstake`, `claim`)
- address: Wallet address initiating the deposit/stake or withdraw/unstake
- stake_pool: Address for a given stake pool
- amount: Amount in Lamports
- token: Token utilized in the stake pool action


================================================================================



Schema: gov

IMPORTANT: All `solana.gov` tables only include staking $SOL on validators; not staking any other tokens.

Table: solana.gov.ez_staking_lp_actions
Purpose: Records staking actions for Solana validators.
Key Columns:
- block_timestamp
- block_id
- tx_id
- index
- inner_index
- commission: The percentage of staked earnings given to the validator.
- succeeded
- node_pubkey: A unique key belonging to the validator node.
- event_type (e.g. withdraw, moveLamports, merge_source, merge_destination, deactivate, split_destination, split_source, delegate, authorize, initialize)
- pool_address
- signers
- stake_authority
- withdraw_authority
- stake_account
- stake_active
- pre_tx_staked_balance
- post_tx_staked_balance
- withdraw_amount
- withdraw_destination
- vote_account
- validator_name (e.g. "validator: joogh validator - 0% fee - top 10 by returns - high uptime", "validator: h68e2xudxk3j8ibfv61vymvhn6aehhphlzsmbbqnzp1m", "36MVUhntTiTY7nsLyoCdRj4wbs2rvw2nPEZiM5XCkJLb")
- validator_rank: The rank of the validator by amount of delegated SOL.
- move_destination: The destination wallet address of the moved SOL.
- move_amount: The amount of SOL moved.


Table: solana.gov.fact_block_production
Purpose: Records block production
Key Columns:
- epoch
- node_pubkey
- num_leader_slots: Number of slots the validator was the leader for in the epoch
- num_blocks_produced: Number of blocks the validator produced in the epoch
- start_slot: The first slot in the epoch
- end_slot: The last slot in the epoch


Table: solana.gov.fact_gauges_creates
Purpose: Records gauge creation
Key Columns:
- program_name
- block_timestamp
- block_id
- tx_id
- succeeded
- signer
- gauge: Address that determines the rewards share to give to a liquidity pool, or in the case of Marinade, the amount of SOL that should be delegated to a given validator
- gaugemeister: Address that manages the rewards shares of all gauges
- validator_account: Validator vote key linked to a gauge


Table: solana.gov.fact_gauges_votes
Purpose: Records gauge votes
Key Columns:
- program_name
- block_timestamp
- block_id
- tx_id
- succeeded
- voter: Address performing the vote
- voter_nft: NFT representing tokens for governance. This is only available for certain types of governance.
- gauge
- power: Total voting power of the voter. This is only available for certain types of gauge votes.
- delegated_shares: Number of shares delegated to vote for this gauge


Table: solana.gov.fact_gov_actions
Purpose: Records governance actions
Key Columns:
- program_name
- block_timestamp
- block_id
- tx_id
- succeeded
- signer
- locker_account: Account holding tokens for governance
- locker_nft: NFT representing tokens for governance. This is only available for certain types of governance.
- mint: Mint being locked or exited
- action
- amount


Table: solana.gov.fact_proposal_creation
Purpose: Records proposal creation
Key Columns:
- governance_platform: platform used for governance space
- program_name (e.g. `GovER5Lthms3bLBqWub97yVrMmEogzX7xNjdXpPPCVZw`)
- block_timestamp
- block_id
- tx_id
- succeeded
- realms_id: An address that is unique to the space or voting group on Realms.
- proposal: Address representing the proposal being voted on.
- proposal_writer: Address of the user who is submitting the proposal for voting.
- proposal_name: The name of the proposal that is being submitted
- vote_type: The type of voting strategy that will be used for voting on the proposal. (e.g.  `SingleChoice`, `MultiChoice { choice_type: FullWeight, min_voter_options: 1, max_voter_options: 2, max_winning_options: 2 }`)
- vote_options: The options that will be available to users who are voting on the proposal


Table: solana.gov.fact_proposal_votes
Purpose: Records proposal votes
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- proposal_address
- voter
- vote
- vote_weight
- governing_token_owner
- program_id


Table: solana.gov.fact_rewards_fee
Purpose: Records fee rewards
Key Columns:
- block_timestamp
- block_id
- dim_epoch_id
- pubkey: The address receiving the fee rewards
- epoch_earned: The epoch that rewards were recieved for. Staking and voting rewards for an epoch are recieved in the first block of the following epoch, so this would identify the prior epoch, which is where the rewards were determined. Fee and rent rewards are determined in the epoch they are recieved.
- reward_amount_sol
- post_balance_sol: The amount of SOL in the account after event


Table: solana.gov.fact_rewards_rent
Purpose: Records rent rewards
Key Columns:
- block_timestamp
- block_id
- vote_pubkey: Account address of voter
- epoch_earned: The epoch that rewards were recieved for. Staking and voting rewards for an epoch are recieved in the first block of the following epoch, so this would identify the prior epoch, which is where the rewards were determined. Fee and rent rewards are determined in the epoch they are recieved.
- reward_amount_sol
- post_balance_sol
- dim_epoch_id


Table: solana.gov.fact_rewards_staking
Purpose: Records staking rewards
Key Columns:
- block_timestamp
- block_id
- stake_pubkey: Address of stake account
- epoch_earned
- reward_amount_sol
- post_balance_sol
- dim_epoch_id


Table: solana.gov.fact_rewards_voting
Purpose: Records voting rewards
Key Columns:
- block_timestamp
- block_id
- vote_pubkey
- epoch_earned
- reward_amount_sol
- post_balance_sol
- dim_epoch_id


Table: solana.gov.fact_validators
Purpose: Records validators
Key Columns:
- epoch: The epoch when data was recorded
- node_pubkey
- vote_pubkey
- active_stake: Amount staked in SOL
- admin_warning: Whether the validator is in admin warning
- commission: % of rewards payout to the vote account
- created_at: Date and time when the validator was created
- data_center_host
- data_center_key
- delinqent: Status whether the validator is offline/delinquent
- details
- epoch_active: last epoch when vote account was active
- epoch_credits
- keybase_id
- latitude
- longitude
- validator_name (e.g. Hard Yaka, Crypto Plant, Laine, stakewiz.com, MonkeDAO, InfStones, Alenka, Decommissioned Validator, Lido / RockX)
- software_version
- updated_at: Date and time when the validator was last updated
- www_url: URL for the validator


Table: solana.gov.fact_votes_agg_block
Purpose: Records vote aggregation by block
Key Columns:
- block_timestamp
- block_id
- num_votes: The number of vote events that occurred within the block


Table: solana.gov.fact_vote_accounts
Purpose: Records vote accounts
Key Columns:
- epoch
- vote_pubkey
- node_pubkey
- authorized_voter
- authorized_withdrawer
- commission
- epoch_credits, ARRAY
- last_epoch_active: last epoch when vote account was active
- last_timestamp_slot: Last slot voted on
- prior_voters, ARRAY: Prior voters for the vote account
- root_slot: latest slot confirmed
- votes, ARRAY: Votes during epoch
- account_sol: SOL assigned to this account
- owner: Program account that owns the vote account
- rent_epoch: Epoch at which this account will next owe rent


Table: solana.gov.dim_epoch
Purpose: Records epochs
Key Columns:
- dim_epoch_id
- epoch: A period of time consisting of 432,000 blocks, which represents a higher-level timekeeping unit within the Solana network
- start_block: The first block within an Epoch
- end_block: The last block within an Epoch


Table: solana.gov.fact_stake_accounts
Purpose: Records stake accounts
Key Columns:
- epoch
- stake_pubkey
- vote_pubkey
- authorized_staker: Account responsible for signing stake delegations/deactivativations transactions
- authorized_withdrawer: Account responsible for signing withdraw transactions
- lockup: Lockup information when tokens can be withdrawn
- rent_exempt_reserve: Minimum SOL balance that must be maintained for this account to remain rent exempt
- credits_observed: Credits observed for the validator
- activation_epoch
- deactivation_epoch
- active_stake: Amount staked in SOL
- warmup_cooldown_rate: Rate at which stake can be activated/deactivated
- type_stake
- program
- account_sol: SOL held in this account
- rent_epoch: Epoch at which this account will next owe rent


================================================================================


Schema: stats

Table: solana.stats.ez_core_metrics_hourly
Purpose: Records core metrics
Key Columns:
- hour
- active_addresses
- new_addresses
- total_transactions
- successful_transactions: Number of successful transactions
- failed_transactions: Number of failed transactions
- total_fees: Total fees paid in lamports
- average_fee: Average fee per transaction
- total_compute_units: Total compute units consumed
- average_compute_units: Average compute units per transaction
- total_program_calls: Total number of program invocations
- unique_program_calls: Number of unique programs called
- native_transfers_amount: Total SOL transferred
- native_transfers_count: Number of SOL transfers


================================================================================


Schema: crosschain

Table: crosschain.core.dim_dates
Purpose: A reference table for dates to be used in joins
Key Columns:
- date_day
- prior_date_day
- next_date_day
- day_of_week_name (Monday, Tuesday, etc.)
- day_of_week_name_short (Mon, Tue, etc.)


================================================================================


General Notes:
- to get current circulating supply, take `mint_amount` from `fact_token_mint_actions` and then subtract `burn_amount` from `solana.defi.fact_token_burn_actions`
- market cap = price * circulating supply


================================================================================

Strategies to identify transactions:
Many analyses will require you to identify and parse the specific transactions types. This means creating queries, joins, and filters to get the data you need.


The easiest and most efficient way to identify and parse the correct transactions you are looking for:
- For swap transactions, use the `solana.defi....` tables. Those are curated tables that have already parsed the correct information.
- For nft transactions, use the `solana.nft....` tables. Those are curated tables that have already parsed the correct information.
- For other transactions, here are the ways to identify the correct transactions, ordered by preference:
  1. use the `solana.core.ez_events_decoded` table using the `program_id` filter.
    - This table is very optimized, fast, and has parsed data very cleanly, but is only curated for certain program ids and may not have data going back in time.
    - If there is data for the `program_id` and correct date range you are looking for, this is the best way to go.
    - BUT, you MUST make sure that it has data for the `program_id` and correct date range you are looking for.
    - To ensure it does, you can cross reference the `solana.core.fact_events` table, which has all entries for all program ids for all dates.
  2. If that does not work, you can typically use some combination of the other tables:
  use the `solana.core.fact_transactions` table and filter for the correct program id.
  3. use the `solana.core.fact_decoded_instructions` table and filter for the correct program id.
  - use the `solana.core.fact_events` table and filter for the correct event type.


================================================================================

Make sure the query includes ALL desired transaction types and excludes all others.