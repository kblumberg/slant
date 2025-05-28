Schema: core
Table: solana.core.dim_labels
Purpose: Provides labels for Solana addresses (programs, accounts, etc.).
Key Columns:
- address
- address_name
- label_type
- label_subtype
- label


Table: solana.core.ez_events_decoded
Purpose: Provides decoded event data from program instructions.
Key Columns:
- signers
- index
- inner_index
- event_type
- program_id
- decoded_instruction
- decoded_accounts
- decoded_args
- decoding_error
- ez_events_decoded_id


Table: solana.core.fact_transactions
Purpose: Provides detailed transaction information.
Key Columns:
- signers
- fee
- account_keys
- pre_balances
- post_balances
- pre_token_balances
- post_token_balances
- instructions
- inner_instructions
- log_messages
- units_consumed
- units_limit
- tx_size


Table: solana.core.fact_transfers
Purpose: Tracks SOL and SPL token transfers.
Key Columns:
- index
- tx_from
- tx_to
- amount
- mint


Table: solana.core.fact_decoded_instructions
Purpose: Provides decoded instruction data based on program IDLs.
Key Columns:
- program_id
- signers
- index
- inner_index
- event_type
- decoded_instruction


Table: solana.core.fact_events
Purpose: Records inner instructions generated during transaction execution.
Key Columns:
- signers
- index
- event_type
- program_id
- instruction
- inner_instruction


================================================================================


Schema: nft

Table: solana.nft.ez_nft_sales (default to using this table for NFT sales)
Purpose: Records NFT sales
Key Columns:
- marketplace
- marketplace_version
- block_timestamp
- tx_id
- program_id
- buyer_address
- seller_address
- mint
- nft_name
- price (if you are using this column, best to filter for currency_address = "So11111111111111111111111111111111111111111" as well to ensure you are getting SOL volume)
- currency_address (typically "So11111111111111111111111111111111111111111")
- price_usd
- is_compressed
- nft_collection_name


Table: solana.nft.dim_nft_metadata
Purpose: Stores metadata and attributes for NFTs on Solana, including collection information, creator details, and NFT characteristics
Key Columns:
- mint
- nft_collection_name
- collection_id
- creators
- authority
- metadata
- image_url
- metadata_uri
- nft_name


Table: solana.nft.fact_nft_mints
Purpose: Tracks NFT minting events
Key Columns:
- program_id
- purchaser
- mint_price
- mint_currency
- mint
- is_compressed


Table: solana.nft.fact_nft_mint_actions
Purpose: Tracks NFT minting events and actions
Key Columns:
- index
- inner_index
- event_type
- mint
- mint_amount
- mint_authority
- signers
- decimal
- mint_standard_type


Table: solana.nft.fact_nft_burn_actions
Purpose: Records NFT burns
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- program_id
- mint
- burn_authority
- amount


================================================================================


Schema: price

Table: solana.price.ez_prices_hourly
Purpose: Provides price data for tokens on Solana
Key Columns:
- hour
- token_address
- symbol
- name
- price
- decimals
- is_native: True for SOL
- is_imputed: If price was estimated/carried forward


Table: solana.price.dim_asset_metadata
Purpose: Provides metadata for tokens on Solana
Key Columns:
- token_address
- symbol
- name
- decimals
- is_native (True for SOL)


================================================================================


Schema: defi

Table: solana.defi.ez_dex_swaps
Purpose: Records DEX swaps
IMPORTANT: Use this table instead of fact_swaps
Key Columns:
- swap_program: Name of DEX program (e.g., 'Raydium Liquidity Pool V4', 'phoenix') - note: excludes jupiter swaps (use fact_swaps_jupiter_inner instead)
- block_id
- block_timestamp
- tx_id
- program_id
- swapper
- swap_from_mint
- swap_from_symbol
- swap_from_amount
- swap_from_amount_usd
- swap_to_mint
- swap_to_symbol
- swap_to_amount
- swap_to_amount_usd


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


Table: solana.defi.fact_liquidity_pool_actions
Purpose: Records liquidity pool actions
Key Columns:
- block_id
- block_timestamp
- tx_id
- succeeded
- program_id
- action: Type of LP action (e.g., 'addLiquidityByStrategy')
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
- token_address
- amount
- destination_chain
- source_chain
- bridge_address


Table: solana.defi.fact_token_burn_actions
Purpose: Records token burns
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- mint
- burn_authority
- amount
- program_id


Table: solana.defi.fact_token_mint_actions
Purpose: Records token mints
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- mint
- mint_authority
- amount
- recipient: Account receiving minted tokens
- program_id


Table: solana.defi.fact_swaps_jupiter_inner
Purpose: Records Jupiter DEX swaps
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- program_id
- user
- input_mint
- output_mint
- in_amount
- out_amount
- route_type


Table: solana.defi.fact_swaps_jupiter_summary
IMPORTANT: Use this table to get Jupiter DEX swaps, the solana.defi.ez_dex_swaps table only shows the underlying swaps, but not the fact that Jupiter router was used
Purpose: Records Jupiter DEX swaps
Key Columns:
- block_timestamp
- block_id
- tx_id
- swapper
- swap_from_mint
- swap_from_amount
- swap_to_mint
- swap_to_amount
- program_id
- is_dca_swap
- is_limit_swap


Table: solana.defi.fact_stake_pool_actions
Purpose: Records stake pool actions
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- pool_address
- action_type
- staker
- validator
- amount
- program_id


================================================================================



Schema: gov


Table: solana.gov.ez_staking_lp_actions
Purpose: Records staking actions for Solana validators. Only includes validator staking; not any other types of staking.
Key Columns:
- block_timestamp
- tx_id
- succeeded
- event_type
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
- validator_name


Table: solana.gov.fact_block_production
Purpose: Records block production
Key Columns:
- block_timestamp
- block_id
- validator_id
- slot
- leader_slots
- blocks_produced
- skipped_slots


Table: solana.gov.fact_gauges_creates
Purpose: Records gauge creation
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- gauge_address
- creator
- pool_address
- program_id


Table: solana.gov.fact_gauges_votes
Purpose: Records gauge votes
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- voter
- gauge_address
- vote_weight
- program_id


Table: solana.gov.fact_gov_actions
Purpose: Records governance actions
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- program_id
- governance_address
- action_type
- proposer
- parameters


Table: solana.gov.fact_proposal_creation
Purpose: Records proposal creation
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- proposal_address
- governance_address
- proposer
- vote_type
- parameters
- program_id


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
- tx_id
- validator
- amount
- epoch


Table: solana.gov.fact_rewards_rent
Purpose: Records rent rewards
Key Columns:
- block_timestamp
- block_id
- tx_id
- recipient
- amount
- epoch


Table: solana.gov.fact_rewards_staking
Purpose: Records staking rewards
Key Columns:
- block_timestamp
- block_id
- tx_id
- validator
- staker
- amount
- epoch


Table: solana.gov.fact_rewards_voting
Purpose: Records voting rewards
Key Columns:
- block_timestamp
- block_id
- tx_id
- validator
- amount: Reward amount in lamports
- epoch
- commission: Validators commission percentage


Table: solana.gov.fact_staking_lp_actions
Purpose: Records staking pool actions
Key Columns:
- block_timestamp
- block_id
- tx_id
- succeeded
- program_id
- action_type: Type of staking action (stake, unstake)
- pool_address
- staker_address
- amount: Amount of LP tokens staked/unstaked


Table: solana.gov.fact_validators
Purpose: Records validators
Key Columns:
- block_timestamp
- block_id
- validator_address: Validators vote account
- node_pubkey: Validators identity pubkey
- commission: Current commission percentage
- epoch_credits: Credits earned in current epoch
- root_slot
- last_vote: Slot of last vote
- activated_stake: Total SOL staked with validator
- total_active_stake: Total SOL staked across all validators


Table: solana.gov.fact_votes_agg_block
Purpose: Records vote aggregation by block
Key Columns:
- block_timestamp
- block_id
- validator_address: Validator voting
- slot_height: Slot being voted on
- confirmed_blocks: Number of blocks confirmed
- total_blocks: Total blocks in voting window
- vote_success: Boolean indicating successful vote submission


Table: solana.gov.fact_vote_accounts
Purpose: Records vote accounts
Key Columns:
- block_timestamp
- block_id
- vote_pubkey: Validator's vote account address
- node_pubkey: Validator's identity pubkey
- authorized_withdrawer: Account authorized to withdraw rewards
- authorized_voter: Account authorized to submit votes
- commission: Current commission percentage
- root_slot: Latest root slot
- last_vote: Slot of last vote
- votes: Array of recent vote history
- epoch_credits: Array of epoch credit history


Table: solana.gov.dim_epoch
Purpose: Records epochs
Key Columns:
- epoch_id
- start_block
- end_block
- start_timestamp
- end_timestamp
- validator_count


Table: solana.gov.fact_stake_accounts
Purpose: Records stake accounts
Key Columns:
- block_timestamp
- block_id
- stake_account: Account holding staked SOL
- staker: Account authorized to stake
- withdrawer: Account authorized to withdraw
- validator: Validator being staked
- stake_amount: Amount of SOL staked in lamports
- activated_epoch: Epoch when stake activated
- deactivating_epoch: Epoch when stake began deactivating


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
