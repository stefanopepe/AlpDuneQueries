-- ============================================================
-- Query: Lending Collateral Ledger (Base Query)
-- Description: Tracks collateral deposits and withdrawals across
--              Aave V3, Morpho Blue, Compound V3, and Compound V2.
--              Captures the non-stablecoin side of lending positions
--              (WBTC, WETH, wstETH, weETH, rETH, cbETH) to answer:
--              "What backs the stablecoin borrows?"
--              Uses incremental processing with 1-day lookback.
-- Author: stefanopepe
-- Created: 2026-02-12
-- Updated: 2026-02-12
-- Architecture: V2 Base Query - parallel to lending_action_ledger_unified
-- Dependencies: None (base query)
-- ============================================================
-- Protocols Included:
--   - aave_v3: Supply/Withdraw events where reserve is NOT a stablecoin
--   - morpho_blue: SupplyCollateral/WithdrawCollateral events
--   - compound_v3: SupplyCollateral/WithdrawCollateral events
--   - compound_v2: Mint/Redeem events for non-stablecoin cTokens
-- ============================================================
-- Collateral Assets Tracked:
--   BTC:     WBTC   0x2260fac5e5542a773aa44fbcfedf7c193bc2c599 (8 decimals)
--   ETH:     WETH   0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 (18 decimals)
--   ETH LST: wstETH 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0 (18 decimals)
--   ETH LST: weETH  0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee (18 decimals)
--   ETH LST: rETH   0xae78736cd615f374d3085123a210448e74fc6393 (18 decimals)
--   ETH LST: cbETH  0xbe9895146f7af43049ca1c1ae358b0541ea49704 (18 decimals)
-- ============================================================
-- Output Columns:
--   block_time           - Event timestamp
--   block_date           - Event date (for aggregation)
--   block_number         - Block number
--   tx_hash              - Transaction hash
--   evt_index            - Event log index
--   protocol             - Protocol identifier
--   action_type          - supply_collateral / withdraw_collateral
--   user_address         - Entity performing action
--   on_behalf_of         - Beneficiary address
--   entity_address       - Canonical entity (COALESCE of on_behalf_of, user)
--   collateral_address   - Underlying collateral asset contract
--   collateral_symbol    - Token symbol (WBTC, WETH, wstETH, etc.)
--   collateral_category  - btc / eth / eth_lst
--   amount_raw           - Raw amount in asset decimals
--   amount               - Decimal-adjusted amount
--   amount_usd           - USD value at event time
-- ============================================================

WITH
-- 1) Previous results (empty on first run)
prev AS (
    SELECT *
    FROM TABLE(previous.query.result(
        schema => DESCRIPTOR(
            block_time TIMESTAMP,
            block_date DATE,
            block_number BIGINT,
            tx_hash VARBINARY,
            evt_index BIGINT,
            protocol VARCHAR,
            action_type VARCHAR,
            user_address VARBINARY,
            on_behalf_of VARBINARY,
            entity_address VARBINARY,
            collateral_address VARBINARY,
            collateral_symbol VARCHAR,
            collateral_category VARCHAR,
            amount_raw VARCHAR,
            amount DOUBLE,
            amount_usd DOUBLE
        )
    ))
),

-- 2) Checkpoint: recompute from 1-day lookback
checkpoint AS (
    SELECT
        COALESCE(MAX(block_date), DATE '2024-01-01') - INTERVAL '1' DAY AS cutoff_date
    FROM prev
),

-- ============================================================
-- COLLATERAL ASSET METADATA (hardcoded — no tokens.erc20 JOIN)
-- ============================================================

collateral_metadata AS (
    SELECT address, symbol, decimals, category
    FROM (
        VALUES
            (0x2260fac5e5542a773aa44fbcfedf7c193bc2c599, 'WBTC',    8, 'btc'),
            (0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 'WETH',   18, 'eth'),
            (0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0, 'wstETH', 18, 'eth_lst'),
            (0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee, 'weETH',  18, 'eth_lst'),
            (0xae78736cd615f374d3085123a210448e74fc6393, 'rETH',   18, 'eth_lst'),
            (0xbe9895146f7af43049ca1c1ae358b0541ea49704, 'cbETH',  18, 'eth_lst')
    ) AS t(address, symbol, decimals, category)
),

collateral_addresses AS (
    SELECT address FROM collateral_metadata
),

-- Stablecoin addresses (to EXCLUDE from Aave V3 supply/withdraw)
stablecoin_addresses AS (
    SELECT address
    FROM (
        VALUES
            (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),  -- USDC
            (0xdac17f958d2ee523a2206206994597c13d831ec7),  -- USDT
            (0x6b175474e89094c44da98b954eedeac495271d0f),  -- DAI
            (0x853d955acef822db058eb8505911ed77f175b99e)   -- FRAX
    ) AS t(address)
),

-- Compound V2 cToken → underlying wrapper mapping (non-stablecoins)
compound_v2_collateral_wrappers AS (
    SELECT wrapper, underlying
    FROM (
        VALUES
            (0xccf4429db6322d5c611ee964527d42e5d685dd6a, 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599)   -- cWBTC2 -> WBTC
    ) AS t(wrapper, underlying)
),

-- ============================================================
-- MORPHO BLUE COLLATERAL EVENTS
-- Morpho Blue has explicit SupplyCollateral/WithdrawCollateral events.
-- We resolve collateral token from createmarket's marketParams.
-- ============================================================

-- Resolve Morpho Blue market IDs to collateral tokens
morpho_blue_collateral_markets AS (
    SELECT
        id AS market_id,
        CAST(json_extract_scalar(marketParams, '$.collateralToken') AS VARBINARY) AS collateral_token
    FROM morpho_blue_ethereum.morphoblue_evt_createmarket
    WHERE CAST(json_extract_scalar(marketParams, '$.collateralToken') AS VARBINARY) IN (
        SELECT address FROM collateral_addresses
    )
),

morpho_blue_supply_collateral AS (
    SELECT
        s.evt_block_time AS block_time,
        s.evt_block_date AS block_date,
        s.evt_block_number AS block_number,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'morpho_blue' AS protocol,
        'supply_collateral' AS action_type,
        s.caller AS user_address,
        s.onBehalf AS on_behalf_of,
        m.collateral_token AS raw_asset_address,
        CAST(s.assets AS UINT256) AS amount_raw
    FROM morpho_blue_ethereum.morphoblue_evt_supplycollateral s
    INNER JOIN morpho_blue_collateral_markets m ON m.market_id = s.id
    CROSS JOIN checkpoint c
    WHERE s.evt_block_date >= c.cutoff_date
      AND s.evt_block_date < CURRENT_DATE
),

morpho_blue_withdraw_collateral AS (
    SELECT
        w.evt_block_time AS block_time,
        w.evt_block_date AS block_date,
        w.evt_block_number AS block_number,
        w.evt_tx_hash AS tx_hash,
        w.evt_index,
        'morpho_blue' AS protocol,
        'withdraw_collateral' AS action_type,
        w.caller AS user_address,
        w.onBehalf AS on_behalf_of,
        m.collateral_token AS raw_asset_address,
        CAST(w.assets AS UINT256) AS amount_raw
    FROM morpho_blue_ethereum.morphoblue_evt_withdrawcollateral w
    INNER JOIN morpho_blue_collateral_markets m ON m.market_id = w.id
    CROSS JOIN checkpoint c
    WHERE w.evt_block_date >= c.cutoff_date
      AND w.evt_block_date < CURRENT_DATE
),

-- ============================================================
-- AAVE V3 COLLATERAL EVENTS
-- Aave V3 uses the same Supply/Withdraw events for all assets.
-- We capture supply/withdraw where reserve is a tracked collateral
-- asset (NOT a stablecoin).
-- ============================================================

aave_v3_supply_collateral AS (
    SELECT
        s.evt_block_time AS block_time,
        s.evt_block_date AS block_date,
        s.evt_block_number AS block_number,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'aave_v3' AS protocol,
        'supply_collateral' AS action_type,
        s."user" AS user_address,
        s.onBehalfOf AS on_behalf_of,
        s.reserve AS raw_asset_address,
        s.amount AS amount_raw
    FROM aave_v3_ethereum.pool_evt_supply s
    CROSS JOIN checkpoint c
    WHERE s.evt_block_date >= c.cutoff_date
      AND s.evt_block_date < CURRENT_DATE
      AND s.reserve IN (SELECT address FROM collateral_addresses)
),

aave_v3_withdraw_collateral AS (
    SELECT
        w.evt_block_time AS block_time,
        w.evt_block_date AS block_date,
        w.evt_block_number AS block_number,
        w.evt_tx_hash AS tx_hash,
        w.evt_index,
        'aave_v3' AS protocol,
        'withdraw_collateral' AS action_type,
        w."user" AS user_address,
        w."to" AS on_behalf_of,
        w.reserve AS raw_asset_address,
        w.amount AS amount_raw
    FROM aave_v3_ethereum.pool_evt_withdraw w
    CROSS JOIN checkpoint c
    WHERE w.evt_block_date >= c.cutoff_date
      AND w.evt_block_date < CURRENT_DATE
      AND w.reserve IN (SELECT address FROM collateral_addresses)
),

-- ============================================================
-- COMPOUND V3 (COMET) COLLATERAL EVENTS
-- Compound V3 has dedicated SupplyCollateral/WithdrawCollateral events
-- with an explicit `asset` field naming the collateral token.
-- ============================================================

compound_v3_supply_collateral AS (
    SELECT
        s.evt_block_time AS block_time,
        s.evt_block_date AS block_date,
        s.evt_block_number AS block_number,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'compound_v3' AS protocol,
        'supply_collateral' AS action_type,
        s."from" AS user_address,
        s.dst AS on_behalf_of,
        s.asset AS raw_asset_address,
        s.amount AS amount_raw
    FROM compound_v3_ethereum.comet_evt_supplycollateral s
    CROSS JOIN checkpoint c
    WHERE s.evt_block_date >= c.cutoff_date
      AND s.evt_block_date < CURRENT_DATE
      AND s.asset IN (SELECT address FROM collateral_addresses)
      -- USDC Comet only (main market)
      AND s.contract_address = 0xc3d688b66703497daa19211eedff47f25384cdc3
),

compound_v3_withdraw_collateral AS (
    SELECT
        w.evt_block_time AS block_time,
        w.evt_block_date AS block_date,
        w.evt_block_number AS block_number,
        w.evt_tx_hash AS tx_hash,
        w.evt_index,
        'compound_v3' AS protocol,
        'withdraw_collateral' AS action_type,
        w.src AS user_address,
        w."to" AS on_behalf_of,
        w.asset AS raw_asset_address,
        w.amount AS amount_raw
    FROM compound_v3_ethereum.comet_evt_withdrawcollateral w
    CROSS JOIN checkpoint c
    WHERE w.evt_block_date >= c.cutoff_date
      AND w.evt_block_date < CURRENT_DATE
      AND w.asset IN (SELECT address FROM collateral_addresses)
      AND w.contract_address = 0xc3d688b66703497daa19211eedff47f25384cdc3
),

-- ============================================================
-- COMPOUND V2 COLLATERAL EVENTS (legacy)
-- Uses Mint/Redeem events for non-stablecoin cTokens (cWBTC)
-- ============================================================

compound_v2_supply_collateral AS (
    SELECT
        m.evt_block_time AS block_time,
        m.evt_block_date AS block_date,
        m.evt_block_number AS block_number,
        m.evt_tx_hash AS tx_hash,
        m.evt_index,
        'compound_v2' AS protocol,
        'supply_collateral' AS action_type,
        m.minter AS user_address,
        m.minter AS on_behalf_of,
        m.contract_address AS raw_asset_address,
        m.mintAmount AS amount_raw
    FROM compound_ethereum.cerc20delegator_evt_mint m
    CROSS JOIN checkpoint c
    WHERE m.evt_block_date >= c.cutoff_date
      AND m.evt_block_date < CURRENT_DATE
      AND m.contract_address IN (SELECT wrapper FROM compound_v2_collateral_wrappers)
),

compound_v2_withdraw_collateral AS (
    SELECT
        r.evt_block_time AS block_time,
        r.evt_block_date AS block_date,
        r.evt_block_number AS block_number,
        r.evt_tx_hash AS tx_hash,
        r.evt_index,
        'compound_v2' AS protocol,
        'withdraw_collateral' AS action_type,
        r.redeemer AS user_address,
        r.redeemer AS on_behalf_of,
        r.contract_address AS raw_asset_address,
        r.redeemAmount AS amount_raw
    FROM compound_ethereum.cerc20delegator_evt_redeem r
    CROSS JOIN checkpoint c
    WHERE r.evt_block_date >= c.cutoff_date
      AND r.evt_block_date < CURRENT_DATE
      AND r.contract_address IN (SELECT wrapper FROM compound_v2_collateral_wrappers)
),

-- ============================================================
-- UNION ALL PROTOCOLS
-- ============================================================

all_events AS (
    -- Morpho Blue
    SELECT * FROM morpho_blue_supply_collateral
    UNION ALL SELECT * FROM morpho_blue_withdraw_collateral
    -- Aave V3
    UNION ALL SELECT * FROM aave_v3_supply_collateral
    UNION ALL SELECT * FROM aave_v3_withdraw_collateral
    -- Compound V3
    UNION ALL SELECT * FROM compound_v3_supply_collateral
    UNION ALL SELECT * FROM compound_v3_withdraw_collateral
    -- Compound V2
    UNION ALL SELECT * FROM compound_v2_supply_collateral
    UNION ALL SELECT * FROM compound_v2_withdraw_collateral
),

-- ============================================================
-- RESOLVE WRAPPER TOKENS AND ENRICH WITH METADATA + PRICES
-- ============================================================

enriched AS (
    SELECT
        e.block_time,
        e.block_date,
        e.block_number,
        e.tx_hash,
        e.evt_index,
        e.protocol,
        e.action_type,
        e.user_address,
        e.on_behalf_of,
        COALESCE(e.on_behalf_of, e.user_address) AS entity_address,
        -- Resolve Compound V2 cTokens to underlying
        COALESCE(cw.underlying, e.raw_asset_address) AS collateral_address,
        cm.symbol AS collateral_symbol,
        cm.category AS collateral_category,
        CAST(e.amount_raw AS VARCHAR) AS amount_raw,
        CAST(e.amount_raw AS DOUBLE) / POWER(10, cm.decimals) AS amount,
        CAST(e.amount_raw AS DOUBLE) / POWER(10, cm.decimals) * p.price AS amount_usd
    FROM all_events e
    LEFT JOIN compound_v2_collateral_wrappers cw
        ON cw.wrapper = e.raw_asset_address
    -- Hardcoded metadata: eliminates tokens.erc20 JOIN
    LEFT JOIN collateral_metadata cm
        ON cm.address = COALESCE(cw.underlying, e.raw_asset_address)
    -- Time-bounded price JOIN: partition pruning on prices.usd
    LEFT JOIN prices.usd p
        ON p.contract_address = COALESCE(cw.underlying, e.raw_asset_address)
        AND p.blockchain = 'ethereum'
        AND p.minute = date_trunc('minute', e.block_time)
        AND p.minute >= (SELECT cutoff_date FROM checkpoint)
        AND p.minute < CURRENT_DATE
),

-- ============================================================
-- INCREMENTAL MERGE
-- ============================================================

new_data AS (
    SELECT * FROM enriched
),

kept_old AS (
    SELECT p.*
    FROM prev p
    CROSS JOIN checkpoint c
    WHERE p.block_date < c.cutoff_date
)

SELECT * FROM kept_old
UNION ALL
SELECT * FROM new_data
