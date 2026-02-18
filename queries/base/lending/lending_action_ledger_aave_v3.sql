-- ============================================================
-- Query: Lending Action Ledger - Aave V3 (Base Query)
-- Description: Unified action ledger for all Aave V3 lending events.
--              Fetches Supply, Borrow, Repay, Withdraw, Liquidation events
--              and normalizes them into a single schema for downstream analysis.
--              Uses incremental processing with 1-day lookback.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Updated: 2026-02-05
-- Architecture: V2 Base Query - computes ALL actions once
-- Dependencies: None (base query)
-- ============================================================
-- Output Columns:
--   block_time           - Event timestamp
--   block_date           - Event date (for aggregation)
--   block_number         - Block number
--   tx_hash              - Transaction hash
--   evt_index            - Event log index
--   protocol             - Protocol identifier ('aave_v3')
--   action_type          - Action: supply/borrow/repay/withdraw/liquidation
--   user_address         - Entity performing action
--   on_behalf_of         - Beneficiary address (for entity resolution)
--   asset_address        - Underlying asset (reserve) contract
--   amount_raw           - Raw amount in asset decimals
--   amount               - Decimal-adjusted amount
--   amount_usd           - USD value at event time
--   interest_rate_mode   - 1=stable, 2=variable (borrow only)
--   collateral_asset     - Collateral seized (liquidation only)
--   debt_asset           - Debt repaid (liquidation only)
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
            asset_address VARBINARY,
            amount_raw VARCHAR,
            amount DOUBLE,
            amount_usd DOUBLE,
            interest_rate_mode BIGINT,
            collateral_asset VARBINARY,
            debt_asset VARBINARY
        )
    ))
),

-- 2) Checkpoint: recompute from 1-day lookback
checkpoint AS (
    SELECT
        COALESCE(MAX(block_date), DATE '2024-01-01') - INTERVAL '1' DAY AS cutoff_date
    FROM prev
),

-- 3) Supply events
supply_events AS (
    SELECT
        s.evt_block_time AS block_time,
        CAST(date_trunc('day', s.evt_block_time) AS DATE) AS block_date,
        s.evt_block_number AS block_number,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'aave_v3' AS protocol,
        'supply' AS action_type,
        s."user" AS user_address,
        s.onBehalfOf AS on_behalf_of,
        s.reserve AS asset_address,
        s.amount AS amount_raw,
        CAST(NULL AS BIGINT) AS interest_rate_mode,
        CAST(NULL AS VARBINARY) AS collateral_asset,
        CAST(NULL AS VARBINARY) AS debt_asset
    FROM aave_v3_base.pool_evt_supply s
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', s.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', s.evt_block_time) AS DATE) < CURRENT_DATE
),

-- 4) Borrow events
borrow_events AS (
    SELECT
        b.evt_block_time AS block_time,
        CAST(date_trunc('day', b.evt_block_time) AS DATE) AS block_date,
        b.evt_block_number AS block_number,
        b.evt_tx_hash AS tx_hash,
        b.evt_index,
        'aave_v3' AS protocol,
        'borrow' AS action_type,
        b."user" AS user_address,
        b.onBehalfOf AS on_behalf_of,
        b.reserve AS asset_address,
        b.amount AS amount_raw,
        CAST(b.interestRateMode AS BIGINT) AS interest_rate_mode,
        CAST(NULL AS VARBINARY) AS collateral_asset,
        CAST(NULL AS VARBINARY) AS debt_asset
    FROM aave_v3_base.pool_evt_borrow b
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', b.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', b.evt_block_time) AS DATE) < CURRENT_DATE
),

-- 5) Repay events
repay_events AS (
    SELECT
        r.evt_block_time AS block_time,
        CAST(date_trunc('day', r.evt_block_time) AS DATE) AS block_date,
        r.evt_block_number AS block_number,
        r.evt_tx_hash AS tx_hash,
        r.evt_index,
        'aave_v3' AS protocol,
        'repay' AS action_type,
        r.repayer AS user_address,
        r."user" AS on_behalf_of,
        r.reserve AS asset_address,
        r.amount AS amount_raw,
        CAST(NULL AS BIGINT) AS interest_rate_mode,
        CAST(NULL AS VARBINARY) AS collateral_asset,
        CAST(NULL AS VARBINARY) AS debt_asset
    FROM aave_v3_base.pool_evt_repay r
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', r.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', r.evt_block_time) AS DATE) < CURRENT_DATE
),

-- 6) Withdraw events
withdraw_events AS (
    SELECT
        w.evt_block_time AS block_time,
        CAST(date_trunc('day', w.evt_block_time) AS DATE) AS block_date,
        w.evt_block_number AS block_number,
        w.evt_tx_hash AS tx_hash,
        w.evt_index,
        'aave_v3' AS protocol,
        'withdraw' AS action_type,
        w."user" AS user_address,
        w."to" AS on_behalf_of,
        w.reserve AS asset_address,
        w.amount AS amount_raw,
        CAST(NULL AS BIGINT) AS interest_rate_mode,
        CAST(NULL AS VARBINARY) AS collateral_asset,
        CAST(NULL AS VARBINARY) AS debt_asset
    FROM aave_v3_base.pool_evt_withdraw w
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', w.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', w.evt_block_time) AS DATE) < CURRENT_DATE
),

-- 7) Liquidation events
liquidation_events AS (
    SELECT
        l.evt_block_time AS block_time,
        CAST(date_trunc('day', l.evt_block_time) AS DATE) AS block_date,
        l.evt_block_number AS block_number,
        l.evt_tx_hash AS tx_hash,
        l.evt_index,
        'aave_v3' AS protocol,
        'liquidation' AS action_type,
        l."user" AS user_address,  -- User being liquidated
        l.liquidator AS on_behalf_of,  -- Liquidator
        l.debtAsset AS asset_address,  -- Primary asset is debt being repaid
        l.debtToCover AS amount_raw,
        CAST(NULL AS BIGINT) AS interest_rate_mode,
        l.collateralAsset AS collateral_asset,
        l.debtAsset AS debt_asset
    FROM aave_v3_base.pool_evt_liquidationcall l
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', l.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', l.evt_block_time) AS DATE) < CURRENT_DATE
),

-- 8) Union all events
all_events AS (
    SELECT * FROM supply_events
    UNION ALL
    SELECT * FROM borrow_events
    UNION ALL
    SELECT * FROM repay_events
    UNION ALL
    SELECT * FROM withdraw_events
    UNION ALL
    SELECT * FROM liquidation_events
),

-- 9) Enrich with token metadata and prices
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
        e.asset_address,
        CAST(e.amount_raw AS VARCHAR) AS amount_raw,
        -- Decimal adjustment using token metadata
        CASE
            WHEN t.decimals IS NOT NULL THEN
                CAST(e.amount_raw AS DOUBLE) / POWER(10, t.decimals)
            ELSE
                CAST(e.amount_raw AS DOUBLE) / 1e18  -- Default to 18 decimals
        END AS amount,
        -- USD value from prices table
        CASE
            WHEN t.decimals IS NOT NULL AND p.price IS NOT NULL THEN
                (CAST(e.amount_raw AS DOUBLE) / POWER(10, t.decimals)) * p.price
            ELSE
                NULL
        END AS amount_usd,
        e.interest_rate_mode,
        e.collateral_asset,
        e.debt_asset
    FROM all_events e
    LEFT JOIN tokens.erc20 t
        ON t.contract_address = e.asset_address
        AND t.blockchain = 'base'
    LEFT JOIN prices.usd p
        ON p.contract_address = e.asset_address
        AND p.blockchain = 'base'
        AND p.minute = date_trunc('minute', e.block_time)
),

-- 10) Incremental merge: keep old data before cutoff, add new data
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
ORDER BY block_date, block_time, tx_hash, evt_index
