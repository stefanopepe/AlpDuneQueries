-- ============================================================
-- Query: Lending Action Ledger - Unified Multi-Protocol (Base Query)
-- Description: Unified action ledger combining Morpho, Aave V3, and Compound V2
--              lending events into a single normalized schema.
--              Scoped to stablecoins (USDC, USDT, DAI, FRAX).
--              This is the primary base query for cross-protocol loop analysis.
--              Uses incremental processing with 1-day lookback.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Updated: 2026-02-11
-- Architecture: V2 Base Query - computes ALL actions once across protocols
-- Dependencies: None (base query)
-- ============================================================
-- Protocols Included:
--   - morpho_aave_v2: Morpho optimizer on Aave V2
--   - aave_v3: Aave V3 direct lending
--   - compound_v2: Compound V2 direct lending
-- ============================================================
-- Asset Scope: Stablecoins only
--   - USDC  0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
--   - USDT  0xdac17f958d2ee523a2206206994597c13d831ec7
--   - DAI   0x6b175474e89094c44da98b954eedeac495271d0f
--   - FRAX  0x853d955acef822db058eb8505911ed77f175b99e
-- ============================================================
-- Output Columns:
--   block_time           - Event timestamp
--   block_date           - Event date (for aggregation)
--   block_number         - Block number
--   tx_hash              - Transaction hash
--   evt_index            - Event log index
--   protocol             - Protocol identifier
--   action_type          - Action: supply/borrow/repay/withdraw/liquidation
--   user_address         - Entity performing action
--   on_behalf_of         - Beneficiary address (for entity resolution)
--   entity_address       - Canonical entity (COALESCE of on_behalf_of, user)
--   asset_address        - Underlying asset contract (resolved from wrapper)
--   asset_symbol         - Token symbol
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
            asset_address VARBINARY,
            asset_symbol VARCHAR,
            amount_raw UINT256,
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
-- STABLECOIN AND WRAPPER TOKEN MAPPINGS
-- ============================================================

-- Stablecoin underlying addresses
stablecoins AS (
    SELECT address
    FROM (
        VALUES
            (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),  -- USDC
            (0xdac17f958d2ee523a2206206994597c13d831ec7),  -- USDT
            (0x6b175474e89094c44da98b954eedeac495271d0f),  -- DAI
            (0x853d955acef822db058eb8505911ed77f175b99e)   -- FRAX
    ) AS t(address)
),

-- Map cTokens (Compound V2) and aTokens (Morpho/Aave V2) to underlying
wrapper_to_underlying AS (
    SELECT wrapper, underlying
    FROM (
        VALUES
            -- Compound V2 cTokens
            (0x39aa39c021dfbae8fac545936693ac917d5e7563, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),  -- cUSDC  -> USDC
            (0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9, 0xdac17f958d2ee523a2206206994597c13d831ec7),  -- cUSDT  -> USDT
            (0x5d3a536e4d6dbd6114cc1ead35777bab948e3643, 0x6b175474e89094c44da98b954eedeac495271d0f),  -- cDAI   -> DAI
            -- Aave V2 aTokens (used by Morpho)
            (0xbcca60bb61934080951369a648fb03df4f96263c, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),  -- aUSDC  -> USDC
            (0x3ed3b47dd13ec9a98b44e6204a523e766b225811, 0xdac17f958d2ee523a2206206994597c13d831ec7),  -- aUSDT  -> USDT
            (0x028171bca77440897b824ca71d1c56cac55b68a3, 0x6b175474e89094c44da98b954eedeac495271d0f),  -- aDAI   -> DAI
            (0xd4937682df3c8aef4fe912a96a74121c0829e664, 0x853d955acef822db058eb8505911ed77f175b99e)   -- aFRAX  -> FRAX
    ) AS t(wrapper, underlying)
),

-- ============================================================
-- MORPHO AAVE V2 EVENTS
-- Morpho uses aToken addresses (_poolToken) and _amount column
-- ============================================================

morpho_supply AS (
    SELECT
        s.evt_block_time AS block_time,
        CAST(date_trunc('day', s.evt_block_time) AS DATE) AS block_date,
        s.evt_block_number AS block_number,
        s.evt_tx_hash AS tx_hash,
        s.evt_index,
        'morpho_aave_v2' AS protocol,
        'supply' AS action_type,
        s._from AS user_address,
        s._onBehalf AS on_behalf_of,
        s._poolToken AS raw_asset_address,
        s._amount AS amount_raw
    FROM morpho_aave_v2_ethereum.morpho_evt_supplied s
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', s.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', s.evt_block_time) AS DATE) < CURRENT_DATE
      -- Filter to stablecoin aTokens only
      AND s._poolToken IN (
          0xbcca60bb61934080951369a648fb03df4f96263c,  -- aUSDC
          0x3ed3b47dd13ec9a98b44e6204a523e766b225811,  -- aUSDT
          0x028171bca77440897b824ca71d1c56cac55b68a3,  -- aDAI
          0xd4937682df3c8aef4fe912a96a74121c0829e664   -- aFRAX
      )
),

morpho_borrow AS (
    SELECT
        b.evt_block_time AS block_time,
        CAST(date_trunc('day', b.evt_block_time) AS DATE) AS block_date,
        b.evt_block_number AS block_number,
        b.evt_tx_hash AS tx_hash,
        b.evt_index,
        'morpho_aave_v2' AS protocol,
        'borrow' AS action_type,
        b._borrower AS user_address,
        b._borrower AS on_behalf_of,
        b._poolToken AS raw_asset_address,
        b._amount AS amount_raw
    FROM morpho_aave_v2_ethereum.morpho_evt_borrowed b
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', b.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', b.evt_block_time) AS DATE) < CURRENT_DATE
      AND b._poolToken IN (
          0xbcca60bb61934080951369a648fb03df4f96263c,
          0x3ed3b47dd13ec9a98b44e6204a523e766b225811,
          0x028171bca77440897b824ca71d1c56cac55b68a3,
          0xd4937682df3c8aef4fe912a96a74121c0829e664
      )
),

morpho_repay AS (
    SELECT
        r.evt_block_time AS block_time,
        CAST(date_trunc('day', r.evt_block_time) AS DATE) AS block_date,
        r.evt_block_number AS block_number,
        r.evt_tx_hash AS tx_hash,
        r.evt_index,
        'morpho_aave_v2' AS protocol,
        'repay' AS action_type,
        r._repayer AS user_address,
        r._onBehalf AS on_behalf_of,
        r._poolToken AS raw_asset_address,
        r._amount AS amount_raw
    FROM morpho_aave_v2_ethereum.morpho_evt_repaid r
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', r.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', r.evt_block_time) AS DATE) < CURRENT_DATE
      AND r._poolToken IN (
          0xbcca60bb61934080951369a648fb03df4f96263c,
          0x3ed3b47dd13ec9a98b44e6204a523e766b225811,
          0x028171bca77440897b824ca71d1c56cac55b68a3,
          0xd4937682df3c8aef4fe912a96a74121c0829e664
      )
),

morpho_withdraw AS (
    SELECT
        w.evt_block_time AS block_time,
        CAST(date_trunc('day', w.evt_block_time) AS DATE) AS block_date,
        w.evt_block_number AS block_number,
        w.evt_tx_hash AS tx_hash,
        w.evt_index,
        'morpho_aave_v2' AS protocol,
        'withdraw' AS action_type,
        w._supplier AS user_address,
        w._receiver AS on_behalf_of,
        w._poolToken AS raw_asset_address,
        w._amount AS amount_raw
    FROM morpho_aave_v2_ethereum.morpho_evt_withdrawn w
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', w.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', w.evt_block_time) AS DATE) < CURRENT_DATE
      AND w._poolToken IN (
          0xbcca60bb61934080951369a648fb03df4f96263c,
          0x3ed3b47dd13ec9a98b44e6204a523e766b225811,
          0x028171bca77440897b824ca71d1c56cac55b68a3,
          0xd4937682df3c8aef4fe912a96a74121c0829e664
      )
),

-- Morpho liquidations: emit two rows (debt repaid + collateral seized)
-- Only include when the debt asset is a stablecoin
morpho_liquidation AS (
    SELECT
        l.evt_block_time AS block_time,
        CAST(date_trunc('day', l.evt_block_time) AS DATE) AS block_date,
        l.evt_block_number AS block_number,
        l.evt_tx_hash AS tx_hash,
        l.evt_index,
        'morpho_aave_v2' AS protocol,
        'liquidation' AS action_type,
        l._liquidator AS user_address,
        l._liquidated AS on_behalf_of,
        l._poolTokenBorrowed AS raw_asset_address,
        l._amountRepaid AS amount_raw
    FROM morpho_aave_v2_ethereum.morpho_evt_liquidated l
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', l.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', l.evt_block_time) AS DATE) < CURRENT_DATE
      AND l._poolTokenBorrowed IN (
          0xbcca60bb61934080951369a648fb03df4f96263c,
          0x3ed3b47dd13ec9a98b44e6204a523e766b225811,
          0x028171bca77440897b824ca71d1c56cac55b68a3,
          0xd4937682df3c8aef4fe912a96a74121c0829e664
      )
),

-- ============================================================
-- AAVE V3 EVENTS
-- Aave V3 uses `reserve` (underlying) and `amount` directly
-- ============================================================

aave_v3_supply AS (
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
        s.reserve AS raw_asset_address,
        s.amount AS amount_raw
    FROM aave_v3_ethereum.pool_evt_supply s
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', s.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', s.evt_block_time) AS DATE) < CURRENT_DATE
      AND s.reserve IN (SELECT address FROM stablecoins)
),

aave_v3_borrow AS (
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
        b.reserve AS raw_asset_address,
        b.amount AS amount_raw
    FROM aave_v3_ethereum.pool_evt_borrow b
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', b.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', b.evt_block_time) AS DATE) < CURRENT_DATE
      AND b.reserve IN (SELECT address FROM stablecoins)
),

aave_v3_repay AS (
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
        r.reserve AS raw_asset_address,
        r.amount AS amount_raw
    FROM aave_v3_ethereum.pool_evt_repay r
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', r.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', r.evt_block_time) AS DATE) < CURRENT_DATE
      AND r.reserve IN (SELECT address FROM stablecoins)
),

aave_v3_withdraw AS (
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
        w.reserve AS raw_asset_address,
        w.amount AS amount_raw
    FROM aave_v3_ethereum.pool_evt_withdraw w
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', w.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', w.evt_block_time) AS DATE) < CURRENT_DATE
      AND w.reserve IN (SELECT address FROM stablecoins)
),

-- Aave V3 liquidations: track debt repaid (stablecoin side)
aave_v3_liquidation AS (
    SELECT
        l.evt_block_time AS block_time,
        CAST(date_trunc('day', l.evt_block_time) AS DATE) AS block_date,
        l.evt_block_number AS block_number,
        l.evt_tx_hash AS tx_hash,
        l.evt_index,
        'aave_v3' AS protocol,
        'liquidation' AS action_type,
        l.liquidator AS user_address,
        l."user" AS on_behalf_of,
        l.debtAsset AS raw_asset_address,
        l.debtToCover AS amount_raw
    FROM aave_v3_ethereum.pool_evt_liquidationcall l
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', l.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', l.evt_block_time) AS DATE) < CURRENT_DATE
      AND l.debtAsset IN (SELECT address FROM stablecoins)
),

-- ============================================================
-- COMPOUND V2 EVENTS
-- Compound uses contract_address (cToken) and per-event amount columns
-- ============================================================

compound_supply AS (
    SELECT
        m.evt_block_time AS block_time,
        CAST(date_trunc('day', m.evt_block_time) AS DATE) AS block_date,
        m.evt_block_number AS block_number,
        m.evt_tx_hash AS tx_hash,
        m.evt_index,
        'compound_v2' AS protocol,
        'supply' AS action_type,
        m.minter AS user_address,
        m.minter AS on_behalf_of,
        m.contract_address AS raw_asset_address,
        m.mintAmount AS amount_raw
    FROM compound_ethereum.cerc20delegator_evt_mint m
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', m.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', m.evt_block_time) AS DATE) < CURRENT_DATE
      AND m.contract_address IN (
          0x39aa39c021dfbae8fac545936693ac917d5e7563,  -- cUSDC
          0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,  -- cUSDT
          0x5d3a536e4d6dbd6114cc1ead35777bab948e3643   -- cDAI
      )
),

compound_borrow AS (
    SELECT
        b.evt_block_time AS block_time,
        CAST(date_trunc('day', b.evt_block_time) AS DATE) AS block_date,
        b.evt_block_number AS block_number,
        b.evt_tx_hash AS tx_hash,
        b.evt_index,
        'compound_v2' AS protocol,
        'borrow' AS action_type,
        b.borrower AS user_address,
        b.borrower AS on_behalf_of,
        b.contract_address AS raw_asset_address,
        b.borrowAmount AS amount_raw
    FROM compound_ethereum.cerc20delegator_evt_borrow b
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', b.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', b.evt_block_time) AS DATE) < CURRENT_DATE
      AND b.contract_address IN (
          0x39aa39c021dfbae8fac545936693ac917d5e7563,
          0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
          0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
      )
),

compound_repay AS (
    SELECT
        r.evt_block_time AS block_time,
        CAST(date_trunc('day', r.evt_block_time) AS DATE) AS block_date,
        r.evt_block_number AS block_number,
        r.evt_tx_hash AS tx_hash,
        r.evt_index,
        'compound_v2' AS protocol,
        'repay' AS action_type,
        r.payer AS user_address,
        r.borrower AS on_behalf_of,
        r.contract_address AS raw_asset_address,
        r.repayAmount AS amount_raw
    FROM compound_ethereum.cerc20delegator_evt_repayborrow r
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', r.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', r.evt_block_time) AS DATE) < CURRENT_DATE
      AND r.contract_address IN (
          0x39aa39c021dfbae8fac545936693ac917d5e7563,
          0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
          0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
      )
),

compound_withdraw AS (
    SELECT
        r.evt_block_time AS block_time,
        CAST(date_trunc('day', r.evt_block_time) AS DATE) AS block_date,
        r.evt_block_number AS block_number,
        r.evt_tx_hash AS tx_hash,
        r.evt_index,
        'compound_v2' AS protocol,
        'withdraw' AS action_type,
        r.redeemer AS user_address,
        r.redeemer AS on_behalf_of,
        r.contract_address AS raw_asset_address,
        r.redeemAmount AS amount_raw
    FROM compound_ethereum.cerc20delegator_evt_redeem r
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', r.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', r.evt_block_time) AS DATE) < CURRENT_DATE
      AND r.contract_address IN (
          0x39aa39c021dfbae8fac545936693ac917d5e7563,
          0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
          0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
      )
),

-- Compound V2 liquidations: track debt repaid (stablecoin side)
compound_liquidation AS (
    SELECT
        l.evt_block_time AS block_time,
        CAST(date_trunc('day', l.evt_block_time) AS DATE) AS block_date,
        l.evt_block_number AS block_number,
        l.evt_tx_hash AS tx_hash,
        l.evt_index,
        'compound_v2' AS protocol,
        'liquidation' AS action_type,
        l.liquidator AS user_address,
        l.borrower AS on_behalf_of,
        l.contract_address AS raw_asset_address,
        l.repayAmount AS amount_raw
    FROM compound_ethereum.cerc20delegator_evt_liquidateborrow l
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', l.evt_block_time) AS DATE) >= c.cutoff_date
      AND CAST(date_trunc('day', l.evt_block_time) AS DATE) < CURRENT_DATE
      AND l.contract_address IN (
          0x39aa39c021dfbae8fac545936693ac917d5e7563,
          0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
          0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
      )
),

-- ============================================================
-- UNION ALL PROTOCOLS
-- ============================================================

all_events AS (
    -- Morpho
    SELECT * FROM morpho_supply
    UNION ALL SELECT * FROM morpho_borrow
    UNION ALL SELECT * FROM morpho_repay
    UNION ALL SELECT * FROM morpho_withdraw
    UNION ALL SELECT * FROM morpho_liquidation
    -- Aave V3
    UNION ALL SELECT * FROM aave_v3_supply
    UNION ALL SELECT * FROM aave_v3_borrow
    UNION ALL SELECT * FROM aave_v3_repay
    UNION ALL SELECT * FROM aave_v3_withdraw
    UNION ALL SELECT * FROM aave_v3_liquidation
    -- Compound V2
    UNION ALL SELECT * FROM compound_supply
    UNION ALL SELECT * FROM compound_borrow
    UNION ALL SELECT * FROM compound_repay
    UNION ALL SELECT * FROM compound_withdraw
    UNION ALL SELECT * FROM compound_liquidation
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
        -- Resolve wrapper tokens (cToken/aToken) to underlying
        COALESCE(w.underlying, e.raw_asset_address) AS asset_address,
        t.symbol AS asset_symbol,
        e.amount_raw,
        CAST(e.amount_raw AS DOUBLE) / POWER(10, t.decimals) AS amount,
        CAST(e.amount_raw AS DOUBLE) / POWER(10, t.decimals) * p.price AS amount_usd
    FROM all_events e
    -- Map wrapper tokens to underlying
    LEFT JOIN wrapper_to_underlying w
        ON w.wrapper = e.raw_asset_address
    -- Token metadata on the resolved underlying
    LEFT JOIN tokens.erc20 t
        ON t.contract_address = COALESCE(w.underlying, e.raw_asset_address)
        AND t.blockchain = 'ethereum'
    -- USD price on the resolved underlying
    LEFT JOIN prices.usd p
        ON p.contract_address = COALESCE(w.underlying, e.raw_asset_address)
        AND p.blockchain = 'ethereum'
        AND p.minute = date_trunc('minute', e.block_time)
),

-- ============================================================
-- INCREMENTAL MERGE
-- ============================================================

new_data AS (
    SELECT * FROM enriched
),

kept_old AS (
    SELECT *
    FROM prev p
    CROSS JOIN checkpoint c
    WHERE p.block_date < c.cutoff_date
)

SELECT * FROM kept_old
UNION ALL
SELECT * FROM new_data
ORDER BY block_date, block_time, tx_hash, evt_index
