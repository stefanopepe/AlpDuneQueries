-- ============================================================
-- Smoke Test: Lending Action Ledger - Unified Multi-Protocol
-- Description: Validates the unified base query combining
--              Morpho, Aave V3, and Compound V2 stablecoin events.
--              Tests amount extraction, stablecoin filtering, and
--              liquidation event inclusion.
-- Author: stefanopepe
-- Created: 2026-02-05
-- Updated: 2026-02-11
-- ============================================================

-- Stablecoin addresses for filtering
WITH stablecoins AS (
    SELECT address, symbol
    FROM (
        VALUES
            (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, 'USDC'),
            (0xdac17f958d2ee523a2206206994597c13d831ec7, 'USDT'),
            (0x6b175474e89094c44da98b954eedeac495271d0f, 'DAI'),
            (0x853d955acef822db058eb8505911ed77f175b99e, 'FRAX')
    ) AS t(address, symbol)
),

-- ============================================================
-- TEST 1: Aave V3 stablecoin supply events have amounts
-- ============================================================
aave_v3_sample AS (
    SELECT
        s.evt_block_time,
        s.reserve,
        s.amount AS amount_raw,
        t.decimals,
        CAST(s.amount AS DOUBLE) / POWER(10, t.decimals) AS amount,
        p.price,
        CAST(s.amount AS DOUBLE) / POWER(10, t.decimals) * p.price AS amount_usd
    FROM aave_v3_ethereum.pool_evt_supply s
    JOIN stablecoins sc ON sc.address = s.reserve
    LEFT JOIN tokens.erc20 t
        ON t.contract_address = s.reserve AND t.blockchain = 'ethereum'
    LEFT JOIN prices.usd p
        ON p.contract_address = s.reserve
        AND p.blockchain = 'ethereum'
        AND p.minute = date_trunc('minute', s.evt_block_time)
    WHERE s.evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
    LIMIT 5
),

-- ============================================================
-- TEST 2: Compound V2 cToken → underlying mapping works
-- ============================================================
compound_mapping_test AS (
    SELECT
        m.contract_address AS ctoken_address,
        CASE
            WHEN m.contract_address = 0x39aa39c021dfbae8fac545936693ac917d5e7563 THEN 'cUSDC'
            WHEN m.contract_address = 0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9 THEN 'cUSDT'
            WHEN m.contract_address = 0x5d3a536e4d6dbd6114cc1ead35777bab948e3643 THEN 'cDAI'
        END AS ctoken_name,
        m.mintAmount AS amount_raw,
        COUNT(*) AS event_count
    FROM compound_ethereum.cerc20delegator_evt_mint m
    WHERE m.evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND m.contract_address IN (
          0x39aa39c021dfbae8fac545936693ac917d5e7563,
          0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
          0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
      )
    GROUP BY m.contract_address, m.mintAmount
    ORDER BY event_count DESC
    LIMIT 5
),

-- ============================================================
-- TEST 3: Stablecoin event counts by protocol and action type
-- ============================================================
protocol_action_counts AS (
    -- Aave V3
    SELECT 'aave_v3' AS protocol, 'supply' AS action_type, COUNT(*) AS cnt
    FROM aave_v3_ethereum.pool_evt_supply
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    SELECT 'aave_v3', 'borrow', COUNT(*)
    FROM aave_v3_ethereum.pool_evt_borrow
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    SELECT 'aave_v3', 'repay', COUNT(*)
    FROM aave_v3_ethereum.pool_evt_repay
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    SELECT 'aave_v3', 'withdraw', COUNT(*)
    FROM aave_v3_ethereum.pool_evt_withdraw
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND reserve IN (SELECT address FROM stablecoins)
    UNION ALL
    SELECT 'aave_v3', 'liquidation', COUNT(*)
    FROM aave_v3_ethereum.pool_evt_liquidationcall
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND debtAsset IN (SELECT address FROM stablecoins)
    -- Compound V2 (stablecoin cTokens only)
    UNION ALL
    SELECT 'compound_v2', 'supply', COUNT(*)
    FROM compound_ethereum.cerc20delegator_evt_mint
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND contract_address IN (
          0x39aa39c021dfbae8fac545936693ac917d5e7563,
          0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
          0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
      )
    UNION ALL
    SELECT 'compound_v2', 'borrow', COUNT(*)
    FROM compound_ethereum.cerc20delegator_evt_borrow
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND contract_address IN (
          0x39aa39c021dfbae8fac545936693ac917d5e7563,
          0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
          0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
      )
    -- Morpho (stablecoin aTokens only)
    UNION ALL
    SELECT 'morpho_aave_v2', 'supply', COUNT(*)
    FROM morpho_aave_v2_ethereum.morpho_evt_supplied
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND _poolToken IN (
          0xbcca60bb61934080951369a648fb03df4f96263c,
          0x3ed3b47dd13ec9a98b44e6204a523e766b225811,
          0x028171bca77440897b824ca71d1c56cac55b68a3,
          0xd4937682df3c8aef4fe912a96a74121c0829e664
      )
    UNION ALL
    SELECT 'morpho_aave_v2', 'borrow', COUNT(*)
    FROM morpho_aave_v2_ethereum.morpho_evt_borrowed
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
      AND _poolToken IN (
          0xbcca60bb61934080951369a648fb03df4f96263c,
          0x3ed3b47dd13ec9a98b44e6204a523e766b225811,
          0x028171bca77440897b824ca71d1c56cac55b68a3,
          0xd4937682df3c8aef4fe912a96a74121c0829e664
      )
)

-- ============================================================
-- FINAL OUTPUT: Combined test results
-- ============================================================
SELECT
    'protocol_action_distribution' AS test_name,
    protocol,
    action_type,
    cnt AS event_count
FROM protocol_action_counts
WHERE cnt > 0
ORDER BY protocol, action_type
