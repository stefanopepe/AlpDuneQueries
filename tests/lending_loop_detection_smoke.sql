-- ============================================================
-- Smoke Test: Lending Loop Detection
-- Description: Validates that multi-hop loops can be detected
--              by looking for entities with borrow+supply on
--              different protocols in the same transaction.
--              Uses stablecoin events from the last 30 days.
-- Author: stefanopepe
-- Created: 2026-02-11
-- ============================================================

WITH stablecoins AS (
    SELECT address FROM (
        VALUES
            (0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),  -- USDC
            (0xdac17f958d2ee523a2206206994597c13d831ec7),  -- USDT
            (0x6b175474e89094c44da98b954eedeac495271d0f),  -- DAI
            (0x853d955acef822db058eb8505911ed77f175b99e)   -- FRAX
    ) AS t(address)
),

-- Entities active on multiple protocols (stablecoin events only)
multi_protocol_entities AS (
    SELECT entity_address, COUNT(DISTINCT protocol) AS protocol_count
    FROM (
        -- Aave V3
        SELECT COALESCE(onBehalfOf, "user") AS entity_address, 'aave_v3' AS protocol
        FROM aave_v3_ethereum.pool_evt_borrow
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND reserve IN (SELECT address FROM stablecoins)
        UNION ALL
        SELECT COALESCE(onBehalfOf, "user"), 'aave_v3'
        FROM aave_v3_ethereum.pool_evt_supply
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND reserve IN (SELECT address FROM stablecoins)
        -- Compound V2
        UNION ALL
        SELECT borrower, 'compound_v2'
        FROM compound_ethereum.cerc20delegator_evt_borrow
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND contract_address IN (
              0x39aa39c021dfbae8fac545936693ac917d5e7563,
              0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
              0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
          )
        UNION ALL
        SELECT minter, 'compound_v2'
        FROM compound_ethereum.cerc20delegator_evt_mint
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND contract_address IN (
              0x39aa39c021dfbae8fac545936693ac917d5e7563,
              0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9,
              0x5d3a536e4d6dbd6114cc1ead35777bab948e3643
          )
        -- Morpho
        UNION ALL
        SELECT _borrower, 'morpho_aave_v2'
        FROM morpho_aave_v2_ethereum.morpho_evt_borrowed
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND _poolToken IN (
              0xbcca60bb61934080951369a648fb03df4f96263c,
              0x3ed3b47dd13ec9a98b44e6204a523e766b225811,
              0x028171bca77440897b824ca71d1c56cac55b68a3,
              0xd4937682df3c8aef4fe912a96a74121c0829e664
          )
        UNION ALL
        SELECT COALESCE(_onBehalf, _from), 'morpho_aave_v2'
        FROM morpho_aave_v2_ethereum.morpho_evt_supplied
        WHERE evt_block_time >= CURRENT_DATE - INTERVAL '30' DAY
          AND _poolToken IN (
              0xbcca60bb61934080951369a648fb03df4f96263c,
              0x3ed3b47dd13ec9a98b44e6204a523e766b225811,
              0x028171bca77440897b824ca71d1c56cac55b68a3,
              0xd4937682df3c8aef4fe912a96a74121c0829e664
          )
    )
    GROUP BY entity_address
    HAVING COUNT(DISTINCT protocol) >= 2
)

-- Results: entities active on 2+ protocols (potential loopers)
SELECT
    'multi_protocol_entity_count' AS test_name,
    protocol_count,
    COUNT(*) AS entity_count
FROM multi_protocol_entities
GROUP BY protocol_count
ORDER BY protocol_count
