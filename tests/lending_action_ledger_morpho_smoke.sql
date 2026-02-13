-- ============================================================
-- Smoke Test: Lending Action Ledger - Morpho Aave V2
-- Description: Validates the Morpho base query on recent data
--              Tests all event types and computed fields
-- Author: stefanopepe
-- Created: 2026-02-05
-- ============================================================

-- Test on last 7 days of data (fast execution)
WITH
recent_data AS (
    SELECT
        evt_block_time AS block_time,
        CAST(date_trunc('day', evt_block_time) AS DATE) AS block_date,
        evt_block_number AS block_number,
        evt_tx_hash AS tx_hash,
        evt_index,
        'morpho_aave_v2' AS protocol,
        'supply' AS action_type,
        _from AS user_address,
        _onBehalf AS on_behalf_of,
        _poolToken AS pool_token,
        _amount AS amount_raw
    FROM morpho_aave_v2_ethereum.morpho_evt_supplied
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY

    UNION ALL

    SELECT
        evt_block_time AS block_time,
        CAST(date_trunc('day', evt_block_time) AS DATE) AS block_date,
        evt_block_number AS block_number,
        evt_tx_hash AS tx_hash,
        evt_index,
        'morpho_aave_v2' AS protocol,
        'borrow' AS action_type,
        _borrower AS user_address,
        _borrower AS on_behalf_of,
        _poolToken AS pool_token,
        _amount AS amount_raw
    FROM morpho_aave_v2_ethereum.morpho_evt_borrowed
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY

    UNION ALL

    SELECT
        evt_block_time AS block_time,
        CAST(date_trunc('day', evt_block_time) AS DATE) AS block_date,
        evt_block_number AS block_number,
        evt_tx_hash AS tx_hash,
        evt_index,
        'morpho_aave_v2' AS protocol,
        'repay' AS action_type,
        _repayer AS user_address,
        _onBehalf AS on_behalf_of,
        _poolToken AS pool_token,
        _amount AS amount_raw
    FROM morpho_aave_v2_ethereum.morpho_evt_repaid
    WHERE evt_block_time >= CURRENT_DATE - INTERVAL '7' DAY
)

-- Validation: Count events by type
SELECT
    'event_type_distribution' AS test_name,
    action_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_address) AS unique_users,
    COUNT(DISTINCT pool_token) AS unique_assets
FROM recent_data
GROUP BY action_type
ORDER BY event_count DESC
