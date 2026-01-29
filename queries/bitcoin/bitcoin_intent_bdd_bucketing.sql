-- ============================================================
-- Query: Bitcoin Transaction Intent by Coin Age Bucket (BDD)
-- Description: Extends bitcoin_intent_heuristics by bucketing inputs
--              based on coin age (days since UTXO creation).
--              Groups transactions by intent AND age bucket.
-- Author: stefanopepe
-- Created: 2026-01-29
-- Updated: 2026-01-29
-- Dependencies: bitcoin_intent_heuristics.sql (conceptually)
-- ============================================================
-- IMPORTANT TERMINOLOGY NOTE:
--   This query uses "coin age" (days since UTXO creation), NOT true
--   Bitcoin Days Destroyed (BDD = coin_age_days × BTC_value).
--   The bucket ranges (0-7 days, 7-30 days, etc.) are age-based.
-- ============================================================
-- RISKS AND PITFALLS:
--   1. SCHEMA ASSUMPTION: Uses spent_tx_id and spent_output_index
--      columns to join inputs with their spent outputs. Verify these
--      column names exist in Dune's bitcoin.inputs table.
--   2. PERFORMANCE: Joining all inputs with outputs is expensive.
--      Consider limiting date ranges for initial testing.
--   3. NULL HANDLING: Some inputs may not match outputs (data gaps,
--      very old UTXOs). These are bucketed as 'unknown'.
--   4. CARDINALITY: 8 age buckets × 7 intent types = 56 combinations
--      per day. Output volume increases significantly.
--   5. INCREMENTAL PROCESSING: This query does NOT use incremental
--      processing. For production, adapt the pattern from
--      bitcoin_intent_heuristics.sql.
-- ============================================================
-- Output Columns:
--   day              - Date of transactions
--   intent           - Classified transaction intent
--   age_bucket       - Coin age bucket (e.g., '0-7 days', '1-2 years')
--   tx_count         - Number of transactions
--   input_count      - Total number of inputs
--   sats_in          - Total input value (satoshis)
--   avg_coin_age     - Average coin age in days
--   median_coin_age  - Median coin age in days
-- ============================================================
-- Parameters:
--   {{start_date}} - Analysis start date (default: 7 days ago)
--   {{end_date}}   - Analysis end date (default: today)
-- ============================================================

WITH
-- 1) Get all inputs with their spent output details for the date range
inputs_with_spent_outputs AS (
    SELECT
        CAST(date_trunc('day', i.block_time) AS DATE) AS day,
        i.tx_id,
        i.block_time AS input_time,
        i.value AS input_value_sats,
        i.spent_tx_id,
        i.spent_output_index,
        o.block_time AS output_time,
        -- Calculate coin age in days
        date_diff('day', o.block_time, i.block_time) AS coin_age_days
    FROM bitcoin.inputs i
    LEFT JOIN bitcoin.outputs o
        ON i.spent_tx_id = o.tx_id
        AND i.spent_output_index = o.index
    WHERE i.block_time >= CAST('{{start_date}}' AS TIMESTAMP)
      AND i.block_time < CAST('{{end_date}}' AS TIMESTAMP)
      AND i.is_coinbase = FALSE
),

-- 2) Bucket each input by coin age
inputs_bucketed AS (
    SELECT
        day,
        tx_id,
        input_value_sats,
        coin_age_days,
        CASE
            WHEN coin_age_days IS NULL THEN 'unknown'
            WHEN coin_age_days < 0 THEN 'invalid_negative'
            WHEN coin_age_days < 7 THEN '0-7 days'
            WHEN coin_age_days >= 7 AND coin_age_days < 30 THEN '7-30 days'
            WHEN coin_age_days >= 30 AND coin_age_days < 90 THEN '30-90 days'
            WHEN coin_age_days >= 90 AND coin_age_days < 180 THEN '90-180 days'
            WHEN coin_age_days >= 180 AND coin_age_days < 365 THEN '180-365 days'
            WHEN coin_age_days >= 365 AND coin_age_days < 730 THEN '1-2 years'
            WHEN coin_age_days >= 730 AND coin_age_days < 1825 THEN '2-5 years'
            WHEN coin_age_days >= 1825 THEN '5+ years'
            ELSE 'unknown'
        END AS age_bucket
    FROM inputs_with_spent_outputs
),

-- 3) Aggregate inputs per tx and bucket
inputs_by_tx_bucket AS (
    SELECT
        day,
        tx_id,
        age_bucket,
        COUNT(*) AS input_count,
        SUM(input_value_sats) AS input_value_sats,
        AVG(coin_age_days) AS avg_coin_age,
        -- Store for later median calculation
        ARRAY_AGG(coin_age_days) AS coin_ages
    FROM inputs_bucketed
    GROUP BY day, tx_id, age_bucket
),

-- 4) Get output counts per tx for intent classification
outputs_by_tx AS (
    SELECT
        CAST(date_trunc('day', o.block_time) AS DATE) AS day,
        o.tx_id,
        COUNT(*) AS output_count,
        SUM(o.value) AS output_value_sats
    FROM bitcoin.outputs o
    WHERE o.block_time >= CAST('{{start_date}}' AS TIMESTAMP)
      AND o.block_time < CAST('{{end_date}}' AS TIMESTAMP)
    GROUP BY 1, 2
),

-- 5) Get total inputs per tx for intent classification
total_inputs_by_tx AS (
    SELECT
        day,
        tx_id,
        SUM(input_count) AS total_input_count
    FROM inputs_by_tx_bucket
    GROUP BY day, tx_id
),

-- 6) Classify each tx by intent (same logic as bitcoin_intent_heuristics.sql)
tx_classified AS (
    SELECT
        i.day,
        i.tx_id,
        i.age_bucket,
        i.input_count,
        i.input_value_sats,
        i.avg_coin_age,
        i.coin_ages,
        COALESCE(o.output_count, 0) AS output_count,
        t.total_input_count,
        CASE
            WHEN COALESCE(o.output_count, 0) = 0
                THEN 'malformed_no_outputs'
            WHEN t.total_input_count >= 10 AND COALESCE(o.output_count, 0) <= 2
                THEN 'consolidation'
            WHEN t.total_input_count <= 2 AND COALESCE(o.output_count, 0) >= 10
                THEN 'fan_out_batch'
            WHEN t.total_input_count >= 5 AND COALESCE(o.output_count, 0) >= 5
                 AND ABS(t.total_input_count - COALESCE(o.output_count, 0)) <= 1
                THEN 'coinjoin_like'
            WHEN t.total_input_count = 1 AND COALESCE(o.output_count, 0) = 1
                THEN 'self_transfer'
            WHEN COALESCE(o.output_count, 0) = 2 AND t.total_input_count >= 2
                THEN 'change_like_2_outputs'
            ELSE 'other'
        END AS intent
    FROM inputs_by_tx_bucket i
    LEFT JOIN outputs_by_tx o
        ON i.day = o.day AND i.tx_id = o.tx_id
    LEFT JOIN total_inputs_by_tx t
        ON i.day = t.day AND i.tx_id = t.tx_id
),

-- 7) Final aggregation by day, intent, and age bucket
final_agg AS (
    SELECT
        day,
        intent,
        age_bucket,
        COUNT(DISTINCT tx_id) AS tx_count,
        SUM(input_count) AS input_count,
        SUM(input_value_sats) AS sats_in,
        AVG(avg_coin_age) AS avg_coin_age,
        APPROX_PERCENTILE(avg_coin_age, 0.5) AS median_coin_age
    FROM tx_classified
    GROUP BY day, intent, age_bucket
)

SELECT
    day,
    intent,
    age_bucket,
    tx_count,
    input_count,
    sats_in,
    ROUND(avg_coin_age, 2) AS avg_coin_age_days,
    ROUND(median_coin_age, 2) AS median_coin_age_days
FROM final_agg
ORDER BY day DESC, intent,
    CASE age_bucket
        WHEN '0-7 days' THEN 1
        WHEN '7-30 days' THEN 2
        WHEN '30-90 days' THEN 3
        WHEN '90-180 days' THEN 4
        WHEN '180-365 days' THEN 5
        WHEN '1-2 years' THEN 6
        WHEN '2-5 years' THEN 7
        WHEN '5+ years' THEN 8
        WHEN 'unknown' THEN 9
        WHEN 'invalid_negative' THEN 10
        ELSE 11
    END;
