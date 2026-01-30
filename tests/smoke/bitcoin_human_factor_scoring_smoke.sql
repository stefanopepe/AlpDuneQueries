-- ============================================================
-- Smoke Test: Bitcoin Human Factor Scoring
-- Description: Quick validation query to test the human factor
--              scoring logic on a small sample of recent transactions.
--              Scores transactions on likelihood of human origin.
-- Usage: Copy/paste to Dune and run. Should complete in <30 seconds.
-- Note: BDD (Bitcoin Days Destroyed) calculation removed due to
--       schema limitations (spent_output_index not available).
-- ============================================================

WITH
-- Get cutoff block height (last 100 blocks)
block_cutoff AS (
    SELECT MAX(height) - 100 AS min_height FROM bitcoin.blocks
),

-- Raw inputs for block range (non-coinbase only)
raw_inputs AS (
    SELECT
        i.tx_id,
        i.value AS input_value_sats
    FROM bitcoin.inputs i
    CROSS JOIN block_cutoff bc
    WHERE i.block_height >= bc.min_height
      AND i.is_coinbase = FALSE
),

-- Raw outputs for block range
raw_outputs AS (
    SELECT
        o.tx_id,
        o.value AS output_value_sats
    FROM bitcoin.outputs o
    CROSS JOIN block_cutoff bc
    WHERE o.block_height >= bc.min_height
),

-- Aggregate input features per transaction
tx_input_stats AS (
    SELECT
        tx_id,
        COUNT(*) AS input_count,
        SUM(input_value_sats) AS total_input_sats
    FROM raw_inputs
    GROUP BY tx_id
),

-- Aggregate output features per transaction
tx_output_stats AS (
    SELECT
        tx_id,
        COUNT(*) AS output_count,
        SUM(output_value_sats) AS total_output_sats,
        -- Dust detection: outputs < 546 satoshis
        SUM(CASE WHEN output_value_sats < 546 THEN 1 ELSE 0 END) AS dust_count,
        -- Round value detection: divisible by 0.001 BTC (100,000 sats)
        SUM(CASE WHEN output_value_sats % 100000 = 0 AND output_value_sats > 0 THEN 1 ELSE 0 END) AS round_value_count
    FROM raw_outputs
    GROUP BY tx_id
),

-- Combine all transaction features
tx_combined AS (
    SELECT
        i.tx_id,
        i.input_count,
        COALESCE(o.output_count, 0) AS output_count,
        i.total_input_sats,
        COALESCE(o.dust_count, 0) AS dust_count,
        COALESCE(o.round_value_count, 0) AS round_value_count,
        -- Derived boolean features
        i.input_count > 50 AS is_high_fan_in,
        COALESCE(o.output_count, 0) > 50 AS is_high_fan_out,
        COALESCE(o.dust_count, 0) > 0 AS has_dust,
        COALESCE(o.round_value_count, 0) > 0 AS has_round_values,
        -- Simple structure: 1-in-1-out or 1-in-2-out
        (i.input_count = 1 AND COALESCE(o.output_count, 0) IN (1, 2)) AS is_simple_structure
    FROM tx_input_stats i
    LEFT JOIN tx_output_stats o ON i.tx_id = o.tx_id
),

-- Calculate human factor score per transaction
-- Note: BDD-based indicators removed (spent_output_index not available)
tx_scored AS (
    SELECT
        tx_id,
        input_count,
        output_count,
        total_input_sats,
        is_high_fan_in,
        is_high_fan_out,
        has_dust,
        has_round_values,
        is_simple_structure,
        -- Calculate raw score (without BDD indicators)
        50  -- BASE_SCORE
        + CASE WHEN is_high_fan_in THEN -15 ELSE 0 END
        + CASE WHEN is_high_fan_out THEN -15 ELSE 0 END
        + CASE WHEN has_round_values THEN -5 ELSE 0 END
        + CASE WHEN has_dust THEN -10 ELSE 0 END
        + CASE WHEN is_simple_structure THEN 10 ELSE 0 END
        + CASE WHEN NOT has_round_values THEN 5 ELSE 0 END
        AS raw_score
    FROM tx_combined
),

-- Clamp scores and assign to bands
tx_with_bands AS (
    SELECT
        tx_id,
        input_count,
        output_count,
        total_input_sats / 1e8 AS btc_volume,
        GREATEST(0, LEAST(100, raw_score)) AS human_factor_score,
        CASE
            WHEN GREATEST(0, LEAST(100, raw_score)) < 10 THEN '0-10'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 20 THEN '10-20'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 30 THEN '20-30'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 40 THEN '30-40'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 50 THEN '40-50'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 60 THEN '50-60'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 70 THEN '60-70'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 80 THEN '70-80'
            WHEN GREATEST(0, LEAST(100, raw_score)) < 90 THEN '80-90'
            ELSE '90-100'
        END AS score_band,
        CASE
            WHEN GREATEST(0, LEAST(100, raw_score)) < 10 THEN 1
            WHEN GREATEST(0, LEAST(100, raw_score)) < 20 THEN 2
            WHEN GREATEST(0, LEAST(100, raw_score)) < 30 THEN 3
            WHEN GREATEST(0, LEAST(100, raw_score)) < 40 THEN 4
            WHEN GREATEST(0, LEAST(100, raw_score)) < 50 THEN 5
            WHEN GREATEST(0, LEAST(100, raw_score)) < 60 THEN 6
            WHEN GREATEST(0, LEAST(100, raw_score)) < 70 THEN 7
            WHEN GREATEST(0, LEAST(100, raw_score)) < 80 THEN 8
            WHEN GREATEST(0, LEAST(100, raw_score)) < 90 THEN 9
            ELSE 10
        END AS score_band_order
    FROM tx_scored
)

-- Final aggregation by score band (no day grouping for smoke test)
SELECT
    score_band,
    score_band_order,
    COUNT(*) AS tx_count,
    SUM(btc_volume) AS btc_volume,
    AVG(human_factor_score) AS avg_score,
    MIN(human_factor_score) AS min_score,
    MAX(human_factor_score) AS max_score,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM tx_with_bands
GROUP BY score_band, score_band_order
ORDER BY score_band_order;
