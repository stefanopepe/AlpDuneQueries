-- ============================================================
-- Query: Bitcoin Human Factor Scoring
-- Description: Scores Bitcoin transactions on likelihood of
--              originating from human-controlled wallets vs
--              automated systems (exchanges, bots, mining pools).
--              Uses behavioral heuristics based on tx structure.
--              Score range: 0 (automated) to 100 (human).
--              Uses incremental processing with 1-day lookback.
-- Author: stefanopepe
-- Created: 2026-01-30
-- Updated: 2026-01-30
-- Reference: Meiklejohn et al. (2013), Ermilov et al. (2017),
--            Zhang et al. (2020), Schnoering et al. (2024)
-- Note: On first run, only processes data from fallback date onwards.
--       Adjust DATE '2026-01-01' in checkpoint CTE for historical analysis.
--       BDD calculation removed due to schema limitations
--       (spent_output_index column not available in bitcoin.inputs).
-- ============================================================
-- Scoring Model:
--   BASE_SCORE = 50 (neutral)
--   Negative (automated signals):
--     high_fan_in (>50 inputs): -15
--     high_fan_out (>50 outputs): -15
--     round_values (divisible by 0.001 BTC): -5
--     dust_output (<546 sats): -10
--   Positive (human signals):
--     simple_structure (1-in-1-out or 1-in-2-out): +10
--     non_round_value: +5
--   Final score clamped to [0, 100]
-- ============================================================
-- Score Band Interpretation:
--   0-30:  Likely automated (exchange, pool, bot)
--   30-50: Probably automated
--   50-60: Ambiguous / uncertain
--   60-80: Likely human-controlled
--   80-100: Strong human indicators
-- ============================================================
-- Output Columns:
--   day             - Date of transactions
--   score_band      - Score range (e.g., '50-60')
--   score_band_order- Numeric ordering (1-10)
--   tx_count        - Number of transactions
--   btc_volume      - Total BTC moved (input value)
--   avg_score       - Average exact score in band
-- ============================================================

WITH
-- 1) Previous results (empty on first ever run)
prev AS (
    SELECT *
    FROM TABLE(previous.query.result(
        schema => DESCRIPTOR(
            day DATE,
            score_band VARCHAR,
            score_band_order BIGINT,
            tx_count BIGINT,
            btc_volume DOUBLE,
            avg_score DOUBLE
        )
    ))
),

-- 2) Checkpoint: recompute from 1-day lookback
checkpoint AS (
    SELECT
        COALESCE(MAX(day), DATE '2026-01-01') - INTERVAL '1' DAY AS cutoff_day
    FROM prev
),

-- 3) Raw inputs for date range (non-coinbase only)
raw_inputs AS (
    SELECT
        CAST(date_trunc('day', i.block_time) AS DATE) AS day,
        i.tx_id,
        i.value AS input_value_sats
    FROM bitcoin.inputs i
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', i.block_time) AS DATE) >= c.cutoff_day
      AND CAST(date_trunc('day', i.block_time) AS DATE) < CURRENT_DATE
      AND i.is_coinbase = FALSE
),

-- 4) Raw outputs for date range
raw_outputs AS (
    SELECT
        CAST(date_trunc('day', o.block_time) AS DATE) AS day,
        o.tx_id,
        o.value AS output_value_sats
    FROM bitcoin.outputs o
    CROSS JOIN checkpoint c
    WHERE CAST(date_trunc('day', o.block_time) AS DATE) >= c.cutoff_day
      AND CAST(date_trunc('day', o.block_time) AS DATE) < CURRENT_DATE
),

-- 5) Aggregate input features per transaction
tx_input_stats AS (
    SELECT
        day,
        tx_id,
        COUNT(*) AS input_count,
        SUM(input_value_sats) AS total_input_sats
    FROM raw_inputs
    GROUP BY day, tx_id
),

-- 6) Aggregate output features per transaction
tx_output_stats AS (
    SELECT
        day,
        tx_id,
        COUNT(*) AS output_count,
        SUM(output_value_sats) AS total_output_sats,
        -- Dust detection: outputs < 546 satoshis
        SUM(CASE WHEN output_value_sats < 546 THEN 1 ELSE 0 END) AS dust_count,
        -- Round value detection: divisible by 0.001 BTC (100,000 sats)
        SUM(CASE WHEN output_value_sats % 100000 = 0 AND output_value_sats > 0 THEN 1 ELSE 0 END) AS round_value_count
    FROM raw_outputs
    GROUP BY day, tx_id
),

-- 7) Combine all transaction features
tx_combined AS (
    SELECT
        i.day,
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
    LEFT JOIN tx_output_stats o
        ON i.day = o.day
        AND i.tx_id = o.tx_id
),

-- 8) Calculate human factor score per transaction
tx_scored AS (
    SELECT
        day,
        tx_id,
        input_count,
        output_count,
        total_input_sats,
        -- Calculate raw score
        50  -- BASE_SCORE
        -- NEGATIVE INDICATORS (reduce score = more automated)
        + CASE WHEN is_high_fan_in THEN -15 ELSE 0 END
        + CASE WHEN is_high_fan_out THEN -15 ELSE 0 END
        + CASE WHEN has_round_values THEN -5 ELSE 0 END
        + CASE WHEN has_dust THEN -10 ELSE 0 END
        -- POSITIVE INDICATORS (increase score = more human)
        + CASE WHEN is_simple_structure THEN 10 ELSE 0 END
        + CASE WHEN NOT has_round_values THEN 5 ELSE 0 END  -- non-round value bonus
        AS raw_score
    FROM tx_combined
),

-- 9) Clamp scores and assign to bands
tx_with_bands AS (
    SELECT
        day,
        tx_id,
        total_input_sats,
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
),

-- 10) Aggregate by day and score band
new_data AS (
    SELECT
        day,
        score_band,
        score_band_order,
        COUNT(*) AS tx_count,
        SUM(total_input_sats) / 1e8 AS btc_volume,  -- Convert satoshis to BTC
        AVG(human_factor_score) AS avg_score
    FROM tx_with_bands
    GROUP BY day, score_band, score_band_order
),

-- 11) Keep historical data before cutoff
kept_old AS (
    SELECT p.*
    FROM prev p
    CROSS JOIN checkpoint c
    WHERE p.day < c.cutoff_day
)

-- 12) Final combined result
SELECT * FROM kept_old
UNION ALL
SELECT * FROM new_data
ORDER BY day, score_band_order;
