-- ============================================================
-- Smoke Test: Bitcoin Cohort Distribution
-- Description: Quick validation query to test the cohort distribution
--              logic on a small sample of recent transactions.
--              Classifies transactions by input value into holder
--              cohorts (Shrimps through Humpback).
-- Usage: Copy/paste to Dune and run. Should complete in <30 seconds.
-- ============================================================

WITH
-- Get cutoff block height (last 100 blocks)
block_cutoff AS (
    SELECT MAX(height) - 100 AS min_height FROM bitcoin.blocks
),

-- Count spent UTXOs per transaction (non-coinbase inputs only)
spent_utxos_per_tx AS (
    SELECT
        i.tx_id,
        COUNT(*) AS spent_utxo_count
    FROM bitcoin.inputs i
    CROSS JOIN block_cutoff bc
    WHERE i.block_height >= bc.min_height
      AND i.is_coinbase = FALSE
    GROUP BY i.tx_id
),

-- Get transaction-level input totals
tx_input_totals AS (
    SELECT
        t.id AS tx_id,
        t.input_value AS input_value_sats
    FROM bitcoin.transactions t
    CROSS JOIN block_cutoff bc
    WHERE t.block_height >= bc.min_height
      AND t.input_count > 0
),

-- Classify transactions by cohort based on input value (in BTC)
tx_cohorts AS (
    SELECT
        t.tx_id,
        t.input_value_sats / 1e8 AS input_value_btc,
        CASE
            WHEN t.input_value_sats / 1e8 < 1 THEN 'Shrimps (<1 BTC)'
            WHEN t.input_value_sats / 1e8 < 10 THEN 'Crab (1-10 BTC)'
            WHEN t.input_value_sats / 1e8 < 50 THEN 'Octopus (10-50 BTC)'
            WHEN t.input_value_sats / 1e8 < 100 THEN 'Fish (50-100 BTC)'
            WHEN t.input_value_sats / 1e8 < 500 THEN 'Dolphin (100-500 BTC)'
            WHEN t.input_value_sats / 1e8 < 1000 THEN 'Shark (500-1,000 BTC)'
            WHEN t.input_value_sats / 1e8 < 5000 THEN 'Whale (1,000-5,000 BTC)'
            ELSE 'Humpback (>5,000 BTC)'
        END AS cohort,
        CASE
            WHEN t.input_value_sats / 1e8 < 1 THEN 1
            WHEN t.input_value_sats / 1e8 < 10 THEN 2
            WHEN t.input_value_sats / 1e8 < 50 THEN 3
            WHEN t.input_value_sats / 1e8 < 100 THEN 4
            WHEN t.input_value_sats / 1e8 < 500 THEN 5
            WHEN t.input_value_sats / 1e8 < 1000 THEN 6
            WHEN t.input_value_sats / 1e8 < 5000 THEN 7
            ELSE 8
        END AS cohort_order
    FROM tx_input_totals t
),

-- Join with UTXO counts
tx_with_utxos AS (
    SELECT
        tc.tx_id,
        tc.input_value_btc,
        tc.cohort,
        tc.cohort_order,
        COALESCE(s.spent_utxo_count, 0) AS spent_utxo_count
    FROM tx_cohorts tc
    LEFT JOIN spent_utxos_per_tx s ON s.tx_id = tc.tx_id
)

-- Final aggregation (by cohort only, not by day for smoke test)
SELECT
    cohort,
    cohort_order,
    SUM(input_value_btc) AS btc_moved,
    COUNT(*) AS tx_count,
    SUM(spent_utxo_count) AS spent_utxo_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM tx_with_utxos
GROUP BY cohort, cohort_order
ORDER BY cohort_order;
