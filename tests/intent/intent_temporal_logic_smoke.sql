-- Gate: temporal logic (borrow-time collateral semantics)
WITH c AS (
    SELECT backing_source
    FROM query_<BTC_BACKED_BORROW_COHORT_INDEPENDENT_ID>
),
viol AS (
    SELECT count(*) AS violations
    FROM c
    WHERE backing_source = 'not_btc_backed'
)
SELECT
    'temporal_logic' AS gate_name,
    CASE WHEN violations = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'Cohort rows must be explicitly BTC-backed at borrow time.' AS detail,
    cast(violations AS varchar) AS observed_value,
    '0 violations' AS threshold_value
FROM viol;
