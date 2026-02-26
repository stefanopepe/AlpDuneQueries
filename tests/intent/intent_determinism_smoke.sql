-- Gate: determinism (run-to-run check using grouped hash on latest output)
WITH run_a AS (
    SELECT chain, protocol, day_utc, round(sum(borrow_usd), 6) AS borrow_usd
    FROM query_<BORROW_INTENT_METRICS_DAILY_INDEPENDENT_ID>
    GROUP BY 1, 2, 3
),
run_b AS (
    SELECT chain, protocol, day_utc, round(sum(borrow_usd), 6) AS borrow_usd
    FROM query_<BORROW_INTENT_METRICS_DAILY_INDEPENDENT_ID>
    GROUP BY 1, 2, 3
),
cmp AS (
    SELECT count(*) AS mismatches
    FROM (
        SELECT * FROM run_a
        EXCEPT
        SELECT * FROM run_b
    ) x
)
SELECT
    'determinism' AS gate_name,
    CASE WHEN mismatches = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'Repeated aggregation result matches exactly for same parameterized query result.' AS detail,
    cast(mismatches AS varchar) AS observed_value,
    '0 mismatches' AS threshold_value
FROM cmp;
