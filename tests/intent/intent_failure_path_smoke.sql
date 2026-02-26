-- Gate: failure-path behavior (provenance + parseability)
WITH m AS (
    SELECT
        query_version,
        as_of_utc,
        parameter_hash
    FROM query_<BORROW_INTENT_SIGNALS_INDEPENDENT_ID>
    LIMIT 100
),
viol AS (
    SELECT count(*) AS violations
    FROM m
    WHERE query_version IS NULL
       OR as_of_utc IS NULL
       OR parameter_hash IS NULL
)
SELECT
    'failure_path' AS gate_name,
    CASE WHEN violations = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'Provenance fields required for failure diagnosis must be populated.' AS detail,
    cast(violations AS varchar) AS observed_value,
    '0 violations' AS threshold_value
FROM viol;
