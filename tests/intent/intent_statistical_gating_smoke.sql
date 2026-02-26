-- Gate: statistical adequacy
WITH s AS (
    SELECT
        chain,
        protocol,
        gate_min_sample_pass,
        gate_ci_width_pass
    FROM query_<BORROW_INTENT_STATISTICAL_SIGNIFICANCE_INDEPENDENT_ID>
),
viol AS (
    SELECT count(*) AS failed_rows
    FROM s
    WHERE gate_min_sample_pass = FALSE
       OR gate_ci_width_pass = FALSE
)
SELECT
    'statistical_gating' AS gate_name,
    CASE WHEN failed_rows = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'Sample size and CI-width gates must pass.' AS detail,
    cast(failed_rows AS varchar) AS observed_value,
    '0 failed rows' AS threshold_value
FROM viol;
