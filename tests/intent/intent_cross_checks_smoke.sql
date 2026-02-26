-- Gate: cross-checks (cohort <= baseline)
WITH c AS (
    SELECT
        chain,
        protocol,
        all_borrow_events,
        cohort_borrow_events,
        all_borrowers,
        cohort_borrowers,
        all_borrow_usd,
        cohort_borrow_usd
    FROM query_<BORROW_INTENT_COVERAGE_SANITY_INDEPENDENT_ID>
),
viol AS (
    SELECT count(*) AS violations
    FROM c
    WHERE cohort_borrow_events > all_borrow_events
       OR cohort_borrowers > all_borrowers
       OR cohort_borrow_usd > all_borrow_usd
)
SELECT
    'cross_checks' AS gate_name,
    CASE WHEN violations = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'Cohort universe must not exceed baseline universe.' AS detail,
    cast(violations AS varchar) AS observed_value,
    '0 violations' AS threshold_value
FROM viol;
