-- Gate: schema contract
WITH sample AS (
    SELECT
        borrow_id,
        chain,
        protocol,
        borrow_time_utc,
        event_time_utc,
        day_utc,
        intent_bucket,
        intent_reason,
        signal_loop_1h,
        signal_loop_24h,
        signal_defi_7d,
        signal_bridge_7d,
        signal_offramp_7d,
        query_version,
        as_of_utc,
        parameter_hash
    FROM query_<BORROW_INTENT_CLASSIFICATION_INDEPENDENT_ID>
    LIMIT 10
)
SELECT
    'schema_contract' AS gate_name,
    CASE WHEN count(*) >= 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'Required columns are selectable from classification output.' AS detail,
    cast(count(*) AS varchar) AS observed_value,
    '>=0 rows' AS threshold_value
FROM sample;
