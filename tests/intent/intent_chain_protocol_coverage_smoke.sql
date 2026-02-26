-- Gate: chain/protocol coverage parity
WITH c AS (
    SELECT chain, protocol
    FROM query_<BORROW_INTENT_COVERAGE_SANITY_INDEPENDENT_ID>
),
expected AS (
    SELECT * FROM (
        VALUES
            ('ethereum', 'morpho_blue'),
            ('ethereum', 'aave_v3'),
            ('base', 'morpho_blue'),
            ('base', 'aave_v3')
    ) AS t(chain, protocol)
),
missing AS (
    SELECT count(*) AS missing_rows
    FROM expected e
    LEFT JOIN c
      ON c.chain = e.chain
     AND c.protocol = e.protocol
    WHERE c.chain IS NULL
)
SELECT
    'chain_protocol_coverage' AS gate_name,
    CASE WHEN missing_rows = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    'Every requested chain+protocol pair must be present, including explicit zero rows.' AS detail,
    cast(missing_rows AS varchar) AS observed_value,
    '0 missing rows' AS threshold_value
FROM missing;
