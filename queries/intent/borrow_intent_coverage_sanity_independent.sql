-- ============================================================
-- Query: borrow_intent_coverage_sanity_independent
-- Description: Cohort coverage vs baseline with explicit chain/protocol parity rows
-- ============================================================

WITH
params AS (
    SELECT
        lower('{{chains}}') AS chains_csv,
        lower('{{protocols}}') AS protocols_csv,
        lower('{{stable_symbols}}') AS stable_symbols_csv,
        lower('{{btc_symbols}}') AS btc_symbols_csv,
        '{{start_ts}}' AS start_ts_txt,
        '{{end_ts}}' AS end_ts_txt
),
selected_chains AS (
    SELECT trim(x) AS chain FROM params CROSS JOIN UNNEST(split(chains_csv, ',')) AS t(x)
),
selected_protocols AS (
    SELECT trim(x) AS protocol FROM params CROSS JOIN UNNEST(split(protocols_csv, ',')) AS t(x)
),
selected_pairs AS (
    SELECT c.chain, p.protocol
    FROM selected_chains c
    CROSS JOIN selected_protocols p
),
meta AS (
    SELECT
        'intent_v1_phase1' AS query_version,
        current_timestamp AS as_of_utc,
        to_hex(md5(to_utf8(concat(
            (SELECT chains_csv FROM params), '|',
            (SELECT protocols_csv FROM params), '|',
            (SELECT stable_symbols_csv FROM params), '|',
            (SELECT btc_symbols_csv FROM params), '|',
            (SELECT start_ts_txt FROM params), '|',
            (SELECT end_ts_txt FROM params)
        )))) AS parameter_hash
),
stable_borrows AS (
    SELECT
        s.chain,
        s.protocol,
        count(*) AS all_borrow_events,
        count(DISTINCT s.borrower) AS all_borrowers,
        sum(s.borrow_amount_usd) AS all_borrow_usd
    FROM (
        SELECT * FROM (
            SELECT
                concat(chain, ':', protocol, ':', lower(to_hex(borrow_tx_hash)), ':', cast(evt_index AS varchar)) AS borrow_id,
                chain,
                protocol,
                lower(to_hex(borrower_address)) AS borrower,
                (debt_amount_raw / power(10, coalesce(dt.decimals, 18))) * coalesce(p.price, 0) AS borrow_amount_usd
            FROM (
                SELECT
                    'ethereum' AS chain, 'morpho_blue' AS protocol,
                    b.evt_tx_hash AS borrow_tx_hash, b.evt_index,
                    coalesce(b.onBehalf, b.caller) AS borrower_address,
                    from_hex(substr(json_extract_scalar(cm.marketParams, '$.loanToken'), 3)) AS debt_token_address,
                    CAST(b.assets AS double) AS debt_amount_raw,
                    b.evt_block_time AS borrow_time
                FROM morpho_blue_ethereum.morphoblue_evt_borrow b
                JOIN morpho_blue_ethereum.morphoblue_evt_createmarket cm ON cm.id = b.id
                WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
                  AND 'morpho_blue' IN (SELECT protocol FROM selected_protocols)
                  AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
                  AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
                UNION ALL
                SELECT
                    'base', 'morpho_blue',
                    b.evt_tx_hash, b.evt_index,
                    coalesce(b.onBehalf, b.caller),
                    from_hex(substr(json_extract_scalar(cm.marketParams, '$.loanToken'), 3)),
                    CAST(b.assets AS double),
                    b.evt_block_time
                FROM morpho_blue_base.morphoblue_evt_borrow b
                JOIN morpho_blue_base.morphoblue_evt_createmarket cm ON cm.id = b.id
                WHERE 'base' IN (SELECT chain FROM selected_chains)
                  AND 'morpho_blue' IN (SELECT protocol FROM selected_protocols)
                  AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
                  AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
                UNION ALL
                SELECT
                    'ethereum', 'aave_v3',
                    b.evt_tx_hash, b.evt_index,
                    b.onBehalfOf,
                    b.reserve,
                    CAST(b.amount AS double),
                    b.evt_block_time
                FROM aave_v3_ethereum.pool_evt_borrow b
                WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
                  AND 'aave_v3' IN (SELECT protocol FROM selected_protocols)
                  AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
                  AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
                UNION ALL
                SELECT
                    'base', 'aave_v3',
                    b.evt_tx_hash, b.evt_index,
                    b.onBehalfOf,
                    b.reserve,
                    CAST(b.amount AS double),
                    b.evt_block_time
                FROM aave_v3_base.pool_evt_borrow b
                WHERE 'base' IN (SELECT chain FROM selected_chains)
                  AND 'aave_v3' IN (SELECT protocol FROM selected_protocols)
                  AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
                  AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
            ) rb
            LEFT JOIN tokens.erc20 dt
              ON dt.blockchain = rb.chain
             AND dt.contract_address = rb.debt_token_address
            LEFT JOIN prices.usd p
              ON p.blockchain = rb.chain
             AND p.contract_address = rb.debt_token_address
             AND p.minute = date_trunc('minute', rb.borrow_time)
            WHERE lower(coalesce(dt.symbol, '')) IN (
                SELECT trim(x) FROM params CROSS JOIN UNNEST(split(stable_symbols_csv, ',')) AS t(x)
            )
        ) b
    ) s
    GROUP BY 1, 2
),
cohort AS (
    SELECT
        chain,
        protocol,
        count(*) AS cohort_borrow_events,
        count(DISTINCT borrower) AS cohort_borrowers,
        sum(borrow_amount_usd) AS cohort_borrow_usd
    FROM (
        SELECT
            chain,
            protocol,
            borrower,
            borrow_amount_usd
        FROM (
            SELECT
                y.*, 
                CASE
                    WHEN y.protocol = 'morpho_blue' AND y.market_collateral_token IN (
                        SELECT token_address FROM (
                            VALUES
                                (0x2260fac5e5542a773aa44fbcfedf7c193bc2c599),
                                (0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf),
                                (0x18084fba666a33d37592fa2633fd49a74dd93a88),
                                (0x0555e30da8f98308edb960aa94c0db47230d2b9c),
                                (0x236aa50979d5f3de3bd1eeb40e81137f22ab794b)
                        ) AS btc(token_address)
                    ) THEN TRUE
                    ELSE FALSE
                END AS is_btc_backed
            FROM (
                SELECT
                    rb.chain,
                    rb.protocol,
                    lower(to_hex(rb.borrower_address)) AS borrower,
                    (rb.debt_amount_raw / power(10, coalesce(dt.decimals, 18))) * coalesce(pp.price, 0) AS borrow_amount_usd,
                    rb.market_collateral_token
                FROM (
                    SELECT
                        'ethereum' AS chain,
                        'morpho_blue' AS protocol,
                        b.evt_block_time,
                        coalesce(b.onBehalf, b.caller) AS borrower_address,
                        from_hex(substr(json_extract_scalar(cm.marketParams, '$.loanToken'), 3)) AS debt_token,
                        from_hex(substr(json_extract_scalar(cm.marketParams, '$.collateralToken'), 3)) AS market_collateral_token,
                        CAST(b.assets AS double) AS debt_amount_raw
                    FROM morpho_blue_ethereum.morphoblue_evt_borrow b
                    JOIN morpho_blue_ethereum.morphoblue_evt_createmarket cm ON cm.id = b.id
                    WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
                      AND 'morpho_blue' IN (SELECT protocol FROM selected_protocols)
                      AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
                      AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
                    UNION ALL
                    SELECT
                        'base',
                        'morpho_blue',
                        b.evt_block_time,
                        coalesce(b.onBehalf, b.caller),
                        from_hex(substr(json_extract_scalar(cm.marketParams, '$.loanToken'), 3)),
                        from_hex(substr(json_extract_scalar(cm.marketParams, '$.collateralToken'), 3)),
                        CAST(b.assets AS double)
                    FROM morpho_blue_base.morphoblue_evt_borrow b
                    JOIN morpho_blue_base.morphoblue_evt_createmarket cm ON cm.id = b.id
                    WHERE 'base' IN (SELECT chain FROM selected_chains)
                      AND 'morpho_blue' IN (SELECT protocol FROM selected_protocols)
                      AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
                      AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
                ) rb
                LEFT JOIN tokens.erc20 dt
                  ON dt.blockchain = rb.chain
                 AND dt.contract_address = rb.debt_token
                LEFT JOIN prices.usd pp
                  ON pp.blockchain = rb.chain
                 AND pp.contract_address = rb.debt_token
                 AND pp.minute = date_trunc('minute', rb.evt_block_time)
                WHERE lower(coalesce(dt.symbol, '')) IN (
                    SELECT trim(x) FROM params CROSS JOIN UNNEST(split(stable_symbols_csv, ',')) AS t(x)
                )
            ) y
        ) z
        WHERE z.is_btc_backed
    ) c
    GROUP BY 1, 2
)
SELECT
    CAST(NULL AS varchar) AS borrow_id,
    sp.chain,
    sp.protocol,
    CAST(NULL AS timestamp) AS borrow_time_utc,
    current_timestamp AS event_time_utc,
    CAST(current_date AS date) AS day_utc,
    coalesce(sb.all_borrow_events, 0) AS all_borrow_events,
    coalesce(c.cohort_borrow_events, 0) AS cohort_borrow_events,
    coalesce(sb.all_borrowers, 0) AS all_borrowers,
    coalesce(c.cohort_borrowers, 0) AS cohort_borrowers,
    coalesce(sb.all_borrow_usd, 0) AS all_borrow_usd,
    coalesce(c.cohort_borrow_usd, 0) AS cohort_borrow_usd,
    CASE WHEN coalesce(sb.all_borrow_events, 0) = 0 THEN 0 ELSE coalesce(c.cohort_borrow_events, 0) / cast(sb.all_borrow_events AS double) END AS event_coverage_share,
    CASE WHEN coalesce(sb.all_borrowers, 0) = 0 THEN 0 ELSE coalesce(c.cohort_borrowers, 0) / cast(sb.all_borrowers AS double) END AS borrower_coverage_share,
    CASE WHEN coalesce(sb.all_borrow_usd, 0) = 0 THEN 0 ELSE coalesce(c.cohort_borrow_usd, 0) / cast(sb.all_borrow_usd AS double) END AS usd_coverage_share,
    CASE WHEN coalesce(sb.all_borrow_events, 0) = 0 THEN TRUE ELSE FALSE END AS explicit_zero_row,
    m.query_version,
    m.as_of_utc,
    m.parameter_hash
FROM selected_pairs sp
LEFT JOIN stable_borrows sb
  ON sb.chain = sp.chain
 AND sb.protocol = sp.protocol
LEFT JOIN cohort c
  ON c.chain = sp.chain
 AND c.protocol = sp.protocol
CROSS JOIN meta m
ORDER BY sp.chain, sp.protocol;
