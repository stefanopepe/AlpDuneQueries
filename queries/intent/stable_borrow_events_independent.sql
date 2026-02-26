-- ============================================================
-- Query: stable_borrow_events_independent
-- Description: Independent stable borrow events for Morpho Blue + Aave V3
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
selected_stable_symbols AS (
    SELECT trim(x) AS symbol FROM params CROSS JOIN UNNEST(split(stable_symbols_csv, ',')) AS t(x)
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
stable_registry AS (
    SELECT * FROM (
        VALUES
            ('ethereum', 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48, 'usdc'),
            ('ethereum', 0xdac17f958d2ee523a2206206994597c13d831ec7, 'usdt'),
            ('ethereum', 0x6b175474e89094c44da98b954eedeac495271d0f, 'dai'),
            ('base', 0x833589fcd6edb6e08f4c7c32d4f71b54bda02913, 'usdc'),
            ('base', 0xd9aaec86b65d86f6a7b5a5b0c42ffa531710b6ca, 'usdbc'),
            ('base', 0xfde4c96c8593536e31f229ea8f37b2ada2699bb2, 'usdt'),
            ('base', 0x50c5725949a6f0c72e6c4a641f24049a917db0cb, 'dai')
    ) AS t(chain, token_address, token_symbol)
    WHERE token_symbol IN (SELECT symbol FROM selected_stable_symbols)
),
discovered_stables AS (
    SELECT t.blockchain AS chain, t.contract_address AS token_address, lower(t.symbol) AS token_symbol
    FROM tokens.erc20 t
    WHERE t.blockchain IN (SELECT chain FROM selected_chains)
      AND lower(t.symbol) IN (SELECT symbol FROM selected_stable_symbols)
),
allowed_stables AS (
    SELECT * FROM stable_registry
    UNION
    SELECT * FROM discovered_stables
),
morpho_market_map AS (
    SELECT
        'ethereum' AS chain,
        cm.id AS market_id,
        from_hex(substr(json_extract_scalar(cm.marketParams, '$.loanToken'), 3)) AS loan_token
    FROM morpho_blue_ethereum.morphoblue_evt_createmarket cm
    WHERE 'ethereum' IN (SELECT chain FROM selected_chains)

    UNION ALL

    SELECT
        'base' AS chain,
        cm.id AS market_id,
        from_hex(substr(json_extract_scalar(cm.marketParams, '$.loanToken'), 3)) AS loan_token
    FROM morpho_blue_base.morphoblue_evt_createmarket cm
    WHERE 'base' IN (SELECT chain FROM selected_chains)
),
morpho_borrows AS (
    SELECT
        'ethereum' AS chain,
        'morpho_blue' AS protocol,
        b.evt_block_time AS borrow_time_utc,
        b.evt_block_time AS event_time_utc,
        CAST(date_trunc('day', b.evt_block_time) AS date) AS day_utc,
        b.evt_tx_hash AS borrow_tx_hash,
        b.evt_index AS evt_index,
        coalesce(b.onBehalf, b.caller) AS borrower_address,
        coalesce(b.onBehalf, b.caller) AS receiver_address,
        mm.loan_token AS debt_token_address,
        CAST(b.assets AS double) AS debt_amount_raw
    FROM morpho_blue_ethereum.morphoblue_evt_borrow b
    INNER JOIN morpho_market_map mm
      ON mm.chain = 'ethereum'
     AND mm.market_id = b.id
    WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
      AND 'morpho_blue' IN (SELECT protocol FROM selected_protocols)
      AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
      AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)

    UNION ALL

    SELECT
        'base' AS chain,
        'morpho_blue' AS protocol,
        b.evt_block_time AS borrow_time_utc,
        b.evt_block_time AS event_time_utc,
        CAST(date_trunc('day', b.evt_block_time) AS date) AS day_utc,
        b.evt_tx_hash AS borrow_tx_hash,
        b.evt_index AS evt_index,
        coalesce(b.onBehalf, b.caller) AS borrower_address,
        coalesce(b.onBehalf, b.caller) AS receiver_address,
        mm.loan_token AS debt_token_address,
        CAST(b.assets AS double) AS debt_amount_raw
    FROM morpho_blue_base.morphoblue_evt_borrow b
    INNER JOIN morpho_market_map mm
      ON mm.chain = 'base'
     AND mm.market_id = b.id
    WHERE 'base' IN (SELECT chain FROM selected_chains)
      AND 'morpho_blue' IN (SELECT protocol FROM selected_protocols)
      AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
      AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
),
aave_borrows AS (
    SELECT
        'ethereum' AS chain,
        'aave_v3' AS protocol,
        b.evt_block_time AS borrow_time_utc,
        b.evt_block_time AS event_time_utc,
        CAST(date_trunc('day', b.evt_block_time) AS date) AS day_utc,
        b.evt_tx_hash AS borrow_tx_hash,
        b.evt_index AS evt_index,
        b.onBehalfOf AS borrower_address,
        b.onBehalfOf AS receiver_address,
        b.reserve AS debt_token_address,
        CAST(b.amount AS double) AS debt_amount_raw
    FROM aave_v3_ethereum.pool_evt_borrow b
    WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
      AND 'aave_v3' IN (SELECT protocol FROM selected_protocols)
      AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
      AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)

    UNION ALL

    SELECT
        'base' AS chain,
        'aave_v3' AS protocol,
        b.evt_block_time AS borrow_time_utc,
        b.evt_block_time AS event_time_utc,
        CAST(date_trunc('day', b.evt_block_time) AS date) AS day_utc,
        b.evt_tx_hash AS borrow_tx_hash,
        b.evt_index AS evt_index,
        b.onBehalfOf AS borrower_address,
        b.onBehalfOf AS receiver_address,
        b.reserve AS debt_token_address,
        CAST(b.amount AS double) AS debt_amount_raw
    FROM aave_v3_base.pool_evt_borrow b
    WHERE 'base' IN (SELECT chain FROM selected_chains)
      AND 'aave_v3' IN (SELECT protocol FROM selected_protocols)
      AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
      AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
),
raw_borrows AS (
    SELECT * FROM morpho_borrows
    UNION ALL
    SELECT * FROM aave_borrows
),
filtered_borrows AS (
    SELECT rb.*
    FROM raw_borrows rb
    WHERE rb.debt_token_address IN (
        SELECT token_address FROM allowed_stables WHERE chain = rb.chain
    )
)
SELECT
    concat(
        fb.chain, ':', fb.protocol, ':', lower(to_hex(fb.borrow_tx_hash)), ':', cast(fb.evt_index AS varchar)
    ) AS borrow_id,
    fb.chain,
    fb.protocol,
    fb.borrow_time_utc,
    fb.event_time_utc,
    fb.day_utc,
    fb.borrow_tx_hash,
    fb.evt_index,
    lower(to_hex(fb.borrower_address)) AS borrower,
    lower(to_hex(fb.receiver_address)) AS receiver,
    lower(to_hex(fb.debt_token_address)) AS debt_token_address,
    lower(coalesce(t.symbol, 'unknown')) AS debt_token_symbol,
    coalesce(t.decimals, 18) AS debt_token_decimals,
    fb.debt_amount_raw AS borrow_amount_raw,
    fb.debt_amount_raw / power(10, coalesce(t.decimals, 18)) AS borrow_amount,
    (fb.debt_amount_raw / power(10, coalesce(t.decimals, 18))) * coalesce(p.price, 0) AS borrow_amount_usd,
    m.query_version,
    m.as_of_utc,
    m.parameter_hash
FROM filtered_borrows fb
CROSS JOIN meta m
LEFT JOIN tokens.erc20 t
  ON t.blockchain = fb.chain
 AND t.contract_address = fb.debt_token_address
LEFT JOIN prices.usd p
  ON p.blockchain = fb.chain
 AND p.contract_address = fb.debt_token_address
 AND p.minute = date_trunc('minute', fb.borrow_time_utc)
ORDER BY fb.borrow_time_utc, fb.chain, fb.protocol, fb.evt_index;
