-- ============================================================
-- Query: btc_collateral_positions_independent
-- Description: BTC collateral event ledger and running positions (Morpho + Aave)
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
selected_btc_symbols AS (
    SELECT trim(x) AS symbol FROM params CROSS JOIN UNNEST(split(btc_symbols_csv, ',')) AS t(x)
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
btc_registry AS (
    SELECT * FROM (
        VALUES
            ('ethereum', 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599, 'wbtc'),
            ('ethereum', 0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf, 'cbbtc'),
            ('ethereum', 0x18084fba666a33d37592fa2633fd49a74dd93a88, 'tbtc'),
            ('base', 0x0555e30da8f98308edb960aa94c0db47230d2b9c, 'wbtc'),
            ('base', 0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf, 'cbbtc'),
            ('base', 0x236aa50979d5f3de3bd1eeb40e81137f22ab794b, 'tbtc')
    ) AS t(chain, token_address, token_symbol)
    WHERE token_symbol IN (SELECT symbol FROM selected_btc_symbols)
),
morpho_markets AS (
    SELECT
        'ethereum' AS chain,
        cm.id AS market_id,
        from_hex(substr(json_extract_scalar(cm.marketParams, '$.collateralToken'), 3)) AS collateral_token
    FROM morpho_blue_ethereum.morphoblue_evt_createmarket cm
    WHERE 'ethereum' IN (SELECT chain FROM selected_chains)

    UNION ALL

    SELECT
        'base' AS chain,
        cm.id AS market_id,
        from_hex(substr(json_extract_scalar(cm.marketParams, '$.collateralToken'), 3)) AS collateral_token
    FROM morpho_blue_base.morphoblue_evt_createmarket cm
    WHERE 'base' IN (SELECT chain FROM selected_chains)
),
morpho_collateral_events AS (
    SELECT
        'ethereum' AS chain,
        'morpho_blue' AS protocol,
        s.evt_block_time AS event_time_utc,
        CAST(date_trunc('day', s.evt_block_time) AS date) AS day_utc,
        s.evt_tx_hash AS event_tx_hash,
        s.evt_index,
        coalesce(s.onBehalf, s.caller) AS borrower_address,
        mm.collateral_token AS collateral_token_address,
        CAST(s.assets AS double) AS delta_raw
    FROM morpho_blue_ethereum.morphoblue_evt_supplycollateral s
    INNER JOIN morpho_markets mm
      ON mm.chain = 'ethereum'
     AND mm.market_id = s.id
    WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
      AND 'morpho_blue' IN (SELECT protocol FROM selected_protocols)

    UNION ALL

    SELECT
        'ethereum' AS chain,
        'morpho_blue' AS protocol,
        w.evt_block_time AS event_time_utc,
        CAST(date_trunc('day', w.evt_block_time) AS date) AS day_utc,
        w.evt_tx_hash AS event_tx_hash,
        w.evt_index,
        coalesce(w.onBehalf, w.caller) AS borrower_address,
        mm.collateral_token AS collateral_token_address,
        -CAST(w.assets AS double) AS delta_raw
    FROM morpho_blue_ethereum.morphoblue_evt_withdrawcollateral w
    INNER JOIN morpho_markets mm
      ON mm.chain = 'ethereum'
     AND mm.market_id = w.id
    WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
      AND 'morpho_blue' IN (SELECT protocol FROM selected_protocols)

    UNION ALL

    SELECT
        'base' AS chain,
        'morpho_blue' AS protocol,
        s.evt_block_time AS event_time_utc,
        CAST(date_trunc('day', s.evt_block_time) AS date) AS day_utc,
        s.evt_tx_hash AS event_tx_hash,
        s.evt_index,
        coalesce(s.onBehalf, s.caller) AS borrower_address,
        mm.collateral_token AS collateral_token_address,
        CAST(s.assets AS double) AS delta_raw
    FROM morpho_blue_base.morphoblue_evt_supplycollateral s
    INNER JOIN morpho_markets mm
      ON mm.chain = 'base'
     AND mm.market_id = s.id
    WHERE 'base' IN (SELECT chain FROM selected_chains)
      AND 'morpho_blue' IN (SELECT protocol FROM selected_protocols)

    UNION ALL

    SELECT
        'base' AS chain,
        'morpho_blue' AS protocol,
        w.evt_block_time AS event_time_utc,
        CAST(date_trunc('day', w.evt_block_time) AS date) AS day_utc,
        w.evt_tx_hash AS event_tx_hash,
        w.evt_index,
        coalesce(w.onBehalf, w.caller) AS borrower_address,
        mm.collateral_token AS collateral_token_address,
        -CAST(w.assets AS double) AS delta_raw
    FROM morpho_blue_base.morphoblue_evt_withdrawcollateral w
    INNER JOIN morpho_markets mm
      ON mm.chain = 'base'
     AND mm.market_id = w.id
    WHERE 'base' IN (SELECT chain FROM selected_chains)
      AND 'morpho_blue' IN (SELECT protocol FROM selected_protocols)
),
aave_collateral_events AS (
    SELECT
        'ethereum' AS chain,
        'aave_v3' AS protocol,
        s.evt_block_time AS event_time_utc,
        CAST(date_trunc('day', s.evt_block_time) AS date) AS day_utc,
        s.evt_tx_hash AS event_tx_hash,
        s.evt_index,
        s.onBehalfOf AS borrower_address,
        s.reserve AS collateral_token_address,
        CAST(s.amount AS double) AS delta_raw
    FROM aave_v3_ethereum.pool_evt_supply s
    WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
      AND 'aave_v3' IN (SELECT protocol FROM selected_protocols)

    UNION ALL

    SELECT
        'ethereum' AS chain,
        'aave_v3' AS protocol,
        w.evt_block_time AS event_time_utc,
        CAST(date_trunc('day', w.evt_block_time) AS date) AS day_utc,
        w.evt_tx_hash AS event_tx_hash,
        w.evt_index,
        w.user AS borrower_address,
        w.reserve AS collateral_token_address,
        -CAST(w.amount AS double) AS delta_raw
    FROM aave_v3_ethereum.pool_evt_withdraw w
    WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
      AND 'aave_v3' IN (SELECT protocol FROM selected_protocols)

    UNION ALL

    SELECT
        'ethereum' AS chain,
        'aave_v3' AS protocol,
        l.evt_block_time AS event_time_utc,
        CAST(date_trunc('day', l.evt_block_time) AS date) AS day_utc,
        l.evt_tx_hash AS event_tx_hash,
        l.evt_index,
        l.user AS borrower_address,
        l.collateralAsset AS collateral_token_address,
        -CAST(l.liquidatedCollateralAmount AS double) AS delta_raw
    FROM aave_v3_ethereum.pool_evt_liquidationcall l
    WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
      AND 'aave_v3' IN (SELECT protocol FROM selected_protocols)

    UNION ALL

    SELECT
        'base' AS chain,
        'aave_v3' AS protocol,
        s.evt_block_time AS event_time_utc,
        CAST(date_trunc('day', s.evt_block_time) AS date) AS day_utc,
        s.evt_tx_hash AS event_tx_hash,
        s.evt_index,
        s.onBehalfOf AS borrower_address,
        s.reserve AS collateral_token_address,
        CAST(s.amount AS double) AS delta_raw
    FROM aave_v3_base.pool_evt_supply s
    WHERE 'base' IN (SELECT chain FROM selected_chains)
      AND 'aave_v3' IN (SELECT protocol FROM selected_protocols)

    UNION ALL

    SELECT
        'base' AS chain,
        'aave_v3' AS protocol,
        w.evt_block_time AS event_time_utc,
        CAST(date_trunc('day', w.evt_block_time) AS date) AS day_utc,
        w.evt_tx_hash AS event_tx_hash,
        w.evt_index,
        w.user AS borrower_address,
        w.reserve AS collateral_token_address,
        -CAST(w.amount AS double) AS delta_raw
    FROM aave_v3_base.pool_evt_withdraw w
    WHERE 'base' IN (SELECT chain FROM selected_chains)
      AND 'aave_v3' IN (SELECT protocol FROM selected_protocols)

    UNION ALL

    SELECT
        'base' AS chain,
        'aave_v3' AS protocol,
        l.evt_block_time AS event_time_utc,
        CAST(date_trunc('day', l.evt_block_time) AS date) AS day_utc,
        l.evt_tx_hash AS event_tx_hash,
        l.evt_index,
        l.user AS borrower_address,
        l.collateralAsset AS collateral_token_address,
        -CAST(l.liquidatedCollateralAmount AS double) AS delta_raw
    FROM aave_v3_base.pool_evt_liquidationcall l
    WHERE 'base' IN (SELECT chain FROM selected_chains)
      AND 'aave_v3' IN (SELECT protocol FROM selected_protocols)
),
all_collateral_events AS (
    SELECT * FROM morpho_collateral_events
    UNION ALL
    SELECT * FROM aave_collateral_events
),
filtered AS (
    SELECT e.*
    FROM all_collateral_events e
    WHERE e.collateral_token_address IN (
        SELECT token_address FROM btc_registry WHERE chain = e.chain
    )
      AND e.event_time_utc >= CAST((SELECT start_ts_txt FROM params) AS timestamp) - INTERVAL '365' DAY
      AND e.event_time_utc < CAST((SELECT end_ts_txt FROM params) AS timestamp)
),
enriched AS (
    SELECT
        f.chain,
        f.protocol,
        f.event_time_utc,
        f.day_utc,
        f.event_tx_hash,
        f.evt_index,
        lower(to_hex(f.borrower_address)) AS borrower,
        lower(to_hex(f.collateral_token_address)) AS collateral_token_address,
        lower(coalesce(t.symbol, 'unknown')) AS collateral_token_symbol,
        coalesce(t.decimals, 18) AS collateral_token_decimals,
        f.delta_raw,
        f.delta_raw / power(10, coalesce(t.decimals, 18)) AS delta_amount,
        (f.delta_raw / power(10, coalesce(t.decimals, 18))) * coalesce(p.price, 0) AS delta_amount_usd
    FROM filtered f
    LEFT JOIN tokens.erc20 t
      ON t.blockchain = f.chain
     AND t.contract_address = f.collateral_token_address
    LEFT JOIN prices.usd p
      ON p.blockchain = f.chain
     AND p.contract_address = f.collateral_token_address
     AND p.minute = date_trunc('minute', f.event_time_utc)
),
positioned AS (
    SELECT
        e.*,
        sum(e.delta_amount) OVER (
            PARTITION BY e.chain, e.protocol, e.borrower, e.collateral_token_address
            ORDER BY e.event_time_utc, e.event_tx_hash, e.evt_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS collateral_position_amount,
        sum(e.delta_amount_usd) OVER (
            PARTITION BY e.chain, e.protocol, e.borrower, e.collateral_token_address
            ORDER BY e.event_time_utc, e.event_tx_hash, e.evt_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS collateral_position_usd
    FROM enriched e
)
SELECT
    CAST(NULL AS varchar) AS borrow_id,
    p.chain,
    p.protocol,
    CAST(NULL AS timestamp) AS borrow_time_utc,
    p.event_time_utc,
    p.day_utc,
    p.event_tx_hash,
    p.evt_index,
    p.borrower,
    p.collateral_token_address,
    p.collateral_token_symbol,
    p.collateral_token_decimals,
    p.delta_amount,
    p.delta_amount_usd,
    p.collateral_position_amount,
    p.collateral_position_usd,
    m.query_version,
    m.as_of_utc,
    m.parameter_hash
FROM positioned p
CROSS JOIN meta m
WHERE p.event_time_utc >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
  AND p.event_time_utc < CAST((SELECT end_ts_txt FROM params) AS timestamp)
ORDER BY p.event_time_utc, p.chain, p.protocol, p.borrower;
