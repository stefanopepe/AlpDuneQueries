-- ============================================================
-- Query: btc_collateral_market_map_independent
-- Description: Independent market/token map for Morpho Blue + Aave V3
-- Scope: ethereum + base, phase-1 protocols only
-- Params: {{start_ts}}, {{end_ts}}, {{chains}}, {{protocols}}, {{stable_symbols}}, {{btc_symbols}}
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
    SELECT trim(x) AS chain
    FROM params
    CROSS JOIN UNNEST(split(chains_csv, ',')) AS t(x)
),
selected_protocols AS (
    SELECT trim(x) AS protocol
    FROM params
    CROSS JOIN UNNEST(split(protocols_csv, ',')) AS t(x)
),
selected_stable_symbols AS (
    SELECT trim(x) AS symbol
    FROM params
    CROSS JOIN UNNEST(split(stable_symbols_csv, ',')) AS t(x)
),
selected_btc_symbols AS (
    SELECT trim(x) AS symbol
    FROM params
    CROSS JOIN UNNEST(split(btc_symbols_csv, ',')) AS t(x)
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
morpho_markets AS (
    SELECT
        'ethereum' AS chain,
        'morpho_blue' AS protocol,
        CAST(cm.id AS varchar) AS market_id,
        from_hex(substr(json_extract_scalar(cm.marketParams, '$.loanToken'), 3)) AS loan_token,
        from_hex(substr(json_extract_scalar(cm.marketParams, '$.collateralToken'), 3)) AS collateral_token,
        cm.evt_block_time AS event_time_utc
    FROM morpho_blue_ethereum.morphoblue_evt_createmarket cm
    WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
      AND 'morpho_blue' IN (SELECT protocol FROM selected_protocols)
      AND cm.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
      AND cm.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)

    UNION ALL

    SELECT
        'base' AS chain,
        'morpho_blue' AS protocol,
        CAST(cm.id AS varchar) AS market_id,
        from_hex(substr(json_extract_scalar(cm.marketParams, '$.loanToken'), 3)) AS loan_token,
        from_hex(substr(json_extract_scalar(cm.marketParams, '$.collateralToken'), 3)) AS collateral_token,
        cm.evt_block_time AS event_time_utc
    FROM morpho_blue_base.morphoblue_evt_createmarket cm
    WHERE 'base' IN (SELECT chain FROM selected_chains)
      AND 'morpho_blue' IN (SELECT protocol FROM selected_protocols)
      AND cm.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
      AND cm.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
),
aave_reserves AS (
    SELECT
        'ethereum' AS chain,
        'aave_v3' AS protocol,
        concat('reserve:', lower(to_hex(b.reserve))) AS market_id,
        b.reserve AS loan_token,
        CAST(NULL AS varbinary) AS collateral_token,
        min(b.evt_block_time) AS event_time_utc
    FROM aave_v3_ethereum.pool_evt_borrow b
    WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
      AND 'aave_v3' IN (SELECT protocol FROM selected_protocols)
      AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
      AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
    GROUP BY 1, 2, 3, 4, 5

    UNION ALL

    SELECT
        'base' AS chain,
        'aave_v3' AS protocol,
        concat('reserve:', lower(to_hex(b.reserve))) AS market_id,
        b.reserve AS loan_token,
        CAST(NULL AS varbinary) AS collateral_token,
        min(b.evt_block_time) AS event_time_utc
    FROM aave_v3_base.pool_evt_borrow b
    WHERE 'base' IN (SELECT chain FROM selected_chains)
      AND 'aave_v3' IN (SELECT protocol FROM selected_protocols)
      AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
      AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
    GROUP BY 1, 2, 3, 4, 5
),
market_union AS (
    SELECT * FROM morpho_markets
    UNION ALL
    SELECT * FROM aave_reserves
)
SELECT
    mu.chain,
    mu.protocol,
    mu.market_id,
    lower(to_hex(mu.loan_token)) AS loan_token_address,
    lower(coalesce(lt.symbol, 'unknown')) AS loan_token_symbol,
    lt.decimals AS loan_token_decimals,
    lower(to_hex(mu.collateral_token)) AS collateral_token_address,
    lower(coalesce(ct.symbol, 'unknown')) AS collateral_token_symbol,
    ct.decimals AS collateral_token_decimals,
    CAST(mu.loan_token IN (SELECT token_address FROM allowed_stables WHERE chain = mu.chain) AS boolean) AS is_stable_loan_token,
    CAST(mu.collateral_token IN (SELECT token_address FROM btc_registry WHERE chain = mu.chain) AS boolean) AS is_btc_collateral_token,
    CAST(mu.event_time_utc AS timestamp) AS event_time_utc,
    CAST(NULL AS timestamp) AS borrow_time_utc,
    CAST(date_trunc('day', mu.event_time_utc) AS date) AS day_utc,
    m.query_version,
    m.as_of_utc,
    m.parameter_hash
FROM market_union mu
CROSS JOIN meta m
LEFT JOIN tokens.erc20 lt
  ON lt.blockchain = mu.chain
 AND lt.contract_address = mu.loan_token
LEFT JOIN tokens.erc20 ct
  ON ct.blockchain = mu.chain
 AND ct.contract_address = mu.collateral_token
ORDER BY mu.chain, mu.protocol, mu.market_id;
