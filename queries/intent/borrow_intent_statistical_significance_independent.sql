-- ============================================================
-- Query: borrow_intent_statistical_significance_independent
-- Description: Signal adequacy and confidence intervals for publication gating
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
        cm.id AS market_id,
        from_hex(substr(json_extract_scalar(cm.marketParams, '$.loanToken'), 3)) AS loan_token,
        from_hex(substr(json_extract_scalar(cm.marketParams, '$.collateralToken'), 3)) AS collateral_token
    FROM morpho_blue_ethereum.morphoblue_evt_createmarket cm
    WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
    UNION ALL
    SELECT
        'base' AS chain,
        cm.id AS market_id,
        from_hex(substr(json_extract_scalar(cm.marketParams, '$.loanToken'), 3)) AS loan_token,
        from_hex(substr(json_extract_scalar(cm.marketParams, '$.collateralToken'), 3)) AS collateral_token
    FROM morpho_blue_base.morphoblue_evt_createmarket cm
    WHERE 'base' IN (SELECT chain FROM selected_chains)
),
raw_borrows AS (
    SELECT 'ethereum' AS chain, 'morpho_blue' AS protocol, b.evt_block_time AS borrow_time_utc, b.evt_tx_hash AS borrow_tx_hash, b.evt_index,
           coalesce(b.onBehalf, b.caller) AS borrower_address, coalesce(b.onBehalf, b.caller) AS receiver_address,
           mm.loan_token AS debt_token_address, mm.collateral_token AS market_collateral_token, CAST(b.assets AS double) AS debt_amount_raw
    FROM morpho_blue_ethereum.morphoblue_evt_borrow b
    INNER JOIN morpho_markets mm ON mm.chain = 'ethereum' AND mm.market_id = b.id
    WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
      AND 'morpho_blue' IN (SELECT protocol FROM selected_protocols)
      AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
      AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)

    UNION ALL
    SELECT 'base', 'morpho_blue', b.evt_block_time, b.evt_tx_hash, b.evt_index,
           coalesce(b.onBehalf, b.caller), coalesce(b.onBehalf, b.caller),
           mm.loan_token, mm.collateral_token, CAST(b.assets AS double)
    FROM morpho_blue_base.morphoblue_evt_borrow b
    INNER JOIN morpho_markets mm ON mm.chain = 'base' AND mm.market_id = b.id
    WHERE 'base' IN (SELECT chain FROM selected_chains)
      AND 'morpho_blue' IN (SELECT protocol FROM selected_protocols)
      AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
      AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)

    UNION ALL
    SELECT 'ethereum', 'aave_v3', b.evt_block_time, b.evt_tx_hash, b.evt_index,
           b.onBehalfOf, b.onBehalfOf, b.reserve, CAST(NULL AS varbinary), CAST(b.amount AS double)
    FROM aave_v3_ethereum.pool_evt_borrow b
    WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
      AND 'aave_v3' IN (SELECT protocol FROM selected_protocols)
      AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
      AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)

    UNION ALL
    SELECT 'base', 'aave_v3', b.evt_block_time, b.evt_tx_hash, b.evt_index,
           b.onBehalfOf, b.onBehalfOf, b.reserve, CAST(NULL AS varbinary), CAST(b.amount AS double)
    FROM aave_v3_base.pool_evt_borrow b
    WHERE 'base' IN (SELECT chain FROM selected_chains)
      AND 'aave_v3' IN (SELECT protocol FROM selected_protocols)
      AND b.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp)
      AND b.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
),
borrows AS (
    SELECT rb.*
    FROM raw_borrows rb
    WHERE rb.debt_token_address IN (SELECT token_address FROM allowed_stables WHERE chain = rb.chain)
),
aave_btc_positions AS (
    SELECT
        ce.chain,
        ce.protocol,
        lower(to_hex(ce.borrower_address)) AS borrower,
        ce.event_time_utc,
        sum(ce.delta_amount) OVER (
            PARTITION BY ce.chain, ce.protocol, ce.borrower_address, ce.collateral_token_address
            ORDER BY ce.event_time_utc, ce.event_tx_hash, ce.evt_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS position_amount,
        lead(ce.event_time_utc) OVER (
            PARTITION BY ce.chain, ce.protocol, ce.borrower_address, ce.collateral_token_address
            ORDER BY ce.event_time_utc, ce.event_tx_hash, ce.evt_index
        ) AS next_event_time_utc
    FROM (
        SELECT 'ethereum' AS chain, 'aave_v3' AS protocol, s.onBehalfOf AS borrower_address, s.reserve AS collateral_token_address,
               s.evt_block_time AS event_time_utc, s.evt_tx_hash AS event_tx_hash, s.evt_index,
               CAST(s.amount AS double) / power(10, coalesce(t.decimals, 18)) AS delta_amount
        FROM aave_v3_ethereum.pool_evt_supply s
        LEFT JOIN tokens.erc20 t ON t.blockchain = 'ethereum' AND t.contract_address = s.reserve
        WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
          AND s.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp) - INTERVAL '365' DAY
          AND s.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
        UNION ALL
        SELECT 'ethereum', 'aave_v3', w.user, w.reserve, w.evt_block_time, w.evt_tx_hash, w.evt_index,
               -CAST(w.amount AS double) / power(10, coalesce(t.decimals, 18))
        FROM aave_v3_ethereum.pool_evt_withdraw w
        LEFT JOIN tokens.erc20 t ON t.blockchain = 'ethereum' AND t.contract_address = w.reserve
        WHERE 'ethereum' IN (SELECT chain FROM selected_chains)
          AND w.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp) - INTERVAL '365' DAY
          AND w.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
        UNION ALL
        SELECT 'base', 'aave_v3', s.onBehalfOf, s.reserve, s.evt_block_time, s.evt_tx_hash, s.evt_index,
               CAST(s.amount AS double) / power(10, coalesce(t.decimals, 18))
        FROM aave_v3_base.pool_evt_supply s
        LEFT JOIN tokens.erc20 t ON t.blockchain = 'base' AND t.contract_address = s.reserve
        WHERE 'base' IN (SELECT chain FROM selected_chains)
          AND s.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp) - INTERVAL '365' DAY
          AND s.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
        UNION ALL
        SELECT 'base', 'aave_v3', w.user, w.reserve, w.evt_block_time, w.evt_tx_hash, w.evt_index,
               -CAST(w.amount AS double) / power(10, coalesce(t.decimals, 18))
        FROM aave_v3_base.pool_evt_withdraw w
        LEFT JOIN tokens.erc20 t ON t.blockchain = 'base' AND t.contract_address = w.reserve
        WHERE 'base' IN (SELECT chain FROM selected_chains)
          AND w.evt_block_time >= CAST((SELECT start_ts_txt FROM params) AS timestamp) - INTERVAL '365' DAY
          AND w.evt_block_time < CAST((SELECT end_ts_txt FROM params) AS timestamp)
    ) ce
    WHERE ce.collateral_token_address IN (SELECT token_address FROM btc_registry WHERE chain = ce.chain)
),
cohort_borrows AS (
    SELECT
        b.*,
        concat(b.chain, ':', b.protocol, ':', lower(to_hex(b.borrow_tx_hash)), ':', cast(b.evt_index AS varchar)) AS borrow_id,
        CASE
            WHEN b.protocol = 'morpho_blue' AND b.market_collateral_token IN (SELECT token_address FROM btc_registry WHERE chain = b.chain)
                THEN TRUE
            WHEN b.protocol = 'aave_v3' AND EXISTS (
                SELECT 1 FROM aave_btc_positions p
                WHERE p.chain = b.chain
                  AND p.protocol = b.protocol
                  AND p.borrower = lower(to_hex(b.borrower_address))
                  AND p.event_time_utc <= b.borrow_time_utc
                  AND (p.next_event_time_utc IS NULL OR b.borrow_time_utc < p.next_event_time_utc)
                  AND p.position_amount > 0
            ) THEN TRUE
            ELSE FALSE
        END AS is_btc_backed
    FROM borrows b
),
cohort AS (
    SELECT *
    FROM cohort_borrows
    WHERE is_btc_backed = TRUE
),
btc_swaps AS (
    SELECT
        c.borrow_id,
        max(CASE WHEN d.block_time <= c.borrow_time_utc + INTERVAL '1' HOUR THEN 1 ELSE 0 END) AS has_swap_1h,
        max(CASE WHEN d.block_time <= c.borrow_time_utc + INTERVAL '24' HOUR THEN 1 ELSE 0 END) AS has_swap_24h
    FROM cohort c
    JOIN dex.trades d
      ON d.blockchain = c.chain
     AND lower(to_hex(d.taker)) = lower(to_hex(c.receiver_address))
     AND d.block_time >= c.borrow_time_utc
     AND d.block_time <= c.borrow_time_utc + INTERVAL '24' HOUR
     AND d.token_sold_address = c.debt_token_address
     AND d.token_bought_address IN (SELECT token_address FROM btc_registry WHERE chain = c.chain)
    GROUP BY 1
),
btc_adds AS (
    SELECT
        c.borrow_id,
        max(CASE WHEN e.event_time_utc <= c.borrow_time_utc + INTERVAL '1' HOUR THEN 1 ELSE 0 END) AS has_add_1h,
        max(CASE WHEN e.event_time_utc <= c.borrow_time_utc + INTERVAL '24' HOUR THEN 1 ELSE 0 END) AS has_add_24h
    FROM cohort c
    JOIN (
        SELECT 'ethereum' AS chain, 'morpho_blue' AS protocol, coalesce(s.onBehalf, s.caller) AS borrower_address,
               s.evt_block_time AS event_time_utc
        FROM morpho_blue_ethereum.morphoblue_evt_supplycollateral s
        UNION ALL
        SELECT 'base', 'morpho_blue', coalesce(s.onBehalf, s.caller), s.evt_block_time
        FROM morpho_blue_base.morphoblue_evt_supplycollateral s
        UNION ALL
        SELECT 'ethereum', 'aave_v3', s.onBehalfOf, s.evt_block_time
        FROM aave_v3_ethereum.pool_evt_supply s
        UNION ALL
        SELECT 'base', 'aave_v3', s.onBehalfOf, s.evt_block_time
        FROM aave_v3_base.pool_evt_supply s
    ) e
      ON e.chain = c.chain
     AND e.protocol = c.protocol
     AND lower(to_hex(e.borrower_address)) = lower(to_hex(c.borrower_address))
     AND e.event_time_utc >= c.borrow_time_utc
     AND e.event_time_utc <= c.borrow_time_utc + INTERVAL '24' HOUR
    GROUP BY 1
),
bridge_registry AS (
    SELECT * FROM (
        VALUES
            ('ethereum', lower('0x3154cf16ccdb4c6d922629664174b904d80f2c35')),
            ('base', lower('0x4200000000000000000000000000000000000010'))
    ) AS t(chain, address)
),
outflows_24h AS (
    SELECT
        c.borrow_id,
        c.chain,
        lower(to_hex(t."to")) AS recipient,
        sum(coalesce(t.amount_usd, 0.0)) AS usd_24h
    FROM cohort c
    JOIN tokens.transfers t
      ON t.blockchain = c.chain
     AND lower(to_hex(t."from")) = lower(to_hex(c.receiver_address))
     AND t.contract_address = c.debt_token_address
     AND t.block_time >= c.borrow_time_utc
     AND t.block_time <= c.borrow_time_utc + INTERVAL '24' HOUR
    GROUP BY 1, 2, 3
),
outflows_ranked AS (
    SELECT
        o.*,
        row_number() OVER (PARTITION BY o.borrow_id ORDER BY o.usd_24h DESC, o.recipient) AS rn,
        sum(o.usd_24h) OVER (PARTITION BY o.borrow_id ORDER BY o.usd_24h DESC, o.recipient) /
            nullif(sum(o.usd_24h) OVER (PARTITION BY o.borrow_id), 0) AS cum_share
    FROM outflows_24h o
),
retained_recipients AS (
    SELECT borrow_id, chain, recipient
    FROM outflows_ranked
    WHERE rn = 1 OR cum_share <= 0.90
),
outflows_7d AS (
    SELECT
        c.borrow_id,
        c.chain,
        rr.recipient,
        sum(coalesce(t.amount_usd, 0.0)) AS usd_7d
    FROM cohort c
    JOIN retained_recipients rr
      ON rr.borrow_id = c.borrow_id
    JOIN tokens.transfers t
      ON t.blockchain = c.chain
     AND lower(to_hex(t."from")) = lower(to_hex(c.receiver_address))
     AND lower(to_hex(t."to")) = rr.recipient
     AND t.contract_address = c.debt_token_address
     AND t.block_time >= c.borrow_time_utc
     AND t.block_time <= c.borrow_time_utc + INTERVAL '7' DAY
    GROUP BY 1, 2, 3
),
recipient_types AS (
    SELECT
        o.borrow_id,
        o.chain,
        o.recipient,
        o.usd_7d,
        CASE
            WHEN o.chain = 'ethereum' AND et.address IS NOT NULL THEN 'contract'
            WHEN o.chain = 'base' AND bt.address IS NOT NULL THEN 'contract'
            ELSE 'eoa'
        END AS recipient_type
    FROM outflows_7d o
    LEFT JOIN ethereum.creation_traces et
      ON o.chain = 'ethereum'
     AND lower(to_hex(et.address)) = o.recipient
    LEFT JOIN base.creation_traces bt
      ON o.chain = 'base'
     AND lower(to_hex(bt.address)) = o.recipient
),
recipient_labels AS (
    SELECT
        rt.borrow_id,
        rt.chain,
        rt.recipient,
        rt.usd_7d,
        rt.recipient_type,
        lower(coalesce(la.category, '')) AS label_category,
        lower(coalesce(la.name, '')) AS label_name
    FROM recipient_types rt
    LEFT JOIN labels.addresses la
      ON lower(to_hex(la.address)) = rt.recipient
     AND lower(la.blockchain) = rt.chain
),
outflow_features AS (
    SELECT
        rl.borrow_id,
        sum(rl.usd_7d) AS total_outflow_usd_7d,
        sum(CASE WHEN rl.recipient_type = 'eoa' THEN rl.usd_7d ELSE 0 END) AS eoa_outflow_usd_7d,
        max(CASE
            WHEN rl.recipient IN (SELECT address FROM bridge_registry WHERE chain = rl.chain)
              OR rl.label_category LIKE '%bridge%'
              OR rl.label_name LIKE '%bridge%'
            THEN 1 ELSE 0 END) AS has_bridge_7d,
        max(CASE
            WHEN rl.label_category LIKE '%cex%'
              OR rl.label_category LIKE '%exchange%'
              OR rl.label_name LIKE '%binance%'
              OR rl.label_name LIKE '%coinbase%'
              OR rl.label_name LIKE '%kraken%'
            THEN 1 ELSE 0 END) AS has_cex_label_7d
    FROM recipient_labels rl
    GROUP BY 1
),
defi_retained AS (
    SELECT
        c.borrow_id,
        max(1) AS has_defi_contract_7d
    FROM cohort c
    JOIN tokens.transfers t
      ON t.blockchain = c.chain
     AND lower(to_hex(t."from")) = lower(to_hex(c.receiver_address))
     AND t.contract_address = c.debt_token_address
     AND t.block_time >= c.borrow_time_utc
     AND t.block_time <= c.borrow_time_utc + INTERVAL '7' DAY
    LEFT JOIN ethereum.creation_traces et
      ON c.chain = 'ethereum'
     AND lower(to_hex(et.address)) = lower(to_hex(t."to"))
    LEFT JOIN base.creation_traces bt
      ON c.chain = 'base'
     AND lower(to_hex(bt.address)) = lower(to_hex(t."to"))
    WHERE (c.chain = 'ethereum' AND et.address IS NOT NULL)
       OR (c.chain = 'base' AND bt.address IS NOT NULL)
    GROUP BY 1
)
,
signals AS (
    SELECT
        c.borrow_id,
        c.chain,
        c.protocol,
        c.borrow_time_utc,
        CAST(coalesce(bs.has_swap_1h, 0) = 1 AND coalesce(ba.has_add_1h, 0) = 1 AS boolean) AS signal_loop_1h,
        CAST(coalesce(bs.has_swap_24h, 0) = 1 AND coalesce(ba.has_add_24h, 0) = 1 AS boolean) AS signal_loop_24h,
        CAST(coalesce(dr.has_defi_contract_7d, 0) = 1 AS boolean) AS signal_defi_7d,
        CAST(coalesce(ofe.has_bridge_7d, 0) = 1 AS boolean) AS signal_bridge_7d,
        CAST(
            coalesce(ofe.has_cex_label_7d, 0) = 1
            OR (
                coalesce(ofe.total_outflow_usd_7d, 0) > 0
                AND coalesce(ofe.eoa_outflow_usd_7d, 0) / nullif(coalesce(ofe.total_outflow_usd_7d, 0), 0) >= 0.70
            )
            AS boolean
        ) AS signal_offramp_7d
    FROM cohort c
    LEFT JOIN btc_swaps bs ON bs.borrow_id = c.borrow_id
    LEFT JOIN btc_adds ba ON ba.borrow_id = c.borrow_id
    LEFT JOIN outflow_features ofe ON ofe.borrow_id = c.borrow_id
    LEFT JOIN defi_retained dr ON dr.borrow_id = c.borrow_id
),
agg AS (
    SELECT
        chain,
        protocol,
        count(*) AS n_borrows,
        sum(CASE WHEN signal_loop_24h THEN 1 ELSE 0 END) AS n_loop24
    FROM signals
    GROUP BY 1, 2
)
SELECT
    CAST(NULL AS varchar) AS borrow_id,
    a.chain,
    a.protocol,
    CAST(NULL AS timestamp) AS borrow_time_utc,
    current_timestamp AS event_time_utc,
    CAST(current_date AS date) AS day_utc,
    a.n_borrows,
    a.n_loop24,
    cast(a.n_loop24 AS double) / nullif(cast(a.n_borrows AS double), 0) AS ls24_event_rate,
    (
        (
            cast(a.n_loop24 AS double) / nullif(cast(a.n_borrows AS double), 0)
            + 1.96 * 1.96 / (2 * a.n_borrows)
            - 1.96 * sqrt(
                (
                    (cast(a.n_loop24 AS double) / nullif(cast(a.n_borrows AS double), 0))
                    * (1 - (cast(a.n_loop24 AS double) / nullif(cast(a.n_borrows AS double), 0)))
                    + 1.96 * 1.96 / (4 * a.n_borrows)
                ) / a.n_borrows
            )
        ) / (1 + 1.96 * 1.96 / a.n_borrows)
    ) AS ci95_low,
    (
        (
            cast(a.n_loop24 AS double) / nullif(cast(a.n_borrows AS double), 0)
            + 1.96 * 1.96 / (2 * a.n_borrows)
            + 1.96 * sqrt(
                (
                    (cast(a.n_loop24 AS double) / nullif(cast(a.n_borrows AS double), 0))
                    * (1 - (cast(a.n_loop24 AS double) / nullif(cast(a.n_borrows AS double), 0)))
                    + 1.96 * 1.96 / (4 * a.n_borrows)
                ) / a.n_borrows
            )
        ) / (1 + 1.96 * 1.96 / a.n_borrows)
    ) AS ci95_high,
    CAST(a.n_borrows >= 100 AS boolean) AS gate_min_sample_pass,
    CAST(
        (
            (
                (
                    cast(a.n_loop24 AS double) / nullif(cast(a.n_borrows AS double), 0)
                    + 1.96 * 1.96 / (2 * a.n_borrows)
                    + 1.96 * sqrt(
                        (
                            (cast(a.n_loop24 AS double) / nullif(cast(a.n_borrows AS double), 0))
                            * (1 - (cast(a.n_loop24 AS double) / nullif(cast(a.n_borrows AS double), 0)))
                            + 1.96 * 1.96 / (4 * a.n_borrows)
                        ) / a.n_borrows
                    )
                ) / (1 + 1.96 * 1.96 / a.n_borrows)
            )
            -
            (
                (
                    cast(a.n_loop24 AS double) / nullif(cast(a.n_borrows AS double), 0)
                    + 1.96 * 1.96 / (2 * a.n_borrows)
                    - 1.96 * sqrt(
                        (
                            (cast(a.n_loop24 AS double) / nullif(cast(a.n_borrows AS double), 0))
                            * (1 - (cast(a.n_loop24 AS double) / nullif(cast(a.n_borrows AS double), 0)))
                            + 1.96 * 1.96 / (4 * a.n_borrows)
                        ) / a.n_borrows
                    )
                ) / (1 + 1.96 * 1.96 / a.n_borrows)
            )
        ) <= 0.20
    AS boolean) AS gate_ci_width_pass,
    CAST(NULL AS boolean) AS signal_loop_1h,
    CAST(NULL AS boolean) AS signal_loop_24h,
    CAST(NULL AS boolean) AS signal_defi_7d,
    CAST(NULL AS boolean) AS signal_bridge_7d,
    CAST(NULL AS boolean) AS signal_offramp_7d,
    'ALL' AS intent_bucket,
    'significance_aggregate' AS intent_reason,
    m.query_version,
    m.as_of_utc,
    m.parameter_hash
FROM agg a
CROSS JOIN meta m
ORDER BY a.chain, a.protocol;
