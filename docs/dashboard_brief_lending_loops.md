# Technical Brief: Lending Loops Dashboard

**For:** Senior Dune data engineer / dashboard builder
**Query suite:** `queries/ethereum/lending/lending_*.sql` (11 queries)
**Date:** 2026-02-17

**References:**
- [queries_schemas.md](./queries_schemas.md) — Full output schemas for every query
- [lending_protocol_schemas.md](./lending_protocol_schemas.md) — Dune table schemas for Aave V3, Morpho Blue, Compound V3/V2
- [dune_database_schemas.md](./dune_database_schemas.md) — General Dune table reference
- [CLAUDE.md](../CLAUDE.md) — SQL conventions, efficiency guidelines, parameterization patterns

---

## 1. What this query suite produces

The lending loops pipeline detects, measures, and visualizes **cross-protocol capital recycling** in Ethereum DeFi lending markets. The core insight: entities borrow stablecoins on Protocol A, supply them on Protocol B, borrow again on Protocol C, etc. — creating leverage loops that amplify credit and systemic risk.

The suite answers four questions:

| Question | Query |
|----------|-------|
| What are entities doing across lending protocols? | `lending_action_ledger_unified` |
| Where is capital flowing between protocols? | `lending_flow_stitching` |
| Who is looping, how deep, and how much? | `lending_loop_detection` → `lending_loop_metrics_daily` |
| What collateral backs these loops? | `lending_collateral_ledger` → `lending_loop_collateral_profile` |

Supporting queries provide drill-down views: per-entity balance sheets, per-entity action storyboards, and Sankey flow visualizations.

---

## 2. Data lineage

```
Raw Dune Tables:
  aave_v3_ethereum.pool_evt_*
  morpho_blue_ethereum.morphoblue_evt_*
  compound_v3_ethereum.comet_evt_*
  compound_ethereum.cerc20delegator_evt_*
  tokens.erc20, prices.usd
        │
        ▼
Tier 1 — Base Queries (materialized, incremental):
┌──────────────────────────────────────────────────────────────────┐
│  lending_action_ledger_unified (query_6687961)                   │
│    Unified stablecoin action ledger: supply/borrow/repay/        │
│    withdraw/liquidation across 4 protocols.                      │
│    Incremental with 1-day lookback.                              │
│                                                                  │
│  lending_collateral_ledger (query_6707791)                       │
│    Non-stablecoin collateral events: WBTC, WETH, wstETH,        │
│    weETH, rETH, cbETH. Independent base query.                  │
└──────────────┬────────────────────────────────┬──────────────────┘
               │                                │
               ▼                                │
Tier 2 — Base Query (materialized, incremental):│
┌──────────────────────────────────────────┐    │
│  lending_flow_stitching (query_6690272)  │    │
│    Cross-protocol flow detection:        │    │
│    borrow on P1 → supply on P2           │    │
│    Same-tx (atomic) + 2-min window       │    │
└──────┬──────┬──────┬─────────────────────┘    │
       │      │      │                          │
       ▼      ▼      ▼                          │
Tier 3 — Nested Queries (lightweight aggregations):
┌────────────┐ ┌────────────┐ ┌─────────────────────────┐
│ loop_      │ │ sankey_    │ │ loop_collateral_        │
│ detection  │ │ flows      │ │ profile                 │
│ (6702204)  │ │ (6708650)  │ │ (6708668)               │
│            │ │            │ │ joins flows +            │
│ Islands &  │ │ Edge list  │ │ collateral_ledger       │
│ gaps algo  │ │ for viz    │ │                         │
└──────┬─────┘ └────────────┘ └─────────────────────────┘
       │
       ▼
┌────────────┐
│ loop_      │
│ metrics_   │
│ daily      │
│ (6708794)  │
└────────────┘

Independent Nested Queries (read from unified ledger):
┌────────────────────┐  ┌──────────────────────┐
│ entity_balance_    │  │ entity_loop_         │
│ sheet (6708623)    │  │ storyboard (6708643) │
│ Running positions  │  │ Per-entity traces    │
└────────────────────┘  └──────────────────────┘

Protocol-Specific Base Queries (standalone):
┌────────────────────┐  ┌────────────────────┐
│ action_ledger_     │  │ action_ledger_     │
│ aave_v3 (6707805)  │  │ morpho (6708253)   │
└────────────────────┘  └────────────────────┘
```

**Key design decisions:**
- Tier 1 and Tier 2 base queries are **materialized** with incremental processing (`previous.query.result()`), breaking the inline query chain for downstream consumers.
- Nested queries are lightweight aggregations — all heavy computation (event normalization, price enrichment, flow stitching) happens in the base queries.
- See [queries_schemas.md § Ethereum / Lending Query Architecture](./queries_schemas.md#ethereum--lending-query-architecture) for the canonical architecture diagram.

---

## 3. Query inventory with Dune IDs

| Query | Dune ID | Type | Role |
|-------|---------|------|------|
| lending_action_ledger_unified | `query_6687961` | Base | Stablecoin action ledger (4 protocols) |
| lending_action_ledger_aave_v3 | `query_6707805` | Base | Aave V3 standalone ledger |
| lending_action_ledger_morpho | `query_6708253` | Base | Morpho Aave V2 standalone ledger |
| lending_flow_stitching | `query_6690272` | Base | Cross-protocol flow detection |
| lending_collateral_ledger | `query_6707791` | Base | Collateral event ledger |
| lending_loop_detection | `query_6702204` | Nested | Multi-hop loop identification |
| lending_loop_metrics_daily | `query_6708794` | Nested | Daily loop aggregates |
| lending_entity_balance_sheet | `query_6708623` | Nested | Per-entity running positions |
| lending_sankey_flows | `query_6708650` | Nested | Sankey edge list |
| lending_entity_loop_storyboard | `query_6708643` | Nested | Per-entity action traces |
| lending_loop_collateral_profile | `query_6708668` | Nested | Loop collateral attribution |

Full output schemas for every query are documented in [queries_schemas.md](./queries_schemas.md#ethereum--lending-query-architecture).

---

## 4. Protocols and asset scope

### Protocols covered

| Protocol | Identifier in data | Event tables |
|----------|--------------------|-------------|
| Aave V3 | `aave_v3` | `aave_v3_ethereum.pool_evt_*` |
| Morpho Blue | `morpho_blue` | `morpho_blue_ethereum.morphoblue_evt_*` |
| Compound V3 | `compound_v3` | `compound_v3_ethereum.comet_evt_*` |
| Compound V2 | `compound_v2` | `compound_ethereum.cerc20delegator_evt_*` |

See [lending_protocol_schemas.md](./lending_protocol_schemas.md) for complete table schemas, column names, and event signatures per protocol.

### Stablecoins tracked (action ledger + flows)

| Symbol | Address | Decimals |
|--------|---------|----------|
| USDC | `0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48` | 6 |
| USDT | `0xdac17f958d2ee523a2206206994597c13d831ec7` | 6 |
| DAI | `0x6b175474e89094c44da98b954eedeac495271d0f` | 18 |
| FRAX | `0x853d955acef822db058eb8505911ed77f175b99e` | 18 |

### Collateral assets tracked (collateral ledger)

| Symbol | Address | Category |
|--------|---------|----------|
| WBTC | `0x2260fac5e5542a773aa44fbcfedf7c193bc2c599` | `btc` |
| WETH | `0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2` | `eth` |
| wstETH | `0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0` | `eth_lst` |
| weETH | `0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee` | `eth_lst` |
| rETH | `0xae78736cd615f374d3085123a210448e74fc6393` | `eth_lst` |
| cbETH | `0xbe9895146f7af43049ca1c1ae358b0541ea49704` | `eth_lst` |

---

## 5. Dashboard layout

The dashboard is organized into four sections, each answering a progressively deeper question. A senior Dune engineer should build these as tabbed sections or vertically stacked panels.

```
┌─────────────────────────────────────────────────────────────┐
│  SECTION A: Loop Activity Overview                          │
│  Source: lending_loop_metrics_daily (query_6708794)          │
│  Panels: 5.1, 5.2, 5.3                                     │
├─────────────────────────────────────────────────────────────┤
│  SECTION B: Protocol Flow Topology                          │
│  Source: lending_sankey_flows (query_6708650)                │
│  Panels: 5.4, 5.5                                           │
├─────────────────────────────────────────────────────────────┤
│  SECTION C: Collateral Composition                          │
│  Source: lending_loop_collateral_profile (query_6708668)     │
│  Panels: 5.6, 5.7, 5.8                                     │
├─────────────────────────────────────────────────────────────┤
│  SECTION D: Entity Deep-Dive                                │
│  Source: entity_balance_sheet (query_6708623),               │
│          entity_loop_storyboard (query_6708643)              │
│  Panels: 5.9, 5.10                                          │
└─────────────────────────────────────────────────────────────┘
```

---

## 5.1 Visualization — Line chart: Daily loop count and unique loopers

**Source query:** `query_6708794` (lending_loop_metrics_daily)
**Purpose:** Track the volume and breadth of loop activity over time.

| Property | Value |
|----------|-------|
| X-axis | `day` |
| Y-axis (left) | `loops_started` |
| Y-axis (right) | `unique_loopers` |
| Chart type | Dual-axis line chart |

**Configuration notes:**
- Use dual Y-axes: left for loop count (absolute), right for unique entity count
- A divergence (loops rising faster than unique_loopers) indicates power users running multiple loops per day
- Consider adding `single_hop_loops`, `double_hop_loops`, `deep_loops` as a stacked area underneath to show depth composition

**What to look for:**
- Spikes in `deep_loops` (3+ hops) signal sophisticated leveraged strategies
- Sustained growth in `unique_loopers` indicates loop adoption is broadening, not just existing power users

---

## 5.2 Visualization — Area chart: Gross credit created (USD)

**Source query:** `query_6708794`
**Purpose:** Show the total synthetic credit amplification from lending loops.

| Property | Value |
|----------|-------|
| X-axis | `day` |
| Y-axis | `gross_credit_created_usd` |
| Chart type | Filled area |

**Configuration notes:**
- Format Y-axis as USD with abbreviations ($1.2M, $50K, etc.)
- This is **gross** borrowed amount across all hops — it double-counts capital that flows through multiple protocols. This is intentional: it measures credit amplification, not unique capital
- Add `avg_recursion_depth` as a secondary line (right axis) to correlate credit creation with loop complexity

**What to look for:**
- Credit creation spikes often precede or follow large market moves (rate changes, liquidation events)
- Increasing `avg_recursion_depth` with flat credit volume means smaller entities are looping more deeply

---

## 5.3 Visualization — Counter cards: Key metrics

**Source query:** `query_6708794` (latest row, `ORDER BY day DESC LIMIT 1`)
**Purpose:** At-a-glance KPIs for the most recent day.

| Card | Column | Format |
|------|--------|--------|
| Loops Today | `loops_started` | Integer |
| Active Loopers | `unique_loopers` | Integer |
| Credit Created | `gross_credit_created_usd` | USD |
| Max Depth | `max_recursion_depth` | Integer |
| Top Route | `top_protocol_pair` | Text |
| Top Route Volume | `top_pair_volume_usd` | USD |

---

## 5.4 Visualization — Sankey diagram: Protocol flow topology

**Source query:** `query_6708650` (lending_sankey_flows)
**Purpose:** Visualize the direction and magnitude of capital flows between protocols.

| Property | Value |
|----------|-------|
| Source node | `source` column (format: `{protocol}:borrow:{asset}`) |
| Target node | `target` column (format: `{protocol}:supply:{asset}`) |
| Link value | `value` (USD volume) |
| Chart type | Sankey diagram |

**Data preparation:**

Dune's native Sankey chart may not be available. If not, use the edge list as a table visualization or export to Plotly/D3. For a Dune-native approximation, use a grouped bar chart:

```sql
SELECT
    source,
    target,
    SUM(value) AS total_volume_usd,
    SUM(flow_count) AS total_flows
FROM query_6708650
WHERE day >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY source, target
ORDER BY total_volume_usd DESC
LIMIT 20
```

**Configuration notes:**
- Filter to recent 7 or 30 days using `day` column to keep the diagram readable
- Node labels are self-documenting: `aave_v3:borrow:USDC → morpho_blue:supply:USDC` reads as "borrow USDC on Aave, supply USDC on Morpho"
- The `atomic_flows` vs `cross_tx_flows` breakdown reveals how much activity is same-transaction (flash loan style) vs cross-transaction (manual or bot-driven)

**What to look for:**
- Dominant flow directions reveal market structure: e.g., "most capital flows from Aave borrows to Morpho supplies" indicates Morpho offers better supply rates
- Asymmetric flows (A→B much larger than B→A) suggest rate arbitrage
- `avg_time_delta_seconds` near zero on dominant edges = flash-loan-style atomic loops

---

## 5.5 Visualization — Table: Protocol pair transition matrix

**Source query:** `query_6708650`
**Purpose:** Tabular breakdown of flow volumes by protocol pair.

```sql
SELECT
    SPLIT_PART(source, ':', 1) AS source_protocol,
    SPLIT_PART(target, ':', 1) AS dest_protocol,
    SUM(value) AS volume_usd,
    SUM(flow_count) AS flows,
    SUM(entity_count) AS entities,
    ROUND(AVG(avg_time_delta_seconds), 0) AS avg_seconds
FROM query_6708650
WHERE day >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY 1, 2
ORDER BY volume_usd DESC
```

Display as a pivot table or heatmap with source protocols as rows, destination protocols as columns, and volume as cell values.

---

## 5.6 Visualization — Stacked bar: Loop volume by collateral category

**Source query:** `query_6708668` (lending_loop_collateral_profile)
**Purpose:** Show what backs the stablecoin borrows that feed into loops.

| Property | Value |
|----------|-------|
| X-axis | `block_date` |
| Y-axis | `SUM(flow_amount_usd)` |
| Series / color | `collateral_category` (5 categories) |
| Chart type | Stacked bar |

**Data preparation:**

```sql
SELECT
    block_date,
    collateral_category,
    SUM(flow_amount_usd) AS loop_volume_usd,
    COUNT(DISTINCT entity_address) AS unique_entities,
    COUNT(*) AS flow_count
FROM query_6708668
GROUP BY block_date, collateral_category
ORDER BY block_date, collateral_category
```

**Configuration notes:**
- Color palette by category: orange for `btc`, blue for `eth`, purple for `eth_lst`, gray for `mixed`, light gray for `unknown`
- The `unknown` category means the entity had no tracked collateral deposits on that date — this is common for entities who deposited collateral before the query's lookback window
- Ordering: `btc` → `eth` → `eth_lst` → `mixed` → `unknown` (bottom to top)

**What to look for:**
- Growing `eth_lst` share indicates LST-backed leverage is increasing (wstETH/weETH as collateral for stablecoin loops)
- `btc` share tracks WBTC usage as DeFi collateral — a proxy for BTC-backed leverage appetite on Ethereum
- High `unknown` ratio may indicate the collateral lookback window needs extension

---

## 5.7 Visualization — Scatter plot: Implied leverage by collateral type

**Source query:** `query_6708668`
**Purpose:** Show the leverage profile of loop participants by collateral backing.

| Property | Value |
|----------|-------|
| X-axis | `collateral_amount_usd` |
| Y-axis | `implied_leverage` |
| Color | `collateral_category` |
| Size | `flow_amount_usd` |
| Chart type | Scatter |

**Data preparation:**

```sql
SELECT
    entity_address,
    block_date,
    collateral_category,
    collateral_amount_usd,
    flow_amount_usd,
    implied_leverage
FROM query_6708668
WHERE implied_leverage IS NOT NULL
  AND implied_leverage < 100  -- filter outliers
  AND block_date >= CURRENT_DATE - INTERVAL '7' DAY
```

**Configuration notes:**
- `implied_leverage` = flow_amount_usd / collateral_amount_usd. Values > 1 mean the flow exceeds the entity's collateral on that protocol
- Values > 10 are likely outliers (partial collateral visibility) — consider capping the Y-axis
- NULL leverage means zero or unknown collateral — excluded from this chart

**What to look for:**
- Clusters of high-leverage BTC-backed loops = potential systemic risk if WBTC depegs
- ETH LST-backed loops tend to be higher leverage (LSTs are considered "safer" collateral)

---

## 5.8 Visualization — Pie chart: Collateral category breakdown

**Source query:** `query_6708668`
**Purpose:** Aggregate collateral composition across all loop activity.

```sql
SELECT
    collateral_category,
    SUM(flow_amount_usd) AS total_loop_volume_usd,
    COUNT(DISTINCT entity_address) AS unique_entities
FROM query_6708668
WHERE block_date >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY collateral_category
ORDER BY total_loop_volume_usd DESC
```

Use a donut chart. Show both volume (USD) and entity count as tooltip metrics. This gives the dashboard's highest-level answer to "what backs DeFi lending loops?"

---

## 5.9 Visualization — Table: Entity balance sheet (drill-down)

**Source query:** `query_6708623` (lending_entity_balance_sheet)
**Purpose:** Per-entity position explorer, enabling deep-dives into individual addresses.

```sql
SELECT
    entity_address,
    protocol,
    asset_symbol,
    block_date,
    cumulative_collateral,
    cumulative_debt,
    net_position
FROM query_6708623
WHERE block_date = (SELECT MAX(block_date) FROM query_6708623)
ORDER BY ABS(cumulative_debt) DESC
LIMIT 100
```

**Configuration notes:**
- Display as a searchable table with entity_address as the primary key
- Format entity_address as a clickable link to Etherscan (`https://etherscan.io/address/{address}`)
- Highlight rows where `net_position < 0` (underwater) in red
- Consider adding a `{{entity_filter}}` Dune parameter (type: text) to let users paste an address and filter the table

**What to look for:**
- Top debtors by protocol reveal concentration risk
- Entities with positions across multiple protocols are likely loop participants

---

## 5.10 Visualization — Timeline: Entity loop storyboard (drill-down)

**Source query:** `query_6708643` (lending_entity_loop_storyboard)
**Purpose:** Step-by-step replay of how an entity constructs a leverage loop.

| Property | Value |
|----------|-------|
| X-axis | `event_sequence` (or `block_time` for time-accurate spacing) |
| Y-axis (left) | `running_collateral_usd` and `running_debt_usd` (dual lines) |
| Y-axis (right) | `leverage_ratio` |
| Annotations | `action_type` + `protocol` at each event |
| Chart type | Annotated line chart |

**Data preparation:**

This query produces many rows. Filter to a specific entity for the drill-down view:

```sql
SELECT *
FROM query_6708643
WHERE entity_address = {{entity_address}}
ORDER BY event_sequence
```

Configure `{{entity_address}}` as a text parameter. Users can click through from the balance sheet table (5.9) or top loopers list.

**Configuration notes:**
- Plot `running_collateral_usd` (green line) and `running_debt_usd` (red line) on the left axis
- The gap between them is `net_equity_usd` — when lines cross, the entity goes underwater
- `leverage_ratio` on the right axis shows the amplification factor
- Color-code data points by `position_health`: green (healthy), yellow (moderate), red (risky/underwater)
- Each point can be annotated with `action_type` + `protocol` (e.g., "borrow @ aave_v3", "supply @ morpho_blue")

**What to look for:**
- The classic loop pattern: supply → borrow → supply → borrow with monotonically increasing leverage
- `position_health` transitions from "healthy" to "risky" reveal tipping points
- Flash unwinds: rapid sequence of repay+withdraw across protocols = loop closure

---

## 6. Parameters

The nested queries in this suite do not use Dune `{{parameters}}` — they are fully self-contained with hardcoded upstream query references. Date filtering happens at the base query level via incremental processing.

**For dashboard-level filtering**, wrap the query references in a date-filtered CTE:

```sql
WITH filtered AS (
    SELECT *
    FROM query_6708794  -- lending_loop_metrics_daily
    WHERE day >= DATE '{{start_date}}'
      AND day <= DATE '{{end_date}}'
)
SELECT * FROM filtered
ORDER BY day DESC
```

**Recommended dashboard parameters:**

| Parameter | Type | Default | Widget |
|-----------|------|---------|--------|
| `{{start_date}}` | DATE | 30 days ago | Date picker |
| `{{end_date}}` | DATE | Today | Date picker |
| `{{entity_address}}` | TEXT | (none) | Text input |

Wire `start_date` and `end_date` to all panels. Wire `entity_address` only to Section D (entity drill-down) panels.

**Date range constraint:** The base queries use `DATE '2024-01-01'` as the initial fallback. Data before this date is unavailable. Set the dashboard date picker minimum to **2024-01-01**.

---

## 7. Known data characteristics and caveats

1. **Entity resolution is address-level.** `entity_address` is `COALESCE(on_behalf_of, user_address)` — it does not cluster smart contract wallets, multisigs, or addresses controlled by the same entity. Two addresses controlled by the same entity will appear as separate loopers.

2. **Flow stitching uses a 2-minute window.** Cross-transaction flows are matched if the same entity borrows on P1 and supplies on P2 within 2 minutes. This catches most bot-driven loops but may miss slow manual loops. Same-transaction flows (atomic) are always captured.

3. **Loop detection uses a 1-hour window.** Chain continuation requires consecutive flows within 1 hour. Loops that span longer gaps will be split into separate chains.

4. **Gross credit is not net credit.** `gross_borrowed_usd` sums all borrows across hops. A $1M initial borrow that loops 3 times produces $3M gross credit. This intentionally measures amplification, not unique capital.

5. **Collateral matching is date-exact.** The collateral profile joins flow dates to collateral deposit dates. If an entity deposited collateral on Day 1 and looped on Day 5, the collateral won't match unless there was also collateral activity on Day 5. This produces `unknown` collateral categories more often than ideal.

6. **Incremental base queries.** Both `lending_action_ledger_unified` and `lending_flow_stitching` use `previous.query.result()` with 1-day lookback. On first run, they process from `DATE '2024-01-01'` forward. Subsequent runs are incremental. See [CLAUDE.md § Development Pipeline](../CLAUDE.md#step-3-validation-dry-run--smoke-test) for details on incremental processing.

7. **Stablecoin scope.** Only USDC, USDT, DAI, and FRAX are tracked. Other stablecoins (GHO, crvUSD, LUSD, etc.) are excluded. Extending coverage requires modifying the stablecoin CTE in `lending_action_ledger_unified.sql`.

8. **Morpho Aave V2 vs Morpho Blue.** The unified ledger covers **Morpho Blue** (current). The standalone `lending_action_ledger_morpho` covers **Morpho Aave V2** (legacy). These are different protocols with different contract architectures. See [lending_protocol_schemas.md](./lending_protocol_schemas.md) for the distinction.

---

## 8. Performance considerations

Follow the efficiency guidelines in [CLAUDE.md § Writing Efficient Queries](../CLAUDE.md#writing-efficient-queries). Key points for this dashboard:

- **Base queries are materialized.** Downstream nested queries read from materialized results, not from raw tables. This means adding new dashboard panels that reference `query_6687961`, `query_6690272`, or `query_6707791` incurs **no additional raw table scan cost**.

- **Nested queries are not cached.** Each nested query re-executes its full logic on every dashboard load. Keep nested queries lightweight (aggregations only, no heavy joins or window functions beyond what's already there).

- **Avoid SELECT * on base queries.** When wrapping query results for dashboard panels, select only the columns you need. The base queries have wide schemas (15+ columns). See [CLAUDE.md § Select Only Needed Columns](../CLAUDE.md#3-select-only-needed-columns).

- **Date-filter early.** When adding wrapper queries for dashboard panels, push date filters into the CTE reading from the upstream query, not in an outer WHERE clause. This enables Dune's query optimizer to prune data early.

---

## 9. Extending the dashboard

### Low-cost additions (new nested queries on existing base queries)

These require only a new lightweight SQL query referencing an existing materialized base — no additional raw table scans:

| Idea | Source query | Approach |
|------|-------------|----------|
| Top 10 loopers by gross credit | `query_6702204` | GROUP BY entity_address, ORDER BY SUM(gross_borrowed_usd) DESC LIMIT 10 |
| Loop duration analysis | `query_6702204` | Histogram of time between first and last hop per loop |
| Protocol market share | `query_6687961` | GROUP BY protocol, action_type, date — bar chart of action counts |
| Liquidation tracker | `query_6687961` | Filter action_type = 'liquidation', track daily liquidation volume |
| Flow velocity distribution | `query_6690272` | Histogram of `time_delta_seconds` to characterize bot vs human speed |

### Higher-cost additions (require new base queries or raw table access)

| Idea | Requirement |
|------|-------------|
| Health factor monitoring | Requires reading Aave V3 on-chain health factor (not in event tables) |
| Interest rate tracking | Requires rate oracle data or ReserveDataUpdated events |
| Additional stablecoins (GHO, crvUSD) | Modify stablecoin CTE in unified ledger + re-materialize |
| L2 protocol coverage (Aave on Arbitrum/Optimism) | New base queries per chain + cross-chain flow stitching |
| Entity labels | Requires `labels.addresses` or custom label mapping table |

---

## 10. Smoke tests

Every query has an associated smoke test in `tests/`. These validate the underlying Dune table schemas and data availability without depending on materialized query results. Run them before modifying any query:

| Query | Smoke test |
|-------|------------|
| lending_action_ledger_unified | `tests/lending_action_ledger_unified_smoke.sql` |
| lending_action_ledger_aave_v3 | `tests/lending_action_ledger_aave_v3_smoke.sql` |
| lending_action_ledger_morpho | `tests/lending_action_ledger_morpho_smoke.sql` |
| lending_flow_stitching | `tests/lending_flow_stitching_smoke.sql` |
| lending_collateral_ledger | `tests/lending_collateral_ledger_smoke.sql` |
| lending_loop_detection | `tests/lending_loop_detection_smoke.sql` |
| lending_loop_metrics_daily | `tests/lending_loop_metrics_daily_smoke.sql` |
| lending_entity_balance_sheet | `tests/lending_entity_balance_sheet_smoke.sql` |
| lending_sankey_flows | `tests/lending_sankey_flows_smoke.sql` |
| lending_entity_loop_storyboard | `tests/lending_entity_loop_storyboard_smoke.sql` |
| lending_loop_collateral_profile | `tests/lending_loop_collateral_profile_smoke.sql` |

Run programmatically via:

```bash
python -m scripts.smoke_runner --all --architecture v2
```

See [CLAUDE.md § Programmatic Smoke Testing](../CLAUDE.md#programmatic-smoke-testing-via-dune-api) for setup instructions.
