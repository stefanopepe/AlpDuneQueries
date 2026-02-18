# Query Constants Ledger

This document tracks hardcoded thresholds, intervals, fallback values, and
allowlists used in lending queries so they can be periodically reviewed.

## Lending Constants

| Constant | Value | File Reference | Rationale | Risk If Wrong | Candidate Alternatives |
|---|---|---|---|---|---|
| Incremental lookback window | `INTERVAL '1' DAY` | `queries/ethereum/lending/lending_action_ledger_unified.sql:73` | Recompute the last day to catch late data and reorg-adjacent updates. | Too short can miss late-arriving events; too long increases cost. | `12h`, `2d`, per-protocol lag windows. |
| Incremental lookback window (Base) | `INTERVAL '1' DAY` | `queries/base/lending/lending_action_ledger_unified.sql:72` | Same incremental safety behavior on Base. | Same as above. | Same as above. |
| Initial fallback start date | `DATE '2024-01-01'` | `queries/ethereum/lending/lending_flow_stitching.sql:64` | Bounds first run cost and historical backfill scope. | Too recent truncates history; too old increases cost. | Protocol launch date per chain; configurable parameter. |
| Initial fallback start date (Base) | `DATE '2024-01-01'` | `queries/base/lending/lending_collateral_ledger.sql:75` | Keep first-run behavior aligned with framework baseline. | Same as above. | Chain/protocol-specific launch checkpoints. |
| Cross-tx stitch window | `INTERVAL '2' MINUTE` | `queries/ethereum/lending/lending_flow_stitching.sql:202` | Capture near-immediate borrow→supply sequencing without overmatching. | Too wide links unrelated flows; too narrow misses valid loops. | `1m`, `5m`, dynamic by chain block time. |
| Cross-tx stitch window (Base) | `INTERVAL '2' MINUTE` | `queries/base/lending/lending_flow_stitching.sql:202` | Same loop-stitching semantics on Base. | Same as above. | Same as above. |
| Flow speed bands | `<=15s`, `<=60s`, else delayed | `queries/ethereum/lending/lending_flow_stitching.sql:248` | Useful dashboard segmentation of flow latency. | Band boundaries can distort behavior classification. | Quantile-based bands from observed distribution. |
| Flow speed bands (Base) | `<=15s`, `<=60s`, else delayed | `queries/base/lending/lending_flow_stitching.sql:248` | Maintain comparability with Ethereum dashboard semantics. | Same as above. | Chain-specific quantiles. |
| Loop chain continuation window | `INTERVAL '1' HOUR` | `queries/ethereum/lending/lending_loop_detection.sql:80` | Defines max time gap for adjacent hops in same loop chain. | Too wide merges separate strategies; too narrow breaks true loops. | `30m`, `2h`, per-entity adaptive windows. |
| Loop chain continuation window (Base) | `INTERVAL '1' HOUR` | `queries/base/lending/lending_loop_detection.sql:80` | Same loop-chain rule on Base. | Same as above. | Same as above. |
| Loop status depth thresholds | `hop_count >= 3`, `hop_count = 2` | `queries/ethereum/lending/lending_loop_detection.sql:138` | Simple categorical storytelling for dashboards. | Coarse bins may hide risk granularity. | Additional bins (`4+`, `6+`) or percentile labels. |
| Loop status depth thresholds (Base) | `hop_count >= 3`, `hop_count = 2` | `queries/base/lending/lending_loop_detection.sql:138` | Keep depth categories comparable across chains. | Same as above. | Same as above. |
| Position health thresholds | `collateral/debt > 2`, `> 1.5` | `queries/ethereum/lending/lending_entity_loop_storyboard.sql:153` | Quick heuristic risk bands for storyboard UI. | Not protocol-aware; may misclassify true liquidation risk. | Use protocol liquidation thresholds per asset pair. |
| Position health thresholds (Base) | `collateral/debt > 2`, `> 1.5` | `queries/base/lending/lending_entity_loop_storyboard.sql:153` | Reuse storyboard semantics across chains. | Same as above. | Protocol-specific LTV/LT-based health score. |
| Ethereum stablecoin allowlist | `USDC`, `USDT`, `DAI`, `FRAX` (hardcoded addresses) | `queries/ethereum/lending/lending_action_ledger_unified.sql:82` | Explicit scope control and deterministic joins. | Missing assets undercounts activity; stale addresses break coverage. | Metadata-driven symbol allowlist + explicit versioning. |
| Base stablecoin allowlist | `USDC`, `USDbC`, `USDT`, `DAI` (hardcoded addresses) | `queries/base/lending/lending_action_ledger_unified.sql:81` | Base-specific stablecoin scope with deterministic filtering. | Same as above. | Add `FRAX`/others after liquidity and table validation. |
| Base collateral core allowlist | `cbBTC`, `WBTC`, `WETH` (hardcoded addresses) | `queries/base/lending/lending_collateral_ledger.sql:83` | Enforces explicit baseline collateral taxonomy on Base. | Missing/incorrect token addresses misclassify collateral. | Extend list with validated collateral set from protocol configs. |
| Base `eth_lst` bucket resolution | `symbol IN ('wstETH','weETH','rETH','cbETH')` | `queries/base/lending/lending_collateral_ledger.sql:93` | Capture LST family while allowing token address discovery on Base. | Symbol collisions or metadata drift can include wrong contracts. | Curated address allowlist with periodic verification. |

## Review Cadence

- Revisit this ledger whenever a lending query changes constants/allowlists.
- Perform a quarterly review for stale token address lists and interval quality.
