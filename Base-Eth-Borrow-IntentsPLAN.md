# Handover + Independent Dune Query Plan: BTC-Collateral Borrowing Intent (All Protocols, API-Only)

## Summary
We will produce a handover document first, then a suite of independently runnable Dune SQL queries (no nested `query_x` dependencies) that let an external agent pull report-ready BTC-collateral borrowing-intent datasets via Dune API.  
Scope is `All lending protocols` and delivery is `API-only` (per your selections), with Ethereum + Base included.

## Current Branch Findings To Carry Into Handover
1. The branch adds a Morpho-focused SIGNAL pipeline and raw 30d ingest bundle, but the released `outputs/signal_metrics.csv` is inconsistent with newer raw extracts (report shows Ethereum-only while raw ingest includes large Base volume).
2. Existing intent logic is partial:
`bridge_7d` is hardcoded `FALSE`, off-ramp is a weak EOA proxy, and outflow extraction can timeout (explicitly noted as underestimated OR7).
3. Current SQL is Morpho-only; it does not yet cover Aave/Compound intents despite broader lending framework existing in repo.
4. New SIGNAL SQL is not registered in query registries and has no smoke tests, so reproducibility on Dune-side assets is not yet operationalized.

## Deliverable 1 (First): Handover Document
Create one implementation-spec document for the external agent at:
`/Users/stefanopepe/Library/Mobile Documents/com~apple~CloudDocs/alpenproduct/AlpDuneQueriesCodex/docs/handover_btc_collateral_borrowing_intent.md`

Required sections:
1. Mission and report objective.
2. What exists now (branch commits, SQL files, generated artifacts, known caveats).
3. Non-negotiable definitions:
`borrow intent`, `BTC-collateral borrower`, `loop`, `defi-retained`, `off-ramp proxy`, `coverage`.
4. Source-of-truth tables by protocol and chain.
5. Independent query contract:
standard parameters, deterministic output schemas, API execution rules.
6. Known shortcomings and challenge checklist (items the external agent must validate or falsify).
7. Acceptance criteria for publishing report findings.
8. Runbook for API extraction, paging, retries, and provenance manifests.

## Deliverable 2: Independent Query Suite (No Inter-Query Dependencies)
All queries accept:
`{{start_ts}}`, `{{end_ts}}`, `{{chains}}`, `{{protocols}}`, `{{stable_symbols}}`, `{{btc_symbols}}`.

1. `btc_collateral_market_map_independent`
Returns protocol/market-level loan token + collateral token decoding for Morpho/Aave/Compound, with normalized asset metadata.
2. `stable_borrow_events_independent`
Raw borrow events across selected protocols/chains with borrower, tx hash, token, native amount, USD valuation.
3. `btc_collateral_positions_independent`
Borrower-level BTC collateral supply/position timeline (WBTC/cbBTC/tBTC and wrapped variants approved in config).
4. `btc_backed_borrow_cohort_independent`
Cohort of borrow events that are BTC-backed at borrow time (explicit temporal rule included in SQL).
5. `borrow_intent_signals_independent`
Per-borrow boolean intent signals in fixed windows (`1h`, `24h`, `7d`) for looping, DeFi reuse, bridging, off-ramp proxy.
6. `borrow_intent_classification_independent`
Maps signals to mutually exclusive intent bucket with reason codes.
7. `borrow_intent_metrics_daily_independent`
Daily aggregates: borrower counts, borrow USD, LS1/LS24/DR7/OR7 rates, concentration, repeat behavior.
8. `borrow_intent_coverage_sanity_independent`
Coverage versus full stable-borrow universe by chain/protocol, plus row-count and value-share diagnostics.
9. `borrow_intent_statistical_significance_independent`
Event-rate confidence intervals and sample adequacy diagnostics for publication gating.

## Public Interfaces / Schemas (Agent Contract)
1. Every query returns stable column names and primitive types only (`VARCHAR`, `DOUBLE`, `BIGINT`, `BOOLEAN`, `TIMESTAMP`, `DATE`).
2. `borrow_id` canonical key:
`concat(chain, ':', protocol, ':', lower(to_hex(borrow_tx_hash)), ':', cast(evt_index as varchar))`.
3. Time columns:
`borrow_time_utc`, `event_time_utc`, `day_utc`.
4. Classification columns:
`intent_bucket`, `intent_reason`, `signal_loop_1h`, `signal_loop_24h`, `signal_defi_7d`, `signal_bridge_7d`, `signal_offramp_7d`.
5. Provenance columns:
`query_version`, `as_of_utc`, `parameter_hash`.

## Test Cases and Validation Scenarios
1. Schema tests:
required columns exist, types stable, no renamed fields.
2. Determinism tests:
same params produce same row count and metric totals within tolerance.
3. Cross-check tests:
`cohort_borrow_events <= all_borrow_events` and same for borrowers/volume.
4. Temporal logic tests:
no post-borrow collateral counted as pre-borrow eligibility.
5. Chain/protocol coverage tests:
each selected chain/protocol appears or emits explicit zero rows.
6. Statistical gating tests:
minimum sample thresholds and CI width checks for report release.
7. Failure-path tests:
API timeout/retry behavior and partial-run manifest correctness.

## Implementation Sequence
1. Draft and review handover doc with shortcomings checklist first.
2. Define shared SQL parameter/template conventions.
3. Implement independent queries in `queries/intent/` (or agreed directory), one by one.
4. Add registry entries and smoke tests for each new independent query.
5. Validate with Dune API executions and produce signed manifest of execution IDs.
6. Finalize report-ready extraction runbook for the external agent.

## Assumptions and Defaults
1. Default chains:
`ethereum`, `base`.
2. Default protocols:
`morpho_blue`, `aave_v3`, `compound_v3`, `compound_v2` where tables are validated; unsupported protocol/chain combos must return explicit empty outputs, not silent omission.
3. Default stables:
`USDC`, `USDT`, `DAI`.
4. Default BTC collateral set:
`WBTC`, `cbBTC`, `tBTC` plus approved wrapped equivalents listed in handover constants.
5. API-only consumption:
external agent uses Dune Execution API and paginated `/results`; CSV downloads are out of scope for the first release.
6. Publication gate:
no report claim is released unless coverage sanity and significance queries pass thresholds defined in handover doc.
