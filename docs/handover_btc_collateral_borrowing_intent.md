# BTC-Collateral Borrowing Intent - External Agent Handover (Phase 1)

## 1. Mission and report objective
Build a source-of-truth, API-consumable query suite that quantifies BTC-collateralized stablecoin borrowing intent on Ethereum and Base. The reporting objective is to publish defensible findings on whether borrowed stablecoins are looped back into BTC collateral, retained in DeFi, bridged, or off-ramped.

Phase-1 scope is hard-constrained to:
- Chains: `ethereum`, `base`
- Protocols: `morpho_blue`, `aave_v3`
- BTC collateral: address-registry selection (`WBTC`, `cbBTC`, `tBTC`), not symbol-only filters
- Debt tokens: explicit stable address registries plus discovered market loan-token sets

Phase-2 expansion to additional protocols is blocked until all acceptance gates pass.

## 2. What exists now
Current branch context:
- A Morpho-focused SIGNAL pipeline exists (`sql/signal_*`, `src/*`) and generated artifacts in `outputs/`.
- Existing branch caveats identified before this handover:
  - prior signal logic included partial placeholders (for example bridge/off-ramp shortcuts)
  - report/raw inconsistencies were observed in earlier output snapshots
  - timeout risk exists in broad outflow extraction joins
- Existing lending framework in the repository already covers Aave/Morpho event decoding per chain, but was not previously packaged as a standalone source-of-truth intent suite.

This handover introduces a dedicated phase-1 independent suite under `queries/intent/` plus explicit smoke gates under `tests/intent/`.

## 3. Non-negotiable definitions
- `borrow intent`: inferred post-borrow behavioral direction for a stable borrow event.
- `BTC-collateral borrower`: borrower is BTC-backed at the exact borrow timestamp.
- `BTC-backed at borrow time`: collateral condition is evaluated as-of borrow timestamp, not "historically supplied at any prior time".
- `loop`: stable borrow followed by stable->BTC conversion and BTC collateral add in bounded windows.
- `defi-retained`: borrowed stables routed to onchain contract destinations without bridge/off-ramp classification.
- `bridge`: borrowed stables transferred to bridge destinations (registry/label identified).
- `off-ramp proxy`: onchain proxy for fiat-facing behavior using CEX-labeled or EOA-dominant transfer patterns.
- `coverage`: cohort universe versus full stable-borrow baseline, with explicit chain+protocol parity rows including zeroes.

## 4. Source-of-truth tables by protocol and chain
Morpho Blue:
- `morpho_blue_ethereum.morphoblue_evt_createmarket`
- `morpho_blue_ethereum.morphoblue_evt_borrow`
- `morpho_blue_ethereum.morphoblue_evt_supplycollateral`
- `morpho_blue_ethereum.morphoblue_evt_withdrawcollateral`
- `morpho_blue_base.morphoblue_evt_createmarket`
- `morpho_blue_base.morphoblue_evt_borrow`
- `morpho_blue_base.morphoblue_evt_supplycollateral`
- `morpho_blue_base.morphoblue_evt_withdrawcollateral`

Aave V3:
- `aave_v3_ethereum.pool_evt_borrow`
- `aave_v3_ethereum.pool_evt_supply`
- `aave_v3_ethereum.pool_evt_withdraw`
- `aave_v3_ethereum.pool_evt_liquidationcall`
- `aave_v3_base.pool_evt_borrow`
- `aave_v3_base.pool_evt_supply`
- `aave_v3_base.pool_evt_withdraw`
- `aave_v3_base.pool_evt_liquidationcall`

Cross-cutting enrichment:
- `tokens.erc20`
- `prices.usd`
- `tokens.transfers`
- `dex.trades`
- `ethereum.creation_traces`, `base.creation_traces`
- `labels.addresses` (when available for bridge/CEX hints)

## 5. Independent query contract
All phase-1 queries are independently runnable and accept:
- `{{start_ts}}`, `{{end_ts}}`, `{{chains}}`, `{{protocols}}`, `{{stable_symbols}}`, `{{btc_symbols}}`

Contract requirements:
- No `query_x` dependency chain inside the core 9 suite queries.
- Canonical borrow identifier:
  - `borrow_id = concat(chain, ':', protocol, ':', lower(to_hex(borrow_tx_hash)), ':', cast(evt_index as varchar))`
- Required time columns where applicable:
  - `borrow_time_utc`, `event_time_utc`, `day_utc`
- Required classification/signal columns in signal/classification/metric layers:
  - `intent_bucket`, `intent_reason`
  - `signal_loop_1h`, `signal_loop_24h`, `signal_defi_7d`, `signal_bridge_7d`, `signal_offramp_7d`
- Required provenance in all suite outputs:
  - `query_version`, `as_of_utc`, `parameter_hash`

Temporal correctness rule:
- Cohort inclusion must be computed at borrow-time state, never by "borrower once supplied BTC" heuristics.

## 6. Known shortcomings and challenge checklist
External agent must challenge and either confirm or falsify:
1. Token registry completeness (BTC wrappers and chain-specific stable variants).
2. Borrow-time collateral state correctness under long-held collateral (pre-window positions).
3. Bridge/off-ramp proxy quality (labels and registry coverage).
4. Outflow boundedness tradeoff (90% top-notional coverage vs. missed tail recipients).
5. Pricing gaps at event minute joins (`prices.usd`) and fallback behavior.
6. Chain/protocol parity guarantees with explicit zero rows.
7. Determinism under repeated API runs with same parameters.

## 7. Acceptance criteria for publishing report findings
Publishing is blocked unless all gates pass:
1. Schema contract gate
2. Determinism gate
3. Cross-check gate (cohort cannot exceed baseline)
4. Temporal logic gate (no post-borrow collateral leakage into cohort)
5. Chain+protocol parity gate (explicit rows for all requested combos)
6. Statistical gating (minimum sample and confidence interval constraints)
7. Failure-path gate (timeout/partial-run semantics are explicit and controlled)

If any gate fails, findings are draft-only and cannot be released as source-of-truth.

## 8. API extraction runbook
1. Execute suite with identical parameter payload for all 9 queries.
2. Use Dune execution API (`/api/v1/sql/execute` + `/execution/{id}/status` + paginated `/results`).
3. Poll until terminal status; capture terminal state and execution ID per query.
4. Page results with `limit/offset`, preserving row order where defined.
5. Retry strategy:
  - exponential backoff for rate-limits/transient API failures
  - bounded retries with terminal failure logging
6. Provenance manifest requirements:
  - query name
  - query text hash
  - execution_id
  - parameter payload
  - row count
  - terminal state
  - extraction timestamp
7. Failure handling:
  - if a required core query fails, stop publication pipeline
  - if non-core enrichment fails, mark outputs partial and block release gates
8. Validation run:
  - execute smoke gate queries in `tests/intent/`
  - proceed only when all gate rows return `PASS`

