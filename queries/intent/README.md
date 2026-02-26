# Intent Query Suite (Phase 1)

Independent Dune SQL suite for BTC-collateral borrowing intent.

## Scope
- Chains: `ethereum`, `base`
- Protocols: `morpho_blue`, `aave_v3`
- BTC collateral set: address-registry first (`WBTC`, `cbBTC`, `tBTC`)
- Debt tokens: stable address registries plus discovered market loan-token sets

## Parameters
Each query accepts:
- `{{start_ts}}`
- `{{end_ts}}`
- `{{chains}}` (example: `ethereum,base`)
- `{{protocols}}` (example: `morpho_blue,aave_v3`)
- `{{stable_symbols}}` (example: `usdc,usdt,dai`)
- `{{btc_symbols}}` (example: `wbtc,cbbtc,tbtc`)

## Execution Order
1. `btc_collateral_market_map_independent.sql`
2. `stable_borrow_events_independent.sql`
3. `btc_collateral_positions_independent.sql`
4. `btc_backed_borrow_cohort_independent.sql`
5. `borrow_intent_signals_independent.sql`
6. `borrow_intent_classification_independent.sql`
7. `borrow_intent_metrics_daily_independent.sql`
8. `borrow_intent_coverage_sanity_independent.sql`
9. `borrow_intent_statistical_significance_independent.sql`

## API-Only Extraction Steps
1. Submit query SQL with parameter payload to Dune `/api/v1/sql/execute`.
2. Poll `/api/v1/execution/{execution_id}/status` until terminal state.
3. Fetch paginated rows from `/api/v1/execution/{execution_id}/results?limit=...&offset=...`.
4. Persist per-query provenance:
   - `query_name`, `execution_id`, `state`, `row_count`, `parameters`, extraction timestamp.
5. Run smoke gates in `tests/intent/` and block publication on any `FAIL`.

## Smoke Tests
- `tests/intent/intent_schema_contract_smoke.sql`
- `tests/intent/intent_determinism_smoke.sql`
- `tests/intent/intent_cross_checks_smoke.sql`
- `tests/intent/intent_temporal_logic_smoke.sql`
- `tests/intent/intent_chain_protocol_coverage_smoke.sql`
- `tests/intent/intent_statistical_gating_smoke.sql`
- `tests/intent/intent_failure_path_smoke.sql`

Expected smoke output format:
- `gate_name`
- `status` (`PASS` or `FAIL`)
- `detail`
- `observed_value`
- `threshold_value`

## Zero-Data Behavior
If a requested chain/protocol has no data, coverage/parity outputs must still include an explicit zero row. This is required for deterministic downstream ingestion and report templates.

## Registry Merge Behavior
A dedicated registry file is added at `queries/registry.intent.json`.
`registry_manager` and `smoke_runner` are updated to merge this registry with existing Bitcoin/Ethereum/Base registries.
