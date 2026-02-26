# Alpen SIGNAL Report - BTC Collateral Borrowing Intent (Morpho, ETH + Base)

- As of: 2026-02-26T11:02:09.982749+00:00
- Cohort window: last 30 days
- Flow windows: LS1=1h, LS24=24h, DR7/OR7=7d
- Off-ramp method: outflow extraction skipped (source query timeout); OR7 is conservative/underestimated

## KPI Table (MEASURED)

| chain | venue | borrowers_90d | borrow_usd_90d | ls1 | ls24 | dr7 | or7 | top25_share | repeat_rate | n_borrows | ls24_event_rate | ls24_event_ci95 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ethereum | morpho_blue | 340 | $339,550,671 | 0.5% | 0.7% | 5.9% | 0.0% | 93.1% | 37.1% | 1451 | 6.2% | [5.1%, 7.6%] |

## Sample Adequacy (MEASURED)

Heuristic: sample is directionally reliable when `n_borrows >= 100` and LS24 event-rate CI width <= 20pp.
- ethereum morpho_blue: n=1451, ci_width=2.5pp -> sufficient

## Verdict

Verdict: Loops are minor; borrowed stables are mixed; wedge = Credit Rail.

## Truth Labels

- VERIFIED: Dune raw onchain tables (Morpho Blue, tokens.transfers, dex.trades).
- MEASURED: KPI outputs in `signal_metrics.csv` and audit rows in `borrow_flows_sample.csv`.
- INFERRED: off-ramp proxy uses recipient EOA dominance; no external CEX labels.
- HYPOTHESIS/UNSURE: OR7 is a proxy, not a direct CEX attribution metric.