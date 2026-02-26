from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from src.config import SignalConfig
from src.dune_client import execute_sql
from src.extract.sql_loader import load_sql
from src.report.generate import write_report
from src.transform.signal import classify_borrows


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    cfg = SignalConfig.from_env()
    root = Path(__file__).resolve().parents[1]
    out_dir = root / "outputs"

    print("[1/4] Executing Morpho borrow cohort extract on Dune...")
    borrows_sql = load_sql("signal_borrow_events.sql", start_ts=cfg.start_time.strftime("%Y-%m-%d %H:%M:%S"))
    borrow_rows = execute_sql(borrows_sql, performance=cfg.dune_performance)

    print("[2/4] Executing borrow outflow extract on Dune...")
    outflows_sql = load_sql("signal_borrow_outflows.sql", start_ts=cfg.start_time.strftime("%Y-%m-%d %H:%M:%S"))
    outflow_rows: list[dict[str, Any]] = []
    outflow_note = "onchain off-ramp proxy (EOA-heavy outflows, no external labels)"
    try:
        outflow_rows = execute_sql(outflows_sql, performance=cfg.dune_performance)
    except Exception as e:
        print(f"[warn] outflow extraction skipped: {e}")
        outflow_note = "outflow extraction skipped (query timeout); OR7 is conservative/underestimated"

    print("[3/4] Computing SIGNAL KPIs...")
    outputs = classify_borrows(
        borrows=borrow_rows,
        outflows=outflow_rows,
        bridge_addresses=cfg.bridge_addresses,
        sample_size=cfg.sample_size,
    )

    print("[4/4] Writing outputs...")
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / "signal_metrics.csv", outputs.metrics)
    _write_csv(out_dir / "borrow_flows_sample.csv", outputs.audit_sample)

    write_report(
        out_dir=out_dir,
        as_of=cfg.as_of,
        lookback_days=cfg.lookback_days,
        metrics=outputs.metrics,
        verdict=outputs.verdict,
        off_ramp_method=outflow_note,
    )

    print("Done")
    print(outputs.verdict)


if __name__ == "__main__":
    main()
