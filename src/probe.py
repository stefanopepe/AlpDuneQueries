from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

from src.dune_client import execute_sql
from src.extract.sql_loader import load_sql


def wilson(successes: int, n: int) -> tuple[float, float]:
    if n <= 0:
        return (0.0, 0.0)
    z = 1.96
    p = successes / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    m = (z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)) / d
    return max(0.0, c - m), min(1.0, c + m)


def main() -> None:
    lookback = int(__import__("os").getenv("LOOKBACK_DAYS", "14"))
    start = (datetime.now(UTC) - timedelta(days=lookback)).strftime("%Y-%m-%d %H:%M:%S")
    sql = load_sql("signal_loop_significance_probe.sql", start_ts=start)
    rows = execute_sql(sql)

    print(f"As of={datetime.now(UTC).isoformat()} lookback_days={lookback}")
    print("chain,n_borrows,n_loop24,ls24_event_rate,ci95_low,ci95_high,ls24_usd_share")
    for r in rows:
        n = int(r.get("n_borrows") or 0)
        k = int(r.get("n_loop24") or 0)
        low, high = wilson(k, n)
        usd = float(r.get("borrow_usd") or 0.0)
        loop_usd = float(r.get("loop24_usd") or 0.0)
        usd_share = (loop_usd / usd) if usd else 0.0
        print(f"{r.get('chain')},{n},{k},{(k/n if n else 0):.4f},{low:.4f},{high:.4f},{usd_share:.4f}")


if __name__ == "__main__":
    main()
