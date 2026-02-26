from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any


def pct(x: float) -> str:
    return f"{x * 100:.1f}%"


def usd(x: float) -> str:
    return f"${x:,.0f}"


def write_report(
    out_dir: Path,
    as_of: datetime,
    lookback_days: int,
    metrics: list[dict[str, Any]],
    verdict: str,
    off_ramp_method: str,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# Alpen SIGNAL Report - BTC Collateral Borrowing Intent (Morpho, ETH + Base)")
    lines.append("")
    lines.append(f"- As of: {as_of.isoformat()}")
    lines.append(f"- Cohort window: last {lookback_days} days")
    lines.append("- Flow windows: LS1=1h, LS24=24h, DR7/OR7=7d")
    lines.append(f"- Off-ramp method: {off_ramp_method}")
    lines.append("")
    lines.append("## KPI Table (MEASURED)")
    lines.append("")

    if not metrics:
        lines.append("No borrowers matched the cohort in the selected window.")
    else:
        headers = [
            "chain",
            "venue",
            "borrowers_90d",
            "borrow_usd_90d",
            "ls1",
            "ls24",
            "dr7",
            "or7",
            "top25_share",
            "repeat_rate",
            "n_borrows",
            "ls24_event_rate",
            "ls24_event_ci95",
        ]
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("|" + "---|" * len(headers))
        for r in metrics:
            ci95 = f"[{pct(float(r['ls24_event_ci95_low']))}, {pct(float(r['ls24_event_ci95_high']))}]"
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(r["chain"]),
                        str(r["venue"]),
                        str(int(r["borrowers_90d"])),
                        usd(float(r["borrow_usd_90d"])),
                        pct(float(r["ls1"])),
                        pct(float(r["ls24"])),
                        pct(float(r["dr7"])),
                        pct(float(r["or7"])),
                        pct(float(r["top25_share"])),
                        pct(float(r["repeat_rate"])),
                        str(int(r["n_borrows"])),
                        pct(float(r["ls24_event_rate"])),
                        ci95,
                    ]
                )
                + " |"
            )

        lines.append("")
        lines.append("## Sample Adequacy (MEASURED)")
        lines.append("")
        lines.append("Heuristic: sample is directionally reliable when `n_borrows >= 100` and LS24 event-rate CI width <= 20pp.")
        for r in metrics:
            n = int(r["n_borrows"])
            width = float(r["ls24_event_ci95_high"]) - float(r["ls24_event_ci95_low"])
            ok = (n >= 100) and (width <= 0.20)
            lines.append(f"- {r['chain']} {r['venue']}: n={n}, ci_width={width*100:.1f}pp -> {'sufficient' if ok else 'insufficient'}")

    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    lines.append(verdict)
    lines.append("")
    lines.append("## Truth Labels")
    lines.append("")
    lines.append("- VERIFIED: Dune raw onchain tables (Morpho Blue, tokens.transfers, dex.trades).")
    lines.append("- MEASURED: KPI outputs in `signal_metrics.csv` and audit rows in `borrow_flows_sample.csv`.")
    lines.append("- INFERRED: off-ramp proxy uses recipient EOA dominance; no external CEX labels.")
    lines.append("- HYPOTHESIS/UNSURE: OR7 is a proxy, not a direct CEX attribution metric.")

    (out_dir / "signal_report.md").write_text("\n".join(lines), encoding="utf-8")
