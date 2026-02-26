from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import random
from typing import Any
import math


@dataclass
class SignalOutputs:
    metrics: list[dict[str, Any]]
    borrow_classified: list[dict[str, Any]]
    audit_sample: list[dict[str, Any]]
    verdict: str


def _as_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in {"true", "t", "1"}
    return bool(v)


def _as_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _wilson_95(successes: int, n: int) -> tuple[float, float]:
    if n <= 0:
        return (0.0, 0.0)
    z = 1.96
    phat = successes / n
    denom = 1 + z * z / n
    center = (phat + z * z / (2 * n)) / denom
    margin = (z * math.sqrt((phat * (1 - phat) + z * z / (4 * n)) / n)) / denom
    return (max(0.0, center - margin), min(1.0, center + margin))


def classify_borrows(
    borrows: list[dict[str, Any]],
    outflows: list[dict[str, Any]],
    bridge_addresses: dict[str, set[str]],
    sample_size: int,
) -> SignalOutputs:
    if not borrows:
        return SignalOutputs([], [], [], "Verdict: no cohort found in the configured window.")

    outflow_by_receiver: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for o in outflows:
        outflow_by_receiver[str(o.get("receiver", "")).lower()].append(o)

    bridge_receivers: set[str] = set()
    for o in outflows:
        receiver = str(o.get("receiver", "")).lower()
        recipient = str(o.get("recipient", "")).lower()
        chain = str(o.get("chain", ""))
        if recipient in {a.lower() for a in bridge_addresses.get(chain, set())}:
            bridge_receivers.add(receiver)

    classified: list[dict[str, Any]] = []
    for b in borrows:
        row = dict(b)
        receiver_lc = str(row.get("receiver", "")).lower()
        row["borrow_amount_usd"] = _as_float(row.get("borrow_amount_usd"))
        row["loop_1h"] = _as_bool(row.get("loop_1h"))
        row["loop_24h"] = _as_bool(row.get("loop_24h"))
        loop_7d = _as_bool(row.get("loop_7d"))
        defi_7d = _as_bool(row.get("defi_7d"))
        bridge_7d = _as_bool(row.get("bridge_7d"))

        recv_out = outflow_by_receiver.get(receiver_lc, [])
        off_ramp_proxy = False
        if recv_out:
            eoa_usd = sum(_as_float(x.get("transfer_amount_usd")) for x in recv_out if str(x.get("recipient_type", "")) == "eoa")
            total_usd = sum(_as_float(x.get("transfer_amount_usd")) for x in recv_out)
            off_ramp_proxy = total_usd > 0 and (eoa_usd / total_usd) >= 0.7

        if loop_7d:
            bucket = "A_looper"
        elif bridge_7d or receiver_lc in bridge_receivers:
            bucket = "C_bridge"
        elif defi_7d:
            bucket = "B_defi_retained"
        elif off_ramp_proxy:
            bucket = "D_offramp_proxy"
        else:
            bucket = "E_idle_eoa"

        row["bucket"] = bucket
        classified.append(row)

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in classified:
        grouped[(str(r.get("chain", "")), str(r.get("venue", "")))].append(r)

    metrics: list[dict[str, Any]] = []
    for (chain, venue), rows in sorted(grouped.items()):
        borrow_usd = sum(r["borrow_amount_usd"] for r in rows)
        borrowers = {str(r.get("borrower", "")).lower() for r in rows}

        ls1_num = sum(r["borrow_amount_usd"] for r in rows if r["loop_1h"])
        ls24_num = sum(r["borrow_amount_usd"] for r in rows if r["loop_24h"])
        dr7_num = sum(r["borrow_amount_usd"] for r in rows if r["bucket"] in {"A_looper", "B_defi_retained", "C_bridge"})
        or7_num = sum(r["borrow_amount_usd"] for r in rows if r["bucket"] == "D_offramp_proxy")

        by_borrower: dict[str, float] = defaultdict(float)
        by_borrower_events: dict[str, int] = defaultdict(int)
        for r in rows:
            b = str(r.get("borrower", "")).lower()
            by_borrower[b] += r["borrow_amount_usd"]
            by_borrower_events[b] += 1

        top25 = sum(v for _, v in sorted(by_borrower.items(), key=lambda kv: kv[1], reverse=True)[:25])
        repeat_count = sum(1 for _, c in by_borrower_events.items() if c >= 3)
        repeat_rate = (repeat_count / len(by_borrower_events)) if by_borrower_events else 0.0
        n_borrows = len(rows)
        ls24_events = sum(1 for r in rows if r["loop_24h"])
        ls24_ci_low, ls24_ci_high = _wilson_95(ls24_events, n_borrows)

        metrics.append(
            {
                "chain": chain,
                "venue": venue,
                "borrowers_90d": len(borrowers),
                "borrow_usd_90d": borrow_usd,
                "ls1": (ls1_num / borrow_usd) if borrow_usd else 0.0,
                "ls24": (ls24_num / borrow_usd) if borrow_usd else 0.0,
                "dr7": (dr7_num / borrow_usd) if borrow_usd else 0.0,
                "or7": (or7_num / borrow_usd) if borrow_usd else 0.0,
                "top25_share": (top25 / borrow_usd) if borrow_usd else 0.0,
                "repeat_rate": repeat_rate,
                "n_borrows": n_borrows,
                "ls24_event_rate": (ls24_events / n_borrows) if n_borrows else 0.0,
                "ls24_event_ci95_low": ls24_ci_low,
                "ls24_event_ci95_high": ls24_ci_high,
            }
        )

    ls24_mean = sum(m["ls24"] for m in metrics) / len(metrics) if metrics else 0.0
    dr7_mean = sum(m["dr7"] for m in metrics) / len(metrics) if metrics else 0.0
    or7_mean = sum(m["or7"] for m in metrics) / len(metrics) if metrics else 0.0

    loop_meaning = "dominant" if ls24_mean >= 0.5 else "meaningful" if ls24_mean >= 0.2 else "minor"
    flow_mix = "DeFi-retained" if dr7_mean >= 0.6 else "off-ramped" if or7_mean >= 0.5 else "mixed"
    wedge = "Looper Lane" if loop_meaning in {"dominant", "meaningful"} else "Credit Rail"
    verdict = f"Verdict: Loops are {loop_meaning}; borrowed stables are {flow_mix}; wedge = {wedge}."

    outflow_by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for o in outflows:
        outflow_by_tx[str(o.get("borrow_tx_hash", ""))].append(o)

    audit_rows: list[dict[str, Any]] = []
    for r in classified:
        tx = str(r.get("borrow_tx_hash", ""))
        rows = outflow_by_tx.get(tx)
        if not rows:
            audit_rows.append(dict(r))
            continue
        for o in rows[:3]:
            merged = dict(r)
            merged["recipient"] = o.get("recipient")
            merged["recipient_type"] = o.get("recipient_type")
            merged["transfer_amount_usd"] = o.get("transfer_amount_usd")
            audit_rows.append(merged)

    if len(audit_rows) > sample_size:
        random.seed(42)
        audit_rows = random.sample(audit_rows, sample_size)

    return SignalOutputs(metrics=metrics, borrow_classified=classified, audit_sample=audit_rows, verdict=verdict)
