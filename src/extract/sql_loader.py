from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "sql"


def load_sql(name: str, **kwargs: str) -> str:
    text = (SQL_DIR / name).read_text(encoding="utf-8")
    return text.format(**kwargs)
