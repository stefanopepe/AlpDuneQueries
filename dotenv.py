"""Local fallback shim for python-dotenv in offline environments."""

from pathlib import Path


def load_dotenv(dotenv_path: str | None = None, *args, **kwargs) -> bool:
    """Best-effort .env loader used when python-dotenv is unavailable."""
    path = Path(dotenv_path) if dotenv_path else Path(".env")
    if not path.exists():
        return False
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        # Only set if not already present
        import os

        os.environ.setdefault(key, value)
    return True
