from __future__ import annotations

import os
from typing import Optional


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)


def main() -> None:
    """Placeholder entrypoint for historical data extraction."""
    exchange = _env("EXCHANGE")
    symbol = _env("SYMBOL")
    data_type = _env("TYPE")
    interval = _env("INTERVAL")
    start_ms = _env("START_MS")
    end_ms = _env("END_MS")
    if not exchange or not symbol or not data_type:
        raise SystemExit("EXCHANGE, SYMBOL, TYPE are required")
    if data_type == "candles" and not interval:
        raise SystemExit("INTERVAL is required for candles")
    if not start_ms or not end_ms:
        raise SystemExit("START_MS and END_MS are required")
    raise SystemExit(
        "mm_history CLI is a stub. Implement exchange clients and wiring next."
    )


if __name__ == "__main__":
    main()

