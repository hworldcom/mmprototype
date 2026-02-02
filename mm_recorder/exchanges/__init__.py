from __future__ import annotations

from .base import ExchangeAdapter
from .binance import BinanceAdapter
from .kraken import KrakenAdapter


_ADAPTERS = {
    "binance": BinanceAdapter,
    "kraken": KrakenAdapter,
}


def get_adapter(name: str) -> ExchangeAdapter:
    key = (name or "binance").strip().lower()
    if key not in _ADAPTERS:
        raise RuntimeError(f"Unknown exchange {name!r}. Available: {', '.join(sorted(_ADAPTERS))}")
    return _ADAPTERS[key]()
