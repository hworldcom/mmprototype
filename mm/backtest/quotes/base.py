from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Protocol


@dataclass(frozen=True)
class Quote:
    side: str          # "BUY" | "SELL"
    price: float
    qty: float
    # If None: the quote does not impose an expiry and the exchange will treat
    # it as Good-Till-Cancel unless BacktestConfig.order_ttl_ms is set.
    ttl_ms: Optional[int] = None


@dataclass(frozen=True)
class MarketState:
    recv_ms: int
    mid: float
    best_bid: float
    best_ask: float
    spread: float
    imbalance: Optional[float] = None  # [-1, 1] where positive means bid-heavy (optional)


@dataclass(frozen=True)
class PositionState:
    inventory: float   # base asset units
    cash: float        # quote currency units


class QuoteModel(Protocol):
    def generate_quotes(self, market: MarketState, position: PositionState) -> List[Quote]:
        ...
