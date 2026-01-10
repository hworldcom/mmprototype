from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

from mm.backtest.quotes.base import MarketState, PositionState


@dataclass(frozen=True)
class BookLevel:
    price: float
    qty: float


@dataclass(frozen=True)
class OrderBookTopN:
    bids: List[BookLevel]
    asks: List[BookLevel]


@dataclass(frozen=True)
class StrategySnapshot:
    """Stable input object passed to strategies/exchange on each replay tick.

    This snapshot intentionally contains only immutable, strategy-relevant data.
    It avoids exposing the mutable OrderBookSyncEngine to reduce coupling.
    """

    recv_ms: int
    recv_seq: int
    market: MarketState
    position: PositionState
    book_topn: Optional[OrderBookTopN] = None


def build_topn_book_from_levels(bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]) -> OrderBookTopN:
    return OrderBookTopN(
        bids=[BookLevel(float(p), float(q)) for p, q in bids],
        asks=[BookLevel(float(p), float(q)) for p, q in asks],
    )
