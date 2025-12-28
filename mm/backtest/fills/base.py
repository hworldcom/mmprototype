from __future__ import annotations

from dataclasses import dataclass
from typing import List, Protocol, Optional

from mm.backtest.quotes.base import MarketState


@dataclass(frozen=True)
class OpenOrder:
    order_id: str
    side: str                 # "BUY" | "SELL"
    price: float
    qty: float                # original qty
    placed_recv_ms: int       # when strategy requested placement
    active_recv_ms: int       # when order becomes active at the exchange (after entry latency)
    expire_recv_ms: Optional[int] = None  # TTL expiry (optional)

    # Cancel realism: cancel is requested at cancel_req_ms but only becomes effective at cancel_effective_ms.
    # Fill models should treat the order as fill-eligible until cancel_effective_ms is reached.
    cancel_req_ms: Optional[int] = None
    cancel_effective_ms: Optional[int] = None
    status: str = "OPEN"      # "OPEN" | "CANCEL_PENDING"


@dataclass(frozen=True)
class Fill:
    order_id: str
    recv_ms: int
    price: float
    qty: float
    reason: str


class FillModel(Protocol):
    def on_tick(self, market: MarketState, open_orders: List[OpenOrder]) -> List[Fill]:
        return []

    def on_trade(
        self,
        trade_recv_ms: int,
        trade_price: float,
        trade_qty: float,
        is_buyer_maker: int,
        open_orders: List[OpenOrder],
    ) -> List[Fill]:
        return []
