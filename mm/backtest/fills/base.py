from __future__ import annotations

from dataclasses import dataclass
from typing import List, Protocol, Optional

from mm.backtest.quotes.base import MarketState


@dataclass(frozen=True)
class OpenOrder:
    order_id: str
    side: str
    price: float
    qty: float
    placed_recv_ms: int
    active_recv_ms: int
    expire_recv_ms: Optional[int] = None


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
