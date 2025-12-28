from __future__ import annotations

from dataclasses import dataclass
from typing import List

from .base import FillModel, OpenOrder, Fill
from .trade_driven import TradeDrivenFillModel
from .poisson import PoissonFillModel
from mm.backtest.quotes.base import MarketState


@dataclass
class HybridFillModel(FillModel):
    trade_model: TradeDrivenFillModel
    poisson_model: PoissonFillModel

    def on_trade(self, trade_recv_ms: int, trade_price: float, trade_qty: float, is_buyer_maker: int,
                 open_orders: List[OpenOrder]) -> List[Fill]:
        return self.trade_model.on_trade(trade_recv_ms, trade_price, trade_qty, is_buyer_maker, open_orders)

    def on_tick(self, market: MarketState, open_orders: List[OpenOrder]) -> List[Fill]:
        return self.poisson_model.on_tick(market, open_orders)
