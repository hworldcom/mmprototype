from __future__ import annotations

from dataclasses import dataclass
from typing import List

from .base import FillModel, OpenOrder, Fill


@dataclass
class TradeDrivenFillModel(FillModel):
    allow_partial: bool = True

    def on_trade(self, trade_recv_ms: int, trade_price: float, trade_qty: float, is_buyer_maker: int,
                 open_orders: List[OpenOrder]) -> List[Fill]:
        fills: List[Fill] = []

        # Binance: m==1 => buyer is maker => sell aggressor
        sell_aggressor = bool(is_buyer_maker)
        buy_aggressor = not sell_aggressor

        remaining = float(trade_qty)

        for o in open_orders:
            if remaining <= 0:
                break
            if o.qty <= 0:
                continue
            if o.active_recv_ms > trade_recv_ms:
                continue
            if o.expire_recv_ms is not None and trade_recv_ms > o.expire_recv_ms:
                continue

            if o.side == "BUY" and sell_aggressor and trade_price <= o.price:
                qty = min(o.qty, remaining) if self.allow_partial else o.qty
                fills.append(Fill(o.order_id, trade_recv_ms, o.price, qty, "trade_cross"))
                remaining -= qty

            elif o.side == "SELL" and buy_aggressor and trade_price >= o.price:
                qty = min(o.qty, remaining) if self.allow_partial else o.qty
                fills.append(Fill(o.order_id, trade_recv_ms, o.price, qty, "trade_cross"))
                remaining -= qty

        return fills
