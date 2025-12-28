from __future__ import annotations

import csv
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from mm.backtest.quotes.base import MarketState, PositionState, Quote, QuoteModel
from mm.backtest.fills.base import OpenOrder, FillModel, Fill


@dataclass
class BacktestConfig:
    maker_fee_rate: float = 0.001  # 0.1% worst-case
    order_latency_ms: int = 50
    quote_interval_ms: int = 100
    order_ttl_ms: int = 1000
    tick_size: float = 0.01


def round_to_tick(px: float, tick: float) -> float:
    if tick <= 0:
        return px
    return round(px / tick) * tick


class PaperExchange:
    def __init__(self, cfg: BacktestConfig, quote_model: QuoteModel, fill_model: FillModel, out_dir: Path, symbol: str):
        self.cfg = cfg
        self.quote_model = quote_model
        self.fill_model = fill_model
        self.symbol = symbol

        self.inventory = 0.0
        self.cash = 0.0

        self.open_orders: Dict[str, OpenOrder] = {}
        self.last_quote_ms: Optional[int] = None

        out_dir.mkdir(parents=True, exist_ok=True)
        self.fills_path = out_dir / f"fills_{symbol}.csv"
        self.state_path = out_dir / f"state_{symbol}.csv"
        self.orders_path = out_dir / f"orders_{symbol}.csv"

        self._fills_f = self.fills_path.open("w", newline="")
        self._state_f = self.state_path.open("w", newline="")
        self._orders_f = self.orders_path.open("w", newline="")

        self.fills_w = csv.writer(self._fills_f)
        self.state_w = csv.writer(self._state_f)
        self.orders_w = csv.writer(self._orders_f)

        self.fills_w.writerow(["recv_ms", "order_id", "side", "price", "qty", "fee", "reason"])
        self.state_w.writerow(["recv_ms", "inventory", "cash", "mid", "mtm_value"])
        self.orders_w.writerow(["recv_ms", "order_id", "side", "price", "qty", "active_recv_ms", "expire_recv_ms"])

    def close(self):
        for f in (self._fills_f, self._state_f, self._orders_f):
            try:
                f.close()
            except Exception:
                pass

    def position(self) -> PositionState:
        return PositionState(inventory=self.inventory, cash=self.cash)

    def cancel_all(self):
        self.open_orders.clear()

    def _place_order(self, recv_ms: int, q: Quote):
        oid = uuid.uuid4().hex[:12]
        active_ms = recv_ms + int(self.cfg.order_latency_ms)
        expire_ms = active_ms + int(q.ttl_ms if q.ttl_ms is not None else self.cfg.order_ttl_ms)

        price = float(round_to_tick(q.price, self.cfg.tick_size))

        oo = OpenOrder(
            order_id=oid,
            side=q.side,
            price=price,
            qty=float(q.qty),
            placed_recv_ms=recv_ms,
            active_recv_ms=active_ms,
            expire_recv_ms=expire_ms,
        )
        self.open_orders[oid] = oo
        self.orders_w.writerow([recv_ms, oid, q.side, f"{oo.price:.8f}", f"{oo.qty:.8f}", active_ms, expire_ms])
        self._orders_f.flush()

    def maybe_quote(self, market: MarketState):
        if self.last_quote_ms is None or (market.recv_ms - self.last_quote_ms) >= self.cfg.quote_interval_ms:
            self.last_quote_ms = market.recv_ms
            self.cancel_all()
            quotes = self.quote_model.generate_quotes(market, self.position())
            for q in quotes:
                self._place_order(market.recv_ms, q)

    def _apply_fill(self, fill: Fill):
        oo = self.open_orders.get(fill.order_id)
        if oo is None:
            return

        qty = min(oo.qty, fill.qty)
        notional = fill.price * qty
        fee = self.cfg.maker_fee_rate * notional

        if oo.side == "BUY":
            self.inventory += qty
            self.cash -= (notional + fee)
        else:
            self.inventory -= qty
            self.cash += (notional - fee)

        self.fills_w.writerow([fill.recv_ms, fill.order_id, oo.side, f"{fill.price:.8f}", f"{qty:.8f}", f"{fee:.8f}", fill.reason])
        self._fills_f.flush()

        remaining = oo.qty - qty
        if remaining <= 1e-12:
            self.open_orders.pop(fill.order_id, None)
        else:
            self.open_orders[fill.order_id] = OpenOrder(
                order_id=oo.order_id,
                side=oo.side,
                price=oo.price,
                qty=remaining,
                placed_recv_ms=oo.placed_recv_ms,
                active_recv_ms=oo.active_recv_ms,
                expire_recv_ms=oo.expire_recv_ms,
            )

    def on_tick(self, market: MarketState):
        # expire orders
        expired = [oid for oid, o in self.open_orders.items() if o.expire_recv_ms is not None and market.recv_ms > o.expire_recv_ms]
        for oid in expired:
            self.open_orders.pop(oid, None)

        self.maybe_quote(market)

        # tick-based fills
        fills = self.fill_model.on_tick(market, list(self.open_orders.values()))
        for f in fills:
            self._apply_fill(f)

        mtm = self.cash + self.inventory * market.mid
        self.state_w.writerow([market.recv_ms, f"{self.inventory:.8f}", f"{self.cash:.8f}", f"{market.mid:.8f}", f"{mtm:.8f}"])
        self._state_f.flush()

    def on_trade(self, recv_ms: int, price: float, qty: float, is_buyer_maker: int):
        fills = self.fill_model.on_trade(recv_ms, price, qty, is_buyer_maker, list(self.open_orders.values()))
        for f in fills:
            self._apply_fill(f)
