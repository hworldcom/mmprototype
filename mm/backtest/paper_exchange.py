from __future__ import annotations

import csv
import math
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, List

from mm.backtest.quotes.base import MarketState, PositionState, Quote, QuoteModel
from mm.backtest.fills.base import OpenOrder, FillModel, Fill


@dataclass
class BacktestConfig:
    # Costs
    maker_fee_rate: float = 0.001  # 0.1% worst-case

    # Latency
    order_latency_ms: int = 50
    cancel_latency_ms: int = 25

    # Quoting cadence / lifetime
    quote_interval_ms: int = 250
    order_ttl_ms: int = 1000

    # Market constraints
    tick_size: float = 0.01
    qty_step: float = 0.0  # 0 => no rounding
    min_notional: float = 0.0  # 0 => no constraint

    # Initial balances (spot)
    initial_cash: float = 0.0
    initial_inventory: float = 0.0

    # If True: reject orders that would exceed balances
    enforce_balances: bool = True


def _floor_to_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.floor(x / step) * step


def _ceil_to_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.ceil(x / step) * step


def round_price_for_side(px: float, tick: float, side: str) -> float:
    """Side-aware tick rounding:
    - BUY: floor (never accidentally cross up)
    - SELL: ceil (never accidentally cross down)
    """
    if tick <= 0:
        return px
    if side.upper() == "BUY":
        return _floor_to_step(px, tick)
    return _ceil_to_step(px, tick)


def round_qty(qty: float, step: float) -> float:
    # Always round down for safety.
    return max(0.0, _floor_to_step(qty, step)) if step > 0 else max(0.0, qty)


@dataclass
class _OrderState:
    oo: OpenOrder
    remaining_qty: float
    reject_reason: str = ""


class PaperExchange:
    """Deterministic paper exchange that supports:
    - entry latency
    - cancel latency (CANCEL_PENDING window)
    - TTL expiry
    - maker fees
    - basic spot balance constraints (cash & inventory)
    """

    def __init__(self, cfg: BacktestConfig, quote_model: QuoteModel, fill_model: FillModel, out_dir: Path, symbol: str):
        self.cfg = cfg
        self.quote_model = quote_model
        self.fill_model = fill_model
        self.symbol = symbol

        self.inventory = float(cfg.initial_inventory)
        self.cash = float(cfg.initial_cash)

        self.open_orders: Dict[str, _OrderState] = {}
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
        self.state_w.writerow(["recv_ms", "inventory", "cash", "mid", "mtm_value", "open_orders"])
        self.orders_w.writerow([
            "recv_ms", "order_id", "action", "status", "side", "price", "qty",
            "remaining_qty",
            "active_recv_ms", "expire_recv_ms",
            "cancel_req_ms", "cancel_effective_ms",
            "reject_reason",
        ])

    def close(self):
        for f in (self._fills_f, self._state_f, self._orders_f):
            try:
                f.close()
            except Exception:
                pass

    def position(self) -> PositionState:
        return PositionState(inventory=self.inventory, cash=self.cash)

    # ---------------------------
    # Order state helpers
    # ---------------------------

    def _log_order(self, recv_ms: int, oid: str, action: str, st: _OrderState):
        oo = st.oo
        self.orders_w.writerow([
            recv_ms,
            oid,
            action,
            oo.status,
            oo.side,
            f"{oo.price:.8f}",
            f"{oo.qty:.8f}",
            f"{st.remaining_qty:.8f}",
            oo.active_recv_ms,
            oo.expire_recv_ms if oo.expire_recv_ms is not None else "",
            oo.cancel_req_ms if oo.cancel_req_ms is not None else "",
            oo.cancel_effective_ms if oo.cancel_effective_ms is not None else "",
            st.reject_reason,
        ])
        self._orders_f.flush()

    def _is_fill_eligible(self, now_ms: int, st: _OrderState) -> bool:
        oo = st.oo
        if now_ms < oo.active_recv_ms:
            return False
        if oo.expire_recv_ms is not None and now_ms >= oo.expire_recv_ms:
            return False
        if oo.cancel_effective_ms is not None and now_ms >= oo.cancel_effective_ms:
            return False
        if st.remaining_qty <= 1e-12:
            return False
        return True

    def _snapshot_open_orders_for_fills(self, now_ms: int) -> List[OpenOrder]:
        # Provide remaining qty to fill models.
        res: List[OpenOrder] = []
        for oid, st in self.open_orders.items():
            if not self._is_fill_eligible(now_ms, st):
                continue
            oo = st.oo
            res.append(OpenOrder(
                order_id=oo.order_id,
                side=oo.side,
                price=oo.price,
                qty=st.remaining_qty,
                placed_recv_ms=oo.placed_recv_ms,
                active_recv_ms=oo.active_recv_ms,
                expire_recv_ms=oo.expire_recv_ms,
                cancel_req_ms=oo.cancel_req_ms,
                cancel_effective_ms=oo.cancel_effective_ms,
                status=oo.status,
            ))
        return res

    # ---------------------------
    # Placement / cancel
    # ---------------------------

    def _can_place(self, side: str, price: float, qty: float) -> Tuple[bool, str]:
        if qty <= 0:
            return False, "qty<=0"
        notional = price * qty
        if self.cfg.min_notional > 0 and notional < self.cfg.min_notional:
            return False, f"min_notional<{self.cfg.min_notional}"
        if not self.cfg.enforce_balances:
            return True, ""
        if side.upper() == "BUY":
            # Reserve cash including fees (conservative)
            fee = self.cfg.maker_fee_rate * notional
            if self.cash + 1e-12 < notional + fee:
                return False, "insufficient_cash"
        else:
            if self.inventory + 1e-12 < qty:
                return False, "insufficient_inventory"
        return True, ""

    def _place_order(self, recv_ms: int, q: Quote):
        oid = str(uuid.uuid4())

        side = q.side.upper()
        px = round_price_for_side(float(q.price), self.cfg.tick_size, side)
        qty = round_qty(float(q.qty), self.cfg.qty_step)

        ok, reason = self._can_place(side, px, qty)
        active_ms = recv_ms + int(self.cfg.order_latency_ms)
        expire_ms = active_ms + int(q.ttl_ms if q.ttl_ms is not None else self.cfg.order_ttl_ms)

        oo = OpenOrder(
            order_id=oid,
            side=side,
            price=px,
            qty=qty,
            placed_recv_ms=recv_ms,
            active_recv_ms=active_ms,
            expire_recv_ms=expire_ms,
            cancel_req_ms=None,
            cancel_effective_ms=None,
            status="OPEN",
        )

        st = _OrderState(oo=oo, remaining_qty=qty, reject_reason=reason if not ok else "")
        # Log the placement attempt regardless.
        self._log_order(recv_ms, oid, "PLACE", st)

        if not ok:
            # Do not add to book.
            return

        self.open_orders[oid] = st

    def cancel_order(self, recv_ms: int, order_id: str):
        st = self.open_orders.get(order_id)
        if st is None:
            return
        oo = st.oo
        if oo.status == "CANCEL_PENDING":
            return
        cancel_eff = recv_ms + int(self.cfg.cancel_latency_ms)
        new_oo = OpenOrder(
            order_id=oo.order_id,
            side=oo.side,
            price=oo.price,
            qty=oo.qty,
            placed_recv_ms=oo.placed_recv_ms,
            active_recv_ms=oo.active_recv_ms,
            expire_recv_ms=oo.expire_recv_ms,
            cancel_req_ms=recv_ms,
            cancel_effective_ms=cancel_eff,
            status="CANCEL_PENDING",
        )
        st.oo = new_oo
        self._log_order(recv_ms, order_id, "CANCEL_REQ", st)

    def cancel_all(self, recv_ms: int):
        for oid in list(self.open_orders.keys()):
            self.cancel_order(recv_ms, oid)

    # ---------------------------
    # Fill application
    # ---------------------------

    def _apply_fill(self, fill: Fill):
        st = self.open_orders.get(fill.order_id)
        if st is None:
            return
        oo = st.oo
        qty = min(float(fill.qty), st.remaining_qty)
        if qty <= 1e-12:
            return

        notional = qty * float(fill.price)
        fee = self.cfg.maker_fee_rate * notional

        # Apply balances
        if oo.side == "BUY":
            self.inventory += qty
            self.cash -= (notional + fee)
        else:
            self.inventory -= qty
            self.cash += (notional - fee)

        self.fills_w.writerow([fill.recv_ms, fill.order_id, oo.side, f"{fill.price:.8f}", f"{qty:.8f}", f"{fee:.8f}", fill.reason])
        self._fills_f.flush()

        st.remaining_qty -= qty
        self._log_order(fill.recv_ms, fill.order_id, "FILL", st)

        if st.remaining_qty <= 1e-12:
            # Terminal
            self.open_orders.pop(fill.order_id, None)
            # Log terminal state row
            self._log_order(fill.recv_ms, fill.order_id, "CLOSE_FILLED", st)

    # ---------------------------
    # Tick/trade handlers
    # ---------------------------

    def maybe_quote(self, market: MarketState):
        now = market.recv_ms
        if self.last_quote_ms is None or (now - self.last_quote_ms) >= int(self.cfg.quote_interval_ms):
            self.last_quote_ms = now

            # Realistic cancel/replace: cancel requests go out now, but orders remain live until cancel ack.
            self.cancel_all(now)

            quotes = self.quote_model.generate_quotes(market, self.position())
            for q in quotes:
                # Use default TTL from cfg if quote doesn't specify (should always specify)
                if q.ttl_ms is None:
                    q = Quote(side=q.side, price=q.price, qty=q.qty, ttl_ms=self.cfg.order_ttl_ms)
                self._place_order(now, q)

    def _expire_and_cancel_ack(self, now_ms: int):
        # Expire
        for oid in list(self.open_orders.keys()):
            st = self.open_orders.get(oid)
            if st is None:
                continue
            oo = st.oo
            if oo.expire_recv_ms is not None and now_ms >= oo.expire_recv_ms:
                self._log_order(now_ms, oid, "EXPIRE", st)
                self.open_orders.pop(oid, None)

        # Cancel acknowledgements
        for oid in list(self.open_orders.keys()):
            st = self.open_orders.get(oid)
            if st is None:
                continue
            oo = st.oo
            if oo.status == "CANCEL_PENDING" and oo.cancel_effective_ms is not None and now_ms >= oo.cancel_effective_ms:
                self._log_order(now_ms, oid, "CANCEL_ACK", st)
                self.open_orders.pop(oid, None)

    def on_tick(self, market: MarketState):
        now = market.recv_ms

        # 1) Generate fills first (orders might still be live during cancel latency)
        open_for_fills = self._snapshot_open_orders_for_fills(now)
        fills = self.fill_model.on_tick(market, open_for_fills)
        for f in fills:
            self._apply_fill(f)

        # 2) Apply expirations and cancel acknowledgements
        self._expire_and_cancel_ack(now)

        # 3) Quote (cancel/replace)
        self.maybe_quote(market)

        # 4) Snapshot state
        mtm = self.cash + self.inventory * market.mid
        self.state_w.writerow([market.recv_ms, f"{self.inventory:.8f}", f"{self.cash:.8f}", f"{market.mid:.8f}", f"{mtm:.8f}", len(self.open_orders)])
        self._state_f.flush()

    def on_trade(self, recv_ms: int, price: float, qty: float, is_buyer_maker: int):
        # Trade-driven fills
        open_for_fills = self._snapshot_open_orders_for_fills(recv_ms)
        fills = self.fill_model.on_trade(recv_ms, price, qty, is_buyer_maker, open_for_fills)
        for f in fills:
            self._apply_fill(f)
