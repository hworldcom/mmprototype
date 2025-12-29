from __future__ import annotations

import csv
import math
import uuid
from dataclasses import dataclass, replace
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
    # If None or 0: treat orders as Good-Till-Cancel (no exchange expiry).
    order_ttl_ms: Optional[int] = None

    # Optional internal refresh policy (independent of quote interval).
    # If set (ms), an unchanged resting order will be cancelled/replaced once its
    # age exceeds this interval. If None/0, unchanged orders are kept.
    refresh_interval_ms: Optional[int] = None

    # Market constraints
    tick_size: float = 0.01
    qty_step: float = 0.0  # 0 => no rounding
    min_notional: float = 0.0  # 0 => no constraint

    # Initial balances (spot)
    initial_cash: float = 0.0
    initial_inventory: float = 0.0

    # If True: reject orders that would exceed balances
    enforce_balances: bool = True

    # Quoting realism
    only_requote_on_change: bool = True
    suppress_unfunded_quotes: bool = True


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
    cancel_reason: str = ""


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
            "cancel_reason",
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
            st.cancel_reason,
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
        # Exchange expiry: default to GTC unless an explicit TTL is configured.
        ttl_ms: Optional[int] = q.ttl_ms
        if ttl_ms is None:
            ttl_ms = self.cfg.order_ttl_ms
        expire_ms: Optional[int] = None
        if ttl_ms is not None and int(ttl_ms) > 0:
            expire_ms = active_ms + int(ttl_ms)

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
            status="OPEN" if ok else "REJECTED",
        )

        st = _OrderState(
            oo=oo,
            remaining_qty=(qty if ok else 0.0),
            reject_reason=reason if not ok else "",
            cancel_reason="",
        )
        # Log the placement attempt regardless.
        self._log_order(recv_ms, oid, "PLACE", st)

        if not ok:
            # Do not add to book.
            return

        self.open_orders[oid] = st

    def cancel_order(self, recv_ms: int, order_id: str, cancel_reason: str = ""):
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
        st.cancel_reason = cancel_reason
        self._log_order(recv_ms, order_id, "CANCEL_REQ", st)

    def cancel_all(self, recv_ms: int, cancel_reason: str = ""):
        for oid in list(self.open_orders.keys()):
            self.cancel_order(recv_ms, oid, cancel_reason=cancel_reason)

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
        if self.last_quote_ms is not None and (now - self.last_quote_ms) < int(self.cfg.quote_interval_ms):
            return
        self.last_quote_ms = now

        # 1) Compute desired quotes from the strategy.
        quotes = self.quote_model.generate_quotes(market, self.position())
        desired_by_side: Dict[str, Quote] = {}
        for q in quotes:
            # Keep ttl_ms as-is; if None, we will treat it as GTC unless cfg.order_ttl_ms is set.
            desired_by_side[q.side.upper()] = q  # one per side (latest wins)

        # 2) Compare against current open orders and only cancel/replace if something changed.
        #    This prevents pathological churn (cancel same order, place same order).
        keep_sides: set[str] = set()
        if self.cfg.only_requote_on_change:
            # Find best existing OPEN order per side that is not already in cancel-pending
            existing_best: Dict[str, _OrderState] = {}
            for st in self.open_orders.values():
                if st.oo.status != "OPEN":
                    continue
                side = st.oo.side.upper()
                if side not in ("BUY", "SELL"):
                    continue
                # Choose most aggressive order for that side (BUY highest px, SELL lowest px)
                cur = existing_best.get(side)
                if cur is None:
                    existing_best[side] = st
                else:
                    if side == "BUY" and st.oo.price > cur.oo.price:
                        existing_best[side] = st
                    if side == "SELL" and st.oo.price < cur.oo.price:
                        existing_best[side] = st

            for side, dq in desired_by_side.items():
                ex = existing_best.get(side)
                if ex is None:
                    continue

                # Compare after applying exchange rounding rules
                dpx = round_price_for_side(float(dq.price), self.cfg.tick_size, side)
                dqty = round_qty(float(dq.qty), self.cfg.qty_step)

                same_quote = (abs(ex.oo.price - dpx) < 1e-12 and abs(ex.remaining_qty - dqty) < 1e-12)

                # Optional refresh policy (independent of quote interval).
                refresh_ms = self.cfg.refresh_interval_ms
                needs_refresh = False
                if refresh_ms is not None and int(refresh_ms) > 0:
                    # Use active_recv_ms as "age" anchor: when the order becomes live.
                    age = max(0, now - int(ex.oo.active_recv_ms))
                    needs_refresh = age >= int(refresh_ms)

                if same_quote and not needs_refresh:
                    keep_sides.add(side)

        # 3) Cancel only sides that need to change
        for oid, st in list(self.open_orders.items()):
            if st.oo.status != "OPEN":
                continue
            if st.oo.side.upper() in keep_sides:
                continue
            # Send cancel request; order remains fill-eligible until cancel ack.
            # Determine cancel reason.
            side = st.oo.side.upper()
            if side in desired_by_side:
                dq = desired_by_side[side]
                dpx = round_price_for_side(float(dq.price), self.cfg.tick_size, side)
                dqty = round_qty(float(dq.qty), self.cfg.qty_step)
                same_quote = (abs(st.oo.price - dpx) < 1e-12 and abs(st.remaining_qty - dqty) < 1e-12)
                if same_quote:
                    reason = "REFRESH"
                else:
                    reason = "QUOTE_CHANGE"
            else:
                reason = "NO_DESIRED_QUOTE"
            self.cancel_order(now, oid, cancel_reason=reason)

        # 4) Place quotes (optionally suppressing unfunded sides)
        for side, dq in desired_by_side.items():
            if side in keep_sides:
                continue

            # Apply rounding now so we can reason about funding before placing.
            px = round_price_for_side(float(dq.price), self.cfg.tick_size, side)
            qty = round_qty(float(dq.qty), self.cfg.qty_step)

            ok, reason = self._can_place(side, px, qty)
            if self.cfg.suppress_unfunded_quotes and not ok:
                # Do not attempt placement; log a SKIP for traceability and to avoid spammy REJECT rows.
                oid = str(uuid.uuid4())
                active_ms = now + int(self.cfg.order_latency_ms)
                # SKIP pseudo-order: include expiry only if TTL configured.
                ttl_ms: Optional[int] = dq.ttl_ms
                if ttl_ms is None:
                    ttl_ms = self.cfg.order_ttl_ms
                expire_ms: Optional[int] = None
                if ttl_ms is not None and int(ttl_ms) > 0:
                    expire_ms = active_ms + int(ttl_ms)
                oo = OpenOrder(
                    order_id=oid,
                    side=side,
                    price=px,
                    qty=qty,
                    placed_recv_ms=now,
                    active_recv_ms=active_ms,
                    expire_recv_ms=expire_ms,
                    cancel_req_ms=None,
                    cancel_effective_ms=None,
                    status="SKIPPED",
                )
                st = _OrderState(oo=oo, remaining_qty=0.0, reject_reason=reason, cancel_reason="")
                self._log_order(now, oid, "SKIP", st)
                continue

            # Proceed with placement; _place_order will enforce balances (and log REJECTED if needed).
            self._place_order(now, dq)
    def _expire_and_cancel_ack(self, now_ms: int):
        # Expire
        for oid in list(self.open_orders.keys()):
            st = self.open_orders.get(oid)
            if st is None:
                continue
            oo = st.oo
            if oo.expire_recv_ms is not None and now_ms >= oo.expire_recv_ms:
                st.oo = replace(st.oo, status="EXPIRED")
                st.cancel_reason = "TTL_EXPIRE"
                self._log_order(now_ms, oid, "EXPIRE", st)
                self.open_orders.pop(oid, None)

        # Cancel acknowledgements
        for oid in list(self.open_orders.keys()):
            st = self.open_orders.get(oid)
            if st is None:
                continue
            oo = st.oo
            if oo.status == "CANCEL_PENDING" and oo.cancel_effective_ms is not None and now_ms >= oo.cancel_effective_ms:
                st.oo = replace(st.oo, status="CANCELLED")
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
            