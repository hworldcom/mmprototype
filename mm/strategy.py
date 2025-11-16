import math
import numpy as np
from dataclasses import dataclass

from .avellaneda_stoikov import AvellanedaStoikovModel
from .utils import round_to_tick, clamp_qty
from .risk import RiskManager, RiskState
from .exchange import ExchangeAPI, Order


@dataclass
class StrategyConfig:
    symbol: str
    tick_size: float
    qty_step: float
    min_qty: float
    min_notional: float
    maker_fee: float
    taker_fee: float

    gamma: float
    horizon_seconds: float
    sigma_window_seconds: float
    A: float
    k: float

    base_order_notional: float
    min_spread_ticks: int
    max_quote_lifetime_ms: int
    inventory_skew_factor: float

    max_inventory: float
    soft_inventory: float
    max_notional_abs: float
    max_daily_loss: float
    max_drawdown: float


class AvellanedaStoikovStrategy:
    def __init__(self, cfg: StrategyConfig, exch: ExchangeAPI):
        self.cfg = cfg
        self.exch = exch

        self.model = AvellanedaStoikovModel(
            gamma=cfg.gamma,
            A=cfg.A,
            k=cfg.k,
            T=cfg.horizon_seconds,
        )

        from .risk import RiskLimits
        self.risk_mgr = RiskManager(
            RiskLimits(
                max_inventory=cfg.max_inventory,
                max_notional_abs=cfg.max_notional_abs,
                max_daily_loss=cfg.max_daily_loss,
                max_drawdown=cfg.max_drawdown,
            )
        )

        self.state = RiskState()
        self.prices: list[tuple[float, float]] = []  # (time, mid)
        self.open_orders: dict[str, Order] = {}

    def _estimate_sigma(self, now_t: float) -> float:
        window = self.cfg.sigma_window_seconds
        if len(self.prices) < 2:
            return 0.0
        # keep only recent
        self.prices = [(t, p) for (t, p) in self.prices if now_t - t <= window]
        if len(self.prices) < 2:
            return 0.0
        ps = np.array([p for (_, p) in self.prices])
        rets = np.diff(ps) / ps[:-1]
        if len(rets) == 0:
            return 0.0
        # std per sqrt(second)
        return float(np.std(rets) * math.sqrt(1.0))

    def _desired_order_size(self, mid: float) -> float:
        qty = self.cfg.base_order_notional / mid
        qty = clamp_qty(qty, self.cfg.qty_step)
        if qty < self.cfg.min_qty:
            return 0.0
        return qty

    def _apply_inventory_skew(self, bid: float, ask: float, mid: float) -> tuple[float, float]:
        q = self.state.inventory
        if q == 0:
            return bid, ask

        # Soft skew: more inventory → push quotes away on that side
        skew_factor = self.cfg.inventory_skew_factor
        if abs(q) > self.cfg.soft_inventory:
            skew_factor *= (abs(q) / self.cfg.soft_inventory)

        if q > 0:
            # Long: make selling easier (lower ask), buying harder (lower bid competitiveness)
            ask = max(ask - skew_factor * self.cfg.tick_size, mid)  # don't cross
            bid = bid - skew_factor * self.cfg.tick_size
        else:
            # Short: make buying easier (raise bid), selling harder
            bid = min(bid + skew_factor * self.cfg.tick_size, mid)
            ask = ask + skew_factor * self.cfg.tick_size

        return bid, ask

    def _enforce_spread_constraints(self, bid: float, ask: float, mid: float) -> tuple[float, float]:
        if bid <= 0 or ask <= 0:
            return bid, ask
        min_spread = self.cfg.min_spread_ticks * self.cfg.tick_size
        if ask - bid < min_spread:
            center = (bid + ask) / 2
            bid = center - min_spread / 2
            ask = center + min_spread / 2
        # don't quote inside mid in weird ways
        return bid, ask

    def on_market_data(self, now_t: float, mid: float):
        # update price history
        self.prices.append((now_t, mid))

    def on_fills(self, fills):
        """
        fills: list of (order_id, side, price, qty)
        """
        for oid, side, price, qty in fills:
            if oid in self.open_orders:
                self.open_orders.pop(oid, None)
            if side == "buy":
                # we bought base asset
                self.state.inventory += qty
                self.state.realized_pnl -= price * qty
            else:
                self.state.inventory -= qty
                self.state.realized_pnl += price * qty

    def recompute_and_quote(self, now_t: float):
        mid = self.exch.get_mid_price()

        # estimate sigma
        sigma = self._estimate_sigma(now_t)
        if sigma <= 0:
            # until we have a signal, be conservative or flat
            return

        # update unrealized pnl
        pos_notional = self.state.inventory * mid
        self.risk_mgr.update_unrealized(self.state, mid, pos_notional)

        if not self.risk_mgr.check_limits(self.state, mid):
            # cancel all & stop quoting
            for oid in list(self.open_orders.keys()):
                self.exch.cancel_order(oid)
            self.open_orders.clear()
            return

        # get theoretical quotes
        t_rel = 0.0  # you can track elapsed strategy time if needed
        raw_bid, raw_ask, r, h = self.model.optimal_quotes(mid, self.state.inventory, sigma, t_rel)

        # snap to tick
        bid = round_to_tick(raw_bid, self.cfg.tick_size)
        ask = round_to_tick(raw_ask, self.cfg.tick_size)

        # apply inventory skew
        bid, ask = self._apply_inventory_skew(bid, ask, mid)

        # enforce min spread etc.
        bid, ask = self._enforce_spread_constraints(bid, ask, mid)

        # size
        qty = self._desired_order_size(mid)
        if qty <= 0:
            return

        # basic validity checks
        if bid <= 0 or ask <= bid:
            return

        # cancel old orders (simple version)
        for oid in list(self.open_orders.keys()):
            self.exch.cancel_order(oid)
            self.open_orders.pop(oid, None)

        # place new ones
        bid_id = self.exch.place_limit_order("buy", bid, qty)
        ask_id = self.exch.place_limit_order("sell", ask, qty)

        from .exchange import Order
        self.open_orders[bid_id] = Order(bid_id, "buy", bid, qty, 0)
        self.open_orders[ask_id] = Order(ask_id, "sell", ask, qty, 0)
