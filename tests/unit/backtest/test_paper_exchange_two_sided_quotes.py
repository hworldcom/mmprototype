from __future__ import annotations

from mm.backtest.paper_exchange import BacktestConfig, PaperExchange
from mm.backtest.quotes.base import MarketState, Quote, PositionState


class _TwoSidedQuoteModel:
    """Deterministic two-sided quote model for unit tests."""

    def __init__(self, bid_px: float, ask_px: float, qty: float):
        self._bid_px = float(bid_px)
        self._ask_px = float(ask_px)
        self._qty = float(qty)

    def generate_quotes(self, market: MarketState, position: PositionState):
        return [
            Quote(side="BUY", price=self._bid_px, qty=self._qty, ttl_ms=None),
            Quote(side="SELL", price=self._ask_px, qty=self._qty, ttl_ms=None),
        ]


class _NoopFillModel:
    def on_tick(self, market, open_orders):
        return []

    def on_trade(self, trade_recv_ms, trade_price, trade_qty, is_buyer_maker, open_orders):
        return []


def _mk_market(recv_ms: int = 1_000, mid: float = 100.0) -> MarketState:
    return MarketState(
        recv_ms=int(recv_ms),
        mid=float(mid),
        best_bid=float(mid - 0.01),
        best_ask=float(mid + 0.01),
        spread=0.02,
        imbalance=None,
    )


def test_paper_exchange_places_both_sides_when_funded(tmp_path):
    """If cash and inventory are sufficient, we should place both BUY and SELL
    in the same quote cycle (no "only 1 open order" artifact).
    """
    q = _TwoSidedQuoteModel(bid_px=99.99, ask_px=100.01, qty=0.001)

    cfg = BacktestConfig(
        initial_cash=1_000.0,
        initial_inventory=1.0,
        enforce_balances=True,
        suppress_unfunded_quotes=True,
        quote_interval_ms=0,  # always quote
        tick_size=0.01,
        qty_step=0.0,
        min_notional=0.0,
        order_ttl_ms=None,
        refresh_interval_ms=None,
    )

    ex = PaperExchange(cfg=cfg, quote_model=q, fill_model=_NoopFillModel(), out_dir=tmp_path, symbol="TEST")
    try:
        ex.maybe_quote(_mk_market())
        assert len(ex.open_orders) == 2
        assert sorted([st.oo.side for st in ex.open_orders.values()]) == ["BUY", "SELL"]
        assert all(st.oo.status == "OPEN" for st in ex.open_orders.values())
    finally:
        ex.close()


def test_paper_exchange_suppresses_unfunded_sell_side(tmp_path):
    """If inventory is insufficient and suppress_unfunded_quotes=True, SELL is
    skipped (logged as SKIP) and only BUY becomes an OPEN order."""
    q = _TwoSidedQuoteModel(bid_px=99.99, ask_px=100.01, qty=0.001)

    cfg = BacktestConfig(
        initial_cash=1_000.0,
        initial_inventory=0.0,
        enforce_balances=True,
        suppress_unfunded_quotes=True,
        quote_interval_ms=0,
        tick_size=0.01,
    )

    ex = PaperExchange(cfg=cfg, quote_model=q, fill_model=_NoopFillModel(), out_dir=tmp_path, symbol="TEST")
    try:
        ex.maybe_quote(_mk_market())
        assert len(ex.open_orders) == 1
        only = next(iter(ex.open_orders.values()))
        assert only.oo.side == "BUY"
        assert only.oo.status == "OPEN"

        # Ensure we did record a SKIP line for SELL for traceability.
        orders_text = (tmp_path / "orders_TEST.csv").read_text(encoding="utf-8")
        assert "SKIP" in orders_text
        assert ",SELL," in orders_text
        assert "insufficient_inventory" in orders_text or "inventory" in orders_text
    finally:
        ex.close()
