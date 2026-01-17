from __future__ import annotations

import csv
from pathlib import Path

import pytest

from mm.backtest.paper_exchange import BacktestConfig, PaperExchange
from mm.backtest.quotes.base import QuoteModel, Quote, MarketState, PositionState
from mm.backtest.fills.base import FillModel, Fill, OpenOrder


class StaticQuoteModel(QuoteModel):
    """Always returns the same bid/ask quotes."""

    def __init__(self, bid_px: float, ask_px: float, qty: float):
        self.bid_px = bid_px
        self.ask_px = ask_px
        self.qty = qty

    def generate_quotes(self, market: MarketState, position: PositionState):
        return [
            Quote(side="BUY", price=self.bid_px, qty=self.qty, ttl_ms=None),
            Quote(side="SELL", price=self.ask_px, qty=self.qty, ttl_ms=None),
        ]


class NoFillModel(FillModel):
    """Never generates fills."""

    def on_tick(self, market: MarketState, open_orders: list[OpenOrder]):
        return []


def _mk_market(recv_ms: int, mid: float) -> MarketState:
    best_bid = mid - 0.5
    best_ask = mid + 0.5
    return MarketState(
        recv_ms=recv_ms,
        mid=mid,
        best_bid=best_bid,
        best_ask=best_ask,
        spread=best_ask - best_bid,
        imbalance=None,
    )


def test_gtc_no_expire_and_no_churn_when_quotes_unchanged(tmp_path: Path):
    cfg = BacktestConfig(
        initial_cash=10_000.0,
        initial_inventory=1.0,
        order_ttl_ms=None,  # GTC
        refresh_interval_ms=None,
        quote_interval_ms=100,
        only_requote_on_change=True,
        tick_size=0.1,
        qty_step=0.0,
    )

    qm = StaticQuoteModel(bid_px=99.9, ask_px=100.1, qty=0.1)
    fm = NoFillModel()
    ex = PaperExchange(cfg=cfg, quote_model=qm, fill_model=fm, out_dir=tmp_path, symbol="T")

    # First tick: should place both sides
    ex.on_tick(_mk_market(0, 100.0))
    assert len(ex.open_orders) == 2
    order_ids = set(ex.open_orders.keys())

    # Several subsequent ticks: quotes unchanged => should keep the same order ids
    ex.on_tick(_mk_market(100, 100.0))
    ex.on_tick(_mk_market(200, 100.0))
    ex.on_tick(_mk_market(300, 100.0))
    assert set(ex.open_orders.keys()) == order_ids

    ex.close()

    # Orders CSV should not contain any EXPIRE actions under GTC.
    orders = list(csv.DictReader((tmp_path / "orders_T.csv").open()))
    assert all(r["action"] != "EXPIRE" for r in orders)
    # expire_recv_ms should remain empty
    assert all((r["expire_recv_ms"] or "") == "" for r in orders)


class OneShotFillModel(FillModel):
    """Fills the first open BUY order completely at the first opportunity."""

    def __init__(self):
        self._done = False

    def on_tick(self, market: MarketState, open_orders: list[OpenOrder]):
        if self._done:
            return []
        buys = [o for o in open_orders if o.side.upper() == "BUY"]
        if not buys:
            return []
        self._done = True
        o = buys[0]
        return [Fill(order_id=o.order_id, recv_ms=market.recv_ms, price=o.price, qty=o.qty, reason="test")]


def test_close_filled_status_logged_as_filled(tmp_path: Path):
    cfg = BacktestConfig(
        initial_cash=10_000.0,
        initial_inventory=0.0,
        order_ttl_ms=None,
        refresh_interval_ms=None,
        quote_interval_ms=100,
        only_requote_on_change=True,
    )

    qm = StaticQuoteModel(bid_px=100.0, ask_px=101.0, qty=0.5)
    fm = OneShotFillModel()
    ex = PaperExchange(cfg=cfg, quote_model=qm, fill_model=fm, out_dir=tmp_path, symbol="T")

    ex.on_tick(_mk_market(0, 100.5))
    ex.on_tick(_mk_market(200, 100.5))  # triggers fill
    ex.close()

    rows = list(csv.DictReader((tmp_path / "orders_T.csv").open()))
    close_rows = [r for r in rows if r["action"] == "CLOSE_FILLED"]
    assert close_rows, "Expected a CLOSE_FILLED row"
    assert all(r["status"] == "FILLED" for r in close_rows)
