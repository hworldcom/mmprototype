from __future__ import annotations

from pathlib import Path

import pytest

from mm.backtest.paper_exchange import BacktestConfig, PaperExchange
from mm.backtest.quotes.base import Quote, MarketState
from mm.backtest.fills.base import FillModel, OpenOrder
from mm.backtest.strategy_snapshot import StrategySnapshot


class NoFillModel(FillModel):
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


def test_paper_exchange_accepts_snapshot_based_quote_model(tmp_path: Path):
    """A strategy that only implements generate_quotes(snapshot) should work end-to-end."""

    class SnapshotOnlyQuoteModel:
        def __init__(self):
            self.seen = []

        def generate_quotes(self, snapshot: StrategySnapshot):
            # record that we were invoked via snapshot path
            self.seen.append((snapshot.recv_ms, snapshot.recv_seq, snapshot.market.mid))
            mid = snapshot.market.mid
            return [
                Quote(side="BUY", price=mid - 1.0, qty=0.01),
                Quote(side="SELL", price=mid + 1.0, qty=0.01),
            ]

    qm = SnapshotOnlyQuoteModel()

    cfg = BacktestConfig(
        initial_cash=1_000.0,
        initial_inventory=0.05,
        quote_interval_ms=0,  # always quote when called
        order_latency_ms=0,
        cancel_latency_ms=0,
        tick_size=0.01,
        qty_step=0.0,
    )

    ex = PaperExchange(
        symbol="T",
        out_dir=tmp_path,
        cfg=cfg,
        quote_model=qm,        # type: ignore[arg-type]
        fill_model=NoFillModel(),
    )

    market = _mk_market(100, 50.0)
    snapshot = StrategySnapshot(
        recv_ms=market.recv_ms,
        recv_seq=123,
        market=market,
        position=ex.position(),
        book_topn=None,
    )

    ex.on_snapshot(snapshot)

    # Quote model invoked and produced two open orders.
    assert qm.seen == [(100, 123, 50.0)]
    assert len(ex.open_orders) == 2

    ex.close()


def test_backtester_builds_snapshot_and_calls_on_snapshot(monkeypatch, tmp_path: Path):
    """Ensure backtester builds StrategySnapshot (incl. book_topn) and calls exch.on_snapshot."""
    import mm.backtest.backtester as bt

    class CapturingExchange:
        last_instance = None

        def __init__(self, *args, **kwargs):
            CapturingExchange.last_instance = self
            self.snapshots = []
            # minimal fields accessed by backtester on return
            self.fills_path = tmp_path / "fills.csv"
            self.orders_path = tmp_path / "orders.csv"
            self.state_path = tmp_path / "state.csv"

        def on_snapshot(self, snapshot: StrategySnapshot):
            self.snapshots.append(snapshot)

        def on_trade(self, *args, **kwargs):
            return None

        def close(self):
            return None

    
def fake_replay_day(*, on_tick, on_trade, **kwargs):
    # Create a minimal dummy engine and trigger one tick.
    class DummyLOB:
        def top_n(self, n: int):
            bids = [(99.0, 1.0), (98.0, 2.0)][:n]
            asks = [(101.0, 1.5), (102.0, 2.5)][:n]
            return bids, asks

    class DummyBook:
        def topn_levels(self, n: int):
            bids = [(99.0, 1.0), (98.0, 2.0)][:n]
            asks = [(101.0, 1.5), (102.0, 2.5)][:n]
            return bids, asks

    class DummyEngine:
        def __init__(self):
            self.last_recv_seq = 777
            self.lob = DummyLOB()
            self.book = DummyBook()

    engine = DummyEngine()
    on_tick(500, engine)
    return type("Stats", (), {"__dict__": {"ticks": 1}})()

    monkeypatch.setattr(bt, "PaperExchange", CapturingExchange)
    monkeypatch.setattr(bt, "replay_day", fake_replay_day)

    stats = bt.backtest_day(
        root=Path("."),
        symbol="T",
        yyyymmdd="20990101",
        out_dir=tmp_path,
        quote_model_override=object(),  # not used by CapturingExchange
        fill_model_override=object(),
    )

    assert stats.replay["ticks"] == 1

    ex = CapturingExchange.last_instance
    assert ex is not None
    assert len(ex.snapshots) == 1
    snap = ex.snapshots[0]
    assert snap.recv_ms == 500
    assert snap.recv_seq == 777
    assert snap.book_topn is not None
    assert [lvl.price for lvl in snap.book_topn.bids] == [99.0, 98.0]
    assert [lvl.price for lvl in snap.book_topn.asks] == [101.0, 102.0]
