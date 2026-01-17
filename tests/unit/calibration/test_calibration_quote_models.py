from mm.calibration.quotes.calibration_ladder import CalibrationLadderQuoteModel
from mm.calibration.quotes.fixed_spread import FixedSpreadQuoteModel
from mm.backtest.quotes.base import MarketState, PositionState


def test_fixed_spread_quotes_distance():
    m = MarketState(recv_ms=0, mid=100.0, best_bid=99.9, best_ask=100.1, spread=0.2)
    p = PositionState(inventory=0.0, cash=1000.0)
    model = FixedSpreadQuoteModel(qty=1.0, tick_size=0.1, delta_ticks=3)
    q = model.generate_quotes(m, p)
    assert len(q) == 2
    bid = next(x for x in q if x.side == "BUY")
    ask = next(x for x in q if x.side == "SELL")
    assert abs((m.mid - bid.price) - 0.3) < 1e-12
    assert abs((ask.price - m.mid) - 0.3) < 1e-12


def test_ladder_sweep_cycles_deltas():
    m0 = MarketState(recv_ms=0, mid=100.0, best_bid=99.9, best_ask=100.1, spread=0.2)
    p = PositionState(inventory=0.0, cash=1000.0)
    model = CalibrationLadderQuoteModel(qty=1.0, tick_size=0.1, deltas=[1, 2], dwell_ms=1000)

    q0 = model.generate_quotes(m0, p)
    bid0 = next(x for x in q0 if x.side == "BUY")
    assert abs(m0.mid - bid0.price - 0.1) < 1e-12

    # Still within dwell, should not change.
    m1 = MarketState(recv_ms=500, mid=100.0, best_bid=99.9, best_ask=100.1, spread=0.2)
    q1 = model.generate_quotes(m1, p)
    bid1 = next(x for x in q1 if x.side == "BUY")
    assert abs(m1.mid - bid1.price - 0.1) < 1e-12

    # After dwell, should advance to next delta.
    m2 = MarketState(recv_ms=1500, mid=100.0, best_bid=99.9, best_ask=100.1, spread=0.2)
    q2 = model.generate_quotes(m2, p)
    bid2 = next(x for x in q2 if x.side == "BUY")
    assert abs(m2.mid - bid2.price - 0.2) < 1e-12
