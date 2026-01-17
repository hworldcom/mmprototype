from mm.backtest.fills.poisson import PoissonFillModel, TimeVaryingPoissonFillModel
from mm.backtest.fills.base import OpenOrder
from mm.backtest.quotes.base import MarketState


def test_poisson_uses_tick_distance_and_dt_ms():
    # With tick_size=1, delta_ticks = abs(price-mid).
    model = PoissonFillModel(A=1000.0, k=0.0, tick_size=1.0, dt_ms=1000, seed=1)
    o = OpenOrder(
        order_id="o1",
        side="BUY",
        price=100.0,
        qty=1.0,
        placed_recv_ms=0,
        active_recv_ms=0,
        expire_recv_ms=None,
    )
    market = MarketState(recv_ms=0, mid=100.0, best_bid=99.0, best_ask=101.0, spread=2.0)
    fills = model.on_tick(market, [o])
    assert len(fills) == 1  # p ~= 1-exp(-1000*1)=~1


def test_time_varying_poisson_only_active_in_segment():
    schedule = [
        {"start_ms": 1000, "end_ms": 2000, "A": 1000.0, "k": 0.0},
        {"start_ms": 2000, "end_ms": 3000, "A": 0.0, "k": 0.0},
    ]
    model = TimeVaryingPoissonFillModel(schedule=schedule, tick_size=1.0, dt_ms=1000, seed=1)

    o = OpenOrder(
        order_id="o1",
        side="BUY",
        price=100.0,
        qty=1.0,
        placed_recv_ms=0,
        active_recv_ms=0,
        expire_recv_ms=None,
    )
    market_before = MarketState(recv_ms=900, mid=100.0, best_bid=99.0, best_ask=101.0, spread=2.0)
    assert model.on_tick(market_before, [o]) == []

    market_active = MarketState(recv_ms=1500, mid=100.0, best_bid=99.0, best_ask=101.0, spread=2.0)
    assert len(model.on_tick(market_active, [o])) == 1

    market_inactive = MarketState(recv_ms=2500, mid=100.0, best_bid=99.0, best_ask=101.0, spread=2.0)
    assert model.on_tick(market_inactive, [o]) == []
