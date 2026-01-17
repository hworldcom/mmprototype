from __future__ import annotations

from pathlib import Path

import math


def test_build_schedule_fallback_and_gating(monkeypatch, tmp_path: Path) -> None:
    """Unit-test schedule logic without requiring market data files.

    We monkeypatch:
    - day bounds (so we don't need trades.csv)
    - window calibration (so we don't need to run backtests)
    """
    from mm.walkforward import runner_calibrate_schedule as r

    monkeypatch.setattr(r, "_read_day_time_bounds_ms", lambda *_args, **_kwargs: (0, 5 * 60_000))

    calls = {"n": 0}

    def _fake_window(**_kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return {
                "usable": True,
                "A": 2.0,
                "k": 1.0,
                "n_deltas_usable": 3,
                "exposure_s_total": 120.0,
                "fills_total": 12,
            }
        if calls["n"] == 2:
            return {
                "usable": False,
                "reason": "no_usable_points",
                "n_deltas_usable": 0,
                "exposure_s_total": 0.0,
                "fills_total": 0,
            }
        return {
            "usable": True,
            "A": 3.0,
            "k": 1.2,
            "n_deltas_usable": 3,
            "exposure_s_total": 200.0,
            "fills_total": 20,
        }

    monkeypatch.setattr(r, "_calibrate_poisson_window", lambda **kwargs: _fake_window(**kwargs))

    schedule = r.build_schedule(
        data_root=tmp_path,
        symbol="BTCUSDT",
        yyyymmdd="20250101",
        tick_size=0.01,
        quote_qty=0.001,
        maker_fee_rate=0.001,
        order_latency_ms=50,
        cancel_latency_ms=25,
        requote_interval_ms=250,
        initial_cash=1000.0,
        initial_inventory=0.0,
        train_window_min=2,
        step_min=1,
        deltas=[1, 2, 3],
        dwell_ms=60_000,
        mid_move_threshold_ticks=2,
        fit_method="poisson_mle",
        poisson_dt_ms=100,
        min_exposure_s=5.0,
        max_delta_ticks=50,
        calib_root=tmp_path / "calib",
        fallback_policy="carry_forward",
        min_usable_deltas=3,
    )

    assert len(schedule) == 3

    # 1st segment: good
    assert schedule[0]["usable"] is True
    assert schedule[0]["A"] == 2.0
    assert schedule[0]["k"] == 1.0

    # 2nd segment: carry-forward from last good
    assert schedule[1]["usable"] is False
    assert schedule[1]["A"] == 2.0
    assert schedule[1]["k"] == 1.0
    assert "FALLBACK_CARRY_FORWARD" in schedule[1]["reason"]

    # 3rd segment: good again
    assert schedule[2]["usable"] is True
    assert schedule[2]["A"] == 3.0
    assert schedule[2]["k"] == 1.2

    # Basic numeric sanity
    for seg in schedule:
        assert math.isfinite(seg["tick_size"])
        assert seg["dt_ms"] == 100
