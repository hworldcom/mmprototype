from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class _FakeLob:
    bids: dict
    asks: dict


@dataclass
class _FakeEngine:
    lob: _FakeLob


@dataclass
class _Trade:
    recv_ms: int
    price: float
    qty: float
    side: str = "UNKNOWN"


def test_virtual_probes_exposure_sum_scales_with_num_deltas(monkeypatch, tmp_path: Path) -> None:
    """Regression test for simultaneous-delta exposure semantics.

    We expect each configured delta to accrue the full tick exposure for the window
    (subject to anchoring/validity rules). Therefore, the sum of exposures across
    deltas should scale with the number of deltas.
    """

    from mm.calibration import virtual_probes as vp

    # Deterministic replay: three tick intervals totaling 3 seconds.
    # - Tick at t=0 anchors mid.
    # - Tick at t=1000 => +1s exposure
    # - Tick at t=2000 => +1s exposure
    # - Tick at t=3000 => +1s exposure
    def _fake_replay_day(*, on_tick=None, on_trade=None, **_kwargs):
        e0 = _FakeEngine(_FakeLob(bids={99.0: 1.0}, asks={101.0: 1.0}))
        if on_tick:
            on_tick(0, e0)
            on_tick(1000, e0)
            on_tick(2000, e0)
            on_tick(3000, e0)
        # No trades needed; we only test exposure accounting.
        return {"replay_done": True}

    monkeypatch.setattr(vp, "replay_day", _fake_replay_day)
    monkeypatch.setattr(vp, "Trade", _Trade)

    deltas = [1, 2, 3]
    res = vp.run_virtual_ladder_window(
        data_root=tmp_path,
        symbol="BTCUSDT",
        yyyymmdd="20250101",
        tick_size=1.0,
        deltas=deltas,
        dwell_ms=60_000,
        mid_move_threshold_ticks=None,
        time_min_ms=0,
        time_max_ms=3000,
        max_delta_ticks=10,
        min_exposure_s=0.5,
    )

    pts = res.points.set_index("delta_bucket")
    assert set(pts.index) == set(deltas)

    # Window tick exposure is 3 seconds for each delta.
    for d in deltas:
        assert pts.loc[d, "exposure_s"] == 3.0

    # Sum exposure should scale with number of deltas.
    assert float(pts["exposure_s"].sum()) == 3.0 * len(deltas)
