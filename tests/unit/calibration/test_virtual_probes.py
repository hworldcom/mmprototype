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


def test_virtual_probes_counts_and_exposure(monkeypatch, tmp_path: Path) -> None:
    """Virtual probes should count trade crosses and accrue exposure by tick time.

    We monkeypatch replay_day to avoid any market data files.
    """

    from mm.calibration import virtual_probes as vp

    # Build a deterministic replay:
    # t=0: depth -> mid=100
    # t=1000: depth (accrue 1s exposure)
    # t=1100: trade at 99 (bid hit for delta=1 tick)
    # t=2000: depth (accrue another 1s exposure)

    def _fake_replay_day(*, on_tick=None, on_trade=None, **_kwargs):
        e0 = _FakeEngine(_FakeLob(bids={99.0: 1.0}, asks={101.0: 1.0}))
        if on_tick:
            on_tick(0, e0)
            on_tick(1000, e0)
        if on_trade:
            on_trade(_Trade(recv_ms=1100, price=99.0, qty=0.1), e0)
        if on_tick:
            on_tick(2000, e0)
        return {"replay_done": True}

    monkeypatch.setattr(vp, "replay_day", _fake_replay_day)
    monkeypatch.setattr(vp, "Trade", _Trade)

    res = vp.run_virtual_ladder_window(
        data_root=tmp_path,
        symbol="BTCUSDT",
        yyyymmdd="20250101",
        tick_size=1.0,
        deltas=[1],
        dwell_ms=60_000,
        mid_move_threshold_ticks=None,
        time_min_ms=0,
        # Keep the window tight so the end-of-window tail exposure is deterministic.
        time_max_ms=2000,
        max_delta_ticks=10,
        min_exposure_s=0.5,
    )

    pts = res.points
    assert len(pts) == 1
    row = pts.iloc[0].to_dict()

    assert row["delta_bucket"] == 1
    # Exposure should be ~2 seconds from the two tick intervals.
    assert row["exposure_s"] == 2.0
    assert row["fill_events"] == 1
    assert row["bid_hits"] == 1
    assert row["ask_hits"] == 0
    assert row["usable"] is True