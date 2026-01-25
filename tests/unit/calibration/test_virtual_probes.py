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


def test_virtual_probes_exposure_accrues_for_all_deltas(monkeypatch, tmp_path: Path) -> None:
    """Exposure should accrue independently for each configured delta.

    With multiple deltas configured, each delta should receive the full tick
    exposure for the window (subject to anchoring/validity rules), rather than
    splitting exposure across deltas.
    """

    from mm.calibration import virtual_probes as vp

    def _fake_replay_day(*, on_tick=None, on_trade=None, **_kwargs):
        e0 = _FakeEngine(_FakeLob(bids={99.0: 1.0}, asks={101.0: 1.0}))
        if on_tick:
            on_tick(0, e0)
            on_tick(1000, e0)
        if on_trade:
            # Trade at 99 hits delta=1 bid (mid=100 => bid_probe(1)=99), but not delta=2.
            on_trade(_Trade(recv_ms=1100, price=99.0, qty=0.1), e0)
            # Trade at 97 hits both delta=1 and delta=2 bids.
            on_trade(_Trade(recv_ms=1200, price=97.0, qty=0.2), e0)
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
        deltas=[1, 2],
        dwell_ms=60_000,
        mid_move_threshold_ticks=None,
        time_min_ms=0,
        time_max_ms=2000,
        max_delta_ticks=10,
        min_exposure_s=0.5,
    )

    pts = res.points.set_index("delta_bucket")
    assert set(pts.index) == {1, 2}

    # Each delta accrues the full 2 seconds of tick exposure.
    assert pts.loc[1, "exposure_s"] == 2.0
    assert pts.loc[2, "exposure_s"] == 2.0

    # delta=1: both trades cross bid => 2 events
    assert int(pts.loc[1, "fill_events"]) == 2
    assert int(pts.loc[1, "bid_hits"]) == 2

    # delta=2: only the 97 trade crosses bid => 1 event
    assert int(pts.loc[2, "fill_events"]) == 1
    assert int(pts.loc[2, "bid_hits"]) == 1
    assert int(pts.loc[2, "ask_hits"]) == 0
    assert bool(pts.loc[2, "usable"]) is True

    assert int(pts.loc[1, "ask_hits"]) == 0
    assert bool(pts.loc[1, "usable"]) is True


def test_virtual_probes_passes_replay_buffers(monkeypatch, tmp_path: Path) -> None:
    from mm.calibration import virtual_probes as vp

    sentinel = object()
    seen = {"replay_buffers": None}

    def _fake_replay_day(*, replay_buffers=None, **_kwargs):
        seen["replay_buffers"] = replay_buffers
        return {"replay_done": True}

    monkeypatch.setattr(vp, "replay_day", _fake_replay_day)

    vp.run_virtual_ladder_window(
        data_root=tmp_path,
        symbol="BTCUSDT",
        yyyymmdd="20250101",
        tick_size=1.0,
        deltas=[1],
        dwell_ms=60_000,
        mid_move_threshold_ticks=None,
        time_min_ms=0,
        time_max_ms=1000,
        max_delta_ticks=10,
        min_exposure_s=0.0,
        replay_buffers=sentinel,
    )

    assert seen["replay_buffers"] is sentinel
