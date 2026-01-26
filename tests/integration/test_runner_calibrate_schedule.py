from __future__ import annotations

from pathlib import Path

import math
import json


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


def _write_manifest(run_dir: Path, symbol: str, yyyymmdd: str, run_id: str, config: dict) -> None:
    manifest = {
        "run_type": "calibrate_schedule",
        "symbol": symbol,
        "day": yyyymmdd,
        "run_id": run_id,
        "log_path": "out/logs/fake.log",
        "schedule_path": str(run_dir / "poisson_schedule.json"),
        "window_metrics_csv": str(run_dir / "window_metrics.csv"),
        "calibration_windows_root": str(run_dir / "calibration_windows"),
        "created_utc": run_id,
    }
    manifest.update(config)
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


def _write_schedule(run_dir: Path, schedule: list[dict]) -> None:
    (run_dir / "poisson_schedule.json").write_text(json.dumps(schedule, indent=2, sort_keys=True), encoding="utf-8")


def _touch_points(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("delta,fills,exposure_s\n", encoding="utf-8")


def test_resume_skips_completed_run(monkeypatch, tmp_path: Path) -> None:
    from mm.walkforward import runner_calibrate_schedule as r

    out_root = tmp_path / "out"
    symbol = "BTCUSDT"
    yyyymmdd = "20250101"
    run_id = "OLD"
    run_dir = out_root / "calibration" / "schedules" / symbol / f"{yyyymmdd}_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "tick_size": 0.01,
        "poisson_dt_ms": 100,
        "train_window_min": 2,
        "step_min": 1,
        "fit_method": "poisson_mle",
        "deltas": [1, 2, 3],
        "dwell_ms": 60_000,
        "mid_move_threshold_ticks": 2,
        "min_exposure_s": 5.0,
        "max_delta_ticks": 50,
        "fallback_policy": "carry_forward",
        "min_usable_deltas": 3,
        "quote_qty": 0.001,
        "maker_fee_rate": 0.001,
        "order_latency_ms": 50,
        "cancel_latency_ms": 25,
        "requote_interval_ms": 250,
        "initial_cash": 1000.0,
        "initial_inventory": 0.0,
        "calib_engine": "virtual",
    }
    _write_manifest(run_dir, symbol, yyyymmdd, run_id, config)

    schedule = []
    for i in range(3):
        start_ms = i * 60_000
        end_ms = (i + 1) * 60_000
        window_dir = run_dir / "calibration_windows" / f"train_{start_ms}_{end_ms}"
        _touch_points(window_dir / "calibration_points.csv")
        schedule.append({"start_ms": start_ms, "end_ms": end_ms, "calib_dir": str(window_dir)})
    _write_schedule(run_dir, schedule)

    monkeypatch.setattr(r, "_generate_run_id", lambda: "NEW")
    run_base, new_run_id, existing, resume_from_ms, resumed = r._resolve_run_base(
        out_root=out_root,
        symbol=symbol,
        yyyymmdd=yyyymmdd,
        resume_enabled=True,
        run_id_override=None,
        expected_config=config,
        day_end_ms=180_000,
    )

    assert resumed is False
    assert new_run_id == "NEW"
    assert existing == []
    assert resume_from_ms is None
    assert run_base.name == f"{yyyymmdd}_NEW"


def test_resume_continues_incomplete_run(monkeypatch, tmp_path: Path) -> None:
    from mm.walkforward import runner_calibrate_schedule as r

    out_root = tmp_path / "out"
    symbol = "BTCUSDT"
    yyyymmdd = "20250101"
    run_id = "OLD"
    run_dir = out_root / "calibration" / "schedules" / symbol / f"{yyyymmdd}_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "tick_size": 0.01,
        "poisson_dt_ms": 100,
        "train_window_min": 2,
        "step_min": 1,
        "fit_method": "poisson_mle",
        "deltas": [1, 2, 3],
        "dwell_ms": 60_000,
        "mid_move_threshold_ticks": 2,
        "min_exposure_s": 5.0,
        "max_delta_ticks": 50,
        "fallback_policy": "carry_forward",
        "min_usable_deltas": 3,
        "quote_qty": 0.001,
        "maker_fee_rate": 0.001,
        "order_latency_ms": 50,
        "cancel_latency_ms": 25,
        "requote_interval_ms": 250,
        "initial_cash": 1000.0,
        "initial_inventory": 0.0,
        "calib_engine": "virtual",
    }
    _write_manifest(run_dir, symbol, yyyymmdd, run_id, config)

    schedule = []
    for i in range(2):
        start_ms = i * 60_000
        end_ms = (i + 1) * 60_000
        window_dir = run_dir / "calibration_windows" / f"train_{start_ms}_{end_ms}"
        _touch_points(window_dir / "calibration_points.csv")
        schedule.append({"start_ms": start_ms, "end_ms": end_ms, "calib_dir": str(window_dir)})
    _write_schedule(run_dir, schedule)

    run_base, new_run_id, existing, resume_from_ms, resumed = r._resolve_run_base(
        out_root=out_root,
        symbol=symbol,
        yyyymmdd=yyyymmdd,
        resume_enabled=True,
        run_id_override=None,
        expected_config=config,
        day_end_ms=180_000,
    )

    assert resumed is True
    assert new_run_id == "OLD"
    assert run_base == run_dir
    assert len(existing) == 2
    assert resume_from_ms == 120_000


def test_resume_truncates_missing_window_artifacts(tmp_path: Path) -> None:
    from mm.walkforward import runner_calibrate_schedule as r

    out_root = tmp_path / "out"
    symbol = "BTCUSDT"
    yyyymmdd = "20250101"
    run_id = "OLD"
    run_dir = out_root / "calibration" / "schedules" / symbol / f"{yyyymmdd}_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "tick_size": 0.01,
        "poisson_dt_ms": 100,
        "train_window_min": 2,
        "step_min": 1,
        "fit_method": "poisson_mle",
        "deltas": [1, 2, 3],
        "dwell_ms": 60_000,
        "mid_move_threshold_ticks": 2,
        "min_exposure_s": 5.0,
        "max_delta_ticks": 50,
        "fallback_policy": "carry_forward",
        "min_usable_deltas": 3,
        "quote_qty": 0.001,
        "maker_fee_rate": 0.001,
        "order_latency_ms": 50,
        "cancel_latency_ms": 25,
        "requote_interval_ms": 250,
        "initial_cash": 1000.0,
        "initial_inventory": 0.0,
        "calib_engine": "virtual",
    }
    _write_manifest(run_dir, symbol, yyyymmdd, run_id, config)

    window_dir0 = run_dir / "calibration_windows" / "train_0_60000"
    _touch_points(window_dir0 / "calibration_points.csv")
    schedule = [
        {"start_ms": 0, "end_ms": 60_000, "calib_dir": str(window_dir0)},
        {"start_ms": 60_000, "end_ms": 120_000, "calib_dir": str(run_dir / "calibration_windows" / "train_60000_120000")},
    ]
    _write_schedule(run_dir, schedule)

    run_base, new_run_id, existing, resume_from_ms, resumed = r._resolve_run_base(
        out_root=out_root,
        symbol=symbol,
        yyyymmdd=yyyymmdd,
        resume_enabled=True,
        run_id_override=None,
        expected_config=config,
        day_end_ms=180_000,
    )

    assert resumed is True
    assert new_run_id == "OLD"
    assert run_base == run_dir
    assert len(existing) == 1
    assert resume_from_ms == 60_000


def test_resume_requires_matching_config(monkeypatch, tmp_path: Path) -> None:
    from mm.walkforward import runner_calibrate_schedule as r

    out_root = tmp_path / "out"
    symbol = "BTCUSDT"
    yyyymmdd = "20250101"
    run_id = "OLD"
    run_dir = out_root / "calibration" / "schedules" / symbol / f"{yyyymmdd}_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "tick_size": 0.01,
        "poisson_dt_ms": 100,
        "train_window_min": 2,
        "step_min": 1,
        "fit_method": "poisson_mle",
        "deltas": [1, 2, 3],
        "dwell_ms": 60_000,
        "mid_move_threshold_ticks": 2,
        "min_exposure_s": 5.0,
        "max_delta_ticks": 50,
        "fallback_policy": "carry_forward",
        "min_usable_deltas": 3,
        "quote_qty": 0.001,
        "maker_fee_rate": 0.001,
        "order_latency_ms": 50,
        "cancel_latency_ms": 25,
        "requote_interval_ms": 250,
        "initial_cash": 1000.0,
        "initial_inventory": 0.0,
        "calib_engine": "virtual",
    }
    mismatched = dict(config)
    mismatched["tick_size"] = 0.02
    _write_manifest(run_dir, symbol, yyyymmdd, run_id, mismatched)

    monkeypatch.setattr(r, "_generate_run_id", lambda: "NEW")
    run_base, new_run_id, existing, resume_from_ms, resumed = r._resolve_run_base(
        out_root=out_root,
        symbol=symbol,
        yyyymmdd=yyyymmdd,
        resume_enabled=True,
        run_id_override=None,
        expected_config=config,
        day_end_ms=180_000,
    )

    assert resumed is False
    assert new_run_id == "NEW"
    assert existing == []
    assert resume_from_ms is None
    assert run_base.name == f"{yyyymmdd}_NEW"


def test_resume_disabled_skips_old_run(monkeypatch, tmp_path: Path) -> None:
    from mm.walkforward import runner_calibrate_schedule as r

    out_root = tmp_path / "out"
    symbol = "BTCUSDT"
    yyyymmdd = "20250101"
    run_id = "OLD"
    run_dir = out_root / "calibration" / "schedules" / symbol / f"{yyyymmdd}_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "tick_size": 0.01,
        "poisson_dt_ms": 100,
        "train_window_min": 2,
        "step_min": 1,
        "fit_method": "poisson_mle",
        "deltas": [1, 2, 3],
        "dwell_ms": 60_000,
        "mid_move_threshold_ticks": 2,
        "min_exposure_s": 5.0,
        "max_delta_ticks": 50,
        "fallback_policy": "carry_forward",
        "min_usable_deltas": 3,
        "quote_qty": 0.001,
        "maker_fee_rate": 0.001,
        "order_latency_ms": 50,
        "cancel_latency_ms": 25,
        "requote_interval_ms": 250,
        "initial_cash": 1000.0,
        "initial_inventory": 0.0,
        "calib_engine": "virtual",
    }
    _write_manifest(run_dir, symbol, yyyymmdd, run_id, config)

    monkeypatch.setattr(r, "_generate_run_id", lambda: "NEW")
    run_base, new_run_id, existing, resume_from_ms, resumed = r._resolve_run_base(
        out_root=out_root,
        symbol=symbol,
        yyyymmdd=yyyymmdd,
        resume_enabled=False,
        run_id_override=None,
        expected_config=config,
        day_end_ms=180_000,
    )

    assert resumed is False
    assert new_run_id == "NEW"
    assert existing == []
    assert resume_from_ms is None
    assert run_base.name == f"{yyyymmdd}_NEW"


def test_resume_with_run_id_override_skips_old_run(monkeypatch, tmp_path: Path) -> None:
    from mm.walkforward import runner_calibrate_schedule as r

    out_root = tmp_path / "out"
    symbol = "BTCUSDT"
    yyyymmdd = "20250101"
    run_id = "OLD"
    run_dir = out_root / "calibration" / "schedules" / symbol / f"{yyyymmdd}_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "tick_size": 0.01,
        "poisson_dt_ms": 100,
        "train_window_min": 2,
        "step_min": 1,
        "fit_method": "poisson_mle",
        "deltas": [1, 2, 3],
        "dwell_ms": 60_000,
        "mid_move_threshold_ticks": 2,
        "min_exposure_s": 5.0,
        "max_delta_ticks": 50,
        "fallback_policy": "carry_forward",
        "min_usable_deltas": 3,
        "quote_qty": 0.001,
        "maker_fee_rate": 0.001,
        "order_latency_ms": 50,
        "cancel_latency_ms": 25,
        "requote_interval_ms": 250,
        "initial_cash": 1000.0,
        "initial_inventory": 0.0,
        "calib_engine": "virtual",
    }
    _write_manifest(run_dir, symbol, yyyymmdd, run_id, config)

    run_base, new_run_id, existing, resume_from_ms, resumed = r._resolve_run_base(
        out_root=out_root,
        symbol=symbol,
        yyyymmdd=yyyymmdd,
        resume_enabled=True,
        run_id_override="OVERRIDE",
        expected_config=config,
        day_end_ms=180_000,
    )

    assert resumed is False
    assert new_run_id == "OVERRIDE"
    assert existing == []
    assert resume_from_ms is None
    assert run_base.name == f"{yyyymmdd}_OVERRIDE"


def test_resume_missing_schedule_starts_fresh(tmp_path: Path) -> None:
    from mm.walkforward import runner_calibrate_schedule as r

    out_root = tmp_path / "out"
    symbol = "BTCUSDT"
    yyyymmdd = "20250101"
    run_id = "OLD"
    run_dir = out_root / "calibration" / "schedules" / symbol / f"{yyyymmdd}_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "tick_size": 0.01,
        "poisson_dt_ms": 100,
        "train_window_min": 2,
        "step_min": 1,
        "fit_method": "poisson_mle",
        "deltas": [1, 2, 3],
        "dwell_ms": 60_000,
        "mid_move_threshold_ticks": 2,
        "min_exposure_s": 5.0,
        "max_delta_ticks": 50,
        "fallback_policy": "carry_forward",
        "min_usable_deltas": 3,
        "quote_qty": 0.001,
        "maker_fee_rate": 0.001,
        "order_latency_ms": 50,
        "cancel_latency_ms": 25,
        "requote_interval_ms": 250,
        "initial_cash": 1000.0,
        "initial_inventory": 0.0,
        "calib_engine": "virtual",
    }
    _write_manifest(run_dir, symbol, yyyymmdd, run_id, config)

    run_base, new_run_id, existing, resume_from_ms, resumed = r._resolve_run_base(
        out_root=out_root,
        symbol=symbol,
        yyyymmdd=yyyymmdd,
        resume_enabled=True,
        run_id_override=None,
        expected_config=config,
        day_end_ms=180_000,
    )

    assert resumed is True
    assert new_run_id == "OLD"
    assert run_base == run_dir
    assert existing == []
    assert resume_from_ms is None


def test_resume_uses_last_usable_for_fallback(monkeypatch, tmp_path: Path) -> None:
    from mm.walkforward import runner_calibrate_schedule as r

    monkeypatch.setattr(r, "_read_day_time_bounds_ms", lambda *_args, **_kwargs: (0, 4 * 60_000))

    def _fake_window(**_kwargs):
        return {
            "usable": False,
            "reason": "no_usable_points",
            "n_deltas_usable": 0,
            "exposure_s_total": 0.0,
            "fills_total": 0,
        }

    monkeypatch.setattr(r, "_calibrate_poisson_window", lambda **kwargs: _fake_window(**kwargs))

    existing_schedule = [
        {
            "start_ms": 120_000,
            "end_ms": 180_000,
            "usable": True,
            "A": 2.5,
            "k": 1.1,
            "calib_dir": str(tmp_path / "calib" / "train_0_120000"),
            "dt_ms": 100,
            "tick_size": 0.01,
        },
        {
            "start_ms": 180_000,
            "end_ms": 240_000,
            "usable": False,
            "A": float("nan"),
            "k": float("nan"),
            "reason": "no_usable_points",
            "calib_dir": str(tmp_path / "calib" / "train_60000_180000"),
            "dt_ms": 100,
            "tick_size": 0.01,
        },
    ]

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
        day_bounds_ms=(0, 4 * 60_000),
        existing_schedule=existing_schedule,
        resume_from_ms=240_000,
    )

    assert len(schedule) == 4
    assert schedule[-1]["usable"] is False
    assert schedule[-1]["A"] == 2.5
    assert schedule[-1]["k"] == 1.1
