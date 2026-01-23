# mm/walkforward/runner_calibrate_schedule.py
"""Mode B: rolling calibration that produces a Poisson parameter schedule ONLY.

This runner intentionally does *not* execute any quoting strategy backtest.
It produces a reusable calibration artifact (poisson_schedule.json) plus
window-level diagnostics for QA.

Typical usage:

  python -m mm.runner_calibrate_schedule --symbol BTCUSDT --day 20250101 \
    --tick-size 0.01 --train-window-min 120 --step-min 15 \
    --deltas 1,2,3,5,8,13 --dwell-ms 60000
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from mm.backtest.io import find_trades_file, iter_trades_csv
from mm.backtest.fills.trade_driven import TradeDrivenFillModel
from mm.backtest.backtester import backtest_day
from mm.calibration.exposure import compute_bucketed_exposure, summarize_bucketed_exposure
from mm.calibration.poisson_fit import fit_poisson_mle, fit_log_linear
from mm.calibration.quotes.calibration_ladder import CalibrationLadderQuoteModel
from mm.calibration.virtual_probes import run_virtual_ladder_window
from mm.logging_config import setup_run_logging

log = logging.getLogger(__name__)


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")


def _read_day_time_bounds_ms(data_root: Path, symbol: str, yyyymmdd: str) -> Tuple[int, int]:
    """Return (min_recv_ms, max_recv_ms) based on the trades stream."""
    p = find_trades_file(data_root, symbol, yyyymmdd)
    mn: Optional[int] = None
    mx: Optional[int] = None
    for tr in iter_trades_csv(p):
        ts = int(tr.recv_ms)
        mn = ts if mn is None else min(mn, ts)
        mx = ts if mx is None else max(mx, ts)
    if mn is None or mx is None:
        raise RuntimeError(f"No trades found in {p}")
    return mn, mx


def _calibrate_poisson_window(
    *,
    data_root: Path,
    symbol: str,
    yyyymmdd: str,
    tick_size: float,
    quote_qty: float,
    maker_fee_rate: float,
    order_latency_ms: int,
    cancel_latency_ms: int,
    requote_interval_ms: int,
    initial_cash: float,
    initial_inventory: float,
    deltas: List[int],
    dwell_ms: int,
    mid_move_threshold_ticks: Optional[int],
    fit_method: str,
    poisson_dt_ms: int,
    min_exposure_s: float,
    max_delta_ticks: int,
    time_min_ms: int,
    time_max_ms: int,
    out_dir: Path,
    calib_engine: str = "paper",
) -> Dict[str, Any]:
    """Run a controlled ladder sweep in [time_min_ms, time_max_ms) and fit A,k."""
    _ensure_dir(out_dir)

    calib_engine = str(calib_engine or "paper").strip().lower()
    if calib_engine not in {"paper", "virtual"}:
        raise ValueError(f"Unknown calib_engine={calib_engine!r} (expected 'paper' or 'virtual')")

    if calib_engine == "virtual":
        # Order-free virtual probes. Significantly cheaper than placing/cancelling
        # simulated orders.
        res = run_virtual_ladder_window(
            data_root=data_root,
            symbol=symbol,
            yyyymmdd=yyyymmdd,
            tick_size=tick_size,
            deltas=deltas,
            dwell_ms=dwell_ms,
            mid_move_threshold_ticks=mid_move_threshold_ticks,
            time_min_ms=time_min_ms,
            time_max_ms=time_max_ms,
            max_delta_ticks=max(max_delta_ticks, max(deltas)),
            min_exposure_s=min_exposure_s,
        )
        points = res.points
        points.to_csv(out_dir / "calibration_points.csv", index=False)
        summary = res.stats
        _write_json(out_dir / "virtual_probe_stats.json", summary)
    else:
        quote_model = CalibrationLadderQuoteModel(
            qty=quote_qty,
            tick_size=tick_size,
            deltas=deltas,
            dwell_ms=dwell_ms,
            mid_move_threshold_ticks=mid_move_threshold_ticks,
            two_sided=True,
        )

        # We backtest with a trade-driven fill model to observe "probe" fills.

        # Calibration runs need two-sided probes to be placeable even on spot.
        # If initial_inventory is 0 and PaperExchange is configured to suppress unfunded quotes,
        # the first SELL probe can be suppressed, biasing the ladder. We therefore apply
        # calibration-only "funding" floors that are comfortably above probe usage.
        calib_initial_cash = max(float(initial_cash or 0.0), 1e6)
        _calib_min_inv = float(quote_qty) * max(100.0, 10.0 * float(len(deltas)))
        calib_initial_inventory = max(float(initial_inventory or 0.0), _calib_min_inv)

        stats = backtest_day(
            root=data_root,
            symbol=symbol,
            yyyymmdd=yyyymmdd,
            out_dir=out_dir,
            time_min_ms=time_min_ms,
            time_max_ms=time_max_ms,
            quote_model_name="avellaneda_stoikov",  # ignored due to override
            fill_model_name="trade_driven",
            quote_model_override=quote_model,
            fill_model_override=TradeDrivenFillModel(allow_partial=True, max_fill_qty=1e18),
            quote_qty=quote_qty,
            maker_fee_rate=maker_fee_rate,
            order_latency_ms=order_latency_ms,
            cancel_latency_ms=cancel_latency_ms,
            requote_interval_ms=requote_interval_ms,
            order_ttl_ms=None,
            refresh_interval_ms=None,
            tick_size=tick_size,
            initial_cash=calib_initial_cash,
            initial_inventory=calib_initial_inventory,
        )

        points = compute_bucketed_exposure(
            orders_path=Path(stats.orders_path),
            fills_path=Path(stats.fills_path),
            state_path=Path(stats.state_path),
            tick_size=tick_size,
            min_delta_ticks=min(deltas),
            max_delta_ticks=max(max_delta_ticks, max(deltas)),
            min_exposure_s=min_exposure_s,
        )
        points.to_csv(out_dir / "calibration_points.csv", index=False)

        summary = summarize_bucketed_exposure(points)

    fit_df = points[points["usable"]].copy()
    if fit_df.empty:
        return {
            "usable": False,
            "reason": "no_usable_points",
            "dt_ms": int(poisson_dt_ms),
            "tick_size": float(tick_size),
            "train_start_ms": int(time_min_ms),
            "train_end_ms": int(time_max_ms),
            "exposure_s_total": float(summary["exposure_s_total"]),
            "fills_total": int(summary["fills_total"]),
            "fills_usable_total": int(summary.get("fills_usable_total", 0)),
            "n_deltas_usable": int(summary.get("n_deltas_usable", 0)),
            "calib_engine": calib_engine,
        }

    if fit_method == "log_linear":
        fit = fit_log_linear(fit_df)
    else:
        fit = fit_poisson_mle(fit_df)

    fit_out = {
        "usable": True,
        "fit_method": fit.method,
        "A": float(fit.A),
        "k": float(fit.k),
        "dt_ms": int(poisson_dt_ms),
        "tick_size": float(tick_size),
        "train_start_ms": int(time_min_ms),
        "train_end_ms": int(time_max_ms),
        "exposure_s_total": float(summary["exposure_s_total"]),
        "fills_total": int(summary["fills_total"]),
        "fills_usable_total": int(summary.get("fills_usable_total", 0)),
        "n_deltas_usable": int(summary.get("n_deltas_usable", 0)),
        "calib_engine": calib_engine,
    }
    _write_json(out_dir / "poisson_fit.json", fit_out)
    return fit_out



def _fmt_hhmm(ms: int) -> str:
    """Format a unix epoch timestamp in milliseconds as HH:MM (UTC)."""
    return datetime.utcfromtimestamp(ms / 1000.0).strftime('%H:%M')
def build_schedule(
    *,
    data_root: Path,
    symbol: str,
    yyyymmdd: str,
    tick_size: float,
    quote_qty: float,
    maker_fee_rate: float,
    order_latency_ms: int,
    cancel_latency_ms: int,
    requote_interval_ms: int,
    initial_cash: float,
    initial_inventory: float,
    train_window_min: int,
    step_min: int,
    deltas: List[int],
    dwell_ms: int,
    mid_move_threshold_ticks: Optional[int],
    fit_method: str,
    poisson_dt_ms: int,
    min_exposure_s: float,
    max_delta_ticks: int,
    calib_root: Path,
    fallback_policy: str,
    min_usable_deltas: int,
    calib_engine: str = "paper",
) -> List[Dict[str, Any]]:
    """Build a piecewise-constant Poisson schedule over the day.

    This is the core Mode-B artifact.
    """
    day_start_ms, day_end_ms = _read_day_time_bounds_ms(data_root, symbol, yyyymmdd)
    train_ms = int(train_window_min * 60_000)
    step_ms = int(step_min * 60_000)

    # Start schedule after we have a full training window.
    t = day_start_ms + train_ms
    offset = (t - day_start_ms) % step_ms
    if offset:
        t += (step_ms - offset)


    # Progress tracking (Mode B schedule-only).
    t0 = int(t)
    n_steps = int(((day_end_ms - t0) + step_ms - 1) // step_ms) if day_end_ms > t0 else 0
    step_idx = 0
    t_wall0 = time.time()
    schedule: List[Dict[str, Any]] = []
    last_good: Optional[Dict[str, Any]] = None

    while t < day_end_ms:
        seg_start = int(t)
        seg_end = int(min(t + step_ms, day_end_ms))
        train_start = int(seg_start - train_ms)
        train_end = int(seg_start)

        # Window header (useful for debugging + correlating with artifacts).
        log.info(
            "[CALIB] window=%d/%d seg=%s–%s train=%s–%s",
            step_idx + 1,
            n_steps,
            _fmt_hhmm(seg_start),
            _fmt_hhmm(seg_end),
            _fmt_hhmm(train_start),
            _fmt_hhmm(train_end),
        )

        window_dir = calib_root / f"train_{train_start}_{train_end}"
        fit = _calibrate_poisson_window(
            data_root=data_root,
            symbol=symbol,
            yyyymmdd=yyyymmdd,
            tick_size=tick_size,
            quote_qty=quote_qty,
            maker_fee_rate=maker_fee_rate,
            order_latency_ms=order_latency_ms,
            cancel_latency_ms=cancel_latency_ms,
            requote_interval_ms=requote_interval_ms,
            initial_cash=initial_cash,
            initial_inventory=initial_inventory,
            deltas=deltas,
            dwell_ms=dwell_ms,
            mid_move_threshold_ticks=mid_move_threshold_ticks,
            fit_method=fit_method,
            poisson_dt_ms=poisson_dt_ms,
            min_exposure_s=min_exposure_s,
            max_delta_ticks=max_delta_ticks,
            time_min_ms=train_start,
            time_max_ms=train_end,
            out_dir=window_dir,
            calib_engine=calib_engine,
        )

        # Guardrail: with simultaneous-delta virtual probes, total exposure should
        # roughly scale with n_deltas * window_length.
        window_s = float(train_end - train_start) / 1000.0
        expected_exposure_s = float(len(deltas)) * window_s
        observed_exposure_s = float(fit.get("exposure_s_total", 0.0) or 0.0)
        if expected_exposure_s > 0 and observed_exposure_s < 0.8 * expected_exposure_s:
            log.warning(
                "[CALIB] exposure_s_total suspiciously low: observed=%.1f expected≈%.1f (n_deltas=%d window_s=%.1f)",
                observed_exposure_s,
                expected_exposure_s,
                int(len(deltas)),
                window_s,
            )

        # Quality gate: even if fit returned usable, require enough distinct deltas.
        usable = bool(fit.get("usable")) and int(fit.get("n_deltas_usable", 0)) >= int(min_usable_deltas)
        reason = "OK" if usable else str(fit.get("reason", "insufficient_deltas"))

        if usable:
            last_good = fit
            A = float(fit["A"])
            k = float(fit["k"])
        else:
            if fallback_policy == "skip_segment":
                A, k = float("nan"), float("nan")
            elif fallback_policy == "global_default" or last_good is None:
                # Conservative defaults: low-ish intensity and moderate decay.
                A, k = 1.0, 1.5
            else:
                # carry_forward
                A, k = float(last_good["A"]), float(last_good["k"])
                reason = f"FALLBACK_CARRY_FORWARD::{reason}"

        # Window fit summary (high-signal debugging line)
        log.info(
            "[CALIB] fit train_end=%s usable=%s A=%.6g k=%.6g fills_usable=%s fills_total=%s exposure_s=%.1f n_deltas_usable=%s reason=%s",
            _fmt_hhmm(train_end),
            bool(usable),
            float(A),
            float(k),
            str(fit.get("fills_usable_total", fit.get("fills_total", 0))),
            str(fit.get("fills_total", 0)),
            float(fit.get("exposure_s_total", 0.0) or 0.0),
            str(fit.get("n_deltas_usable", 0)),
            str(reason),
        )

        schedule.append(
            {
                "start_ms": seg_start,
                "end_ms": seg_end,
                "A": A,
                "k": k,
                "usable": bool(usable),
                "reason": reason,
                "train_start_ms": train_start,
                "train_end_ms": train_end,
                "calib_dir": str(window_dir),
                "dt_ms": int(poisson_dt_ms),
                "tick_size": float(tick_size),
                "exposure_s_total": float(fit.get("exposure_s_total", 0.0)),
                "fills_total": int(fit.get("fills_total", 0)),
                "n_deltas_usable": int(fit.get("n_deltas_usable", 0)),
            }
        )

        # One-line progress summary per segment.
        step_idx += 1
        if n_steps > 0:
            pct = 100.0 * float(step_idx) / float(n_steps)
        else:
            pct = 100.0
        log.info(
            "[CALIB] %5.1f%% (%d/%d) %s–%s usable=%s A=%.6g k=%.6g reason=%s",
            pct,
            step_idx,
            n_steps,
            _fmt_hhmm(seg_start),
            _fmt_hhmm(seg_end),
            bool(usable),
            float(A),
            float(k),
            str(reason),
        )
        t += step_ms


    log.info("[CALIB] 100.0%% (%d/%d) completed in %.1fs", n_steps, n_steps, time.time() - t_wall0)
    return schedule


def main() -> None:
    ap = argparse.ArgumentParser(description="Rolling calibration that outputs a Poisson schedule (Mode B).")
    ap.add_argument("--symbol", default=os.getenv("SYMBOL", "BTCUSDT"))
    ap.add_argument("--day", dest="yyyymmdd", default=os.getenv("DAY", ""), help="Trading day YYYYMMDD")
    ap.add_argument("--data-root", default=os.getenv("DATA_ROOT", "data"))
    ap.add_argument("--out-root", default=os.getenv("OUT_ROOT", "out"))

    # Market conventions
    ap.add_argument("--tick-size", type=float, default=float(os.getenv("TICK_SIZE", "0.01")))

    # Walk-forward settings
    ap.add_argument("--train-window-min", type=int, default=int(os.getenv("WF_TRAIN_WINDOW_MIN", "120")))
    ap.add_argument("--step-min", type=int, default=int(os.getenv("WF_STEP_MIN", "15")))
    ap.add_argument("--fit-method", choices=["poisson_mle", "log_linear"], default=os.getenv("FIT_METHOD", "poisson_mle"))
    ap.add_argument("--poisson-dt-ms", type=int, default=int(os.getenv("POISSON_DT_MS", "100")))

    # Ladder calibration settings
    ap.add_argument("--quote-qty", type=float, default=float(os.getenv("QUOTE_QTY", "0.001")))
    ap.add_argument("--maker-fee-rate", type=float, default=float(os.getenv("MAKER_FEE_RATE", "0.001")))
    ap.add_argument("--order-latency-ms", type=int, default=int(os.getenv("ORDER_LATENCY_MS", "50")))
    ap.add_argument("--cancel-latency-ms", type=int, default=int(os.getenv("CANCEL_LATENCY_MS", "25")))
    ap.add_argument("--requote-interval-ms", type=int, default=int(os.getenv("REQUOTE_INTERVAL_MS", "250")))

    ap.add_argument(
        "--calib-engine",
        choices=["paper", "virtual"],
        default=os.getenv("CALIB_ENGINE", "paper"),
        help=(
            "Calibration execution engine. 'paper' uses PaperExchange and writes full orders/fills/state logs. "
            "'virtual' uses virtual probes (no order objects) and is significantly faster."
        ),
    )
    ap.add_argument("--initial-cash", type=float, default=float(os.getenv("INITIAL_CASH", "1000")))
    ap.add_argument("--initial-inventory", type=float, default=float(os.getenv("INITIAL_INVENTORY", "0")))

    ap.add_argument("--deltas", default=os.getenv("CALIB_DELTAS", "1,2,3,5,8,13"))
    ap.add_argument("--dwell-ms", type=int, default=int(os.getenv("CALIB_DWELL_MS", "60000")))
    ap.add_argument("--mid-move-threshold-ticks", type=int, default=int(os.getenv("CALIB_MID_MOVE_THRESHOLD_TICKS", "2")))
    ap.add_argument("--min-exposure-s", type=float, default=float(os.getenv("MIN_EXPOSURE_S", "5.0")))
    ap.add_argument("--max-delta-ticks", type=int, default=int(os.getenv("MAX_DELTA_TICKS", "50")))

    # Robustness knobs
    ap.add_argument(
        "--fallback-policy",
        choices=["carry_forward", "global_default", "skip_segment"],
        default=os.getenv("FALLBACK_POLICY", "carry_forward"),
        help="What to do when a segment cannot be calibrated.",
    )
    ap.add_argument("--min-usable-deltas", type=int, default=int(os.getenv("MIN_USABLE_DELTAS", "3")))

    args = ap.parse_args()
    if not args.yyyymmdd:
        raise SystemExit("--day (or DAY env var) is required (YYYYMMDD).")

    deltas = [int(x.strip()) for x in str(args.deltas).split(",") if x.strip()]
    if not deltas:
        raise SystemExit("--deltas must contain at least one integer")

    data_root = Path(args.data_root)
    out_root = Path(args.out_root)

    run_id = os.getenv("RUN_ID", datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"))
    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_root = os.getenv("LOG_ROOT", "out/logs")

    log_path = setup_run_logging(
        level=log_level,
        run_type="calibrate_schedule",
        symbol=args.symbol,
        yyyymmdd=args.yyyymmdd,
        run_id=run_id,
        base_dir=log_root,
    )
    log.info(
        "Calibration(schedule-only) start symbol=%s day=%s run_id=%s tick_size=%s",
        args.symbol,
        args.yyyymmdd,
        run_id,
        args.tick_size,
    )

    # Log all relevant configuration in a single, greppable line.
    log.info(
        "Calibration config: engine=%s symbol=%s day=%s tick=%.6g "
        "train_window_min=%s step_min=%s fit_method=%s poisson_dt_ms=%s "
        "deltas=%s dwell_ms=%s mid_move_threshold_ticks=%s min_exposure_s=%.6g max_delta_ticks=%s "
        "quote_qty=%.6g maker_fee_rate=%.6g order_latency_ms=%s cancel_latency_ms=%s requote_interval_ms=%s "
        "fallback_policy=%s min_usable_deltas=%s simultaneous_deltas=%s",
        args.calib_engine,
        args.symbol,
        args.yyyymmdd,
        float(args.tick_size),
        args.train_window_min,
        args.step_min,
        args.fit_method,
        args.poisson_dt_ms,
        deltas,
        args.dwell_ms,
        (args.mid_move_threshold_ticks if args.mid_move_threshold_ticks > 0 else None),
        float(args.min_exposure_s),
        args.max_delta_ticks,
        float(args.quote_qty),
        float(args.maker_fee_rate),
        args.order_latency_ms,
        args.cancel_latency_ms,
        args.requote_interval_ms,
        args.fallback_policy,
        args.min_usable_deltas,
        bool(str(args.calib_engine).strip().lower() == "virtual"),
    )

    run_base = out_root / "calibration" / "schedules" / args.symbol / f"{args.yyyymmdd}_{run_id}"
    calib_root = run_base / "calibration_windows"
    _ensure_dir(calib_root)

    schedule = build_schedule(
        data_root=data_root,
        symbol=args.symbol,
        yyyymmdd=args.yyyymmdd,
        tick_size=args.tick_size,
        quote_qty=args.quote_qty,
        maker_fee_rate=args.maker_fee_rate,
        order_latency_ms=args.order_latency_ms,
        cancel_latency_ms=args.cancel_latency_ms,
        requote_interval_ms=args.requote_interval_ms,
        initial_cash=args.initial_cash,
        initial_inventory=args.initial_inventory,
        train_window_min=args.train_window_min,
        step_min=args.step_min,
        deltas=deltas,
        dwell_ms=args.dwell_ms,
        mid_move_threshold_ticks=(args.mid_move_threshold_ticks if args.mid_move_threshold_ticks > 0 else None),
        fit_method=args.fit_method,
        poisson_dt_ms=args.poisson_dt_ms,
        min_exposure_s=args.min_exposure_s,
        max_delta_ticks=args.max_delta_ticks,
        calib_root=calib_root,
        fallback_policy=args.fallback_policy,
        min_usable_deltas=args.min_usable_deltas,
        calib_engine=args.calib_engine,
    )

    _ensure_dir(run_base)
    schedule_path = run_base / "poisson_schedule.json"
    _write_json(schedule_path, schedule)

    # Also write a flat CSV for notebook QA.
    df = pd.DataFrame(schedule)
    df.to_csv(run_base / "window_metrics.csv", index=False)

    manifest = {
        "run_type": "calibrate_schedule",
        "symbol": args.symbol,
        "day": args.yyyymmdd,
        "run_id": run_id,
        "log_path": str(log_path),
        "tick_size": args.tick_size,
        "poisson_dt_ms": args.poisson_dt_ms,
        "train_window_min": args.train_window_min,
        "step_min": args.step_min,
        "fit_method": args.fit_method,
        "deltas": deltas,
        "dwell_ms": args.dwell_ms,
        "mid_move_threshold_ticks": (args.mid_move_threshold_ticks if args.mid_move_threshold_ticks > 0 else None),
        "min_exposure_s": args.min_exposure_s,
        "max_delta_ticks": args.max_delta_ticks,
        "fallback_policy": args.fallback_policy,
        "min_usable_deltas": args.min_usable_deltas,
        "schedule_path": str(schedule_path),
        "window_metrics_csv": str(run_base / "window_metrics.csv"),
        "calibration_windows_root": str(calib_root),
        "created_utc": run_id,
    }
    _write_json(run_base / "manifest.json", manifest)

    usable_ratio = float(df["usable"].mean()) if not df.empty and "usable" in df else 0.0
    log.info("Calibration(schedule-only) complete. schedule=%s usable_ratio=%.2f", schedule_path, usable_ratio)


if __name__ == "__main__":
    main()
