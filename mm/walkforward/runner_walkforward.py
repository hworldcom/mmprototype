# mm/walkforward/runner_walkforward.py
from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd

from mm.backtest.backtester import backtest_day, BacktestRunStats
from mm.backtest.io import find_trades_file, iter_trades_csv
from mm.backtest.fills.trade_driven import TradeDrivenFillModel
from mm.backtest.fills.poisson import TimeVaryingPoissonFillModel
from mm.calibration.exposure import compute_bucketed_exposure
from mm.calibration.poisson_fit import fit_poisson_mle, fit_log_linear
from mm.calibration.quotes.calibration_ladder import CalibrationLadderQuoteModel
from mm.logging_config import setup_run_logging

log = logging.getLogger(__name__)


def _read_day_time_bounds_ms(data_root: Path, symbol: str, yyyymmdd: str) -> Tuple[int, int]:
    """Return (min_recv_ms, max_recv_ms) based on the trades stream.

    Trades are typically sparser than depth diffs and are sufficient for determining a usable day range.
    """
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


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")


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
) -> Dict[str, Any]:
    """Run a controlled ladder sweep in [time_min_ms, time_max_ms) and fit A,k."""
    _ensure_dir(out_dir)

    quote_model = CalibrationLadderQuoteModel(
        qty=quote_qty,
        tick_size=tick_size,
        deltas=deltas,
        dwell_ms=dwell_ms,
        mid_move_threshold_ticks=mid_move_threshold_ticks,
        two_sided=True,
    )

    stats: BacktestRunStats = backtest_day(
        root=data_root,
        symbol=symbol,
        yyyymmdd=yyyymmdd,
        out_dir=out_dir,
        time_min_ms=time_min_ms,
        time_max_ms=time_max_ms,
        quote_model_name="avellaneda_stoikov",
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
        initial_cash=initial_cash,
        initial_inventory=initial_inventory,
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

    fit_df = points[points["usable"]].copy()
    if fit_df.empty:
        return {"usable": False, "reason": "no_usable_points"}

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
    }
    _write_json(out_dir / "poisson_fit.json", fit_out)
    return fit_out


def main() -> None:
    ap = argparse.ArgumentParser(description="Walk-forward rolling calibration and continuous backtest (piecewise params)")
    ap.add_argument("--symbol", default=os.getenv("SYMBOL", "BTCUSDT"))
    ap.add_argument("--day", dest="yyyymmdd", default=os.getenv("DAY", ""), help="Trading day YYYYMMDD")
    ap.add_argument("--data-root", default=os.getenv("DATA_ROOT", "data"))
    ap.add_argument("--out-root", default=os.getenv("OUT_ROOT", "out"))

    # Strategy under test
    ap.add_argument("--quote-model", default=os.getenv("QUOTE_MODEL", "avellaneda_stoikov"))
    ap.add_argument("--quote-qty", type=float, default=float(os.getenv("QUOTE_QTY", "0.001")))
    ap.add_argument("--tick-size", type=float, default=float(os.getenv("TICK_SIZE", "0.01")))
    ap.add_argument("--qty-step", type=float, default=float(os.getenv("QTY_STEP", "0.0")))
    ap.add_argument("--min-notional", type=float, default=float(os.getenv("MIN_NOTIONAL", "0.0")))

    ap.add_argument("--maker-fee-rate", type=float, default=float(os.getenv("MAKER_FEE_RATE", "0.001")))
    ap.add_argument("--order-latency-ms", type=int, default=int(os.getenv("ORDER_LATENCY_MS", "50")))
    ap.add_argument("--cancel-latency-ms", type=int, default=int(os.getenv("CANCEL_LATENCY_MS", "25")))
    ap.add_argument("--requote-interval-ms", type=int, default=int(os.getenv("REQUOTE_INTERVAL_MS", "250")))
    ap.add_argument("--order-ttl-ms", type=int, default=int(os.getenv("ORDER_TTL_MS", "0")))
    ap.add_argument("--refresh-interval-ms", type=int, default=int(os.getenv("REFRESH_INTERVAL_MS", "0")))
    ap.add_argument("--initial-cash", type=float, default=float(os.getenv("INITIAL_CASH", "1000")))
    ap.add_argument("--initial-inventory", type=float, default=float(os.getenv("INITIAL_INVENTORY", "0")))

    # Walk-forward settings
    ap.add_argument("--train-window-min", type=int, default=int(os.getenv("WF_TRAIN_WINDOW_MIN", "120")))
    ap.add_argument("--step-min", type=int, default=int(os.getenv("WF_STEP_MIN", "15")))
    ap.add_argument("--fit-method", choices=["poisson_mle", "log_linear"], default=os.getenv("FIT_METHOD", "poisson_mle"))
    ap.add_argument("--poisson-dt-ms", type=int, default=int(os.getenv("POISSON_DT_MS", "100")))

    # Ladder calibration settings
    ap.add_argument("--deltas", default=os.getenv("CALIB_DELTAS", "1,2,3,5,8,13"))
    ap.add_argument("--dwell-ms", type=int, default=int(os.getenv("CALIB_DWELL_MS", "60000")))
    ap.add_argument("--mid-move-threshold-ticks", type=int, default=int(os.getenv("CALIB_MID_MOVE_THRESHOLD_TICKS", "2")))
    ap.add_argument("--min-exposure-s", type=float, default=float(os.getenv("MIN_EXPOSURE_S", "5.0")))
    ap.add_argument("--max-delta-ticks", type=int, default=int(os.getenv("MAX_DELTA_TICKS", "50")))

    args = ap.parse_args()
    if not args.yyyymmdd:
        raise SystemExit("--day (or DAY env var) is required (YYYYMMDD).")

    data_root = Path(args.data_root)
    out_root = Path(args.out_root)

    run_id = os.getenv("RUN_ID", datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"))
    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_root = os.getenv("LOG_ROOT", "out/logs")

    log_path = setup_run_logging(
        level=log_level,
        run_type="walkforward",
        symbol=args.symbol,
        yyyymmdd=args.yyyymmdd,
        run_id=run_id,
        base_dir=log_root,
    )
    log.info("Walk-forward start symbol=%s day=%s run_id=%s log_path=%s", args.symbol, args.yyyymmdd, run_id, log_path)

    # Time bounds
    day_start_ms, day_end_ms = _read_day_time_bounds_ms(data_root, args.symbol, args.yyyymmdd)

    train_ms = int(args.train_window_min * 60_000)
    step_ms = int(args.step_min * 60_000)

    # Build schedule
    schedule: List[Dict[str, Any]] = []
    calib_root = out_root / "walkforward" / "calibration_windows" / args.symbol / f"{args.yyyymmdd}_{run_id}"
    _ensure_dir(calib_root)

    # Start schedule after we have a full training window.
    t = day_start_ms + train_ms
    # Align t to step boundaries relative to day_start_ms (deterministic)
    offset = (t - day_start_ms) % step_ms
    if offset:
        t += (step_ms - offset)

    last_good: Optional[Dict[str, Any]] = None

    deltas = [int(x.strip()) for x in args.deltas.split(",") if x.strip()]
    if not deltas:
        raise SystemExit("--deltas must contain at least one integer")

    while t < day_end_ms:
        seg_start = int(t)
        seg_end = int(min(t + step_ms, day_end_ms))
        train_start = int(seg_start - train_ms)
        train_end = int(seg_start)

        window_dir = calib_root / f"train_{train_start}_{train_end}"
        fit = _calibrate_poisson_window(
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
            deltas=deltas,
            dwell_ms=args.dwell_ms,
            mid_move_threshold_ticks=(args.mid_move_threshold_ticks if args.mid_move_threshold_ticks > 0 else None),
            fit_method=args.fit_method,
            poisson_dt_ms=args.poisson_dt_ms,
            min_exposure_s=args.min_exposure_s,
            max_delta_ticks=args.max_delta_ticks,
            time_min_ms=train_start,
            time_max_ms=train_end,
            out_dir=window_dir,
        )

        if fit.get("usable"):
            last_good = fit
            A = float(fit["A"])
            k = float(fit["k"])
            usable = True
        else:
            # Fallback: carry forward last parameters (common in practice)
            usable = False
            if last_good is None:
                A, k = 1.0, 1.5
            else:
                A, k = float(last_good["A"]), float(last_good["k"])

        schedule.append(
            {
                "start_ms": seg_start,
                "end_ms": seg_end,
                "A": A,
                "k": k,
                "usable": usable,
                "train_start_ms": train_start,
                "train_end_ms": train_end,
                "calib_dir": str(window_dir),
            }
        )
        t += step_ms

    run_base = out_root / "walkforward" / "runs" / args.symbol / f"{args.yyyymmdd}_{run_id}"
    _ensure_dir(run_base)

    schedule_path = run_base / "poisson_schedule.json"
    _write_json(schedule_path, schedule)

    # Continuous backtest using the schedule
    backtest_dir = run_base / "backtest"
    _ensure_dir(backtest_dir)

    fill_model = TimeVaryingPoissonFillModel(
        schedule=schedule,
        tick_size=args.tick_size,
        dt_ms=args.poisson_dt_ms,
        seed=int(os.getenv("POISSON_SEED", "42")),
    )

    stats = backtest_day(
        root=data_root,
        symbol=args.symbol,
        yyyymmdd=args.yyyymmdd,
        out_dir=backtest_dir,
        quote_model_name=args.quote_model,
        fill_model_name="poisson_schedule",
        fill_model_override=fill_model,
        quote_qty=args.quote_qty,
        maker_fee_rate=args.maker_fee_rate,
        order_latency_ms=args.order_latency_ms,
        cancel_latency_ms=args.cancel_latency_ms,
        requote_interval_ms=args.requote_interval_ms,
        order_ttl_ms=(None if args.order_ttl_ms in (0, None) else int(args.order_ttl_ms)),
        refresh_interval_ms=(None if args.refresh_interval_ms in (0, None) else int(args.refresh_interval_ms)),
        tick_size=args.tick_size,
        qty_step=args.qty_step,
        min_notional=args.min_notional,
        initial_cash=args.initial_cash,
        initial_inventory=args.initial_inventory,
    )

    manifest = {
        "run_type": "walkforward",
        "symbol": args.symbol,
        "day": args.yyyymmdd,
        "run_id": run_id,
        "log_path": str(log_path),
        "train_window_min": args.train_window_min,
        "step_min": args.step_min,
        "tick_size": args.tick_size,
        "poisson_dt_ms": args.poisson_dt_ms,
        "schedule_path": str(schedule_path),
        "calibration_root": str(calib_root),
        "backtest_dir": str(backtest_dir),
        "backtest_stats": asdict(stats),
        "created_utc": run_id,
    }
    _write_json(run_base / "manifest.json", manifest)

    log.info("Walk-forward complete symbol=%s day=%s run_id=%s", args.symbol, args.yyyymmdd, run_id)
    log.info("Outputs schedule=%s manifest=%s backtest_dir=%s", schedule_path, run_base / "manifest.json", backtest_dir)


if __name__ == "__main__":
    main()
