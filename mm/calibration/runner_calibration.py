"""Calibration runner.

This module provides two calibration designs for Poisson fill parameters:

Design A (ladder sweep):
  - one backtest run with a `CalibrationLadderQuoteModel`
  - output is bucketed by delta ticks

Design B (fixed spread runs):
  - multiple backtest runs, one per fixed delta
  - output is one point per delta

Both designs:
  - run with FILL_MODEL=trade_driven (recommended)
  - fit A,k to lambda(delta)=A*exp(-k*delta)
  - emit `poisson_fit.json` consumable by backtests
"""

from __future__ import annotations

import argparse
import json
import os
import logging
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd

from mm.backtest.backtester import backtest_day
from mm.calibration.exposure import compute_bucketed_exposure, compute_run_level_exposure
from mm.calibration.poisson_fit import fit_log_linear, fit_poisson_mle
from mm.calibration.quotes.calibration_ladder import CalibrationLadderQuoteModel
from mm.calibration.quotes.fixed_spread import FixedSpreadQuoteModel
from mm.logging_config import setup_run_logging


def _parse_int_list(s: str) -> List[int]:
    return [int(x.strip()) for x in s.split(",") if x.strip()]


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Calibrate Poisson fill parameters (A,k) from controlled quoting runs")
    ap.add_argument("--symbol", default=os.getenv("SYMBOL", "BTCUSDT"))
    ap.add_argument("--day", dest="yyyymmdd", default=os.getenv("DAY", ""), help="Trading day YYYYMMDD")
    ap.add_argument("--method", choices=["ladder", "fixed"], default=os.getenv("CALIB_METHOD", "ladder"))
    ap.add_argument("--deltas", default=os.getenv("CALIB_DELTAS", "1,2,3,5,8,13"))
    ap.add_argument("--dwell-ms", type=int, default=int(os.getenv("CALIB_DWELL_MS", "60000")))
    ap.add_argument("--mid-move-threshold-ticks", type=int, default=int(os.getenv("CALIB_MID_MOVE_THRESHOLD_TICKS", "2")))
    ap.add_argument("--out-root", default=os.getenv("OUT_ROOT", "out"))
    ap.add_argument("--data-root", default=os.getenv("DATA_ROOT", "data"))
    ap.add_argument("--tick-size", type=float, default=float(os.getenv("TICK_SIZE", "0.01")))
    ap.add_argument("--quote-qty", type=float, default=float(os.getenv("QUOTE_QTY", "0.001")))
    ap.add_argument("--maker-fee-rate", type=float, default=float(os.getenv("MAKER_FEE_RATE", "0.001")))
    # IMPORTANT: Calibration runs need non-zero balances, otherwise BalanceAwareQuoteModel
    # will filter out all quotes and you will see 0 orders/fills.
    ap.add_argument("--initial-cash", type=float, default=float(os.getenv("INITIAL_CASH", "1000")))
    ap.add_argument("--initial-inventory", type=float, default=float(os.getenv("INITIAL_INVENTORY", "0")))
    ap.add_argument("--order-latency-ms", type=int, default=int(os.getenv("ORDER_LATENCY_MS", "50")))
    ap.add_argument("--cancel-latency-ms", type=int, default=int(os.getenv("CANCEL_LATENCY_MS", "25")))
    ap.add_argument("--requote-interval-ms", type=int, default=int(os.getenv("REQUOTE_INTERVAL_MS", "250")))
    ap.add_argument("--fit-method", choices=["poisson_mle", "log_linear"], default=os.getenv("FIT_METHOD", "poisson_mle"))
    ap.add_argument("--poisson-dt-ms", type=int, default=int(os.getenv("POISSON_DT_MS", "100")))
    ap.add_argument("--min-exposure-s", type=float, default=float(os.getenv("MIN_EXPOSURE_S", "5.0")))
    ap.add_argument("--max-delta-ticks", type=int, default=int(os.getenv("MAX_DELTA_TICKS", "50")))

    args = ap.parse_args()
    if not args.yyyymmdd:
        raise SystemExit("--day (or DAY env var) is required (YYYYMMDD).")

    deltas = _parse_int_list(args.deltas)
    if not deltas:
        raise SystemExit("--deltas must contain at least one integer delta")

    # Run-scoped logging for calibration (batch job).
    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_root = os.getenv("LOG_ROOT", "out/logs")
    run_id = os.getenv("RUN_ID", datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"))
    log_path = setup_run_logging(
        level=log_level,
        run_type="calibration",
        method=args.method,
        symbol=args.symbol,
        yyyymmdd=args.yyyymmdd,
        run_id=run_id,
        base_dir=log_root,
    )
    logger = logging.getLogger(__name__)
    logger.info(
        "Calibration run start symbol=%s day=%s method=%s run_id=%s log_path=%s",
        args.symbol,
        args.yyyymmdd,
        args.method,
        run_id,
        log_path,
    )

    ts = run_id
    out_base = Path(args.out_root) / "calibration" / args.method / args.symbol / f"{args.yyyymmdd}_{ts}"
    _ensure_dir(out_base)

    # Always recommend calibrating against trade-driven fills.
    fill_model_name = os.getenv("FILL_MODEL", "trade_driven")
    if fill_model_name.lower() != "trade_driven":
        print("WARNING: Calibration is typically run with FILL_MODEL=trade_driven.")

    run_manifest = {
        "symbol": args.symbol,
        "day": args.yyyymmdd,
        "method": args.method,
        "run_id": run_id,
        "log_path": str(log_path),
        "deltas": deltas,
        "tick_size": args.tick_size,
        "quote_qty": args.quote_qty,
        "fit_method": args.fit_method,
        "poisson_dt_ms": args.poisson_dt_ms,
        "created_utc": ts,
        "runs": [],
    }

    if args.method == "ladder":
        run_dir = out_base / "run"
        _ensure_dir(run_dir)

        quote_model = CalibrationLadderQuoteModel(
            qty=args.quote_qty,
            tick_size=args.tick_size,
            deltas=deltas,
            dwell_ms=args.dwell_ms,
            mid_move_threshold_ticks=args.mid_move_threshold_ticks if args.mid_move_threshold_ticks > 0 else None,
            two_sided=True,
        )

        stats = backtest_day(
            root=Path(args.data_root),
            symbol=args.symbol,
            yyyymmdd=args.yyyymmdd,
            out_dir=run_dir,
            quote_model_name="avellaneda_stoikov",  # ignored by override
            fill_model_name=fill_model_name,
            quote_model_override=quote_model,
            quote_qty=args.quote_qty,
            maker_fee_rate=args.maker_fee_rate,
            order_latency_ms=args.order_latency_ms,
            cancel_latency_ms=args.cancel_latency_ms,
            requote_interval_ms=args.requote_interval_ms,
            order_ttl_ms=None,
            refresh_interval_ms=None,
            tick_size=args.tick_size,
            initial_cash=args.initial_cash,
            initial_inventory=args.initial_inventory,
        )
        run_manifest["runs"].append({"type": "ladder", "dir": str(run_dir), "stats": asdict(stats)})

        points = compute_bucketed_exposure(
            orders_path=Path(stats.orders_path),
            fills_path=Path(stats.fills_path),
            state_path=Path(stats.state_path),
            tick_size=args.tick_size,
            min_delta_ticks=min(deltas),
            max_delta_ticks=max(args.max_delta_ticks, max(deltas)),
            min_exposure_s=args.min_exposure_s,
        )

        points_path = out_base / "calibration_points.csv"
        points.to_csv(points_path, index=False)

        fit_df = points[points["usable"]].copy()
        if args.fit_method == "log_linear":
            fit = fit_log_linear(fit_df)
        else:
            fit = fit_poisson_mle(fit_df)

    else:
        runs_root = out_base / "runs"
        _ensure_dir(runs_root)
        rows = []
        for d in deltas:
            run_dir = runs_root / f"delta_{d}"
            _ensure_dir(run_dir)
            quote_model = FixedSpreadQuoteModel(qty=args.quote_qty, tick_size=args.tick_size, delta_ticks=int(d))

            stats = backtest_day(
                root=Path(args.data_root),
                symbol=args.symbol,
                yyyymmdd=args.yyyymmdd,
                out_dir=run_dir,
                quote_model_name="avellaneda_stoikov",  # ignored by override
                fill_model_name=fill_model_name,
                quote_model_override=quote_model,
                quote_qty=args.quote_qty,
                maker_fee_rate=args.maker_fee_rate,
                order_latency_ms=args.order_latency_ms,
                cancel_latency_ms=args.cancel_latency_ms,
                requote_interval_ms=args.requote_interval_ms,
                order_ttl_ms=None,
                refresh_interval_ms=None,
                tick_size=args.tick_size,
                initial_cash=args.initial_cash,
                initial_inventory=args.initial_inventory,
            )

            run_manifest["runs"].append({"type": "fixed", "delta": d, "dir": str(run_dir), "stats": asdict(stats)})

            rows.append(
                compute_run_level_exposure(
                    orders_path=Path(stats.orders_path),
                    fills_path=Path(stats.fills_path),
                    state_path=Path(stats.state_path),
                    tick_size=args.tick_size,
                    expected_delta_ticks=int(d),
                )
            )

        points = pd.DataFrame(rows)
        points["delta_bucket"] = points["delta_expected"].astype(int)
        points["usable"] = (points["exposure_s"] >= float(args.min_exposure_s))

        points_path = out_base / "calibration_points.csv"
        points.to_csv(points_path, index=False)

        fit_df = points[points["usable"]].copy()
        # For fixed runs we treat each run as a bucket.
        if args.fit_method == "log_linear":
            fit = fit_log_linear(fit_df, delta_col="delta_bucket", lambda_col="lambda_events_per_s")
        else:
            fit = fit_poisson_mle(fit_df, delta_col="delta_bucket", count_col="fill_events")

    fit_out = {
        "symbol": args.symbol,
        "day": args.yyyymmdd,
        "distance_unit": "ticks",
        "fit_method": fit.method,
        "A": float(fit.A),
        "k": float(fit.k),
        "dt_ms": int(args.poisson_dt_ms),
        "calibration_method": args.method,
        "deltas": deltas,
        "tick_size": float(args.tick_size),
        "min_exposure_s": float(args.min_exposure_s),
        "created_utc": ts,
    }

    _write_json(out_base / "poisson_fit.json", fit_out)
    _write_json(out_base / "run_manifest.json", run_manifest)

    logger.info("Calibration run complete symbol=%s day=%s method=%s run_id=%s", args.symbol, args.yyyymmdd, args.method, run_id)
    logger.info("Outputs points=%s fit=%s manifest=%s", out_base / "calibration_points.csv", out_base / "poisson_fit.json", out_base / "run_manifest.json")

    print("Calibration complete")
    print("Output directory:", out_base)
    print("Suggested backtest settings:")
    print("  export FILL_MODEL=poisson")
    print(f"  export FILL_PARAMS_FILE={out_base / 'poisson_fit.json'}")


if __name__ == "__main__":
    main()
