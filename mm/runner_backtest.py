# mm/runner_backtest.py

import os
import json
import logging
from datetime import datetime
from pathlib import Path

from mm.backtest.backtester import backtest_day
from mm.logging_config import setup_run_logging


def _env_float(key: str, default: float) -> float:
    v = os.getenv(key, "")
    return float(v) if v else default


def _env_int(key: str, default: int) -> int:
    v = os.getenv(key, "")
    return int(v) if v else default


def _env_opt_int(key: str):
    """Parse an optional int from env.

    Returns None if unset or set to 0.
    """
    v = os.getenv(key, "")
    if not v:
        return None
    iv = int(v)
    return None if iv == 0 else iv


def _env_json(key: str) -> dict:
    v = os.getenv(key, "")
    if not v:
        return {}
    return json.loads(v)


def _load_params_file(path: str) -> dict:
    """Load a JSON params file if provided."""
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"Params file not found: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def main():
    data_root = Path(os.getenv("DATA_ROOT", "data"))
    out_dir = Path(os.getenv("OUT_DIR", "out_backtest"))

    symbol = os.getenv("SYMBOL", "BTCUSDT")
    yyyymmdd = os.getenv("DAY", "")

    if not yyyymmdd:
        raise SystemExit("DAY env var is required (YYYYMMDD).")

    # Run-scoped logging (batch job). This makes debugging deterministic.
    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_root = os.getenv("LOG_ROOT", "out/logs")
    run_id = os.getenv("RUN_ID", datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"))
    log_path = setup_run_logging(
        level=log_level,
        run_type="backtest",
        symbol=symbol,
        yyyymmdd=yyyymmdd,
        run_id=run_id,
        base_dir=log_root,
    )
    logger = logging.getLogger(__name__)
    logger.info("Backtest run start symbol=%s day=%s run_id=%s log_path=%s", symbol, yyyymmdd, run_id, log_path)

    quote_model = os.getenv("QUOTE_MODEL", "avellaneda_stoikov")
    fill_model = os.getenv("FILL_MODEL", "trade_driven")

    quote_qty = _env_float("QUOTE_QTY", 0.001)

    maker_fee_rate = _env_float("MAKER_FEE_RATE", 0.001)
    order_latency_ms = _env_int("ORDER_LATENCY_MS", 50)
    cancel_latency_ms = _env_int("CANCEL_LATENCY_MS", 25)
    requote_interval_ms = _env_int("REQUOTE_INTERVAL_MS", 250)
    # ORDER_TTL_MS: 0 or unset => treat as GTC
    order_ttl_ms = _env_opt_int("ORDER_TTL_MS")
    # REFRESH_INTERVAL_MS: 0 or unset => no refresh of unchanged quotes
    refresh_interval_ms = _env_opt_int("REFRESH_INTERVAL_MS")

    tick_size = _env_float("TICK_SIZE", 0.01)
    qty_step = _env_float("QTY_STEP", 0.0)
    min_notional = _env_float("MIN_NOTIONAL", 0.0)

    initial_cash = _env_float("INITIAL_CASH", 0.0)
    initial_inventory = _env_float("INITIAL_INVENTORY", 0.0)

    quote_params = _env_json("QUOTE_PARAMS_JSON")

    # Optionally load Poisson/Hybrid parameters from a file (recommended for calibration outputs).
    fill_params_file = os.getenv("FILL_PARAMS_FILE", "")
    fill_params = _load_params_file(fill_params_file) if fill_params_file else _env_json("FILL_PARAMS_JSON")

    # If the file contains metadata (e.g. from calibration), keep only model parameters.
    if fill_params_file and any(k in fill_params for k in ("A", "k")):
        fill_params = {
            "A": float(fill_params.get("A")),
            "k": float(fill_params.get("k")),
            "dt_ms": int(fill_params.get("dt_ms", 100)),
            "tick_size": float(fill_params.get("tick_size", tick_size)),
            # allow_partial/max_fill_qty may still be supplied by user for hybrid/trade-driven
            **{k: v for k, v in fill_params.items() if k in ("allow_partial", "max_fill_qty")},
        }

    stats = backtest_day(
        root=data_root,
        symbol=symbol,
        yyyymmdd=yyyymmdd,
        out_dir=out_dir,
        quote_model_name=quote_model,
        fill_model_name=fill_model,
        quote_qty=quote_qty,
        maker_fee_rate=maker_fee_rate,
        order_latency_ms=order_latency_ms,
        cancel_latency_ms=cancel_latency_ms,
        requote_interval_ms=requote_interval_ms,
        order_ttl_ms=order_ttl_ms,
        refresh_interval_ms=refresh_interval_ms,
        tick_size=tick_size,
        qty_step=qty_step,
        min_notional=min_notional,
        initial_cash=initial_cash,
        initial_inventory=initial_inventory,
        quote_params=quote_params,
        fill_params=fill_params,
    )

    logger.info("Backtest run complete symbol=%s day=%s run_id=%s", symbol, yyyymmdd, run_id)
    logger.info("Outputs orders=%s fills=%s state=%s", getattr(stats, "orders_path", ""), getattr(stats, "fills_path", ""), getattr(stats, "state_path", ""))
    print(f"Backtest finished for {symbol} {yyyymmdd}")
    print(stats)


if __name__ == "__main__":
    main()
