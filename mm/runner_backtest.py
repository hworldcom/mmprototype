# mm/runner_backtest.py

import os
import json
from pathlib import Path

from mm.backtest.backtester import backtest_day


def _env_float(key: str, default: float) -> float:
    v = os.getenv(key, "")
    return float(v) if v else default


def _env_int(key: str, default: int) -> int:
    v = os.getenv(key, "")
    return int(v) if v else default


def _env_json(key: str) -> dict:
    v = os.getenv(key, "")
    if not v:
        return {}
    return json.loads(v)


def main():
    data_root = Path(os.getenv("DATA_ROOT", "data"))
    out_dir = Path(os.getenv("OUT_DIR", "out_backtest"))

    symbol = os.getenv("SYMBOL", "BTCUSDT")
    yyyymmdd = os.getenv("DAY", "")

    if not yyyymmdd:
        raise SystemExit("DAY env var is required (YYYYMMDD).")

    quote_model = os.getenv("QUOTE_MODEL", "avellaneda_stoikov")
    fill_model = os.getenv("FILL_MODEL", "trade_driven")

    quote_qty = _env_float("QUOTE_QTY", 0.001)

    maker_fee_rate = _env_float("MAKER_FEE_RATE", 0.001)
    order_latency_ms = _env_int("ORDER_LATENCY_MS", 50)
    cancel_latency_ms = _env_int("CANCEL_LATENCY_MS", 25)
    requote_interval_ms = _env_int("REQUOTE_INTERVAL_MS", 250)
    order_ttl_ms = _env_int("ORDER_TTL_MS", 1000)

    tick_size = _env_float("TICK_SIZE", 0.01)
    qty_step = _env_float("QTY_STEP", 0.0)
    min_notional = _env_float("MIN_NOTIONAL", 0.0)

    initial_cash = _env_float("INITIAL_CASH", 0.0)
    initial_inventory = _env_float("INITIAL_INVENTORY", 0.0)

    quote_params = _env_json("QUOTE_PARAMS_JSON")
    fill_params = _env_json("FILL_PARAMS_JSON")

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
        tick_size=tick_size,
        qty_step=qty_step,
        min_notional=min_notional,
        initial_cash=initial_cash,
        initial_inventory=initial_inventory,
        quote_params=quote_params,
        fill_params=fill_params,
    )

    print(f"Backtest finished for {symbol} {yyyymmdd}")
    print(stats)


if __name__ == "__main__":
    main()
