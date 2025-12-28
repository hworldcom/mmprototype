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


def main():
    root = Path(os.getenv("DATA_ROOT", "data"))
    out_dir = Path(os.getenv("OUT_DIR", "out_backtest"))
    symbol = os.getenv("SYMBOL", "BTCUSDT").upper()
    yyyymmdd = os.getenv("DAY", "")
    if not yyyymmdd:
        raise RuntimeError("Set DAY=YYYYMMDD, e.g. DAY=20251216")

    quote_model = os.getenv("QUOTE_MODEL", "avellaneda_stoikov")
    fill_model = os.getenv("FILL_MODEL", "trade_driven")

    quote_qty = _env_float("QUOTE_QTY", 0.001)
    maker_fee_rate = _env_float("MAKER_FEE_RATE", 0.001)  # 0.1% worst-case
    order_latency_ms = _env_int("ORDER_LATENCY_MS", 50)
    requote_interval_ms = _env_int("REQUOTE_INTERVAL_MS", 250)

    # Optional JSON blobs for parameters
    quote_params = json.loads(os.getenv("QUOTE_PARAMS_JSON", "{}"))
    fill_params = json.loads(os.getenv("FILL_PARAMS_JSON", "{}"))

    stats = backtest_day(
        root=root,
        symbol=symbol,
        yyyymmdd=yyyymmdd,
        out_dir=out_dir,
        quote_model_name=quote_model,
        fill_model_name=fill_model,
        quote_qty=quote_qty,
        maker_fee_rate=maker_fee_rate,
        order_latency_ms=order_latency_ms,
        requote_interval_ms=requote_interval_ms,
        quote_params=quote_params,
        fill_params=fill_params,
    )

    print(f"Backtest finished for {symbol} {yyyymmdd}")
    print(stats)


if __name__ == "__main__":
    main()
