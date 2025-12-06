# mm/orderbook_recorder.py

import os
import time
import csv
import logging
from pathlib import Path
from datetime import datetime, time as dtime

import yaml
from binance.client import Client
from zoneinfo import ZoneInfo  # Python 3.9+ standard lib

from .logging_config import setup_logging


DECIMALS_PRICE = 8
DECIMALS_QTY = 8

def fmt_price(x: float) -> str:
    # fixed decimal, no scientific notation
    return f"{x:.{DECIMALS_PRICE}f}"

def fmt_qty(x: float) -> str:
    return f"{x:.{DECIMALS_QTY}f}"

def load_config(default_path: str) -> dict:
    """
    Load YAML config, optionally overridden by CONFIG_PATH env var.
    We reuse the same binance test config file structure.
    """
    path = os.getenv("CONFIG_PATH", default_path)
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_berlin_now():
    tz = ZoneInfo("Europe/Berlin")
    return datetime.now(tz)


def get_today_end_berlin(hour: int = 22, minute: int = 0) -> datetime:
    """Return today's end time (e.g., 22:00) in Europe/Berlin."""
    now = get_berlin_now()
    end_today = now.replace(
        hour=hour, minute=minute, second=0, microsecond=0
    )
    # If we've already passed today's end time, just stop immediately.
    return end_today


def main():
    # 1) Config & logging
    cfg_raw = load_config("config/config.binance.test.yaml")
    setup_logging(cfg_raw.get("log_level", "INFO"))
    log = logging.getLogger("orderbook_recorder")

    symbol = cfg_raw["symbol"].upper()

    # How often to snapshot (ms). Suggest 200ms to start.
    interval_ms = cfg_raw.get("record_interval_ms", 200)

    # Hard stop at today 22:00 Berlin time
    end_dt_berlin = get_today_end_berlin(hour=22, minute=0)

    # Use mainnet public data for order book recording
    client = Client(api_key=None, api_secret=None)  # mainnet, public

    out_dir = Path("data")
    out_dir.mkdir(exist_ok=True)

    # One file per day & symbol, using UTC date in filename
    utc_date_str = datetime.utcnow().strftime("%Y%m%d")
    outfile = out_dir / f"orderbook_depth5_{symbol}_{utc_date_str}.csv"

    log.info(
        "Recording L2 depth5 for %s to %s until %s (Berlin time), interval=%dms",
        symbol,
        outfile,
        end_dt_berlin.isoformat(),
        interval_ms,
    )

    columns = ["timestamp_ms"]
    for i in range(1, 6):
        columns += [f"bid{i}_price", f"bid{i}_qty", f"ask{i}_price", f"ask{i}_qty"]

    with outfile.open("w", newline="") as f:
        writer = csv.writer(f,delimiter =';')
        writer.writerow(columns)

        snap_idx = 0

        while True:
            now_berlin = get_berlin_now()
            if now_berlin >= end_dt_berlin:
                log.info("Reached end of recording window (%s). Stopping.", end_dt_berlin)
                break

            snap_idx += 1
            ts_ms = int(time.time() * 1000)  # UTC ms

            try:
                ob = client.get_order_book(symbol=symbol, limit=5)
            except Exception as e:
                log.exception("Error fetching order book: %s", e)
                time.sleep(interval_ms / 1000.0)
                continue

            bids = ob.get("bids", [])
            asks = ob.get("asks", [])

            def pad_levels(levels):
                lv = levels[:5]
                while len(lv) < 5:
                    lv.append(["0", "0"])
                return lv

            bids = pad_levels(bids)
            asks = pad_levels(asks)

            row = [ts_ms]
            row = [ts_ms]  # keep timestamp as integer

            for i in range(5):
                bid_price_f, bid_qty_f = float(bids[i][0]), float(bids[i][1])
                ask_price_f, ask_qty_f = float(asks[i][0]), float(asks[i][1])

                # convert to nicely formatted strings to avoid scientific notation
                row += [
                    fmt_price(bid_price_f),
                    fmt_qty(bid_qty_f),
                    fmt_price(ask_price_f),
                    fmt_qty(ask_qty_f),
                ]

            writer.writerow(row)
            f.flush()

            best_bid = float(bids[0][0])
            best_ask = float(asks[0][0])
            mid = 0.5 * (best_bid + best_ask)

            log.info(
                "[snap %d] %s | ts_ms=%d mid≈%.2f | best_bid=%.2f (%.4f) best_ask=%.2f (%.4f)",
                snap_idx,
                now_berlin.strftime("%Y-%m-%d %H:%M:%S"),
                ts_ms,
                mid,
                best_bid,
                float(bids[0][1]),
                best_ask,
                float(asks[0][1]),
            )

            time.sleep(interval_ms / 1000.0)

    log.info("Finished recording order book depth5 for %s", symbol)


if __name__ == "__main__":
    main()
