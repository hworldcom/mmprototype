# mm/orderbook_recorder.py

import os
import time
import csv
import logging
from pathlib import Path
from datetime import datetime, time as dtime
from datetime import UTC

import yaml
from binance.client import Client
from zoneinfo import ZoneInfo  # Python 3.9+

from .logging_config import setup_logging


DECIMALS_PRICE = 8
DECIMALS_QTY = 8


def fmt_price(x: float) -> str:
    # fixed decimal, no scientific notation
    return f"{x:.{DECIMALS_PRICE}f}"


def fmt_qty(x: float) -> str:
    return f"{x:.{DECIMALS_QTY}f}"


def load_config(default_path: str) -> dict:
    path = os.getenv("CONFIG_PATH", default_path)
    with open(path, "r") as f:
        return yaml.safe_load(f)


def berlin_now() -> datetime:
    return datetime.now(ZoneInfo("Europe/Berlin"))


def today_window_berlin(start_h: int = 8, end_h: int = 22) -> tuple[datetime, datetime]:
    """
    Returns (start_dt, end_dt) for today's recording window in Europe/Berlin.
    """
    now = berlin_now()
    start_dt = now.replace(hour=start_h, minute=0, second=0, microsecond=0)
    end_dt = now.replace(hour=end_h, minute=0, second=0, microsecond=0)
    return start_dt, end_dt


def main():
    # --- config & logging ---
    cfg_raw = load_config("config/config.binance.test.yaml")
    setup_logging(cfg_raw.get("log_level", "INFO"))
    log = logging.getLogger("orderbook_recorder")

    symbol = cfg_raw["symbol"].upper()
    interval_ms = cfg_raw.get("record_interval_ms", 200)  # snapshot every 200ms by default

    start_dt, end_dt = today_window_berlin(start_h=8, end_h=22)

    now_b = berlin_now()
    if now_b > end_dt:
        log.info(
            "Current time %s is past today's end window %s. Exiting.",
            now_b.isoformat(),
            end_dt.isoformat(),
        )
        return

    if now_b < start_dt:
        wait_sec = (start_dt - now_b).total_seconds()
        log.info(
            "Current time %s is before start window %s. Sleeping for %.1fs.",
            now_b.isoformat(),
            start_dt.isoformat(),
            wait_sec,
        )
        time.sleep(max(0.0, wait_sec))

    log.info(
        "Starting order book recording for %s. Window: %s → %s (Berlin time). Interval=%dms",
        symbol,
        start_dt.isoformat(),
        end_dt.isoformat(),
        interval_ms,
    )

    # --- Binance client (mainnet, public data only) ---
    client = Client(api_key=None, api_secret=None)  # mainnet, no auth needed for order book

    # --- output file ---
    out_dir = Path("data")
    out_dir.mkdir(exist_ok=True)

    utc_date_str = datetime.now(UTC).strftime("%Y%m%d")

    outfile = out_dir / f"orderbook_depth5_{symbol}_{utc_date_str}.csv"

    log.info("Writing depth5 snapshots to %s", outfile)

    columns = ["timestamp_ms"]
    for i in range(1, 6):
        columns += [f"bid{i}_price", f"bid{i}_qty", f"ask{i}_price", f"ask{i}_qty"]

    with outfile.open("w", newline="") as f:
        writer = csv.writer(f,delimiter =';')
        writer.writerow(columns)

        snap_idx = 0

        while True:
            now_b = berlin_now()
            if now_b >= end_dt:
                log.info(
                    "Reached end of recording window (%s Berlin). Stopping.",
                    end_dt.isoformat(),
                )
                break

            snap_idx += 1
            ts_ms = int(time.time() * 1000)  # Unix epoch, UTC ms

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
            # build row with *string* decimals (no scientific notation)
            for i in range(5):
                bid_price_f, bid_qty_f = float(bids[i][0]), float(bids[i][1])
                ask_price_f, ask_qty_f = float(asks[i][0]), float(asks[i][1])
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
                "[snap %d] %s | ts_ms=%d mid≈%.2f | best_bid=%.2f (%s) best_ask=%.2f (%s)",
                snap_idx,
                now_b.strftime("%Y-%m-%d %H:%M:%S"),
                ts_ms,
                mid,
                best_bid,
                fmt_qty(float(bids[0][1])),
                best_ask,
                fmt_qty(float(asks[0][1])),
            )

            time.sleep(interval_ms / 1000.0)

    log.info("Finished recording order book depth5 for %s.", symbol)


if __name__ == "__main__":
    main()
