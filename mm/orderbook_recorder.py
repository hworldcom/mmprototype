# mm/orderbook_recorder.py

import os
import time
import csv
import logging
from pathlib import Path

import yaml
from binance.client import Client

from .logging_config import setup_logging


def load_config(default_path: str) -> dict:
    """
    Load YAML config, optionally overridden by CONFIG_PATH env var.
    We reuse the same binance test config file structure.
    """
    path = os.getenv("CONFIG_PATH", default_path)
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    # 1) Config & logging
    cfg_raw = load_config("config/config.binance.test.yaml")
    setup_logging(cfg_raw.get("log_level", "INFO"))
    log = logging.getLogger("orderbook_recorder")

    symbol = cfg_raw["symbol"].upper()
    duration_sec = cfg_raw.get("record_duration_seconds", 60)  # how long to record
    interval_ms = cfg_raw.get("record_interval_ms", 200)       # how often to snapshot (ms)

    bcfg = cfg_raw["binance"]
    api_key = bcfg.get("api_key", "") or os.getenv("BINANCE_API_KEY", "")
    api_secret = bcfg.get("api_secret", "") or os.getenv("BINANCE_API_SECRET", "")
    testnet = bcfg.get("testnet", False) # data on testnet are different

    if not api_key or not api_secret:
        log.warning("No API key/secret provided; using public endpoints only (which is fine for order book).")

    # 2) Init Binance client
    client = Client(api_key, api_secret, testnet=testnet)

    # 3) Prepare output file
    out_dir = Path("data")
    out_dir.mkdir(exist_ok=True)
    outfile = out_dir / f"orderbook_depth5_{symbol}.csv"

    log.info("Recording L2 depth5 for %s to %s (duration=%ds, interval=%dms, testnet=%s)",
             symbol, outfile, duration_sec, interval_ms, testnet)

    # CSV columns: timestamp + 5 bid levels + 5 ask levels
    # bid_i_price, bid_i_qty, ask_i_price, ask_i_qty for i=1..5
    columns = ["timestamp"]
    for i in range(1, 6):
        columns += [f"bid{i}_price", f"bid{i}_qty", f"ask{i}_price", f"ask{i}_qty"]

    with outfile.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)

        end_time = time.time() + duration_sec
        snap_idx = 0

        while time.time() < end_time:
            snap_idx += 1

            # Use integer milliseconds (recommended for trading data)
            ts = int(time.time() * 1000)

            try:
                ob = client.get_order_book(symbol=symbol, limit=5)
                ticker = client.get_orderbook_ticker(symbol=symbol)
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

            row = [ts]
            for i in range(5):
                bid_price, bid_qty = float(bids[i][0]), float(bids[i][1])
                ask_price, ask_qty = float(asks[i][0]), float(asks[i][1])
                row += [bid_price, bid_qty, ask_price, ask_qty]

            writer.writerow(row)
            f.flush()

            best_bid = float(bids[0][0])
            best_ask = float(asks[0][0])

            log.info(
                "[snap %d] ts=%d mid≈%.2f | best_bid=%.2f (%.4f) best_ask=%.2f (%.4f)",
                snap_idx,
                ts,
                0.5 * (best_bid + best_ask),
                best_bid,
                float(bids[0][1]),
                best_ask,
                float(asks[0][1]),
            )

            time.sleep(interval_ms / 1000.0)

    log.info("Finished recording order book depth5 for %s", symbol)


if __name__ == "__main__":
    main()
