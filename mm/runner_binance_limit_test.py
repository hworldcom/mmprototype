# mm/runner_binance_limit_test.py

import os
import time
import yaml
import logging

from .logging_config import setup_logging
from .binance_exchange import BinanceExchange, BinanceCredentials
from .utils import round_to_tick, clamp_qty


def load_config(default_path: str) -> dict:
    """
    Load YAML config, optionally overridden by CONFIG_PATH env var.
    """
    path = os.getenv("CONFIG_PATH", default_path)
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    # 1) Load config & logging
    cfg_raw = load_config("config/config.binance.test.yaml")
    setup_logging(cfg_raw.get("log_level", "INFO"))
    log = logging.getLogger("runner_binance_limit_test")

    symbol = cfg_raw["symbol"].upper()
    tick_size = cfg_raw["tick_size"]
    qty_step = cfg_raw["qty_step"]
    min_qty = cfg_raw["min_qty"]
    base_order_notional = cfg_raw.get("base_order_notional", 10.0)  # small

    bcfg = cfg_raw["binance"]
    creds = BinanceCredentials(
        api_key=bcfg.get("api_key", "") or os.getenv("BINANCE_API_KEY", ""),
        api_secret=bcfg.get("api_secret", "") or os.getenv("BINANCE_API_SECRET", ""),
        testnet=bcfg.get("testnet", True),
        recv_window=bcfg.get("recv_window", 5000),
    )

    log.info("Starting Binance LIMIT ORDER test for symbol=%s (testnet=%s)",
             symbol, creds.testnet)

    # 2) Instantiate exchange connector
    try:
        exch = BinanceExchange(creds, symbol)
    except Exception as e:
        log.exception("Failed to create BinanceExchange: %s", e)
        return

    # 3) Get mid price
    try:
        mid = exch.get_mid_price()
    except Exception as e:
        log.exception("Error getting mid price: %s", e)
        return

    log.info("Current mid for %s: %.8f", symbol, mid)

    # 4) Build a small BUY limit order a bit below mid (less likely to insta-fill)
    side = "buy"
    ticks_below = 10
    raw_price = mid - ticks_below * tick_size
    price = round_to_tick(raw_price, tick_size)

    # size: base_order_notional / price, but snap to qty_step and ensure >= min_qty
    raw_qty = base_order_notional / price
    qty = clamp_qty(raw_qty, qty_step)
    if qty < min_qty:
        qty = min_qty

    log.info(
        "Placing testnet LIMIT %s: price=%.8f qty=%.8f (raw_price=%.8f, raw_qty=%.8f)",
        side.upper(),
        price,
        qty,
        raw_price,
        raw_qty,
    )

    # 5) Place the order
    try:
        order_id = exch.place_limit_order(side, price, qty)
        log.info("Order placed. order_id=%s", order_id)
    except Exception as e:
        log.exception("Error placing limit order: %s", e)
        return

    # 6) Show open orders
    time.sleep(2.0)
    try:
        open_orders = exch.get_open_orders()
        log.info("Open orders after placement: %d", len(open_orders))
        for o in open_orders:
            log.info(
                "  - id=%s side=%s price=%.8f qty=%.8f ts=%d",
                o.order_id,
                o.side,
                o.price,
                o.qty,
                o.timestamp_ms,
            )
    except Exception as e:
        log.warning("Could not fetch open orders: %s", e)

    # 7) Wait a bit, then cancel
    log.info("Sleeping 5 seconds before cancel...")
    time.sleep(5.0)

    try:
        exch.cancel_order(order_id)
        log.info("Cancel request sent for order_id=%s", order_id)
    except Exception as e:
        log.warning("Error canceling order %s: %s", order_id, e)

    # 8) Show open orders again
    time.sleep(2.0)
    try:
        open_orders = exch.get_open_orders()
        log.info("Open orders after cancel: %d", len(open_orders))
        for o in open_orders:
            log.info(
                "  - id=%s side=%s price=%.8f qty=%.8f ts=%d",
                o.order_id,
                o.side,
                o.price,
                o.qty,
                o.timestamp_ms,
            )
    except Exception as e:
        log.warning("Could not fetch open orders: %s", e)

    log.info("Binance LIMIT ORDER test finished.")


if __name__ == "__main__":
    main()
