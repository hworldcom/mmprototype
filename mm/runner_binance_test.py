# mm/runner_binance_test.py
import time
import yaml
import logging
import os

from .logging_config import setup_logging
from .strategy import StrategyConfig, AvellanedaStoikovStrategy
from .utils import now_ms

from .binance_exchange import BinanceExchange, BinanceCredentials  # when ready


def load_config(default_path: str) -> dict:
    path = os.getenv("CONFIG_PATH", default_path)
    with open(path, "r") as f:
        return yaml.safe_load(f)

def main():
    cfg_raw = load_config("config/config.binance.test.yaml")
    setup_logging(cfg_raw.get("log_level", "INFO"))
    log = logging.getLogger("runner_binance_test")

    # StrategyConfig (same as sim, minus simulation section)
    sc = StrategyConfig(
        symbol=cfg_raw["symbol"],
        tick_size=cfg_raw["tick_size"],
        qty_step=cfg_raw["qty_step"],
        min_qty=cfg_raw["min_qty"],
        min_notional=cfg_raw["min_notional"],
        maker_fee=cfg_raw["maker_fee"],
        taker_fee=cfg_raw["taker_fee"],
        gamma=cfg_raw["gamma"],
        horizon_seconds=cfg_raw["horizon_seconds"],
        sigma_window_seconds=cfg_raw["sigma_window_seconds"],
        A=cfg_raw["A"],
        k=cfg_raw["k"],
        base_order_notional=cfg_raw["base_order_notional"],
        min_spread_ticks=cfg_raw["min_spread_ticks"],
        max_quote_lifetime_ms=cfg_raw["max_quote_lifetime_ms"],
        inventory_skew_factor=cfg_raw["inventory_skew_factor"],
        max_inventory=cfg_raw["max_inventory"],
        soft_inventory=cfg_raw["soft_inventory"],
        max_notional_abs=cfg_raw["max_notional_abs"],
        max_daily_loss=cfg_raw["max_daily_loss"],
        max_drawdown=cfg_raw["max_drawdown"],
    )

    bcfg = cfg_raw["binance"]
    creds = BinanceCredentials(
        api_key=bcfg.get("api_key", ""),
        api_secret=bcfg.get("api_secret", ""),
        testnet=bcfg.get("testnet", True),
        recv_window=bcfg.get("recv_window", 5000),
    )

    exch = BinanceExchange(creds, sc.symbol)
    strat = AvellanedaStoikovStrategy(sc, exch)

    log.info("Starting Binance TEST loop (no real trading at first)")

    start = now_ms()
    duration_sec = cfg_raw.get("test_duration_seconds", 60)
    end = start + duration_sec * 1000
    update_interval_ms = cfg_raw.get("update_interval_ms", 1000)

    dry_run = cfg_raw.get("dry_run", True)

    while now_ms() < end:
        mid = exch.get_mid_price()
        t_sec = (now_ms() - start) / 1000.0

        strat.on_market_data(t_sec, mid)

        # For *first* tests, we don't place orders.
        # Just compute what we *would* quote:
        if dry_run:
            sigma_est = strat._estimate_sigma(t_sec)
            if sigma_est > 0:
                # compute quotes but DON'T send
                t_rel = 0.0
                raw_bid, raw_ask, r, h = strat.model.optimal_quotes(
                    mid, strat.state.inventory, sigma_est, t_rel
                )
                log.info(
                    "mid=%.2f sigma=%.4f -> theo bid=%.2f ask=%.2f (r=%.2f, h=%.4f)",
                    mid,
                    sigma_est,
                    raw_bid,
                    raw_ask,
                    r,
                    h,
                )
        else:
            # Once confident, you could enable actual quoting:
            fills = exch.poll_fills()
            if fills:
                strat.on_fills(fills)
            strat.recompute_and_quote(t_sec)

        time.sleep(update_interval_ms / 1000.0)

    log.info("Binance test loop finished")


if __name__ == "__main__":
    main()
