# mm/runner_sim.py
import time
import yaml
import logging
import os

from .logging_config import setup_logging
from .exchange import SimulatedExchange
from .strategy import StrategyConfig, AvellanedaStoikovStrategy
from .utils import now_ms


def load_config(default_path: str) -> dict:
    path = os.getenv("CONFIG_PATH", default_path)
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    cfg_raw = load_config("config/config.sim.yaml")
    setup_logging(cfg_raw.get("log_level", "INFO"))
    log = logging.getLogger("runner_sim")

    # Build StrategyConfig
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

    sim_cfg = cfg_raw["simulation"]
    exch = SimulatedExchange(
        mid_start=sim_cfg["midprice_start"],
        sigma=sim_cfg["midprice_sigma"],
        tick_size=sc.tick_size,
    )

    strat = AvellanedaStoikovStrategy(sc, exch)

    log.info("Starting simulated Avellaneda–Stoikov MM loop")

    start = now_ms()
    duration_ms = sim_cfg.get("duration_ms", 30_000)  # default: 30s
    end = start + duration_ms
    update_interval_ms = sim_cfg["update_interval_ms"]

    while now_ms() < end:
        exch.step_price()
        mid = exch.get_mid_price()
        t_sec = (now_ms() - start) / 1000.0

        strat.on_market_data(t_sec, mid)

        fills = exch.poll_fills()
        if fills:
            strat.on_fills(fills)

        strat.recompute_and_quote(t_sec)

        time.sleep(update_interval_ms / 1000.0)

    log.info("Simulation finished")


if __name__ == "__main__":
    main()
