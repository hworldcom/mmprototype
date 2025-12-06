# mm/runner_binance_test.py
import os
import time
import yaml
import logging

from .logging_config import setup_logging
from .strategy import StrategyConfig, AvellanedaStoikovStrategy
from .binance_exchange import BinanceExchange, BinanceCredentials
from .utils import now_ms


def load_config(default_path: str) -> dict:
    """
    Load YAML config, optionally overridden by CONFIG_PATH env var.
    """
    path = os.getenv("CONFIG_PATH", default_path)
    with open(path, "r") as f:
        return yaml.safe_load(f)


def build_strategy_config(cfg_raw: dict) -> StrategyConfig:
    return StrategyConfig(
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


def main():
    # 1) Load config & logging
    cfg_raw = load_config("config/config.binance.test.yaml")
    setup_logging(cfg_raw.get("log_level", "INFO"))
    log = logging.getLogger("runner_binance_test")

    dry_run = bool(cfg_raw.get("dry_run", True))
    symbol = cfg_raw["symbol"].upper()
    test_duration_sec = cfg_raw.get("test_duration_seconds", 60)
    update_interval_ms = cfg_raw.get("update_interval_ms", 1000)

    # 2) Build strategy config and Binance credentials
    sc = build_strategy_config(cfg_raw)

    bcfg = cfg_raw["binance"]
    creds = BinanceCredentials(
        api_key=bcfg.get("api_key", ""),
        api_secret=bcfg.get("api_secret", ""),
        testnet=bcfg.get("testnet", True),
        recv_window=bcfg.get("recv_window", 5000),
    )

    log.info("key is ")
    log.info(bcfg.get("api_key", ""))

    mode_str = "DRY-RUN (no orders)" if dry_run else "QUOTE-TEST (placing testnet orders)"
    log.info(
        "Starting Binance test loop for symbol=%s (testnet=%s) in %s",
        symbol,
        creds.testnet,
        mode_str,
    )

    # 3) Instantiate exchange and strategy
    try:
        exch = BinanceExchange(creds, symbol)
    except Exception as e:
        log.exception("Failed to create BinanceExchange: %s", e)
        return

    strat = AvellanedaStoikovStrategy(sc, exch)

    start_ms = now_ms()
    end_ms = start_ms + test_duration_sec * 1000
    iter_idx = 0

    while now_ms() < end_ms:
        iter_idx += 1
        t_sec = (now_ms() - start_ms) / 1000.0

        # 4) Get mid price from Binance
        try:
            mid = exch.get_mid_price()
        except Exception as e:
            log.exception("Error getting mid price: %s", e)
            break

        log.info("[tick %d] mid=%f", iter_idx, mid)

        # feed market data into strategy (for sigma estimation etc.)
        strat.on_market_data(t_sec, mid)

        # --- DRY RUN MODE: only compute/print quotes, no orders ---
        if dry_run:
            sigma_est = strat._estimate_sigma(t_sec)
            if sigma_est <= 0:
                log.info("    Not enough data yet for sigma estimate.")
            else:
                # compute theoretical quotes without placing them
                t_rel = 0.0  # relative horizon clock, can refine later
                raw_bid, raw_ask, r, h = strat.model.optimal_quotes(
                    mid,
                    strat.state.inventory,
                    sigma_est,
                    t_rel,
                )
                log.info(
                    "    sigma=%.6f inv=%.6f -> theo: bid=%.2f ask=%.2f (r=%.2f, h=%.4f)",
                    sigma_est,
                    strat.state.inventory,
                    raw_bid,
                    raw_ask,
                    r,
                    h,
                )
            # No fills to poll, no orders sent
        # --- QUOTE TEST MODE: actually place/cancel tiny testnet orders ---
        else:
            # 1) poll fills (if any)
            try:
                fills = exch.poll_fills()
                if fills:
                    for (oid, side, price, qty) in fills:
                        log.info(
                            "    Fill: order_id=%s side=%s price=%.8f qty=%.8f",
                            oid,
                            side,
                            price,
                            qty,
                        )
                    strat.on_fills(fills)
                else:
                    log.info("    No new fills.")
            except Exception as e:
                log.warning("    Could not poll fills: %s", e)

            # 2) recompute quotes and place orders via strategy
            try:
                strat.recompute_and_quote(t_sec)
            except Exception as e:
                log.exception("    Error during recompute_and_quote: %s", e)
                break

        time.sleep(update_interval_ms / 1000.0)

    log.info("Binance test loop finished (%s).", mode_str)


if __name__ == "__main__":
    main()
