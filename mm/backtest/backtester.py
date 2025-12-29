# mm/backtest/backtester.py

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any

from mm.backtest.paper_exchange import PaperExchange, BacktestConfig
from mm.backtest.quotes.base import MarketState
from mm.backtest.replay import replay_day
from mm.market_data.sync_engine import OrderBookSyncEngine

# Quote models
from mm.backtest.quotes.avellaneda_stoikov import AvellanedaStoikovQuoteModel
from mm.backtest.quotes.inventory_skew import InventorySkewQuoteModel
from mm.backtest.quotes.microstructure import MicrostructureQuoteModel
from mm.backtest.quotes.hybrid import HybridASMicrostructureQuoteModel
from mm.backtest.quotes.balance_aware import BalanceAwareQuoteModel

# Fill models
from mm.backtest.fills.trade_driven import TradeDrivenFillModel
from mm.backtest.fills.poisson import PoissonFillModel
from mm.backtest.fills.hybrid import HybridFillModel


@dataclass
class BacktestRunStats:
    replay: Dict[str, Any]
    fills_path: str
    orders_path: str
    state_path: str


def _market_state_from_engine(recv_ms: int, engine: OrderBookSyncEngine) -> Optional[MarketState]:
    lob = engine.lob
    if lob is None:
        return None

    bids, asks = lob.top_n(5)
    if not bids or not asks:
        return None

    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    if best_bid <= 0 or best_ask <= 0:
        return None

    mid = 0.5 * (best_bid + best_ask)
    spread = best_ask - best_bid
    imbalance = None

    # If L2 sizes exist we can compute a simple top-of-book imbalance.
    try:
        bid_sz = float(bids[0][1])
        ask_sz = float(asks[0][1])
        denom = bid_sz + ask_sz
        if denom > 0:
            imbalance = (bid_sz - ask_sz) / denom
    except Exception:
        imbalance = None

    return MarketState(
        recv_ms=int(recv_ms),
        mid=mid,
        best_bid=best_bid,
        best_ask=best_ask,
        spread=spread,
        imbalance=imbalance,
    )




def make_quote_model(name: str, *, qty: float, tick_size: float, params: Optional[Dict[str, float]] = None):
    """Factory for quote models.

    Parameter names are aligned to the actual dataclass fields in mm/backtest/quotes/*.
    """
    params = params or {}
    n = name.lower()

    if n in ("avellaneda_stoikov", "as"):
        return AvellanedaStoikovQuoteModel(
            qty=qty,
            gamma=float(params.get("gamma", 0.1)),
            sigma=float(params.get("sigma", 0.001)),
            k=float(params.get("k", 1.5)),
            tau_sec=float(params.get("tau_sec", 1.0)),
        )

    if n in ("inventory_skew", "skew"):
        return InventorySkewQuoteModel(
            qty=qty,
            half_spread=float(params.get("half_spread", 2.0 * tick_size)),
            inv_skew=float(params.get("inv_skew", 0.0)),
        )

    if n in ("microstructure", "micro"):
        return MicrostructureQuoteModel(
            qty=qty,
            join=bool(params.get("join", True)),
            tick_size=float(params.get("tick_size", tick_size)),
            inv_skew=float(params.get("inv_skew", 0.0)),
            imbalance_aggression=float(params.get("imbalance_aggression", 0.0)),
        )

    if n in ("hybrid", "as_micro"):
        return HybridASMicrostructureQuoteModel(
            qty=qty,
            gamma=float(params.get("gamma", 0.1)),
            sigma=float(params.get("sigma", 0.001)),
            k=float(params.get("k", 1.5)),
            tau_sec=float(params.get("tau_sec", 1.0)),
            tick_size=float(params.get("tick_size", tick_size)),
            anchor_band_ticks=int(params.get("anchor_band_ticks", 3)),
        )

    raise ValueError(f"Unknown quote model: {name!r}")


def make_fill_model(name: str, params: Optional[Dict[str, float]] = None):
    """Factory for fill models."""
    params = params or {}
    n = name.lower()

    if n in ("trade_driven", "trade"):
        return TradeDrivenFillModel(
            allow_partial=bool(params.get("allow_partial", True)),
            max_fill_qty=float(params.get("max_fill_qty", 1e18)),
        )

    if n in ("poisson",):
        return PoissonFillModel(
            A=float(params.get("A", 1.0)),
            k=float(params.get("k", 1.5)),
            dt_ms=int(params.get("dt_ms", 100)),
        )

    if n in ("hybrid",):
        return HybridFillModel(
            trade_model=TradeDrivenFillModel(
                allow_partial=bool(params.get("allow_partial", True)),
                max_fill_qty=float(params.get("max_fill_qty", 1e18)),
            ),
            tick_model=PoissonFillModel(
                A=float(params.get("A", 1.0)),
                k=float(params.get("k", 1.5)),
                dt_ms=int(params.get("dt_ms", 100)),
            ),
        )

    raise ValueError(f"Unknown fill model: {name!r}")

def backtest_day(
    *,
    root: Path,
    symbol: str,
    yyyymmdd: str,
    out_dir: Path,
    quote_model_name: str = "avellaneda_stoikov",
    fill_model_name: str = "trade_driven",
    quote_qty: float = 0.001,
    maker_fee_rate: float = 0.001,
    order_latency_ms: int = 50,
    cancel_latency_ms: int = 25,
    requote_interval_ms: int = 250,
    # If None/0: treat orders as Good-Till-Cancel (no expiry).
    order_ttl_ms: Optional[int] = None,
    # If None/0: do not refresh unchanged quotes.
    refresh_interval_ms: Optional[int] = None,
    tick_size: float = 0.01,
    qty_step: float = 0.0,
    min_notional: float = 0.0,
    initial_cash: float = 0.0,
    initial_inventory: float = 0.0,
    quote_params: Optional[Dict[str, float]] = None,
    fill_params: Optional[Dict[str, float]] = None,
) -> BacktestRunStats:
    quote_model = make_quote_model(quote_model_name, qty=quote_qty, tick_size=tick_size, params=quote_params)
    # Filter unfundable quotes under spot constraints (cash/inventory/min-notional).
    quote_model = BalanceAwareQuoteModel(inner=quote_model, maker_fee_rate=maker_fee_rate, min_notional=min_notional)
    fill_model = make_fill_model(fill_model_name, params=fill_params)

    cfg = BacktestConfig(
        maker_fee_rate=maker_fee_rate,
        order_latency_ms=order_latency_ms,
        cancel_latency_ms=cancel_latency_ms,
        quote_interval_ms=requote_interval_ms,
        order_ttl_ms=order_ttl_ms,
        refresh_interval_ms=refresh_interval_ms,
        tick_size=tick_size,
        qty_step=qty_step,
        min_notional=min_notional,
        initial_cash=initial_cash,
        initial_inventory=initial_inventory,
        enforce_balances=True,
    )

    exch = PaperExchange(cfg=cfg, quote_model=quote_model, fill_model=fill_model, out_dir=out_dir, symbol=symbol)

    def on_tick(recv_ms: int, engine: OrderBookSyncEngine):
        ms = _market_state_from_engine(recv_ms, engine)
        if ms is None:
            return
        exch.on_tick(ms)

    def on_trade(tr, engine: OrderBookSyncEngine):
        exch.on_trade(tr.recv_ms, float(tr.price), float(tr.qty), int(tr.is_buyer_maker))

    stats = replay_day(root=root, symbol=symbol, yyyymmdd=yyyymmdd, on_tick=on_tick, on_trade=on_trade)

    exch.close()

    return BacktestRunStats(
        replay=stats.__dict__,
        fills_path=str(exch.fills_path),
        orders_path=str(exch.orders_path),
        state_path=str(exch.state_path),
    )