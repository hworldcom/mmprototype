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
    if best_ask <= 0 or best_bid <= 0 or best_ask <= best_bid:
        return None

    mid = 0.5 * (best_bid + best_ask)
    spread = best_ask - best_bid

    bid_v = sum(q for _, q in bids)
    ask_v = sum(q for _, q in asks)
    denom = bid_v + ask_v
    imbalance = (bid_v - ask_v) / denom if denom > 0 else None

    return MarketState(
        recv_ms=recv_ms,
        mid=mid,
        best_bid=best_bid,
        best_ask=best_ask,
        spread=spread,
        imbalance=imbalance,
    )


def make_quote_model(name: str, *, qty: float, params: Dict[str, float]):
    n = (name or "").lower().strip()
    if n in ("as", "avellaneda", "avellaneda_stoikov", "avellaneda-stoikov"):
        return AvellanedaStoikovQuoteModel(
            qty=qty,
            gamma=float(params.get("gamma", 0.1)),
            sigma=float(params.get("sigma", 0.002)),
            k=float(params.get("k", 1.5)),
            tau_sec=float(params.get("tau_sec", 60.0)),
        )
    if n in ("inventory_skew", "skew"):
        return InventorySkewQuoteModel(
            qty=qty,
            half_spread=float(params.get("half_spread", 1.0)),
            skew=float(params.get("skew", 0.5)),
        )
    if n in ("micro", "microstructure"):
        return MicrostructureQuoteModel(
            qty=qty,
            half_spread=float(params.get("half_spread", 1.0)),
            imbalance_sensitivity=float(params.get("imbalance_sensitivity", 1.0)),
        )
    if n in ("hybrid",):
        return HybridASMicrostructureQuoteModel(
            qty=qty,
            gamma=float(params.get("gamma", 0.1)),
            sigma=float(params.get("sigma", 0.002)),
            k=float(params.get("k", 1.5)),
            tau_sec=float(params.get("tau_sec", 60.0)),
            tick_size=float(params.get("tick_size", 0.01)),
            anchor_band_ticks=int(params.get("anchor_band_ticks", 3)),
        )

    raise ValueError(f"Unknown quote model: {name!r}")


def make_fill_model(name: str, params: Dict[str, float]):
    n = (name or "").lower().strip()
    if n in ("trade", "trade_driven", "trade-driven"):
        return TradeDrivenFillModel(allow_partial=bool(int(params.get("allow_partial", 1))))
    if n in ("poisson",):
        return PoissonFillModel(
            A=float(params.get("A", 0.5)),
            k=float(params.get("k", 1.5)),
            dt_sec=float(params.get("dt_sec", 0.1)),
        )
    if n in ("hybrid",):
        return HybridASMicrostructureQuoteModel(
            qty=qty,
            gamma=float(params.get("gamma", 0.1)),
            sigma=float(params.get("sigma", 0.002)),
            k=float(params.get("k", 1.5)),
            tau_sec=float(params.get("tau_sec", 60.0)),
            tick_size=float(params.get("tick_size", 0.01)),
            anchor_band_ticks=int(params.get("anchor_band_ticks", 3)),
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
    requote_interval_ms: int = 250,
    quote_params: Optional[Dict[str, float]] = None,
    fill_params: Optional[Dict[str, float]] = None,
) -> BacktestRunStats:
    quote_params = quote_params or {}
    fill_params = fill_params or {}

    cfg = BacktestConfig(
        maker_fee_rate=maker_fee_rate,
        order_latency_ms=order_latency_ms,
        quote_interval_ms=requote_interval_ms,
    )

    quote_model = make_quote_model(quote_model_name, qty=quote_qty, params=quote_params)
    fill_model = make_fill_model(fill_model_name, fill_params)

    exch = PaperExchange(cfg=cfg, quote_model=quote_model, fill_model=fill_model, out_dir=out_dir, symbol=symbol)

    def on_tick(recv_ms: int, engine: OrderBookSyncEngine):
        ms = _market_state_from_engine(recv_ms, engine)
        if ms is None:
            return
        exch.on_tick(ms)

    def on_trade(tr, engine: OrderBookSyncEngine):
        # Trade dataclass from mm.backtest.io
        exch.on_trade(tr.recv_ms, float(tr.price), float(tr.qty), int(tr.is_buyer_maker))

    stats = replay_day(root=root, symbol=symbol, yyyymmdd=yyyymmdd, on_tick=on_tick, on_trade=on_trade)

    exch.close()

    return BacktestRunStats(
        replay=stats.__dict__,
        fills_path=str(exch.fills_path),
        orders_path=str(exch.orders_path),
        state_path=str(exch.state_path),
    )
