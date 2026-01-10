# mm/backtest/replay.py

from __future__ import annotations

import heapq
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterator, Optional, Tuple

from mm.market_data.local_orderbook import LocalOrderBook
from mm.market_data.sync_engine import OrderBookSyncEngine

from .io import DepthDiff, Trade, EventRow, find_depth_diffs_file, find_trades_file, find_events_file
from .io import iter_depth_diffs, iter_trades_csv, iter_events_csv

log = logging.getLogger("backtest.replay")


@dataclass
class ReplayStats:
    depth_msgs: int = 0
    trade_msgs: int = 0
    snapshots_loaded: int = 0
    gaps: int = 0
    applied: int = 0
    synced: int = 0


def _load_snapshot_from_event(details_json: str) -> Optional[Tuple[int, str]]:
    """
    Recorder emits events with details_json containing snapshot info.
    We rely on that to know when to 'adopt' a new snapshot in replay.
    Returns (lastUpdateId, path) if present.
    """
    try:
        d = json.loads(details_json)
        if "lastUpdateId" in d and "path" in d:
            return int(d["lastUpdateId"]), str(d["path"])
    except Exception:
        return None
    return None


def load_snapshot_csv(path: Path) -> LocalOrderBook:
    """
    Snapshot CSV format (from your recorder/snapshot.py):
      run_id,event_id,side,price,qty,lastUpdateId
    """
    import csv

    bids = []
    asks = []
    last_uid = None

    with path.open("r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            side = row["side"]
            price = row["price"]
            qty = row["qty"]
            last_uid = int(row["lastUpdateId"])
            if side == "bid":
                bids.append([price, qty])
            else:
                asks.append([price, qty])

    if last_uid is None:
        raise ValueError(f"Snapshot CSV {path} missing lastUpdateId")

    lob = LocalOrderBook()
    lob.load_snapshot(bids=bids, asks=asks, last_update_id=last_uid)
    return lob


def _validate_book_state(lob: LocalOrderBook, context: str) -> None:
    """Ensure recent snapshot/resync produced a sane book."""
    best_bid = max(lob.bids) if lob.bids else None
    best_ask = min(lob.asks) if lob.asks else None

    if best_bid is None or best_ask is None:
        log.warning(
            "Order book missing side after %s: bids=%d asks=%d",
            context,
            len(lob.bids),
            len(lob.asks),
        )
        return

    if best_bid >= best_ask:
        msg = (
            f"Invalid order book after {context}: "
            f"best_bid={best_bid} >= best_ask={best_ask}"
        )
        log.error(msg)
        raise AssertionError(msg)

    log.debug(
        "Validated order book after %s (best_bid=%.8f, best_ask=%.8f, spread=%.8f)",
        context,
        best_bid,
        best_ask,
        best_ask - best_bid,
    )


def replay_day(
    root: Path,
    symbol: str,
    yyyymmdd: str,
    on_tick: Optional[Callable[[int, OrderBookSyncEngine], None]] = None,
    on_trade: Optional[Callable[[Trade, OrderBookSyncEngine], None]] = None,
    time_min_ms: Optional[int] = None,
    time_max_ms: Optional[int] = None,
) -> ReplayStats:
    """
    Reconstructs a local book and replays events in recv_ms time order.

    The key idea:
      - Use events_*.csv to know when snapshots were taken and where they are.
      - Feed depth diffs into OrderBookSyncEngine.
      - Strategy can hook into:
          * on_tick(recv_ms, engine) when book is valid
          * on_trade(trade, engine) for trade prints and fill-modeling
    """
    stats = ReplayStats()

    depth_path = find_depth_diffs_file(root, symbol, yyyymmdd)
    trades_path = find_trades_file(root, symbol, yyyymmdd)
    events_path = find_events_file(root, symbol, yyyymmdd)

    depth_it = iter_depth_diffs(depth_path)
    trade_it = iter_trades_csv(trades_path)
    event_it = iter_events_csv(events_path)

    engine = OrderBookSyncEngine()

    # Merge 3 iterators by (recv_ms, recv_seq) using a heap.
    # recv_seq is a globally increasing receive sequence recorded by the recorder.
    # If older datasets lack recv_seq, we fall back to a local monotonic sequence
    # to ensure deterministic ordering and avoid heap tie-compare issues.
    # items: (recv_ms, seq_key, tie_seq, stream_name, payload)
    heap: list[tuple[int, int, int, str, object]] = []
    tie_seq = 0

    def push_next(it: Iterator, name: str):
        nonlocal tie_seq
        try:
            item = next(it)
        except StopIteration:
            return
        recv_ms = int(getattr(item, "recv_ms"))
        recv_seq = getattr(item, "recv_seq", None)
        seq_key = int(recv_seq) if recv_seq is not None else tie_seq
        heapq.heappush(heap, (recv_ms, seq_key, tie_seq, name, item))
        tie_seq += 1

    push_next(depth_it, "depth")
    push_next(trade_it, "trade")
    push_next(event_it, "event")

    while heap:
        recv_ms, _, _, name, item = heapq.heappop(heap)

        if name == "event":
            ev: EventRow = item
            if ev.type == "snapshot_loaded":
                info = _load_snapshot_from_event(ev.details_json)
                if info:
                    _, p_str = info
                    lob = load_snapshot_csv(Path(p_str))
                    engine.adopt_snapshot(lob)
                    _validate_book_state(engine.lob, context=f"snapshot {Path(p_str).name}")
                    stats.snapshots_loaded += 1
            push_next(event_it, "event")

        elif name == "depth":
            dd: DepthDiff = item
            # Propagate global ordering into the engine for downstream consumers.
            try:
                engine.last_recv_seq = int(getattr(dd, 'recv_seq'))
            except Exception:
                engine.last_recv_seq = None
            stats.depth_msgs += 1
            result = engine.feed_depth_event(
                {"E": dd.E, "U": dd.U, "u": dd.u, "b": dd.b, "a": dd.a}
            )
            if result.action == "gap":
                stats.gaps += 1
            elif result.action == "synced":
                stats.synced += 1
                _validate_book_state(engine.lob, context=f"resync recv_ms={recv_ms}")
            elif result.action == "applied":
                stats.applied += 1

            # call tick hook only when valid and within optional time window
            if engine.depth_synced and engine.snapshot_loaded and on_tick:
                if (time_min_ms is None or recv_ms >= time_min_ms) and (time_max_ms is None or recv_ms < time_max_ms):
                    on_tick(recv_ms, engine)

            push_next(depth_it, "depth")

        elif name == "trade":
            tr: Trade = item
            stats.trade_msgs += 1
            if on_trade:
                if (time_min_ms is None or tr.recv_ms >= time_min_ms) and (time_max_ms is None or tr.recv_ms < time_max_ms):
                    on_trade(tr, engine)
            push_next(trade_it, "trade")

    return stats
