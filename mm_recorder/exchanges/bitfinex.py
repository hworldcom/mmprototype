from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List

from .base import ExchangeAdapter
from .types import BookSnapshot, DepthDiff, Trade
from mm_core.checksum.bitfinex import BitfinexSyncEngine


def _to_ms(ts: Any) -> int:
    if ts is None:
        return 0
    try:
        val = float(ts)
    except Exception:
        return 0
    # Bitfinex v2 generally uses ms; fall back to seconds if small.
    return int(val if val > 1e12 else val * 1000)


class BitfinexAdapter(ExchangeAdapter):
    name = "bitfinex"
    sync_mode = "checksum"

    def __init__(self) -> None:
        self.book_chan_id: int | None = None
        self.trades_chan_id: int | None = None

    def normalize_symbol(self, symbol: str) -> str:
        s = symbol.replace("/", "").replace("-", "").replace(":", "").strip().upper()
        if not s:
            return s
        # Bitfinex pairs use leading "t"
        return s if s.startswith("T") else f"T{s}"

    def normalize_depth(self, depth: int) -> int:
        # Bitfinex checksum uses top 25 bids/asks.
        return 25

    def ws_url(self, symbol: str) -> str:
        return "wss://api-pub.bitfinex.com/ws/2"

    def subscribe_messages(self, symbol: str, depth: int) -> list:
        pair = self.normalize_symbol(symbol)
        return [
            {"event": "conf", "flags": 131072},
            {"event": "subscribe", "channel": "book", "pair": pair, "prec": "P0", "freq": "F0", "len": 25},
            {"event": "subscribe", "channel": "trades", "pair": pair},
        ]

    @property
    def uses_custom_ws_messages(self) -> bool:
        return True

    def create_sync_engine(self, depth: int):
        return BitfinexSyncEngine(depth=25)

    def parse_ws_message(self, data: dict | list):
        snapshots: List[BookSnapshot] = []
        diffs: List[DepthDiff] = []
        trades: List[Trade] = []

        if isinstance(data, dict):
            if data.get("event") == "subscribed":
                chan_id = data.get("chanId")
                channel = data.get("channel")
                if channel == "book":
                    self.book_chan_id = int(chan_id)
                elif channel == "trades":
                    self.trades_chan_id = int(chan_id)
            return snapshots, diffs, trades

        if not isinstance(data, list) or not data:
            return snapshots, diffs, trades

        chan_id = data[0]
        if chan_id == self.book_chan_id:
            if len(data) >= 2 and data[1] == "hb":
                return snapshots, diffs, trades
            if len(data) >= 2 and data[1] == "cs":
                diffs.append(
                    DepthDiff(
                        event_time_ms=0,
                        U=0,
                        u=0,
                        bids=[],
                        asks=[],
                        checksum=int(data[2]),
                        raw={"type": "checksum", "checksum": int(data[2])},
                    )
                )
                return snapshots, diffs, trades
            # Snapshot: [chanId, [ [price, count, amount], ... ]]
            if len(data) == 2 and isinstance(data[1], list) and data[1] and isinstance(data[1][0], list):
                bids: List[List[str]] = []
                asks: List[List[str]] = []
                for price, count, amount in data[1]:
                    amount_val = Decimal(str(amount))
                    row = [str(price), str(amount_val)]
                    if amount_val > 0:
                        bids.append(row)
                    else:
                        asks.append(row)
                snapshots.append(
                    BookSnapshot(
                        event_time_ms=0,
                        bids=bids,
                        asks=asks,
                        checksum=None,
                        raw={"type": "snapshot"},
                    )
                )
                return snapshots, diffs, trades

            # Update: [chanId, price, count, amount]
            if len(data) >= 4:
                price, count, amount = data[1], int(data[2]), Decimal(str(data[3]))
                if count == 0:
                    if amount < 0:
                        asks = [[str(price), "0"]]
                        bids = []
                    else:
                        bids = [[str(price), "0"]]
                        asks = []
                elif amount > 0:
                    bids = [[str(price), str(amount)]]
                    asks = []
                else:
                    asks = [[str(price), str(amount)]]
                    bids = []
                diffs.append(
                    DepthDiff(
                        event_time_ms=0,
                        U=0,
                        u=0,
                        bids=bids,
                        asks=asks,
                        checksum=None,
                        raw={"type": "update", "price": str(price), "count": count, "amount": str(amount)},
                    )
                )
                return snapshots, diffs, trades

        if chan_id == self.trades_chan_id:
            if len(data) >= 2 and data[1] == "hb":
                return snapshots, diffs, trades
            # Snapshot: [chanId, [ [seq|id, ts, price, amount], ... ]]
            if len(data) == 2 and isinstance(data[1], list):
                for entry in data[1]:
                    if not isinstance(entry, list):
                        continue
                    if len(entry) == 4:
                        seq, ts, price, amount = entry
                        trade_id = int(seq)
                    else:
                        seq, trade_id, ts, price, amount = entry[0:5]
                    amount_val = float(amount)
                    trade_time_ms = _to_ms(ts)
                    trades.append(
                        Trade(
                            event_time_ms=trade_time_ms,
                            trade_id=int(trade_id),
                            trade_time_ms=trade_time_ms,
                            price=float(price),
                            qty=abs(amount_val),
                            is_buyer_maker=0 if amount_val > 0 else 1,
                            raw={"type": "snapshot", "entry": entry},
                        )
                    )
                return snapshots, diffs, trades

            # Updates: [chanId, 'te'|'tu', ...]
            if len(data) >= 3 and data[1] in ("te", "tu"):
                if data[1] != "tu":
                    return snapshots, diffs, trades
                seq = data[2]
                trade_id = data[3]
                ts = data[4]
                price = data[5]
                amount = data[6]
                amount_val = float(amount)
                trade_time_ms = _to_ms(ts)
                trades.append(
                    Trade(
                        event_time_ms=trade_time_ms,
                        trade_id=int(trade_id) if trade_id is not None else int(seq),
                        trade_time_ms=trade_time_ms,
                        price=float(price),
                        qty=abs(amount_val),
                        is_buyer_maker=0 if amount_val > 0 else 1,
                        raw={"type": data[1], "entry": data},
                    )
                )
                return snapshots, diffs, trades

        return snapshots, diffs, trades

    def parse_depth(self, data: dict) -> DepthDiff:
        raise RuntimeError("Bitfinex uses custom WS parsing; parse_depth is unused.")

    def parse_trade(self, data: dict) -> Trade:
        raise RuntimeError("Bitfinex uses custom WS parsing; parse_trade is unused.")
