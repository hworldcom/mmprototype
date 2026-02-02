from __future__ import annotations

import binascii
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from mm_core.local_orderbook import LocalOrderBook
from mm_core.sync_engine import SyncResult
from mm_recorder.exchanges.types import DepthDiff, BookSnapshot


def _norm_crc_str(val: str) -> str:
    s = str(val)
    s = s.replace(".", "")
    s = s.lstrip("0")
    return s if s else "0"


class KrakenBook:
    def __init__(self, depth: int) -> None:
        self.bids: Dict[Decimal, str] = {}
        self.asks: Dict[Decimal, str] = {}
        self.depth = int(depth)

    def _trim(self) -> None:
        if self.depth <= 0:
            return
        if len(self.bids) > self.depth:
            bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[: self.depth]
            self.bids = {p: q for p, q in bids_sorted}
        if len(self.asks) > self.depth:
            asks_sorted = sorted(self.asks.items(), key=lambda x: x[0])[: self.depth]
            self.asks = {p: q for p, q in asks_sorted}

    def load_snapshot(self, bids: List[List[str]], asks: List[List[str]]) -> None:
        self.bids.clear()
        self.asks.clear()
        for p, q in bids:
            if Decimal(str(q)) != 0:
                self.bids[Decimal(p)] = str(q)
        for p, q in asks:
            if Decimal(str(q)) != 0:
                self.asks[Decimal(p)] = str(q)
        self._trim()

    def apply_update(self, bids: List[List[str]], asks: List[List[str]]) -> None:
        for p, q in bids:
            price = Decimal(p)
            qty = str(q)
            if Decimal(qty) == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty
        for p, q in asks:
            price = Decimal(p)
            qty = str(q)
            if Decimal(qty) == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty
        self._trim()

    def top_n(self, n: int) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        return (
            [(float(p), float(q)) for p, q in bids_sorted],
            [(float(p), float(q)) for p, q in asks_sorted],
        )

    def checksum(self, n: int = 10) -> int:
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        parts: List[str] = []
        for p, q in asks_sorted:
            parts.append(_norm_crc_str(p))
            parts.append(_norm_crc_str(q))
        for p, q in bids_sorted:
            parts.append(_norm_crc_str(p))
            parts.append(_norm_crc_str(q))
        payload = "".join(parts).encode()
        return binascii.crc32(payload) & 0xFFFFFFFF


@dataclass
class KrakenSyncEngine:
    lob: LocalOrderBook
    depth_synced: bool = False
    snapshot_loaded: bool = False
    buffer: List[DepthDiff] = None

    def __init__(self, depth: int) -> None:
        self.depth = int(depth)
        self.book = KrakenBook(self.depth)
        self.lob = LocalOrderBook()
        self.depth_synced = False
        self.snapshot_loaded = False
        self.buffer = []
        self.last_recv_seq: Optional[int] = None

    def adopt_snapshot(self, snapshot: BookSnapshot) -> None:
        self.book.load_snapshot(snapshot.bids, snapshot.asks)
        bids, asks = self.book.top_n(self.depth)
        self.lob.bids = {p: q for p, q in bids}
        self.lob.asks = {p: q for p, q in asks}
        self.snapshot_loaded = True
        self.depth_synced = True
        if self.buffer:
            for ev in list(self.buffer):
                self.feed_depth_event(ev)
            self.buffer.clear()

    def reset_for_resync(self) -> None:
        self.book = KrakenBook(self.depth)
        self.lob = LocalOrderBook()
        self.snapshot_loaded = False
        self.depth_synced = False
        self.buffer.clear()

    def feed_depth_event(self, ev: DepthDiff) -> SyncResult:
        if not self.snapshot_loaded:
            self.buffer.append(ev)
            return SyncResult("buffered", "no_snapshot")

        self.book.apply_update(ev.bids, ev.asks)
        bids, asks = self.book.top_n(self.depth)
        self.lob.bids = {p: q for p, q in bids}
        self.lob.asks = {p: q for p, q in asks}

        if ev.checksum is not None:
            calc = self.book.checksum(10)
            if int(calc) != int(ev.checksum):
                return SyncResult("gap", f"checksum_mismatch expected={ev.checksum} got={calc}")

        return SyncResult("applied", "checksum_ok")
