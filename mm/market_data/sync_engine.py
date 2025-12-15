# mm/market_data/sync_engine.py

from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, List, Optional

from .local_orderbook import LocalOrderBook


@dataclass
class SyncResult:
    action: str  # "buffered" | "synced" | "applied" | "gap"
    details: str = ""


class OrderBookSyncEngine:
    """
    Testable engine:
      - load_snapshot(lastUpdateId, bids, asks)
      - feed_depth_event({U,u,b,a,E})
      - detects gap => returns action "gap"
    """

    def __init__(self, lob: Optional[LocalOrderBook] = None):
        self.lob = lob or LocalOrderBook()
        self.snapshot_loaded = False
        self.depth_synced = False
        self.buffer: List[dict] = []

    def load_snapshot(self, bids, asks, last_update_id: int) -> None:
        self.lob.load_snapshot(bids=bids, asks=asks, last_update_id=last_update_id)
        self.snapshot_loaded = True
        self.depth_synced = False

    def _try_initial_sync(self) -> bool:
        if not self.snapshot_loaded or self.lob.last_update_id is None:
            return False

        lu = self.lob.last_update_id
        self.buffer.sort(key=lambda ev: int(ev.get("u", 0)))

        for ev in list(self.buffer):
            U, u = int(ev["U"]), int(ev["u"])
            if u <= lu:
                self.buffer.remove(ev)
                continue

            bridges = (U <= lu <= u) or (U <= lu + 1 <= u)
            if bridges:
                ok = self.lob.apply_diff(U, u, ev.get("b", []), ev.get("a", []))
                if ok:
                    self.depth_synced = True
                    self.buffer.remove(ev)
                    return True
        return False

    def feed_depth_event(self, ev: dict) -> SyncResult:
        # Always buffer until snapshot exists
        if not self.snapshot_loaded:
            self.buffer.append(ev)
            return SyncResult("buffered", "no_snapshot")

        # Not synced: buffer and attempt initial bridge
        if not self.depth_synced:
            self.buffer.append(ev)
            if self._try_initial_sync():
                return SyncResult("synced", f"lastUpdateId={self.lob.last_update_id}")
            return SyncResult("buffered", "not_synced")

        # Synced: apply sequentially or detect gap
        U, u = int(ev["U"]), int(ev["u"])
        ok = self.lob.apply_diff(U, u, ev.get("b", []), ev.get("a", []))
        if not ok:
            return SyncResult("gap", f"gap_detected U={U} u={u} last={self.lob.last_update_id}")
        return SyncResult("applied", f"lastUpdateId={self.lob.last_update_id}")

    def reset_for_resync(self) -> None:
        """
        Called when a gap is detected. We keep buffering, but require a new snapshot to re-sync.
        """
        self.depth_synced = False
        self.snapshot_loaded = False
        self.lob = LocalOrderBook()
        self.buffer.clear()
