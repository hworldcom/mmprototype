# mm/market_data/sync_engine.py

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional

from .local_orderbook import LocalOrderBook


@dataclass
class SyncResult:
    action: str
    details: str = ""


class OrderBookSyncEngine:
    """
    Pure state machine for Binance diff-depth local book correctness.

    Responsibilities:
      - buffer events until snapshot is available
      - bridge snapshot lastUpdateId to WS diffs (initial sync)
      - apply diffs sequentially once synced
      - detect gaps and signal "gap"

    Non-responsibilities:
      - fetching snapshots
      - websocket lifecycle
      - file writing / logging policies
    """

    def __init__(self, lob: Optional[LocalOrderBook] = None):
        self.lob = lob or LocalOrderBook()
        self.snapshot_loaded = False
        self.depth_synced = False
        self.buffer: List[dict] = []

    def adopt_snapshot(self, lob: LocalOrderBook) -> None:
        """
        Adopt a fully-loaded LocalOrderBook from a REST snapshot.
        Resets sync state but keeps any already-buffered WS events.
        """
        self.lob = lob
        self.snapshot_loaded = True
        self.depth_synced = False
        # keep buffer (events may have arrived before snapshot)

    def reset_for_resync(self) -> None:
        """
        Called when a gap is detected.
        We keep things simple: clear the book and buffer and require a fresh snapshot.
        """
        self.lob = LocalOrderBook()
        self.snapshot_loaded = False
        self.depth_synced = False
        self.buffer.clear()

    def _try_initial_sync(self) -> bool:
        if not self.snapshot_loaded or self.lob.last_update_id is None:
            return False

        lu = self.lob.last_update_id
        self.buffer.sort(key=lambda ev: int(ev.get("u", 0)))

        # NEW: if the earliest buffered diff starts after lu+1, we can never bridge
        if self.buffer:
            min_U = int(self.buffer[0]["U"])
            if min_U > lu + 1:
                # mark a special flag by raising, or handle via a separate method;
                # see below for the clean way via returning a SyncResult("gap", ...)
                return False

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

        return False

    def feed_depth_event(self, ev: dict) -> SyncResult:
        """
        Feed one WS depth-diff event.

        Returns:
          - buffered: not enough state to apply yet
          - synced: initial bridge completed (book now valid)
          - applied: sequential update applied (book remains valid)
          - gap: sequence gap detected (book invalid until resync)
        """
        # No snapshot: buffer everything
        if not self.snapshot_loaded:
            self.buffer.append(ev)
            return SyncResult("buffered", "no_snapshot")

        if not self.depth_synced:
            self.buffer.append(ev)

            if self.lob.last_update_id is not None:
                self.buffer.sort(key=lambda e: int(e.get("u", 0)))
                lu = self.lob.last_update_id
                min_U = int(self.buffer[0]["U"])
                if min_U > lu + 1:
                    return SyncResult("gap", f"bridge_impossible min_U={min_U} lastUpdateId={lu}")

            if self._try_initial_sync():
                return SyncResult("synced", f"lastUpdateId={self.lob.last_update_id}")
            return SyncResult("buffered", "not_synced")

        U, u = int(ev["U"]), int(ev["u"])
        ok = self.lob.apply_diff(U, u, ev.get("b", []), ev.get("a", []))
        if not ok:
            return SyncResult("gap", f"gap U={U} u={u} last={self.lob.last_update_id}")
        return SyncResult("applied", f"lastUpdateId={self.lob.last_update_id}")
