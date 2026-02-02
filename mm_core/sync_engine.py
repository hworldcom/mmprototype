from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from .local_orderbook import LocalOrderBook


@dataclass
class SyncResult:
    action: str  # "buffered" | "synced" | "applied" | "gap"
    details: str = ""


class OrderBookSyncEngine:
    """Pure state machine for Binance diff-depth book synchronization.

    This module is intentionally I/O-free (no WS, no REST, no files). It is suitable
    for unit testing and for production use inside the recorder/strategy.

    Key behaviors:
      - buffer until snapshot exists
      - bridge snapshot lastUpdateId using buffered WS diffs
      - apply diffs sequentially once synced
      - detect gaps; additionally detect 'bridge impossible' (min_U > lastUpdateId+1)
    """

    def __init__(self, lob: Optional[LocalOrderBook] = None):
        self.lob = lob or LocalOrderBook()
        self.snapshot_loaded: bool = False
        self.depth_synced: bool = False
        self.buffer: List[dict] = []
        # Updated by replay/recorder to carry global ordering through callbacks.
        self.last_recv_seq: Optional[int] = None

    def adopt_snapshot(self, lob: LocalOrderBook) -> None:
        """Adopt a fully-loaded snapshot book. Resets sync state but keeps buffered events."""
        self.lob = lob
        self.snapshot_loaded = True
        self.depth_synced = False

    def reset_for_resync(self) -> None:
        """Clear state for a fresh snapshot after a gap."""
        self.lob = LocalOrderBook()
        self.snapshot_loaded = False
        self.depth_synced = False
        self.buffer.clear()

    def _try_initial_sync(self) -> SyncResult:
        if not self.snapshot_loaded or self.lob.last_update_id is None:
            return SyncResult("buffered", "no_snapshot")

        lu = int(self.lob.last_update_id)

        # Sort by starting update id so we reason about the earliest possible bridge correctly.
        self.buffer.sort(key=lambda ev: int(ev.get("U", 0)))

        # If the earliest buffered event starts after lu+1, bridging is impossible for this snapshot.
        if self.buffer:
            min_U = int(self.buffer[0].get("U", 0))
            if min_U > lu + 1:
                return SyncResult("gap", f"bridge_impossible min_U={min_U} lastUpdateId={lu}")

        bridged = False

        for ev in list(self.buffer):
            U, u = int(ev["U"]), int(ev["u"])

            if u <= lu:
                self.buffer.remove(ev)
                continue

            if not self.depth_synced:
                bridges = (U <= lu <= u) or (U <= lu + 1 <= u) # TODO: having two checks for bridges redundant, do we need or?
                if not bridges:
                    continue

                ok = self.lob.apply_diff(U, u, ev.get("b", []), ev.get("a", []))
                if not ok:
                    return SyncResult("gap", f"bridge_apply_failed U={U} u={u} lastUpdateId={lu}")

                self.depth_synced = True
                bridged = True
                lu = int(self.lob.last_update_id)
                self.buffer.remove(ev)
                continue

            ok = self.lob.apply_diff(U, u, ev.get("b", []), ev.get("a", []))
            if not ok:
                return SyncResult("gap", f"gap U={U} u={u} last={self.lob.last_update_id}")

            lu = int(self.lob.last_update_id)
            self.buffer.remove(ev)

        if self.depth_synced:
            action = "synced" if bridged else "applied"
            return SyncResult(action, f"lastUpdateId={self.lob.last_update_id}")

        return SyncResult("buffered", "not_synced")

    def feed_depth_event(self, ev: dict) -> SyncResult:
        """Feed one WS depth-diff event."""
        if not self.snapshot_loaded:
            self.buffer.append(ev)
            return SyncResult("buffered", "no_snapshot")

        if not self.depth_synced:
            self.buffer.append(ev)
            return self._try_initial_sync()

        U, u = int(ev["U"]), int(ev["u"])
        ok = self.lob.apply_diff(U, u, ev.get("b", []), ev.get("a", []))
        if not ok:
            return SyncResult("gap", f"gap U={U} u={u} last={self.lob.last_update_id}")
        return SyncResult("applied", f"lastUpdateId={self.lob.last_update_id}")
