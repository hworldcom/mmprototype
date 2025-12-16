from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class LocalOrderBook:
    """In-memory L2 book keyed by price.

    Notes:
      - Uses float keys for simplicity. This is fine for recording/replay, but for production
        execution you will likely want Decimal/int ticks for exactness.
      - last_update_id follows Binance depth diff semantics.
    """

    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    last_update_id: Optional[int] = None

    def load_snapshot(self, bids: List[List[str]], asks: List[List[str]], last_update_id: int) -> None:
        self.bids.clear()
        self.asks.clear()

        for p_s, q_s in bids:
            p = float(p_s)
            q = float(q_s)
            if q != 0.0:
                self.bids[p] = q

        for p_s, q_s in asks:
            p = float(p_s)
            q = float(q_s)
            if q != 0.0:
                self.asks[p] = q

        self.last_update_id = int(last_update_id)

    def apply_diff(self, U: int, u: int, bids, asks) -> bool:
        """Apply a Binance diff-depth update.

        Returns:
          True  -> applied or safely ignored (stale)
          False -> sequence gap detected (book invalid; resync required)
        """
        if self.last_update_id is None:
            return False

        U = int(U)
        u = int(u)
        last = int(self.last_update_id)

        # stale event
        if u <= last:
            return True

        # gap
        if U > last + 1:
            return False

        # apply bids
        for p_s, q_s in bids:
            p = float(p_s)
            q = float(q_s)
            if q == 0.0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q

        # apply asks
        for p_s, q_s in asks:
            p = float(p_s)
            q = float(q_s)
            if q == 0.0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q

        self.last_update_id = u
        return True

    def top_n(self, n: int) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        return bids_sorted, asks_sorted
