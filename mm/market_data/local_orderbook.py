from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

@dataclass
class LocalOrderBook:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    last_update_id: Optional[int] = None

    def load_snapshot(self, bids: List[List[str]], asks: List[List[str]], last_update_id: int) -> None:
        self.bids.clear(); self.asks.clear()
        for p, q in bids:
            self.bids[float(p)] = float(q)
        for p, q in asks:
            self.asks[float(p)] = float(q)
        self.last_update_id = int(last_update_id)

    def apply_diff(self, U: int, u: int, bids: List[List[str]], asks: List[List[str]]) -> bool:
        if self.last_update_id is None:
            return False
        last = int(self.last_update_id)
        U = int(U); u = int(u)

        if u <= last:
            return True
        if U > last + 1:
            return False

        for p_s, q_s in bids:
            p = float(p_s); q = float(q_s)
            if q == 0.0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q

        for p_s, q_s in asks:
            p = float(p_s); q = float(q_s)
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
