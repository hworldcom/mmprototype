from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple

@dataclass
class LocalOrderBook:
    bids: Dict[float, float]
    asks: Dict[float, float]
    last_update_id: int | None = None

    def __init__(self):
        self.bids = {}
        self.asks = {}
        self.last_update_id = None

    def load_snapshot(self, bids: List[List[str]], asks: List[List[str]], last_update_id: int) -> None:
        self.bids.clear()
        self.asks.clear()
        for p_str, q_str in bids:
            p, q = float(p_str), float(q_str)
            if q > 0:
                self.bids[p] = q
        for p_str, q_str in asks:
            p, q = float(p_str), float(q_str)
            if q > 0:
                self.asks[p] = q
        self.last_update_id = last_update_id

    def apply_diff(self, U: int, u: int, bids: List[List[str]], asks: List[List[str]]) -> bool:
        """
        Returns:
          True  -> applied or safely ignored
          False -> sequence gap detected (must resync)
        """
        if self.last_update_id is None:
            return False

        if u <= self.last_update_id:
            return True  # ignore old event

        if U > self.last_update_id + 1:
            return False  # gap detected

        for p_str, q_str in bids:
            p, q = float(p_str), float(q_str)
            if q == 0.0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q

        for p_str, q_str in asks:
            p, q = float(p_str), float(q_str)
            if q == 0.0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q

        self.last_update_id = u
        return True

    def top_n(self, n: int = 5) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        bids_sorted = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        asks_sorted = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        return bids_sorted, asks_sorted
