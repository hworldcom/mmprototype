# mm/market_data/local_orderbook.py

from typing import Dict, List, Tuple


class LocalOrderBook:
    def __init__(self):
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_update_id: int | None = None

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

    def apply_diff(self, U: int, u: int, bids, asks) -> bool:
        if self.last_update_id is None:
            return False

        if u <= self.last_update_id:
            return True

        if U > self.last_update_id + 1:
            return False

        for p_str, q_str in bids:
            p, q = float(p_str), float(q_str)
            if q == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q

        for p_str, q_str in asks:
            p, q = float(p_str), float(q_str)
            if q == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q

        self.last_update_id = u
        return True

    def top_n(self, n: int = 5):
        bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)
        asks = sorted(self.asks.items(), key=lambda x: x[0])
        return bids[:n], asks[:n]
