from __future__ import annotations

from dataclasses import dataclass, field
from itertools import islice
from typing import List, Optional, Tuple

from sortedcontainers import SortedDict


@dataclass
class LocalOrderBook:
    """In-memory L2 book keyed by price.

    Notes:
      - Uses float keys for simplicity. This is fine for recording/replay, but for production
        execution you will likely want Decimal/int ticks for exactness.
      - last_update_id follows Binance depth diff semantics.
    """

    bids: SortedDict = field(default_factory=SortedDict)
    asks: SortedDict = field(default_factory=SortedDict)
    last_update_id: Optional[int] = None

    def load_snapshot(self, bids: List[List[str]], asks: List[List[str]], last_update_id: int) -> None:
        self.bids.clear()
        self.asks.clear()

        for price_snapshot, quantity_snapshot in bids:
            price = float(price_snapshot)
            quantity = float(quantity_snapshot)
            if quantity != 0.0:
                self.bids[price] = quantity

        for price_snapshot, quantity_snapshot in asks:
            price = float(price_snapshot)
            quantity = float(quantity_snapshot)
            if quantity != 0.0:
                self.asks[price] = quantity

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
        for price_snapshot, quantity_snapshot in bids:
            price = float(price_snapshot)
            quantity = float(quantity_snapshot)
            if quantity == 0.0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = quantity

        # apply asks
        for price_snapshot, quantity_snapshot in asks:
            price = float(price_snapshot)
            quantity = float(quantity_snapshot)
            if quantity == 0.0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = quantity

        self.last_update_id = u
        return True

    def top_n(self, n: int) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        if n <= 0:
            return [], []
        bids_iter = reversed(self.bids.items())
        asks_iter = iter(self.asks.items())
        bids_sorted = list(islice(bids_iter, n))
        asks_sorted = list(islice(asks_iter, n))
        return bids_sorted, asks_sorted
