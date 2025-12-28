from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import List

from .base import FillModel, OpenOrder, Fill
from mm.backtest.quotes.base import MarketState


@dataclass
class PoissonFillModel(FillModel):
    A: float
    k: float
    dt_sec: float = 0.1
    seed: int = 42

    def __post_init__(self):
        self.rng = random.Random(self.seed)

    def on_tick(self, market: MarketState, open_orders: List[OpenOrder]) -> List[Fill]:
        fills: List[Fill] = []
        for o in open_orders:
            if o.active_recv_ms > market.recv_ms:
                continue
            if o.expire_recv_ms is not None and market.recv_ms > o.expire_recv_ms:
                continue

            delta = abs(o.price - market.mid)
            lam = self.A * math.exp(-self.k * delta)
            p = 1.0 - math.exp(-lam * self.dt_sec)
            if self.rng.random() < p:
                fills.append(Fill(o.order_id, market.recv_ms, o.price, o.qty, "poisson"))
        return fills
