from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List

from .base import Quote, MarketState, PositionState, QuoteModel


@dataclass
class AvellanedaStoikovQuoteModel(QuoteModel):
    qty: float
    gamma: float
    sigma: float
    k: float
    tau_sec: float  # fixed remaining horizon for a first pass

    def generate_quotes(self, market: MarketState, position: PositionState) -> List[Quote]:
        q = position.inventory
        tau = max(0.0, self.tau_sec)

        r = market.mid - q * self.gamma * (self.sigma ** 2) * tau
        half_spread = 0.5 * (self.gamma * (self.sigma ** 2) * tau) + (1.0 / self.gamma) * math.log(1.0 + self.gamma / self.k)

        bid = r - half_spread
        ask = r + half_spread

        return [Quote("BUY", bid, self.qty), Quote("SELL", ask, self.qty)]
