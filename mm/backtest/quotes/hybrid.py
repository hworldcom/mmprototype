from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List

from .base import Quote, MarketState, PositionState, QuoteModel, Quote


@dataclass
class HybridASMicrostructureQuoteModel(QuoteModel):
    qty: float
    gamma: float
    sigma: float
    k: float
    tau_sec: float
    tick_size: float = 0.01
    anchor_band_ticks: int = 3

    def generate_quotes(self, market: MarketState, position: PositionState) -> List[Quote]:
        q = position.inventory
        tau = max(0.0, self.tau_sec)

        r = market.mid - q * self.gamma * (self.sigma ** 2) * tau
        half_spread = 0.5 * (self.gamma * (self.sigma ** 2) * tau) + (1.0 / self.gamma) * math.log(1.0 + self.gamma / self.k)

        bid = r - half_spread
        ask = r + half_spread

        band = self.anchor_band_ticks * self.tick_size
        bid = min(bid, market.best_bid + band)
        ask = max(ask, market.best_ask - band)

        bid = min(bid, market.best_ask - self.tick_size)
        ask = max(ask, market.best_bid + self.tick_size)

        return [Quote("BUY", bid, self.qty), Quote("SELL", ask, self.qty)]
