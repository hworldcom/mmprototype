from __future__ import annotations

from dataclasses import dataclass
from typing import List

from .base import Quote, MarketState, PositionState, QuoteModel


@dataclass
class InventorySkewQuoteModel(QuoteModel):
    qty: float
    half_spread: float
    inv_skew: float  # price shift per 1 unit inventory

    def generate_quotes(self, market: MarketState, position: PositionState) -> List[Quote]:
        q = position.inventory
        skew = self.inv_skew * q
        bid = market.mid - self.half_spread - skew
        ask = market.mid + self.half_spread - skew
        return [Quote("BUY", bid, self.qty), Quote("SELL", ask, self.qty)]
