from __future__ import annotations

from dataclasses import dataclass
from typing import List

from .base import Quote, MarketState, PositionState, QuoteModel, Quote


@dataclass
class MicrostructureQuoteModel(QuoteModel):
    qty: float
    join: bool = True
    tick_size: float = 0.01
    inv_skew: float = 0.0
    imbalance_aggression: float = 0.0

    def generate_quotes(self, market: MarketState, position: PositionState) -> List[Quote]:
        q = position.inventory
        skew = self.inv_skew * q
        imb = market.imbalance if market.imbalance is not None else 0.0
        imb_shift = self.imbalance_aggression * imb

        if self.join:
            bid = market.best_bid - skew + imb_shift
            ask = market.best_ask - skew + imb_shift
        else:
            bid = (market.best_bid + self.tick_size) - skew + imb_shift
            ask = (market.best_ask - self.tick_size) - skew + imb_shift

        # Do not cross
        bid = min(bid, market.best_ask - self.tick_size)
        ask = max(ask, market.best_bid + self.tick_size)

        return [Quote("BUY", bid, self.qty), Quote("SELL", ask, self.qty)]
