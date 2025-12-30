from __future__ import annotations

from dataclasses import dataclass
from typing import List

from mm.backtest.quotes.base import Quote, MarketState, PositionState


@dataclass
class FixedSpreadQuoteModel:
    """Calibration quote model: always quote at a fixed half-spread.

    The model quotes both sides at a fixed distance (in ticks) from mid:
      bid = mid - delta_ticks * tick_size
      ask = mid + delta_ticks * tick_size

    This is useful for Design B (fixed-spread runs).
    """

    qty: float
    tick_size: float
    delta_ticks: int

    def generate_quotes(self, market: MarketState, position: PositionState) -> List[Quote]:
        d = float(self.delta_ticks) * float(self.tick_size)
        bid = market.mid - d
        ask = market.mid + d

        return [
            Quote(side="BUY", price=bid, qty=self.qty, ttl_ms=None),
            Quote(side="SELL", price=ask, qty=self.qty, ttl_ms=None),
        ]
