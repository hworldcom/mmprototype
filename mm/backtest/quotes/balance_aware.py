from __future__ import annotations

from dataclasses import dataclass
from typing import List

from mm.backtest.quotes.base import QuoteModel, MarketState, PositionState, Quote


@dataclass
class BalanceAwareQuoteModel(QuoteModel):
    """Filters quotes that are not fundable under spot constraints.

    This prevents repeated rejected or skipped quotes when starting with zero inventory/cash.
    """
    inner: QuoteModel
    maker_fee_rate: float = 0.0
    min_notional: float = 0.0

    def generate_quotes(self, market: MarketState, position: PositionState) -> List[Quote]:
        quotes = self.inner.generate_quotes(market, position)
        res: List[Quote] = []
        cash = float(position.cash)
        inv = float(position.inventory)

        for q in quotes:
            side = q.side.upper()
            px = float(q.price)
            qty = float(q.qty)
            notional = px * qty

            if self.min_notional and notional < self.min_notional:
                continue

            if side == "BUY":
                # Require enough cash to cover notional + maker fees (worst-case).
                need = notional * (1.0 + float(self.maker_fee_rate))
                if cash + 1e-12 < need:
                    continue
            elif side == "SELL":
                if inv + 1e-12 < qty:
                    continue

            res.append(q)

        return res
