from __future__ import annotations

from dataclasses import dataclass
from typing import List
import logging

from mm.backtest.quotes.base import QuoteModel, MarketState, PositionState, Quote


logger = logging.getLogger(__name__)


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
                logger.debug(
                    "DROP_QUOTE reason=min_notional side=%s px=%.8f qty=%.8f notional=%.8f min_notional=%.8f",
                    side,
                    px,
                    qty,
                    notional,
                    float(self.min_notional),
                )
                continue

            if side == "BUY":
                # Require enough cash to cover notional + maker fees (worst-case).
                need = notional * (1.0 + float(self.maker_fee_rate))
                if cash + 1e-12 < need:
                    logger.debug(
                        "DROP_QUOTE reason=insufficient_cash side=%s px=%.8f qty=%.8f need=%.8f cash=%.8f fee_rate=%.6f",
                        side,
                        px,
                        qty,
                        need,
                        cash,
                        float(self.maker_fee_rate),
                    )
                    continue
            elif side == "SELL":
                if inv + 1e-12 < qty:
                    logger.debug(
                        "DROP_QUOTE reason=insufficient_inventory side=%s px=%.8f qty=%.8f inv=%.8f",
                        side,
                        px,
                        qty,
                        inv,
                    )
                    continue

            res.append(q)

        return res
