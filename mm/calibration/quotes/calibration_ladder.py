from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence, Optional

from mm.backtest.quotes.base import Quote, MarketState, PositionState


@dataclass
class CalibrationLadderQuoteModel:
    """Calibration quote model: ladder sweep over deltas.

    Design A (ladder sweep) intentionally creates controlled exposure by cycling
    through a fixed list of half-spreads (in ticks). For each dwell block it
    quotes symmetrically around mid:

      bid = mid - delta*tick_size
      ask = mid + delta*tick_size

    The model changes the target delta when:
      - dwell_ms has elapsed since the last change, OR
      - mid has moved beyond mid_move_threshold_ticks (optional)

    This model is meant for calibration only.
    """

    qty: float
    tick_size: float
    deltas: Sequence[int]
    dwell_ms: int = 60_000
    mid_move_threshold_ticks: Optional[int] = None
    two_sided: bool = True
    repost_on_fill: bool = False  # reserved for future use

    # internal state
    _idx: int = 0
    _last_switch_ms: Optional[int] = None
    _anchor_mid: Optional[float] = None

    def _should_switch(self, market: MarketState) -> bool:
        if self._last_switch_ms is None:
            return True

        if int(market.recv_ms) - int(self._last_switch_ms) >= int(self.dwell_ms):
            return True

        if self.mid_move_threshold_ticks is not None and self._anchor_mid is not None:
            threshold = float(self.mid_move_threshold_ticks) * float(self.tick_size)
            if abs(float(market.mid) - float(self._anchor_mid)) >= threshold:
                return True

        return False

    def _advance(self, market: MarketState) -> None:
        if not self.deltas:
            raise ValueError("CalibrationLadderQuoteModel requires non-empty deltas")

        if self._last_switch_ms is None:
            self._idx = 0
        else:
            self._idx = (self._idx + 1) % len(self.deltas)

        self._last_switch_ms = int(market.recv_ms)
        self._anchor_mid = float(market.mid)

    def generate_quotes(self, market: MarketState, position: PositionState) -> List[Quote]:
        if self._should_switch(market):
            self._advance(market)

        delta_ticks = int(self.deltas[self._idx])
        d = float(delta_ticks) * float(self.tick_size)
        bid = float(market.mid) - d
        ask = float(market.mid) + d

        quotes = [Quote(side="BUY", price=bid, qty=self.qty, ttl_ms=None)]
        if self.two_sided:
            quotes.append(Quote(side="SELL", price=ask, qty=self.qty, ttl_ms=None))
        return quotes
