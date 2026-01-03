from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import List, Optional, Sequence, Dict, Any

from .base import FillModel, OpenOrder, Fill
from mm.backtest.quotes.base import MarketState


@dataclass
class PoissonFillModel(FillModel):
    """Poisson (A-S style) fill model.

    Model: lambda(delta_ticks) = A * exp(-k * delta_ticks)

    We simulate fills on each tick using:
        p = 1 - exp(-lambda * dt)

    Notes:
    - Distances are measured in *ticks* (not absolute price units).
    - `dt_ms` is the simulation time step for the hazard approximation.
    """

    A: float
    k: float
    tick_size: float = 0.01
    dt_ms: int = 100
    seed: int = 42

    def __post_init__(self):
        self.dt_sec = max(float(self.dt_ms) / 1000.0, 1e-9)
        self.rng = random.Random(self.seed)

    def _delta_ticks(self, price: float, mid: float) -> float:
        if self.tick_size <= 0:
            return abs(price - mid)
        return abs(price - mid) / self.tick_size

    def on_tick(self, market: MarketState, open_orders: List[OpenOrder]) -> List[Fill]:
        fills: List[Fill] = []
        for o in open_orders:
            if o.active_recv_ms > market.recv_ms:
                continue
            if o.expire_recv_ms is not None and market.recv_ms > o.expire_recv_ms:
                continue

            delta_ticks = self._delta_ticks(o.price, market.mid)
            lam = self.A * math.exp(-self.k * delta_ticks)
            p = 1.0 - math.exp(-lam * self.dt_sec)
            if self.rng.random() < p:
                fills.append(Fill(o.order_id, market.recv_ms, o.price, o.qty, "poisson"))
        return fills


@dataclass
class TimeVaryingPoissonFillModel(FillModel):
    """Poisson fill model with piecewise-constant parameters over time.

    schedule entries:
      - start_ms (inclusive)
      - end_ms (exclusive)
      - A
      - k

    At each tick, the model selects the active segment by recv_ms.
    """

    schedule: Sequence[Dict[str, Any]]
    tick_size: float = 0.01
    dt_ms: int = 100
    seed: int = 42

    def __post_init__(self):
        self.dt_sec = max(float(self.dt_ms) / 1000.0, 1e-9)
        self.rng = random.Random(self.seed)
        # Normalize schedule to sorted list
        self._sched = sorted(
            [
                {
                    "start_ms": int(s["start_ms"]),
                    "end_ms": int(s["end_ms"]),
                    "A": float(s["A"]),
                    "k": float(s["k"]),
                }
                for s in self.schedule
            ],
            key=lambda x: x["start_ms"],
        )
        self._idx = 0

    def _delta_ticks(self, price: float, mid: float) -> float:
        if self.tick_size <= 0:
            return abs(price - mid)
        return abs(price - mid) / self.tick_size

    def _active_params(self, recv_ms: int) -> Optional[Dict[str, float]]:
        if not self._sched:
            return None

        # Move pointer forward as time progresses
        while self._idx < len(self._sched) - 1 and recv_ms >= self._sched[self._idx]["end_ms"]:
            self._idx += 1

        s = self._sched[self._idx]
        if recv_ms < s["start_ms"] or recv_ms >= s["end_ms"]:
            return None
        return {"A": s["A"], "k": s["k"]}

    def on_tick(self, market: MarketState, open_orders: List[OpenOrder]) -> List[Fill]:
        params = self._active_params(market.recv_ms)
        if params is None:
            return []
        A = params["A"]
        k = params["k"]

        fills: List[Fill] = []
        for o in open_orders:
            if o.active_recv_ms > market.recv_ms:
                continue
            if o.expire_recv_ms is not None and market.recv_ms > o.expire_recv_ms:
                continue

            delta_ticks = self._delta_ticks(o.price, market.mid)
            lam = A * math.exp(-k * delta_ticks)
            p = 1.0 - math.exp(-lam * self.dt_sec)
            if self.rng.random() < p:
                fills.append(Fill(o.order_id, market.recv_ms, o.price, o.qty, "poisson_schedule"))
        return fills
