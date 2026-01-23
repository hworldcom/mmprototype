"""Virtual probe calibration engine.

This module implements a faster *order-free* alternative to the calibration
ladder sweep.

Instead of creating simulated orders (PLACE/CANCEL/ACK) in the
``PaperExchange`` on each quote refresh, we maintain *virtual probes*.

The key design choice is how to treat multiple deltas (quote distances). This
implementation evaluates **all configured deltas simultaneously** within a
window:

* For each dwell block we anchor the mid price (``mid_anchor``).
* For each configured delta (in ticks), we define virtual bid/ask probe prices:

  - bid_probe(delta) = mid_anchor - delta * tick_size
  - ask_probe(delta) = mid_anchor + delta * tick_size

* Exposure accrues **independently for each delta** over time while the
  anchored mid is defined.
* A trade is counted as a *hit* (fill event) for **each** delta it crosses.

This makes the per-delta exposures comparable (typically each delta sees close
to the full window length), which is the intended input for
``mm.calibration.poisson_fit``.

The output is a bucketed exposure table compatible with
``mm.calibration.poisson_fit``.

Important limitations
---------------------
Virtual probes are designed for *fill intensity calibration* only. They do not
attempt to model queue position, partial fills, order rejections, or
post-only/maker-taker constraints. Those belong in the PaperExchange and are
still used for realistic end-to-end backtests.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

from mm.backtest.replay import replay_day
from mm.backtest.io import Trade

log = logging.getLogger("calibration.virtual_probes")


@dataclass
class VirtualProbeResult:
    points: pd.DataFrame
    stats: Dict[str, float | int]


def _best_bid_ask(engine) -> Tuple[Optional[float], Optional[float]]:
    """Return best bid/ask from an OrderBookSyncEngine."""

    lob = getattr(engine, "lob", None)
    if lob is None:
        return None, None
    try:
        best_bid = max(lob.bids) if lob.bids else None
        best_ask = min(lob.asks) if lob.asks else None
        if best_bid is None or best_ask is None:
            return None, None
        return float(best_bid), float(best_ask)
    except Exception:
        return None, None


def run_virtual_ladder_window(
    *,
    data_root: Path,
    symbol: str,
    yyyymmdd: str,
    tick_size: float,
    deltas: Sequence[int],
    dwell_ms: int,
    mid_move_threshold_ticks: Optional[int],
    time_min_ms: int,
    time_max_ms: int,
    max_delta_ticks: int = 50,
    min_exposure_s: float = 5.0,
) -> VirtualProbeResult:
    """Run virtual probes over a (time_min_ms, time_max_ms) window.

    Returns a DataFrame with the same schema as ``compute_bucketed_exposure``:
    ``delta_bucket, exposure_s, fill_events, filled_qty, lambda_events_per_s, ...``
    plus a few diagnostic columns.
    """

    if not deltas:
        raise ValueError("deltas must be non-empty")

    deltas_sorted = sorted({int(d) for d in deltas})

    # Per-delta accumulators.
    exposure_s: Dict[int, float] = {int(d): 0.0 for d in deltas_sorted}
    fill_events: Dict[int, int] = {int(d): 0 for d in deltas_sorted}
    filled_qty: Dict[int, float] = {int(d): 0.0 for d in deltas_sorted}
    bid_hits: Dict[int, int] = {int(d): 0 for d in deltas_sorted}
    ask_hits: Dict[int, int] = {int(d): 0 for d in deltas_sorted}

    # Anchor state.
    last_switch_ms: Optional[int] = None
    anchor_mid: Optional[float] = None
    last_tick_ms: Optional[int] = None

    # Latest mid available (from depth ticks).
    last_mid: Optional[float] = None

    def should_reanchor(now_ms: int, mid: float) -> bool:
        nonlocal last_switch_ms, anchor_mid
        if last_switch_ms is None:
            return True
        if int(now_ms) - int(last_switch_ms) >= int(dwell_ms):
            return True
        if mid_move_threshold_ticks is not None and anchor_mid is not None:
            thr = float(mid_move_threshold_ticks) * float(tick_size)
            if abs(float(mid) - float(anchor_mid)) >= thr:
                return True
        return False

    def reanchor(now_ms: int, mid: float) -> None:
        nonlocal last_switch_ms, anchor_mid
        last_switch_ms = int(now_ms)
        anchor_mid = float(mid)

    def probe_prices(delta_ticks: int) -> Tuple[Optional[float], Optional[float]]:
        if anchor_mid is None:
            return None, None
        d = float(delta_ticks) * float(tick_size)
        return float(anchor_mid) - d, float(anchor_mid) + d

    def on_tick(recv_ms: int, engine) -> None:
        nonlocal last_mid, last_tick_ms
        bb, ba = _best_bid_ask(engine)
        if bb is None or ba is None:
            return
        mid = 0.5 * (bb + ba)
        last_mid = mid

        # Accrue exposure for *all* deltas over the tick interval.
        # Exposure only accrues once an anchor exists (i.e. after the first tick
        # within the window has established anchor_mid).
        if last_tick_ms is not None and anchor_mid is not None:
            dt_s = max(0.0, float(int(recv_ms) - int(last_tick_ms)) / 1000.0)
            for d in deltas_sorted:
                if 0 <= d <= int(max_delta_ticks):
                    exposure_s[d] += dt_s

        # Re-anchor if needed.
        if should_reanchor(int(recv_ms), float(mid)):
            reanchor(int(recv_ms), float(mid))

        last_tick_ms = int(recv_ms)

    def on_trade(tr: Trade, engine) -> None:
        nonlocal last_mid
        # We rely on the latest anchored mid/probe. If we haven't seen any depth
        # yet in this window, we skip the trade.
        if last_mid is None or anchor_mid is None:
            return

        px = float(tr.price)
        qty = float(tr.qty)

        # Count hits for each delta. We treat each trade that crosses a probe as
        # one fill *event* for that delta.
        for d in deltas_sorted:
            if d < 0 or d > int(max_delta_ticks):
                continue
            bid_p, ask_p = probe_prices(d)
            if bid_p is None or ask_p is None:
                continue

            hit = False
            if px <= bid_p:
                bid_hits[d] += 1
                hit = True
            if px >= ask_p:
                ask_hits[d] += 1
                hit = True
            if hit:
                fill_events[d] += 1
                filled_qty[d] += qty

    # Replay only for the window. Note: replay_day still parses the full files;
    # windowing is enforced by on_tick/on_trade time gates.
    replay_day(
        root=Path(data_root),
        symbol=symbol,
        yyyymmdd=yyyymmdd,
        on_tick=on_tick,
        on_trade=on_trade,
        time_min_ms=int(time_min_ms),
        time_max_ms=int(time_max_ms),
    )

    # If we ended with an active anchor, accrue exposure until time_max_ms.
    if last_tick_ms is not None and anchor_mid is not None:
        dt_s = max(0.0, float(int(time_max_ms) - int(last_tick_ms)) / 1000.0)
        for d in deltas_sorted:
            if 0 <= d <= int(max_delta_ticks):
                exposure_s[d] += dt_s

    rows: List[Dict[str, object]] = []
    for d in deltas_sorted:
        ex = float(exposure_s.get(d, 0.0))
        fe = int(fill_events.get(d, 0))
        fq = float(filled_qty.get(d, 0.0))
        lam = (fe / ex) if ex > 0 else float("nan")
        rows.append(
            {
                "delta_bucket": int(d),
                "exposure_s": ex,
                "fill_events": fe,
                "filled_qty": fq,
                "lambda_events_per_s": lam,
                "lambda_qty_per_s": (fq / ex) if ex > 0 else float("nan"),
                "n_orders": 0,  # virtual engine does not create order objects
                "bid_hits": int(bid_hits.get(d, 0)),
                "ask_hits": int(ask_hits.get(d, 0)),
            }
        )

    points = pd.DataFrame(rows)
    if not points.empty:
        points["usable"] = (points["exposure_s"] >= float(min_exposure_s)) & (points["lambda_events_per_s"] > 0)
    else:
        points["usable"] = False

    stats = {
        "exposure_s_total": float(points["exposure_s"].sum()) if not points.empty else 0.0,
        "fills_total": int(points["fill_events"].sum()) if not points.empty else 0,
        "fills_usable_total": int(points.loc[points["usable"].astype(bool), "fill_events"].sum()) if not points.empty else 0,
        "n_deltas_usable": int(points["usable"].astype(bool).sum()) if not points.empty else 0,
    }

    return VirtualProbeResult(points=points.sort_values("delta_bucket").reset_index(drop=True), stats=stats)
