from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class CalibrationPoint:
    delta_ticks: int
    exposure_s: float
    fill_events: int
    filled_qty: float


def compute_bucketed_exposure(
    *,
    orders_path: Path,
    fills_path: Path,
    state_path: Path,
    tick_size: float,
    min_delta_ticks: int = 0,
    max_delta_ticks: int = 50,
    min_exposure_s: float = 5.0,
) -> pd.DataFrame:
    """Compute exposure and fills by quote distance bucket.

    This function is designed for calibration where we estimate an empirical
    intensity curve lambda(delta) ~ fills / exposure.

    Notes
    -----
    - Exposure is measured from `active_recv_ms` (or `recv_ms` if missing)
      until the earliest of:
        * first fill time
        * cancel_effective_ms (if present)
        * end of run
      This is consistent with calibrating an *event intensity* for time-to-first-fill.
    - Distance is computed to `state.mid` at activation time using merge_asof.
    """

    orders = pd.read_csv(orders_path)
    fills = pd.read_csv(fills_path) if fills_path.exists() else pd.DataFrame(columns=["order_id", "recv_ms", "qty"])
    state = pd.read_csv(state_path)

    # dtypes
    for col in ("recv_ms", "active_recv_ms", "cancel_effective_ms"):
        if col in orders.columns:
            orders[col] = pd.to_numeric(orders[col], errors="coerce")
    if not fills.empty:
        fills["recv_ms"] = pd.to_numeric(fills["recv_ms"], errors="coerce")
        fills["qty"] = pd.to_numeric(fills["qty"], errors="coerce")
    state["recv_ms"] = pd.to_numeric(state["recv_ms"], errors="coerce")
    state["mid"] = pd.to_numeric(state["mid"], errors="coerce")

    placed = orders[orders.get("action") == "PLACE"].copy()
    if "status" in placed.columns:
        placed = placed[placed["status"] != "REJECTED"].copy()

    if placed.empty:
        return pd.DataFrame(columns=[
            "delta_bucket", "exposure_s", "fill_events", "filled_qty", "lambda_events_per_s", "lambda_qty_per_s", "n_orders"
        ])

    placed["active_ms"] = placed.get("active_recv_ms").fillna(placed["recv_ms"]).astype("int64")

    end_run_ms = int(np.nanmax([
        state["recv_ms"].max(),
        orders["recv_ms"].max(),
        fills["recv_ms"].max() if not fills.empty else state["recv_ms"].max(),
    ]))

    # first fill per order
    if not fills.empty:
        first_fill_ms = fills.groupby("order_id", as_index=False)["recv_ms"].min().rename(columns={"recv_ms": "first_fill_ms"})
        fill_event_counts = fills.groupby("order_id").size().rename("fill_events").reset_index()
        filled_qty = fills.groupby("order_id", as_index=False)["qty"].sum().rename(columns={"qty": "filled_qty"})
    else:
        first_fill_ms = pd.DataFrame(columns=["order_id", "first_fill_ms"])
        fill_event_counts = pd.DataFrame(columns=["order_id", "fill_events"])
        filled_qty = pd.DataFrame(columns=["order_id", "filled_qty"])

    cancel_req = orders[orders.get("action") == "CANCEL_REQ"].copy()
    if "cancel_effective_ms" in cancel_req.columns and not cancel_req.empty:
        cancel_eff = cancel_req.groupby("order_id", as_index=False)["cancel_effective_ms"].min().rename(
            columns={"cancel_effective_ms": "cancel_eff_ms"}
        )
    else:
        cancel_eff = pd.DataFrame(columns=["order_id", "cancel_eff_ms"])

    placed = placed.merge(first_fill_ms, on="order_id", how="left")
    placed = placed.merge(cancel_eff, on="order_id", how="left")

    placed["end_ms"] = end_run_ms
    if "first_fill_ms" in placed.columns:
        m = placed["first_fill_ms"].notna()
        placed.loc[m, "end_ms"] = np.minimum(placed.loc[m, "end_ms"], placed.loc[m, "first_fill_ms"])
    if "cancel_eff_ms" in placed.columns:
        m = placed["cancel_eff_ms"].notna()
        placed.loc[m, "end_ms"] = np.minimum(placed.loc[m, "end_ms"], placed.loc[m, "cancel_eff_ms"])

    placed["exposure_s"] = ((placed["end_ms"] - placed["active_ms"]).clip(lower=0)) / 1000.0

    # mid at activation
    state_sorted = state[["recv_ms", "mid"]].sort_values("recv_ms")
    placed_sorted = placed.sort_values("active_ms")
    merged = pd.merge_asof(
        placed_sorted,
        state_sorted,
        left_on="active_ms",
        right_on="recv_ms",
        direction="backward",
    ).rename(columns={"mid": "mid_at_active"})

    merged["delta_ticks"] = (np.abs(merged["price"] - merged["mid_at_active"]) / float(tick_size))
    merged["delta_bucket"] = np.floor(merged["delta_ticks"]).astype(int)

    merged = merged[(merged["delta_bucket"] >= int(min_delta_ticks)) & (merged["delta_bucket"] <= int(max_delta_ticks))].copy()

    df = (
        merged
        .merge(fill_event_counts, on="order_id", how="left")
        .merge(filled_qty, on="order_id", how="left")
    )
    df["fill_events"] = df["fill_events"].fillna(0).astype(int)
    df["filled_qty"] = df["filled_qty"].fillna(0.0)

    agg = df.groupby("delta_bucket", as_index=False).agg(
        exposure_s=("exposure_s", "sum"),
        fill_events=("fill_events", "sum"),
        filled_qty=("filled_qty", "sum"),
        n_orders=("order_id", "count"),
    )
    agg["lambda_events_per_s"] = agg["fill_events"] / agg["exposure_s"].replace(0, np.nan)
    agg["lambda_qty_per_s"] = agg["filled_qty"] / agg["exposure_s"].replace(0, np.nan)

    # Filter for fitting
    agg["usable"] = (agg["exposure_s"] >= float(min_exposure_s)) & (agg["lambda_events_per_s"] > 0)
    return agg.sort_values("delta_bucket").reset_index(drop=True)


def compute_run_level_exposure(
    *,
    orders_path: Path,
    fills_path: Path,
    state_path: Path,
    tick_size: float,
    expected_delta_ticks: Optional[int] = None,
) -> Dict[str, float]:
    """Compute a single exposure/filled summary for a run.

    Intended for Design B (fixed-spread runs). Returns a dict that can be
    appended to a CSV.
    """

    agg = compute_bucketed_exposure(
        orders_path=orders_path,
        fills_path=fills_path,
        state_path=state_path,
        tick_size=tick_size,
        min_delta_ticks=0,
        max_delta_ticks=10_000,
        min_exposure_s=0.0,
    )

    exposure_s = float(agg["exposure_s"].sum())
    fill_events = int(agg["fill_events"].sum())
    filled_qty = float(agg["filled_qty"].sum())

    # Estimate effective delta as an exposure-weighted mean.
    if exposure_s > 0 and not agg.empty:
        delta_mean = float((agg["delta_bucket"] * agg["exposure_s"]).sum() / exposure_s)
    else:
        delta_mean = float("nan")

    return {
        "delta_expected": float(expected_delta_ticks) if expected_delta_ticks is not None else float("nan"),
        "delta_mean": delta_mean,
        "exposure_s": exposure_s,
        "fill_events": fill_events,
        "filled_qty": filled_qty,
        "lambda_events_per_s": (fill_events / exposure_s) if exposure_s > 0 else float("nan"),
    }
