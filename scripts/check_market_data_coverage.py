#!/usr/bin/env python3
"""Check market data availability/coverage for a given day.

Expected layout (MMPrototype recorder):
  data/<SYMBOL>/<YYYYMMDD>/
    orderbook_ws_depth_<SYMBOL>_<YYYYMMDD>.csv.gz
    trades_<SYMBOL>_<YYYYMMDD>.csv.gz                (optional)
    events_<SYMBOL>_<YYYYMMDD>.csv.gz                (optional)

Outputs (human-readable):
  - file presence + row counts
  - first/last timestamp (UTC) for each file
  - gap report (missing intervals) using an expected cadence

Examples:
  python check_market_data_coverage.py --symbol BTCUSDT --day 20251222
  python check_market_data_coverage.py --symbol BTCUSDT --day 20251222 --all
  python check_market_data_coverage.py --symbol BTCUSDT --day 20251222 --time-col event_time_ms
  python check_market_data_coverage.py --symbol BTCUSDT --day 20251222 --expected-ms 1000 --min-gap-s 5

Notes:
  - If --expected-ms is omitted, cadence is inferred from the median positive timestamp delta.
  - A "gap" is reported when delta >= max(2.5x expected cadence, min-gap-s).
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd

DEFAULT_PATTERNS = {
    "orderbook": "orderbook_ws_depth_{symbol}_{day}.csv.gz",
    "trades": "trades_{symbol}_{day}.csv.gz",
    "events": "events_{symbol}_{day}.csv.gz",
}

TIME_COL_CANDIDATES = [
    "event_time_ms",
    "ts_ms",
    "timestamp_ms",
    "time_ms",
    "T",
    "E",
    "transact_time_ms",
]


@dataclass
class CoverageReport:
    kind: str
    path: Path
    exists: bool
    n_rows: int = 0
    time_col: Optional[str] = None
    first_ms: Optional[int] = None
    last_ms: Optional[int] = None
    inferred_expected_ms: Optional[float] = None
    gaps: Optional[pd.DataFrame] = None  # start_ms, end_ms, gap_s


def human_ts(ms: Optional[int]) -> str:
    if ms is None:
        return "n/a"
    return pd.to_datetime(int(ms), unit="ms", utc=True).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"


def pick_time_col(df_columns: Iterable[str], preferred: Optional[str]) -> str:
    cols = list(df_columns)
    if preferred:
        if preferred in cols:
            return preferred
        raise ValueError(f"Requested --time-col '{preferred}' not in columns. Available (first 30): {cols[:30]}")
    for c in TIME_COL_CANDIDATES:
        if c in cols:
            return c
    for c in cols:
        cl = c.lower()
        if cl.endswith("_ms") and "time" in cl:
            return c
    raise ValueError("Could not infer timestamp column. Use --time-col. Columns (first 30): " + str(cols[:30]))


def estimate_expected_ms(ts_ms: np.ndarray) -> float:
    d = np.diff(ts_ms.astype(np.int64))
    d = d[d > 0]
    if len(d) == 0:
        return float("nan")
    return float(np.median(d))


def compute_gaps(ts_ms: np.ndarray, expected_ms: float, min_gap_s: float) -> pd.DataFrame:
    if len(ts_ms) < 2:
        return pd.DataFrame(columns=["start_ms", "end_ms", "gap_s"])
    ts_ms = np.unique(ts_ms.astype(np.int64))
    ts_ms.sort()
    d = np.diff(ts_ms)
    thresh = max(int(expected_ms * 2.5), int(min_gap_s * 1000))
    idx = np.where(d >= thresh)[0]
    starts = ts_ms[idx]
    ends = ts_ms[idx + 1]
    gap_s = (ends - starts) / 1000.0
    return pd.DataFrame({"start_ms": starts, "end_ms": ends, "gap_s": gap_s})


def analyze_file(path: Path, kind: str, time_col_arg: Optional[str], expected_ms_arg: Optional[int], min_gap_s: float) -> CoverageReport:
    rep = CoverageReport(kind=kind, path=path, exists=path.exists())
    if not rep.exists:
        return rep

    df_head = pd.read_csv(path, nrows=5)
    time_col = pick_time_col(df_head.columns, time_col_arg)
    rep.time_col = time_col

    try:
        df = pd.read_csv(path, usecols=[time_col])
    except Exception:
        df = pd.read_csv(path)
        if time_col not in df.columns:
            raise

    rep.n_rows = int(len(df))
    if rep.n_rows == 0:
        rep.gaps = pd.DataFrame(columns=["start_ms", "end_ms", "gap_s"])
        return rep

    ts = pd.to_numeric(df[time_col], errors="coerce").dropna().astype("int64").values
    if len(ts) == 0:
        rep.gaps = pd.DataFrame(columns=["start_ms", "end_ms", "gap_s"])
        return rep

    rep.first_ms = int(np.min(ts))
    rep.last_ms = int(np.max(ts))

    expected_ms = float(expected_ms_arg) if expected_ms_arg is not None else estimate_expected_ms(ts)
    rep.inferred_expected_ms = expected_ms

    if np.isfinite(expected_ms) and expected_ms > 0:
        rep.gaps = compute_gaps(ts, expected_ms=expected_ms, min_gap_s=min_gap_s)
    else:
        rep.gaps = pd.DataFrame(columns=["start_ms", "end_ms", "gap_s"])

    return rep


def print_report(reps: list[CoverageReport], day: str, symbol: str, min_gap_s: float, expected_ms_arg: Optional[int]) -> None:
    print("=" * 100)
    print(f"Market data coverage report | symbol={symbol} day={day} (UTC)")
    print("=" * 100)

    for rep in reps:
        print(f"
[{rep.kind}] {rep.path}")
        if not rep.exists:
            print("  status: MISSING")
            continue

        print("  status: OK")
        print(f"  rows: {rep.n_rows:,}")
        print(f"  time_col: {rep.time_col}")
        print(f"  first: {human_ts(rep.first_ms)}")
        print(f"  last : {human_ts(rep.last_ms)}")

        if expected_ms_arg is not None:
            print(f"  expected cadence (ms): {expected_ms_arg} (user-specified)")
        else:
            if rep.inferred_expected_ms is None or not np.isfinite(rep.inferred_expected_ms):
                print("  expected cadence (ms): n/a (could not infer)")
            else:
                print(f"  expected cadence (ms): {rep.inferred_expected_ms:.1f} (median inferred)")

        print(f"  gap threshold: >= max(2.5x expected cadence, {min_gap_s:.1f}s)")

        gaps = rep.gaps if rep.gaps is not None else pd.DataFrame()
        if gaps.empty:
            print("  gaps: none detected")
            continue

        print(f"  gaps detected: {len(gaps)}")
        g = gaps.copy()
        g["start_ts"] = g["start_ms"].apply(human_ts)
        g["end_ts"] = g["end_ms"].apply(human_ts)
        g = g[["start_ts", "end_ts", "gap_s"]].sort_values("gap_s", ascending=False)

        for _, row in g.head(30).iterrows():
            print(f"    - {row['start_ts']}  ->  {row['end_ts']}   gap={row['gap_s']:.3f}s")
        if len(g) > 30:
            print(f"    ... and {len(g) - 30} more")

        print(f"  gap stats (s): count={len(gaps)} min={gaps['gap_s'].min():.3f} median={gaps['gap_s'].median():.3f} max={gaps['gap_s'].max():.3f}")

    print("
Done.")


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Check market data availability and missing intervals for a given day.")
    ap.add_argument("--root", type=Path, default=Path("data"), help="Data root (default: ./data)")
    ap.add_argument("--symbol", required=True, help="Symbol, e.g. BTCUSDT")
    ap.add_argument("--day", required=True, help="Day in YYYYMMDD, e.g. 20251222")
    ap.add_argument("--kind", choices=["orderbook", "trades", "events"], default="orderbook", help="Which data kind to check")
    ap.add_argument("--all", action="store_true", help="Check all kinds (orderbook, trades, events) if present")
    ap.add_argument("--time-col", default=None, help="Timestamp column name (ms). If omitted, tries common names.")
    ap.add_argument("--expected-ms", type=int, default=None, help="Expected cadence in milliseconds. If omitted, inferred from median delta.")
    ap.add_argument("--min-gap-s", type=float, default=5.0, help="Minimum gap (seconds) to report (default: 5s)")
    args = ap.parse_args(argv)

    day_dir = args.root / args.symbol / args.day
    if not day_dir.exists():
        print(f"Day directory not found: {day_dir}", file=sys.stderr)
        return 2

    kinds = list(DEFAULT_PATTERNS.keys()) if args.all else [args.kind]
    reps: list[CoverageReport] = []
    for k in kinds:
        path = day_dir / DEFAULT_PATTERNS[k].format(symbol=args.symbol, day=args.day)
        try:
            rep = analyze_file(path, kind=k, time_col_arg=args.time_col, expected_ms_arg=args.expected_ms, min_gap_s=args.min_gap_s)
        except Exception as e:
            print(f"
[{k}] ERROR while analyzing {path}: {e}", file=sys.stderr)
            rep = CoverageReport(kind=k, path=path, exists=path.exists())
        reps.append(rep)

    print_report(reps, day=args.day, symbol=args.symbol, min_gap_s=args.min_gap_s, expected_ms_arg=args.expected_ms)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
