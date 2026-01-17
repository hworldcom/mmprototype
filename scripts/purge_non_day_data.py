#!/usr/bin/env python3
"""Purge contaminated data inside a day folder by timestamp.

This script is intended to repair cases where a recorder process kept running
past midnight and continued appending records into the *startup day* directory
(e.g. data/<SYMBOL>/<YYYYMMDD>/), contaminating that day's files with records
from subsequent days.

It operates in two modes:

- scan (default): reports contamination per file (kept vs removed rows/lines),
  but does not modify anything.
- delete: rewrites each file in-place keeping only records whose timestamps fall
  within the target day (in the configured timezone). It also removes snapshot
  files that don't belong to the target day.

Supported file types
- CSV / CSV.GZ (line-based): trades, events, gaps, orderbook rows, etc.
- NDJSON / NDJSON.GZ (line-based JSON): depth diffs
- Snapshot CSV files under snapshots/

Notes
- This script filters *within* each file. It does not delete entire files unless
  they become empty after filtering (in which case they are removed).
- It is conservative with timestamp detection:
  - CSV: uses the first matching column among:
    recv_time_ms, recv_ms, event_dt_ms, dt_ms
  - NDJSON: uses the first matching key among:
    recv_ms, recv_time_ms, event_dt_ms, dt_ms

Usage examples
  # Scan (no changes)
  python scripts/purge_non_day_data.py --symbol BTCUSDT --day 20260114 --mode scan

  # Delete (rewrite files to keep only 2026-01-14 in Europe/Berlin)
  python scripts/purge_non_day_data.py --symbol BTCUSDT --day 20260114 --mode delete

  # Operate on a custom data root
  python scripts/purge_non_day_data.py --symbol BTCUSDT --day 20260114 --data-root /mnt/data --mode delete
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
from dataclasses import dataclass

from datetime import datetime

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional, Tuple

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


TIME_COL_CANDIDATES = ("recv_time_ms", "recv_ms", "event_dt_ms", "dt_ms")
TIME_KEY_CANDIDATES = ("recv_ms", "recv_time_ms", "event_dt_ms", "dt_ms")


def _parse_day(day: str) -> Tuple[int, int, int]:
    if len(day) != 8 or not day.isdigit():
        raise ValueError(f"day must be YYYYMMDD (got {day!r})")
    y = int(day[:4]); m = int(day[4:6]); d = int(day[6:8])
    return y, m, d


def _day_bounds_ms(day: str, tz: str) -> Tuple[int, int]:
    if ZoneInfo is None:
        raise RuntimeError("zoneinfo is required (Python 3.9+)")

    y, m, d = _parse_day(day)
    z = ZoneInfo(tz)
    start = datetime(y, m, d, 0, 0, 0, tzinfo=z)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)

def _fmt_ts(ms: int | None, tz: str) -> str | None:
    if ms is None:
        return None
    if ZoneInfo is None:
        return str(ms)
    dt = datetime.fromtimestamp(ms / 1000.0, tz=ZoneInfo(tz))
    return dt.isoformat()



def _open_text(path: Path, mode: str):
    # mode: 'rt' or 'wt'
    if path.suffix == ".gz":
        return gzip.open(path, mode, encoding="utf-8", newline="")
    return path.open(mode, encoding="utf-8", newline="")


def _detect_csv_time_col(fieldnames: Iterable[str]) -> Optional[str]:
    fields = set(fieldnames)
    for c in TIME_COL_CANDIDATES:
        if c in fields:
            return c
    return None


def _detect_json_time_key(obj: dict) -> Optional[str]:
    for k in TIME_KEY_CANDIDATES:
        if k in obj:
            return k
    return None


@dataclass
class FileReport:
    path: Path
    kind: str
    total: int
    kept: int
    removed: int
    reason: str
    last_seen_ms: int | None = None
    last_seen_local: str | None = None
    last_kept_ms: int | None = None
    last_kept_local: str | None = None


def _filter_csv_file(path: Path, start_ms: int, end_ms: int, write: bool, tz: str) -> FileReport:
    total = kept = 0
    reason = ""
    truncated = False
    last_seen_ms: int | None = None
    last_kept_ms: int | None = None

    with _open_text(path, "rt") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return FileReport(path, "csv", 0, 0, 0, "empty_or_missing_header")
        tcol = _detect_csv_time_col(reader.fieldnames)
        if tcol is None:
            return FileReport(path, "csv", 0, 0, 0, "no_timestamp_column_found")

        tmp_path = path.with_suffix(path.suffix + ".tmp") if write else None
        writer = None
        out_f = None
        if write:
            out_f = _open_text(tmp_path, "wt")  # type: ignore[arg-type]
            writer = csv.DictWriter(out_f, fieldnames=reader.fieldnames)
            writer.writeheader()

        row_idx = 0
        try:
            for row in reader:
                row_idx += 1
                if row_idx % 500000 == 0:
                    log(f"{path.name}: scanned {row_idx} rows...")

                total += 1
                v = row.get(tcol)
                try:
                    ts = int(float(v)) if v not in (None, "") else None
                except Exception:
                    ts = None

                if ts is not None:
                    last_seen_ms = ts if last_seen_ms is None else max(last_seen_ms, ts)

                if ts is not None and start_ms <= ts < end_ms:
                    kept += 1
                    last_kept_ms = ts if last_kept_ms is None else max(last_kept_ms, ts)
                    if write:
                        writer.writerow(row)  # type: ignore[union-attr]
        except EOFError:
            truncated = True
            log(
                f"WARNING: {path.name}: gzip stream ended unexpectedly (truncated/corrupt). "
                "Proceeding with partial data."
            )

        if write and out_f is not None:
            out_f.flush()
            out_f.close()

    removed = total - kept
    if total == 0:
        reason = "no_rows"
    elif removed == 0 and not truncated:
        reason = "clean"
    else:
        base = f"removed_rows_outside_day({tcol})"
        reason = f"gzip_truncated; {base}" if truncated else base

    if write:
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        if kept == 0:
            try:
                tmp_path.unlink(missing_ok=True)  # py3.8+
            except TypeError:
                if tmp_path.exists():
                    tmp_path.unlink()
            path.unlink()
        else:
            os.replace(tmp_path, path)

    return FileReport(
        path,
        "csv",
        total,
        kept,
        removed,
        reason,
        last_seen_ms=last_seen_ms,
        last_seen_local=_fmt_ts(last_seen_ms, tz),
        last_kept_ms=last_kept_ms,
        last_kept_local=_fmt_ts(last_kept_ms, tz),
    )


def _filter_ndjson_file(path: Path, start_ms: int, end_ms: int, write: bool, tz: str) -> FileReport:
    total = kept = 0
    reason = ""
    truncated = False
    last_seen_ms: int | None = None
    last_kept_ms: int | None = None

    tmp_path = path.with_suffix(path.suffix + ".tmp") if write else None
    out_f = None
    if write:
        out_f = _open_text(tmp_path, "wt")  # type: ignore[arg-type]

    time_key_used = None

    with _open_text(path, "rt") as f:
        line_idx = 0
        try:
            for line in f:
                line_idx += 1
                if line_idx % 500000 == 0:
                    log(f"{path.name}: scanned {line_idx} lines...")

                line = line.strip()
                if not line:
                    continue
                total += 1
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if time_key_used is None:
                    time_key_used = _detect_json_time_key(obj)
                ts = None
                if time_key_used is not None:
                    try:
                        ts = int(float(obj.get(time_key_used)))
                    except Exception:
                        ts = None

                if ts is not None:
                    last_seen_ms = ts if last_seen_ms is None else max(last_seen_ms, ts)

                if ts is not None and start_ms <= ts < end_ms:
                    kept += 1
                    last_kept_ms = ts if last_kept_ms is None else max(last_kept_ms, ts)
                    if write and out_f is not None:
                        out_f.write(json.dumps(obj, separators=(",", ":")) + "\n")

        except EOFError:
            truncated = True
            log(
                f"WARNING: {path.name}: gzip stream ended unexpectedly (truncated/corrupt). "
                "Proceeding with partial data."
            )

    if write and out_f is not None:
        out_f.flush()
        out_f.close()

    removed = total - kept
    if total == 0:
        reason = "no_lines"
    elif removed == 0 and not truncated:
        reason = "clean"
    else:
        if truncated:
            reason = f"gzip_truncated; removed_lines_outside_day({time_key_used or 'unknown'})"
        else:
            reason = f"removed_lines_outside_day({time_key_used or 'unknown'})"

    if write:
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        if kept == 0:
            try:
                tmp_path.unlink(missing_ok=True)
            except TypeError:
                if tmp_path.exists():
                    tmp_path.unlink()
            path.unlink()
        else:
            os.replace(tmp_path, path)

    return FileReport(
        path,
        "ndjson",
        total,
        kept,
        removed,
        reason,
        last_seen_ms=last_seen_ms,
        last_seen_local=_fmt_ts(last_seen_ms, tz),
        last_kept_ms=last_kept_ms,
        last_kept_local=_fmt_ts(last_kept_ms, tz),
    )


def _purge_snapshots(snap_dir: Path, target_day: str, tz: str, write: bool) -> FileReport:
    # Snapshot filenames in this project follow:
    # snapshot_<recv_ms>_<tag>.csv[.gz]
    # We keep snapshot files whose recv_ms falls within target day.
    if not snap_dir.exists():
        return FileReport(snap_dir, "snapshots", 0, 0, 0, "missing")

    start_ms, end_ms = _day_bounds_ms(target_day, tz)

    total = kept = 0
    removed_files = 0
    last_seen_ms = None
    last_kept_ms = None

    for p in sorted(snap_dir.iterdir()):
        if not p.is_file():
            continue
        if not p.name.startswith("snapshot_"):
            continue
        total += 1
        parts = p.name.split("_")
        ts = None
        if len(parts) >= 2:
            try:
                ts = int(parts[1])
            except Exception:
                ts = None
        if ts is not None:
            last_seen_ms = ts if last_seen_ms is None else max(last_seen_ms, ts)
        if ts is not None and start_ms <= ts < end_ms:
            kept += 1
            continue
        removed_files += 1
        if write:
            p.unlink()

    reason = "clean" if removed_files == 0 else "removed_snapshot_files_outside_day"
    return FileReport(snap_dir, "snapshots", total, kept, removed_files, reason, last_seen_ms=last_seen_ms, last_seen_local=_fmt_ts(last_seen_ms, tz), last_kept_ms=last_kept_ms, last_kept_local=_fmt_ts(last_kept_ms, tz))


def _iter_target_files(day_dir: Path) -> Iterable[Path]:
    # All files under the day folder except schema.json
    for p in day_dir.rglob("*"):
        if not p.is_file():
            continue
        if p.name == "schema.json":
            continue
        yield p


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, help="Symbol directory name (e.g. BTCUSDT)")
    ap.add_argument("--day", required=True, help="Target day folder YYYYMMDD (e.g. 20260114)")
    ap.add_argument("--data-root", default="data", help="Data root (default: ./data)")
    ap.add_argument("--tz", default="Europe/Berlin", help="Timezone for day boundaries (default: Europe/Berlin)")
    ap.add_argument("--mode", choices=("scan", "delete"), default="scan", help="scan (no changes) or delete (rewrite/purge)")
    ap.add_argument("--limit-files", type=int, default=0, help="Optional max files to process (debug)")
    args = ap.parse_args()

    day_dir = Path(args.data_root) / args.symbol / args.day
    if not day_dir.exists():
        raise FileNotFoundError(f"Day folder not found: {day_dir}")

    start_ms, end_ms = _day_bounds_ms(args.day, args.tz)
    do_write = args.mode == "delete"

    log(f"Starting purge_non_day_data: mode={args.mode}, symbol={args.symbol}, day={args.day}")
    reports = []

    # Snapshots first (separate logic)
    reports.append(_purge_snapshots(day_dir / "snapshots", args.day, args.tz, do_write))

    # Other files
    processed = 0
    for p in _iter_target_files(day_dir):
        if args.limit_files and processed >= args.limit_files:
            break

        name = p.name
        if "snapshot_" in name:
            # handled by snapshot pass
            continue

        # Determine by extension
        suffixes = "".join(p.suffixes)
        if suffixes.endswith(".ndjson") or suffixes.endswith(".ndjson.gz") or suffixes.endswith(".jsonl") or suffixes.endswith(".jsonl.gz"):
            reports.append(_filter_ndjson_file(p, start_ms, end_ms, do_write, args.tz))
        elif suffixes.endswith(".csv") or suffixes.endswith(".csv.gz"):
            reports.append(_filter_csv_file(p, start_ms, end_ms, do_write, args.tz))
        else:
            # Ignore unknown files
            continue

        processed += 1

    # Output summary
    affected = [r for r in reports if r.removed > 0]
    total_removed = sum(r.removed for r in reports)
    total_kept = sum(r.kept for r in reports)
    total_total = sum(r.total for r in reports)

    print(f"Mode: {args.mode}")
    print(f"Target: {day_dir}")
    print(f"Timezone: {args.tz}")
    print(f"Day window ms: [{start_ms}, {end_ms})")
    print()
    print(f"Files scanned: {len([r for r in reports if r.kind not in ('snapshots',) or r.total > 0])}")
    print(f"Records/lines total: {total_total}")
    print(f"Kept: {total_kept}")
    print(f"Removed: {total_removed}")
    print(f"Affected files: {len(affected)}")
    print()

    for r in affected:
        last_seen = r.last_seen_local or "n/a"
        last_kept = r.last_kept_local or "n/a"
        print(f"- {r.kind:9s} | removed={r.removed:8d} kept={r.kept:8d} total={r.total:8d} | last_kept={last_kept} last_seen={last_seen} | {r.reason} | {r.path}")

    if args.mode == "delete":
        # If the day is partial (e.g. truncated streams), also remove snapshots after the last kept timestamp
        # across the kept (in-day) records we observed in this folder.
        non_snap = [r for r in reports if r.kind in ("csv", "ndjson") and r.last_kept_ms is not None]
        effective_end_ms = max((r.last_kept_ms for r in non_snap), default=None)

        snap_dir = day_dir / "snapshots"
        if effective_end_ms is not None and snap_dir.exists():
            removed_extra = 0
            for p in snap_dir.iterdir():
                if not p.is_file() or not p.name.startswith("snapshot_"):
                    continue
                parts = p.name.split("_")
                ts = None
                if len(parts) >= 2:
                    try:
                        ts = int(parts[1])
                    except Exception:
                        ts = None
                if ts is not None and ts > effective_end_ms:
                    p.unlink()
                    removed_extra += 1
            if removed_extra:
                log(f"Removed {removed_extra} snapshot(s) after last_kept cutoff: {_fmt_ts(effective_end_ms, args.tz)}")

        print()
        print("Delete mode completed. Files were rewritten/purged in-place.")
        print("Recommendation: run a replay/backtest for this day to validate integrity.")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())