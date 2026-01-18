#!/usr/bin/env python3
"""Align and repair truncated .gz data files by enforcing a common cutoff timestamp.

Problem
-------
If a recorder/container is killed mid-write, gzip files (e.g. *.csv.gz, *.ndjson.gz)
may become truncated and unreadable beyond a point, raising:

  EOFError: Compressed file ended before the end-of-stream marker was reached

Different streams may truncate at different times. For consistency, this script
computes the **latest common timestamp** across all target streams (i.e. the
minimum of each stream's last readable timestamp within the target day), then
rewrites every stream to end at that cutoff, producing **valid, properly closed**
gzip outputs.

What it does
------------
For a given symbol/day folder (data/<SYMBOL>/<YYYYMMDD>/):

1) Scan all *.csv.gz and *.ndjson.gz (recursively, excluding schema.json).
2) For each file, stream-parse until EOFError (if any) and track:
   - last_seen_ms: maximum timestamp successfully parsed
   - last_seen_local: human-readable in the configured timezone
3) Compute a global cutoff_ms:
   - cutoff_ms = min(last_seen_ms across all files that had a timestamp)
   - optionally restricted to timestamps within the day window
4) In delete mode:
   - rewrite every target file in-place keeping only records with:
       start_of_day_ms <= ts <= cutoff_ms
     (for ndjson: one json object per line)
   - remove snapshots with timestamp > cutoff_ms

Modes
-----
- scan  (default): compute per-file last_seen and the resulting cutoff; no changes
- delete: rewrite files and delete snapshots after cutoff

Timestamp fields
----------------
- CSV: first matching column among recv_time_ms, recv_ms, event_dt_ms, dt_ms
- NDJSON: first matching key among recv_ms, recv_time_ms, event_dt_ms, dt_ms

Usage
-----
  # Scan and show proposed cutoff
  python scripts/align_stream_end_cutoff.py --symbol BTCUSDT --day 20260114 --mode scan

  # Repair in-place
  python scripts/align_stream_end_cutoff.py --symbol BTCUSDT --day 20260114 --mode delete
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

TIME_COL_CANDIDATES = ("recv_time_ms", "recv_ms", "event_dt_ms", "dt_ms")
TIME_KEY_CANDIDATES = ("recv_ms", "recv_time_ms", "event_dt_ms", "dt_ms")


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _parse_day(day: str) -> Tuple[int, int, int]:
    if len(day) != 8 or not day.isdigit():
        raise ValueError(f"day must be YYYYMMDD (got {day!r})")
    return int(day[:4]), int(day[4:6]), int(day[6:8])


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
class StreamInfo:
    path: Path
    kind: str  # csv|ndjson
    time_field: str | None
    last_seen_ms: int | None
    last_seen_local: str | None
    truncated: bool


def _iter_target_files(day_dir: Path) -> Iterable[Path]:
    for p in day_dir.rglob("*"):
        if not p.is_file():
            continue
        if p.name == "schema.json":
            continue
        suffixes = "".join(p.suffixes)
        if suffixes.endswith(".csv.gz") or suffixes.endswith(".ndjson.gz") or suffixes.endswith(".jsonl.gz"):
            yield p


def scan_csv_last_seen(path: Path, tz: str, start_ms: int, end_ms: int) -> StreamInfo:
    last_seen = None
    tcol = None
    truncated = False
    rows = 0

    with _open_text(path, "rt") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return StreamInfo(path, "csv", None, None, None, False)
        tcol = _detect_csv_time_col(reader.fieldnames)
        if tcol is None:
            return StreamInfo(path, "csv", None, None, None, False)

        try:
            for row in reader:
                rows += 1
                if rows % 500000 == 0:
                    log(f"{path.name}: scanned {rows} rows...")
                v = row.get(tcol)
                try:
                    ts = int(float(v)) if v not in (None, "") else None
                except Exception:
                    ts = None
                if ts is None:
                    continue
                # restrict to day window for determining "in-day last"
                if start_ms <= ts < end_ms:
                    last_seen = ts if last_seen is None else max(last_seen, ts)
        except EOFError:
            truncated = True
            log(f"WARNING: {path.name}: gzip truncated while scanning. Using readable prefix.")

    return StreamInfo(path, "csv", tcol, last_seen, _fmt_ts(last_seen, tz), truncated)


def scan_ndjson_last_seen(path: Path, tz: str, start_ms: int, end_ms: int) -> StreamInfo:
    last_seen = None
    time_key = None
    truncated = False
    lines = 0

    with _open_text(path, "rt") as f:
        try:
            for line in f:
                lines += 1
                if lines % 500000 == 0:
                    log(f"{path.name}: scanned {lines} lines...")
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if time_key is None:
                    time_key = _detect_json_time_key(obj)
                if time_key is None:
                    continue
                try:
                    ts = int(float(obj.get(time_key)))
                except Exception:
                    continue
                if start_ms <= ts < end_ms:
                    last_seen = ts if last_seen is None else max(last_seen, ts)
        except EOFError:
            truncated = True
            log(f"WARNING: {path.name}: gzip truncated while scanning. Using readable prefix.")

    return StreamInfo(path, "ndjson", time_key, last_seen, _fmt_ts(last_seen, tz), truncated)


def rewrite_csv_to_cutoff(path: Path, tcol: str, start_ms: int, cutoff_ms: int, tz: str) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    kept = total = 0
    truncated = False

    with _open_text(path, "rt") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return
        out_f = _open_text(tmp_path, "wt")
        writer = csv.DictWriter(out_f, fieldnames=reader.fieldnames)
        writer.writeheader()
        try:
            for row in reader:
                total += 1
                v = row.get(tcol)
                try:
                    ts = int(float(v)) if v not in (None, "") else None
                except Exception:
                    ts = None
                if ts is None:
                    continue
                if start_ms <= ts <= cutoff_ms:
                    writer.writerow(row)
                    kept += 1
        except EOFError:
            truncated = True
            log(f"WARNING: {path.name}: gzip truncated while rewriting. Keeping readable prefix.")
        out_f.flush()
        out_f.close()

    if kept == 0:
        tmp_path.unlink(missing_ok=True)  # type: ignore[arg-type]
        path.unlink()
        log(f"Deleted empty file after cutoff filtering: {path}")
        return

    os.replace(tmp_path, path)
    log(f"Rewrote {path.name}: kept={kept} cutoff={_fmt_ts(cutoff_ms, tz)} truncated_input={truncated}")


def rewrite_ndjson_to_cutoff(path: Path, time_key: str, start_ms: int, cutoff_ms: int, tz: str) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    kept = total = 0
    truncated = False

    out_f = _open_text(tmp_path, "wt")
    with _open_text(path, "rt") as f:
        try:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                total += 1
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                try:
                    ts = int(float(obj.get(time_key)))
                except Exception:
                    continue
                if start_ms <= ts <= cutoff_ms:
                    out_f.write(json.dumps(obj, separators=(",", ":")) + "\n")
                    kept += 1
        except EOFError:
            truncated = True
            log(f"WARNING: {path.name}: gzip truncated while rewriting. Keeping readable prefix.")
    out_f.flush()
    out_f.close()

    if kept == 0:
        tmp_path.unlink(missing_ok=True)  # type: ignore[arg-type]
        path.unlink()
        log(f"Deleted empty file after cutoff filtering: {path}")
        return

    os.replace(tmp_path, path)
    log(f"Rewrote {path.name}: kept={kept} cutoff={_fmt_ts(cutoff_ms, tz)} truncated_input={truncated}")


def purge_snapshots_after_cutoff(snap_dir: Path, cutoff_ms: int, tz: str) -> int:
    if not snap_dir.exists():
        return 0
    removed = 0
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
        if ts is not None and ts > cutoff_ms:
            p.unlink()
            removed += 1
    if removed:
        log(f"Removed {removed} snapshot(s) after cutoff={_fmt_ts(cutoff_ms, tz)}")
    return removed


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--day", required=True, help="YYYYMMDD")
    ap.add_argument("--data-root", default="data")
    ap.add_argument("--tz", default="Europe/Berlin")
    ap.add_argument("--mode", choices=("scan", "delete"), default="scan")
    args = ap.parse_args()

    day_dir = Path(args.data_root) / args.symbol / args.day
    if not day_dir.exists():
        raise FileNotFoundError(f"Day folder not found: {day_dir}")

    start_ms, end_ms = _day_bounds_ms(args.day, args.tz)
    log(f"Starting align_stream_end_cutoff: mode={args.mode}, symbol={args.symbol}, day={args.day}, tz={args.tz}")
    log(f"Day window: [{_fmt_ts(start_ms, args.tz)}, {_fmt_ts(end_ms - 1, args.tz)}]")

    infos: list[StreamInfo] = []
    for p in sorted(_iter_target_files(day_dir)):
        suffixes = "".join(p.suffixes)
        if suffixes.endswith(".csv.gz"):
            infos.append(scan_csv_last_seen(p, args.tz, start_ms, end_ms))
        else:
            infos.append(scan_ndjson_last_seen(p, args.tz, start_ms, end_ms))

    usable = [i for i in infos if i.last_seen_ms is not None]
    if not usable:
        log("No usable timestamped .gz files found; nothing to do.")
        return 0

    cutoff_ms = min(i.last_seen_ms for i in usable if i.last_seen_ms is not None)  # type: ignore[arg-type]
    log(f"Computed latest common cutoff (min last_seen across streams): {_fmt_ts(cutoff_ms, args.tz)}")

    # Print per-file summary
    for i in infos:
        log(
            f"File: {i.path} kind={i.kind} time_field={i.time_field or 'n/a'} "
            f"last_seen={i.last_seen_local or 'n/a'} truncated={i.truncated}"
        )

    if args.mode == "scan":
        return 0

    # delete mode: rewrite each file to cutoff
    for i in infos:
        if i.time_field is None or i.last_seen_ms is None:
            continue
        if i.kind == "csv":
            rewrite_csv_to_cutoff(i.path, i.time_field, start_ms, cutoff_ms, args.tz)
        else:
            rewrite_ndjson_to_cutoff(i.path, i.time_field, start_ms, cutoff_ms, args.tz)

    purge_snapshots_after_cutoff(day_dir / "snapshots", cutoff_ms, args.tz)

    log("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
