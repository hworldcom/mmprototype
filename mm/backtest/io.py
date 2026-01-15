# mm/backtest/io.py

from __future__ import annotations

import csv
import gzip
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Generator, Iterable, Iterator, List, Optional, Tuple


@dataclass(frozen=True)
class DepthDiff:
    recv_ms: int
    E: int
    U: int
    u: int
    b: list
    a: list
    recv_seq: int | None = None


@dataclass(frozen=True)
class Trade:
    recv_ms: int
    E: int
    price: float
    qty: float
    is_buyer_maker: int
    trade_id: int | None = None
    trade_time_ms: int | None = None
    recv_seq: int | None = None


@dataclass(frozen=True)
class EventRow:
    event_id: int
    recv_ms: int
    run_id: int
    type: str
    epoch_id: int
    details_json: str
    recv_seq: int | None = None


def day_dir(root: Path, symbol: str, yyyymmdd: str) -> Path:
    """
    Return the directory containing all data for a given symbol and day.
    """
    return Path(root) / symbol / yyyymmdd


def find_depth_diffs_file(root: Path, symbol: str, yyyymmdd: str) -> Path:
    ddir = day_dir(root, symbol, yyyymmdd) / "diffs"
    # expected: depth_diffs_SYMBOL_YYYYMMDD.ndjson.gz
    matches = list(ddir.glob(f"depth_diffs_{symbol.upper()}_{yyyymmdd}.ndjson.gz"))
    if not matches:
        # fallback: any depth_diffs_SYMBOL_*.ndjson.gz
        matches = list(ddir.glob(f"depth_diffs_{symbol.upper()}_*.ndjson.gz"))
    if not matches:
        raise FileNotFoundError(f"No depth diffs file found in {ddir}")
    return sorted(matches)[-1]


def find_trades_file(root: Path, symbol: str, yyyymmdd: str) -> Path:
    ddir = day_dir(root, symbol, yyyymmdd)
    matches = list(ddir.glob(f"trades_ws_{symbol.upper()}_{yyyymmdd}.csv.gz"))
    if not matches:
        matches = list(ddir.glob(f"trades_ws_{symbol.upper()}_*.csv.gz"))
    if not matches:
        raise FileNotFoundError(f"No trades file found in {ddir}")
    return sorted(matches)[-1]


def find_events_file(root: Path, symbol: str, yyyymmdd: str) -> Path:
    ddir = day_dir(root, symbol, yyyymmdd)
    matches = list(ddir.glob(f"events_{symbol.upper()}_{yyyymmdd}.csv.gz"))
    if not matches:
        matches = list(ddir.glob(f"events_{symbol.upper()}_*.csv.gz"))
    if not matches:
        raise FileNotFoundError(f"No events file found in {ddir}")
    return sorted(matches)[-1]


def iter_depth_diffs(path: Path) -> Iterator[DepthDiff]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            yield DepthDiff(
                recv_ms=int(obj["recv_ms"]),
                recv_seq=int(obj["recv_seq"]) if "recv_seq" in obj else None,
                E=int(obj.get("E", 0)),
                U=int(obj["U"]),
                u=int(obj["u"]),
                b=obj.get("b", []),
                a=obj.get("a", []),
            )


def iter_trades_csv(path: Path) -> Iterator[Trade]:
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt', encoding='utf-8', newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            trade_id = row.get("trade_id")
            trade_time_ms = row.get("trade_time_ms")
            recv_seq = row.get("recv_seq")
            yield Trade(
                E=int(row["event_time_ms"]),
                recv_ms=int(row.get("recv_time_ms", row["event_time_ms"])),
                recv_seq=int(recv_seq) if recv_seq not in (None, "") else None,
                trade_id=int(trade_id) if trade_id not in (None, "") else None,
                trade_time_ms=int(trade_time_ms) if trade_time_ms not in (None, "") else None,
                price=float(row["price"]),
                qty=float(row["qty"]),
                is_buyer_maker=int(row["is_buyer_maker"]),
            )


def iter_events_csv(path: Path) -> Iterator[EventRow]:
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt', encoding='utf-8', newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            recv_seq = row.get("recv_seq")
            yield EventRow(
                event_id=int(row["event_id"]),
                recv_ms=int(row["recv_time_ms"]),
                recv_seq=int(recv_seq) if recv_seq not in (None, "") else None,
                run_id=int(row["run_id"]),
                type=row["type"],
                epoch_id=int(row["epoch_id"]),
                details_json=row["details_json"],
            )


def snapshot_paths(root: Path, symbol: str, yyyymmdd: str) -> List[Path]:
    sdir = day_dir(root, symbol, yyyymmdd) / "snapshots"
    if not sdir.exists():
        return []
    return sorted(sdir.glob("snapshot_*.csv"))
