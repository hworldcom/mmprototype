from __future__ import annotations

from pathlib import Path
from typing import Optional


def _symbol_fs(symbol: str) -> str:
    return symbol.replace("/", "").replace("-", "").replace(":", "").replace(" ", "").upper()


def _latest_day_dir(root: Path) -> Optional[Path]:
    if not root.exists():
        return None
    day_dirs = [p for p in root.iterdir() if p.is_dir() and p.name.isdigit()]
    if not day_dirs:
        return None
    return sorted(day_dirs, key=lambda p: p.name)[-1]


def resolve_latest_paths(exchange: str, symbol: str) -> dict:
    symbol_fs = _symbol_fs(symbol)
    base = Path("data") / exchange / symbol_fs
    day_dir = _latest_day_dir(base)
    if day_dir is None:
        return {}
    diffs = day_dir / "diffs"
    trades = day_dir / "trades"
    snapshots = day_dir / "snapshots"
    live = day_dir / "live"
    paths = {
        "day_dir": day_dir,
        "diffs": _latest_file(diffs, f"depth_diffs_{symbol_fs}_*.ndjson.gz"),
        "trades": _latest_file(trades, f"trades_ws_raw_{symbol_fs}_*.ndjson.gz"),
        "live_diffs": live / "live_depth_diffs.ndjson" if live.exists() else None,
        "live_trades": live / "live_trades.ndjson" if live.exists() else None,
        "events": _latest_file(day_dir, f"events_{symbol_fs}_*.csv.gz"),
        "snapshot": _latest_file(snapshots, "snapshot_*_*.json"),
    }
    return {k: v for k, v in paths.items() if v is not None}


def _latest_file(folder: Path, pattern: str) -> Optional[Path]:
    if not folder.exists():
        return None
    candidates = sorted(folder.glob(pattern))
    if not candidates:
        return None
    return candidates[-1]
