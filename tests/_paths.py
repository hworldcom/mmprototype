# tests/_paths.py

from pathlib import Path


def day_str(recorder_mod) -> str:
    # Recorder uses Berlin-local day for folder naming
    return recorder_mod.berlin_now().strftime("%Y%m%d")


def day_dir(tmp_path: Path, recorder_mod, symbol: str) -> Path:
    return tmp_path / "data" / symbol / day_str(recorder_mod)


def orderbook_path(tmp_path: Path, recorder_mod, symbol: str) -> Path:
    d = day_dir(tmp_path, recorder_mod, symbol)
    day = day_str(recorder_mod)
    return d / f"orderbook_ws_depth_{symbol}_{day}.csv"


def trades_path(tmp_path: Path, recorder_mod, symbol: str) -> Path:
    d = day_dir(tmp_path, recorder_mod, symbol)
    day = day_str(recorder_mod)
    return d / f"trades_ws_{symbol}_{day}.csv"


def gaps_path(tmp_path: Path, recorder_mod, symbol: str) -> Path:
    d = day_dir(tmp_path, recorder_mod, symbol)
    day = day_str(recorder_mod)
    return d / f"gaps_{symbol}_{day}.csv"


def events_path(tmp_path: Path, recorder_mod, symbol: str) -> Path:
    d = day_dir(tmp_path, recorder_mod, symbol)
    day = day_str(recorder_mod)
    return d / f"events_{symbol}_{day}.csv"
