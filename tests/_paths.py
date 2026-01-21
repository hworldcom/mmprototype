# tests/_paths.py

from pathlib import Path


def day_str(recorder_mod) -> str:
    now = recorder_mod.window_now()
    window_start, window_end = recorder_mod.compute_window(now)
    if now < window_start:
        prev_start = window_start - recorder_mod.timedelta(days=1)
        prev_end = window_end - recorder_mod.timedelta(days=1)
        if now <= prev_end:
            window_start = prev_start
    return window_start.strftime("%Y%m%d")


def day_dir(tmp_path: Path, recorder_mod, symbol: str) -> Path:
    return tmp_path / "data" / symbol / day_str(recorder_mod)


def orderbook_path(tmp_path: Path, recorder_mod, symbol: str) -> Path:
    d = day_dir(tmp_path, recorder_mod, symbol)
    day = day_str(recorder_mod)
    return d / f"orderbook_ws_depth_{symbol}_{day}.csv.gz"


def trades_path(tmp_path: Path, recorder_mod, symbol: str) -> Path:
    d = day_dir(tmp_path, recorder_mod, symbol)
    day = day_str(recorder_mod)
    return d / f"trades_ws_{symbol}_{day}.csv.gz"


def gaps_path(tmp_path: Path, recorder_mod, symbol: str) -> Path:
    d = day_dir(tmp_path, recorder_mod, symbol)
    day = day_str(recorder_mod)
    return d / f"gaps_{symbol}_{day}.csv.gz"


def events_path(tmp_path: Path, recorder_mod, symbol: str) -> Path:
    d = day_dir(tmp_path, recorder_mod, symbol)
    day = day_str(recorder_mod)
    return d / f"events_{symbol}_{day}.csv.gz"
