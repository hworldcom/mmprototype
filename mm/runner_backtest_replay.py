# mm/runner_backtest_replay.py

import os
from pathlib import Path

from mm.backtest.replay import replay_day


def main():
    root = Path(os.getenv("DATA_ROOT", "data"))
    symbol = os.getenv("SYMBOL", "BTCUSDT").upper()
    yyyymmdd = os.getenv("DAY", "")  # e.g. 20251216
    if not yyyymmdd:
        raise RuntimeError("Set DAY=YYYYMMDD, e.g. DAY=20251216")

    stats = replay_day(root=root, symbol=symbol, yyyymmdd=yyyymmdd)

    print(f"Replay finished for {symbol} {yyyymmdd}")
    print(stats)


if __name__ == "__main__":
    main()
