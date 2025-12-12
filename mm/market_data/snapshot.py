# mm/market_data/snapshot.py

import csv
from pathlib import Path
from datetime import datetime, UTC
from binance.client import Client

from .local_orderbook import LocalOrderBook


def record_rest_snapshot(
    client: Client,
    symbol: str,
    out_dir: Path,
    limit: int = 1000,
) -> LocalOrderBook:
    snap = client.get_order_book(symbol=symbol, limit=limit)

    lob = LocalOrderBook()
    lob.load_snapshot(
        bids=snap["bids"],
        asks=snap["asks"],
        last_update_id=int(snap["lastUpdateId"]),
    )

    fname = out_dir / f"orderbook_rest_snapshot_{symbol}_{datetime.now(UTC).strftime('%Y%m%d')}.csv"

    with fname.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["side", "price", "qty"])
        for p, q in sorted(lob.bids.items(), reverse=True):
            w.writerow(["bid", f"{p:.8f}", f"{q:.8f}"])
        for p, q in sorted(lob.asks.items()):
            w.writerow(["ask", f"{p:.8f}", f"{q:.8f}"])

    return lob
