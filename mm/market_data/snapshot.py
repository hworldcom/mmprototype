import csv
from pathlib import Path
from binance.client import Client
from .local_orderbook import LocalOrderBook

def record_rest_snapshot(
    client: Client,
    symbol: str,
    day_dir: Path,
    snapshots_dir: Path,
    limit: int,
    run_id: int,
    event_id: int,
    tag: str,
    decimals: int = 8,
) -> tuple[LocalOrderBook, Path, int]:
    snap = client.get_order_book(symbol=symbol, limit=limit)
    last_update_id = int(snap["lastUpdateId"])

    lob = LocalOrderBook()
    lob.load_snapshot(bids=snap["bids"], asks=snap["asks"], last_update_id=last_update_id)

    snapshots_dir.mkdir(parents=True, exist_ok=True)
    path = snapshots_dir / f"snapshot_{event_id:06d}_{tag}.csv"

    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run_id", "event_id", "side", "price", "qty", "lastUpdateId"])
        for p, q in sorted(lob.bids.items(), reverse=True):
            w.writerow([run_id, event_id, "bid", f"{p:.{decimals}f}", f"{q:.{decimals}f}", last_update_id])
        for p, q in sorted(lob.asks.items()):
            w.writerow([run_id, event_id, "ask", f"{p:.{decimals}f}", f"{q:.{decimals}f}", last_update_id])

    return lob, path, last_update_id
