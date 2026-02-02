import csv
from pathlib import Path

import requests

from mm_core.local_orderbook import LocalOrderBook


def record_rest_snapshot(
    client,
    symbol: str,
    day_dir: Path,
    snapshots_dir: Path,
    limit: int,
    run_id: int,
    event_id: int,
    tag: str,
    decimals: int = 8,
) -> tuple[LocalOrderBook, Path, int]:
    """Fetch a REST snapshot and persist it for audit/replay.

    Snapshot path naming is event-driven so multiple snapshots per day (resyncs) do not overwrite.
    """
    if client is not None:
        try:
            snap = client.get_order_book(symbol=symbol, limit=limit)
        except Exception as exc:
            raise RuntimeError(f"REST snapshot via client failed: {exc}") from exc
    else:
        url = "https://api.binance.com/api/v3/depth"
        resp = requests.get(url, params={"symbol": symbol, "limit": limit}, timeout=10)
        resp.raise_for_status()
        snap = resp.json()
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
