import csv
from pathlib import Path

import json
import requests

from mm_core.local_orderbook import LocalOrderBook


def write_snapshot_csv(
    *,
    path: Path,
    run_id: int,
    event_id: int,
    bids: list[list[str]],
    asks: list[list[str]],
    last_update_id: int,
    checksum: int | None = None,
    decimals: int = 8,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        header = ["run_id", "event_id", "side", "price", "qty", "lastUpdateId"]
        if checksum is not None:
            header.append("checksum")
        w.writerow(header)
        for p, q in sorted(((float(b[0]), float(b[1])) for b in bids), reverse=True):
            row = [run_id, event_id, "bid", f"{p:.{decimals}f}", f"{q:.{decimals}f}", last_update_id]
            if checksum is not None:
                row.append(int(checksum))
            w.writerow(row)
        for p, q in sorted(((float(a[0]), float(a[1])) for a in asks)):
            row = [run_id, event_id, "ask", f"{p:.{decimals}f}", f"{q:.{decimals}f}", last_update_id]
            if checksum is not None:
                row.append(int(checksum))
            w.writerow(row)


def write_snapshot_json(*, path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, default=str))


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
) -> tuple[LocalOrderBook, Path, int, dict]:
    """Fetch a REST snapshot and persist it for audit/replay.

    Snapshot path naming is event-driven so multiple snapshots per day (resyncs) do not overwrite.
    """
    if client is not None:
        try:
            snap = client.get_order_book(symbol=symbol, limit=limit)
        except Exception as exc:
            raise RuntimeError(f"REST snapshot via client failed: {exc}") from exc
    else:
        raise RuntimeError(
            "REST snapshot requires a client; Binance-only fallback removed to prevent cross-exchange misuse."
        )
    last_update_id = int(snap["lastUpdateId"])

    lob = LocalOrderBook()
    lob.load_snapshot(bids=snap["bids"], asks=snap["asks"], last_update_id=last_update_id)

    path = snapshots_dir / f"snapshot_{event_id:06d}_{tag}.csv"
    write_snapshot_csv(
        path=path,
        run_id=run_id,
        event_id=event_id,
        bids=[[str(p), str(q)] for p, q in lob.bids.items()],
        asks=[[str(p), str(q)] for p, q in lob.asks.items()],
        last_update_id=last_update_id,
        decimals=decimals,
    )

    return lob, path, last_update_id, snap
