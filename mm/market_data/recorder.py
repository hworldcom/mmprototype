# mm/market_data/recorder.py

import csv
import time
import logging
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

from binance.client import Client

from mm.logging_config import setup_logging
from .local_orderbook import LocalOrderBook
from .snapshot import record_rest_snapshot
from .ws_stream import BinanceWSStream

DECIMALS = 8
DEPTH_LEVELS = 10


def berlin_now():
    return datetime.now(ZoneInfo("Europe/Berlin"))


def run_recorder():
    setup_logging("INFO")
    log = logging.getLogger("market_data.recorder")

    symbol = "BTCUSDT"
    out_dir = Path("data")
    out_dir.mkdir(exist_ok=True)

    start = berlin_now().replace(hour=8, minute=0, second=0, microsecond=0)
    end = berlin_now().replace(hour=22, minute=0, second=0, microsecond=0)

    now = berlin_now()
    if now > end:
        log.info("Now is past end of window (%s). Exiting.", end.isoformat())
        return

    if now < start:
        sleep_s = (start - now).total_seconds()
        log.info("Before start window. Sleeping %.1fs until %s.", sleep_s, start.isoformat())
        time.sleep(max(0.0, sleep_s))

    date = datetime.utcnow().strftime("%Y%m%d")

    ob_path = out_dir / f"orderbook_ws_depth_{symbol}_{date}.csv"
    tr_path = out_dir / f"trades_ws_{symbol}_{date}.csv"

    ob_f = ob_path.open("w", newline="")
    tr_f = tr_path.open("w", newline="")

    ob_w = csv.writer(ob_f)
    tr_w = csv.writer(tr_f)

    ob_w.writerow(
        ["event_time_ms", "recv_time_ms"]
        + sum(
            [[f"bid{i}_price", f"bid{i}_qty", f"ask{i}_price", f"ask{i}_qty"] for i in range(1, DEPTH_LEVELS + 1)],
            [],
        )
    )
    tr_w.writerow(["event_time_ms", "price", "qty", "is_buyer_maker"])

    log.info("Orderbook output: %s", ob_path)
    log.info("Trades output:    %s", tr_path)

    lob = LocalOrderBook()
    depth_buffer = []
    depth_synced = False
    snapshot_loaded = False

    def on_open():
        nonlocal lob, snapshot_loaded
        log.info("WS opened → fetching REST snapshot")
        client = Client(api_key=None, api_secret=None)
        lob = record_rest_snapshot(client, symbol, out_dir, limit=1000)
        snapshot_loaded = True
        log.info("Snapshot loaded lastUpdateId=%s", lob.last_update_id)

    def try_sync():
        nonlocal depth_synced, depth_buffer
        if not snapshot_loaded or lob.last_update_id is None:
            return

        lu = lob.last_update_id

        # Sort for stability
        depth_buffer.sort(key=lambda ev: int(ev.get("u", 0)))

        for ev in list(depth_buffer):
            U, u = int(ev["U"]), int(ev["u"])

            if u <= lu:
                depth_buffer.remove(ev)
                continue

            bridges = (U <= lu <= u) or (U <= lu + 1 <= u)
            if bridges:
                ok = lob.apply_diff(U, u, ev.get("b", []), ev.get("a", []))
                if ok:
                    depth_synced = True
                    depth_buffer.remove(ev)
                    log.info("Depth synced at updateId=%s", lob.last_update_id)
                return  # either synced or not; stop scan for now

    def on_depth(data, recv_ms):
        nonlocal depth_synced

        # Stop at end of window
        if berlin_now() >= end:
            log.info("Reached end of window (%s). Closing.", end.isoformat())
            stream.close()
            return

        # Buffer until snapshot exists
        if not snapshot_loaded:
            depth_buffer.append(data)
            return

        # Sync using buffered events
        if not depth_synced:
            depth_buffer.append(data)
            try_sync()
            return

        # Apply diffs once synced
        ok = lob.apply_diff(int(data["U"]), int(data["u"]), data.get("b", []), data.get("a", []))
        if not ok:
            log.warning("Depth gap detected (sequence broken). Closing stream; restart to resync.")
            stream.close()
            return

        bids, asks = lob.top_n(DEPTH_LEVELS)
        bids += [(0.0, 0.0)] * (DEPTH_LEVELS - len(bids))
        asks += [(0.0, 0.0)] * (DEPTH_LEVELS - len(asks))

        row = [int(data.get("E", 0)), recv_ms]
        for i in range(DEPTH_LEVELS):
            bp, bq = bids[i]
            ap, aq = asks[i]
            row += [f"{bp:.{DECIMALS}f}", f"{bq:.{DECIMALS}f}", f"{ap:.{DECIMALS}f}", f"{aq:.{DECIMALS}f}"]

        ob_w.writerow(row)
        ob_f.flush()

    def on_trade(data, recv_ms):
        tr_w.writerow(
            [
                int(data.get("E", 0)),
                f'{float(data["p"]):.{DECIMALS}f}',
                f'{float(data["q"]):.{DECIMALS}f}',
                int(data["m"]),
            ]
        )
        tr_f.flush()

    ws_url = "wss://stream.binance.com:9443/stream?streams=btcusdt@depth@100ms/btcusdt@trade"

    # NOTE: insecure_tls=True is for debugging only (certificate issues).
    # Once your certs are fixed, set it to False.
    stream = BinanceWSStream(
        ws_url=ws_url,
        on_depth=on_depth,
        on_trade=on_trade,
        on_open=on_open,
        insecure_tls=True,
    )

    log.info("Connecting WS: %s", ws_url)
    try:
        stream.run_forever()
    finally:
        try:
            ob_f.close()
        except Exception:
            pass
        try:
            tr_f.close()
        except Exception:
            pass
        log.info("Recorder stopped.")


def main():
    run_recorder()


if __name__ == "__main__":
    main()
