# mm/market_data/recorder.py

import os
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

HEARTBEAT_SEC = 30          # log a heartbeat every N seconds
SYNC_WARN_AFTER_SEC = 10    # warn if not synced after N seconds
MAX_BUFFER_WARN = 5000      # warn if depth buffer grows beyond this
SNAPSHOT_LIMIT = 1000       # REST snapshot depth


def berlin_now():
    return datetime.now(ZoneInfo("Europe/Berlin"))


def run_recorder():
    symbol = os.getenv("SYMBOL", "").upper().strip()
    if not symbol:
        raise RuntimeError("SYMBOL environment variable is required (e.g. SYMBOL=BTCUSDT).")


    out_dir = Path("data") / symbol
    out_dir.mkdir(parents=True, exist_ok=True)

    log_path = setup_logging("INFO", component="recorder", subdir=symbol)
    log = logging.getLogger("market_data.recorder")
    log.info("Recorder logging to %s", log_path)


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
    gap_path = out_dir / f"gaps_{symbol}_{date}.csv"

    ob_f = ob_path.open("w", newline="")
    tr_f = tr_path.open("w", newline="")
    gap_f = gap_path.open("w", newline="")

    ob_w = csv.writer(ob_f)
    tr_w = csv.writer(tr_f)
    gap_w = csv.writer(gap_f)

    ob_w.writerow(
        ["event_time_ms", "recv_time_ms"]
        + sum(
            [[f"bid{i}_price", f"bid{i}_qty", f"ask{i}_price", f"ask{i}_qty"] for i in range(1, DEPTH_LEVELS + 1)],
            [],
        )
    )
    tr_w.writerow(["event_time_ms", "price", "qty", "is_buyer_maker"])
    gap_w.writerow(["recv_time_ms", "event", "details"])

    log.info("Orderbook output: %s", ob_path)
    log.info("Trades output:    %s", tr_path)
    log.info("Gaps output:      %s", gap_path)

    # --- runtime state ---
    lob = LocalOrderBook()
    depth_buffer = []
    depth_synced = False
    snapshot_loaded = False
    resync_count = 0

    # counters / telemetry
    proc_t0 = time.time()
    last_hb = time.time()
    last_sync_warn = time.time()

    # time reference for "not synced" warnings (reset after each snapshot load)
    sync_t0 = time.time()

    depth_msg_count = 0
    trade_msg_count = 0
    ob_rows_written = 0
    tr_rows_written = 0

    last_depth_event_ms = None
    last_trade_event_ms = None

    def write_gap(event: str, details: str):
        """
        Append an event to gaps CSV. This is the 'source of truth' for data quality windows.
        """
        try:
            recv_ms = int(time.time() * 1000)
            gap_w.writerow([recv_ms, event, details])
            gap_f.flush()
        except Exception:
            log.exception("Failed writing gap event (%s, %s)", event, details)

    def log_heartbeat(force: bool = False):
        nonlocal last_hb
        now_s = time.time()
        if (not force) and (now_s - last_hb < HEARTBEAT_SEC):
            return
        last_hb = now_s

        uptime = now_s - proc_t0
        ob_size = ob_path.stat().st_size if ob_path.exists() else 0
        tr_size = tr_path.stat().st_size if tr_path.exists() else 0
        gap_size = gap_path.stat().st_size if gap_path.exists() else 0

        log.info(
            "HEARTBEAT uptime=%.0fs synced=%s snapshot=%s lastUpdateId=%s "
            "depth_msgs=%d trade_msgs=%d ob_rows=%d tr_rows=%d buffer=%d "
            "last_depth_E=%s last_trade_E=%s files=(ob=%dB tr=%dB gaps=%dB)",
            uptime,
            depth_synced,
            snapshot_loaded,
            lob.last_update_id,
            depth_msg_count,
            trade_msg_count,
            ob_rows_written,
            tr_rows_written,
            len(depth_buffer),
            last_depth_event_ms,
            last_trade_event_ms,
            ob_size,
            tr_size,
            gap_size,
        )

    def fetch_snapshot(tag: str):
        """
        Fetch REST snapshot and replace local order book state.
        Raises on failure.
        """
        nonlocal lob, snapshot_loaded, sync_t0, last_sync_warn

        client = Client(api_key=None, api_secret=None)
        lob, path = record_rest_snapshot(
            client,
            symbol,
            out_dir,
            limit=SNAPSHOT_LIMIT,
            tag=tag,
        )
        snapshot_loaded = True

        # reset sync timers
        sync_t0 = time.time()
        last_sync_warn = time.time()

        log.info("Snapshot %s loaded lastUpdateId=%s (%s)", tag, lob.last_update_id, path)

    def on_open():
        try:
            log.info("WS opened → fetching REST snapshot (tag=initial, limit=%d)", SNAPSHOT_LIMIT)
            fetch_snapshot("initial")
        except Exception:
            # no snapshot means we can’t sync depth; close to avoid silent buffering forever
            log.exception("Failed initial snapshot; closing WS")
            write_gap("fatal", "initial_snapshot_failed")
            stream.close()

    def try_sync():
        """
        Attempt to bridge from REST snapshot lastUpdateId using buffered WS diffs.
        """
        nonlocal depth_synced, depth_buffer, last_sync_warn

        if not snapshot_loaded or lob.last_update_id is None:
            return

        lu = lob.last_update_id

        if len(depth_buffer) > MAX_BUFFER_WARN:
            log.warning("Depth buffer large: %d events (not synced yet). lastUpdateId=%s", len(depth_buffer), lu)

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
                    log.info("Depth synced at updateId=%s (bridge U=%s u=%s)", lob.last_update_id, U, u)
                else:
                    log.warning("Bridge found but apply_diff failed (U=%s u=%s last=%s)", U, u, lu)
                return

        # periodic warn
        now_s = time.time()
        if (now_s - sync_t0) > SYNC_WARN_AFTER_SEC and (now_s - last_sync_warn) > SYNC_WARN_AFTER_SEC:
            last_sync_warn = now_s
            log.warning("Still not synced after %.0fs (buffer=%d lastUpdateId=%s)", now_s - sync_t0, len(depth_buffer), lu)

    def resync(reason: str):
        """
        Triggered on sequence gap or fatal on_depth exception.
        Resets book state, writes gaps events, fetches a new snapshot, then waits to re-sync via buffered diffs.
        """
        nonlocal lob, depth_synced, snapshot_loaded, depth_buffer, resync_count

        resync_count += 1
        tag = f"resync_{resync_count:03d}"

        log.warning("Resync triggered: %s", reason)
        write_gap("resync_start", reason)

        # reset state
        lob = LocalOrderBook()
        depth_buffer.clear()
        depth_synced = False
        snapshot_loaded = False

        try:
            fetch_snapshot(tag)
        except Exception:
            log.exception("Resync snapshot failed; closing WS")
            write_gap("fatal", f"{tag}_snapshot_failed")
            stream.close()
            return

        write_gap("resync_done", f"tag={tag} lastUpdateId={lob.last_update_id}")

    def on_depth(data, recv_ms):
        nonlocal depth_synced, depth_msg_count, ob_rows_written, last_depth_event_ms

        try:
            depth_msg_count += 1
            last_depth_event_ms = int(data.get("E", 0))

            # Stop at end of window
            if berlin_now() >= end:
                log.info("End window reached; closing")
                stream.close()
                return

            # Buffer until snapshot exists
            if not snapshot_loaded:
                depth_buffer.append(data)
                log_heartbeat()
                return

            # Sync using buffered events
            if not depth_synced:
                depth_buffer.append(data)
                try_sync()
                log_heartbeat()
                return

            # Apply diffs once synced
            ok = lob.apply_diff(int(data["U"]), int(data["u"]), data.get("b", []), data.get("a", []))
            if not ok:
                resync(f"gap U={data.get('U')} u={data.get('u')} last={lob.last_update_id}")
                return

            bids, asks = lob.top_n(DEPTH_LEVELS)
            bids += [(0.0, 0.0)] * (DEPTH_LEVELS - len(bids))
            asks += [(0.0, 0.0)] * (DEPTH_LEVELS - len(asks))

            row = [int(data.get("E", 0)), recv_ms]
            for i in range(DEPTH_LEVELS):
                bp, bq = bids[i]
                ap, aq = asks[i]
                row += [
                    f"{bp:.{DECIMALS}f}",
                    f"{bq:.{DECIMALS}f}",
                    f"{ap:.{DECIMALS}f}",
                    f"{aq:.{DECIMALS}f}",
                ]

            ob_w.writerow(row)
            ob_f.flush()
            ob_rows_written += 1

            log_heartbeat()

        except Exception:
            log.exception("Unhandled exception in on_depth")
            resync("exception_in_on_depth")

    def on_trade(data, recv_ms):
        nonlocal trade_msg_count, tr_rows_written, last_trade_event_ms

        try:
            trade_msg_count += 1
            last_trade_event_ms = int(data.get("E", 0))

            tr_w.writerow(
                [
                    int(data.get("E", 0)),
                    f'{float(data["p"]):.{DECIMALS}f}',
                    f'{float(data["q"]):.{DECIMALS}f}',
                    int(data["m"]),
                ]
            )
            tr_f.flush()
            tr_rows_written += 1

        except Exception:
            # Trade errors are non-fatal: log and continue
            log.exception("Unhandled exception in on_trade (message=%s)", data)

        finally:
            log_heartbeat()

    ws_url = f"wss://stream.binance.com:9443/stream?streams={symbol.lower()}@depth@100ms/{symbol.lower()}@trade"

    # NOTE: insecure_tls=True is for debugging only (certificate issues).
    # Once your certs are fixed, set it to False.
    stream = BinanceWSStream(
        ws_url=ws_url,
        on_depth=on_depth,
        on_trade=on_trade,
        on_open=on_open,
        insecure_tls=True,
    )

    log.info("Starting recorder: symbol=%s depth_levels=%d snapshot_limit=%d heartbeat=%ds",
             symbol, DEPTH_LEVELS, SNAPSHOT_LIMIT, HEARTBEAT_SEC)
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
        try:
            gap_f.close()
        except Exception:
            pass
        log_heartbeat(force=True)
        log.info("Recorder stopped.")


def main():
    run_recorder()


if __name__ == "__main__":
    main()
