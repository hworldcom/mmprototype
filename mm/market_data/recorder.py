# mm/market_data/recorder.py

import os
import csv
import time
import json
import gzip
import logging
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

from binance.client import Client

from mm.logging_config import setup_logging
from .ws_stream import BinanceWSStream
from .sync_engine import OrderBookSyncEngine
from .snapshot import record_rest_snapshot
from .buffered_writer import BufferedCSVWriter

ORIGINAL_RECORD_REST_SNAPSHOT = record_rest_snapshot

DECIMALS = 8
DEPTH_LEVELS = 10

HEARTBEAT_SEC = 30
SYNC_WARN_AFTER_SEC = 10
MAX_BUFFER_WARN = 5000
SNAPSHOT_LIMIT = 1000
ORDERBOOK_BUFFER_ROWS = 500
TRADES_BUFFER_ROWS = 1000
BUFFER_FLUSH_INTERVAL_SEC = 1.0

# If True, write raw WS depth diffs for production-faithful replay
STORE_DEPTH_DIFFS = True


def berlin_now():
    return datetime.now(ZoneInfo("Europe/Berlin"))


def run_recorder():
    symbol = os.getenv("SYMBOL", "").upper().strip()
    if not symbol:
        raise RuntimeError("SYMBOL environment variable is required (e.g. SYMBOL=BTCUSDT).")

    # Per-day folder (Berlin date)
    day_str = berlin_now().strftime("%Y%m%d")
    symbol_dir = Path("data") / symbol
    day_dir = symbol_dir / day_str
    day_dir.mkdir(parents=True, exist_ok=True)

    snapshots_dir = day_dir / "snapshots"
    diffs_dir = day_dir / "diffs"
    if STORE_DEPTH_DIFFS:
        diffs_dir.mkdir(parents=True, exist_ok=True)

    log_path = setup_logging("INFO", component="recorder", subdir=symbol)
    log = logging.getLogger("market_data.recorder")
    log.info("Recorder logging to %s", log_path)

    # Recording window in Berlin time (8:00 -> 22:00)
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

    # Run-scoped ids for audit and file naming
    run_id = int(time.time() * 1000)
    event_id = int(time.time() * 1000)  # or run_id

    epoch_id = 0  # increments on each (re)sync epoch, useful for replay alignment

    def next_event_id() -> int:
        nonlocal event_id
        event_id += 1
        return event_id

    # Outputs
    ob_path = day_dir / f"orderbook_ws_depth_{symbol}_{day_str}.csv"
    tr_path = day_dir / f"trades_ws_{symbol}_{day_str}.csv"
    gap_path = day_dir / f"gaps_{symbol}_{day_str}.csv"
    ev_path = day_dir / f"events_{symbol}_{day_str}.csv"

    def open_csv_append(path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        existed = path.exists()
        f = path.open("a", newline="")
        is_new = (not existed) or (path.stat().st_size == 0)
        return f, is_new

    ob_header = (
        ["event_time_ms", "recv_time_ms", "run_id", "epoch_id"]
        + sum(
            [
                [f"bid{i}_price", f"bid{i}_qty", f"ask{i}_price", f"ask{i}_qty"]
                for i in range(1, DEPTH_LEVELS + 1)
            ],
            [],
        )
    )
    tr_header = ["event_time_ms", "recv_time_ms", "run_id", "price", "qty", "is_buyer_maker"]

    ob_writer = BufferedCSVWriter(
        ob_path,
        header=ob_header,
        flush_rows=ORDERBOOK_BUFFER_ROWS,
        flush_interval_s=BUFFER_FLUSH_INTERVAL_SEC,
    )
    tr_writer = BufferedCSVWriter(
        tr_path,
        header=tr_header,
        flush_rows=TRADES_BUFFER_ROWS,
        flush_interval_s=BUFFER_FLUSH_INTERVAL_SEC,
    )

    ob_writer.ensure_file()
    tr_writer.ensure_file()

    gap_f, gap_new = open_csv_append(gap_path)
    ev_f, ev_new = open_csv_append(ev_path)

    gap_w = csv.writer(gap_f)
    ev_w = csv.writer(ev_f)

    if gap_new:
        gap_w.writerow(["recv_time_ms", "run_id", "epoch_id", "event", "details"])
        gap_f.flush()

    if ev_new:
        ev_w.writerow(["event_id", "recv_time_ms", "run_id", "type", "epoch_id", "details_json"])
        ev_f.flush()
    diff_f = None
    if STORE_DEPTH_DIFFS:
        diff_path = diffs_dir / f"depth_diffs_{symbol}_{day_str}.ndjson.gz"
        diff_f = gzip.open(diff_path, "at", encoding="utf-8")

    log.info("Day dir:         %s", day_dir)
    log.info("Orderbook out:   %s", ob_path)
    log.info("Trades out:      %s", tr_path)
    log.info("Gaps out:        %s", gap_path)
    log.info("Events out:      %s", ev_path)
    if STORE_DEPTH_DIFFS:
        log.info("Diffs out:       %s", diff_path)

    engine = OrderBookSyncEngine()
    resync_count = 0

    # Telemetry
    proc_t0 = time.time()
    last_hb = time.time()
    sync_t0 = time.time()
    last_sync_warn = time.time()

    depth_msg_count = 0
    trade_msg_count = 0
    ob_rows_written = 0
    tr_rows_written = 0
    last_depth_event_ms = None
    last_trade_event_ms = None

    def emit_event(ev_type: str, details: dict | str) -> int:
        # Avoid failing during shutdown if events file is already closed
        if ev_f.closed:
            return -1

        eid = next_event_id()
        ts_recv_ms = int(time.time() * 1000)
        details_s = json.dumps(details, ensure_ascii=False) if isinstance(details, dict) else str(details)
        ev_w.writerow([eid, ts_recv_ms, run_id, ev_type, epoch_id, details_s])
        ev_f.flush()
        return eid

    def write_gap(event: str, details: str):
        ts_recv_ms = int(time.time() * 1000)
        gap_w.writerow([ts_recv_ms, run_id, epoch_id, event, details])
        gap_f.flush()

    def heartbeat(force: bool = False):
        nonlocal last_hb
        now_s = time.time()
        if (not force) and (now_s - last_hb < HEARTBEAT_SEC):
            return
        last_hb = now_s
        uptime = now_s - proc_t0

        log.info(
            "HEARTBEAT uptime=%.0fs synced=%s snapshot=%s lastUpdateId=%s "
            "depth_msgs=%d trade_msgs=%d ob_rows=%d tr_rows=%d buffer=%d "
            "last_depth_E=%s last_trade_E=%s epoch_id=%d",
            uptime,
            engine.depth_synced,
            engine.snapshot_loaded,
            engine.lob.last_update_id,
            depth_msg_count,
            trade_msg_count,
            ob_rows_written,
            tr_rows_written,
            len(engine.buffer),
            last_depth_event_ms,
            last_trade_event_ms,
            epoch_id,
        )

    def warn_not_synced():
        nonlocal last_sync_warn
        if engine.depth_synced:
            return

        if len(engine.buffer) > MAX_BUFFER_WARN:
            log.warning("Depth buffer large: %d events (not synced). lastUpdateId=%s",
                        len(engine.buffer), engine.lob.last_update_id)

        now_s = time.time()
        if (now_s - sync_t0) > SYNC_WARN_AFTER_SEC and (now_s - last_sync_warn) > SYNC_WARN_AFTER_SEC:
            last_sync_warn = now_s
            log.warning("Still not synced after %.0fs (buffer=%d)", now_s - sync_t0, len(engine.buffer))

    def fetch_snapshot(tag: str):
        nonlocal sync_t0, last_sync_warn, epoch_id
        client = None
        if record_rest_snapshot is ORIGINAL_RECORD_REST_SNAPSHOT:
            client = Client(api_key=None, api_secret=None)

        eid = emit_event("snapshot_request", {"tag": tag, "limit": SNAPSHOT_LIMIT})
        lob, path, last_uid = record_rest_snapshot(
            client=client,
            symbol=symbol,
            day_dir=day_dir,
            snapshots_dir=snapshots_dir,
            limit=SNAPSHOT_LIMIT,
            run_id=run_id,
            event_id=eid,
            tag=tag,
            decimals=DECIMALS,
        )

        engine.adopt_snapshot(lob)
        sync_t0 = time.time()
        last_sync_warn = time.time()

        emit_event("snapshot_loaded", {"tag": tag, "lastUpdateId": last_uid, "path": str(path)})
        log.info("Snapshot %s loaded lastUpdateId=%s (%s)", tag, last_uid, path)

    def resync(reason: str):
        nonlocal resync_count, epoch_id
        resync_count += 1
        epoch_id += 1
        tag = f"resync_{resync_count:06d}"

        log.warning("Resync triggered: %s", reason)
        write_gap("resync_start", reason)
        emit_event("resync_start", {"reason": reason, "tag": tag})

        engine.reset_for_resync()

        try:
            fetch_snapshot(tag)
        except Exception as e:
            log.exception("Resync snapshot failed; closing WS")
            write_gap("fatal", f"{tag}_snapshot_failed: {e}")
            emit_event("fatal", {"reason": "resync_snapshot_failed", "tag": tag, "error": str(e)})
            stream.close()
            return

        write_gap("resync_done", f"tag={tag} lastUpdateId={engine.lob.last_update_id}")
        emit_event("resync_done", {"tag": tag, "lastUpdateId": engine.lob.last_update_id})

    def write_topn(event_time_ms: int, recv_ms: int):
        nonlocal ob_rows_written
        bids, asks = engine.lob.top_n(DEPTH_LEVELS)
        bids += [(0.0, 0.0)] * (DEPTH_LEVELS - len(bids))
        asks += [(0.0, 0.0)] * (DEPTH_LEVELS - len(asks))

        row = [event_time_ms, recv_ms, run_id, epoch_id]
        for i in range(DEPTH_LEVELS):
            bp, bq = bids[i]
            ap, aq = asks[i]
            row += [
                f"{bp:.{DECIMALS}f}",
                f"{bq:.{DECIMALS}f}",
                f"{ap:.{DECIMALS}f}",
                f"{aq:.{DECIMALS}f}",
            ]

        ob_writer.write_row(row)
        ob_rows_written += 1

    def on_open():
        nonlocal epoch_id
        epoch_id = 0
        emit_event("ws_open", {"ws_url": ws_url})
        try:
            fetch_snapshot("initial")
        except Exception as e:
            log.exception("Failed initial snapshot; closing WS")
            write_gap("fatal", f"initial_snapshot_failed: {e}")
            emit_event("fatal", {"reason": "initial_snapshot_failed", "error": str(e)})
            stream.close()

    def on_depth(data, recv_ms: int):
        nonlocal depth_msg_count, last_depth_event_ms

        depth_msg_count += 1
        last_depth_event_ms = int(data.get("E", 0))

        # Always store raw diffs for replay, even when not synced
        if diff_f is not None:
            try:
                minimal = {
                    "recv_ms": recv_ms,
                    "E": int(data.get("E", 0)),
                    "U": int(data.get("U", 0)),
                    "u": int(data.get("u", 0)),
                    "b": data.get("b", []),
                    "a": data.get("a", []),
                }
                diff_f.write(json.dumps(minimal, ensure_ascii=False) + "\n")
                diff_f.flush()
            except Exception:
                log.exception("Failed writing depth diffs")

        # Stop at end of window
        if berlin_now() >= end:
            emit_event("window_end", {"end": end.isoformat()})
            stream.close()
            return

        try:
            result = engine.feed_depth_event(data)

            if result.action == "gap":
                resync(result.details)
                heartbeat()
                return

            if result.action in ("synced", "applied") and engine.depth_synced:
                write_topn(event_time_ms=int(data.get("E", 0)), recv_ms=recv_ms)

            if result.action == "buffered":
                warn_not_synced()

        except Exception:
            log.exception("Unhandled exception in on_depth")
            resync("exception_in_on_depth")

        finally:
            heartbeat()

    def on_trade(data: dict, recv_ms: int):
        nonlocal trade_msg_count, tr_rows_written, last_trade_event_ms
        trade_msg_count += 1
        last_trade_event_ms = int(data.get("E", 0))

        try:
            tr_writer.write_row(
                [
                    int(data.get("E", 0)),
                    recv_ms,
                    run_id,
                    f'{float(data["p"]):.{DECIMALS}f}',
                    f'{float(data["q"]):.{DECIMALS}f}',
                    int(data["m"]),
                ]
            )
            tr_rows_written += 1
        except Exception:
            log.exception("Unhandled exception in on_trade (message=%s)", data)
        finally:
            heartbeat()

    ws_url = f"wss://stream.binance.com:9443/stream?streams={symbol.lower()}@depth@100ms/{symbol.lower()}@trade"

    stream = BinanceWSStream(
        ws_url=ws_url,
        on_depth=on_depth,
        on_trade=on_trade,
        on_open=on_open,
        insecure_tls=True,
    )

    emit_event("run_start", {"symbol": symbol, "day": day_str})
    log.info("Connecting WS: %s", ws_url)

    try:
        stream.run_forever()
    finally:
        # Emit stop event BEFORE closing event file
        try:
            emit_event("run_stop", {"symbol": symbol})
        except Exception:
            log.exception("Failed to emit run_stop event")

        # heartbeat can still run after this (it only logs)
        heartbeat(force=True)

        for f in (gap_f, ev_f):
            try:
                f.close()
            except Exception:
                pass

        for writer in (ob_writer, tr_writer):
            try:
                writer.close()
            except Exception:
                pass

        if diff_f is not None:
            try:
                diff_f.close()
            except Exception:
                pass

        log.info("Recorder stopped.")


def main():
    run_recorder()


if __name__ == "__main__":
    main()
