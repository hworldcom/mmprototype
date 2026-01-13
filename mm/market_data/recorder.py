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

"""Market data recorder.

Note: The project supports running unit tests in minimal environments where
`python-binance` may not be installed. We therefore import it lazily.
"""

try:
    from binance.client import Client  # type: ignore
except Exception:  # pragma: no cover
    Client = None  # type: ignore

from mm.logging_config import setup_logging
from .ws_stream import BinanceWSStream
from .sync_engine import OrderBookSyncEngine
from .snapshot import record_rest_snapshot
from .buffered_writer import BufferedCSVWriter, BufferedTextWriter
from .schema import write_schema, SCHEMA_VERSION

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

# WS keepalive/reconnect
WS_PING_INTERVAL_S = int(os.getenv("WS_PING_INTERVAL_S", "20"))
WS_PING_TIMEOUT_S = int(os.getenv("WS_PING_TIMEOUT_S", "10"))
WS_RECONNECT_BACKOFF_S = float(os.getenv("WS_RECONNECT_BACKOFF_S", "1.0"))

# TLS verification should remain enabled by default.
INSECURE_TLS = os.getenv("INSECURE_TLS", "0").strip() in ("1", "true", "True")

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

    # Current wall-clock time (Berlin). Used to decide whether to sleep until
    # the recording window starts.
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

    # Global receive sequence. This is incremented for every recorded message
    # (depth diff, trade, and internal recorder events) so that replay can
    # establish a deterministic total order even when recv_time_ms ties occur.
    recv_seq = 0

    def next_recv_seq() -> int:
        nonlocal recv_seq
        recv_seq += 1
        return recv_seq

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
        ["event_time_ms", "recv_time_ms", "recv_seq", "run_id", "epoch_id"]
        + sum(
            [
                [f"bid{i}_price", f"bid{i}_qty", f"ask{i}_price", f"ask{i}_qty"]
                for i in range(1, DEPTH_LEVELS + 1)
            ],
            [],
        )
    )
    tr_header = [
        "event_time_ms",
        "recv_time_ms",
        "recv_seq",
        "run_id",
        "trade_id",
        "trade_time_ms",
        "price",
        "qty",
        "is_buyer_maker",
    ]




    # Write per-day schema metadata for controlled format evolution.
    # This file is overwritten on each recorder start to reflect current schema.
    schema_path = day_dir / "schema.json"
    files_schema = {
        "orderbook_ws_depth_csv": {
            "path": str(ob_path.name),
            "format": "csv",
            "columns": ob_header,
        },
        "trades_ws_csv": {
            "path": str(tr_path.name),
            "format": "csv",
            "columns": tr_header,
        },
        "gaps_csv": {
            "path": str(gap_path.name),
            "format": "csv",
            "columns": ["recv_time_ms", "recv_seq", "run_id", "epoch_id", "event", "details"],
        },
        "events_csv": {
            "path": str(ev_path.name),
            "format": "csv",
            "columns": ["event_id", "recv_time_ms", "recv_seq", "run_id", "type", "epoch_id", "details_json"],
        },
    }
    if STORE_DEPTH_DIFFS:
        files_schema["depth_diffs_ndjson_gz"] = {
            "path": f"diffs/depth_diffs_{symbol}_{day_str}.ndjson.gz",
            "format": "ndjson.gz",
            "fields": ["recv_ms", "recv_seq", "E", "U", "u", "b", "a"],
        }
    write_schema(schema_path, files_schema)

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
        gap_w.writerow(["recv_time_ms", "recv_seq", "run_id", "epoch_id", "event", "details"])
        gap_f.flush()

    if ev_new:
        ev_w.writerow(["event_id", "recv_time_ms", "recv_seq", "run_id", "type", "epoch_id", "details_json"])
        ev_f.flush()
    diff_writer: BufferedTextWriter | None = None
    if STORE_DEPTH_DIFFS:
        diff_path = diffs_dir / f"depth_diffs_{symbol}_{day_str}.ndjson.gz"
        # Buffer gzip writes to avoid per-message flush costs.
        diff_writer = BufferedTextWriter(
            diff_path,
            flush_lines=5000,
            flush_interval_s=BUFFER_FLUSH_INTERVAL_SEC,
            opener=lambda p: gzip.open(p, "at", encoding="utf-8"),
        )

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
        ts_recv_seq = next_recv_seq()
        details_s = json.dumps(details, ensure_ascii=False) if isinstance(details, dict) else str(details)
        ev_w.writerow([eid, ts_recv_ms, ts_recv_seq, run_id, ev_type, epoch_id, details_s])
        ev_f.flush()
        return eid

    def write_gap(event: str, details: str):
        ts_recv_ms = int(time.time() * 1000)
        ts_recv_seq = next_recv_seq()
        gap_w.writerow([ts_recv_ms, ts_recv_seq, run_id, epoch_id, event, details])
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
            if Client is None:
                raise RuntimeError(
                    "python-binance is required to fetch REST snapshots. "
                    "Install dependencies from requirements.txt (pip install -r requirements.txt)."
                )
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

    def write_topn(event_time_ms: int, recv_ms: int, recv_seq: int):
        nonlocal ob_rows_written
        bids, asks = engine.lob.top_n(DEPTH_LEVELS)
        bids += [(0.0, 0.0)] * (DEPTH_LEVELS - len(bids))
        asks += [(0.0, 0.0)] * (DEPTH_LEVELS - len(asks))

        row = [event_time_ms, recv_ms, recv_seq, run_id, epoch_id]
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

    ws_open_count = 0

    def on_open():
        nonlocal epoch_id
        nonlocal ws_open_count
        ws_open_count += 1

        # First open: initial snapshot. Any subsequent open is treated as a reconnect and triggers a resync.
        if ws_open_count == 1:
            epoch_id = 0
            emit_event(
                "ws_open",
                {
                    "ws_url": ws_url,
                    "ping_interval_s": WS_PING_INTERVAL_S,
                    "ping_timeout_s": WS_PING_TIMEOUT_S,
                    "insecure_tls": INSECURE_TLS,
                },
            )
            try:
                fetch_snapshot("initial")
            except Exception as e:
                log.exception("Failed initial snapshot; closing WS")
                write_gap("fatal", f"initial_snapshot_failed: {e}")
                emit_event("fatal", {"reason": "initial_snapshot_failed", "error": str(e)})
                stream.close()
        else:
            emit_event("ws_reconnect_open", {"ws_url": ws_url, "open_count": ws_open_count})
            # Any reconnect implies potential missed diffs; always resync.
            resync("ws_reconnect")

    def on_depth(data, recv_ms: int):
        nonlocal depth_msg_count, last_depth_event_ms

        msg_recv_seq = next_recv_seq()

        depth_msg_count += 1
        last_depth_event_ms = int(data.get("E", 0))

        # Always store raw diffs for replay, even when not synced
        if diff_writer is not None:
            try:
                minimal = {
                    "recv_ms": recv_ms,
                    "recv_seq": msg_recv_seq,
                    "E": int(data.get("E", 0)),
                    "U": int(data.get("U", 0)),
                    "u": int(data.get("u", 0)),
                    "b": data.get("b", []),
                    "a": data.get("a", []),
                }
                diff_writer.write_line(json.dumps(minimal, ensure_ascii=False) + "\n")
            except Exception:
                log.exception("Failed writing depth diffs")

        # Stop at end of window
        #if berlin_now() >= end:
        #    emit_event("window_end", {"end": end.isoformat()})
        #    stream.close()
        #    return

        try:
            result = engine.feed_depth_event(data)

            if result.action == "gap":
                resync(result.details)
                heartbeat()
                return

            if result.action in ("synced", "applied") and engine.depth_synced:
                write_topn(event_time_ms=int(data.get("E", 0)), recv_ms=recv_ms, recv_seq=msg_recv_seq)

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

        msg_recv_seq = next_recv_seq()

        try:
            tr_writer.write_row(
                [
                    int(data.get("E", 0)),
                    recv_ms,
                    msg_recv_seq,
                    run_id,
                    int(data.get("t", 0)),
                    int(data.get("T", 0)),
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

    def on_status(typ: str, details: dict):
        # Keep the on-disk events ledger authoritative for operational debugging.
        emit_event(typ, details)

    # Backwards-compatible construction: tests may monkeypatch BinanceWSStream with a
    # simplified fake that does not accept newer parameters.
    try:
        stream = BinanceWSStream(
            ws_url=ws_url,
            on_depth=on_depth,
            on_trade=on_trade,
            on_open=on_open,
            on_status=on_status,
            insecure_tls=INSECURE_TLS,
            ping_interval_s=WS_PING_INTERVAL_S,
            ping_timeout_s=WS_PING_TIMEOUT_S,
            reconnect_backoff_s=WS_RECONNECT_BACKOFF_S,
        )
    except TypeError:
        stream = BinanceWSStream(
            ws_url=ws_url,
            on_depth=on_depth,
            on_trade=on_trade,
            on_open=on_open,
            insecure_tls=INSECURE_TLS,
        )

    emit_event("run_start", {"symbol": symbol, "day": day_str})
    log.info("Connecting WS: %s", ws_url)

    try:
        run_fn = getattr(stream, "run", None) or getattr(stream, "run_forever", None)
        if run_fn is None:
            raise RuntimeError("BinanceWSStream has no run()/run_forever()")
        run_fn()
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

        if diff_writer is not None:
            try:
                diff_writer.close()
            except Exception:
                pass

        log.info("Recorder stopped.")


def main():
    # Ensure we always surface exceptions in logs (both file and stdout).
    # In cron/docker contexts stderr may be discarded, so we log the traceback.
    try:
        run_recorder()
    except Exception:
        logging.getLogger("market_data.recorder").exception("Recorder crashed")
        raise


if __name__ == "__main__":
    main()
