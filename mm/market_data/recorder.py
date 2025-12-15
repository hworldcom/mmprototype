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
from .snapshot import record_rest_snapshot
from .ws_stream import BinanceWSStream
from .sync_engine import OrderBookSyncEngine

DECIMALS = 8
DEPTH_LEVELS = 10

HEARTBEAT_SEC = 30          # log a heartbeat every N seconds
SYNC_WARN_AFTER_SEC = 10    # warn if not synced after N seconds
MAX_BUFFER_WARN = 5000      # warn if depth buffer grows beyond this
SNAPSHOT_LIMIT = 1000       # REST snapshot depth


def berlin_now():
    return datetime.now(ZoneInfo("Europe/Berlin"))


def _has_data(p: Path) -> bool:
    return p.exists() and p.stat().st_size > 0


def run_recorder():
    symbol = os.getenv("SYMBOL", "").upper().strip()
    if not symbol:
        raise RuntimeError("SYMBOL environment variable is required (e.g. SYMBOL=BTCUSDT).")

    out_dir = Path("data") / symbol
    out_dir.mkdir(parents=True, exist_ok=True)

    log_path = setup_logging("INFO", component="recorder", subdir=symbol)
    log = logging.getLogger("market_data.recorder")
    log.info("Recorder logging to %s", log_path)

    # Unique identifier for this recorder process
    run_id = int(time.time() * 1000)
    log.info("Run ID: %s", run_id)

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

    ob_exists = _has_data(ob_path)
    tr_exists = _has_data(tr_path)
    gap_exists = _has_data(gap_path)

    # Append mode prevents overwriting on restarts
    ob_f = ob_path.open("a", newline="")
    tr_f = tr_path.open("a", newline="")
    gap_f = gap_path.open("a", newline="")

    ob_w = csv.writer(ob_f)
    tr_w = csv.writer(tr_f)
    gap_w = csv.writer(gap_f)

    # Headers (only once per file)
    if not ob_exists:
        ob_w.writerow(
            ["run_id", "event_time_ms", "recv_time_ms"]
            + sum(
                [[f"bid{i}_price", f"bid{i}_qty", f"ask{i}_price", f"ask{i}_qty"]
                 for i in range(1, DEPTH_LEVELS + 1)],
                [],
            )
        )

    if not tr_exists:
        tr_w.writerow(["run_id", "event_time_ms", "price", "qty", "is_buyer_maker"])

    if not gap_exists:
        gap_w.writerow(["run_id", "recv_time_ms", "event", "details"])

    log.info("Orderbook output: %s (%s)", ob_path, "append" if ob_exists else "new")
    log.info("Trades output:    %s (%s)", tr_path, "append" if tr_exists else "new")
    log.info("Gaps output:      %s (%s)", gap_path, "append" if gap_exists else "new")

    # --- state ---
    engine = OrderBookSyncEngine()
    resync_count = 0

    # telemetry
    proc_t0 = time.time()
    last_hb = time.time()

    depth_msg_count = 0
    trade_msg_count = 0
    ob_rows_written = 0
    tr_rows_written = 0

    last_depth_event_ms = None
    last_trade_event_ms = None

    sync_t0 = time.time()
    last_sync_warn = time.time()

    def write_gap(event: str, details: str):
        try:
            recv_ms = int(time.time() * 1000)
            gap_w.writerow([run_id, recv_ms, event, details])
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
        log.info(
            "HEARTBEAT uptime=%.0fs synced=%s snapshot=%s lastUpdateId=%s "
            "depth_msgs=%d trade_msgs=%d ob_rows=%d tr_rows=%d buffer=%d "
            "last_depth_E=%s last_trade_E=%s",
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
        )

    def fetch_snapshot(tag: str):
        nonlocal sync_t0, last_sync_warn
        client = Client(api_key=None, api_secret=None)
        lob, path = record_rest_snapshot(
            client,
            symbol,
            out_dir,
            limit=SNAPSHOT_LIMIT,
            tag=tag,
        )
        engine.adopt_snapshot(lob)
        sync_t0 = time.time()
        last_sync_warn = time.time()
        log.info("Snapshot %s loaded lastUpdateId=%s (%s)", tag, engine.lob.last_update_id, path)

    def maybe_warn_not_synced():
        nonlocal last_sync_warn
        if engine.depth_synced:
            return

        if len(engine.buffer) > MAX_BUFFER_WARN:
            log.warning("Depth buffer large: %d events", len(engine.buffer))

        now_s = time.time()
        if (now_s - sync_t0) > SYNC_WARN_AFTER_SEC and (now_s - last_sync_warn) > SYNC_WARN_AFTER_SEC:
            last_sync_warn = now_s
            log.warning("Still not synced after %.0fs (buffer=%d)",
                        now_s - sync_t0, len(engine.buffer))

    def write_topn(event_time_ms: int, recv_ms: int):
        nonlocal ob_rows_written
        bids, asks = engine.lob.top_n(DEPTH_LEVELS)
        bids += [(0.0, 0.0)] * (DEPTH_LEVELS - len(bids))
        asks += [(0.0, 0.0)] * (DEPTH_LEVELS - len(asks))

        row = [run_id, event_time_ms, recv_ms]
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

    def resync(reason: str):
        nonlocal resync_count
        resync_count += 1
        tag = f"resync_{resync_count:03d}"

        log.warning("Resync triggered: %s", reason)
        write_gap("resync_start", reason)

        engine.reset_for_resync()

        try:
            fetch_snapshot(tag)
        except Exception:
            log.exception("Resync snapshot failed; closing WS")
            write_gap("fatal", f"{tag}_snapshot_failed")
            stream.close()
            return

        write_gap("resync_done", f"tag={tag} lastUpdateId={engine.lob.last_update_id}")

    def on_open():
        try:
            log.info("WS opened → fetching REST snapshot (initial)")
            fetch_snapshot("initial")
        except Exception:
            log.exception("Failed initial snapshot; closing WS")
            write_gap("fatal", "initial_snapshot_failed")
            stream.close()

    def on_depth(data, recv_ms):
        nonlocal depth_msg_count, last_depth_event_ms
        try:
            depth_msg_count += 1
            last_depth_event_ms = int(data.get("E", 0))

            if berlin_now() >= end:
                log.info("End window reached; closing")
                stream.close()
                return

            result = engine.feed_depth_event(data)

            if result.action == "gap":
                resync(result.details)
                return

            if result.action in ("synced", "applied") and engine.depth_synced:
                write_topn(event_time_ms=int(data.get("E", 0)), recv_ms=recv_ms)

            if result.action == "buffered":
                maybe_warn_not_synced()

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
                    run_id,
                    int(data.get("E", 0)),
                    f'{float(data["p"]):.{DECIMALS}f}',
                    f'{float(data["q"]):.{DECIMALS}f}',
                    int(data["m"]),
                ]
            )
            tr_f.flush()
            tr_rows_written += 1

        except Exception:
            log.exception("Unhandled exception in on_trade (message=%s)", data)

        finally:
            log_heartbeat()

    ws_url = f"wss://stream.binance.com:9443/stream?streams={symbol.lower()}@depth@100ms/{symbol.lower()}@trade"

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
        for f in (ob_f, tr_f, gap_f):
            try:
                f.close()
            except Exception:
                pass
        log_heartbeat(force=True)
        log.info("Recorder stopped.")


def main():
    run_recorder()


if __name__ == "__main__":
    main()
