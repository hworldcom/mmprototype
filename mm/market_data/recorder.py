import os
import csv
import time
import json
import logging
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

from binance.client import Client

from mm.logging_config import setup_logging
from .ws_stream import BinanceWSStream
from .sync_engine import OrderBookSyncEngine
from .snapshot import record_rest_snapshot

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


def _read_last_event_state(events_path: Path) -> tuple[int, int]:
    if not _has_data(events_path):
        return 0, 0
    last_event_id, last_epoch_id = 0, 0
    with events_path.open("r", newline="") as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if not row:
                continue
            try:
                last_event_id = int(row[0])
                last_epoch_id = int(row[4])
            except Exception:
                continue
    return last_event_id, last_epoch_id


def run_recorder():
    symbol = os.getenv("SYMBOL", "").upper().strip()
    if not symbol:
        raise RuntimeError("SYMBOL environment variable is required (e.g. SYMBOL=BTCUSDT).")

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
    day_dir = Path("data") / symbol / date
    day_dir.mkdir(parents=True, exist_ok=True)
    snapshots_dir = day_dir / "snapshots"

    orderbook_path = day_dir / "orderbook.csv"
    trades_path = day_dir / "trades.csv"
    events_path = day_dir / "events.csv"

    ob_exists = _has_data(orderbook_path)
    tr_exists = _has_data(trades_path)
    ev_exists = _has_data(events_path)

    ob_f = orderbook_path.open("a", newline="")
    tr_f = trades_path.open("a", newline="")
    ev_f = events_path.open("a", newline="")

    ob_w = csv.writer(ob_f)
    tr_w = csv.writer(tr_f)
    ev_w = csv.writer(ev_f)

    # Headers (only once per file)
    if not ob_exists:
        ob_w.writerow(
            ["run_id", "epoch_id", "event_time_ms", "recv_time_ms"]
            + sum(
                [[f"bid{i}_price", f"bid{i}_qty", f"ask{i}_price", f"ask{i}_qty"]
                 for i in range(1, DEPTH_LEVELS + 1)],
                [],
            )
        )

    if not tr_exists:
        tr_w.writerow(["run_id", "event_time_ms", "price", "qty", "is_buyer_maker"])
    if not ev_exists:
        ev_w.writerow(["event_id", "ts_recv_ms", "run_id", "type", "epoch_id", "details"])

    log.info("Data dir: %s", day_dir)

    last_event_id, last_epoch_id = _read_last_event_state(events_path)
    event_id = last_event_id
    epoch_id = last_epoch_id

    def next_event_id() -> int:
        nonlocal event_id
        event_id += 1
        return event_id

    def emit_event(ev_type: str, details: dict | str, epoch: int | None = None) -> int:
        eid = next_event_id()
        ts_recv_ms = int(time.time() * 1000)
        ep = epoch_id if epoch is None else epoch
        details_s = json.dumps(details, ensure_ascii=False) if isinstance(details, dict) else str(details)
        ev_w.writerow([eid, ts_recv_ms, run_id, ev_type, ep, details_s])
        ev_f.flush()
        return eid

    emit_event("run_start", {"symbol": symbol, "date": date}, epoch=epoch_id)

    engine = OrderBookSyncEngine()
    resync_count = 0

    # telemetry
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

    def heartbeat(force: bool = False):
        nonlocal last_hb
        now_s = time.time()
        if (not force) and (now_s - last_hb < HEARTBEAT_SEC):
            return
        last_hb = now_s
        log.info(
            "HEARTBEAT uptime=%.0fs synced=%s snapshot=%s lastUpdateId=%s "
            "depth_msgs=%d trade_msgs=%d ob_rows=%d tr_rows=%d buffer=%d "
            "last_depth_E=%s last_trade_E=%s epoch_id=%d",
            now_s - proc_t0,
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
            log.warning("Depth buffer large: %d events (not synced yet).", len(engine.buffer))
        now_s = time.time()
        if (now_s - sync_t0) > SYNC_WARN_AFTER_SEC and (now_s - last_sync_warn) > SYNC_WARN_AFTER_SEC:
            last_sync_warn = now_s
            log.warning("Still not synced after %.0fs (buffer=%d)", now_s - sync_t0, len(engine.buffer))

    def fetch_snapshot(tag: str):
        nonlocal sync_t0, last_sync_warn
        client = Client(api_key=None, api_secret=None)
        eid = next_event_id()  # reserve ID for snapshot file + event row
        lob, snap_path, last_uid = record_rest_snapshot(
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
        ts_recv_ms = int(time.time() * 1000)
        ev_w.writerow([eid, ts_recv_ms, run_id, "snapshot_taken", epoch_id, json.dumps({
            "path": str(snap_path),
            "lastUpdateId": last_uid,
            "tag": tag,
        })])
        ev_f.flush()

        engine.adopt_snapshot(lob)
        sync_t0 = time.time()
        last_sync_warn = time.time()
        log.info("Snapshot %s loaded lastUpdateId=%s (%s)", tag, last_uid, snap_path)

    def mark_synced():
        nonlocal epoch_id
        if epoch_id == 0:
            epoch_id = 1
        emit_event("synced", {"lastUpdateId": engine.lob.last_update_id}, epoch=epoch_id)

    def resync(reason: str):
        nonlocal resync_count, epoch_id
        resync_count += 1
        tag = f"resync_{resync_count:03d}"

        emit_event("resync_start", {"reason": reason, "lastUpdateId": engine.lob.last_update_id}, epoch=epoch_id)
        log.warning("Resync triggered: %s", reason)

        engine.reset_for_resync()
        fetch_snapshot(tag)

        epoch_id += 1
        emit_event("resync_done", {"lastUpdateId": engine.lob.last_update_id, "tag": tag}, epoch=epoch_id)

    def write_topn(event_time_ms: int, recv_ms: int):
        nonlocal ob_rows_written
        bids, asks = engine.lob.top_n(DEPTH_LEVELS)
        bids += [(0.0, 0.0)] * (DEPTH_LEVELS - len(bids))
        asks += [(0.0, 0.0)] * (DEPTH_LEVELS - len(asks))
        row = [run_id, epoch_id, event_time_ms, recv_ms]
        for i in range(DEPTH_LEVELS):
            bp, bq = bids[i]
            ap, aq = asks[i]
            row += [f"{bp:.{DECIMALS}f}", f"{bq:.{DECIMALS}f}", f"{ap:.{DECIMALS}f}", f"{aq:.{DECIMALS}f}"]
        ob_w.writerow(row)
        ob_f.flush()
        ob_rows_written += 1

    def on_open():
        fetch_snapshot("initial")

    def on_depth(data: dict, recv_ms: int):
        nonlocal depth_msg_count, last_depth_event_ms
        depth_msg_count += 1
        last_depth_event_ms = int(data.get("E", 0))

        if berlin_now() >= end:
            emit_event("run_stop", {"reason": "end_window"}, epoch=epoch_id)
            log.info("End window reached; closing")
            stream.close()
            return

        result = engine.feed_depth_event(data)

        if result.action == "gap":
            resync(result.details)
            return

        if result.action == "synced":
            mark_synced()
            write_topn(event_time_ms=int(data.get("E", 0)), recv_ms=recv_ms)
        elif result.action == "applied" and engine.depth_synced and epoch_id >= 1:
            write_topn(event_time_ms=int(data.get("E", 0)), recv_ms=recv_ms)
        else:
            warn_not_synced()

        heartbeat()

    def on_trade(data: dict, recv_ms: int):
        nonlocal trade_msg_count, tr_rows_written, last_trade_event_ms
        trade_msg_count += 1
        last_trade_event_ms = int(data.get("E", 0))
        tr_w.writerow([
            run_id,
            int(data.get("E", 0)),
            f"{float(data['p']):.{DECIMALS}f}",
            f"{float(data['q']):.{DECIMALS}f}",
            int(data["m"]),
        ])
        tr_f.flush()
        tr_rows_written += 1
        heartbeat()

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
        for f in (ob_f, tr_f, ev_f):
            try:
                f.close()
            except Exception:
                pass
        heartbeat(force=True)
        log.info("Recorder stopped.")


def main():
    run_recorder()


if __name__ == "__main__":
    main()
