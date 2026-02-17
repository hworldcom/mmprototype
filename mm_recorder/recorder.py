# mm/market_data/recorder.py

import os
import csv
import time
import json
import gzip
import logging
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from enum import Enum

"""Market data recorder.

Note: The project supports running unit tests in minimal environments where
`python-binance` may not be installed. We therefore import it lazily.
"""

from mm_recorder.logging_config import setup_logging
from mm_recorder.ws_stream import BinanceWSStream
from mm_recorder.snapshot import record_rest_snapshot, write_snapshot_csv, write_snapshot_json
from mm_recorder.buffered_writer import BufferedCSVWriter, BufferedTextWriter, _is_empty_text_file
from mm_recorder.live_writer import LiveNdjsonWriter
from mm_recorder.exchanges import get_adapter
from mm_recorder.exchanges.types import BookSnapshot, DepthDiff, Trade
from mm_core.schema import write_schema, SCHEMA_VERSION

ORIGINAL_RECORD_REST_SNAPSHOT = record_rest_snapshot

DECIMALS = 8
DEPTH_LEVELS = 20

HEARTBEAT_SEC = 30
SYNC_WARN_AFTER_SEC = 10
MAX_BUFFER_WARN = 5000
SNAPSHOT_LIMIT = 1000
ORDERBOOK_BUFFER_ROWS = 500
TRADES_BUFFER_ROWS = 1000
BUFFER_FLUSH_INTERVAL_SEC = 1.0

# WS keepalive/reconnect
WS_PING_INTERVAL_S = int(os.getenv("WS_PING_INTERVAL_S", "20"))
WS_PING_TIMEOUT_S = int(os.getenv("WS_PING_TIMEOUT_S", "60"))
WS_RECONNECT_BACKOFF_S = float(os.getenv("WS_RECONNECT_BACKOFF_S", "1.0"))
WS_RECONNECT_BACKOFF_MAX_S = float(os.getenv("WS_RECONNECT_BACKOFF_MAX_S", "30.0"))
WS_MAX_SESSION_S = float(os.getenv("WS_MAX_SESSION_S", str(23 * 3600 + 50 * 60)))
WS_OPEN_TIMEOUT_S = float(os.getenv("WS_OPEN_TIMEOUT_S", "10.0"))
WS_NO_DATA_WARN_S = float(os.getenv("WS_NO_DATA_WARN_S", "10.0"))

# TLS verification should remain enabled by default.
INSECURE_TLS = os.getenv("INSECURE_TLS", "0").strip() in ("1", "true", "True")

# If True, write raw WS depth diffs for production-faithful replay
STORE_DEPTH_DIFFS = True
LIVE_STREAM_ENABLED = os.getenv("LIVE_STREAM", "1").strip() in ("1", "true", "True")
LIVE_STREAM_ROTATE_S = float(os.getenv("LIVE_STREAM_ROTATE_S", "60"))
LIVE_STREAM_RETENTION_S = float(os.getenv("LIVE_STREAM_RETENTION_S", str(60 * 60)))

class RecorderPhase(str, Enum):
    CONNECTING = "connecting"
    SNAPSHOT = "snapshot"
    SYNCING = "syncing"
    SYNCED = "synced"
    RESYNCING = "resyncing"
    STOPPED = "stopped"

def window_now():
    """Current wall-clock time in the configured recording timezone.

    We intentionally read environment variables at call time so unit tests
    (and production launch scripts) can override the window parameters
    without requiring a module reload.
    """
    tz = os.getenv("WINDOW_TZ", "Europe/Berlin")
    return datetime.now(ZoneInfo(tz))


@dataclass
class RecorderState:
    recv_seq: int = 0
    event_id: int = 0
    epoch_id: int = 0
    resync_count: int = 0
    ws_open_count: int = 0
    window_end_emitted: bool = False
    last_hb: float = 0.0
    sync_t0: float = 0.0
    last_sync_warn: float = 0.0
    depth_msg_count: int = 0
    trade_msg_count: int = 0
    ob_rows_written: int = 0
    tr_rows_written: int = 0
    last_depth_event_ms: int | None = None
    last_trade_event_ms: int | None = None
    needs_snapshot: bool = False
    pending_snapshot_tag: str | None = None
    phase: RecorderPhase = RecorderPhase.CONNECTING
    last_ws_msg_time: float | None = None
    last_no_data_warn: float = 0.0
    first_data_emitted: bool = False


def _parse_hhmm(value: str, label: str) -> tuple[int, int]:
    try:
        hour_str, minute_str = value.strip().split(":")
        hour = int(hour_str)
        minute = int(minute_str)
    except Exception as exc:
        raise RuntimeError(f"{label} must be in HH:MM format (got {value!r}).") from exc
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise RuntimeError(f"{label} must be a valid 24h time (got {value!r}).")
    return hour, minute


def compute_window(now: datetime) -> tuple[datetime, datetime]:
    # Read env at runtime (not import time) so tests and launchers can set
    # the recording window via environment variables.
    start_hhmm = os.getenv("WINDOW_START_HHMM", "00:00")
    end_hhmm = os.getenv("WINDOW_END_HHMM", "00:15")
    end_day_offset = int(os.getenv("WINDOW_END_DAY_OFFSET", "1"))

    start_h, start_m = _parse_hhmm(start_hhmm, "WINDOW_START_HHMM")
    end_h, end_m = _parse_hhmm(end_hhmm, "WINDOW_END_HHMM")

    start = now.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
    end = now.replace(hour=end_h, minute=end_m, second=0, microsecond=0) + timedelta(days=end_day_offset)
    if end <= start:
        end += timedelta(days=1)
    return start, end


def run_recorder():
    exchange = os.getenv("EXCHANGE", "binance").strip().lower()
    adapter = get_adapter(exchange)
    symbol = adapter.normalize_symbol(os.getenv("SYMBOL", "").strip())
    symbol_fs = symbol.replace("/", "").replace("-", "").replace(":", "").replace(" ", "")
    if not symbol:
        raise RuntimeError("SYMBOL environment variable is required (e.g. SYMBOL=BTCUSDT).")

    now = window_now()
    window_start, window_end = compute_window(now)
    if now < window_start:
        prev_start = window_start - timedelta(days=1)
        prev_end = window_end - timedelta(days=1)
        if now <= prev_end:
            window_start = prev_start
            window_end = prev_end

    # Per-day folder (window start date)
    day_str = window_start.strftime("%Y%m%d")
    symbol_dir = Path("data") / exchange / symbol_fs
    day_dir = symbol_dir / day_str
    day_dir.mkdir(parents=True, exist_ok=True)

    snapshots_dir = day_dir / "snapshots"
    diffs_dir = day_dir / "diffs"
    trades_dir = day_dir / "trades"
    if STORE_DEPTH_DIFFS:
        diffs_dir.mkdir(parents=True, exist_ok=True)
    trades_dir.mkdir(parents=True, exist_ok=True)

    log_subdir = f"{exchange}/{symbol_fs}"
    log_path = setup_logging("INFO", component="recorder", subdir=log_subdir)
    log = logging.getLogger("market_data.recorder")
    log.info("Recorder logging to %s", log_path)
    log.info(
        "Recorder config exchange=%s symbol=%s symbol_fs=%s window=%s–%s tz=%s depth_levels=%s store_depth_diffs=%s",
        exchange,
        symbol,
        symbol_fs,
        window_start.isoformat(),
        window_end.isoformat(),
        os.getenv("WINDOW_TZ", "Europe/Berlin"),
        DEPTH_LEVELS,
        STORE_DEPTH_DIFFS,
    )
    log.info(
        "WS config ping_interval_s=%s ping_timeout_s=%s reconnect_backoff_s=%.2f reconnect_backoff_max_s=%.2f max_session_s=%.0f",
        WS_PING_INTERVAL_S,
        WS_PING_TIMEOUT_S,
        WS_RECONNECT_BACKOFF_S,
        WS_RECONNECT_BACKOFF_MAX_S,
        WS_MAX_SESSION_S,
    )
    log.info("WS connect timeout open_timeout_s=%.1f", WS_OPEN_TIMEOUT_S)

    # Recording window in configured timezone.
    start = window_start
    end = window_end

    # Current wall-clock time. Used to decide whether to sleep until
    # the recording window starts.
    if now > end:
        log.info("Now is past end of window (%s). Exiting.", end.isoformat())
        return

    if now < start:
        sleep_s = (start - now).total_seconds()
        log.info("Before start window. Sleeping %.1fs until %s.", sleep_s, start.isoformat())
        time.sleep(max(0.0, sleep_s))

    # Run-scoped ids for audit and file naming
    run_id = int(time.time() * 1000)

    def next_recv_seq() -> int:
        state.recv_seq += 1
        return state.recv_seq

    def next_event_id() -> int:
        state.event_id += 1
        return state.event_id

    sub_depth = adapter.normalize_depth(DEPTH_LEVELS)

    # Outputs
    ob_path = day_dir / f"orderbook_ws_depth_{symbol_fs}_{day_str}.csv.gz"
    tr_path = day_dir / f"trades_ws_{symbol_fs}_{day_str}.csv.gz"
    gap_path = day_dir / f"gaps_{symbol_fs}_{day_str}.csv.gz"
    ev_path = day_dir / f"events_{symbol_fs}_{day_str}.csv.gz"

    def open_csv_append(path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        existed = path.exists()
        if path.suffix == ".gz":
            f = gzip.open(path, "at", encoding="utf-8", newline="")
        else:
            f = path.open("a", newline="")
        is_new = (not existed) or _is_empty_text_file(path)
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
        "side",
        "ord_type",
        "exchange",
        "symbol",
    ]




    # Write per-day schema metadata for controlled format evolution.
    # This file is overwritten on each recorder start to reflect current schema.
    schema_path = day_dir / "schema.json"
    files_schema = {
            "orderbook_ws_depth_csv": {
            "path": str(ob_path.name),
            "format": "csv",
            "compression": "gzip",
            "columns": ob_header,
        },
        "trades_ws_csv": {
            "path": str(tr_path.name),
            "format": "csv",
            "compression": "gzip",
            "columns": tr_header,
        },
        "gaps_csv": {
            "path": str(gap_path.name),
            "format": "csv",
            "compression": "gzip",
            "columns": ["recv_time_ms", "recv_seq", "run_id", "epoch_id", "event", "details"],
        },
        "events_csv": {
            "path": str(ev_path.name),
            "format": "csv",
            "compression": "gzip",
            "columns": ["event_id", "recv_time_ms", "recv_seq", "run_id", "type", "epoch_id", "details_json"],
        },
        "snapshots_raw_json": {
            "path": "snapshots/snapshot_<event_id>_<tag>.json",
            "format": "json",
            "notes": "Raw exchange snapshot payload (REST for Binance, WS for checksum exchanges).",
        },
        "trades_ws_raw_ndjson_gz": {
            "path": f"trades/trades_ws_raw_{symbol_fs}_{day_str}.ndjson.gz",
            "format": "ndjson.gz",
            "fields": ["recv_ms", "recv_seq", "event_time_ms", "trade_id", "exchange", "symbol", "raw"],
        },
    }
    if STORE_DEPTH_DIFFS:
        diff_fields = ["recv_ms", "recv_seq", "E", "U", "u", "b", "a"]
        if adapter.sync_mode == "checksum":
            diff_fields.append("checksum")
        diff_fields.extend(["exchange", "symbol", "raw"])
        files_schema["depth_diffs_ndjson_gz"] = {
            "path": f"diffs/depth_diffs_{symbol_fs}_{day_str}.ndjson.gz",
            "format": "ndjson.gz",
            "fields": diff_fields,
            "depth": sub_depth,
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
        diff_path = diffs_dir / f"depth_diffs_{symbol_fs}_{day_str}.ndjson.gz"
        # Buffer gzip writes to avoid per-message flush costs.
        diff_writer = BufferedTextWriter(
            diff_path,
            flush_lines=5000,
            flush_interval_s=BUFFER_FLUSH_INTERVAL_SEC,
            opener=lambda p: gzip.open(p, "at", encoding="utf-8"),
        )
    tr_raw_writer = BufferedTextWriter(
        trades_dir / f"trades_ws_raw_{symbol_fs}_{day_str}.ndjson.gz",
        flush_lines=5000,
        flush_interval_s=BUFFER_FLUSH_INTERVAL_SEC,
        opener=lambda p: gzip.open(p, "at", encoding="utf-8"),
    )
    live_diff_writer: LiveNdjsonWriter | None = None
    live_trade_writer: LiveNdjsonWriter | None = None
    if LIVE_STREAM_ENABLED:
        live_dir = day_dir / "live"
        live_diff_writer = LiveNdjsonWriter(
            live_dir / "live_depth_diffs.ndjson",
            rotate_interval_s=LIVE_STREAM_ROTATE_S,
            retention_s=LIVE_STREAM_RETENTION_S,
        )
        live_trade_writer = LiveNdjsonWriter(
            live_dir / "live_trades.ndjson",
            rotate_interval_s=LIVE_STREAM_ROTATE_S,
            retention_s=LIVE_STREAM_RETENTION_S,
        )

    log.info("Day dir:         %s", day_dir)
    log.info("Orderbook out:   %s", ob_path)
    log.info("Trades out:      %s", tr_path)
    log.info("Gaps out:        %s", gap_path)
    log.info("Events out:      %s", ev_path)
    if STORE_DEPTH_DIFFS:
        log.info("Diffs out:       %s", diff_path)
    if LIVE_STREAM_ENABLED:
        log.info("Live diffs out:  %s", live_dir / "live_depth_diffs.ndjson")
        log.info("Live trades out: %s", live_dir / "live_trades.ndjson")

    engine = adapter.create_sync_engine(sub_depth)

    state = RecorderState(
        event_id=int(time.time() * 1000),
        last_hb=time.time(),
        sync_t0=time.time(),
        last_sync_warn=time.time(),
    )

    # Telemetry
    proc_t0 = time.time()

    def emit_event(ev_type: str, details: dict | str) -> int:
        # Avoid failing during shutdown if events file is already closed
        if ev_f.closed:
            return -1

        eid = next_event_id()
        ts_recv_ms = int(time.time() * 1000)
        ts_recv_seq = next_recv_seq()
        details_s = json.dumps(details, ensure_ascii=False) if isinstance(details, dict) else str(details)
        ev_w.writerow([eid, ts_recv_ms, ts_recv_seq, run_id, ev_type, state.epoch_id, details_s])
        ev_f.flush()
        return eid

    def set_phase(new_phase: RecorderPhase, reason: str | None = None) -> None:
        if state.phase == new_phase:
            return
        prev = state.phase
        state.phase = new_phase
        details = {"from": prev.value, "to": new_phase.value}
        if reason:
            details["reason"] = reason
        emit_event("state_change", details)

    def write_gap(event: str, details: str):
        ts_recv_ms = int(time.time() * 1000)
        ts_recv_seq = next_recv_seq()
        gap_w.writerow([ts_recv_ms, ts_recv_seq, run_id, state.epoch_id, event, details])
        gap_f.flush()

    def heartbeat(force: bool = False):
        now_s = time.time()
        # Hard stop at end of recording window.
        # This check is in heartbeat so we stop even if depth messages stop.
        if (not state.window_end_emitted) and window_now() >= end:
            state.window_end_emitted = True
            emit_event("window_end", {"end": end.isoformat()})
            try:
                stream.close()
            except Exception:
                log.exception("Failed to close stream on window end (heartbeat)")
            return
        if (not force) and (now_s - state.last_hb < HEARTBEAT_SEC):
            return
        state.last_hb = now_s
        uptime = now_s - proc_t0

        if state.ws_open_count > 0 and state.last_ws_msg_time is not None:
            idle_s = now_s - state.last_ws_msg_time
            if idle_s >= WS_NO_DATA_WARN_S and (now_s - state.last_no_data_warn) >= WS_NO_DATA_WARN_S:
                state.last_no_data_warn = now_s
                emit_event("ws_no_data", {"idle_s": float(idle_s)})
                log.warning("No WS data for %.1fs (phase=%s)", idle_s, state.phase.value)

        log.info(
            "HEARTBEAT uptime=%.0fs synced=%s snapshot=%s lastUpdateId=%s "
            "depth_msgs=%d trade_msgs=%d ob_rows=%d tr_rows=%d buffer=%d "
            "last_depth_E=%s last_trade_E=%s epoch_id=%d",
            uptime,
            engine.depth_synced,
            engine.snapshot_loaded,
            engine.lob.last_update_id,
            state.depth_msg_count,
            state.trade_msg_count,
            state.ob_rows_written,
            state.tr_rows_written,
            len(engine.buffer),
            state.last_depth_event_ms,
            state.last_trade_event_ms,
            state.epoch_id,
        )

    def warn_not_synced():
        if engine.depth_synced:
            return

        if len(engine.buffer) > MAX_BUFFER_WARN:
            log.warning("Depth buffer large: %d events (not synced). lastUpdateId=%s",
                        len(engine.buffer), engine.lob.last_update_id)

        now_s = time.time()
        # Hard stop at end of recording window.
        # This check is in heartbeat so we stop even if depth messages stop.
        if (not state.window_end_emitted) and window_now() >= end:
            state.window_end_emitted = True
            emit_event("window_end", {"end": end.isoformat()})
            try:
                stream.close()
            except Exception:
                log.exception("Failed to close stream on window end (heartbeat)")
            return
        if (now_s - state.sync_t0) > SYNC_WARN_AFTER_SEC and (now_s - state.last_sync_warn) > SYNC_WARN_AFTER_SEC:
            state.last_sync_warn = now_s
            log.warning("Still not synced after %.0fs (buffer=%d)", now_s - state.sync_t0, len(engine.buffer))

    def fetch_snapshot(tag: str):
        client = None

        eid = emit_event("snapshot_request", {"tag": tag, "limit": SNAPSHOT_LIMIT})
        lob, path, last_uid, raw_snapshot = record_rest_snapshot(
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
        raw_path = snapshots_dir / f"snapshot_{eid:06d}_{tag}.json"
        write_snapshot_json(path=raw_path, payload=raw_snapshot)

        engine.adopt_snapshot(lob)
        state.sync_t0 = time.time()
        state.last_sync_warn = time.time()

        emit_event(
            "snapshot_loaded",
            {"tag": tag, "lastUpdateId": last_uid, "path": str(path), "raw_path": str(raw_path)},
        )
        log.info("Snapshot %s loaded lastUpdateId=%s (%s)", tag, last_uid, path)

    def resync(reason: str):
        state.resync_count += 1
        state.epoch_id += 1
        tag = f"resync_{state.resync_count:06d}"

        set_phase(RecorderPhase.RESYNCING, reason)
        log.warning("Resync triggered: %s", reason)
        write_gap("resync_start", reason)
        emit_event("resync_start", {"reason": reason, "tag": tag})

        if "checksum_mismatch" in reason and hasattr(engine, "last_checksum_payload"):
            payload = getattr(engine, "last_checksum_payload", None)
            if payload:
                debug_dir = day_dir / "debug"
                debug_dir.mkdir(parents=True, exist_ok=True)
                path = debug_dir / f"checksum_payload_{tag}.txt"
                path.write_text(payload)
                emit_event("checksum_payload_saved", {"tag": tag, "path": str(path)})

        engine.reset_for_resync()

        if adapter.sync_mode == "checksum":
            state.needs_snapshot = True
            state.pending_snapshot_tag = tag
            try:
                reconnect = getattr(stream, "disconnect", None) or getattr(stream, "close", None)
                if reconnect is not None:
                    reconnect()
            except Exception:
                log.exception("Failed to close stream for checksum resync")
            return

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

    def handle_snapshot(snapshot: BookSnapshot, tag: str):
        set_phase(RecorderPhase.SYNCING, "snapshot_loaded")
        details = {"tag": tag, "lastUpdateId": 0}
        if snapshot.checksum is not None:
            details["checksum"] = int(snapshot.checksum)
        eid = emit_event("snapshot_loaded", details)
        path = snapshots_dir / f"snapshot_{eid:06d}_{tag}.csv"
        raw_path = snapshots_dir / f"snapshot_{eid:06d}_{tag}.json"
        write_snapshot_csv(
            path=path,
            run_id=run_id,
            event_id=eid,
            bids=snapshot.bids,
            asks=snapshot.asks,
            last_update_id=0,
            checksum=(int(snapshot.checksum) if snapshot.checksum is not None else None),
            decimals=DECIMALS,
        )
        if snapshot.raw is not None:
            write_snapshot_json(path=raw_path, payload=snapshot.raw)
            emit_event("snapshot_raw_saved", {"path": str(raw_path), "tag": tag})
        engine.adopt_snapshot(snapshot)
        state.sync_t0 = time.time()
        state.last_sync_warn = time.time()
        if tag != "initial":
            write_gap("resync_done", f"tag={tag} lastUpdateId=0")
            emit_event("resync_done", {"tag": tag, "lastUpdateId": 0})

    def write_topn(event_time_ms: int, recv_ms: int, recv_seq: int):
        bids, asks = engine.lob.top_n(DEPTH_LEVELS)
        bids += [(0.0, 0.0)] * (DEPTH_LEVELS - len(bids))
        asks += [(0.0, 0.0)] * (DEPTH_LEVELS - len(asks))

        row = [event_time_ms, recv_ms, recv_seq, run_id, state.epoch_id]
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
        state.ob_rows_written += 1

    def on_open():
        state.ws_open_count += 1
        set_phase(RecorderPhase.SNAPSHOT, "ws_open")

        # First open: initial snapshot. Any subsequent open is treated as a reconnect and triggers a resync.
        if state.ws_open_count == 1:
            state.epoch_id = 0
            emit_event(
                "ws_open",
                {
                    "ws_url": ws_url,
                    "ping_interval_s": WS_PING_INTERVAL_S,
                    "ping_timeout_s": WS_PING_TIMEOUT_S,
                    "insecure_tls": INSECURE_TLS,
                },
            )
            if adapter.sync_mode == "checksum":
                state.needs_snapshot = True
                state.pending_snapshot_tag = "initial"
            else:
                try:
                    fetch_snapshot("initial")
                except Exception as e:
                    log.exception("Failed initial snapshot; closing WS")
                    write_gap("fatal", f"initial_snapshot_failed: {e}")
                    emit_event("fatal", {"reason": "initial_snapshot_failed", "error": str(e)})
                    stream.close()
        else:
            emit_event("ws_reconnect_open", {"ws_url": ws_url, "open_count": state.ws_open_count})
            # Any reconnect implies potential missed diffs; always resync.
            if adapter.sync_mode == "checksum":
                if not state.needs_snapshot:
                    resync("ws_reconnect")
            else:
                resync("ws_reconnect")

    def handle_depth(parsed: DepthDiff, recv_ms: int):
        msg_recv_seq = next_recv_seq()

        state.depth_msg_count += 1
        state.last_depth_event_ms = int(parsed.event_time_ms)
        state.last_ws_msg_time = time.time()
        if not state.first_data_emitted:
            state.first_data_emitted = True
            emit_event("ws_first_data", {"type": "depth"})
            log.info("WS data flowing (first depth message).")

        # Always store raw diffs for replay, even when not synced
        if diff_writer is not None:
            try:
                minimal = {
                    "recv_ms": recv_ms,
                    "recv_seq": msg_recv_seq,
                    "E": int(parsed.event_time_ms),
                    "U": int(parsed.U),
                    "u": int(parsed.u),
                    "b": parsed.bids,
                    "a": parsed.asks,
                }
                if parsed.checksum is not None:
                    minimal["checksum"] = int(parsed.checksum)
                minimal["exchange"] = exchange
                minimal["symbol"] = symbol
                if parsed.raw is not None:
                    minimal["raw"] = parsed.raw
                diff_writer.write_line(json.dumps(minimal, ensure_ascii=False, default=str) + "\n")
            except Exception:
                log.exception("Failed writing depth diffs")
        if live_diff_writer is not None:
            try:
                minimal_live = {
                    "recv_ms": recv_ms,
                    "recv_seq": msg_recv_seq,
                    "E": int(parsed.event_time_ms),
                    "U": int(parsed.U),
                    "u": int(parsed.u),
                    "b": parsed.bids,
                    "a": parsed.asks,
                    "exchange": exchange,
                    "symbol": symbol,
                }
                if parsed.checksum is not None:
                    minimal_live["checksum"] = int(parsed.checksum)
                if parsed.raw is not None:
                    minimal_live["raw"] = parsed.raw
                live_diff_writer.write_line(json.dumps(minimal_live, ensure_ascii=False, default=str) + "\n")
            except Exception:
                log.exception("Failed writing live depth diffs")

        # Stop at end of window.
        #
        # This must remain enabled in production. If disabled, the recorder will
        # continue running and will keep writing into the *startup* day directory,
        # effectively mixing multiple trading days into the same folder.
        if (not state.window_end_emitted) and window_now() >= end:
            state.window_end_emitted = True
            emit_event("window_end", {"end": end.isoformat()})
            try:
                stream.close()
            except Exception:
                log.exception("Failed to close stream on window end")
            return

        try:
            if adapter.sync_mode == "checksum":
                result = engine.feed_depth_event(parsed)
            else:
                result = engine.feed_depth_event(
                    {"E": parsed.event_time_ms, "U": parsed.U, "u": parsed.u, "b": parsed.bids, "a": parsed.asks}
                )

            if result.action == "gap":
                resync(result.details)
                heartbeat()
                return

            if result.action in ("synced", "applied") and engine.depth_synced:
                set_phase(RecorderPhase.SYNCED, "depth_synced")
                write_topn(event_time_ms=int(parsed.event_time_ms), recv_ms=recv_ms, recv_seq=msg_recv_seq)

            if result.action == "buffered":
                warn_not_synced()

        except Exception:
            log.exception("Unhandled exception in on_depth")
            resync("exception_in_on_depth")

        finally:
            heartbeat()

    def handle_trade(parsed: Trade, recv_ms: int):

        state.trade_msg_count += 1
        state.last_trade_event_ms = int(parsed.event_time_ms)
        state.last_ws_msg_time = time.time()
        if not state.first_data_emitted:
            state.first_data_emitted = True
            emit_event("ws_first_data", {"type": "trade"})
            log.info("WS data flowing (first trade message).")

        msg_recv_seq = next_recv_seq()

        try:
            side = parsed.side
            if side is None:
                side = "sell" if int(parsed.is_buyer_maker) == 1 else "buy"
            tr_writer.write_row(
                [
                    int(parsed.event_time_ms),
                    recv_ms,
                    msg_recv_seq,
                    run_id,
                    int(parsed.trade_id),
                    int(parsed.trade_time_ms),
                    f"{float(parsed.price):.{DECIMALS}f}",
                    f"{float(parsed.qty):.{DECIMALS}f}",
                    int(parsed.is_buyer_maker),
                    side or "",
                    parsed.ord_type or "",
                    exchange,
                    symbol,
                ]
            )
            state.tr_rows_written += 1
            raw_payload = None
            if parsed.raw is not None:
                raw_payload = {
                    "recv_ms": recv_ms,
                    "recv_seq": msg_recv_seq,
                    "event_time_ms": int(parsed.event_time_ms),
                    "trade_id": int(parsed.trade_id),
                    "price": parsed.price,
                    "qty": parsed.qty,
                    "side": side,
                    "exchange": exchange,
                    "symbol": symbol,
                    "raw": parsed.raw,
                }
                tr_raw_writer.write_line(json.dumps(raw_payload, ensure_ascii=False, default=str) + "\n")
            if live_trade_writer is not None:
                try:
                    live_payload = raw_payload if raw_payload is not None else {
                        "recv_ms": recv_ms,
                        "recv_seq": msg_recv_seq,
                        "event_time_ms": int(parsed.event_time_ms),
                        "trade_id": int(parsed.trade_id),
                        "price": parsed.price,
                        "qty": parsed.qty,
                        "side": side,
                        "exchange": exchange,
                        "symbol": symbol,
                    }
                    live_trade_writer.write_line(json.dumps(live_payload, ensure_ascii=False, default=str) + "\n")
                except Exception:
                    log.exception("Failed writing live trades")
        except Exception:
            log.exception(
                "Unhandled exception in on_trade trade_id=%s event_time_ms=%s recv_ms=%s",
                getattr(parsed, "trade_id", None),
                getattr(parsed, "event_time_ms", None),
                recv_ms,
            )
        finally:
            heartbeat()

    def on_depth(data, recv_ms: int):
        try:
            parsed = adapter.parse_depth(data)
        except Exception:
            log.exception("Failed to parse depth message")
            return
        handle_depth(parsed, recv_ms)

    def on_trade(data: dict, recv_ms: int):
        try:
            parsed = adapter.parse_trade(data)
        except Exception:
            log.exception("Failed to parse trade message")
            return
        handle_trade(parsed, recv_ms)

    def on_message(data: dict, recv_ms: int):
        state.last_ws_msg_time = time.time()
        if not state.first_data_emitted:
            state.first_data_emitted = True
            emit_event("ws_first_data", {"type": "custom"})
            log.info("WS data flowing (first custom message).")
        if isinstance(data, dict) and data.get("method") == "subscribe":
            emit_event(
                "ws_subscribe_ack",
                {
                    "success": data.get("success"),
                    "result": data.get("result"),
                    "error": data.get("error"),
                },
            )
            if data.get("error"):
                log.warning("WS subscribe error: %s", data.get("error"))
        elif isinstance(data, dict) and data.get("event") == "error":
            emit_event("ws_error_payload", {"error": data.get("msg") or data})
            log.warning("WS error payload: %s", data.get("msg") or data)
        elif isinstance(data, dict) and data.get("event") == "info":
            code = data.get("code")
            emit_event("ws_info", {"code": code, "msg": data.get("msg")})
            if code == 20051:
                emit_event("ws_info_reconnect", {"code": code, "msg": data.get("msg")})
                try:
                    if stream:
                        stream.disconnect()
                except Exception:
                    log.exception("Failed to disconnect after ws_info reconnect")
        elif isinstance(data, dict) and data.get("error"):
            emit_event("ws_error_payload", {"error": data.get("error")})
            log.warning("WS error payload: %s", data.get("error"))
        try:
            snapshots, diffs, trades = adapter.parse_ws_message(data)
        except Exception:
            log.exception("Failed to parse WS message")
            return
        for snap in snapshots:
            if state.needs_snapshot:
                tag = state.pending_snapshot_tag or "snapshot"
                handle_snapshot(snap, tag)
                state.needs_snapshot = False
                state.pending_snapshot_tag = None
        for diff in diffs:
            handle_depth(diff, recv_ms)
        for tr in trades:
            handle_trade(tr, recv_ms)

    ws_url = adapter.ws_url(symbol)

    def on_status(typ: str, details: dict):
        # Keep the on-disk events ledger authoritative for operational debugging.
        emit_event(typ, details)
        log.info("WS status: %s %s", typ, details)
        if typ == "ws_connecting":
            set_phase(RecorderPhase.CONNECTING, "ws_connecting")

    # Backwards-compatible construction: tests may monkeypatch BinanceWSStream with a
    # simplified fake that does not accept newer parameters.
    try:
        stream = BinanceWSStream(
            ws_url=ws_url,
            on_depth=on_depth,
            on_trade=on_trade,
            on_open=on_open,
            on_status=on_status,
            on_message=(on_message if adapter.uses_custom_ws_messages else None),
            insecure_tls=INSECURE_TLS,
            ping_interval_s=WS_PING_INTERVAL_S,
            ping_timeout_s=WS_PING_TIMEOUT_S,
            reconnect_backoff_s=WS_RECONNECT_BACKOFF_S,
            reconnect_backoff_max_s=WS_RECONNECT_BACKOFF_MAX_S,
            max_session_s=WS_MAX_SESSION_S,
            open_timeout_s=WS_OPEN_TIMEOUT_S,
            subscribe_messages=adapter.subscribe_messages(symbol, sub_depth),
        )
    except TypeError:
        stream = BinanceWSStream(
            ws_url=ws_url,
            on_depth=on_depth,
            on_trade=on_trade,
            on_open=on_open,
            insecure_tls=INSECURE_TLS,
        )

    emit_event("run_start", {"symbol": symbol, "symbol_fs": symbol_fs, "day": day_str})
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

        set_phase(RecorderPhase.STOPPED, "run_stop")
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

        try:
            tr_raw_writer.close()
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
