# Market Data Module

This package houses every component required to capture Binance Spot depth and trade data, reconstruct level-2 books, and persist replay-ready artifacts for downstream research.

It includes the `mm_core` package for the shared order book and sync state machine.
The recorder avoids `python-binance` and calls the public REST depth endpoint directly
for snapshots; if you later need signed endpoints or user streams, you can reintroduce it.

## Architecture

| File | Responsibility |
|------|----------------|
| `mm_recorder/recorder.py` | End-to-end orchestration: enforces the Berlin trading window, wires callbacks, persists CSV/NDJSON outputs, and emits telemetry/events. |
| `mm_core/sync_engine.py` | Pure state machine that bridges REST snapshots with WebSocket depth diffs and detects any sequencing gap. |
| `mm_core/local_orderbook.py` | Lightweight in-memory book keyed by price, used by the recorder and sync engine. |
| `mm_recorder/buffered_writer.py` | Buffered CSV writer that batches rows in memory to reduce fsync pressure. |
| `mm_recorder/ws_stream.py` | Async websocket client with reconnect, ping/pong, and backoff. |
| `mm_recorder/snapshot.py` | REST snapshot helper that serializes snapshots to disk for audit and resyncs. |

The repository includes tests that validate epochs, header handling, and recorder output contracts. Those tests run offline thanks to the client-creation guard in `recorder.py`.

## Output contract

Each recorder run (one symbol per process) produces the following files under `data/<SYMBOL>/<YYYYMMDD>/`:

- `orderbook_ws_depth_<SYMBOL>_<YYYYMMDD>.csv.gz` — top-N book frames whenever the local book is synced.
- `trades_ws_<SYMBOL>_<YYYYMMDD>.csv.gz` — individual trade prints with event/receive timestamps and trade identifiers.
- `events_<SYMBOL>_<YYYYMMDD>.csv.gz` — authoritative ledger covering WS lifecycle, snapshot tags, resync epochs, and run boundaries.
- `gaps_<SYMBOL>_<YYYYMMDD>.csv.gz` — optional audit of detected sequencing issues.
- `snapshots/snapshot_<event_id>_<tag>.csv` — REST snapshots referenced by the events ledger.
- `diffs/depth_diffs_<SYMBOL>_<YYYYMMDD>.ndjson.gz` — optional compressed raw WS diffs for exact replays.

Uncompressed outputs are intentionally not supported. Avoid renaming columns or folders unless you also update downstream consumers.

## Running the recorder

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Export the target symbol (case-insensitive):
   ```bash
   export SYMBOL=ETHUSDT
   ```
3. Launch the recorder:
   ```bash
   python -m mm_recorder.recorder
   ```

### Local run notes

- Logs are written to `logs/recorder/<SYMBOL>/<YYYY-MM-DD>.log`.
- If your local TLS inspection blocks the websocket handshake, set `INSECURE_TLS=1`
  (only for local debugging).

## Tests

```bash
python3 -m pytest -q
```

### Docker

```
docker build -t mm-recorder .
docker run --rm \
  -e SYMBOL=ETHUSDT \
  -v "$PWD/data":/app/data \
  mm-recorder
```

If you vendor this repo into another build context, ensure `mm_core` is present alongside `mm_recorder`.

### Configuration knobs

| Variable/Const | Meaning |
|----------------|---------|
| `SYMBOL` (env) | Trading pair to subscribe (e.g., `BTCUSDT`). Required. |
| `DEPTH_LEVELS` | Number of L2 levels persisted per book snapshot row. |
| `STORE_DEPTH_DIFFS` | Toggle gzip’d NDJSON logging of raw WS depth diffs for replay. |
| `WS_PING_INTERVAL_S`, `WS_PING_TIMEOUT_S` | Client ping cadence and pong timeout (seconds). |
| `WS_RECONNECT_BACKOFF_S`, `WS_RECONNECT_BACKOFF_MAX_S` | Reconnect backoff base and cap (seconds). |
| `WS_MAX_SESSION_S` | Max WS session duration before forced reconnect (seconds). |
| `WINDOW_TZ` (env) | Timezone used for start/end windows (default: `Europe/Berlin`). |
| `WINDOW_START_HHMM` (env) | Window start time in 24h `HH:MM` (default: `00:00`). |
| `WINDOW_END_HHMM` (env) | Window end time in 24h `HH:MM` (default: `00:15`). |
| `WINDOW_END_DAY_OFFSET` (env) | Day offset added to the end time (default: `1`). Use `1` for next-day cutoff. |
| `HEARTBEAT_SEC`, `SYNC_WARN_AFTER_SEC`, `MAX_BUFFER_WARN` | Telemetry cadence and warning thresholds. |
| `ORDERBOOK_BUFFER_ROWS`, `TRADES_BUFFER_ROWS`, `BUFFER_FLUSH_INTERVAL_SEC` | Tune throughput vs. fsync pressure. |

### Dependencies and testing notes

- `mm_recorder.logging_config` is used to configure per-run logging.
- `mm_core` supplies the shared order book and sync engine.
- Tests under `tests/` monkeypatch `record_rest_snapshot`; `recorder.py` only instantiates a real `binance.Client` when the original function is in use.
