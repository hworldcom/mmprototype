# Market Data Module

This package houses every component required to capture Binance Spot depth and trade data, reconstruct level-2 books, and persist replay-ready artifacts for downstream research.

## Architecture

| File | Responsibility |
|------|----------------|
| `recorder.py` | End-to-end orchestration: enforces the Berlin trading window, wires callbacks, persists CSV/NDJSON outputs, and emits telemetry/events. |
| `sync_engine.py` | Pure state machine that bridges REST snapshots with WebSocket depth diffs and detects any sequencing gap. |
| `local_orderbook.py` | Lightweight in-memory book keyed by price, used by the recorder and sync engine. |
| `buffered_writer.py` | Buffered CSV writer that batches rows in memory to reduce fsync pressure. |
| `ws_stream.py` | Thin wrapper around `websocket.WebSocketApp`, dispatching depth and trade messages to the recorder callbacks. |
| `snapshot.py` | REST snapshot helper that serializes snapshots to disk for audit and resyncs. |

The folder additionally contains tests that validate epochs, header handling, and recorder-to-backtest contracts. Those tests run offline thanks to the client-creation guard in `recorder.py`.

## Output contract (consumed by `mm/backtest`)

Each recorder run (one symbol per process) produces the following files under `data/<SYMBOL>/<YYYYMMDD>/`:

- `orderbook_ws_depth_<SYMBOL>_<YYYYMMDD>.csv.gz` — top-N book frames whenever the local book is synced.
- `trades_ws_<SYMBOL>_<YYYYMMDD>.csv.gz` — individual trade prints with event/receive timestamps and trade identifiers.
- `events_<SYMBOL>_<YYYYMMDD>.csv.gz` — authoritative ledger covering WS lifecycle, snapshot tags, resync epochs, and run boundaries.
- `gaps_<SYMBOL>_<YYYYMMDD>.csv.gz` — optional audit of detected sequencing issues.
- `snapshots/snapshot_<event_id>_<tag>.csv` — REST snapshots referenced by the events ledger.
- `diffs/depth_diffs_<SYMBOL>_<YYYYMMDD>.ndjson.gz` — optional compressed raw WS diffs for exact replays.

Uncompressed outputs are intentionally not supported in the loader path. If you have historical `.csv`/`.ndjson` artifacts, convert them with `./scripts/compress_existing_data.sh` before attempting replay/backtests.

The `mm/backtest` package expects this structure verbatim. Avoid renaming columns or folders unless you also update the replay loaders.

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
   python -m mm.market_data.recorder
   ```

### Docker

```
docker build -t mm-recorder .
docker run --rm \
  -e SYMBOL=ETHUSDT \
  -v "$PWD/data":/app/data \
  mm-recorder
```

### Configuration knobs

| Variable/Const | Meaning |
|----------------|---------|
| `SYMBOL` (env) | Trading pair to subscribe (e.g., `BTCUSDT`). Required. |
| `DEPTH_LEVELS` | Number of L2 levels persisted per book snapshot row. |
| `STORE_DEPTH_DIFFS` | Toggle gzip’d NDJSON logging of raw WS depth diffs for replay. |
| `WINDOW_TZ` (env) | Timezone used for start/end windows (default: `Europe/Berlin`). |
| `WINDOW_START_HHMM` (env) | Window start time in 24h `HH:MM` (default: `00:00`). |
| `WINDOW_END_HHMM` (env) | Window end time in 24h `HH:MM` (default: `00:15`). |
| `WINDOW_END_DAY_OFFSET` (env) | Day offset added to the end time (default: `1`). Use `1` for next-day cutoff. |
| `HEARTBEAT_SEC`, `SYNC_WARN_AFTER_SEC`, `MAX_BUFFER_WARN` | Telemetry cadence and warning thresholds. |
| `ORDERBOOK_BUFFER_ROWS`, `TRADES_BUFFER_ROWS`, `BUFFER_FLUSH_INTERVAL_SEC` | Tune throughput vs. fsync pressure. |

### Dependencies on other packages

- `mm.logging_config` is used to configure per-run logging.
- Tests under `tests/` monkeypatch `record_rest_snapshot`; `recorder.py` only instantiates a real `binance.Client` when the original function is in use.
- Backtests and analytics read the CSV/NDJSON outputs; treat the data layout as a stability contract.

## Feeding the backtest

Once a run finishes, copy the entire `data/<SYMBOL>/<YYYYMMDD>/` directory to any machine running `mm/backtest`. The replay modules will:

1. Load `orderbook_ws_depth...csv` for book states (filtering by `epoch_id`).
2. Align `trades_ws...csv` by `event_time_ms`.
3. Consult `events...csv` to segment epochs, detect resyncs, or align with snapshots.

Any new recorder feature (e.g., more depth levels or new files) should update this README and the backtest README so the producer/consumer contract remains clear.
