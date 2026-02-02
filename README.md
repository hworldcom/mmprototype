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
| `mm_core/checksum_engine.py` | Checksum-based sync engine for exchanges like Kraken (verifies CRC checksums instead of sequence ids). |
| `mm_core/local_orderbook.py` | Lightweight in-memory book keyed by price, used by the recorder and sync engine. |
| `mm_recorder/buffered_writer.py` | Buffered CSV writer that batches rows in memory to reduce fsync pressure. |
| `mm_recorder/ws_stream.py` | Async websocket client with reconnect, ping/pong, and backoff. |
| `mm_recorder/snapshot.py` | Snapshot helper that serializes CSV + raw JSON snapshots for audit and resyncs. |

The repository includes tests that validate epochs, header handling, and recorder output contracts. Those tests run offline thanks to the client-creation guard in `recorder.py`.

## Output contract

Each recorder run (one symbol per process) produces the following files under `data/<EXCHANGE>/<SYMBOL_FS>/<YYYYMMDD>/`:

- `orderbook_ws_depth_<SYMBOL_FS>_<YYYYMMDD>.csv.gz` — top-N book frames whenever the local book is synced.
- `trades_ws_<SYMBOL_FS>_<YYYYMMDD>.csv.gz` — individual trade prints with event/receive timestamps and trade identifiers.
- `events_<SYMBOL_FS>_<YYYYMMDD>.csv.gz` — authoritative ledger covering WS lifecycle, snapshot tags, resync epochs, and run boundaries.
- `gaps_<SYMBOL_FS>_<YYYYMMDD>.csv.gz` — optional audit of detected sequencing issues.
- `snapshots/snapshot_<event_id>_<tag>.csv` — REST snapshots referenced by the events ledger.
- `snapshots/snapshot_<event_id>_<tag>.json` — raw snapshot payload (REST for Binance, WS for checksum exchanges).
- `diffs/depth_diffs_<SYMBOL_FS>_<YYYYMMDD>.ndjson.gz` — optional compressed raw WS diffs for exact replays (checksum exchanges include a `checksum` field per diff).
- `trades/trades_ws_raw_<SYMBOL_FS>_<YYYYMMDD>.ndjson.gz` — raw trade payloads with recv sequence metadata.

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
   Optional exchange (defaults to Binance):
   ```bash
   export EXCHANGE=binance  # or kraken
   ```
3. Launch the recorder:
   ```bash
   python -m mm_recorder.recorder
   ```

### Local run notes

- Logs are written to `logs/recorder/<EXCHANGE>/<SYMBOL_FS>/<YYYY-MM-DD>.log` where `SYMBOL_FS` strips `/ - :` and spaces.
- If your local TLS inspection blocks the websocket handshake, set `INSECURE_TLS=1`
  (only for local debugging).
- Kraken adapter uses WebSocket v2 `book` channel with checksum-based sync and subscribes to `trade` for fills.

## Exchange data formats

### Binance (spot)
- **Order book diffs**: `diffs/depth_diffs_*.ndjson.gz` uses Binance `depthUpdate` fields (`E`, `U`, `u`, `b`, `a`) and includes `raw` for the full payload.
- **Trades**: `trades_ws_*.csv.gz` captures standard Binance trade fields; raw payloads are stored in `trades/trades_ws_raw_*.ndjson.gz`.
- **Snapshots**: REST snapshot saved to CSV plus raw JSON (`snapshot_*.json`).

### Kraken (spot, WS v2)
- **Order book diffs**: checksum-driven; `U/u` are `0` because Kraken doesn’t provide them. `checksum`, `exchange`, `symbol`, and `raw` are persisted in the diff NDJSON for replay verification.
- **Trades**: `trade` channel parsed into the shared trade schema; raw payloads stored in `trades/trades_ws_raw_*.ndjson.gz`.
- **Snapshots**: WS snapshot saved to CSV plus raw JSON (`snapshot_*.json`), with checksum stored in both the snapshot CSV and events ledger.

Note: raw JSON payloads may contain Decimal values serialized as strings to preserve precision for checksum verification.

## Replay notes

To rebuild the order book for a day:

1. Load the first snapshot CSV (or JSON) for the session.
2. Apply each diff in `diffs/depth_diffs_*.ndjson.gz` in order of `recv_seq`.
3. For Binance, validate sequential `U/u` ranges. For Kraken, validate the per-diff `checksum` after applying each update.
4. If a gap is detected, jump to the next snapshot tagged in `events_*.csv.gz` (look for `resync_start`/`resync_done`).

Replay should ignore diffs received before the initial snapshot is loaded. The `events_*.csv.gz` ledger provides the authoritative timeline (window boundaries, reconnects, resync tags).

### Replay validator

An offline validator is included to replay recorded diffs against snapshots and confirm reconciliation:

```bash
python -m mm_recorder.replay_validator --day-dir data/<EXCHANGE>/<SYMBOL_FS>/<YYYYMMDD>
```

It returns exit code `0` if no gaps are detected, and `1` if any gap/checksum mismatch is found.

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
| `EXCHANGE` (env) | Exchange adapter to use (default: `binance`). Supported: `binance`, `kraken`. |
| `SYMBOL` (env) | Trading pair to subscribe (e.g., `BTCUSDT`). Required. |
| `DEPTH_LEVELS` | Number of L2 levels persisted per book snapshot row. |
| `STORE_DEPTH_DIFFS` | Toggle gzip’d NDJSON logging of raw WS depth diffs for replay. |
| `WS_PING_INTERVAL_S`, `WS_PING_TIMEOUT_S` | Client ping cadence and pong timeout (seconds). |
| `WS_RECONNECT_BACKOFF_S`, `WS_RECONNECT_BACKOFF_MAX_S` | Reconnect backoff base and cap (seconds). |
| `WS_MAX_SESSION_S` | Max WS session duration before forced reconnect (seconds). |
| `WS_OPEN_TIMEOUT_S` | WebSocket handshake/open timeout (seconds). |
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
