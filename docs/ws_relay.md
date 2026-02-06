# WebSocket Relay (Prototype)

This document describes the WebSocket relay that streams live recorder
outputs to a frontend. It is a lightweight, read-only service that tails recorder
files and pushes updates to clients.

## Goals
- Live updates for UI (order book diffs, trades, events).
- No changes required to the recorder.
- Minimal infrastructure for the prototype phase.

## Server Structure (Sketch)

```
mm_api/
  __init__.py
  relay.py           # WS server entrypoint
  tailer.py          # file tailing helpers
  sources.py         # locate latest files per exchange/symbol/day
  protocols.py       # message format helpers
```

### Responsibilities

- **relay.py**
  - Accept WS connections.
  - Parse query params: `exchange`, `symbol`.
  - Start tailing the latest diff/trade/event files for that pair.
  - Push messages to client in a consistent format.

- **tailer.py**
  - Tail gzip NDJSON and gzip CSV files (append-only).
  - Resume from last byte offset or from tail.

- **sources.py**
  - Resolve `data/<exchange>/<symbol_fs>/<YYYYMMDD>/...` paths.
  - Pick the latest day folder by date.

- **protocols.py**
  - Normalize records into the WS message format below.

## Connection

### URL
```
ws://<host>:<port>/ws?exchange=binance&symbol=BTCUSDT
```

### Start the relay
```
python -m mm_api.relay
```

### Start the minimal REST endpoint (latest snapshot)
```
python -m mm_api.rest
```

Example:
```
http://localhost:8080/snapshot?exchange=binance&symbol=BTCUSDT
```

## Metrics WebSocket (server-computed)

Start the metrics server:
```
python -m mm_api.metrics
```

Connect:
```
ws://localhost:8766/metrics?exchange=binance&symbols=BTCUSDC,ETHUSDC&interval=1m&window=180d&metric=correlation
```

Supported metrics:
- `correlation` (two symbols)
- `volatility` (one symbol)

The server computes metrics on 1m candles and updates **every second** using partial candles built from live trades.

### Query parameters
- `exchange` (required) — `binance`, `kraken`, `bitfinex`
- `symbol` (required) — trading pair in exchange format (e.g., `BTCUSDT`, `BTC/USD`, `tBTCUSD`)
- `from` (optional) — `tail` (default) or `start` to replay from file start

### Environment flags
- `WS_RELAY_LIVE_ONLY=1` to skip gzip events/fallbacks and only tail `live/` files.

## Message Format

All messages are JSON objects with:

```
{
  "type": "snapshot|diff|trade|event|status",
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "ts_ms": 1700000000000,
  "data": { ... }
}
```

### `snapshot`
Sent once on connect (optional), using the latest snapshot JSON:

```
{
  "type": "snapshot",
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "ts_ms": 1700000000000,
  "data": {
    "bids": [["50000.0","0.5"], ...],
    "asks": [["50010.0","0.7"], ...],
    "raw": { ... }
  }
}
```

### `diff`
Forwarded from `diffs/depth_diffs_*.ndjson.gz`:

```
{
  "type": "diff",
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "ts_ms": 1700000000123,
  "data": {
    "E": 1700000000123,
    "U": 123,
    "u": 456,
    "b": [["49999.0","0.1"]],
    "a": [["50001.0","0.2"]],
    "checksum": 12345678
  }
}
```

### `trade`
Forwarded from `trades/trades_ws_raw_*.ndjson.gz` (raw) or
`trades_ws_*.csv.gz` (normalized):

```
{
  "type": "trade",
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "ts_ms": 1700000000456,
  "data": {
    "price": "50000.0",
    "qty": "0.01",
    "side": "buy",
    "trade_id": "123456"
  }
}
```

### `event`
Forwarded from `events_*.csv.gz`:

```
{
  "type": "event",
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "ts_ms": 1700000000789,
  "data": {
    "event": "state_change",
    "phase": "SYNCED"
  }
}
```

### `status`
Server-side status updates (optional):

```
{
  "type": "status",
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "ts_ms": 1700000000999,
  "data": {
    "message": "tailing latest diff/trade/event files"
  }
}
```

## Notes
- The relay is read-only; it does **not** modify recorder outputs.
- File tailing is append-only; the relay should reopen the newest day folder if the date rolls over.
- Current prototype re-reads gzip files and tracks line counts; this is simple but not efficient for very large files.
  TODO: replace with incremental gzip tailing / stream offsets.
- If `live/` files exist (e.g. `live_depth_diffs.ndjson`), the relay prefers them over `.ndjson.gz`.
- If a file is missing (e.g., `diffs` disabled), the relay sends `status` and continues with available streams.
