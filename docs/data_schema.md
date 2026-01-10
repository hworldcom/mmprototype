# Market Data Schema & Versioning

## Purpose
This repository records market data (depth diffs, trades, and internal events)
for deterministic replay and backtesting. File formats evolve over time. To keep
replay deterministic and format evolution controlled, each day directory SHOULD
contain an explicit `schema.json` file describing the expected columns/fields.

## Where `schema.json` lives
For a given symbol and trading day:

- `data/<SYMBOL>/<YYYYMMDD>/schema.json`

The recorder overwrites this file on startup so it reflects the writer that
produced the data.

## Current schema version
**Schema version:** `2`

Version `2` introduces a global, monotonic `recv_seq` that increments for every
message regardless of type. Together, `(recv_ms, recv_seq)` defines a total
order across streams.

## Common concepts

### Time fields
- `event_time_ms`: exchange-provided timestamp (when available)
- `recv_time_ms` / `recv_ms`: when the client received the message

### Global ordering field
- `recv_seq`: a global, strictly increasing receive sequence for the recorder
  process. It is incremented for every message (depth diff, trade, and recorder
  events). In replay, it is used as the tiebreaker when `recv_ms` ties occur.

## File formats

### Depth diffs (`diffs/depth_diffs_*.ndjson.gz`)
One JSON object per websocket depth-diff message (gzip-compressed).

Required fields:
- `recv_ms` (int)
- `recv_seq` (int)
- `E` (event time, int)
- `U` (first update id, int)
- `u` (final update id, int)
- `b` (bids, list)
- `a` (asks, list)

### Trades (`trades_ws_*.csv`)
CSV. Required columns:
- `event_time_ms`
- `recv_time_ms`
- `recv_seq`
- `run_id`
- `trade_id`
- `trade_time_ms`
- `price`
- `qty`
- `is_buyer_maker`

### Events (`events_*.csv`)
CSV. Used to record internal recorder events (e.g., snapshot loaded) so replay
can be aligned to the same timeline.

Required columns:
- `event_id`
- `recv_time_ms`
- `recv_seq`
- `run_id`
- `type`
- `epoch_id`
- `details_json`

### Gaps (`gaps_*.csv`)
CSV. Used for diagnostics.

Required columns:
- `recv_time_ms`
- `recv_seq`
- `run_id`
- `epoch_id`
- `event`
- `details`

## Versioning policy
- Additive, backward-compatible changes increment the schema version when they
  affect deterministic replay or downstream parsing.
- Breaking changes (renamed/removed columns, semantic changes) MUST increment
  the schema version and update replay/parsers accordingly.
- Replay tools SHOULD enforce that a single day directory is internally
  consistent (no mixing of schema versions across files).
