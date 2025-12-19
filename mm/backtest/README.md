# Backtesting & Replay Documentation

## Objective

The goal of the **replay subsystem** is to reconstruct, as faithfully as possible, the same
market state that your production market maker would have seen, using the data recorded
by the live recorder.

This allows you to:

- deterministically replay market conditions
- test quoting logic and inventory control
- compare fill models (Poisson / price-driven / hybrid)
- debug sync issues and resync behavior
- run paper trading without exchange connectivity

The replay pipeline mirrors the **production data flow**, not a simplified snapshot-based backtest.

---

## High-level Architecture

```
Binance (live)
   │
   ├─ depth diffs (WS)
   ├─ trades (WS)
   └─ snapshots (REST, on open + resync)
        │
        ▼
Recorder (production)
        │
        ├─ depth_diffs_*.ndjson.gz
        ├─ trades_ws_*.csv
        ├─ snapshots/*.csv
        ├─ gaps_*.csv
        └─ events_*.csv
        │
        ▼
Replay (backtest)
        │
        ├─ OrderBookSyncEngine
        └─ Strategy / Fill model
```

---

## Directory Layout

```
data/
└── BTCUSDT/
    └── 20251216/
        ├── orderbook_ws_depth_BTCUSDT_20251216.csv
        ├── trades_ws_BTCUSDT_20251216.csv
        ├── gaps_BTCUSDT_20251216.csv
        ├── events_BTCUSDT_20251216.csv
        ├── snapshots/
        │   ├── snapshot_000002_initial.csv
        │   └── snapshot_000123_resync_000001.csv
        └── diffs/
            └── depth_diffs_BTCUSDT_20251216.ndjson.gz
```

---

## File Roles

| File | Purpose |
|----|----|
| depth_diffs_*.ndjson.gz | Raw WS depth diff events |
| snapshots/*.csv | Authoritative REST snapshots |
| events_*.csv | Snapshot & resync timeline |
| gaps_*.csv | Explicit sync failures |
| orderbook_ws_depth_*.csv | Derived top-N (analysis only) |
| trades_ws_*.csv | Trade prints |

Replay uses **snapshots + diffs**, not the derived top-N CSV.

---

## Replay Mechanics

Replay merges three streams by recv_ms:

1. Depth diffs
2. Trades
3. Recorder events

All streams feed the same OrderBookSyncEngine used in production.

---

## Strategy Hooks

Replay exposes:

```python
on_tick(recv_ms, engine)
on_trade(trade, engine)
```

- on_tick only fires when the book is valid
- on_trade receives raw trades

---

## Running Replay

```bash
export SYMBOL=BTCUSDT
export DAY=20251216
python -m mm.runner_backtest_replay
```

---

## Guarantees

✔ Deterministic replay  
✔ Same sync/resync behavior as production  
✔ Faithful order book reconstruction  

---

## Next Steps

- PaperTrader
- Fill models A / B / C
- Parameter calibration
