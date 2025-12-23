# Backtesting & Replay Documentation

## Objective

The goal of the **replay subsystem** is to reconstruct, as faithfully as possible, the same
market state that your production market maker would have seen, using the data recorded
by the live recorder (`mm/market_data`). The backtest therefore depends directly on
the artifacts described in `mm/market_data/README.md`.

This allows you to:

- deterministically replay market conditions
- test quoting logic and inventory control
- compare fill models (Poisson / price-driven / hybrid)
- debug sync issues and resync behavior
- run paper trading without exchange connectivity

The replay pipeline mirrors the **production data flow**, not a simplified snapshot-based backtest.

---

## Folder Structure

```
mm/backtest/
├── README.md                # This document
├── replay.py                # Day-level orchestrator & stats
├── io.py                    # File discovery + CSV/NDJSON iterators
├── paper_exchange.py        # Paper fills & balance tracking
├── fills/
│   ├── __init__.py
│   ├── base.py
│   ├── poisson.py
│   ├── trade_driven.py
│   └── hybrid.py
├── quotes/
│   ├── __init__.py
│   ├── base.py
│   ├── avellaneda_stoikov.py
│   ├── hybrid.py
│   ├── inventory_skew.py
│   └── microstructure.py
├── __init__.py
└── __pycache__/             # Generated
```

`replay.py` consumes the recorder outputs and feeds them through the same `OrderBookSyncEngine`
used in production. `io.py` is the shared bridge to disk, keeping the producer→consumer contract
in one place. The `fills/` and `quotes/` subpackages stay intentionally lightweight so their
components can be reused in notebooks or additional runners.

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

### Dependency on `mm/market_data`

- **Producer → Consumer contract:** folders and filenames must match the recorder output exactly (`data/<SYMBOL>/<YYYYMMDD>/...`). Keep both modules on the same commit so schema changes are synchronized.
- **Events ledger:** the replay bootstrapper reads `events_*.csv` to determine which snapshot tags to load and how to segment epochs.
- **Gaps file:** optional but useful when diagnosing why replay could not bridge a day.
- **Top-N CSVs:** not used for core replay but helpful for quick visualizations; they share the same buffering logic as production.

If you add new recorder outputs (e.g., quote intentions or latency logs), update both READMEs so everyone understands how the data flows between modules.

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
