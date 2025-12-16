# Avellaneda–Stoikov Market Making Research Project

## Objective

The goal of this project is to **research, test, and validate market‑making models**, with a primary focus on the **Avellaneda–Stoikov framework**, before any production deployment.

This repository is intentionally structured as a **research pipeline**, not a ready‑made trading bot.

---

## Why This Project Exists

Market making models depend critically on:
- execution uncertainty
- inventory risk
- order arrival dynamics
- market microstructure

Without realistic data and fill assumptions, theoretical results are misleading.
This project exists to close that gap.

---

## What Is Required to Test Market‑Making Models

To correctly test an Avellaneda–Stoikov‑style strategy we need:

- L2 order book states (bids & asks)
- Executed trade flow
- Exchange timestamps
- Robust fill models
- Deterministic backtesting

---

## Current State (Implemented)

### Market Data Collection

- WebSocket‑based data ingestion
- Binance Spot exchange
- Order book diff stream (`@depth@100ms`)
- Trade stream (`@trade`, upgradeable to `@aggTrade`)
- Local order book reconstruction using:
  - REST snapshot
  - WebSocket diffs
  - update‑ID sequencing
- Data persisted to CSV
- Berlin trading window: **08:00–22:00 Europe/Berlin**
- Dockerized & cron‑friendly

---

## Recorded Data Files

Per symbol, per day:

- `orderbook_rest_snapshot_<SYMBOL>_<YYYYMMDD>.csv`
- `orderbook_ws_depth_<SYMBOL>_<YYYYMMDD>.csv`
- `trades_ws_<SYMBOL>_<YYYYMMDD>.csv`

All numeric values are stored in **human‑readable fixed decimals**.

---

## Project Roadmap

### Phase 1 — Market Data
- [x] REST order book snapshot
- [x] WebSocket depth stream
- [x] Local order book reconstruction
- [x] Trade stream
- [ ] Switch to aggTrades
- [ ] Latency metrics
- [ ] Data validation

### Phase 2 — Backtesting Engine
- [ ] Order book replay
- [ ] Trade replay
- [ ] Time‑aligned simulation

### Phase 3 — Fill Models
- [ ] Poisson (Avellaneda–Stoikov)
- [ ] Price‑cross
- [ ] Hybrid
- [ ] Trade‑driven

### Phase 4 — Strategy Research
- [ ] Quoting logic
- [ ] Parameter calibration
- [ ] Inventory control
- [ ] PnL attribution

### Phase 5 — Live / Testnet
- [ ] Testnet trading
- [ ] Shadow trading
- [ ] Monitoring
- [ ] Gradual production rollout

---

# Binance Market Data Recorder (Depth + Trades + Events Ledger)

One process per symbol. Produces three primary files per day:

- `orderbook.csv`: top-10 bids/asks frames (only when book is synced)
- `trades.csv`: trade prints
- `events.csv`: authoritative ledger for run boundaries and sync/resync epochs
- `snapshots/`: optional REST snapshots referenced by `events.csv`

## Layout

```
data/<SYMBOL>/<YYYYMMDD>/
  orderbook.csv
  trades.csv
  events.csv
  snapshots/
    snapshot_<event_id>_<tag>.csv
```

## Run

```bash
pip install -r requirements.txt
SYMBOL=ETHUSDT python -m mm.market_data.recorder
```

Docker:

```bash
docker build -t mm-recorder:latest .
docker run --rm -e SYMBOL=ETHUSDT -v "$PWD/data":/app/data mm-recorder:latest
```

## Backtesting inputs

Load:
- `orderbook.csv` (filter `epoch_id >= 1`)
- `trades.csv` (align by `event_time_ms`)
- `events.csv` (optional: segment by `epoch_id`, diagnose gaps, run boundaries)
