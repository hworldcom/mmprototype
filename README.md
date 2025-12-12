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

## How to Run the Data Recorder

### Local Python

```bash
export CONFIG_PATH=config/config.binance.test.yaml
python -m mm.market_data.recorder
```

### Docker (Recommended)

```bash
docker build -t avellaneda-mm .

docker run --rm \
  -e CONFIG_PATH=config/config.binance.test.yaml \
  -v "$PWD/data":/app/data \
  avellaneda-mm:latest \
  python -m mm.market_data.recorder
```

---

## Data Collector Design Rules

- REST snapshot is the initial truth
- WebSocket diffs are applied sequentially
- Quantity = 0 removes a price level
- Sequence gaps trigger resync
- Exchange timestamps are authoritative

---

## Status

This project is **research‑first**.
No capital is deployed until:
- data is validated
- backtests are convincing
- risk is understood

---

## References

- Avellaneda & Stoikov (2008)
- Binance Spot API Docs
- Cartea, Jaimungal & Penalva – Algorithmic and High‑Frequency Trading
