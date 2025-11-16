# 🏦 Avellaneda–Stoikov Market Maker (Crypto CEX)

This repository implements a modular, production-oriented **Avellaneda–Stoikov market-making engine** suitable for a centralized crypto exchange (CEX).  
It includes simulation, risk management, and a clear separation between **strategy logic**, **market model**, and **execution layer**.

---

## 📂 Project Structure

```
avellaneda_mm/
├─ README.md
├─ requirements.txt
├─ config/
│  └─ config.example.yaml
└─ mm/
   ├─ __init__.py
   ├─ avellaneda_stoikov.py
   ├─ strategy.py
   ├─ exchange.py
   ├─ risk.py
   ├─ utils.py
   ├─ logging_config.py
   └─ runner.py
```

---

## ⚙️ Overview

The system is built around a **modular architecture**:

| Layer | Responsibility | Key File(s) |
|-------|----------------|-------------|
| **Model** | Implements Avellaneda–Stoikov math (reservation price, spread) | `avellaneda_stoikov.py` |
| **Strategy** | Combines model + config + exchange data → outputs bid/ask quotes | `strategy.py` |
| **Exchange Interface** | Abstract API + simulated exchange backend | `exchange.py` |
| **Risk Management** | Inventory, PnL, and kill-switch logic | `risk.py` |
| **Utils & Logging** | Helper functions, rounding, timestamps, logging setup | `utils.py`, `logging_config.py` |
| **Runner** | Event loop wiring everything together (simulation) | `runner.py` |
| **Config** | YAML configuration for model and risk limits | `config/config.example.yaml` |

---

## 📘 File-by-File Description

### `README.md`
You’re reading it!  
Explains project purpose, architecture, and usage.

---

### `requirements.txt`
Lists minimal Python dependencies:

- `numpy` — numerical operations  
- `pyyaml` — configuration parsing  

Extend this with real-exchange SDKs (Binance, OKX, etc.) or analytics libs as needed.

---

### `config/config.example.yaml`
Central configuration file defining:

- **Market microstructure:** tick size, min size, fees.  
- **Model parameters:** `gamma`, `A`, `k`, `T`, and `sigma_window`.  
- **Quoting logic:** base order size, spread limits, inventory skew factor.  
- **Inventory & risk:** caps, PnL and drawdown limits.  
- **Simulation setup:** midprice start, volatility, refresh rate.

Copy this as `config/config.yaml` for your environment.

---

### `mm/avellaneda_stoikov.py`
Mathematical core implementing the Avellaneda–Stoikov model.

**Methods:**
- `reservation_price(S, q, sigma, t)` → fair value adjusted for inventory  
- `half_spread(sigma, t)` → optimal half-spread for given volatility and horizon  
- `optimal_quotes(S, q, sigma, t)` → returns `(bid, ask, reservation, spread)`

Contains **no trading logic** — only model math.

---

### `mm/strategy.py`
Implements the **trading strategy** that wraps the model.

**Responsibilities:**
- Compute rolling volatility from mid-prices.  
- Use Avellaneda–Stoikov formulas to get theoretical quotes.  
- Apply tick rounding, inventory skew, and min-spread rules.  
- Interact with the exchange layer for placing/cancelling orders.  
- Manage open orders, position, and PnL.  

Includes:
- `StrategyConfig` dataclass — parameters loaded from YAML.  
- `AvellanedaStoikovStrategy` — the main live strategy engine.

---

### `mm/exchange.py`
Defines an **exchange abstraction layer** plus a **simulated backend**.

**Components:**
- `ExchangeAPI` — abstract interface (for real or simulated exchanges).  
- `SimulatedExchange` — random-walk mid-price with probabilistic fills, for offline testing.

Swap in a real implementation (e.g. Binance API) by subclassing `ExchangeAPI`.

---

### `mm/risk.py`
Contains **risk management** primitives.

**Classes:**
- `RiskState` — tracks inventory, realized/unrealized PnL, and peak equity.  
- `RiskLimits` — config for hard limits.  
- `RiskManager` — updates PnL and enforces inventory, loss, and drawdown limits.  

Used by the strategy to auto-cancel orders or halt trading when limits are breached.

---

### `mm/utils.py`
Helper functions:
- `now_ms()` → current timestamp in milliseconds.  
- `round_to_tick(price, tick_size)` → snap to valid price increments.  
- `clamp_qty(qty, step)` → ensure valid lot sizes.

---

### `mm/logging_config.py`
Sets up consistent logging across modules.

```python
setup_logging(level="INFO")
```

Used by `runner.py` to configure global logging format.

---

### `mm/runner.py`
Demo **event loop** that wires everything together for testing or simulation.

**Logic:**
1. Loads YAML config.  
2. Creates:
   - `SimulatedExchange` (fake price feed and fills),
   - `AvellanedaStoikovStrategy` (strategy logic).  
3. Runs a loop:
   - Steps mid-price.  
   - Feeds market data into strategy.  
   - Polls fills and updates inventory.  
   - Recomputes and sends quotes.  

To use a real exchange, implement your own `ExchangeAPI` adapter and plug it in here.

---

## 🧮 High-Level Flow

1. **Market Data →** latest mid-price → volatility estimate.  
2. **Model →** compute reservation price & half-spread.  
3. **Strategy →** apply inventory skew, tick rounding, and min spread.  
4. **Execution →** cancel old orders, place new bid/ask quotes.  
5. **Risk →** check limits (PnL, exposure, drawdown).  
6. **Repeat →** every few hundred milliseconds.

---

## 🧪 Quick Start (Simulation)

### 1️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

### 2️⃣ Run the Demo Simulation

```bash
python -m mm.runner
```

### 3️⃣ Observe Logs

```
INFO runner - Starting simulated Avellaneda–Stoikov MM loop
INFO runner - Demo finished
```

The simulation will:
- Generate a random mid-price path.  
- Continuously quote bid/ask around it.  
- Update inventory and wealth dynamically.

---

## 🧱 Extending Toward Production

✅ Replace `SimulatedExchange` with a real exchange API (Binance, OKX, etc.).  
✅ Add WebSocket market-data listener for live mid-price updates.  
✅ Re-estimate `sigma`, `A`, and `k` from real trade/order-book data.  
✅ Add metrics (Prometheus, Influx, etc.) and logging to disk.  
✅ Use Docker or systemd for safe process management.

---

## ⚠️ Disclaimer

This project is for **educational and research purposes** only.  
It provides mathematical and architectural scaffolding for a market-making engine,  
but **it is not ready for live trading**.  
Test thoroughly in a sandbox environment before any deployment with real funds.
