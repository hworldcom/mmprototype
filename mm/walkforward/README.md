# Walk-forward calibration + continuous backtest

This package implements a **rolling-window calibration loop** for Poisson fill parameters `(A, k)` and then runs a **single continuous backtest** using **piecewise-constant** parameters.

## What it does

Given a trading day with recorded market data:

1. Split the day into fixed **step intervals** (default: 15 minutes).
2. For each step interval `[t, t+step)`:
   - calibrate Poisson `(A, k)` on the **preceding training window** `[t-train, t)` (default: 2 hours)
   - store the fitted parameters as the segment's parameters
3. Run one continuous backtest over the full day using `TimeVaryingPoissonFillModel`, which selects `(A, k)` by `recv_ms`.

If a training window produces no usable calibration points (insufficient exposure), the system **carries forward the most recent successful parameters** (common operational fallback).

## How to run

From the repository root:

```bash
python -m mm.runner_walkforward \
  --symbol BTCUSDT \
  --day 20250101 \
  --data-root data \
  --out-root out
```

Or via environment variables (same as the other runners):

```bash
export SYMBOL=BTCUSDT
export DAY=20250101
export DATA_ROOT=data
export OUT_ROOT=out

# Walk-forward settings
export WF_TRAIN_WINDOW_MIN=120
export WF_STEP_MIN=15

# Calibration ladder settings
export CALIB_DELTAS="1,2,3,5,8,13"
export CALIB_DWELL_MS=60000
export CALIB_MID_MOVE_THRESHOLD_TICKS=2
export FIT_METHOD=poisson_mle
export POISSON_DT_MS=100

# Strategy under test (quote model)
export QUOTE_MODEL=avellaneda_stoikov
export QUOTE_QTY=0.001
export TICK_SIZE=0.01
export INITIAL_CASH=1000

python -m mm.runner_walkforward
```

## Outputs

A run creates:

- `out/walkforward/calibration_windows/<SYMBOL>/<DAY>_<RUN_ID>/...`
  - one directory per training window, containing:
    - `orders.csv`, `fills.csv`, `state.csv` (controlled ladder run)
    - `calibration_points.csv`
    - `poisson_fit.json` (if usable)
- `out/walkforward/runs/<SYMBOL>/<DAY>_<RUN_ID>/`
  - `poisson_schedule.json` — piecewise parameters used in the backtest
  - `manifest.json` — provenance/config + key paths
  - `backtest/` — backtest outputs (`orders.csv`, `fills.csv`, `state.csv`, reports)

