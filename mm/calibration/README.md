# Calibration (Poisson Fill Model)

This repository separates **calibration** (parameter estimation) from **backtesting** (strategy evaluation).

Calibration produces a `poisson_fit.json` containing `(A, k, dt_ms)` for the Poisson intensity model:

\[
\lambda(\delta) = A e^{-k\delta}
\]

where `δ` is quote distance in **ticks** from mid.

---

## Why calibration is separate

Poisson parameters are *strategy-conditioned*: they depend on venue microstructure **and** your quoting/exposure policy (latency, cancels, inventory constraints).  
Calibration therefore runs a **controlled quoting policy** designed for measurement, and writes outputs into:

- `out/calibration/...`

Backtests consume calibration artifacts (e.g., `poisson_fit.json`) and write into:

- `out/backtest/...`

---

## Two supported calibration designs

### Design A — Ladder sweep (probing)
A deterministic calibration quote model cycles through a list of deltas (ticks), holding each for a dwell period.

- Quotes: `bid = mid - δ*tick`, `ask = mid + δ*tick`
- Holds for `dwell_ms`, optionally repositions if mid moves too far.

Use when you want the most statistically efficient estimate of the full curve.

### Design B — Fixed-spread multi-run
Runs multiple independent backtests, each with a fixed delta (ticks).  
Aggregates fill intensity vs delta across runs.

Use when you want an operationally simple calibration method.

---

## How to run calibration

> Note: commands assume you run from the repo root.

### A) Ladder sweep calibration

```bash
export SYMBOL=BTCUSDT
export OUT_DIR=out
export CALIB_METHOD=ladder
export CALIB_DELTAS="1,2,3,5,8,13"
export CALIB_DWELL_MS=60000
export CALIB_MID_MOVE_THRESHOLD_TICKS=2

# Use trade-driven fills to anchor calibration to the tape
export FILL_MODEL=trade_driven

python -m mm.calibration.runner_calibration
```

### B) Fixed-spread multi-run calibration

```bash
export SYMBOL=BTCUSDT
export OUT_DIR=out
export CALIB_METHOD=fixed
export CALIB_DELTAS="1,2,3,5,8,13"

# Use trade-driven fills to anchor calibration to the tape
export FILL_MODEL=trade_driven

python -m mm.calibration.runner_calibration
```

### Fit method
Choose how `(A,k)` are estimated from empirical points:

- `FIT_METHOD=poisson_mle` (recommended; handles zero-fill buckets)
- `FIT_METHOD=log_linear`

Example:

```bash
export FIT_METHOD=poisson_mle
python -m mm.calibration.runner_calibration
```

---

## Rolling schedule calibration (Mode B)

If you want **time-varying Poisson parameters** over a day (e.g., a new `(A,k)` every 15 minutes, each fit on the prior 2 hours),
use the schedule-only runner. This produces a reusable artifact without running any quoting strategy backtest.

```bash
python -m mm.runner_calibrate_schedule \
  --symbol BTCUSDT \
  --day 20250101 \
  --data-root data \
  --out-root out \
  --tick-size 0.01 \
  --train-window-min 120 \
  --step-min 15 \
  --deltas 1,2,3,5,8,13 \
  --dwell-ms 60000 \
  --mid-move-threshold-ticks 2 \
  --min-exposure-s 5.0 \
  --max-delta-ticks 50
```

Outputs are written to:

```
out/calibration/schedules/<SYMBOL>/<YYYYMMDD>_<RUN_ID>/
  poisson_schedule.json
  window_metrics.csv
  manifest.json
  calibration_windows/
    train_<start_ms>_<end_ms>/...
```

The included notebook `calibration_schedule_qa.ipynb` can be used to QA the schedule (coverage, parameter stability, implied fill rates)
and optionally correlate it with market metrics from the trades stream.

---

## Calibration outputs

Each calibration run creates a timestamped folder like:

```
out/calibration/<method>/<SYMBOL>/<YYYYMMDD_HHMMSS>/
  run_manifest.json
  calibration_points.csv
  poisson_fit.json
  runs/
    ... per-run backtest outputs (orders/fills/state)
```

### `calibration_points.csv`
Empirical measurements used for fitting, typically containing:

- `delta_ticks`
- `exposure_s_total`
- `fills_total`
- `lambda_total` (= fills/exposure)

(Optionally bid/ask split if enabled.)

### `poisson_fit.json`
The artifact you use later in backtests. Example:

```json
{
  "distance_unit": "ticks",
  "fit_method": "poisson_mle",
  "A": 0.42,
  "k": 1.35,
  "dt_ms": 100,
  "symbol": "BTCUSDT",
  "method": "ladder",
  "deltas": [1,2,3,5,8,13]
}
```

---

## Using calibration output in backtests

Point your backtest at the calibration artifact:

```bash
export FILL_MODEL=poisson
export FILL_PARAMS_FILE=out/calibration/ladder/BTCUSDT/20240115_120000/poisson_fit.json
python -m mm.runner_backtest
```

You can also use the same `(A,k)` in Hybrid mode:

```bash
export FILL_MODEL=hybrid
export FILL_PARAMS_FILE=.../poisson_fit.json
python -m mm.runner_backtest
```

---

## Implementation overview (code map)

Calibration code lives in `mm/calibration/`.

- `mm/calibration/runner_calibration.py`
  - Entry point. Reads env vars, chooses Design A or B, executes runs, aggregates, fits `(A,k)`, writes outputs.

- `mm/calibration/quotes/calibration_ladder.py`
  - Implements the ladder sweep quote generator (Design A).

- `mm/calibration/quotes/fixed_spread.py`
  - Implements constant-delta quoting (used by Design B runs).

- `mm/calibration/exposure.py`
  - Loads `orders_*.csv`, `fills_*.csv`, `state_*.csv` and computes exposure/fill counts per delta.

- `mm/calibration/poisson_fit.py`
  - Fit routines:
    - Poisson MLE (recommended)
    - log-linear regression

Backtest integration:
- `mm/runner_backtest.py` supports `FILL_PARAMS_FILE` to load `poisson_fit.json`.

---

## Notes / best practices

- Always calibrate on **past** data and apply to **future** data (walk-forward) to avoid look-ahead bias.
- Exclude very small deltas (e.g., δ=0) if queue-position effects dominate.
- Recalibrate periodically (e.g., hourly/daily) or by regime (volatility/trade-rate buckets).

