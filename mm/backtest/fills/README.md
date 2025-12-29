# Fill Models Summary

This backtesting stack currently supports three fill models. Each model determines **when** an order fills, **how much** fills, and (indirectly) how inventory/PnL evolves in replay.

The models live under `mm/backtest/fills/` and are selected via:

- `FILL_MODEL` env var (e.g., `trade_driven`, `poisson`, `hybrid`)
- optional `FILL_PARAMS_JSON` for per-model parameters

---

## 1) Trade-driven fill model (`trade_cross`)

**Concept**

A deterministic, event-driven model that generates fills **only when recorded trades cross your resting limit price**.

- Your **BUY** can fill when a trade prints at a price **≤ your bid**
- Your **SELL** can fill when a trade prints at a price **≥ your ask**

This corresponds to the fill reason `trade_cross` in `fills_*.csv`.

**When to use**

- Best “first realistic baseline” when you have trade prints in the replay.
- Good for stress-testing adverse selection and latency sensitivity.
- Still optimistic unless you model queue position (it assumes you are fill-eligible as soon as the trade crosses, without explicit queue priority).

**Key behavior**

- Fills occur on `on_trade(...)` callbacks.
- Can produce partial fills if enabled.

**Parameters**

- `allow_partial` *(bool, default: True)*  
  If `True`, orders can be partially filled across one or multiple trade events.

- `max_fill_qty` *(float, default: very large)*  
  Caps the fill size per trade event (useful to avoid unrealistically filling the entire order on a single trade).

---

## 2) Poisson fill model (Avellaneda–Stoikov style)

**Concept**

A stochastic fill model consistent with Avellaneda–Stoikov assumptions: fills arrive with intensity that decays exponentially with distance from the mid.

A typical form is:

\[
\lambda(\delta) = A e^{-k\delta}
\]

where:

- `δ` is distance from mid (or best) in price units (or ticks, depending on implementation)
- `A` is base intensity
- `k` controls how quickly intensity decays with distance

**When to use**

- When you want a parameterized fill process that is easy to calibrate from replay statistics.
- Useful for fast parameter sweeps and scenario testing.
- Less “microstructure faithful” than trade-driven (because it is not tied to individual prints).

**Key behavior**

- Fills may occur on ticks even if there are no explicit trade prints crossing your price.
- Intended to be calibrated: choose `A` and `k` to match observed fill rates at different quote distances.

**Parameters**

- `A` *(float)* — base arrival rate (higher ⇒ more fills)
- `k` *(float)* — distance sensitivity (higher ⇒ fewer fills as you quote wider)
- `dt_ms` *(int)* — simulation time step for the hazard approximation (if used)

---

## 3) Hybrid fill model

**Concept**

Combines the strengths of both approaches:

- **Trade-driven** fills on trade events (captures aggressing flow and realistic cross behavior)
- **Poisson background** fills on ticks (captures “ambient” passive fill probability when the tape alone would under-fill)

**When to use**

- Best default if you want realism *and* stable fill behavior across different market regimes.
- Helpful when trade prints are sparse or timestamping is coarse, but you still want passive fills to occur.

**Parameters**

Union of:
- Trade-driven: `allow_partial`, `max_fill_qty`
- Poisson: `A`, `k`, `dt_ms`

---

## Output mapping

All fill models write to:

- `fills_<SYMBOL>.csv` — one row per fill event  
  - `reason` indicates why the fill occurred (e.g., `trade_cross`)
- `orders_<SYMBOL>.csv` — order lifecycle (PLACE/CANCEL/FILL/CLOSE_FILLED, etc.)
- `state_<SYMBOL>.csv` — inventory/cash/mtm over time

---

## Practical calibration guidance

1. Start with **trade-driven** to validate lifecycle correctness and latency/cancel behavior.
2. Add **Poisson** for quick sweeps and for matching average fill rates at different spreads.
3. Use **Hybrid** once you have rough `A/k` calibration and want fewer edge cases (sparse trades) without losing tape realism.
