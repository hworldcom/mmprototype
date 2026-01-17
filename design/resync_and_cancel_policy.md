# Resync & Order Safety Policy

## Purpose
This document defines **mandatory behavior during resynchronization events** in a live trading environment. Its goal is to eliminate hidden inventory risk and stale-order exposure when market data continuity is lost.

This policy is **not** about market-data recording or backtesting. It applies **only** to live order placement systems.

---

## Definitions

- **Resync**: Any event where the system detects loss of continuity in market data (e.g. missed WebSocket messages, reconnect, snapshot reload).
- **Outstanding Orders**: Any orders previously sent to the exchange that may still be resting, partially filled, or pending cancellation.
- **Safe State**: A state where the system has authoritative knowledge of:
  - current order book state
  - current open orders
  - current inventory / position

---

## Why Resync Is Dangerous

During a resync, the system **cannot trust its internal state**:

- fills may have occurred during the outage
- cancel acknowledgements may be missing
- order statuses may be stale

Continuing to trade under these conditions can result in:

- duplicated exposure
- uncontrolled inventory accumulation
- quoting against incorrect assumptions

> **Assumption to enforce:** after a resync, *all prior assumptions are invalid*.

---

## Industry-Standard Default: Cancel-All on Resync

**Sending a cancel-all (or equivalent) on resync is normal, professional, and recommended.**

This policy is widely used in:
- market making systems
- latency-sensitive strategies
- systems with tight inventory limits

The rationale is simple:

> If you do not know the truth about your orders, remove them.

---

## Required System Behavior

### On WebSocket Disconnect or Data Gap Detection

1. **Immediately stop placing new orders**
2. Transition system state to `UNSAFE`
3. Block all strategy actions

---

### On Resync Start

1. **Send Cancel-All** to the exchange
   - Use exchange-native `cancelAllOrders` if available
   - Otherwise cancel by symbol / account scope

2. Record event:
   - timestamp
   - reason (e.g. `ws_reconnect`, `ping_timeout`)

3. Start reconciliation timer

---

### During Resync

- No new orders allowed
- No assumptions about inventory
- Market data buffers may refill

---

### On Snapshot Loaded

1. Load fresh order book snapshot
2. Fetch open orders via REST (if applicable)
3. Fetch current inventory / balances

---

### Reconciliation Phase

Before resuming trading, **all conditions must be true**:

- No unexpected open orders remain
- Inventory matches exchange-reported balances
- Market data is fully synchronized

If any condition fails:
- repeat cancel-all
- escalate to operator / halt trading

---

### Resume Trading

Only after reconciliation succeeds:

1. Transition state to `SAFE`
2. Resume quoting from a clean slate
3. Treat this as a *normal recovery*, not an error

---

## State Machine (Conceptual)

```
RUNNING
   │
   ├── WS disconnect / data gap
   ▼
UNSAFE
   │
   ├── cancel-all
   ├── snapshot reload
   ├── reconciliation
   ▼
SAFE
   │
   └── resume trading
```

At no point should the system place orders while in `UNSAFE`.

---

## When Cancel-All Might Be Skipped (Advanced Only)

Cancel-all may be delayed or skipped **only if all of the following are true**:

- system fetches open orders from REST
- full fill reconciliation is performed
- inventory is verified
- latency is not critical

This is **not recommended** for market making or high-frequency strategies.

---

## Logging & Observability Requirements

Every resync must log:

- resync reason
- whether cancel-all was sent
- number of open orders before/after
- time spent in UNSAFE state

Resyncs are **not errors**, but they **must be observable**.

---

## Summary

- Resync is a **safety boundary**, not a nuisance
- Cancel-all on resync is the safest default
- Resume trading only after full reconciliation
- This behavior is standard in professional trading systems

**Principle:**
> When in doubt, flatten risk first — then resume.

