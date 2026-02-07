# Local Client Setup (Relay + Metrics)

This guide shows how to run the recorder and connect local clients to the relay
and metrics WebSocket servers.

## 1) Run the recorder

```bash
EXCHANGE=binance SYMBOL=BTCUSDC python -m mm_recorder.recorder
```

Optional: disable gzip events/tail and use only live files:
```bash
WS_RELAY_LIVE_ONLY=1
```

## 2) Run the WebSocket relay

```bash
python -m mm_api.relay
```

Connect a local relay client:
```bash
SYMBOL=BTCUSDC python ws_clients/relay_client.py
```

## 3) Run the metrics server

```bash
python -m mm_api.metrics
```

Connect a local metrics client:
```bash
SYMBOLS=BTCUSDC,ETHUSDC WINDOW=30d python ws_clients/metrics_client.py
```

## 4) Run the REST snapshot endpoint (optional)

```bash
python -m mm_api.rest
```

Example request:
```
http://localhost:8080/snapshot?exchange=binance&symbol=BTCUSDC
```

## Notes
- Metrics use **1m candles** and update **every 1 second** with partial candles from live trades.
- If the metrics server is slow on first run, it is likely fetching a large history window.
  Reduce the window with `WINDOW=7d` or prefetch candles via `mm_history.cli`.
- If live updates stop, ensure the recorder is still running and that the **latest day**
  folder is being tailed (e.g., `data/binance/BTCUSDC/<YYYYMMDD>/live/`).
