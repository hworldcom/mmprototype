from __future__ import annotations

import asyncio
import json
import os
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import websockets
from websockets.server import WebSocketServerProtocol

from mm_api.protocols import make_message
from mm_api.sources import resolve_latest_paths
from mm_api.tailer import (
    TailState,
    count_gzip_lines,
    count_text_lines,
    tail_csv,
    tail_ndjson,
    tail_text_ndjson,
)


POLL_INTERVAL_S = float(os.getenv("WS_RELAY_POLL_INTERVAL_S", "1.0"))
LIVE_ONLY = os.getenv("WS_RELAY_LIVE_ONLY", "0").strip() in ("1", "true", "True")
log = logging.getLogger("mm_api.relay")


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _parse_query(path: str) -> Dict[str, str]:
    if "?" not in path:
        return {}
    _, query = path.split("?", 1)
    items = [kv.split("=", 1) for kv in query.split("&") if kv]
    return {k: v for k, v in items if len(k) > 0}


async def _send_json(ws: WebSocketServerProtocol, payload: Dict[str, Any]) -> None:
    await ws.send(json.dumps(payload, ensure_ascii=False))


async def _send_status(ws: WebSocketServerProtocol, exchange: str, symbol: str, message: str) -> None:
    await _send_json(
        ws,
        make_message(
            msg_type="status",
            exchange=exchange,
            symbol=symbol,
            ts_ms=_now_ms(),
            data={"message": message},
        ),
    )


async def _send_snapshot(ws: WebSocketServerProtocol, exchange: str, symbol: str, path: Optional[str]) -> None:
    if not path:
        await _send_status(ws, exchange, symbol, "snapshot not found")
        return
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        await _send_json(
            ws,
            make_message(
                msg_type="snapshot",
                exchange=exchange,
                symbol=symbol,
                ts_ms=_now_ms(),
                data=raw if isinstance(raw, dict) else {"raw": raw},
            ),
        )
    except Exception as exc:
        await _send_status(ws, exchange, symbol, f"snapshot read failed: {exc}")


async def _stream_loop(
    ws: WebSocketServerProtocol,
    exchange: str,
    symbol: str,
    from_mode: str,
) -> None:
    paths = resolve_latest_paths(exchange, symbol)
    if not paths:
        await _send_status(ws, exchange, symbol, "no data directory found")
        return

    await _send_snapshot(ws, exchange, symbol, str(paths.get("snapshot")) if paths.get("snapshot") else None)

    diff_state = TailState()
    trade_state = TailState()
    event_state = TailState()
    diff_path = paths.get("live_diffs") if LIVE_ONLY else (paths.get("live_diffs") or paths.get("diffs"))
    trade_path = paths.get("live_trades") if LIVE_ONLY else (paths.get("live_trades") or paths.get("trades"))
    current_day_dir = paths.get("day_dir")
    if from_mode == "tail":
        for path, state in (
            (diff_path, diff_state),
            (trade_path, trade_state),
            (None if LIVE_ONLY else paths.get("events"), event_state),
        ):
            if path:
                if path.suffix == ".gz":
                    state.line_index = count_gzip_lines(path)
                else:
                    state.line_index = count_text_lines(path)

    await _send_status(ws, exchange, symbol, "tailing latest files")

    while True:
        await asyncio.sleep(POLL_INTERVAL_S)
        latest_paths = resolve_latest_paths(exchange, symbol)
        if latest_paths and latest_paths.get("day_dir") != current_day_dir:
            current_day_dir = latest_paths.get("day_dir")
            paths = latest_paths
            diff_path = paths.get("live_diffs") if LIVE_ONLY else (paths.get("live_diffs") or paths.get("diffs"))
            trade_path = paths.get("live_trades") if LIVE_ONLY else (paths.get("live_trades") or paths.get("trades"))
            diff_state = TailState()
            trade_state = TailState()
            event_state = TailState()
            if from_mode == "tail":
                for path, state in (
                    (diff_path, diff_state),
                    (trade_path, trade_state),
                    (None if LIVE_ONLY else paths.get("events"), event_state),
                ):
                    if path:
                        if path.suffix == ".gz":
                            state.line_index = count_gzip_lines(path)
                        else:
                            state.line_index = count_text_lines(path)
            await _send_status(ws, exchange, symbol, f"switched to new day folder {current_day_dir}")
            await _send_snapshot(
                ws,
                exchange,
                symbol,
                str(paths.get("snapshot")) if paths.get("snapshot") else None,
            )
        if diff_path:
            tailer = tail_ndjson if diff_path.suffix == ".gz" else tail_text_ndjson
            for payload in tailer(diff_path, diff_state):
                await _send_json(
                    ws,
                    make_message(
                        msg_type="diff",
                        exchange=exchange,
                        symbol=symbol,
                        ts_ms=int(payload.get("E") or payload.get("recv_ms") or _now_ms()),
                        data=payload,
                    ),
                )
        if trade_path:
            tailer = tail_ndjson if trade_path.suffix == ".gz" else tail_text_ndjson
            for payload in tailer(trade_path, trade_state):
                await _send_json(
                    ws,
                    make_message(
                        msg_type="trade",
                        exchange=exchange,
                        symbol=symbol,
                        ts_ms=int(payload.get("E") or payload.get("recv_ms") or _now_ms()),
                        data=payload,
                    ),
                )
        if (not LIVE_ONLY) and paths.get("events"):
            for payload in tail_csv(paths["events"], event_state):
                await _send_json(
                    ws,
                    make_message(
                        msg_type="event",
                        exchange=exchange,
                        symbol=symbol,
                        ts_ms=int(payload.get("recv_ms") or _now_ms()),
                        data=payload,
                    ),
                )


def _get_path(ws: WebSocketServerProtocol) -> str:
    req = getattr(ws, "request", None)
    if req is not None and hasattr(req, "path"):
        return req.path
    return getattr(ws, "path", "")


async def _handler(ws: WebSocketServerProtocol) -> None:
    params = _parse_query(_get_path(ws))
    exchange = params.get("exchange", "binance")
    symbol = params.get("symbol")
    from_mode = params.get("from", "tail")
    if not symbol:
        await _send_status(ws, exchange, "", "symbol is required")
        return
    log.info("Relay client connected exchange=%s symbol=%s from=%s", exchange, symbol, from_mode)
    await _send_status(ws, exchange, symbol, "connected")
    try:
        await _stream_loop(ws, exchange, symbol, from_mode)
    except websockets.ConnectionClosed:
        return


async def _run_server(host: str, port: int) -> None:
    async with websockets.serve(_handler, host, port):
        await asyncio.Future()


def main() -> None:
    host = os.getenv("WS_RELAY_HOST", "0.0.0.0")
    port = int(os.getenv("WS_RELAY_PORT", "8765"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    log.info("WS relay listening on ws://%s:%s/ws", host, port)
    asyncio.run(_run_server(host, port))


if __name__ == "__main__":
    main()
