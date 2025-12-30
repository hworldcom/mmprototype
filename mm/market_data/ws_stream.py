import ssl
import time
import json
import logging
from typing import Callable, Optional

import websocket


class BinanceWSStream:
    """Thin websocket wrapper that routes depth/trade messages to callbacks."""

    def __init__(
        self,
        ws_url: str,
        on_depth: Callable[[dict, int], None],
        on_trade: Callable[[dict, int], None],
        on_open: Optional[Callable[[], None]] = None,
        on_status: Optional[Callable[[str, dict], None]] = None,
        insecure_tls: bool = False,
        ping_interval_s: int = 20,
        ping_timeout_s: int = 10,
        reconnect_backoff_s: float = 1.0,
    ):
        self.ws_url = ws_url
        self.on_depth = on_depth
        self.on_trade = on_trade
        self.on_open_cb = on_open
        self.on_status_cb = on_status
        self.insecure_tls = insecure_tls

        self.ping_interval_s = int(ping_interval_s)
        self.ping_timeout_s = int(ping_timeout_s)
        self.reconnect_backoff_s = max(0.0, float(reconnect_backoff_s))

        self._ws_app: Optional[websocket.WebSocketApp] = None
        self._log = logging.getLogger("websocket")
        self._stop = False

    def _emit_status(self, typ: str, details: dict) -> None:
        try:
            if self.on_status_cb:
                self.on_status_cb(typ, details)
        except Exception:
            self._log.exception("Status callback error (type=%s)", typ)

    def run(self) -> None:
        """Run the websocket with keepalive and automatic reconnect.

        Binance market data websockets require prompt pong responses and will periodically
        disconnect clients. We therefore run with explicit ping/pong and reconnect.
        """

        self._stop = False

        while not self._stop:
            def _on_open(ws):
                if self.on_open_cb:
                    self.on_open_cb()

            def _on_message(ws, msg: str):
                recv_ms = int(time.time() * 1000)
                try:
                    payload = json.loads(msg)
                except Exception:
                    self._log.exception("Failed to parse WS message")
                    return

                # Combined streams send {"stream": "...", "data": {...}}
                stream = payload.get("stream", "")
                data = payload.get("data", payload)

                try:
                    if "@depth" in stream or data.get("e") == "depthUpdate":
                        self.on_depth(data, recv_ms)
                    elif "@trade" in stream or data.get("e") == "trade":
                        self.on_trade(data, recv_ms)
                except Exception:
                    self._log.exception("Callback error (stream=%s)", stream)

            def _on_error(ws, err):
                self._emit_status("ws_error", {"error": str(err)})
                self._log.error("WebSocket error: %s", err)

            def _on_close(ws, status_code, msg):
                self._emit_status("ws_close", {"code": status_code, "msg": msg})
                self._log.info("WebSocket closed: code=%s msg=%s", status_code, msg)

            def _on_ping(ws, message):
                # websocket-client will reply with pong automatically; we just record telemetry.
                self._emit_status("ws_ping", {"nbytes": len(message) if message else 0})

            def _on_pong(ws, message):
                self._emit_status("ws_pong", {"nbytes": len(message) if message else 0})

            self._ws_app = websocket.WebSocketApp(
                self.ws_url,
                on_open=_on_open,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
                on_ping=_on_ping,
                on_pong=_on_pong,
            )

            sslopt = None
            if self.insecure_tls:
                sslopt = {"cert_reqs": ssl.CERT_NONE, "check_hostname": False}

            # If the connection drops, run_forever returns; we'll reconnect unless stopped.
            try:
                self._ws_app.run_forever(
                    sslopt=sslopt,
                    ping_interval=self.ping_interval_s,
                    ping_timeout=self.ping_timeout_s,
                )
            except Exception as e:
                self._emit_status("ws_run_exception", {"error": str(e)})
                self._log.exception("WebSocket run_forever exception")

            if not self._stop:
                time.sleep(self.reconnect_backoff_s)

    def run_forever(self) -> None:
        """
        Backwards-compatible alias for run().

        Note: websocket-client's WebSocketApp uses run_forever(); we keep the same
        name here so older code can continue to call BinanceWSStream.run_forever().
        """
        self.run()

    def close(self) -> None:
        self._stop = True
        if self._ws_app is not None:
            try:
                self._ws_app.close()
            except Exception:
                pass
