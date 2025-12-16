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
        insecure_tls: bool = False,
    ):
        self.ws_url = ws_url
        self.on_depth = on_depth
        self.on_trade = on_trade
        self.on_open_cb = on_open
        self.insecure_tls = insecure_tls

        self._ws_app: Optional[websocket.WebSocketApp] = None
        self._log = logging.getLogger("websocket")

    def run_forever(self) -> None:
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
            self._log.error("WebSocket error: %s", err)

        def _on_close(ws, status_code, msg):
            self._log.info("WebSocket closed: code=%s msg=%s", status_code, msg)

        self._ws_app = websocket.WebSocketApp(
            self.ws_url,
            on_open=_on_open,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close,
        )

        sslopt = None
        if self.insecure_tls:
            sslopt = {"cert_reqs": ssl.CERT_NONE, "check_hostname": False}

        self._ws_app.run_forever(sslopt=sslopt)

    def close(self) -> None:
        if self._ws_app is not None:
            try:
                self._ws_app.close()
            except Exception:
                pass
