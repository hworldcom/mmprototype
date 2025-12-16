import ssl, time, json, logging
from typing import Callable, Optional
import websocket

class BinanceWSStream:
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
        self._ws_app = None
        self._log = logging.getLogger("market_data.ws_stream")

    def close(self):
        try:
            if self._ws_app and self._ws_app.sock:
                self._ws_app.close()
        except Exception:
            pass

    def run_forever(self):
        def _on_open(ws):
            self._log.info("Websocket connected")
            if self.on_open_cb:
                self.on_open_cb()

        def _on_message(ws, message: str):
            recv_ms = int(time.time() * 1000)
            payload = json.loads(message)
            data = payload.get("data", payload)
            et = data.get("e")
            if et == "depthUpdate":
                self.on_depth(data, recv_ms)
            elif et == "trade":
                self.on_trade(data, recv_ms)

        def _on_error(ws, error):
            self._log.error("WebSocket error: %s", error)

        def _on_close(ws, code, msg):
            self._log.info("Websocket closed code=%s msg=%s", code, msg)

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
