# mm/market_data/ws_stream.py

import json
import time
import ssl
import websocket
from typing import Callable, Optional


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
        self.ws = None

    def run_forever(self):
        def _on_open(ws):
            if self.on_open_cb:
                self.on_open_cb()

        def _on_message(ws, msg):
            recv_time_ms = int(time.time() * 1000)
            payload = json.loads(msg)
            stream = payload.get("stream", "")
            data = payload.get("data", {})

            if "@depth" in stream:
                self.on_depth(data, recv_time_ms)
            elif "@trade" in stream:
                self.on_trade(data, recv_time_ms)

        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=_on_open,
            on_message=_on_message,
        )

        if self.insecure_tls:
            self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
        else:
            self.ws.run_forever()

    def close(self):
        if self.ws:
            self.ws.close()
