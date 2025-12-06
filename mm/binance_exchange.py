# mm/binance_exchange.py
from dataclasses import dataclass
from typing import Optional, List, Tuple

from .exchange import ExchangeAPI, Order
from .utils import now_ms


@dataclass
class BinanceCredentials:
    api_key: str
    api_secret: str
    testnet: bool = True
    recv_window: int = 5000


class BinanceExchange(ExchangeAPI):
    """
    Minimal Binance connector implementing ExchangeAPI.

    NOTE:
    - This expects `python-binance` to be installed.
    - Not production-hardened (no full error handling / backoff).
    """

    def __init__(self, creds: BinanceCredentials, symbol: str):
        from binance.client import Client
        import os

        api_key = creds.api_key or os.getenv("BINANCE_API_KEY", "")
        api_secret = creds.api_secret or os.getenv("BINANCE_API_SECRET", "")

        if not api_key or not api_secret:
            raise ValueError("Binance API key/secret missing (config or env).")

        self.symbol = symbol.upper()
        self.client = Client(api_key, api_secret, testnet=creds.testnet)
        self.recv_window = creds.recv_window

        self.orders: dict[str, Order] = {}
        self._last_trade_id: Optional[int] = None

    # --- pricing --------------------------------------------------------------

    def get_mid_price(self) -> float:
        ticker = self.client.get_orderbook_ticker(symbol=self.symbol)
        bid = float(ticker["bidPrice"])
        ask = float(ticker["askPrice"])
        return 0.5 * (bid + ask)

    # --- orders ---------------------------------------------------------------

    def place_limit_order(self, side: str, price: float, qty: float) -> str:
        from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_LIMIT_MAKER

        side_binance = SIDE_BUY if side.lower() == "buy" else SIDE_SELL

        # Format price/qty as Binance-friendly strings
        price_str = f"{price:.10f}".rstrip("0").rstrip(".")
        qty_str = f"{qty:.10f}".rstrip("0").rstrip(".")

        order = self.client.create_order(
            symbol=self.symbol,
            side=side_binance,
            type=ORDER_TYPE_LIMIT_MAKER,  # post-only spot order
            quantity=qty_str,
            price=price_str,
            recvWindow=self.recv_window,
            # you *could* add newOrderRespType="RESULT" or "FULL" here,
            # but we don't actually need it for now.
        )

        oid = str(order["orderId"])

        # IMPORTANT: response does NOT contain "price"/"origQty" in ACK mode,
        # so we trust our own arguments for local state.
        self.orders[oid] = Order(
            order_id=oid,
            side=side.lower(),
            price=price,
            qty=qty,
            timestamp_ms=now_ms(),
        )

        return oid

    def cancel_order(self, order_id: str) -> None:
        try:
            self.client.cancel_order(
                symbol=self.symbol,
                orderId=int(order_id),
                recvWindow=self.recv_window,
            )
        except Exception:
            # already filled or canceled; ignore
            pass
        self.orders.pop(order_id, None)

    def get_open_orders(self) -> List[Order]:
        binance_orders = self.client.get_open_orders(
            symbol=self.symbol, recvWindow=self.recv_window
        )
        result: List[Order] = []
        for o in binance_orders:
            oid = str(o["orderId"])
            side = o["side"].lower()
            price = float(o["price"])
            qty = float(o["origQty"])
            ts = o.get("time", 0)
            result.append(
                Order(
                    order_id=oid,
                    side=side,
                    price=price,
                    qty=qty,
                    timestamp_ms=int(ts),
                )
            )
        return result

    # --- fills ----------------------------------------------------------------

    def poll_fills(self) -> List[Tuple[str, str, float, float]]:
        trades = self.client.get_my_trades(
            symbol=self.symbol, recvWindow=self.recv_window
        )

        fills: List[Tuple[str, str, float, float]] = []
        max_id = self._last_trade_id if self._last_trade_id is not None else -1

        for t in trades:
            trade_id = int(t["id"])
            if self._last_trade_id is not None and trade_id <= self._last_trade_id:
                continue

            order_id = str(t["orderId"])
            price = float(t["price"])
            qty = float(t["qty"])
            side = "buy" if t["isBuyer"] else "sell"

            fills.append((order_id, side, price, qty))
            if trade_id > max_id:
                max_id = trade_id

        if max_id >= 0:
            self._last_trade_id = max_id

        for oid, _, _, _ in fills:
            self.orders.pop(oid, None)

        return fills
