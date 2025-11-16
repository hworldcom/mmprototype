# mm/exchange.py
import abc
import random
import numpy as np
from dataclasses import dataclass
from .utils import now_ms


@dataclass
class Order:
    order_id: str
    side: str          # "buy" or "sell"
    price: float
    qty: float
    timestamp_ms: int


class ExchangeAPI(abc.ABC):
    @abc.abstractmethod
    def get_mid_price(self) -> float:
        ...

    @abc.abstractmethod
    def place_limit_order(self, side: str, price: float, qty: float) -> str:
        ...

    @abc.abstractmethod
    def cancel_order(self, order_id: str) -> None:
        ...

    @abc.abstractmethod
    def get_open_orders(self) -> list[Order]:
        ...

    @abc.abstractmethod
    def poll_fills(self):
        """
        Returns list of tuples: (order_id, side, price, qty)
        """
        ...


class SimulatedExchange(ExchangeAPI):
    """
    Very simplified simulator:
    - Mid-price follows random walk.
    - Orders fill probabilistically based on distance from mid.
    """

    def __init__(self, mid_start: float, sigma: float, tick_size: float):
        self.mid = mid_start
        self.sigma = sigma
        self.tick_size = tick_size
        self.orders: dict[str, Order] = {}
        self.last_id = 0

    def step_price(self):
        dW = np.random.normal(0.0, 1.0)
        self.mid += self.sigma * dW

    def get_mid_price(self) -> float:
        return self.mid

    def place_limit_order(self, side: str, price: float, qty: float) -> str:
        self.last_id += 1
        oid = f"sim-{self.last_id}"
        self.orders[oid] = Order(
            order_id=oid,
            side=side,
            price=price,
            qty=qty,
            timestamp_ms=now_ms(),
        )
        return oid

    def cancel_order(self, order_id: str) -> None:
        self.orders.pop(order_id, None)

    def get_open_orders(self) -> list[Order]:
        return list(self.orders.values())

    def poll_fills(self):
        """
        Super naive fill model:
        - Higher chance to fill when price is close or inside mid.
        """
        fills = []
        to_delete = []
        for oid, o in self.orders.items():
            distance = abs(o.price - self.mid)
            # simple fill prob: closer → more likely
            p_fill = max(0.0, 0.5 - distance / (10 * self.tick_size))
            if random.random() < p_fill:
                fills.append((oid, o.side, o.price, o.qty))
                to_delete.append(oid)
        for oid in to_delete:
            self.orders.pop(oid, None)
        return fills
