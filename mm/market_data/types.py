from dataclasses import dataclass
from typing import List, Tuple, Optional

@dataclass
class DepthDiffEvent:
    event_time_ms: int
    U: int
    u: int
    bids: List[Tuple[str, str]]  # raw ["price","qty"]
    asks: List[Tuple[str, str]]

@dataclass
class TradeEvent:
    event_time_ms: int
    trade_time_ms: int
    price: str
    qty: str
    is_buyer_maker: bool
