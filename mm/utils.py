import time
import math

def now_ms() -> int:
    return int(time.time() * 1000)

def round_to_tick(price: float, tick_size: float) -> float:
    return math.floor(price / tick_size) * tick_size

def clamp_qty(qty: float, step: float) -> float:
    steps = math.floor(qty / step)
    return max(0.0, steps * step)
