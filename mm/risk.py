from dataclasses import dataclass

@dataclass
class RiskState:
    inventory: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    peak_equity: float = 0.0

    @property
    def equity(self) -> float:
        return self.realized_pnl + self.unrealized_pnl


@dataclass
class RiskLimits:
    max_inventory: float
    max_notional_abs: float
    max_daily_loss: float
    max_drawdown: float


class RiskManager:
    def __init__(self, limits: RiskLimits):
        self.limits = limits

    def update_unrealized(self, state: RiskState, mid: float, position_notional: float):
        state.unrealized_pnl = position_notional
        if state.equity > state.peak_equity:
            state.peak_equity = state.equity

    def check_limits(self, state: RiskState, mid: float) -> bool:
        """
        Returns True if trading is allowed.
        """
        # Inventory notional
        notional = abs(state.inventory * mid)
        if notional > self.limits.max_notional_abs:
            return False

        # Absolute inventory units
        if abs(state.inventory) > self.limits.max_inventory:
            return False

        # Max daily loss
        if state.equity < -self.limits.max_daily_loss:
            return False

        # Drawdown
        if state.peak_equity - state.equity > self.limits.max_drawdown:
            return False

        return True
