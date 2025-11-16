import numpy as np

class AvellanedaStoikovModel:
    """
    Core Avellaneda–Stoikov formulas (no market connectivity).
    """

    def __init__(self, gamma: float, A: float, k: float, T: float):
        self.gamma = gamma
        self.A = A
        self.k = k
        self.T = T

    def reservation_price(self, S: float, q: float, sigma: float, t: float) -> float:
        tau = max(self.T - t, 0.0)
        return S - self.gamma * (sigma ** 2) * tau * q

    def half_spread(self, sigma: float, t: float) -> float:
        tau = max(self.T - t, 0.0)
        term1 = (1.0 / self.gamma) * np.log(1.0 + self.gamma / self.k)
        term2 = 0.5 * self.gamma * (sigma ** 2) * tau
        return term1 + term2

    def optimal_quotes(
        self,
        S: float,
        q: float,
        sigma: float,
        t: float,
    ):
        """
        Return (bid, ask, reservation_price, half_spread)
        before microstructure adjustments.
        """
        r = self.reservation_price(S, q, sigma, t)
        h = self.half_spread(sigma, t)
        return r - h, r + h, r, h
