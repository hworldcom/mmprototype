import numpy as np

class AvellanedaStoikovMM:
    """
    Minimal Avellaneda–Stoikov market maker simulation.
    - Midprice: dS_t = sigma * dW_t (no drift)
    - Order arrivals: Poisson with intensity A * exp(-k * delta)
    - Optimal quotes: classical closed-form formulas.
    """

    def __init__(
        self,
        S0=100.0,          # initial mid price
        sigma=1.0,         # volatility (per sqrt(time_unit))
        gamma=0.01,        # risk aversion
        A=5.0,             # base order arrival intensity
        k=1.5,             # liquidity sensitivity
        T=1.0,             # horizon (in time units)
        dt=0.01,           # time step
        q0=0,              # initial inventory
        X0=0.0,            # initial cash
        q_max=10           # hard inventory cap (for realism)
    ):
        
        self.S0 = S0
        self.sigma = sigma
        self.gamma = gamma
        self.A = A
        self.k = k
        self.T = T
        self.dt = dt
        self.q0 = q0
        self.X0 = X0
        self.q_max = q_max

    def optimal_quotes(self, S, q, t):
        """
        Compute optimal bid/ask using Avellaneda–Stoikov formulas
        (no drift, quadratic inventory penalty approximation).
        """
        tau = max(self.T - t, 0.0)  # time to horizon

        # Reservation price (skewed by inventory)
        r = S - self.gamma * (self.sigma ** 2) * tau * q

        # Optimal half-spread
        # First term: trade-off between spread and fill probability
        # Second term: inventory risk term
        half_spread = (
            (1.0 / self.gamma) * np.log(1.0 + self.gamma / self.k)
            + 0.5 * self.gamma * (self.sigma ** 2) * tau
        )

        bid = r - half_spread
        ask = r + half_spread
        return bid, ask, r, half_spread

    def simulate(self, seed=42):
        np.random.seed(seed)

        n_steps = int(self.T / self.dt)

        # State variables
        S = self.S0
        q = self.q0
        X = self.X0

        # History for analysis
        times = np.zeros(n_steps + 1)
        mid_prices = np.zeros(n_steps + 1)
        bids = np.zeros(n_steps + 1)
        asks = np.zeros(n_steps + 1)
        inventories = np.zeros(n_steps + 1)
        cash_history = np.zeros(n_steps + 1)
        wealth_history = np.zeros(n_steps + 1)

        # Initial
        mid_prices[0] = S
        inventories[0] = q
        cash_history[0] = X
        wealth_history[0] = X + q * S

        for i in range(1, n_steps + 1):
            t = i * self.dt

            # 1) Compute optimal quotes
            bid, ask, r, half_spread = self.optimal_quotes(S, q, t)

            # Enforce inventory caps by killing one side if needed
            if q >= self.q_max:
                # too long: don't post bid (no more buying)
                bid = -np.inf
            if q <= -self.q_max:
                # too short: don't post ask (no more selling)
                ask = np.inf

            # 2) Simulate midprice move: dS = sigma * sqrt(dt) * N(0,1)
            dW = np.random.normal(0.0, np.sqrt(self.dt))
            S = S + self.sigma * dW

            # 3) Order arrival intensities based on distance from mid
            # If we removed a side via +/-inf, intensity becomes 0.
            if np.isfinite(bid):
                delta_b = max(S - bid, 0.0)
                lambda_b = self.A * np.exp(-self.k * delta_b)
            else:
                lambda_b = 0.0

            if np.isfinite(ask):
                delta_a = max(ask - S, 0.0)
                lambda_a = self.A * np.exp(-self.k * delta_a)
            else:
                lambda_a = 0.0

            # 4) Sample number of fills in this dt (Poisson)
            # For small dt, usually 0 or 1, but Poisson is general.
            n_bid_fills = np.random.poisson(lambda_b * self.dt)
            n_ask_fills = np.random.poisson(lambda_a * self.dt)

            # Optional: at most one fill per side per step for simplicity
            n_bid_fills = min(n_bid_fills, 1)
            n_ask_fills = min(n_ask_fills, 1)

            # 5) Update inventory and cash

            # Bid fills: we BUY at our bid
            if n_bid_fills > 0 and np.isfinite(bid):
                # Respect inventory cap
                if q + 1 <= self.q_max:
                    q += 1
                    X -= bid

            # Ask fills: we SELL at our ask
            if n_ask_fills > 0 and np.isfinite(ask):
                # Respect inventory cap
                if q - 1 >= -self.q_max:
                    q -= 1
                    X += ask

            # 6) Record
            times[i] = t
            mid_prices[i] = S
            bids[i] = bid if np.isfinite(bid) else np.nan
            asks[i] = ask if np.isfinite(ask) else np.nan
            inventories[i] = q
            cash_history[i] = X
            wealth_history[i] = X + q * S

        results = {
            "t": times,
            "S": mid_prices,
            "bid": bids,
            "ask": asks,
            "q": inventories,
            "X": cash_history,
            "W": wealth_history,
        }
        return results


if __name__ == "__main__":
    mm = AvellanedaStoikovMM(
        S0=100.0,
        sigma=2.0,
        gamma=0.01,
        A=5.0,
        k=1.5,
        T=1.0,
        dt=0.001,
        q0=0,
        X0=0.0,
        q_max=10
    )

    res = mm.simulate(seed=1)

    # Simple text summary
    final_W = res["W"][-1]
    final_q = res["q"][-1]
    final_S = res["S"][-1]
    print(f"Final mid price: {final_S:.4f}")
    print(f"Final inventory: {final_q}")
    print(f"Final wealth (X + q*S): {final_W:.4f}")
