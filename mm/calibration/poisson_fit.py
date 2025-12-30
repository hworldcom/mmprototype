from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Tuple

import numpy as np
import pandas as pd

from scipy.optimize import minimize


@dataclass(frozen=True)
class PoissonFitResult:
    A: float
    k: float
    method: str
    n_points: int


def fit_log_linear(
    points: pd.DataFrame,
    *,
    delta_col: str = "delta_bucket",
    exposure_col: str = "exposure_s",
    lambda_col: str = "lambda_events_per_s",
) -> PoissonFitResult:
    """Fit log(lambda)=log(A)-k*delta using weighted least squares."""

    df = points.copy()
    df = df[(df[exposure_col] > 0) & (df[lambda_col] > 0)].copy()
    if len(df) < 2:
        raise ValueError("Need at least two (delta, lambda) points with positive exposure and intensity")

    x = df[delta_col].to_numpy(dtype=float)
    y = np.log(df[lambda_col].to_numpy(dtype=float))
    w = df[exposure_col].to_numpy(dtype=float)

    b1, b0 = np.polyfit(x, y, deg=1, w=w)
    k_hat = float(-b1)
    A_hat = float(np.exp(b0))
    return PoissonFitResult(A=A_hat, k=k_hat, method="log_linear", n_points=int(len(df)))


def fit_poisson_mle(
    points: pd.DataFrame,
    *,
    delta_col: str = "delta_bucket",
    exposure_col: str = "exposure_s",
    count_col: str = "fill_events",
) -> PoissonFitResult:
    """Fit A,k by maximizing a Poisson likelihood.

    We assume fill counts per bucket follow:
      N_i ~ Poisson( lambda(delta_i) * T_i )
      lambda(delta)=A*exp(-k*delta)

    This estimator is robust to zero-fill buckets (no log problems).
    """

    df = points.copy()
    df = df[(df[exposure_col] > 0) & (df[delta_col].notna()) & (df[count_col].notna())].copy()
    if len(df) < 2:
        raise ValueError("Need at least two buckets with positive exposure")

    d = df[delta_col].to_numpy(dtype=float)
    T = df[exposure_col].to_numpy(dtype=float)
    N = df[count_col].to_numpy(dtype=float)

    # Negative log-likelihood in log-parameterization for positivity.
    def nll(theta: np.ndarray) -> float:
        logA, logk = float(theta[0]), float(theta[1])
        A = np.exp(logA)
        k = np.exp(logk)
        lam = A * np.exp(-k * d)
        mu = lam * T
        mu = np.clip(mu, 1e-12, None)
        # NLL up to constant: sum(mu - N*log(mu))
        return float(np.sum(mu - N * np.log(mu)))

    # Initial guess from log-linear if possible, otherwise heuristic.
    try:
        df2 = df.copy()
        df2["lambda_events_per_s"] = df2[count_col] / df2[exposure_col].replace(0, np.nan)
        init = fit_log_linear(df2, delta_col=delta_col, exposure_col=exposure_col, lambda_col="lambda_events_per_s")
        x0 = np.array([np.log(max(init.A, 1e-9)), np.log(max(init.k, 1e-9))], dtype=float)
    except Exception:
        # Heuristic: A ~ total rate at delta=0, k ~ 1
        total_rate = float(N.sum() / max(T.sum(), 1e-9))
        x0 = np.array([np.log(max(total_rate, 1e-9)), np.log(1.0)], dtype=float)

    res = minimize(nll, x0=x0, method="Nelder-Mead")
    if not res.success:
        raise RuntimeError(f"Poisson MLE optimization failed: {res.message}")

    logA, logk = float(res.x[0]), float(res.x[1])
    A_hat = float(np.exp(logA))
    k_hat = float(np.exp(logk))
    return PoissonFitResult(A=A_hat, k=k_hat, method="poisson_mle", n_points=int(len(df)))
