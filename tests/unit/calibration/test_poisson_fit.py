import numpy as np
import pandas as pd

from mm.calibration.poisson_fit import fit_poisson_mle, fit_log_linear


def test_poisson_mle_recovers_params_approximately():
    rng = np.random.default_rng(0)
    A_true = 2.0
    k_true = 0.5
    deltas = np.array([1, 2, 3, 4, 5], dtype=float)
    T = np.ones_like(deltas) * 1000.0  # seconds
    lam = A_true * np.exp(-k_true * deltas)
    N = rng.poisson(lam * T)

    df = pd.DataFrame({
        "delta_bucket": deltas.astype(int),
        "exposure_s": T,
        "fill_events": N,
    })

    res = fit_poisson_mle(df)
    # With large exposure, should be close.
    assert abs(res.A - A_true) / A_true < 0.2
    assert abs(res.k - k_true) / k_true < 0.2


def test_log_linear_runs_on_positive_rates():
    df = pd.DataFrame({
        "delta_bucket": [1, 2, 3],
        "exposure_s": [100.0, 100.0, 100.0],
        "lambda_events_per_s": [1.0, 0.5, 0.25],
    })
    res = fit_log_linear(df)
    assert res.A > 0
    assert res.k > 0
