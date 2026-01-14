from __future__ import annotations

import pandas as pd

from mm.calibration.exposure import summarize_bucketed_exposure


def test_summarize_bucketed_exposure_uses_fill_events_and_usable():
    points = pd.DataFrame(
        {
            "delta_bucket": [1, 2, 3],
            "exposure_s": [10.0, 20.0, 30.0],
            "fill_events": [5, 0, 7],
            "usable": [True, False, True],
        }
    )
    summary = summarize_bucketed_exposure(points)
    assert summary["exposure_s_total"] == 60.0
    assert summary["fills_total"] == 12
    assert summary["fills_usable_total"] == 12  # only usable buckets have nonzero fills in this example
    assert summary["n_deltas_usable"] == 2


def test_summarize_bucketed_exposure_backcompat_n_fills():
    points = pd.DataFrame(
        {
            "delta_bucket": [1, 2],
            "exposure_s": [5.0, 5.0],
            "n_fills": [2, 3],
            "usable": [True, True],
        }
    )
    summary = summarize_bucketed_exposure(points)
    assert summary["fills_total"] == 5
    assert summary["fills_usable_total"] == 5


def test_summarize_bucketed_exposure_empty_df():
    summary = summarize_bucketed_exposure(pd.DataFrame())
    assert summary["exposure_s_total"] == 0.0
    assert summary["fills_total"] == 0
    assert summary["fills_usable_total"] == 0
    assert summary["n_deltas_usable"] == 0
