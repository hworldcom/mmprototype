"""Calibration utilities.

Calibration is treated as a prerequisite step that *produces* parameters
(e.g., Poisson fill intensities) which are later *consumed* by backtests.

The calibration package intentionally lives outside `mm.backtest` to avoid
mixing measurement logic with evaluation logic.
"""
