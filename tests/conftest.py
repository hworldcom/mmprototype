"""Pytest configuration for mm-recorder."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CORE = ROOT / "mm_core"
if CORE.exists() and str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
