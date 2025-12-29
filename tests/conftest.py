"""Pytest configuration.

The project is intentionally lightweight and does not require installation as
an editable package for development. In CI/automation environments, however,
`pytest` may be executed without the repository root on `sys.path`, which
breaks imports like `import mm...`.

This file ensures the repository root is importable.
"""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
