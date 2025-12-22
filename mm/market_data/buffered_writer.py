from __future__ import annotations

import csv
import time
from pathlib import Path
from typing import Iterable, Sequence


class BufferedCSVWriter:
    """Batch rows in memory before flushing to disk to reduce fsync pressure."""

    def __init__(
        self,
        path: str | Path,
        header: Sequence[str] | None = None,
        flush_rows: int = 1000,
        flush_interval_s: float = 1.0,
    ) -> None:
        self.path = Path(path)
        self.header = list(header) if header else None
        self.flush_rows = max(1, flush_rows)
        self.flush_interval_s = max(0.0, float(flush_interval_s))

        self._buffer: list[list[str]] = []
        self._file = None
        self._writer: csv.writer | None = None
        self._last_flush = time.monotonic()

    def _ensure_open(self) -> None:
        if self._file is not None:
            return

        self.path.parent.mkdir(parents=True, exist_ok=True)
        existed = self.path.exists()
        self._file = self.path.open("a", newline="")
        self._writer = csv.writer(self._file)

        if self.header and ((not existed) or self.path.stat().st_size == 0):
            self._writer.writerow(self.header)
            self._file.flush()

    def write_row(self, row: Sequence[str | int | float]) -> None:
        self._ensure_open()
        self._buffer.append([str(v) for v in row])
        if self._should_flush():
            self.flush()

    def write_rows(self, rows: Iterable[Sequence[str | int | float]]) -> None:
        for row in rows:
            self.write_row(row)

    def flush(self) -> None:
        if not self._buffer or self._writer is None or self._file is None:
            self._last_flush = time.monotonic()
            return

        self._writer.writerows(self._buffer)
        self._file.flush()
        self._buffer.clear()
        self._last_flush = time.monotonic()

    def close(self) -> None:
        try:
            self.flush()
        finally:
            if self._file is not None:
                self._file.close()
                self._file = None
                self._writer = None

    def _should_flush(self) -> bool:
        if len(self._buffer) >= self.flush_rows:
            return True
        if self.flush_interval_s == 0.0:
            return False
        return (time.monotonic() - self._last_flush) >= self.flush_interval_s

    def __enter__(self) -> "BufferedCSVWriter":
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
