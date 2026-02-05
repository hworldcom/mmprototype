from __future__ import annotations

import csv
import gzip
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class TailState:
    line_index: int = 0


def read_gzip_lines(path: Path) -> List[str]:
    if not path.exists():
        return []
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return fh.read().splitlines()


def read_text_lines(path: Path) -> List[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8").splitlines()


def count_gzip_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return sum(1 for _ in fh)


def count_text_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with open(path, "r", encoding="utf-8") as fh:
        return sum(1 for _ in fh)


def tail_ndjson(path: Path, state: TailState) -> List[Dict[str, Any]]:
    lines = read_gzip_lines(path)
    if state.line_index >= len(lines):
        return []
    new_lines = lines[state.line_index :]
    state.line_index = len(lines)
    payloads: List[Dict[str, Any]] = []
    for line in new_lines:
        if not line:
            continue
        payloads.append(json.loads(line))
    return payloads


def tail_text_ndjson(path: Path, state: TailState) -> List[Dict[str, Any]]:
    lines = read_text_lines(path)
    if state.line_index >= len(lines):
        return []
    new_lines = lines[state.line_index :]
    state.line_index = len(lines)
    payloads: List[Dict[str, Any]] = []
    for line in new_lines:
        if not line:
            continue
        payloads.append(json.loads(line))
    return payloads


def tail_csv(path: Path, state: TailState) -> List[Dict[str, Any]]:
    lines = read_gzip_lines(path)
    if not lines:
        return []
    if state.line_index == 0:
        header = lines[0].split(",")
        start = 1
    else:
        header = lines[0].split(",")
        start = state.line_index
    if start >= len(lines):
        return []
    new_lines = lines[start:]
    state.line_index = len(lines)
    reader = csv.DictReader(new_lines, fieldnames=header)
    return [row for row in reader]
