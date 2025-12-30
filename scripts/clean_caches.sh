#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Cleaning Python caches in: $ROOT_DIR"

# Python bytecode
find "$ROOT_DIR" -type d -name "__pycache__" -prune -exec rm -rf {} +
find "$ROOT_DIR" -type f \( -name "*.pyc" -o -name "*.pyo" \) -delete

# Test / tool caches
rm -rf \
  "$ROOT_DIR/.pytest_cache" \
  "$ROOT_DIR/.mypy_cache" \
  "$ROOT_DIR/.ruff_cache" \
  "$ROOT_DIR/.coverage"

# Jupyter
find "$ROOT_DIR" -type d -name ".ipynb_checkpoints" -prune -exec rm -rf {} +

# OS / editor noise
find "$ROOT_DIR" -type f -name ".DS_Store" -delete

echo "Cache cleanup complete."
