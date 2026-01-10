#!/usr/bin/env bash
set -euo pipefail

# Clean generated outputs and logs.
# Non-destructive to recorded market data under ./data.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

OUT_DIR="${ROOT_DIR}/out"
LOG_DIR="${ROOT_DIR}/logs"

if [[ -d "${OUT_DIR}" ]]; then
  rm -rf "${OUT_DIR:?}/"*
  echo "Cleaned: ${OUT_DIR}"
fi

if [[ -d "${LOG_DIR}" ]]; then
  rm -rf "${LOG_DIR:?}/"*
  echo "Cleaned: ${LOG_DIR}"
fi

# Common caches (optional)
find "${ROOT_DIR}" -type d -name "__pycache__" -prune -exec rm -rf {} + 2>/dev/null || true
find "${ROOT_DIR}" -type d -name ".pytest_cache" -prune -exec rm -rf {} + 2>/dev/null || true

echo "Done."
