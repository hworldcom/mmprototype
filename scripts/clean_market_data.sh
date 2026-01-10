#!/usr/bin/env bash
set -euo pipefail

# Clean ALL recorded market data under ./data
# This is destructive and cannot be undone.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${ROOT_DIR}/data"

if [[ ! -d "${DATA_DIR}" ]]; then
  echo "No data directory found at: ${DATA_DIR}"
  exit 0
fi

echo "WARNING: This will permanently delete ALL market data under:"
echo "  ${DATA_DIR}"
echo
read -r -p "Type 'DELETE' to continue: " CONFIRM

if [[ "${CONFIRM}" != "DELETE" ]]; then
  echo "Aborted."
  exit 1
fi

rm -rf "${DATA_DIR:?}/"*
echo "Deleted all market data under ${DATA_DIR}"
