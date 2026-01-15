#!/usr/bin/env bash
set -euo pipefail

# Compress existing recorded market data files in-place.
#
# Default behavior is SAFE:
#   - Creates <file>.gz next to each <file>
#   - Verifies the gzip output (gzip -t)
#   - Keeps the original uncompressed file
#
# Optional:
#   --delete-src   Delete the original file after successful compression and verification
#   --path <dir>   Target directory to scan (default: ./data)
#   --dry-run      Print what would be done without making changes
#
# Examples:
#   ./scripts/compress_existing_data.sh
#   ./scripts/compress_existing_data.sh --path data/BTCUSDT/20251223
#   ./scripts/compress_existing_data.sh --path data --delete-src
#   ./scripts/compress_existing_data.sh --dry-run

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_PATH="${ROOT_DIR}/data"
DELETE_SRC=0
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --delete-src)
      DELETE_SRC=1
      shift
      ;;
    --path)
      TARGET_PATH="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

if [[ ! -d "${TARGET_PATH}" ]]; then
  echo "Target path not found: ${TARGET_PATH}"
  exit 1
fi

echo "Target path: ${TARGET_PATH}"
echo "Delete originals: ${DELETE_SRC}"
echo "Dry run: ${DRY_RUN}"
echo

if [[ "${DELETE_SRC}" -eq 1 && "${DRY_RUN}" -eq 0 ]]; then
  echo "WARNING: --delete-src will permanently remove original uncompressed files after verifying .gz outputs."
  read -r -p "Type 'YES' to continue: " CONFIRM
  if [[ "${CONFIRM}" != "YES" ]]; then
    echo "Aborted."
    exit 1
  fi
  echo
fi

# Candidate extensions (exclude already gzipped)
mapfile -t FILES < <(find "${TARGET_PATH}" -type f   \( -name "*.csv" -o -name "*.ndjson" -o -name "*.jsonl" \)   ! -name "*.gz"   -print)

if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "No uncompressed .csv/.ndjson/.jsonl files found."
  exit 0
fi

echo "Found ${#FILES[@]} files to compress."
echo

compressed=0
skipped=0

for f in "${FILES[@]}"; do
  gz="${f}.gz"
  if [[ -f "${gz}" ]]; then
    echo "Skip (already exists): ${gz}"
    skipped=$((skipped + 1))
    continue
  fi

  echo "Compress: ${f} -> ${gz}"
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    continue
  fi

  # -c: write to stdout; -n: don't store original filename in header (slightly cleaner, more reproducible)
  gzip -c -n "${f}" > "${gz}"

  # Verify gzip integrity
  gzip -t "${gz}"

  if [[ "${DELETE_SRC}" -eq 1 ]]; then
    rm -f "${f}"
  fi

  compressed=$((compressed + 1))
done

echo
echo "Done. Compressed: ${compressed}, skipped: ${skipped}"
