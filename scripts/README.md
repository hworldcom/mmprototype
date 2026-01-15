# Scripts

This folder contains operational helper scripts for development and data hygiene.

## clean_generated_output.sh

Deletes generated outputs and logs without touching recorded market data.

- Cleans: `./out/`, `./logs/`
- Also removes: `__pycache__/`, `.pytest_cache/`

Usage:
```bash
./scripts/clean_generated_output.sh
```

## clean_market_data.sh

**Destructive**: deletes all recorded market data under `./data/`.

This script prompts for confirmation and requires typing `DELETE`.

Usage:
```bash
./scripts/clean_market_data.sh
```

## clean_caches.sh

Removes common Python caches across the repository.

Usage:
```bash
./scripts/clean_caches.sh
```

## compress_existing_data.sh

Compresses existing recorded data files in-place.

- Compresses: `*.csv`, `*.ndjson`, `*.jsonl` to `*.gz`
- Validates output with `gzip -t`
- By default keeps original uncompressed files (safe mode)

Usage:
```bash
# Safe mode (creates .gz files, keeps originals)
./scripts/compress_existing_data.sh

# Target a specific day/symbol folder
./scripts/compress_existing_data.sh --path data/BTCUSDT/20251223

# Delete originals after successful compression
./scripts/compress_existing_data.sh --delete-src

# Preview actions without changing files
./scripts/compress_existing_data.sh --dry-run
```
