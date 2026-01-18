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


## split_mixed_day_data.py

Repairs a mixed-day recording folder when the recorder kept running past its intended window and wrote multiple days into the startup day directory (e.g., `data/BTCUSDT/20260114`).

It splits rows/lines into new per-day folders based on `recv_time_ms` (Berlin time) and writes outputs as `.gz`.

Usage:
```bash
# Write fixed data under data_split/ (default)
python scripts/split_mixed_day_data.py --symbol BTCUSDT --source-day 20260114

# Choose a custom output root
python scripts/split_mixed_day_data.py --symbol BTCUSDT --source-day 20260114 --out-root /tmp/data_fixed
```

## purge_non_day_data.py

Repairs a contaminated day folder by **removing any rows/lines whose timestamps are outside the target day**.

This is intended for cases where the recorder process ran past midnight and continued appending data into
the startup day directory (e.g., `data/BTCUSDT/20260114/`), contaminating the day's trades/events/diffs.

Modes:
- `scan` (default): reports what would be removed (no changes)
- `delete`: rewrites files in-place keeping only records in the target day, and deletes snapshots outside the day

Usage:
```bash
# Scan only (no changes)
python scripts/purge_non_day_data.py --symbol BTCUSDT --day 20260114 --mode scan

# Delete (rewrite/purge in-place)
python scripts/purge_non_day_data.py --symbol BTCUSDT --day 20260114 --mode delete

# Custom data root (if needed)
python scripts/purge_non_day_data.py --symbol BTCUSDT --day 20260114 --data-root /mnt/data --mode delete
```

Notes on `purge_non_day_data.py` output:
- `last_kept`: last timestamp that was **kept** for the target day window
- `last_seen`: last timestamp that was **readable** in the file (may be in later days if the folder is contaminated)

## Why these repair scripts exist (what happened)

We had an operational incident where recorder containers continued writing into a *single day folder* for multiple calendar days (e.g. `data/BTCUSDT/20260114/` contained records from 2026-01-15/16/17). This can happen if:

- the recorder is started without a hard stop time (end-of-day / window end), or
- the stop condition is present but not reached due to a bug/regression, or
- the container is restarted while still pointing at the same day directory.

Separately, some `.gz` files became unreadable with:

- `EOFError: Compressed file ended before the end-of-stream marker was reached`

This indicates the gzip stream was cut off mid-write (e.g. container killed, host reboot, out-of-disk, or abrupt process termination). The readable prefix is valid, but the file is not properly closed.

### Why this is dangerous

Cross-day contamination and truncated gzip streams can corrupt downstream workflows:

- calibration (exposure/fills counts and intensity curves)
- replay/backtest integrity (book state reconstruction and event alignment)
- operational monitoring (false resync storms)

### What the scripts do

- `split_mixed_day_data.py`: helps separate mixed multi-day folders into per-day folders.
- `purge_non_day_data.py`: scans a day folder and (optionally) rewrites each file so it contains only rows/lines within the requested day. It also prunes snapshots outside the day window. It is designed to continue even if `.gz` inputs are truncated.
- `align_stream_end_cutoff.py`: repairs truncated `.gz` inputs by finding the **latest common cutoff timestamp** across all streams (intersection), then rewriting every stream to end at that cutoff and producing valid gzip outputs. This is useful when different streams truncate at different times.

### Permissions note (Docker)

If your recorder runs in Docker as root (default), it may create root-owned files in `data/` on the host. The repair scripts rewrite files in-place (create `*.tmp` next to the originals), so your host user must have write permissions. Recommended fix:

- `sudo chown -R <user>:<user> data/<SYMBOL>`

Long-term improvement is to run containers with `--user $(id -u):$(id -g)`.
