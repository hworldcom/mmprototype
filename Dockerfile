# Dockerfile
#
# Multi-target Dockerfile:
#  - default (dev): keeps prior behavior (make help)
#  - recorder: runs the market data recorder
#  - calibration: runs Mode B (schedule-only) calibration
#
# Build examples:
#   docker build -t mm-dev:latest .
#   docker build -t mm-recorder:latest --target recorder .
#   docker build -t mm-calibration:latest --target calibration .
#
# Run examples:
#   docker run --rm mm-dev:latest
#   docker run --rm -e SYMBOL=BTCUSDT mm-recorder:latest
#   docker run --rm mm-calibration:latest --help

FROM python:3.11-slim AS base

# Set workdir inside the container
WORKDIR /app

# Install system deps (keep minimal; add build tools for wheels if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first for better layer caching
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

# Copy project files into the container
COPY . /app

# -----------------------
# Default target (dev)
# -----------------------
FROM base AS dev

# Default command: just show Makefile help
# You can override this in `docker run` (e.g., `make run-sim`)
CMD ["make", "help"]

# -----------------------
# Recorder target
# -----------------------
FROM base AS recorder

# Recorder entrypoint.
# NOTE: If your recorder is invoked differently (e.g., `make run-recorder`),
# adjust the module path below accordingly.
ENTRYPOINT ["python", "-m", "mm.market_data.recorder"]

# -----------------------
# Calibration target (Mode B schedule-only)
# -----------------------
FROM base AS calibration

ENTRYPOINT ["python", "-m", "mm.runner_calibrate_schedule"]
