# Dockerfile
#
# Multi-target Dockerfile:
#  - dev (default): keeps prior behavior (make help)
#  - recorder: defaults to running the market data recorder
#  - calibration: runs Mode B (schedule-only) calibration
#
# Build examples:
#   docker build -t mm-dev:latest .
#   docker build -t mm-recorder:latest --target recorder .
#   docker build -t mm-calibration:latest --target calibration .
#
# Why recorder uses CMD (not ENTRYPOINT)
# -------------------------------------
# Your current cron invokes the recorder like:
#   docker run ... mm-recorder:latest python -m mm.market_data.recorder
# If the image had an ENTRYPOINT of `python -m mm.market_data.recorder`, that
# cron command would become:
#   python -m mm.market_data.recorder python -m mm.market_data.recorder
# and fail.
#
# Using CMD keeps both workflows working:
#  - `docker run ... mm-recorder:latest` runs the recorder by default
#  - `docker run ... mm-recorder:latest python -m ...` overrides CMD (cron-compatible)

FROM python:3.11-slim AS base

WORKDIR /app

# System dependencies (adjust if you need extra libs)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies (cache-friendly layer)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /app/requirements.txt

# Copy project
COPY . /app

# -----------------------
# Default target (dev)
# -----------------------
FROM base AS dev
CMD ["make", "help"]

# -----------------------
# Recorder target
# -----------------------
FROM base AS recorder
# Default behavior if no command is provided
CMD ["python", "-m", "mm.market_data.recorder"]

# -----------------------
# Calibration target (Mode B schedule-only)
# -----------------------
FROM base AS calibration
ENTRYPOINT ["python", "-m", "mm.runner_calibrate_schedule"]
