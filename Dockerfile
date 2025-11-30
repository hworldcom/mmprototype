# Dockerfile
FROM python:3.11-slim

# Set workdir inside the container
WORKDIR /app

# Install system deps (optional but good to have build tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy project files into the container
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Default command: just show Makefile help
# You can override this in `docker run` (e.g., `make run-sim`)
CMD ["make", "help"]
