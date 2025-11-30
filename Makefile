# Makefile for Avellaneda–Stoikov MM project

# -----------------------------
# Global settings / variables
# -----------------------------

PYTHON ?= python3

# Config paths (adjust if you rename/move configs)
CONFIG_SIM ?= config/config.sim.yaml
CONFIG_BINANCE_TEST ?= config/config.binance.test.yaml

# Docker (optional)
DOCKER_IMAGE ?= avellaneda-mm:latest

# -----------------------------
# Base targets
# -----------------------------

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  make install            - Install Python dependencies"
	@echo "  make run-sim            - Run simulation runner"
	@echo "  make run-binance-test   - Run Binance TEST runner (dry run by default)"
	@echo "  make lint               - Run code formatter / linter (if installed)"
	@echo "  make test               - Run tests (if you add them)"
	@echo "  make docker-build       - Build Docker image"
	@echo "  make docker-run-sim     - Run simulation in Docker"
	@echo "  make docker-run-binance-test - Run Binance test in Docker"

# -----------------------------
# Dev & setup
# -----------------------------

.PHONY: install
install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

.PHONY: lint
lint:
	@echo "Running formatter / linter (black/ruff if installed)..."
	- black mm
	- ruff check mm

.PHONY: test
test:
	@echo "No tests configured yet. Add pytest later if you like."
	# pytest

# -----------------------------
# Runners
# -----------------------------

.PHONY: run-sim
run-sim:
	CONFIG_PATH=$(CONFIG_SIM) $(PYTHON) -m mm.runner_sim

.PHONY: run-binance-test
run-binance-test:
	CONFIG_PATH=$(CONFIG_BINANCE_TEST) $(PYTHON) -m mm.runner_binance_test

# -----------------------------
# Docker helpers (optional)
# -----------------------------

.PHONY: docker-build
docker-build:
	docker build -t $(DOCKER_IMAGE) .

.PHONY: docker-run-sim
docker-run-sim:
	docker run --rm \
		-e CONFIG_PATH=$(CONFIG_SIM) \
		$(DOCKER_IMAGE) \
		make run-sim

.PHONY: docker-run-binance-test
docker-run-binance-test:
	docker run --rm \
		-e CONFIG_PATH=$(CONFIG_BINANCE_TEST) \
		-e BINANCE_API_KEY \
		-e BINANCE_API_SECRET \
		$(DOCKER_IMAGE) \
		make run-binance-test
