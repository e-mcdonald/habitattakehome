# Host-side convenience wrapper. All targets shell into `docker compose run --rm pipeline habitat-pipeline ...`.
# The slim runtime image does NOT ship `make`; the pipeline CLI is what actually runs inside the container.

SOURCE ?= neso_dfr_results
COMPOSE_RUN := docker compose run --rm pipeline

.PHONY: help up down migrate demo demo-offline extract load-raw validate transform all report test lint typecheck fmt

help:
	@echo "Targets:"
	@echo "  up           - start Postgres (healthchecked)"
	@echo "  down         - stop all services"
	@echo "  migrate      - apply sql/schema.sql"
	@echo "  demo         - full pipeline against 5k live rows (~30s)"
	@echo "  demo-offline - full pipeline against tests/fixtures (no network)"
	@echo "  extract      - extract stage only"
	@echo "  load-raw     - load stage only"
	@echo "  validate     - validate stage only"
	@echo "  transform    - transform stage only"
	@echo "  all          - full pipeline against all new rows since last watermark"
	@echo "  report       - dump ops.pipeline_runs and ops.freshness"
	@echo "  test         - run pytest inside compose"
	@echo "  lint         - ruff check + format --check"
	@echo "  typecheck    - mypy strict on src/"
	@echo "  fmt          - ruff format + fix"

up:
	docker compose up -d postgres

down:
	docker compose down

migrate:
	$(COMPOSE_RUN) migrate

demo:
	$(COMPOSE_RUN) run --source $(SOURCE) --all --limit 5000

demo-offline:
	$(COMPOSE_RUN) demo-offline --source $(SOURCE)

extract:
	$(COMPOSE_RUN) run --source $(SOURCE) --stage extract

load-raw:
	$(COMPOSE_RUN) run --source $(SOURCE) --stage load-raw

validate:
	$(COMPOSE_RUN) run --source $(SOURCE) --stage validate

transform:
	$(COMPOSE_RUN) run --source $(SOURCE) --stage transform

all:
	$(COMPOSE_RUN) run --source $(SOURCE) --all

report:
	$(COMPOSE_RUN) report

test:
	docker compose run --rm test -m "db or not db"

lint:
	ruff check .
	ruff format --check .

typecheck:
	mypy

fmt:
	ruff format .
	ruff check --fix .
