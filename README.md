# Habitat DFR Auction Pipeline

A Python data pipeline that ingests NESO Dynamic Frequency Response (DFR) auction results into Postgres via four stages: **extract → load raw → validate → transform**.

For the deep rationale on storage, data model, library choices, observability, and scaling, see [NOTES.md](NOTES.md).
For repo-editing conventions and invariants, see [CLAUDE.md](CLAUDE.md).

## Requirements

- Docker + Docker Compose (recommended), **or**
- Python 3.11+ and a running Postgres 16 (native path).

## Quickstart — Docker (recommended)

```bash
cp .env.example .env
docker compose up -d postgres

# One-time schema init
docker compose run --rm pipeline migrate

# Live demo (~5-10s): 5000 rows through the full pipeline
docker compose run --rm pipeline run --source neso_dfr_results --all --limit 5000

# No-network demo (uses tests/fixtures/sample_records.json)
docker compose run --rm pipeline demo-offline --source neso_dfr_results

# Full extract (all rows since last watermark)
docker compose run --rm pipeline run --source neso_dfr_results --all

# Ops report — dumps ops.pipeline_runs and ops.freshness
docker compose run --rm pipeline report
```

The `pipeline` image's `ENTRYPOINT` is already `habitat-pipeline`, so the subcommand follows directly. The `pipeline` service is one-shot per invocation, not a long-running daemon. `make` wraps all of the above — see the `Makefile`.

## Per-stage commands

Each stage can be run individually. They are idempotent — safe to re-run.

```bash
docker compose run --rm pipeline run --source neso_dfr_results --stage extract
docker compose run --rm pipeline run --source neso_dfr_results --stage load-raw
docker compose run --rm pipeline run --source neso_dfr_results --stage validate
docker compose run --rm pipeline run --source neso_dfr_results --stage transform
```

## Quickstart — Native (no Docker)

```bash
pip install -e ".[dev]"
export DATABASE_URL=postgresql://user:pass@localhost:5432/habitat

habitat-pipeline migrate
habitat-pipeline run --source neso_dfr_results --all --limit 5000
habitat-pipeline report
```

## Tests

Tests run through a dedicated `test` compose service/build stage (not the `pipeline` service — the slim runtime image doesn't ship dev deps or the full `tests/` tree).

```bash
make test
# equivalent to:
docker compose run --rm test -m "db or not db"
```

`@pytest.mark.db` tests run against their own `habitat_test` database and reset it before each test, so they never depend on (or clobber) whatever `pipeline` has loaded.

## Repo layout

```
sources/     Declarative per-source YAML configs.
sql/         schema.sql plus numbered transform SQL files.
src/         Application code.
tests/       Pytest suite + fixtures.
```

## Design in one line

Extract lands JSONL. Load Raw ingests it into `raw.*` (JSONB, unfiltered — the legal record and replay source). Validate reads from `raw`, writes typed rows to `staging.*`, and quarantines failures to `ops.rejected_records`. Transform builds `marts.dim_unit` and `marts.fact_auction_result` from `staging`. Every stage records a row in `ops.pipeline_runs`.

See [NOTES.md](NOTES.md) for the reasoning behind every choice.
