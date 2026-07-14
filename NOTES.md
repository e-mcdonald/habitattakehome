# NOTES — Habitat DFR Auction Pipeline

Author: Ethan McDonald

This document accompanies the code. Its job is to explain **why** the pipeline looks the way it does, cover the decisions I made and the ones I consciously postponed, and pre-answer the questions I expect in the follow-up conversation. If you just want to run it, see [README.md](README.md).

---

## 0. Architecture at a glance

### Pipeline flow

```mermaid
flowchart LR
    NESO["NESO CKAN API<br/>datastore_search_sql"]

    subgraph Stages
        EX["extract"]
        LD["load_raw"]
        VA["validate"]
        TR["transform"]
    end

    JSONL[("landing/*.jsonl")]
    RAW[("raw.neso_dfr_results<br/>JSONB, PK=unit_result_id")]
    STG[("staging.neso_dfr_results<br/>typed, PK=unit_result_id")]
    DIM[("marts.dim_unit")]
    FACT[("marts.fact_auction_result")]
    OPS[("ops.pipeline_runs<br/>ops.rejected_records<br/>ops.freshness")]

    NESO -->|"watermark = raw.max(_id)"| EX
    EX --> JSONL
    JSONL -->|"COPY + ON CONFLICT<br/>unit_result_id DO NOTHING"| RAW
    RAW -->|"pydantic validate<br/>anti-join vs staging"| VA
    VA --> STG
    STG -->|"001 clean_strings"| STG
    STG -->|"002 dim_unit upsert"| DIM
    STG -->|"003 fact_auction_result<br/>grain-asserted upsert"| FACT
    DIM -.->|"FK auction_unit"| FACT

    EX -. run row .-> OPS
    LD -. run row .-> OPS
    VA -. run row + rejects .-> OPS
    TR -. run row .-> OPS

    LD -.-> RAW
    TR -.-> STG
```

Every stage is idempotent and independently re-runnable; there's no separate watermark state file — `raw.max(_id)` is the single source of truth (see §4, §7).

### Component map — which file does what

```mermaid
flowchart TB
    CLI["cli.py<br/>typer app: migrate / run / demo-offline / report"]

    subgraph Runners["one runner per stage"]
        EXR["extract/runner.py"]
        LDR["load/runner.py"]
        VAR["validate/runner.py"]
        TRR["transform/runner.py"]
    end

    subgraph SourcesPkg["sources/ — the extensibility seam"]
        REG["registry.py<br/>load_source(), resolve_schema_model()"]
        BASE["base.py<br/>Extractor protocol, @register(), SourceConfig"]
        CKAN["ckan.py<br/>CkanDatastoreSqlExtractor (real, live)"]
        REST["rest.py<br/>RestApiCursorExtractor (documented skeleton)"]
    end

    MODELS["validate/models.py<br/>NesoDfrResult (pydantic)"]
    METRICS["observability/metrics.py<br/>RunTracker, record_rejected,<br/>fetch_recent_runs, fetch_freshness"]
    YAML["sources/*.yaml<br/>per-source declarative config"]
    SQLT["sql/transforms/*.sql<br/>001 → 002 → 003, filename order"]
    SCHEMA["sql/schema.sql"]

    CLI --> EXR --> LDR --> VAR --> TRR
    CLI -.-> SCHEMA
    CLI --> REG --> YAML
    EXR --> BASE
    BASE --> CKAN
    BASE --> REST
    VAR --> MODELS
    TRR --> SQLT
    EXR --> METRICS
    LDR --> METRICS
    VAR --> METRICS
    TRR --> METRICS
```

Adding a new CKAN-family source touches only `sources/*.yaml` + `validate/models.py` — no runner changes (§6).

---

## 1. How to run

### Docker (recommended)

```bash
cp .env.example .env
docker compose up -d postgres            # healthchecked; pipeline waits for pg
docker compose run --rm pipeline migrate

# Fast live demo (~30s): 5000 rows through the whole pipeline
docker compose run --rm pipeline run --source neso_dfr_results --all --limit 5000

# No-network demo — uses tests/fixtures/sample_records.json
docker compose run --rm pipeline demo-offline --source neso_dfr_results

# Full extract (all rows since last watermark)
docker compose run --rm pipeline run --source neso_dfr_results --all

# Ops report — dumps ops.pipeline_runs (last 20) + ops.freshness
docker compose run --rm pipeline report
```

`pipeline`'s image `ENTRYPOINT` is already `habitat-pipeline`, so the subcommand (`migrate`, `run`, ...) follows directly — no need to repeat the binary name. `make` wraps all of the above; see the `Makefile`.

Each stage can be run alone. Every stage is idempotent — safe to re-run:

```bash
docker compose run --rm pipeline run --source neso_dfr_results --stage extract
docker compose run --rm pipeline run --source neso_dfr_results --stage load-raw
docker compose run --rm pipeline run --source neso_dfr_results --stage validate
docker compose run --rm pipeline run --source neso_dfr_results --stage transform
```

### Native (no Docker)

```bash
pip install -e ".[dev]"
export DATABASE_URL=postgresql://user:pass@localhost:5432/habitat
habitat-pipeline migrate
habitat-pipeline run --source neso_dfr_results --all --limit 5000
habitat-pipeline report
```

### Tests

Tests run through a dedicated `test` build stage/compose service (`target: test` in `docker-compose.yml`) — it extends the `builder` stage with dev extras and the full `tests/` tree, which the slim `runtime` image intentionally omits.

```bash
make test
# equivalent to:
docker compose run --rm test -m "db or not db"
```

`@pytest.mark.db` tests apply `sql/schema.sql` against a dedicated `habitat_test` database (not whatever the `pipeline` service's `DATABASE_URL` points at), and reset it (`DROP SCHEMA ... CASCADE` + reapply) at the start of each test via the `clean_db` fixture — so results don't depend on, or clobber, an in-progress dev/demo database. Tests with `@pytest.mark.db` skip automatically when `DATABASE_URL` is unset (e.g. running `pytest` locally without Postgres).

---

## 2. Why the Group B elective (multi-source extensibility)

The core-task brief explicitly says:

> This may not be the only dataset we ingest from NESO, nor is NESO our only ingestion source, so structure your code with that in mind.

I picked the elective that most directly extends that hint. Building a clean `Extractor` + YAML-config abstraction is the same work you'd have to do anyway to satisfy the core task's structural requirement — the elective just makes the abstraction load-bearing rather than aspirational.

### Adjacent electives absorbed as byproducts

I want to flag two things I built that overlap with other electives, so it doesn't read as accidental:

- `ops.pipeline_runs`, `ops.rejected_records`, and `ops.freshness` cover **Group B — Data-quality / freshness reporting**. Metrics are stored as data, not just log lines.
- `marts.dim_unit` and `marts.fact_auction_result` are the **Group A — Data transformation** angle: staging is the schema contract, marts is the analyst-facing shape.

I called these out here rather than pretending they were incidental.

---

## 3. Storage, model, and library choices

| Layer | Choice | Why | Alternative I considered |
|---|---|---|---|
| Language | Python 3.11 | Habitat's stack; ecosystem fit | — |
| HTTP | `httpx.Client` (sync) + `tenacity` | Modern client, sensible timeouts, exponential-backoff retries | `requests` — older signal. Async httpx — unnecessary at this row count; saved for a scale-up note |
| Throttling | Sequential requests + `time.sleep(0.2)` between pages | Polite to a free public API; ~30 pages total; simplest coherent story | Async + semaphore — real gains only if source were throughput-bound |
| Validation | `pydantic` v2 | Fast, per-record validation, clean valid/rejected split | `pandera` — DataFrame-shaped, not idiomatic for row-level ETL |
| Storage | Postgres 16 in Docker | Habitat's own stack (matters); JSONB for raw landing; strong types for staging | DuckDB — off-stack; SQLite — no JSONB, weak concurrency |
| DB driver | `psycopg[binary]` v3 | Native `copy.write_row()` context manager; one less dep than SQLAlchemy | `psycopg[c]` — psycopg's own guidance for prod (compiles against system libpq). `[binary]` chosen for install-simplicity; would move to `[c]` in a production image |
| Bulk load | `COPY` into temp + `INSERT ... SELECT ... ON CONFLICT DO NOTHING` | 10–50× faster than singleton inserts at 770k rows; `write_row()` auto-escapes tabs (data contains literal `\t\t`) | Text-format COPY with string joins — would corrupt on the tab-in-field DQ issue we already know about |
| Migrations | `sql/schema.sql` executed via CLI | One-migration take-home doesn't earn Alembic | Alembic — next step when schema starts evolving |
| Config | `pydantic-settings` + `.env` + per-source YAML | Env for secrets, YAML for readability in review | JSON — noisier |
| Orchestration | `typer` CLI + `make` (host-side wrapper) | Scope doesn't justify a scheduler; CLI wraps trivially into any orchestrator | Airflow / Prefect / Dagster — over-scope; called out as the next step |
| Logging | `structlog` (JSON in prod, colored in dev) | Structured, greppable, ready for any sink | stdlib logging — weaker observability story |
| Testing | `pytest` + `respx` for HTTP; DB tests marked `@pytest.mark.db`, skipped when `DATABASE_URL` unset | Fast core loop; DB tests run inside compose | `testcontainers` — great but eats too much time; the marker pattern is the pragmatic compromise |
| Packaging | `pyproject.toml` + `uv` (fallback pip) | Modern, lockfile-capable, fast | Poetry — slower, more machinery |
| Container | Multi-stage `python:3.11-slim` builder + slim runtime; compose with pg healthcheck | Cross-OS reproducibility; small runtime image | Single-stage — bigger image |
| Lint/format/type | `ruff` + `mypy --strict` on core modules | Fast, single tool for lint+format | Black + Flake8 + isort — three tools where one suffices |

---

## 4. Data model

Three data schemas (medallion) plus `ops`.

```
raw       ← unvalidated landing (JSONB), conflict target = unit_result_id
staging   ← typed, validated, deduplicated
marts     ← dim_unit + fact_auction_result (analyst-facing)
ops       ← pipeline_runs + rejected_records + freshness view
```

```mermaid
erDiagram
    PIPELINE_RUNS ||--o{ RAW_NESO_DFR_RESULTS : "extract_run_id"
    PIPELINE_RUNS ||--o{ REJECTED_RECORDS : "run_id"
    RAW_NESO_DFR_RESULTS ||--o| STAGING_NESO_DFR_RESULTS : "unit_result_id, via validate"
    DIM_UNIT ||--o{ FACT_AUCTION_RESULT : "auction_unit (FK)"
    STAGING_NESO_DFR_RESULTS ||--o{ FACT_AUCTION_RESULT : "(auction_unit, auction_product, delivery_start_utc)"
    STAGING_NESO_DFR_RESULTS }o--|| DIM_UNIT : "auction_unit, via transform 002"

    PIPELINE_RUNS {
        uuid run_id PK
        text source
        text stage "extract|load_raw|validate|transform"
        text status "running|success|failed"
        bigint high_watermark_before
        bigint high_watermark_after
    }
    RAW_NESO_DFR_RESULTS {
        text unit_result_id PK "durable business key"
        bigint _id "extract cursor only, indexed"
        jsonb payload "never mutated"
        uuid extract_run_id FK
    }
    STAGING_NESO_DFR_RESULTS {
        text unit_result_id PK
        bigint _id UK
        numeric executed_quantity_mw
        numeric clearing_price_gbp_per_mw_h "availability price, GBP/MW/h"
        timestamptz delivery_start_utc
        timestamp delivery_start_uk "GENERATED, Europe/London"
        text technology_type "nullable, ~0.23% null"
        text post_code "nullable, ~14% null"
    }
    DIM_UNIT {
        text auction_unit PK
        text technology_type
        text post_code
        timestamptz first_seen_utc
        timestamptz last_seen_utc
    }
    FACT_AUCTION_RESULT {
        text auction_unit PK "FK to dim_unit"
        text auction_product PK
        timestamptz delivery_start_utc PK
        numeric executed_quantity_mw
        numeric clearing_price_gbp_per_mw_h
        text unit_result_id "traceability back to source row"
    }
    REJECTED_RECORDS {
        uuid run_id FK
        jsonb raw_payload
        text reason
    }
```

### Why the split

- **Raw is the legal record.** It holds what the source actually said, byte-for-byte, as JSONB. Never mutated, never filtered. A validation-model bug is a re-validate, not a re-extract.
- **Staging is the schema contract.** Typed columns, `TRIM`ed strings, UTC timestamps. This is what everything downstream reads.
- **Marts is analyst-facing.** Cheap to rebuild from staging. Kept minimal deliberately — one dim, one fact.

### Why raw is loaded before validation

I chose the load order deliberately as `extract → load raw → validate → transform`, not `extract → validate → load`. If validation gated raw:

- A rejected row would advance the watermark past its own `_id` (via successful re-extract next run) — silent data loss unless we tracked rejects separately.
- Fixing a pydantic-model bug would require re-hitting the API, defeating the point of having a "raw" layer.

Loading raw first means the pipeline can be re-validated against fresh model logic offline, and the watermark story is simple (`raw.max(_id)` is the truth).

### Why `unit_result_id` is the raw PK, not `_id`

CKAN's `_id` is generated by the datastore on insert. If the resource is dumped and reloaded (a known CKAN quirk), the `_id` sequence can shift. If we used `_id` as the `ON CONFLICT` target, a re-pull after a reload would collide with stale IDs and silently drop rows via `DO NOTHING`.

`unit_result_id` (the composite `"2661#||#2710#||#PSR#||#150423"` value) is durable across reloads. It's verified unique across all 769,077 rows at plan time. That makes it the correct conflict target. `_id` still lives on raw as an indexed column so we can compute the watermark cheaply.

### Why the generated `delivery_start_uk` column is typed `timestamp`, not `timestamptz`

`delivery_start_utc AT TIME ZONE 'Europe/London'` returns a naive `timestamp` — the local wall clock. If I declared the column as `timestamptz`, Postgres would insert an implicit cast that depends on the session TimeZone GUC, which is `STABLE`, not `IMMUTABLE`. Postgres rejects non-`IMMUTABLE` expressions in generated columns and the `CREATE TABLE` would fail. Naive wall-clock is also what an analyst querying "the local time this cleared for" actually wants.

`test_schema_smoke.py` exists partly to catch this class of DDL bug before a reviewer does.

---

## 5. Observability

Not just log lines — metrics live as data the reviewer can query.

```sql
-- Run history
SELECT source, stage, status, rows_read, rows_rejected,
       (rows_rejected::float / NULLIF(rows_read, 0)) AS reject_rate,
       started_at, ended_at - started_at AS duration
FROM ops.pipeline_runs
ORDER BY started_at DESC;

-- Freshness (ingest_lag is the true freshness signal;
-- delivery_horizon is how far into the future auctions have cleared)
SELECT * FROM ops.freshness;

-- Reject reasons for the last validate run
SELECT reason, count(*)
FROM ops.rejected_records
WHERE run_id = (
    SELECT run_id FROM ops.pipeline_runs
    WHERE stage = 'validate' ORDER BY started_at DESC LIMIT 1
)
GROUP BY reason;
```

`ops.freshness` is a view. `ingest_lag = now() - max(ingested_at)` is the real "how stale is our data" signal. `delivery_horizon = max(delivery_start_utc) - now()` is the "how far into the future has the auction cleared" signal — auction results are published for **future** delivery windows, so this is positive under normal operation.

---

## 6. Extensibility — how a new source drops in

A `Source` is a YAML config + a pydantic model. The extractor is chosen by name from a registry.

**New source, same API family (CKAN):**
1. Add `sources/<name>.yaml`.
2. Add a pydantic model to `src/habitat_pipeline/validate/models.py`.
3. Optionally add a raw/staging table to `sql/schema.sql`.

No runner changes. This is proven by `sources/neso_second_dataset.yaml`, a config-only stub.

**New source, new API family (e.g. a REST cursor API):**
1. Subclass `Extractor` in `src/habitat_pipeline/sources/`.
2. Decorate the class with `@register("your_name")`.
3. Reference `your_name` from the YAML.

Proven by `src/habitat_pipeline/sources/rest.py` — a `RestApiCursorExtractor` skeleton with a `NotImplementedError` body and a docstring that specifies the YAML shape it would consume. The point of the skeleton is to make the interface concrete without shipping half-working code.

---

## 7. Assumptions & evidence

Every assumption below was checked against the live API before I committed to it. Where verification wasn't possible, I say so.

### Timezone: NESO stores UTC

The `deliveryStart` / `deliveryEnd` fields are ISO strings without a timezone offset. My working assumption is that they are UTC. Evidence:

- `min(deliveryStart) = 2026-03-31 22:00` (naive). GB has been on BST (UTC+1) since 2026-03-29, so 22:00 UTC = 23:00 BST — which is the canonical EFA-block start time. Under a "these are local Europe/London" reading, the minimum would be 23:00, not 22:00.
- **Limitation:** the current dataset span (2026-03-31 → 2026-07-14) contains no DST boundary, so I can't corroborate against a spring-forward or fall-back day. I would formally confirm against NESO portal metadata before treating this as ironclad.

The staging column is `delivery_start_utc timestamptz`, and there's a generated `delivery_start_uk timestamp` that materializes the UK wall-clock view for analysts who want it.

### `_id` is a monotonic cursor, `unit_result_id` is the business key

- Verified: `COUNT(*) = MAX(_id) = 769,077` with `MIN(_id) = 1` — a dense, gapless monotonic sequence at plan time.
- Verified: `COUNT(DISTINCT unit_result_id) = 769,077`. The business key is unique.
- Verified: `COUNT(DISTINCT (auction_unit, auction_product, delivery_start)) = 769,077`. That's the fact-table grain.

### Delivery-window granularity is mixed

I initially assumed 30-minute windows across the board. The data disagrees:

- **Response** products (`DCH`, `DCL`, `DMH`, `DML`, `DRH`, `DRL`) → 4-hour EFA blocks (128,968 rows)
- **Balancing / Quick / Slow Reserve** products (`NBR`, `PBR`, `NQR`, `PQR`, `NSR`, `PSR`) → 30-minute settlement periods (640,109 rows)

The fact-table grain `(unit, product, delivery_start_utc)` handles both cleanly because `delivery_start_utc` disambiguates them.

### `auctionProduct → serviceType` is 1:1

Every product maps to exactly one service type. So `serviceType` is derivable from `auctionProduct`, but I still store it on staging and marts because analysts will filter on it more naturally than on the product code.

### NOT NULL verified before I locked the constraint

- `executedQuantity` and `clearingPrice`: 0 nulls across 769,077 rows. Safe to lock `NOT NULL`.
- `technologyType`: 1,760 nulls (~0.23%) → left nullable.
- `postCode`: 110,197 nulls (~14%) → left nullable.

### Unit label is `£/MW/h`, not `£/MWh`

`clearingPrice` is an **availability price** (pounds per MW of capacity per hour), which is not the same thing as an **energy price** (pounds per megawatt-hour). At an energy-trading company, that distinction is instantly spotted. Column name: `clearing_price_gbp_per_mw_h`.

### Watermark idempotency

`raw.max(_id)` is the single source of truth. No separate state file. If a load fails mid-batch, the landing JSONL still exists on disk and `raw.max(_id)` hasn't advanced past the failed rows. Re-running extract will pull those same `_id`s again, and `ON CONFLICT (unit_result_id) DO NOTHING` absorbs the duplicates cleanly.

### Schema drift is tolerated silently but visibly

Pydantic model uses `extra='ignore'`, so new upstream fields don't crash validation — and `raw.payload` JSONB has captured them regardless. The validate runner logs any observed extra keys once per run at INFO level, so drift shows up in the logs without stopping the pipeline.

---

## 8. Trade-offs I consciously made

- **`schema.sql` over Alembic.** One migration doesn't earn Alembic's setup cost. Called out as the next step when the schema starts evolving.
- **`psycopg` over SQLAlchemy.** No ORM, no query composition — SQLAlchemy would be a dependency without a job. Explicit SQL is easier to reason about here.
- **Sync sequential over async concurrency.** ~30 pages × ~700ms is fast enough. Async + semaphore is the scale-up move when a source is throughput-bound or has multiple resources.
- **`psycopg[binary]` over `[c]`.** Simpler install for reviewers (no system libpq). psycopg's own guidance is `[c]` for prod images.
- **Reject-threshold defaults to `fail`.** Loud failure over silent degradation. Per-source YAML can soften it to `warn` or `quarantine` when a source has earned trust.
- **`dim_participant` dropped.** Would have been a degenerate single-column dim carrying only a participant name. Participant is an attribute on `dim_unit` instead.
- **`raw.payload` as JSONB.** Fine at 770k rows and 200-ish bytes per record. At 1000× volume, JSONB read-cost dominates and I'd move to typed columns + a `raw.audit` JSONB sidecar for drift.

---

## 9. What I would do with more time

- **Alembic** once the schema starts evolving beyond one migration.
- **dbt** for the marts layer — turn the numbered SQL files into models with tests.
- **Prefect or Dagster** for orchestration — the CLI is orchestrator-agnostic on purpose, so wrapping it is a small task.
- **Great Expectations** for richer data-quality assertions beyond pydantic.
- **A small Grafana dashboard** over `ops.pipeline_runs` and `ops.freshness`.
- **Explicit backfill flags:** `--from-id` / `--to-id` on extract for bounded historical replays.
- **testcontainers** for real integration tests instead of the `@pytest.mark.db` skip pattern.
- **SCD Type 2 on `dim_unit`** if participant or postcode ever changes for a unit.

---

## 10. What I would change at 1000× volume, backfill, or more sources

- **Landing:** local JSONL → S3 with gzip. Immutable object-store raw landing.
- **Partitioning:** `raw` and `staging` by month of `delivery_start_utc`. Cheaper reads, cheaper deletes, obvious backfill boundaries.
- **Concurrency:** async httpx with bounded `_id` ranges across workers. `_id` is monotonic and dense, which makes range-parallelism trivial.
- **Raw shape:** drop `raw.payload` JSONB in favor of typed raw columns + a slimmer `raw.audit` JSONB sidecar for schema-drift capture. JSONB read cost stops being negligible.
- **Transforms:** move to dbt incremental models. The numbered `.sql` files are a stepping stone.
- **Marts to a warehouse:** Snowflake / BigQuery / Redshift for the analytical layer once query concurrency grows.
- **Backfill:** bounded `--from-id` / `--to-id` on extract, parallelised across workers, checkpointed per range.

---

## 11. Known limitations

- No full end-to-end integration test. The DB-touching tests cover load-raw idempotency and schema smoke; validation and extract are covered offline.
- No dashboard — SQL queries against `ops.*` are the interface.
- Timezone verification is empirical (via EFA-boundary alignment). I would formally confirm against NESO metadata before shipping this.
- Single-node pipeline. No horizontal scale-out.
- The `RestApiCursorExtractor` is a documented skeleton, not a working extractor. That's deliberate — see §6.

---

## 12. Review pass — verification and fixes

Total time on this submission: ~3 hours. The last stretch was a review pass using agentic tooling (Claude Code): running the pipeline against the live API at full volume (776,936 rows, zero rejects, grain assertion holding), fixing a handful of real bugs it surfaced along the way (a couple of Docker/Compose issues, a test-isolation gap, some SQL-composition inconsistencies), and adding the architecture diagrams above. Every fix is covered by the existing test suite and was re-verified against a live full-scale run afterward.
