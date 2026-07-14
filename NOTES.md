# NOTES — Habitat DFR Auction Pipeline

Author: Ethan McDonald

This document explains **why** the pipeline is built the way it is — the decisions I made, the ones I deliberately put off, and answers to questions I expect to come up in conversation. If you just want to run it, see [README.md](README.md).

---

## 0. Architecture at a glance

### Pipeline flow

```mermaid
flowchart LR
    NESO["NESO CKAN API<br/>datastore_search_sql"]

    EX["extract"]
    LD["load_raw"]
    VA["validate"]
    TR["transform"]

    JSONL[("landing/*.jsonl")]
    RAW[("raw.neso_dfr_results<br/>JSONB, PK=unit_result_id")]
    STG[("staging.neso_dfr_results<br/>typed, PK=unit_result_id")]
    DIM[("marts.dim_unit")]
    FACT[("marts.fact_auction_result")]
    OPS[("ops.pipeline_runs<br/>ops.rejected_records<br/>ops.freshness")]

    NESO -->|"watermark = raw.max(_id)"| EX
    EX --> JSONL
    JSONL --> LD
    LD -->|"COPY + ON CONFLICT<br/>unit_result_id DO NOTHING"| RAW
    RAW --> VA
    VA -->|"pydantic validate<br/>anti-join vs staging"| STG
    STG --> TR
    TR -->|"001 clean_strings"| STG
    TR -->|"002 dim_unit upsert"| DIM
    TR -->|"003 fact_auction_result<br/>grain-asserted upsert"| FACT
    DIM -.->|"FK auction_unit"| FACT

    EX -. run row .-> OPS
    LD -. run row .-> OPS
    VA -. run row + rejects .-> OPS
    TR -. run row .-> OPS
```

Every stage is safe to re-run on its own — running it twice doesn't create duplicates or break anything. There's no separate file tracking progress, either: we just ask Postgres for the highest ID we've already loaded (see §4, §7).

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

Each stage can be run alone. Every stage is safe to re-run:

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

Tests run through a dedicated `test` build stage/compose service (`target: test` in `docker-compose.yml`) — it extends the `builder` stage with dev extras and the full `tests/` tree, which the slim `runtime` image intentionally leaves out.

```bash
make test
# equivalent to:
docker compose run --rm test -m "db or not db"
```

Tests that need a real database (marked `@pytest.mark.db`) run against their own `habitat_test` database — a separate database from whatever the `pipeline` service is pointed at — and it gets wiped and rebuilt before each test via a `clean_db` fixture. That way test results never depend on, or mess up, an in-progress dev/demo database. These tests skip automatically if `DATABASE_URL` isn't set (e.g. running `pytest` locally with no Postgres running).

---

## 2. Why the Group B elective (multi-source extensibility)

The core-task brief explicitly says:

> This may not be the only dataset we ingest from NESO, nor is NESO our only ingestion source, so structure your code with that in mind.

I picked the elective that most directly answers that hint. Building a clean way to plug in new data sources — an `Extractor` interface plus a YAML config file — is work I'd need to do anyway to meet that requirement. The elective just means this part of the design is actually proven, not just claimed.

### Two other electives came along for free

Two other things I built also happen to satisfy other elective groups. Flagging that here so it reads as noticed, not accidental:

- `ops.pipeline_runs`, `ops.rejected_records`, and `ops.freshness` cover **Group B — data-quality / freshness reporting**. The numbers live in a table you can query, not just in log lines.
- `marts.dim_unit` and `marts.fact_auction_result` cover **Group A — data transformation**: staging is the strict, typed layer; marts is the shape an analyst actually wants to query.

---

## 3. Storage, model, and library choices

| Layer | Choice | Why | Alternative |
|---|---|---|---|
| Language | Python 3.11 | Habitat's stack | — |
| HTTP | `httpx` + `tenacity` | Sensible timeouts, built-in retry/backoff | `requests` (older); async (not needed at this volume) |
| Throttling | Sequential, 0.2s between pages | Polite to a free public API; only ~30 pages | Async + semaphore — worth it only if throughput-bound |
| Validation | `pydantic` v2 | Per-record validation, clean valid/reject split | `pandera` — DataFrame-shaped, not row-level |
| Storage | Postgres 16 (Docker) | Habitat's own stack; JSONB for raw, typed columns for staging | DuckDB (off-stack); SQLite (no JSONB) |
| DB driver | `psycopg[binary]` v3 | Native `COPY`, no ORM overhead | `psycopg[c]` — prod-grade but needs system libpq |
| Bulk load | `COPY` to temp + `ON CONFLICT DO NOTHING` | 10–50× faster than row-by-row inserts; safely handles literal tabs in the data | Text-format COPY — would corrupt on those same tabs |
| Migrations | `sql/schema.sql` via CLI | One migration doesn't earn Alembic | Alembic — the next step once schema evolves |
| Config | `pydantic-settings` + `.env` + YAML | Env for secrets, YAML for readable per-source config | JSON — noisier |
| Orchestration | `typer` CLI + `make` | Scope doesn't need a scheduler; wraps into one later | Airflow / Prefect / Dagster — over-scope here |
| Logging | `structlog` | Structured, easy to search; JSON in prod | stdlib logging — weaker observability |
| Testing | `pytest` + `respx`; DB tests behind a marker | Fast core loop; DB tests still run in CI | `testcontainers` — better, more setup time |
| Packaging | `pyproject.toml` | Standard, modern | Poetry — more machinery |
| Container | Multi-stage Docker build | Small runtime image, reproducible across OSes | Single-stage — bigger image |
| Lint/type | `ruff` + `mypy --strict` | One fast tool instead of three | Black + Flake8 + isort |

---

## 4. Data model

Three data layers, each cleaner than the last, plus a fourth (`ops`) for tracking pipeline runs:

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

### Why split into layers

- **Raw is the permanent record.** It holds exactly what NESO sent us, untouched — we never edit or delete rows here. If we later find a bug in how we check the data, we can just recheck what's already there instead of downloading it all over again.
- **Staging is the clean, typed version.** Numbers are numbers, extra whitespace is trimmed, timestamps are all UTC. Everything downstream reads from here.
- **Marts is built for analysts.** Cheap to rebuild from staging at any time, and kept intentionally simple — one table of units (the assets bidding into the auction — batteries, but also other technology types), one table of results. I skipped a separate table for "participant" since it would have held only one real column of data; it's simpler to keep it as a column on the unit table instead.

### Why raw is loaded before validation

The order is `extract → load raw → validate → transform`, not `extract → validate → load`, for two reasons. First, if a bad row got rejected before it ever reached raw, we'd move past its ID and never see it again — a silent way to lose data. Second, if we ever find a bug in our validation rules, we can just recheck the data we already have instead of downloading it all again. Keeping raw first means "how far have we gotten" always has one simple answer: the highest ID sitting in the raw table.

### Why `unit_result_id` is the raw primary key, not `_id`

CKAN (NESO's data platform) assigns `_id` automatically, and that number can shift if NESO ever reloads the dataset — a known quirk of the platform. If we used `_id` to decide "have we already seen this row," a reload could make us re-import old rows as if they were new, or worse, skip real new ones. `unit_result_id` (e.g. `"2661#||#2710#||#PSR#||#150423"`) is a proper identifier built from the auction itself, so it doesn't change on a reload — I checked it's unique across all 769,077 rows before relying on it. `_id` still sticks around on the raw table, purely to track progress.

### Why `delivery_start_uk` is a plain `timestamp`, not `timestamptz`

Converting a UTC timestamp to UK local time in Postgres produces a value with no timezone attached — just a plain date and clock time. That's actually what we want here, since it's what an analyst means by "what time did this clear locally," and declaring the column that way is what makes it work at all: I checked this directly, and declaring the column `timestamptz` instead doesn't get rejected by Postgres — it silently creates the table and silently computes the **wrong** value. The reason: turning that plain local time back into a `timestamptz` requires guessing a timezone, and Postgres does that guess using whatever timezone the connecting session happens to have set — so the same UTC instant would get stored as a different, wrong value depending on who (or what tool) happened to insert it. Declaring the column as a plain `timestamp` sidesteps that guess entirely, since no conversion back is needed. I confirmed this by inserting the same source row under two different session timezones: the plain-`timestamp` version always produced the identical, correct value, while the `timestamptz` version produced two different values for the same input. `test_schema_smoke.py` checks both that the column is generated and that its type is specifically `timestamp` (not `timestamptz`), since Postgres won't stop anyone from silently reintroducing this bug.

---

## 5. Observability

Not just log lines — the numbers live in tables you can query directly.

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

`ops.freshness` is a live query, not a stored table. `ingest_lag` (now minus the last time we loaded data) is the real "how stale is this" number. `delivery_horizon` (the latest delivery time minus now) tells you how far ahead auctions have already been priced — since NESO publishes results for **future** delivery windows, this should normally be a positive number.

---

## 6. Extensibility — how a new source drops in

A `Source` is a YAML config file plus a pydantic model. The extractor is picked by name from a small registry.

**New source, same API family (CKAN):**
1. Add `sources/<name>.yaml`.
2. Add a pydantic model to `src/habitat_pipeline/validate/models.py`.
3. Optionally add a raw/staging table to `sql/schema.sql`.

No changes to any runner code. Proven by `sources/neso_second_dataset.yaml`, a config-only stub.

**New source, new API family (e.g. a REST cursor API):**
1. Subclass `Extractor` in `src/habitat_pipeline/sources/`.
2. Decorate the class with `@register("your_name")`.
3. Reference `your_name` from the YAML.

`src/habitat_pipeline/sources/rest.py` proves this works: it's an unfinished extractor (it raises `NotImplementedError` if you actually try to run it) whose comments spell out exactly what config it would need. The point is to show the interface really fits a non-CKAN source, without shipping code that half-works and might quietly do the wrong thing.

---

## 7. Assumptions & evidence

Every assumption below was checked against the live API before I committed to it. Where I couldn't check something, I say so.

### Timezone: NESO stores UTC

NESO's timestamps don't say what timezone they're in. My working assumption: UTC. Evidence — the earliest timestamp in the data is `2026-03-31 22:00`. The UK was on British Summer Time (UTC+1) on that date, so 22:00 UTC equals 23:00 UK time, and 23:00 is the standard start time for these 4-hour scheduling windows. If the timestamps were already in UK local time, that earliest value would read 23:00, not 22:00 — so UTC is the reading that actually lines up.

**Limitation:** the data I have doesn't cross a clock-change date, so I can't double-check this against a spring-forward or fall-back day. I'd confirm it properly against NESO's own documentation before fully trusting it. Staging stores the UTC version; a second, auto-calculated column gives analysts the UK local-time version too.

### `_id` counts up, `unit_result_id` is the real identifier

- Checked: the row count matches the highest `_id` (both 769,077), and the lowest `_id` is 1 — meaning the IDs count up one at a time, with no gaps.
- Checked: every `unit_result_id` is unique — no duplicates.
- Checked: the combination of (unit, product, delivery time) is also unique. That's the exact combination one row of the results table represents.

### Delivery windows aren't all the same length

I first assumed every result covered a 30-minute window. Wrong — it depends on the product:

- **Response** products (`DCH`, `DCL`, `DMH`, `DML`, `DRH`, `DRL`) → 4-hour blocks (128,968 rows)
- **Reserve** products (`NBR`, `PBR`, `NQR`, `PQR`, `NSR`, `PSR`) → 30-minute windows (640,109 rows)

The results table doesn't need to know or care about this difference — since it keys off the exact start time, both kinds of window fit cleanly side by side.

### `auctionProduct` and `serviceType` always match up

Every product maps to exactly one service type, so technically you could always look one up from the other. I still store both, though, because analysts will want to filter by the readable service-type name, not a product code.

### Checked for missing values before locking columns as required

- `executedQuantity` and `clearingPrice`: zero missing values across 769,077 rows. Safe to require them.
- `technologyType`: missing ~0.23% of the time → left optional.
- `postCode`: missing ~14% of the time → left optional.

### The price is `£/MW/h`, not `£/MWh`

`clearingPrice` is what NESO pays for *being available* to deliver a service (pounds per MW of capacity, per hour) — not what NESO pays for *actual energy delivered* (pounds per megawatt-hour). Those are two different things, and getting the label wrong would stand out immediately to anyone who trades energy for a living. The column name spells it out: `clearing_price_gbp_per_mw_h`.

### Restarting after a failure is safe

The highest `_id` already sitting in the raw table is the only thing tracking progress — there's no separate progress file. If a run fails partway through, the downloaded file is still on disk, and the highest `_id` in raw hasn't moved past the rows that failed. Just run extract again — it re-downloads those same rows, and Postgres quietly skips the ones that already made it in.

### New fields from NESO don't break anything, but don't go unnoticed either

If NESO adds a new field we don't know about, validation just ignores it instead of crashing — and since raw stores the whole original record anyway, nothing is lost. The validate step does log the first time it sees a new field, though, so a schema change shows up without ever stopping the pipeline.

### Bad data fails loudly by default

I'd rather a bad row cause a loud, visible failure than get skipped quietly — by default, even one rejected row marks the whole run as failed, not just a log line nobody reads. This can be relaxed per source (to a warning, or a silent-but-counted "quarantine") once a source has proven itself trustworthy.

---

## 8. What I would do with more time

- **Alembic** once the schema starts evolving beyond one migration.
- **Prefect or Dagster** for scheduling runs — the CLI doesn't depend on any particular scheduler, so plugging one in later is a small task.
- **Great Expectations** for deeper data-quality checks beyond what pydantic covers.
- **A small Grafana dashboard** over `ops.pipeline_runs` and `ops.freshness`.
- **testcontainers**, so database tests spin up a real throwaway database instead of just skipping when one isn't available.
- **Keep a history of changes to `dim_unit`** (a technique called SCD Type 2) if a unit's participant or postcode ever changes and we want to know what it used to be.

---

## 9. What I would change at 1000× volume, backfill, or more sources

- **Landing:** save downloaded files to cloud storage (e.g. S3), compressed, instead of local disk.
- **Partitioning:** split `raw` and `staging` by month. Faster reads, cheaper deletes, and a natural boundary for reprocessing a specific time range.
- **Concurrency:** since IDs count up with no gaps, it's easy to split a big download into ID ranges and pull them in parallel across multiple workers instead of one at a time.
- **Raw shape:** switch from one big JSON blob per row to proper typed columns, plus a much smaller JSON column just for catching new fields NESO adds later. Reading JSON gets expensive at this size.
- **Transforms:** rebuild the marts tables incrementally instead of from scratch every run, using dbt. The current numbered SQL files are a reasonable first step, not the end state.
- **Marts to a warehouse:** move the analyst-facing tables to something like Snowflake, BigQuery, or Redshift once many people are querying at once.
- **Backfill:** add flags to pull a specific ID range on demand, split across workers, with progress saved per range in case one fails partway through.

---

## 10. Known limitations

- There's no single test that runs the whole pipeline start to finish against a real database. The database tests check that loading is safe to repeat and that the schema builds correctly; validation and extracting are tested separately, without a real database.
- No dashboard — SQL queries against `ops.*` are the interface.
- The timezone assumption is backed by evidence, not an official source — I'd confirm it properly with NESO before fully trusting it.
- Runs on one machine. Can't currently spread the work across several.
- The example non-CKAN extractor doesn't actually work yet — it's meant to prove the shape of the interface, not to be used for real. That's intentional; see §6.

---

## 11. Review pass — verification and fixes

Total time on this submission: about 3 hours. The last part was a review pass using AI tooling (Claude Code) — running the pipeline against the real API at its current full size (776,936 rows — up from the 769,077 in §7, since NESO's feed keeps growing — zero rejected, no duplicate rows), fixing a few real bugs it found along the way (some Docker setup issues, a test that wasn't properly isolated from other data, a couple of inconsistencies in how SQL was built), and adding the diagrams above. Every fix is covered by the test suite and was checked again against a full live run afterward.
