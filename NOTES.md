# NOTES — Habitat DFR Auction Pipeline

Author: Ethan McDonald

This is the "why" behind the pipeline: the decisions made, the evidence behind them, and what's next. If you just want to run it, jump to §1.

## Architecture at a glance

```mermaid
flowchart LR
    NESO["NESO CKAN API<br/>datastore_search_sql"]

    EX["① EXTRACT<br/>pull rows newer than<br/>the last watermark"]
    LD["② LOAD RAW<br/>land as-is, untouched"]
    VA["③ VALIDATE<br/>typecheck &amp; clean;<br/>split good vs. rejected"]
    TR["④ TRANSFORM<br/>build analyst-ready tables"]

    JSONL[("landing/*.jsonl")]
    RAW[("raw.neso_dfr_results<br/>JSONB, PK=unit_result_id")]
    STG[("staging.neso_dfr_results<br/>typed, PK=unit_result_id")]
    DIM[("marts.dim_unit")]
    FACT[("marts.fact_auction_result")]
    OPS[("ops.pipeline_runs<br/>ops.rejected_records<br/>ops.freshness")]

    RAW -.->|"watermark = max(_id)"| EX
    NESO --> EX
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

**In plain terms:** ① **Extract** asks NESO's API for anything newer than what we've already stored and saves it to a plain text file. ② **Load raw** copies that file into Postgres exactly as received — nothing is cleaned or interpreted yet; this is our permanent, unedited copy of what NESO actually said. ③ **Validate** checks each record against a strict shape (right fields present, numbers are numbers, dates parse) — records that pass move into a clean `staging` table with proper types; anything that fails is logged separately without breaking the rest of the batch. ④ **Transform** reshapes the validated data into two simple, analyst-friendly tables: one row per battery/asset, one row per accepted auction result.

Every stage is safe to re-run on its own — running it twice doesn't duplicate or corrupt anything. There's no separate file tracking progress: the watermark is read straight out of Postgres (`raw.max(_id)`), so "how far have we gotten" always has one unambiguous answer.

## The three decisions that matter most

Everything else here is downstream of three calls made before writing any pipeline code.

**1. Raw lands before anything validates it, and is never mutated afterward.** The order is `extract → load raw → validate → transform`, not `extract → validate → load`. If a bad row got filtered out before it ever reached storage, we'd move past its ID and never see it again — silent data loss. Raw-first means a validation bug is a same-day fix (re-run `validate` against data already sitting in Postgres) instead of a re-download against a source with no uptime guarantee.

**2. `unit_result_id`, not NESO's own `_id`, is the identity everything hangs off — and there's no separate progress file.** CKAN (NESO's data platform) can reissue `_id` if a resource ever reloads — a known platform quirk, not a hypothetical. Using it as the dedup key would risk silently dropping real rows via `ON CONFLICT ... DO NOTHING`. `unit_result_id` is NESO's own business key, verified unique across the full dataset before relying on it. And since the watermark is just `MAX(_id)` read live from `raw`, there's no checkpoint file that can drift out of sync with what's actually in the database — a failed run's only recovery step is "run it again."

**3. Sources are config, not code.** The brief says outright: *"This may not be the only dataset we ingest from NESO, nor is NESO our only ingestion source, so structure your code with that in mind."* A `Source` is a YAML file plus a pydantic model; an `Extractor` is a small `Protocol` looked up by name from a registry. Adding a same-API-family source touches zero runner code — proven by `sources/neso_second_dataset.yaml`, a working config-only stub. A non-CKAN source is a new class plus one decorator — proven by `sources/rest.py`, a narrated skeleton for a cursor-paginated REST API.

---

## 1. How to run

### Docker (recommended)

```bash
cp .env.example .env
docker compose up -d postgres            # healthchecked; pipeline waits for pg
docker compose run --rm pipeline migrate

# No-network smoke test — uses tests/fixtures/sample_records.json
docker compose run --rm pipeline demo-offline --source neso_dfr_results

# Live demo (~5-10s): 5000 rows through the whole pipeline
docker compose run --rm pipeline run --source neso_dfr_results --all --limit 5000

# Full extract (all rows since last watermark — safe to re-run any time)
docker compose run --rm pipeline run --source neso_dfr_results --all

# Ops report — dumps ops.pipeline_runs (last 20) + ops.freshness
docker compose run --rm pipeline report
```

Each stage can also be run alone (`--stage extract|load-raw|validate|transform`) — useful for demoing that a mid-pipeline failure is recoverable without redoing earlier stages. `make` wraps all of the above; see the `Makefile`.

### Native (no Docker)

```bash
pip install -e ".[dev]"
export DATABASE_URL=postgresql://user:pass@localhost:5432/habitat
habitat-pipeline migrate
habitat-pipeline run --source neso_dfr_results --all --limit 5000
habitat-pipeline report
```

### Tests

```bash
make test
# equivalent to: docker compose run --rm test -m "db or not db"
```

Tests marked `@pytest.mark.db` run against a dedicated `habitat_test` database (auto-provisioned alongside the dev DB) and skip automatically if `DATABASE_URL` is unset.

### Bonus — the elective: multi-source extensibility

Picked because it's the most direct answer to the brief's own extensibility hint (quoted above), not a generic "let's build something reusable." Two other electives came along as a natural byproduct rather than separate effort:
- **Data-quality / freshness reporting** — `ops.pipeline_runs`, `ops.rejected_records`, and `ops.freshness` (§3) surface metrics as queryable data, not log lines.
- **Data transformation** — `marts.dim_unit` / `marts.fact_auction_result` (§2) reshape staging into something an analyst actually wants to query.

---

## 2. Storage, data model, and library choices

| Layer | Choice | Why |
|---|---|---|
| Storage | Postgres 16 (Docker) | Habitat's own stack; JSONB for raw, strong types for staging/marts, in one database |
| HTTP | `httpx` + `tenacity` | Sensible timeouts, built-in retry/backoff |
| Validation | `pydantic` v2 | Per-record validation with a clean valid/reject split |
| DB driver | `psycopg[binary]` v3 | Native `COPY`, no ORM overhead |
| Bulk load | `COPY` to temp table + `ON CONFLICT DO NOTHING` | 10–50× faster than row-by-row inserts; `copy.write_row()` auto-escapes the literal tabs found in the source data (a hand-built COPY buffer would corrupt on exactly that) |
| Migrations | `sql/schema.sql` via CLI | One migration doesn't earn Alembic at this scope |
| Config | `pydantic-settings` + `.env` + per-source YAML | Env for secrets/deployment, YAML for readable per-source config |
| Orchestration | `typer` CLI + `make` | No scheduler needed yet; a CLI wraps trivially into one later |
| Logging | `structlog` | Structured, queryable; JSON in prod |
| Testing | `pytest` + `respx`; DB tests behind a marker | Fast core loop; DB tests still run in CI |
| Container | Multi-stage Docker build | Small runtime image, reproducible across OSes |

**Data model** — four layers, each cleaner than the last:

```
raw       ← unvalidated landing (JSONB), never mutated, conflict target = unit_result_id
staging   ← typed, validated, deduplicated — everything downstream reads from here
marts     ← dim_unit + fact_auction_result, shaped for an analyst
ops       ← pipeline_runs + rejected_records + a freshness view
```

`marts.fact_auction_result` is keyed on `(auction_unit, auction_product, delivery_start_utc)` — verified unique across the full dataset. `003_fact_auction_result.sql` re-asserts that uniqueness on every run and fails loudly if it's ever violated, rather than silently deduping a real data problem away. `dim_participant` was deliberately dropped as a degenerate single-column dimension — participant lives as an attribute on `dim_unit` instead.

One naming choice worth calling out: `clearing_price_gbp_per_mw_h` is named that specifically (not `_gbp_per_mwh`) because NESO pays for *availability* — £ per MW of capacity, per hour — not for energy delivered. Those are genuinely different units, and it's the easiest thing in this dataset to get wrong out loud.

---

## 3. Data handling, observability, and pipeline runs

**Rejects don't stop the batch, but they do get noticed.** Each record is validated independently; a bad row is logged to `ops.rejected_records` with the specific pydantic error, and the rest of the batch keeps going. By default (`on_reject_threshold: fail`, set per-source in YAML), *any* reject still marks the whole run `failed` in `ops.pipeline_runs` — loud failure over silent degradation, until a source has earned a softer policy (`warn` or `quarantine`, both configurable with no code change).

**Every run is a row, not a log line.** Every stage opens a tracked run (`ops.pipeline_runs`: status, row counts, watermark before/after, error) before doing any work and updates it on exit — success or failure. That means "did last night's run work" is a `SELECT`, not a log search:

```sql
-- Run history
SELECT source, stage, status, rows_read, rows_rejected,
       (rows_rejected::float / NULLIF(rows_read, 0)) AS reject_rate,
       started_at, ended_at - started_at AS duration
FROM ops.pipeline_runs
ORDER BY started_at DESC;

-- Freshness — ingest_lag is the true staleness signal; delivery_horizon is how
-- far into the future auctions have already cleared (should normally be positive,
-- since NESO publishes results ahead of the delivery window)
SELECT * FROM ops.freshness;

-- Reject reasons for the latest validate run
SELECT reason, count(*) FROM ops.rejected_records
WHERE run_id = (SELECT run_id FROM ops.pipeline_runs
                WHERE stage = 'validate' ORDER BY started_at DESC LIMIT 1)
GROUP BY reason;
```

**Schema drift doesn't break anything, but it doesn't go unnoticed either.** `pydantic`'s `extra="ignore"` means a new field NESO adds tomorrow is silently dropped from the *typed* model — but it's still sitting untouched in `raw.payload` (nothing is lost), and the first time a run sees an unrecognized key it's logged once (`validate.extra_keys_observed`), so a schema change is visible without ever stopping the pipeline.

---

## 4. What I'd improve with more time

- **Alembic**, once the schema evolves past one migration.
- **dbt** for the marts layer — the numbered SQL files are a reasonable first step, not the end state, especially once transforms need to be incremental.
- **Prefect or Dagster** for scheduling — the CLI doesn't assume any particular scheduler, so this is a small wrapper, not a redesign.
- **Great Expectations** for data-quality checks deeper than what pydantic covers.
- **A second, real live source** — the extensibility abstraction is proven structurally (a working stub + a narrated non-CKAN skeleton) but hasn't been battle-tested with two real integrations yet.
- **A full start-to-finish integration test** — current DB tests cover idempotency and schema correctness in isolation; there's no single test that runs extract→load→validate→transform end to end against a real database.

## 5. What I'd change at 1000× volume, a backfill, or more sources

- **Landing:** JSONL to S3 (compressed), not local disk.
- **Partitioning:** split `raw`/`staging` by month — faster reads, cheaper deletes, a natural boundary for reprocessing a specific range.
- **Concurrency:** `_id` is dense and gap-free, so a backfill splits cleanly into ID ranges pulled in parallel by multiple workers instead of one sequential stream.
- **Raw shape:** typed columns instead of one JSONB blob per row, with a much smaller JSON column reserved for genuinely new/unexpected fields — reading JSON gets expensive at this size.
- **Transforms:** incremental dbt models instead of rebuilding marts from the full staging table every run.
- **Marts:** move analyst-facing tables to a warehouse (Snowflake/BigQuery/Redshift) once concurrent query load matters.
- **Backfill specifically:** add an explicit `--from-id`/`--to-id` range flag, with per-range progress tracked so a partial backfill failure doesn't require restarting the whole thing.

## 6. Other assumptions and decisions

- **Timezone: NESO's naive timestamps are UTC.** Not documented anywhere by NESO, so this was checked empirically: the earliest `deliveryStart` in the dataset reads `22:00`, and only lines up with the canonical 23:00 EFA-block start time if read as UTC on a date the UK was on BST (23:00 BST = 22:00 UTC). Read as UK local time, the same value would be `23:00`, not `22:00` — so UTC is the reading that's actually internally consistent. Limitation: the dataset doesn't span a clock-change boundary, so this can't be cross-checked against a spring-forward/fall-back date — I'd confirm formally against NESO's own documentation given more time.
- **Delivery windows aren't a fixed width.** Response products run 4-hour EFA blocks; Reserve products run 30-minute settlement periods. The fact table's grain doesn't care, since it keys off the exact `delivery_start_utc` rather than an assumed slot width — but it was worth checking rather than assuming.
- **`executed_quantity_mw` and `clearing_price_gbp_per_mw_h` are `NOT NULL`; `technology_type` and `post_code` are nullable** — not a guess. Checked null rates directly against the full dataset first (0% for the required two, ~0.23% and ~14% respectively for the other two) before locking the constraint either way.
- **AI tooling disclosure:** used throughout the build, plus a dedicated review pass that caught and fixed real issues (a Docker entrypoint problem, a test-isolation gap, a couple of SQL inconsistencies) — all covered by tests. Before submission, the entire environment was rebuilt from a wiped Docker volume and every documented command re-run cold: schema migration, offline demo, live small-batch demo, a full live extract of the current dataset (784,655 rows, up from 769,077 at initial build — the feed keeps growing daily), a second full run proving idempotency (zero rows moved on the re-run), full test suite (16/16 passing), and `ruff`/`mypy --strict` clean. I'm accountable for all of it, which is the standard the brief itself asks for.
