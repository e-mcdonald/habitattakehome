# CLAUDE.md — Repo Conventions for AI Collaborators

Purpose: prevent AI collaborators from re-litigating decisions or violating invariants when editing this repo. Complements `NOTES.md` (which is for human reviewers) — this file focuses on *what not to change* and *where to add things*.

## Repo overview

A Python data pipeline that ingests NESO DFR auction results into Postgres via **extract → load raw → validate → transform**. Deep rationale lives in `NOTES.md`; every non-obvious design choice is defended there. Read it before making architectural changes.

## Non-obvious invariants — do not violate without discussion

- **`raw.*` tables are never mutated after write.** `INSERT ... ON CONFLICT (unit_result_id) DO NOTHING` only. No `UPDATE`, no `DELETE`. Raw is the legal record of what the source said.
- **The `ON CONFLICT` target is `unit_result_id`, never `_id`.** CKAN can re-issue `_id` on resource reload; using `_id` as the conflict target would silently drop rows via `DO NOTHING`.
- **Timezone: NESO's naive ISO strings are UTC** (verified empirically — see NOTES §6). Never convert to local at parse time. UK-local is served by the generated `staging.neso_dfr_results.delivery_start_uk` column.
- **The generated `delivery_start_uk` column is typed `timestamp`, not `timestamptz`.** `AT TIME ZONE 'zone'` on a `timestamptz` returns naive `timestamp`; the implicit cast to `timestamptz` is STABLE, and Postgres rejects non-IMMUTABLE generated column expressions. Do not "fix" the type.
- **Watermark is derived from `raw.max(_id)` at run start.** No separate state file. Do not add one.
- **Load order is raw-first:** `extract → load_raw → validate → transform`. Reordering breaks replay (a validation fix would require re-hitting the API) and breaks the watermark story (rejected rows would advance the cursor).
- **`on_reject_threshold` default is `fail`.** Do not soften without explicit sign-off per source.
- **`dim_participant` was intentionally dropped** as a degenerate single-column dim. Participant is an attribute on `dim_unit`. Do not re-add without new attributes.
- **`_id` is the extract cursor only. `unit_result_id` is the business key.** Any change to this contract needs the resource-reload consequences worked out.
- **Fact grain is `(auction_unit, auction_product, delivery_start_utc)`.** Verified unique across the source dataset. `sql/transforms/003_fact_auction_result.sql` asserts this at load time — do not remove the assertion.

## Where to add things

- **New source, same API family (CKAN):** add a YAML to `sources/` and a Pydantic model to `src/habitat_pipeline/validate/models.py`. No runner changes.
- **New source, new API family:** subclass `Extractor` in `src/habitat_pipeline/sources/`, decorate the class with `@register("name")`, then add a YAML that references the new extractor name.
- **New transform:** numbered `.sql` file in `sql/transforms/`. Must be idempotent (`CREATE TABLE IF NOT EXISTS`, `ON CONFLICT DO NOTHING/UPDATE`).
- **New observability metric:** add a view to `sql/schema.sql` under the `ops` schema. Don't compute in Python — surface metrics as data.

## Testing conventions

- Use `respx` for HTTP mocking. Never hit the live NESO API in tests.
- Mark tests that need a live Postgres with `@pytest.mark.db`. They skip when `DATABASE_URL` is unset. Run inside compose via `docker compose run --rm pipeline pytest -m "db or not db"`.
- Fixtures in `tests/fixtures/` also power `habitat-pipeline demo-offline`. Keep them small (< 1000 rows).
- If `test_schema_smoke.py` fails, `sql/schema.sql` has a typo. Do not "fix" the test to make the schema error pass.

## Commands

- `habitat-pipeline migrate` — applies `sql/schema.sql`.
- `habitat-pipeline run --source <name> --stage extract|load-raw|validate|transform` — single stage.
- `habitat-pipeline run --source <name> --all [--limit N]` — full pipeline; `--limit` caps rows per stage for demos.
- `habitat-pipeline demo-offline --source <name>` — feeds the fixture through the full pipeline, no network.
- `habitat-pipeline report` — dumps `ops.pipeline_runs` and `ops.freshness`.

## Decisions and reasoning — pointer index into NOTES.md

- Storage / model / library choices → NOTES §3
- Why raw/staging/marts split, raw-first ordering → NOTES §5
- Observability model → NOTES §6
- Extensibility abstraction → NOTES §7
- Assumptions & evidence (timezone, nulls, grain) → NOTES §8
- Trade-offs consciously made → NOTES §9
- Scaling story (1000×, backfill, more sources) → NOTES §11

## What NOT to add without discussion

- **Alembic** — `schema.sql` is intentional at this scope.
- **SQLAlchemy** — psycopg + explicit SQL is intentional.
- **An orchestrator** (Airflow/Prefect/Dagster) — CLI is intentional; any scheduler wraps trivially.
- **A dashboard framework** — SQL queries against `ops.*` are the interface.
- **Async everywhere** — sync + `time.sleep(0.2)` throttle is intentional.
- **`make` inside the runtime image** — the `Makefile` is a host-side wrapper; the CLI is what runs in the container.
- **A separate watermark state file** — `raw.max(_id)` is the single source of truth.
