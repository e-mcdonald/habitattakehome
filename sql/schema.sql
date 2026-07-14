-- Habitat DFR Auction Pipeline — schema
--
-- Applied by `habitat-pipeline migrate`. Safe to run repeatedly (all objects use IF NOT EXISTS).
--
-- Design summary (see NOTES.md for full rationale):
--   raw      : landing zone, unvalidated JSONB, never mutated
--   staging  : validated, typed, deduplicated (conflict target = unit_result_id)
--   marts    : analytics-ready dims + fact
--   ops      : observability as data (runs, rejects, freshness view)

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS ops;

-- ---------------------------------------------------------------------------
-- ops: created first because raw references pipeline_runs.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id                 uuid        PRIMARY KEY,
    source                 text        NOT NULL,
    stage                  text        NOT NULL,   -- extract | load_raw | validate | transform
    started_at             timestamptz NOT NULL DEFAULT now(),
    ended_at               timestamptz,
    status                 text        NOT NULL,   -- running | success | failed
    rows_read              integer,
    rows_written           integer,
    rows_rejected          integer,
    high_watermark_before  bigint,
    high_watermark_after   bigint,
    error                  text
);

CREATE INDEX IF NOT EXISTS pipeline_runs_source_stage_started_idx
    ON ops.pipeline_runs (source, stage, started_at DESC);

CREATE TABLE IF NOT EXISTS ops.rejected_records (
    run_id       uuid        NOT NULL REFERENCES ops.pipeline_runs (run_id),
    source       text        NOT NULL,
    raw_payload  jsonb       NOT NULL,
    reason       text        NOT NULL,
    rejected_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS rejected_records_run_idx
    ON ops.rejected_records (run_id);

-- ---------------------------------------------------------------------------
-- raw: unvalidated landing. unit_result_id is the conflict target (survives
-- CKAN resource reloads). _id is the extract cursor only.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.neso_dfr_results (
    unit_result_id  text         PRIMARY KEY,   -- durable business key
    _id             bigint       NOT NULL,      -- extract cursor
    payload         jsonb        NOT NULL,
    ingested_at     timestamptz  NOT NULL DEFAULT now(),
    extract_run_id  uuid         NOT NULL REFERENCES ops.pipeline_runs (run_id)
);

CREATE INDEX IF NOT EXISTS raw_neso_dfr_results_id_idx
    ON raw.neso_dfr_results (_id);
CREATE INDEX IF NOT EXISTS raw_neso_dfr_results_ingested_idx
    ON raw.neso_dfr_results (ingested_at);

-- ---------------------------------------------------------------------------
-- staging: typed, validated.
--
-- delivery_start_uk is `timestamp` (naive), not `timestamptz`. AT TIME ZONE
-- 'zone' on a timestamptz returns a naive timestamp, and the implicit cast to
-- timestamptz would be STABLE (depends on session TimeZone), which Postgres
-- rejects for generated column expressions. Naive-local wall-clock is also
-- semantically what analysts want here.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.neso_dfr_results (
    unit_result_id                  text          PRIMARY KEY,
    _id                             bigint        NOT NULL UNIQUE,
    registered_auction_participant  text          NOT NULL,
    auction_unit                    text          NOT NULL,
    service_type                    text          NOT NULL,
    auction_product                 text          NOT NULL,
    executed_quantity_mw            numeric(12,4) NOT NULL,
    -- Availability price (£ per MW of capacity per hour). NOT an energy price (£/MWh).
    clearing_price_gbp_per_mw_h     numeric(12,4) NOT NULL,
    delivery_start_utc              timestamptz   NOT NULL,
    delivery_end_utc                timestamptz   NOT NULL,
    delivery_start_uk               timestamp     GENERATED ALWAYS AS
                                        (delivery_start_utc AT TIME ZONE 'Europe/London') STORED,
    technology_type                 text,
    post_code                       text,
    ingested_at                     timestamptz   NOT NULL,
    extract_run_id                  uuid          NOT NULL REFERENCES ops.pipeline_runs (run_id)
);

CREATE INDEX IF NOT EXISTS staging_neso_dfr_delivery_start_idx
    ON staging.neso_dfr_results (delivery_start_utc);
CREATE INDEX IF NOT EXISTS staging_neso_dfr_unit_delivery_idx
    ON staging.neso_dfr_results (auction_unit, delivery_start_utc);

-- ---------------------------------------------------------------------------
-- marts: analytics-ready. Populated by sql/transforms/*.sql.
-- dim_participant intentionally omitted (would be degenerate).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS marts.dim_unit (
    auction_unit                    text        PRIMARY KEY,
    technology_type                 text,
    post_code                       text,
    registered_auction_participant  text        NOT NULL,
    first_seen_utc                  timestamptz NOT NULL,
    last_seen_utc                   timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS marts.fact_auction_result (
    auction_unit                 text          NOT NULL REFERENCES marts.dim_unit (auction_unit),
    auction_product              text          NOT NULL,
    service_type                 text          NOT NULL,
    delivery_start_utc           timestamptz   NOT NULL,
    delivery_end_utc             timestamptz   NOT NULL,
    executed_quantity_mw         numeric(12,4) NOT NULL,
    clearing_price_gbp_per_mw_h  numeric(12,4) NOT NULL,
    unit_result_id               text          NOT NULL,
    PRIMARY KEY (auction_unit, auction_product, delivery_start_utc)
);

CREATE INDEX IF NOT EXISTS fact_delivery_start_idx
    ON marts.fact_auction_result (delivery_start_utc);

-- ---------------------------------------------------------------------------
-- ops.freshness: ingest_lag is the true freshness signal. delivery_horizon is
-- how far into the future the auction has cleared (positive = future delivery
-- already priced).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW ops.freshness AS
SELECT
    'neso_dfr_results'::text                         AS source,
    max(ingested_at)                                 AS last_ingest,
    max(delivery_start_utc)                          AS last_delivery,
    now() - max(ingested_at)                         AS ingest_lag,
    max(delivery_start_utc) - now()                  AS delivery_horizon
FROM staging.neso_dfr_results;
