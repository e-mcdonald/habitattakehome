"""Schema smoke test — proves sql/schema.sql applies cleanly.

Catches SQL typos before a reviewer does, including the ``delivery_start_uk``
generated-column type (see NOTES.md §4 — Postgres silently accepts
``timestamptz`` here and computes a session-dependent, wrong value instead of
rejecting it, so the type itself has to be asserted directly). Marked
``@pytest.mark.db`` — needs a live Postgres; the ``clean_db`` fixture resets to
an empty schema first so this doesn't clobber (or get clobbered by) an
in-progress database.
"""

from __future__ import annotations

import psycopg
import pytest


@pytest.mark.db
def test_schema_applies_and_core_objects_exist(clean_db: str) -> None:
    with psycopg.connect(clean_db) as conn, conn.cursor() as cur:
        # Core tables present
        for qualified in [
            ("raw", "neso_dfr_results"),
            ("staging", "neso_dfr_results"),
            ("marts", "dim_unit"),
            ("marts", "fact_auction_result"),
            ("ops", "pipeline_runs"),
            ("ops", "rejected_records"),
        ]:
            cur.execute("SELECT to_regclass(%s)", (f"{qualified[0]}.{qualified[1]}",))
            (present,) = cur.fetchone()
            assert present, f"missing table: {qualified[0]}.{qualified[1]}"

        # Freshness view present
        cur.execute("SELECT to_regclass('ops.freshness')")
        assert cur.fetchone()[0]

        # Generated column compiled with the right type. Postgres does NOT
        # reject `timestamptz` here (verified directly against Postgres 16) —
        # it silently accepts it and computes a value that depends on the
        # connecting session's timezone, which is wrong. Asserting the type
        # is `timestamp` (not `timestamptz`) is the only thing that actually
        # guards against that regression; `is_generated` alone would not.
        cur.execute(
            """
            SELECT is_generated, data_type
            FROM information_schema.columns
            WHERE table_schema = 'staging'
              AND table_name = 'neso_dfr_results'
              AND column_name = 'delivery_start_uk'
            """
        )
        row = cur.fetchone()
        assert row is not None
        is_generated, data_type = row
        assert is_generated == "ALWAYS"
        assert data_type == "timestamp without time zone", (
            f"delivery_start_uk must stay a naive timestamp, got {data_type!r} — "
            "see NOTES.md §4 for why timestamptz silently computes the wrong value"
        )
