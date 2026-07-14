"""Schema smoke test — proves sql/schema.sql applies cleanly.

Catches SQL typos (including the generated-column IMMUTABLE trap around
``AT TIME ZONE``) before a reviewer does. Marked ``@pytest.mark.db`` — needs a
live Postgres; the ``clean_db`` fixture resets to an empty schema first so
this doesn't clobber (or get clobbered by) an in-progress database.
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

        # Generated column compiled — proves the AT TIME ZONE / IMMUTABLE fix.
        cur.execute(
            """
            SELECT is_generated
            FROM information_schema.columns
            WHERE table_schema = 'staging'
              AND table_name = 'neso_dfr_results'
              AND column_name = 'delivery_start_uk'
            """
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] == "ALWAYS"
