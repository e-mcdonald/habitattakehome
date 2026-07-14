"""Idempotency: loading the same batch twice must not change row counts.

Marked ``@pytest.mark.db`` — skipped when ``DATABASE_URL`` is unset. Run inside
compose via::

    make test

The ``clean_db`` fixture resets raw/staging/marts/ops to empty before each
test, so results don't depend on what a prior run left in the database.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from habitat_pipeline.load import connect, run_load_raw
from habitat_pipeline.sources.registry import load_source


@pytest.mark.db
def test_load_raw_is_idempotent(
    tmp_path: Path, project_root: Path, fixture_records: list[dict], clean_db: str
) -> None:
    """Two loads of the same JSONL produce the same rowcount as one."""
    database_url = clean_db

    # Write the fixture as JSONL landing.
    import json

    landing = tmp_path / "landing.jsonl"
    with landing.open("w", encoding="utf-8") as fh:
        for record in fixture_records:
            fh.write(json.dumps(record))
            fh.write("\n")

    cfg = load_source(project_root / "sources", "neso_dfr_results")

    with connect(database_url) as conn:
        first = run_load_raw(conn=conn, source=cfg, landing_path=landing)
        second = run_load_raw(conn=conn, source=cfg, landing_path=landing)

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw.neso_dfr_results")
            (total,) = cur.fetchone()

    assert first.rows_read == len(fixture_records)
    assert first.rows_written == len(fixture_records)
    # Second load reads the same rows but ON CONFLICT DO NOTHING inserts zero new rows.
    assert second.rows_read == len(fixture_records)
    assert second.rows_written == 0
    assert total == len(fixture_records)


@pytest.mark.db
def test_ckan_reload_shift_does_not_lose_rows(
    tmp_path: Path, project_root: Path, fixture_records: list[dict], clean_db: str
) -> None:
    """Simulate a CKAN resource reload: same unit_result_id, different _id.

    The conflict target must be unit_result_id (not _id), so re-ingesting the
    same business row with a bumped _id must not duplicate the row.
    """
    database_url = clean_db

    import json

    landing1 = tmp_path / "landing1.jsonl"
    landing2 = tmp_path / "landing2.jsonl"

    with landing1.open("w", encoding="utf-8") as fh:
        for r in fixture_records:
            fh.write(json.dumps(r))
            fh.write("\n")

    # Second landing: same unit_result_ids, but every _id bumped by 1_000_000.
    with landing2.open("w", encoding="utf-8") as fh:
        for r in fixture_records:
            reissued = dict(r)
            reissued["_id"] = r["_id"] + 1_000_000
            fh.write(json.dumps(reissued))
            fh.write("\n")

    cfg = load_source(project_root / "sources", "neso_dfr_results")

    with connect(database_url) as conn:
        run_load_raw(conn=conn, source=cfg, landing_path=landing1)
        second = run_load_raw(conn=conn, source=cfg, landing_path=landing2)

        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw.neso_dfr_results")
            (total,) = cur.fetchone()

    assert second.rows_written == 0  # every row collides on unit_result_id
    assert total == len(fixture_records)
