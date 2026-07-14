"""Load Raw runner.

Reads the landing JSONL, streams it into a temp table via ``psycopg.copy``,
then merges into ``raw.<table>`` with ``ON CONFLICT (unit_result_id) DO NOTHING``.

Why COPY into a temp table rather than direct COPY into ``raw``:
    ``COPY`` cannot express ``ON CONFLICT`` semantics on its own. Landing the
    rows in a session-scoped temp and then ``INSERT ... SELECT ... ON CONFLICT``
    is the standard pattern for a fast, idempotent bulk load.

Why ``copy.write_row`` and not string-built COPY payloads:
    The NESO source contains literal ``\\t\\t`` in string fields (per the API's
    own metadata). Text-format COPY uses tab as its delimiter — a hand-rolled
    buffer would corrupt on exactly the data-quality issue we already know
    about. psycopg's ``write_row`` handles quoting/escaping for us.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from habitat_pipeline.logging import get_logger
from habitat_pipeline.observability.metrics import RunTracker
from habitat_pipeline.sources.base import SourceConfig, split_schema_table

log = get_logger(__name__)


@dataclass(frozen=True)
class LoadResult:
    rows_read: int
    rows_written: int  # after ON CONFLICT
    landing_path: Path


def run_load_raw(
    *,
    conn: psycopg.Connection[Any],
    source: SourceConfig,
    landing_path: Path,
) -> LoadResult:
    """Load a landing JSONL file into ``source.raw_table``."""
    if not landing_path.exists():
        raise FileNotFoundError(f"landing file not found: {landing_path}")

    schema, table = split_schema_table(source.raw_table)
    tracker = RunTracker(conn=conn, source=source.name, stage="load_raw")

    try:
        tracker.start()

        rows_read = 0
        rows_written = 0

        with conn.cursor() as cur:
            # Temp table lives only for this transaction.
            cur.execute(
                """
                CREATE TEMP TABLE tmp_raw_load (
                    unit_result_id text NOT NULL,
                    _id            bigint NOT NULL,
                    payload        jsonb NOT NULL
                ) ON COMMIT DROP
                """
            )

            copy_stmt = sql.SQL("COPY tmp_raw_load (unit_result_id, _id, payload) FROM STDIN")
            with cur.copy(copy_stmt) as cp, landing_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.rstrip("\n")
                    if not line:
                        continue
                    payload = json.loads(line)
                    unit_result_id = payload.get("unitResultID")
                    _id = payload.get("_id")
                    if unit_result_id is None or _id is None:
                        # Structurally broken row — skip and log; validation will not see it.
                        log.warning(
                            "load_raw.skip_row_missing_keys",
                            source=source.name,
                            has_id=_id is not None,
                            has_business_key=unit_result_id is not None,
                        )
                        continue
                    cp.write_row((str(unit_result_id), int(_id), json.dumps(payload)))
                    rows_read += 1

            # Merge into raw with idempotent conflict handling.
            merge = sql.SQL(
                """
                INSERT INTO {schema}.{table} (unit_result_id, _id, payload, extract_run_id)
                SELECT t.unit_result_id, t._id, t.payload, %s
                FROM tmp_raw_load AS t
                ON CONFLICT (unit_result_id) DO NOTHING
                """
            ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))
            cur.execute(merge, (tracker.run_id,))
            rows_written = cur.rowcount

        conn.commit()

        tracker.rows_read = rows_read
        tracker.rows_written = rows_written
        tracker.rows_rejected = 0
        tracker.finish_success()

        return LoadResult(rows_read=rows_read, rows_written=rows_written, landing_path=landing_path)
    except Exception as exc:
        conn.rollback()
        tracker.finish_failure(str(exc))
        raise
