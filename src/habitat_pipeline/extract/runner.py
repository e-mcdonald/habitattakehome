"""Extract runner: pull records from an extractor into a JSONL landing file.

The extractor is the thing that knows *how* to pull (pagination, retries,
throttling). The runner is the thing that knows *where* the results go and
how the run is tracked in ``ops.pipeline_runs``.
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
from habitat_pipeline.sources.base import (
    ExtractorContext,
    SourceConfig,
    get_extractor,
    split_schema_table,
)

log = get_logger(__name__)


@dataclass(frozen=True)
class ExtractResult:
    """Summary of a single extract run."""

    landing_path: Path
    rows_written: int
    watermark_before: int | None
    watermark_after: int | None


def _watermark_from_raw(conn: psycopg.Connection[Any], raw_table: str) -> int | None:
    """Return ``max(_id)`` from the source's raw table, or ``None`` on cold start."""
    schema, name = split_schema_table(raw_table)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass(%s) IS NOT NULL",
            (f"{schema}.{name}",),
        )
        exists = cur.fetchone()
        if not exists or not exists[0]:
            return None
        query = sql.SQL("SELECT MAX(_id) FROM {schema}.{table}").format(
            schema=sql.Identifier(schema), table=sql.Identifier(name)
        )
        cur.execute(query)
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None


def run_extract(
    *,
    conn: psycopg.Connection[Any],
    source: SourceConfig,
    data_dir: Path,
    limit: int | None = None,
) -> ExtractResult:
    """Run the extract stage for a single source.

    Args:
        conn: DB connection used only for watermark lookup + run tracking.
        source: Parsed source config.
        data_dir: Root of the landing tree. Files go to ``<data_dir>/landing/<source>/<run_id>.jsonl``.
        limit: If set, stop after this many rows. Used by demo commands.
    """
    watermark = _watermark_from_raw(conn, source.raw_table)
    tracker = RunTracker(
        conn=conn,
        source=source.name,
        stage="extract",
        high_watermark_before=watermark,
    )

    landing_dir = data_dir / "landing" / source.name
    landing_dir.mkdir(parents=True, exist_ok=True)
    landing_path = landing_dir / f"{tracker.run_id}.jsonl"

    try:
        tracker.start()

        extractor = get_extractor(source.extractor)
        context = ExtractorContext(source_name=source.name)
        rows_written = 0
        max_id_seen = watermark

        with landing_path.open("w", encoding="utf-8") as fh:
            for record in extractor.extract(source.extractor_config, watermark, context):
                fh.write(json.dumps(record, ensure_ascii=False))
                fh.write("\n")
                rows_written += 1
                rid = record.get("_id")
                if isinstance(rid, int) and (max_id_seen is None or rid > max_id_seen):
                    max_id_seen = rid
                if limit is not None and rows_written >= limit:
                    log.info(
                        "extract.limit_reached",
                        source=source.name,
                        limit=limit,
                        rows_written=rows_written,
                    )
                    break

        tracker.rows_read = rows_written
        tracker.rows_written = rows_written
        tracker.high_watermark_after = max_id_seen
        tracker.finish_success()

        return ExtractResult(
            landing_path=landing_path,
            rows_written=rows_written,
            watermark_before=watermark,
            watermark_after=max_id_seen,
        )
    except Exception as exc:
        tracker.finish_failure(str(exc))
        raise
