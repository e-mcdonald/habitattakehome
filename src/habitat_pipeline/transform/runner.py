"""Transform runner.

Executes ``sql/transforms/*.sql`` in filename order. Each file is expected to
be idempotent (``CREATE TABLE IF NOT EXISTS``, ``ON CONFLICT ...``); the runner
does not attempt to track "already applied" state — each run replays everything
against current staging.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg

from habitat_pipeline.logging import get_logger
from habitat_pipeline.observability.metrics import RunTracker

log = get_logger(__name__)


@dataclass(frozen=True)
class TransformResult:
    files_applied: list[str]


def run_transform(
    *,
    conn: psycopg.Connection[Any],
    source_name: str,
    sql_dir: Path,
) -> TransformResult:
    """Apply every SQL file under ``<sql_dir>/transforms/`` in filename order."""
    transforms_dir = sql_dir / "transforms"
    if not transforms_dir.exists():
        raise FileNotFoundError(f"transforms dir not found: {transforms_dir}")

    files = sorted(transforms_dir.glob("*.sql"))
    if not files:
        raise RuntimeError(f"no .sql files under {transforms_dir}")

    tracker = RunTracker(conn=conn, source=source_name, stage="transform")
    applied: list[str] = []

    try:
        tracker.start()

        with conn.cursor() as cur:
            for path in files:
                log.info("transform.apply", source=source_name, file=path.name)
                cur.execute(path.read_text(encoding="utf-8"))
                applied.append(path.name)

        conn.commit()

        tracker.rows_read = 0
        tracker.rows_written = 0
        tracker.finish_success()

        return TransformResult(files_applied=applied)
    except Exception as exc:
        conn.rollback()
        tracker.finish_failure(str(exc))
        raise
