"""Pipeline-run tracking and ops-table queries.

Every stage opens a :class:`RunTracker`, updates it on start/finish, and calls
:func:`record_rejected` for any rows that fail validation. That gives us
observability as data in ``ops.pipeline_runs`` and ``ops.rejected_records``.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg import sql

from habitat_pipeline.logging import get_logger

log = get_logger(__name__)


@dataclass
class RunTracker:
    """Bookkeeping for a single stage run.

    Emit ``start()`` before doing work; call ``finish_success()`` or
    ``finish_failure()`` in a ``finally``. All fields default to sensible
    zeros so a stage can update just the counters it cares about.
    """

    conn: psycopg.Connection[Any]
    source: str
    stage: str  # extract | load_raw | validate | transform
    run_id: uuid.UUID = field(default_factory=uuid.uuid4)
    rows_read: int = 0
    rows_written: int = 0
    rows_rejected: int = 0
    high_watermark_before: int | None = None
    high_watermark_after: int | None = None

    def start(self) -> None:
        """Insert the initial ``running`` row."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.pipeline_runs (
                    run_id, source, stage, started_at, status,
                    rows_read, rows_written, rows_rejected,
                    high_watermark_before
                ) VALUES (%s, %s, %s, %s, 'running', 0, 0, 0, %s)
                """,
                (
                    self.run_id,
                    self.source,
                    self.stage,
                    datetime.now(UTC),
                    self.high_watermark_before,
                ),
            )
        self.conn.commit()
        log.info(
            "pipeline_run.start",
            run_id=str(self.run_id),
            source=self.source,
            stage=self.stage,
            watermark=self.high_watermark_before,
        )

    def _finish(self, status: str, error: str | None) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ops.pipeline_runs
                SET ended_at = %s,
                    status = %s,
                    rows_read = %s,
                    rows_written = %s,
                    rows_rejected = %s,
                    high_watermark_after = %s,
                    error = %s
                WHERE run_id = %s
                """,
                (
                    datetime.now(UTC),
                    status,
                    self.rows_read,
                    self.rows_written,
                    self.rows_rejected,
                    self.high_watermark_after,
                    error,
                    self.run_id,
                ),
            )
        self.conn.commit()

    def finish_success(self) -> None:
        self._finish("success", None)
        log.info(
            "pipeline_run.success",
            run_id=str(self.run_id),
            source=self.source,
            stage=self.stage,
            rows_read=self.rows_read,
            rows_written=self.rows_written,
            rows_rejected=self.rows_rejected,
            watermark_after=self.high_watermark_after,
        )

    def finish_failure(self, error: str) -> None:
        self._finish("failed", error)
        log.error(
            "pipeline_run.failure",
            run_id=str(self.run_id),
            source=self.source,
            stage=self.stage,
            error=error,
        )


def record_rejected(
    conn: psycopg.Connection[Any],
    *,
    run_id: uuid.UUID,
    source: str,
    rejects: Iterable[tuple[dict[str, Any], str]],
) -> int:
    """Bulk-insert rejected records with their reasons.

    Returns the number of rows written.
    """
    count = 0
    with conn.cursor() as cur:
        for payload, reason in rejects:
            cur.execute(
                """
                INSERT INTO ops.rejected_records (run_id, source, raw_payload, reason)
                VALUES (%s, %s, %s::jsonb, %s)
                """,
                (run_id, source, json.dumps(payload), reason),
            )
            count += 1
    conn.commit()
    return count


def fetch_recent_runs(conn: psycopg.Connection[Any], limit: int = 20) -> list[dict[str, Any]]:
    """Latest N runs, most recent first."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                run_id::text,
                source,
                stage,
                status,
                started_at,
                ended_at,
                rows_read,
                rows_written,
                rows_rejected,
                high_watermark_before,
                high_watermark_after,
                error
            FROM ops.pipeline_runs
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        cols = [c.name for c in cur.description or []]
        return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def fetch_freshness(conn: psycopg.Connection[Any]) -> list[dict[str, Any]]:
    """One row per source with ingest lag and delivery horizon."""
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT * FROM ops.freshness"))
        cols = [c.name for c in cur.description or []]
        return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]
